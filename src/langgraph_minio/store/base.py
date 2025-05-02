import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, TYPE_CHECKING, Union, Literal, Iterable, Tuple
from minio import Minio
import logging
import io
import threading
import time
from collections import defaultdict
from langgraph.store.base import (
    SearchItem,
    GetOp,
    PutOp,
    SearchOp,
    ListNamespacesOp,
    NotProvided,
    NOT_PROVIDED,
    Op,
    Result
)
import aiohttp
from datetime import timedelta
from minio.error import S3Error


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class WriteBuffer:
    """A thread-safe buffer for write operations."""
    
    def __init__(self, client: Minio, bucket_name: str, max_size: int = 1000, flush_interval: float = 1.0):
        self.client = client
        self.bucket_name = bucket_name
        self.max_size = max_size
        self.flush_interval = flush_interval
        self.buffer = defaultdict(list)
        self.lock = threading.Lock()
        self.last_flush = time.time()
        self.flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self.flush_thread.start()
    
    def add(self, key: str, data: bytes, metadata: Dict[str, str]) -> None:
        """Add a write operation to the buffer."""
        with self.lock:
            self.buffer[key].append((data, metadata))
            if len(self.buffer) >= self.max_size:
                self._flush()
    
    def _flush(self) -> None:
        """Flush the buffer to storage."""
        with self.lock:
            if not self.buffer:
                return
            
            # Group writes by key
            grouped_writes = {}
            for key, writes in self.buffer.items():
                if not writes:
                    continue
                
                # Take the latest write for each key
                data, metadata = writes[-1]
                grouped_writes[key] = (data, metadata)
            
            self.buffer.clear()
            self.last_flush = time.time()
            
            # Write to MinIO
            for key, (data, metadata) in grouped_writes.items():
                try:
                    # Convert metadata to MinIO format
                    metadata_str = {}
                    if metadata:
                        for k, v in metadata.items():
                            if not k.lower().startswith('x-amz-meta-'):
                                metadata_str[f'x-amz-meta-{k}'] = str(v)
                            else:
                                metadata_str[k] = str(v)
                    
                    # Write to MinIO
                    self.client.put_object(
                        bucket_name=self.bucket_name,
                        object_name=key,
                        data=io.BytesIO(data),
                        length=len(data),
                        metadata=metadata_str
                    )
                    logger.debug(f"Flushed write for key {key}")
                except Exception as e:
                    logger.error(f"Failed to flush write for key {key}: {e}")
    
    def _flush_loop(self) -> None:
        """Background thread that periodically flushes the buffer."""
        while True:
            time.sleep(self.flush_interval)
            self._flush()

class Cache:
    """A thread-safe cache for frequently accessed data."""
    
    def __init__(self, max_size: int = 100000, ttl: float = 5.0):
        self.max_size = max_size
        self.ttl = ttl
        self.cache = {}
        self.timestamps = {}
        self.lock = threading.Lock()
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
    
    def get(self, key: str) -> Optional[bytes]:
        """Get a value from the cache."""
        with self.lock:
            if key not in self.cache:
                return None
            
            # Check if item has expired
            if time.time() - self.timestamps[key] > self.ttl:
                del self.cache[key]
                del self.timestamps[key]
                return None
            
            return self.cache[key]
    
    def put(self, key: str, value: bytes) -> None:
        """Put a value in the cache."""
        with self.lock:
            # Remove oldest item if cache is full
            if len(self.cache) >= self.max_size:
                oldest_key = min(self.timestamps.items(), key=lambda x: x[1])[0]
                del self.cache[oldest_key]
                del self.timestamps[oldest_key]
            
            self.cache[key] = value
            self.timestamps[key] = time.time()
    
    def _cleanup_loop(self) -> None:
        """Background thread that periodically cleans up expired items."""
        while True:
            time.sleep(self.ttl)
            with self.lock:
                current_time = time.time()
                expired_keys = [
                    key for key, timestamp in self.timestamps.items()
                    if current_time - timestamp > self.ttl
                ]
                for key in expired_keys:
                    del self.cache[key]
                    del self.timestamps[key]

class MinioStore:
    """A high-performance MinIO store with caching and write buffering."""

    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket_name: str = None,
        client: Optional[Minio] = None,
        cache_size: int = 100000,
        cache_ttl: float = 5.0,
        buffer_size: int = 1000,
        buffer_interval: float = 1.0,
    ):
        """Initialize the store.
        
        Args:
            endpoint_url: The MinIO server endpoint URL
            access_key: The MinIO access key
            secret_key: The MinIO secret key
            bucket_name: The name of the bucket to use
            client: An existing MinIO client to use
            cache_size: Maximum number of items to cache (default: 100000)
            cache_ttl: Time-to-live for cached items in seconds (default: 5.0)
            buffer_size: Maximum number of writes to buffer (default: 1000)
            buffer_interval: Interval between buffer flushes in seconds (default: 1.0)
        """
        if not bucket_name:
            raise ValueError("bucket_name is required")
            
        try:
            if client:
                self.client = client
            else:
                if not endpoint_url:
                    raise ValueError("endpoint_url is required")
                if not access_key:
                    raise ValueError("access_key is required") 
                if not secret_key:
                    raise ValueError("secret_key is required")
                    
                self.client = Minio(
                    endpoint_url.replace("http://", "").replace("https://", ""),
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=False,
                    http_client=None  # Let MinIO handle HTTP client creation
                )
                
            self.bucket_name = bucket_name
            
            # Test connection by checking bucket exists
            self.client.bucket_exists(self.bucket_name)
            
            # Initialize cache and write buffer
            self.cache = Cache(max_size=cache_size, ttl=cache_ttl)
            self.write_buffer = WriteBuffer(
                client=self.client,
                bucket_name=self.bucket_name,
                max_size=buffer_size,
                flush_interval=buffer_interval
            )
            
        except Exception as e:
            raise RuntimeError(f"Failed to initialize MinIO client: {str(e)}")

    def _ensure_bucket_exists(self) -> None:
        """Ensure the bucket exists, creating it if necessary."""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.debug(f"Created bucket {self.bucket_name}")
            else:
                logger.debug(f"Bucket {self.bucket_name} already exists")
            
            # Test bucket access by listing objects
            objects = list(self.client.list_objects(self.bucket_name, "", True))
            logger.debug("Bucket access test successful")
        except Exception as e:
            logger.error(f"Failed to ensure bucket {self.bucket_name} exists or is accessible: {e}")
            raise RuntimeError(f"Failed to access bucket {self.bucket_name}: {e}")

    def list_objects(self, prefix: str = "") -> List[str]:
        """List objects in the store with a prefix and caching."""
        # Use a cache key for the list operation
        cache_key = f"list:{prefix}"
        
        # Try cache first
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return json.loads(cached_result.decode())
        
        # If not in cache, get from MinIO
        try:
            objects = []
            for obj in self.client.list_objects(self.bucket_name, prefix, True):
                objects.append(obj.object_name)
            
            # Cache the result with a short TTL (5 seconds) to prevent excessive calls
            self.cache.put(cache_key, json.dumps(objects).encode())
            
            return objects
        except Exception as e:
            logger.error(f"Failed to list objects with prefix {prefix}: {e}")
            raise RuntimeError(f"Failed to list objects in bucket {self.bucket_name}: {e}")

    def get_object(self, key: str, include_metadata: bool = False, refresh_ttl: bool = False) -> Union[Optional[bytes], Optional[Tuple[bytes, Dict[str, str]]]]:
        """Get an object from MinIO with caching."""
        # Try cache first
        cached_data = self.cache.get(key)
        if cached_data is not None:
            if include_metadata:
                # Get metadata from MinIO
                try:
                    metadata = {}
                    response = self.client.stat_object(self.bucket_name, key)
                    for key, value in response.metadata.items():
                        if key.startswith('x-amz-meta-'):
                            metadata[key[11:]] = value
                    return (cached_data, metadata)
                except Exception as e:
                    logger.warning(f"Failed to get metadata: {e}")
                    return (cached_data, {})
            return cached_data

        # If not in cache, get from MinIO
        try:
            response = self.client.get_object(self.bucket_name, key)
            data = response.read()
            response.close()
            response.release_conn()

            # Cache the data
            self.cache.put(key, data)

            if include_metadata:
                metadata = {}
                try:
                    response = self.client.stat_object(self.bucket_name, key)
                    for key, value in response.metadata.items():
                        if key.startswith('x-amz-meta-'):
                            metadata[key[11:]] = value
                except Exception as e:
                    logger.warning(f"Failed to get metadata: {e}")
                return (data, metadata)

            return data
        except Exception as e:
            logger.error(f"Failed to get object {key}: {e}")
            return None

    def put_object(self, key: str, data: bytes, metadata: Optional[Dict[str, str]] = None) -> None:
        """Put an object in the store."""
        try:
            # Convert metadata to MinIO format
            metadata_str = {}
            if metadata:
                for k, v in metadata.items():
                    if not k.lower().startswith('x-amz-meta-'):
                        metadata_str[f'x-amz-meta-{k}'] = str(v)
                    else:
                        metadata_str[k] = str(v)
            
            # Put object
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=key,
                data=io.BytesIO(data),
                length=len(data),
                metadata=metadata_str
            )
            
            # Cache the data
            self.cache.put(key, data)
            
        except Exception as e:
            logger.error(f"Failed to put object {key}: {e}")
            raise RuntimeError(f"Failed to put object in bucket {self.bucket_name}: {e}")

    def delete_object(self, key: str) -> None:
        """Delete an object from the store."""
        # Remove from cache
        self.cache.put(key, None)  # Using None as a sentinel value
        
        # Delete from MinIO
        try:
            self.client.remove_object(self.bucket_name, key)
        except Exception as e:
            logger.error(f"Failed to delete object: {e}")
            raise

    def put(self, namespace, key, data, ttl=None):
        """Put an object in the store."""
        self._ensure_bucket_exists()
        object_key = self._get_object_key(namespace, key)
        metadata_key = f"{object_key}.metadata"

        # Create metadata with ISO format timestamps
        metadata = {
            'created_at': datetime.now(timezone.utc).isoformat(),
            'updated_at': datetime.now(timezone.utc).isoformat(),
            'namespace': namespace,
            'key': key
        }
        
        if ttl is not None:
            metadata.update({
                'ttl': str(ttl * 60),  # Convert minutes to seconds
                'last_accessed': str(time.time())  # Use Unix timestamp for easier comparison
            })

        # Put the object
        try:
            data_bytes = json.dumps(data).encode('utf-8')
            self.client.put_object(
                self.bucket_name,
                object_key,
                io.BytesIO(data_bytes),
                len(data_bytes)
            )
            self._put_metadata(metadata_key, metadata)
        except Exception as e:
            logger.warning(f"Failed to put object {object_key}: {str(e)}")
            raise

    def get(self, namespace, key, refresh_ttl=False):
        """Get an object from the store."""
        self._ensure_bucket_exists()
        object_key = self._get_object_key(namespace, key)
        metadata_key = self._get_metadata_key(object_key)

        try:
            # Get metadata first to check TTL
            metadata = self._get_metadata(metadata_key)
            if metadata and 'ttl' in metadata:
                last_accessed = time.time()
                if 'last_accessed' in metadata:
                    try:
                        last_accessed = float(metadata['last_accessed'])
                    except ValueError:
                        # Handle ISO format timestamp
                        dt = datetime.fromisoformat(metadata['last_accessed'])
                        last_accessed = dt.timestamp()

                ttl = float(metadata['ttl'])
                if time.time() - last_accessed > ttl:
                    # TTL expired, delete object and metadata
                    self.delete(namespace, key)
                    return None

            # Get the actual object
            response = self.client.get_object(self.bucket_name, object_key)
            data = json.loads(response.read().decode('utf-8'))

            # Refresh TTL if requested and TTL exists
            if refresh_ttl and metadata and 'ttl' in metadata:
                metadata['last_accessed'] = str(time.time())  # Store as Unix timestamp
                self._put_metadata(metadata_key, metadata)

            return data
        except S3Error as e:
            if e.code == 'NoSuchKey':
                return None
            raise
        except Exception as e:
            logger.error(f"Failed to get object {object_key}: {str(e)}")
            return None

    def _match_filter(self, item: Dict[str, Any], filter: Dict[str, Any]) -> bool:
        """Match an item against a filter.
        
        Args:
            item: The item to match
            filter: The filter to match against
            
        Returns:
            True if the item matches the filter, False otherwise
        """
        for key, value in filter.items():
            if key not in item:
                return False
            
            if isinstance(value, dict):
                # Handle operators
                for op, op_value in value.items():
                    if op == "$eq":
                        if item[key] != op_value:
                            return False
                    elif op == "$ne":
                        if item[key] == op_value:
                            return False
                    elif op == "$gt":
                        if not item[key] > op_value:
                            return False
                    elif op == "$lt":
                        if not item[key] < op_value:
                            return False
                    elif op == "$gte":
                        if not item[key] >= op_value:
                            return False
                    elif op == "$lte":
                        if not item[key] <= op_value:
                            return False
                    elif op == "$in":
                        if item[key] not in op_value:
                            return False
                    elif op == "$nin":
                        if item[key] in op_value:
                            return False
                    else:
                        raise ValueError(f"Unsupported operator: {op}")
            else:
                # Simple equality
                if item[key] != value:
                    return False
                
        return True
    def search(self, namespace_prefix, query=None, filter=None, limit=None, offset=None):
        """Search for objects in the store."""
        self._ensure_bucket_exists()
        prefix = self._get_object_key(namespace_prefix, "")
        
        try:
            objects = self.client.list_objects(self.bucket_name, prefix=prefix, recursive=True)
            results = []
            
            for obj in objects:
                if obj.object_name.endswith('.metadata'):
                    continue
                    
                try:
                    # Get object data
                    response = self.client.get_object(self.bucket_name, obj.object_name)
                    data = json.loads(response.read().decode('utf-8'))
                    response.close()
                    response.release_conn()
                    
                    # Get metadata
                    metadata_key = self._get_metadata_key(obj.object_name)
                    metadata = self._get_metadata(metadata_key) or {}
                    
                    # Apply filter if provided
                    if filter:
                        if not self._match_filter(data, filter):
                            continue
                    
                    # Create search result
                    namespace_parts = obj.object_name.split('/')
                    key = namespace_parts[-1]
                    namespace = tuple(namespace_parts[:-1])
                    
                    # Convert timestamps to ISO format if needed
                    created_at = metadata.get('created_at', datetime.now(timezone.utc).isoformat())
                    updated_at = metadata.get('updated_at', datetime.now(timezone.utc).isoformat())
                    
                    result = SearchItem(
                        namespace=namespace,
                        key=key,
                        value=data,
                        created_at=created_at,
                        updated_at=updated_at
                    )
                    results.append(result)
                    
                except Exception as e:
                    logger.warning(f"Failed to process object {obj.object_name}: {str(e)}")
                    continue
            
            # Apply pagination
            if offset:
                results = results[offset:]
            if limit:
                results = results[:limit]
                
            return results
            
        except Exception as e:
            logger.warning(f"Failed to search objects with prefix {prefix}: {str(e)}")
            return []

    def delete(self, namespace: Tuple[str, ...], key: str) -> None:
        """Delete an item.

        Args:
            namespace: Hierarchical path for the item.
            key: Unique identifier within the namespace.
        """
        try:
            # Construct full key from namespace and key
            full_key = "/".join(namespace + (key,))

            # Delete the main data object
            self.delete_object(full_key)

            # Delete associated metadata and index files if they exist
            metadata_key = f"{full_key}.metadata"
            index_key = f"{full_key}.index"
            
            self.delete_object(metadata_key)
            self.delete_object(index_key)

        except Exception as e:
            logger.error(f"Failed to delete item: {e}")
            raise

    def list_namespaces(
        self,
        *,
        prefix: Optional[Tuple[str, ...]] = None,
        suffix: Optional[Tuple[str, ...]] = None,
        max_depth: Optional[int] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Tuple[str, ...]]:
        """List namespaces in the store."""
        try:
            # Convert prefix and suffix to strings for MinIO
            prefix_str = "/".join(prefix) + "/" if prefix else ""
            suffix_str = "/".join(suffix) if suffix else ""
            
            # Get all objects with the prefix
            objects = []
            for obj in self.client.list_objects(self.bucket_name, prefix_str, True):
                if suffix_str and not obj.object_name.endswith(suffix_str):
                    continue
                objects.append(obj.object_name)
            
            # Extract namespaces from object names
            namespaces = set()
            for obj_name in objects:
                parts = obj_name.split("/")
                if max_depth is not None and len(parts) > max_depth:
                    continue
                namespace = tuple(parts[:-1])  # Remove the key part
                if namespace:
                    namespaces.add(namespace)
            
            # Apply limit and offset
            namespaces = sorted(namespaces)
            return namespaces[offset:offset + limit]
            
        except Exception as e:
            logger.error(f"Failed to list namespaces: {e}")
            raise RuntimeError(f"Failed to list namespaces in bucket {self.bucket_name}: {e}")

    def batch(self, ops: Iterable[Op]) -> List[Result]:
        """Execute multiple operations synchronously in a single batch."""
        results = []
        for op in ops:
            if isinstance(op, GetOp):
                result = self.get(op.namespace, op.key, refresh_ttl=op.refresh_ttl)
            elif isinstance(op, PutOp):
                self.put(op.namespace, op.key, op.value, ttl=op.ttl)
                result = None
            # DeleteOp is not supported in langgraph.store.base
            elif isinstance(op, SearchOp):
                result = self.search(
                    op.namespace_prefix,
                    query=op.query,
                    filter=op.filter,
                    limit=op.limit,
                    offset=op.offset
                )
            elif isinstance(op, ListNamespacesOp):
                result = self.list_namespaces(
                    prefix=op.match_conditions,
                    max_depth=op.max_depth,
                    limit=op.limit,
                    offset=op.offset
                )
            else:
                raise ValueError(f"Unsupported operation type: {type(op)}")
            results.append(result)
        return results

    async def aput(
        self,
        namespace: Tuple[str, ...],
        key: str,
        data: Dict[str, Any],
        *,
        ttl: Optional[float] = None
    ) -> None:
        """Asynchronously put an item into the store.

        Args:
            namespace: Hierarchical path for the item
            key: Unique identifier within the namespace
            data: Dictionary of data to store
            ttl: Optional time-to-live in minutes
        """
        try:
            # Construct full key from namespace and key
            full_key = "/".join(namespace + (key,))
            metadata_key = f"{full_key}.metadata"

            # Store the data
            data_bytes = json.dumps(data).encode('utf-8')
            await self.aput_object(full_key, data_bytes)

            # Store metadata with timestamps and TTL
            metadata = {
                'created': datetime.now().isoformat(),
                'last_accessed': datetime.now().isoformat(),
                'ttl': ttl
            }
            metadata_bytes = json.dumps(metadata).encode('utf-8')
            await self.aput_object(metadata_key, metadata_bytes)
        except Exception as e:
            logger.error(f"Failed to put item: {e}")
            raise

    async def aput_object(self, key: str, data: bytes) -> None:
        """Asynchronously put a raw object into MinIO.

        Args:
            key: Object key
            data: Raw bytes to store
        """
        try:
            async with aiohttp.ClientSession() as session:
                # Get presigned URL for PUT
                url = self.client.get_presigned_url(
                    "PUT",
                    self.bucket_name,
                    key,
                    expires=timedelta(minutes=5)
                )
                
                # Upload data using presigned URL
                async with session.put(url, data=data) as response:
                    response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to put object: {e}")
            raise

    def _get_object_key(self, namespace, key):
        """Get the full object key from namespace and key."""
        namespace_str = "/".join(namespace) if namespace else ""
        return f"{namespace_str}/{key}" if namespace_str else key

    def _get_metadata_key(self, object_key):
        """Get the metadata key for an object."""
        return f"{object_key}.metadata"

    def _put_metadata(self, metadata_key, metadata):
        """Put metadata for an object."""
        try:
            metadata_bytes = json.dumps(metadata).encode('utf-8')
            self.client.put_object(
                self.bucket_name,
                metadata_key,
                io.BytesIO(metadata_bytes),
                len(metadata_bytes)
            )
        except Exception as e:
            logger.warning(f"Failed to put metadata {metadata_key}: {str(e)}")
            raise

    def _get_metadata(self, metadata_key):
        """Get metadata for an object."""
        try:
            response = self.client.get_object(self.bucket_name, metadata_key)
            return json.loads(response.read().decode('utf-8'))
        except S3Error as e:
            if e.code == 'NoSuchKey':
                return None
            raise
        except Exception as e:
            logger.warning(f"Failed to get metadata {metadata_key}: {str(e)}")
            return None

