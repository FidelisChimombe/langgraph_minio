import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, TYPE_CHECKING, Union, Literal, Iterable, Iterator, Tuple, AsyncIterator, Sequence
from minio import Minio
from minio.error import S3Error
import logging
import io
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
import pickle

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class MinioStore:
    """A store that uses MinIO as the backend storage."""

    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket_name: str = None,
        client: Optional[Minio] = None,
    ):
        """Initialize the store.
        
        Args:
            endpoint_url: The MinIO server endpoint URL
            access_key: The MinIO access key
            secret_key: The MinIO secret key
            bucket_name: The name of the bucket to use
            client: An existing MinIO client to use
        """
        if not client and not (endpoint_url and access_key and secret_key):
            raise ValueError("Either client or endpoint_url, access_key, and secret_key must be provided")
        
        self.client = client or Minio(
            endpoint_url.replace("http://", "").replace("https://", ""),
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        self.bucket_name = bucket_name or "default"
        self._ensure_bucket_exists()

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
        """List objects in the store with a prefix.
        
        Args:
            prefix: The prefix to filter objects by
            
        Returns:
            List of object keys
        """
        try:
            objects = list(self.client.list_objects(self.bucket_name, prefix, True))
            return [obj.object_name for obj in objects]
        except Exception as e:
            logger.error(f"Failed to list objects: {e}")
            raise RuntimeError(f"Failed to list objects in bucket {self.bucket_name}: {e}")

    def get_object(self, key: str, include_metadata: bool = False, refresh_ttl: bool = False) -> Union[Optional[bytes], Optional[Tuple[bytes, Dict[str, str]]]]:
        """Get an object from MinIO.

        Args:
            key: The key of the object.
            include_metadata: Whether to include metadata in the response.
            refresh_ttl: Whether to refresh the TTL if it exists.

        Returns:
            If include_metadata is False, returns the object data as bytes or None if not found.
            If include_metadata is True, returns a tuple of (data, metadata) or None if not found.

        Raises:
            RuntimeError: If the object cannot be retrieved.
        """
        try:
            # Get object and metadata
            try:
                response = self.client.get_object(self.bucket_name, key)
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    return None
                raise

            # Extract metadata from headers
            metadata = {}
            for header_key, value in response.headers.items():
                if header_key.lower().startswith('x-amz-meta-'):
                    clean_key = header_key.lower()[11:]  # Remove 'x-amz-meta-' prefix
                    metadata[clean_key] = value

            # Read data
            data = response.read()
            response.close()

            if include_metadata:
                return (data, metadata)
            return data
        except Exception as e:
            logger.error(f"Failed to get object {key}: {e}")
            raise

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
            
            # Handle TTL if present in metadata
            if metadata and "ttl" in metadata:
                ttl = float(metadata["ttl"])
                if ttl > 0:
                    # Set expiration time
                    expiration = datetime.now(timezone.utc) + timedelta(seconds=ttl)
                    metadata_str["x-amz-meta-expiration"] = expiration.isoformat()
                    metadata_str["x-amz-meta-last_accessed"] = datetime.now(timezone.utc).isoformat()
            
            # Wrap data in BytesIO for MinIO client
            if isinstance(data, str):
                data_io = io.BytesIO(data.encode('utf-8'))
            else:
                data_io = io.BytesIO(data)
            
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=key,
                data=data_io,
                length=len(data),
                metadata=metadata_str
            )
        except Exception as e:
            logger.error(f"Error putting object {key}: {e}")
            raise

    def delete_object(self, key: str) -> None:
        """Delete an object from the store.
        
        Args:
            key: Object key
        """
        try:
            self.client.remove_object(self.bucket_name, key)
        except Exception as e:
            logger.error(f"Failed to delete object: {e}")
            raise

    def put(
        self,
        namespace: Tuple[str, ...],
        key: str,
        value: Dict[str, Any],
        index: Optional[Union[Literal[False], List[str]]] = None,
        *,
        ttl: Union[Optional[float], NotProvided] = NOT_PROVIDED
    ) -> None:
        """Store or update an item in the store.

        Args:
            namespace: Hierarchical path for the item, represented as a tuple of strings.
                Example: ("documents", "user123")
            key: Unique identifier within the namespace. Together with namespace forms
                the complete path to the item.
            value: Dictionary containing the item's data. Must contain string keys and
                JSON-serializable values.
            index: Controls how the item's fields are indexed for search:
                None (default): Use fields configured when creating store (if any)
                False: Disable indexing for this item
                list[str]: List of field paths to index, supporting:
                    - Nested fields: "metadata.title"
                    - Array access: "chapters[*].content"
                    - Specific indices: "authors[0].name"
            ttl: Time to live in seconds. If specified, item will expire after this many
                seconds from the last access time.
        """
        try:
            # Construct the full key from namespace and key
            full_key = "/".join(namespace + (key,))

            # Convert value to JSON and encode as bytes
            data = json.dumps(value).encode('utf-8')

            # Store the data
            self.put_object(full_key, data)

            # Store metadata
            metadata_key = f"{full_key}.metadata"
            now = datetime.now(timezone.utc).isoformat()
            metadata = {
                'created_at': now,
                'updated_at': now
            }
            
            # Add TTL if provided (convert to seconds)
            if ttl is not NOT_PROVIDED and ttl is not None:
                metadata.update({
                    'ttl': str(ttl),  # Store TTL in seconds
                    'last_accessed': now
                })

            metadata_data = json.dumps(metadata).encode('utf-8')
            self.put_object(metadata_key, metadata_data)

            # Handle indexing if needed
            if index is not False:
                # Store index information
                index_key = f"{full_key}.index"
                index_data = {
                    "fields": index or [],
                    "timestamp": now
                }
                index_bytes = json.dumps(index_data).encode('utf-8')
                self.put_object(index_key, index_bytes)

        except Exception as e:
            logger.error(f"Failed to put item: {e}")
            raise

    def get(
        self,
        namespace: Tuple[str, ...],
        key: str,
        *,
        refresh_ttl: Optional[bool] = None
    ) -> Optional[Dict[str, Any]]:
        """Get an item from the store.

        Args:
            namespace: Hierarchical path for the item
            key: Unique identifier within the namespace
            refresh_ttl: If True and item has TTL, update last access time

        Returns:
            The item's data as a dictionary, or None if not found
        """
        try:
            # Construct full key from namespace and key
            full_key = "/".join(namespace + (key,))
            metadata_key = f"{full_key}.metadata"

            # Get metadata first to check TTL
            metadata_bytes = self.get_object(metadata_key)
            if metadata_bytes is None:
                return None

            metadata = json.loads(metadata_bytes.decode('utf-8'))
            
            # Check TTL expiration
            if 'ttl' in metadata and metadata['ttl'] is not None:
                ttl_seconds = float(metadata['ttl'])  # TTL is stored in seconds
                last_accessed = datetime.fromisoformat(metadata['last_accessed'])
                now = datetime.now(timezone.utc)
                
                # If TTL has expired, delete the item and return None
                if (now - last_accessed).total_seconds() > ttl_seconds:
                    logger.debug(f"Object {full_key} has expired (TTL: {ttl_seconds} seconds, Last accessed: {last_accessed})")
                    self.delete(namespace, key)
                    return None

                # Update last_accessed if refresh_ttl is True
                if refresh_ttl:
                    metadata['last_accessed'] = now.isoformat()
                    metadata_data = json.dumps(metadata).encode('utf-8')
                    self.put_object(metadata_key, metadata_data)
                    logger.debug(f"Refreshed TTL for object {full_key} (new last_accessed: {now.isoformat()})")

            # Get the actual data with TTL refresh
            data_bytes = self.get_object(full_key, refresh_ttl=refresh_ttl)
            if data_bytes is None:
                return None

            return json.loads(data_bytes.decode('utf-8'))
        except Exception as e:
            logger.error(f"Failed to get item: {e}")
            raise

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

    def search(
        self,
        namespace_prefix: Tuple[str, ...],
        /,
        *,
        query: Optional[str] = None,
        filter: Optional[Dict[str, Any]] = None,
        limit: int = 10,
        offset: int = 0,
        refresh_ttl: Optional[bool] = None,
    ) -> List[SearchItem]:
        """Search for items within a namespace prefix.

        Args:
            namespace_prefix: Hierarchical path prefix to search within.
            query: Optional query for natural language search.
            filter: Key-value pairs to filter results.
            limit: Maximum number of items to return.
            offset: Number of items to skip before returning results.
            refresh_ttl: Whether to refresh TTLs for the returned items.
                If no TTL is specified, this argument is ignored.

        Returns:
            List of items matching the search criteria.
        """
        try:
            # Construct prefix for listing objects
            prefix = "/".join(namespace_prefix) if namespace_prefix else ""
            
            # List all objects in namespace
            objects = self.list_objects(prefix)
            if not objects:
                return []
            
            results = []
            for obj_key in objects:
                # Skip metadata and index files
                if obj_key.endswith(('.metadata', '.index')):
                    continue
                    
                # Get and parse the item
                data = self.get_object(obj_key)
                if data is None:
                    continue
                    
                try:
                    # Try to deserialize as pickle first
                    item = pickle.loads(data)
                except Exception:
                    try:
                        # Fall back to JSON if pickle fails
                        item = json.loads(data.decode('utf-8'))
                    except Exception as e:
                        logger.warning(f"Failed to deserialize data for {obj_key}: {e}")
                        continue
                
                # Apply filters if specified
                if filter and not self._match_filter(item, filter):
                    continue
                
                # Get metadata for timestamps
                metadata_key = f"{obj_key}.metadata"
                metadata_data = self.get_object(metadata_key)
                
                created_at = datetime.now().isoformat()
                updated_at = created_at
                
                if metadata_data is not None:
                    try:
                        metadata = json.loads(metadata_data.decode('utf-8'))
                        created_at = metadata.get('created_at', created_at)
                        updated_at = metadata.get('updated_at', updated_at)
                    except Exception as e:
                        logger.warning(f"Failed to parse metadata for {obj_key}: {e}")
                
                # Add to results
                results.append(SearchItem(
                    key=obj_key.split('/')[-1],
                    value=item,
                    namespace=namespace_prefix,
                    created_at=created_at,
                    updated_at=updated_at,
                    score=1.0  # Basic implementation without real scoring
                ))
            
            # Handle pagination
            if not results:
                return []
                
            # Handle None values for offset and limit
            offset = offset if offset is not None else 0
            limit = limit if limit is not None else len(results)
            
            start = min(offset, len(results))
            end = min(offset + limit, len(results))
            paginated_results = results[start:end]
            
            # Refresh TTLs if needed
            if refresh_ttl and paginated_results:
                for result in paginated_results:
                    metadata_key = f"{prefix}/{result.key}.metadata"
                    metadata_data = self.get_object(metadata_key)
                    if metadata_data is not None:
                        try:
                            metadata = json.loads(metadata_data.decode('utf-8'))
                            if 'ttl' in metadata:
                                metadata['last_accessed'] = datetime.now().isoformat()
                                self.put_object(metadata_key, json.dumps(metadata).encode('utf-8'))
                        except Exception as e:
                            logger.warning(f"Failed to refresh TTL for {metadata_key}: {e}")
            
            return paginated_results

        except Exception as e:
            logger.error(f"Failed to search items: {e}")
            raise

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
        """List and filter namespaces in the store.

        Used to explore the organization of data, find specific collections, or navigate 
        the namespace hierarchy.

        Args:
            prefix: Filter namespaces that start with this path.
            suffix: Filter namespaces that end with this path.
            max_depth: Return namespaces up to this depth in the hierarchy.
                      Namespaces deeper than this level will be truncated.
            limit: Maximum number of namespaces to return (default 100).
            offset: Number of namespaces to skip for pagination (default 0).

        Returns:
            List[Tuple[str, ...]]: A list of namespace tuples that match the criteria.
            Each tuple represents a full namespace path up to max_depth.
        """
        try:
            # Get prefix string for listing objects
            prefix_str = "/".join(prefix) + "/" if prefix else ""
            
            # List all objects
            objects = self.list_objects(prefix_str)
            
            # Extract unique namespaces
            namespaces = set()
            for obj_key in objects:
                # Skip metadata and index files
                if obj_key.endswith(('.metadata', '.index')):
                    continue
                
                # Split path into components
                parts = tuple(obj_key.split('/')[:-1])  # Exclude the key
                if not parts:  # Skip if no namespace parts
                    continue
                
                # Apply max_depth if specified
                if max_depth is not None:
                    parts = parts[:max_depth]
                
                # Apply suffix filter if specified
                if suffix:
                    if len(parts) < len(suffix):
                        continue
                    if parts[-len(suffix):] != suffix:
                        continue
                
                namespaces.add(parts)
            
            # Convert to sorted list and apply pagination
            sorted_namespaces = sorted(list(namespaces))
            
            # Handle empty results
            if not sorted_namespaces:
                return []
                
            # Handle offset
            if offset:
                if offset >= len(sorted_namespaces):
                    return []
                
                end_idx = min(offset + limit, len(sorted_namespaces))   
                return sorted_namespaces[offset:end_idx]
            else:
                return sorted_namespaces

        except Exception as e:
            logger.error(f"Failed to list namespaces: {e}")
            raise

    def batch(self, ops: Iterable[Op]) -> List[Result]:
        """Execute multiple operations synchronously in a single batch.

        Args:
            ops: An iterable of operations to execute.

        Returns:
            A list of results, where each result corresponds to an operation in the input.
            The order of results matches the order of input operations.
        """
        results = []
        for op in ops:
            if isinstance(op, GetOp):
                result = self.get(op.namespace, op.key, refresh_ttl=op.refresh_ttl)
            elif isinstance(op, PutOp):
                self.put(op.namespace, op.key, op.value, op.index, ttl=op.ttl)
                result = None
            elif isinstance(op, SearchOp):
                result = self.search(
                    op.namespace_prefix,
                    query=op.query,
                    filter=op.filter,
                    limit=op.limit,
                    offset=op.offset,
                    refresh_ttl=op.refresh_ttl
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

