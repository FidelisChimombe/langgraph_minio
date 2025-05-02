from typing import Dict, Any, List, Optional, Tuple, cast, TypedDict, Sequence, Iterator, Union, TypeVar, Generic
from typing_extensions import TypeAlias
import json
from langchain_core.runnables import RunnableConfig
from langgraph_minio.store.base import MinioStore
from langgraph.checkpoint.base import BaseCheckpointSaver, CheckpointTuple
import logging
from dataclasses import dataclass
from datetime import datetime, UTC, timezone
from langgraph.checkpoint.base import SendProtocol, ChannelVersions, PendingWrite
import pickle

# Type aliases
PendingSend: TypeAlias = Tuple[str, bytes]
ChannelValue: TypeAlias = Any
ChannelVersions: TypeAlias = Dict[str, str]

# Type variable for store
StoreType = TypeVar('StoreType', bound=MinioStore)

class Checkpoint(TypedDict):
    """State snapshot at a given point in time."""
    
    v: int
    """The version of the checkpoint format. Currently 1."""
    
    id: str
    """The ID of the checkpoint. This is both unique and monotonically
    increasing, so can be used for sorting checkpoints from first to last."""
    
    ts: str
    """The timestamp of the checkpoint in ISO 8601 format."""
    
    channel_values: Dict[str, Any]
    """The values of the channels at the time of the checkpoint.
    Mapping from channel name to deserialized channel snapshot value."""
    
    channel_versions: ChannelVersions
    """The versions of the channels at the time of the checkpoint.
    The keys are channel names and the values are monotonically increasing
    version strings for each channel."""
    
    versions_seen: Dict[str, ChannelVersions]
    """Map from node ID to map from channel name to version seen.
    This keeps track of the versions of the channels that each node has seen.
    Used to determine which nodes to execute next."""
    
    pending_sends: List[SendProtocol]
    """List of inputs pushed to nodes but not yet processed.
    Cleared by the next checkpoint."""
    
    @classmethod
    def from_prosci(cls, checkpoint_id: str, data: Dict[str, Any], description: Optional[str] = None) -> "Checkpoint":
        """Create a checkpoint from ProSci format.
        
        Args:
            checkpoint_id: The ID of the checkpoint
            data: The checkpoint data
            description: Optional description of the checkpoint
            
        Returns:
            A new Checkpoint instance
        """
        return cls(
            v=1,
            id=checkpoint_id,
            ts=datetime.now(UTC).isoformat(),
            channel_values=data,
            channel_versions={},
            versions_seen={},
            pending_sends=[]
        )
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Checkpoint":
        """Create a checkpoint from a dictionary.
        
        Args:
            data: Dictionary representation of the checkpoint
            
        Returns:
            The created checkpoint
        """
        return cls(
            v=data["v"],
            id=data["id"],
            ts=data["ts"],
            channel_values=data["channel_values"],
            channel_versions=data["channel_versions"],
            versions_seen=data["versions_seen"],
            pending_sends=data.get("pending_sends", [])
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the checkpoint to a dictionary.
        
        Returns:
            Dictionary representation of the checkpoint
        """
        return {
            "v": self.v,
            "id": self.id,
            "ts": self.ts,
            "channel_values": self.channel_values,
            "channel_versions": self.channel_versions,
            "versions_seen": self.versions_seen,
            "pending_sends": self.pending_sends
        }

@dataclass
class CheckpointMetadata:
    """Metadata for a checkpoint."""
    
    checkpoint_id: str
    timestamp: datetime
    version: str
    description: str
    tags: List[str]
    metadata: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the metadata to a dictionary.
        
        Returns:
            Dictionary representation of the metadata
        """
        return {
            "checkpoint_id": self.checkpoint_id,
            "timestamp": self.timestamp.isoformat(),
            "version": self.version,
            "description": self.description,
            "tags": self.tags,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CheckpointMetadata":
        """Create metadata from a dictionary.
        
        Args:
            data: Dictionary representation of the metadata
            
        Returns:
            The created metadata
        """
        return cls(
            checkpoint_id=data["checkpoint_id"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            version=data["version"],
            description=data["description"],
            tags=data["tags"],
            metadata=data["metadata"],
        )

logger = logging.getLogger(__name__)

class CheckpointMetadata(TypedDict):
    """Type definition for checkpoint metadata."""
    pass  # This is a flexible dictionary type

class Serde:
    """Serialization/deserialization utility."""
    
    def loads_typed(self, data: Tuple[str, Any]) -> Any:
        """Deserialize typed data.
        
        Args:
            data: Tuple of (type, value)
            
        Returns:
            Deserialized value
        """
        type_str, value = data
        if type_str == "json":
            return json.loads(value)
        elif type_str == "str":
            return str(value)
        elif type_str == "int":
            return int(value)
        elif type_str == "float":
            return float(value)
        elif type_str == "bool":
            return bool(value)
        elif type_str == "bytes":
            return bytes(value)
        elif type_str == "none":
            return None
        else:
            return value
            
    def dumps_typed(self, value: Any) -> Tuple[str, Any]:
        """Serialize value with type information.
        
        Args:
            value: Value to serialize
            
        Returns:
            Tuple of (type, serialized_value)
        """
        if value is None:
            return ("none", None)
        elif isinstance(value, str):
            return ("str", value)
        elif isinstance(value, int):
            return ("int", value)
        elif isinstance(value, float):
            return ("float", value)
        elif isinstance(value, bool):
            return ("bool", value)
        elif isinstance(value, bytes):
            return ("bytes", value)
        else:
            return ("json", json.dumps(value))

class MinioSaver(BaseCheckpointSaver, Generic[StoreType]):
    """A high-performance checkpoint saver that uses MinIO as the backend storage."""
    
    EMPTY_ID_SENTINEL = "empty"
    
    def __init__(
        self,
        store: StoreType,
        cache_size: int = 10000,
        cache_ttl: float = 5.0,
        buffer_size: int = 1000,
        buffer_interval: float = 1.0,
    ):
        """Initialize the checkpoint saver.
        
        Args:
            store: The MinIO store to use
            cache_size: Maximum number of items to cache (default: 10000)
            cache_ttl: Time-to-live for cached items in seconds (default: 5.0)
            buffer_size: Maximum number of writes to buffer (default: 1000)
            buffer_interval: Interval between buffer flushes in seconds (default: 1.0)
        """
        if store is None:
            raise ValueError("Store cannot be None")
        
        
        self.store = store
        self.serde = Serde()
        self.prefix = "checkpoints"  # Set the prefix here
        
        # Configure store with optimized settings if available
        if hasattr(store, 'cache'):
            store.cache.max_size = cache_size
            store.cache.ttl = cache_ttl
        if hasattr(store, 'write_buffer'):
            store.write_buffer.max_size = buffer_size
            store.write_buffer.flush_interval = buffer_interval

    def _get_key(self, thread_id: str, checkpoint_id: str, checkpoint_ns: str = "") -> str:
        """Get the key for a checkpoint."""
        def sanitize(component: str) -> str:
            """Sanitize a path component to be a valid S3 object name."""
            if not component:
                return ""
            # Replace invalid chars with underscore
            sanitized = "".join(c if c.isalnum() or c in "-_" else "_" for c in component)
            # Collapse multiple underscores
            while "__" in sanitized:
                sanitized = sanitized.replace("__", "_")
            # Remove leading/trailing underscores and hyphens
            sanitized = sanitized.strip("_-")
            # Ensure the result is not empty
            result = sanitized or "empty"
            return result
                
        # Sanitize each component
        thread_id = sanitize(thread_id)
        checkpoint_id = sanitize(checkpoint_id)
        checkpoint_ns = sanitize(checkpoint_ns)
                
        # Build path components, filtering out empty ones
        path_components = []
        if self.prefix:
            path_components.append(self.prefix)
        if thread_id:
            path_components.append(thread_id)
        if checkpoint_ns:
            path_components.append(checkpoint_ns)
        if checkpoint_id:
            path_components.append(checkpoint_id)
            path_components.append("checkpoint.json")
                
        # Join with forward slashes and ensure no double slashes
        key = "/".join(path_components)
        while "//" in key:
            key = key.replace("//", "/")
        return key

    def _get_checkpoint_key(self, thread_id: str, checkpoint_ns: str, checkpoint_id: str) -> str:
        """Get the key for a checkpoint."""
        # Ensure checkpoint_ns is not None or empty
        if not checkpoint_ns:
            checkpoint_ns = ""
        return self._get_key(thread_id, checkpoint_id, checkpoint_ns)

    def _get_checkpoint_blob_key(self, thread_id: str, checkpoint_ns: str, channel: str, version: str) -> str:
        """Get the key for a checkpoint blob."""
        base_key = self._get_key(thread_id, "", checkpoint_ns)
        return f"{base_key}/channels/{channel}/{version}/blob.json"

    def _get_checkpoint_write_key(
        self,
        thread_id: str,
        checkpoint_id: str,
        task_id: str,
        task_path: Optional[str] = None,
        channel: str = "",
    ) -> str:
        """Get the key for a checkpoint write.

        Args:
            thread_id: ID of the thread
            checkpoint_id: ID of the checkpoint
            task_id: ID of the task
            task_path: Optional path of the task
            channel: Optional channel name

        Returns:
            The key for the checkpoint write
        """
        base_key = self._get_key(thread_id, checkpoint_id, "")
        base_key = base_key.replace("/checkpoint.json", "")
        
        # Construct the path components
        path_components = [base_key, "pending_writes", task_id]
        if task_path:
            path_components.append(task_path)
        if channel:
            path_components.append(channel)
            
        # Join components with forward slashes
        key = "/".join(path_components)
        
        # Ensure no double slashes
        while "//" in key:
            key = key.replace("//", "/")
            
        return key

    def _dump_checkpoint(self, checkpoint: Checkpoint) -> dict[str, Any]:
        """Convert checkpoint to MinIO format."""
        type_, data = self.serde.dumps_typed(checkpoint)
        return {"type": type_, **json.loads(data), "pending_sends": []}

    def _load_checkpoint(self, checkpoint: dict[str, Any], channel_values: dict[str, Any], pending_sends: list[Any]) -> Checkpoint:
        """Load checkpoint from MinIO format."""
        if not checkpoint:
            return {}
        return {
            **checkpoint,
            "pending_sends": [self.serde.loads_typed((c.decode(), b)) for c, b in pending_sends or []],
            "channel_values": channel_values,
        }

    def _dump_blobs(self, thread_id: str, checkpoint_ns: str, values: dict[str, Any], versions: ChannelVersions) -> list[tuple[str, dict[str, Any]]]:
        """Convert blob data for MinIO storage."""
        if not versions:
            return []

        return [
            (
                self._get_checkpoint_blob_key(thread_id, checkpoint_ns, k, cast(str, ver)),
                {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "channel": k,
                    "version": cast(str, ver),
                    "type": self._get_type_and_blob(values[k])[0] if k in values else "empty",
                    "blob": self._get_type_and_blob(values[k])[1] if k in values else None,
                },
            )
            for k, ver in versions.items()
        ]

    def _get_type_and_blob(self, value: Any) -> tuple[str, Optional[bytes]]:
        """Helper to get type and blob from a value."""
        t, b = self.serde.dumps_typed(value)
        return t, b

    def _load_blobs(self, blob_values: dict[str, Any]) -> dict[str, Any]:
        """Load binary data from MinIO."""
        if not blob_values:
            return {}
        return {
            k: self.serde.loads_typed((v["type"], v["blob"]))
            for k, v in blob_values.items()
            if v["type"] != "empty"
        }

    def put(self, config: RunnableConfig, checkpoint: Any, metadata: Optional[Dict[str, Any]] = None, new_versions: Optional[Dict[str, Any]] = None) -> RunnableConfig:
        """Save a checkpoint and flush pending writes."""
        thread_id = config["configurable"]["thread_id"]
        checkpoint_id = checkpoint["id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        # Format metadata for MinIO
        minio_metadata = {
            "thread_id": thread_id,
            "checkpoint_id": checkpoint_id,
            "checkpoint_ns": checkpoint_ns,
            "version": "1.0",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "step": checkpoint.get("step", 0)  # Get step from checkpoint
        }

        # Add parent_checkpoint_id if available
        if "parent_checkpoint_id" in checkpoint:
            minio_metadata["parent_checkpoint_id"] = checkpoint["parent_checkpoint_id"]

        # Get TTL from metadata if present
        if metadata and "ttl" in metadata:
            ttl = metadata["ttl"]
            ttl = ttl * 60 if ttl else None
            minio_metadata["ttl"] = str(ttl) if ttl else "None"
            minio_metadata["last_accessed"] = datetime.now(timezone.utc).isoformat()

        # Update step from metadata if present
        if metadata and "step" in metadata:
            minio_metadata["step"] = metadata["step"]
            checkpoint["step"] = metadata["step"]

        # Ensure checkpoint has required fields
        if "v" not in checkpoint:
            checkpoint["v"] = 1
        if "ts" not in checkpoint:
            checkpoint["ts"] = datetime.now(UTC).isoformat()
        if "channel_values" not in checkpoint:
            checkpoint["channel_values"] = {}
        if "channel_versions" not in checkpoint:
            checkpoint["channel_versions"] = {}
        if "versions_seen" not in checkpoint:
            checkpoint["versions_seen"] = {}
        if "pending_sends" not in checkpoint:
            checkpoint["pending_sends"] = []

        # Store checkpoint
        key = self._get_checkpoint_key(thread_id, checkpoint_ns, checkpoint_id)
        checkpoint_data = {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
            "parent_checkpoint_id": checkpoint.get("parent_checkpoint_id"),
            "checkpoint": checkpoint,
            "metadata": {
                **(metadata or {}),
                "step": minio_metadata["step"]
            }
        }
    
        try:
            self.store.put_object(
                key=key,
                data=json.dumps(checkpoint_data).encode('utf-8'),
                metadata=minio_metadata
            )
            logger.debug(f"Saved checkpoint {checkpoint_id} to {key}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint {checkpoint_id}: {e}")
            raise

        # Store new versions if provided
        if new_versions:
            for channel, version in new_versions.items():
                version_path = self._get_checkpoint_blob_key(thread_id, checkpoint_ns, channel, version)
                version_data = {
                    "type": "json",
                    "value": version
                }
                try:
                    self.store.put_object(
                        key=version_path,
                        data=json.dumps(version_data).encode('utf-8'),
                        metadata=minio_metadata
                    )
                    logger.debug(f"Saved version {version} for channel {channel}")
                except Exception as e:
                    logger.error(f"Failed to save version {version} for channel {channel}: {e}")

        return config

    def get(self, config: RunnableConfig) -> Optional[Checkpoint]:
        """Get a checkpoint.
        
        If checkpoint_id is not provided in the config, returns the latest checkpoint.
        """
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = config["configurable"].get("checkpoint_id")

        # If no checkpoint_id provided, get the latest checkpoint
        if not checkpoint_id:
            latest = self.get_latest(thread_id)
            if latest is None:
                return None
            checkpoint_id = latest["id"]
            config["configurable"]["checkpoint_id"] = checkpoint_id

        key = self._get_checkpoint_key(thread_id, checkpoint_ns, checkpoint_id)
        result = self.store.get_object(key, include_metadata=True)
        if result is None:
            return None

        # Handle different return value structures
        if isinstance(result, tuple):
            data, metadata = result
        else:
            data = result
            metadata = {}

        if data is None:
            return None

        try:
            checkpoint_data = json.loads(data.decode('utf-8'))
            checkpoint = checkpoint_data["checkpoint"]

            # Ensure checkpoint has a step key
            if "step" not in checkpoint:
                checkpoint["step"] = metadata.get("step", 0)

            # Check TTL expiration
            if metadata and 'ttl' in metadata and metadata['ttl'] != 'None':
                try:
                    ttl_seconds = float(metadata['ttl'])
                    last_accessed = datetime.fromisoformat(metadata['last_accessed'])
                    now = datetime.now(timezone.utc)

                    if (now - last_accessed).total_seconds() > ttl_seconds:
                        self.store.delete_object(key)
                        return None

                    # Update last_accessed
                    metadata['last_accessed'] = now.isoformat()
                    self.store.put_object(key, data, metadata=metadata)
                except (ValueError, TypeError) as e:
                    logger.error(f"Error processing TTL: {e}")

            return checkpoint
        except (json.JSONDecodeError, AttributeError) as e:
            logger.error(f"Error decoding checkpoint data: {e}")
            return None

    def get_latest(self, thread_id: str) -> Optional[Checkpoint]:
        """Get the latest checkpoint for a thread.
    
        Args:
            thread_id: The thread ID to get the latest checkpoint for
            
        Returns:
            The latest checkpoint, or None if no checkpoints exist
            
        Raises:
            RuntimeError: If there's an error accessing the MinIO store
            ValueError: If the thread_id is invalid
        """
        if not thread_id:
            raise ValueError("thread_id cannot be empty")
        
        prefix = self._get_key(thread_id, "", "")
        
        try:
            objects = self.store.list_objects(prefix)
        except Exception as e:
            raise RuntimeError(f"Failed to list objects in MinIO store: {str(e)}") from e
        print(objects)
        if not objects:
            return None
        
        latest_key = None
        latest_timestamp = None
        
        for obj in objects:
            if not obj.endswith("checkpoint.json"):
                continue
                
            try:
                data = self.store.get_object(obj)
                if data is None:
                    continue
                    
                checkpoint_data = json.loads(data.decode('utf-8'))
                checkpoint = checkpoint_data["checkpoint"]
                timestamp = datetime.fromisoformat(checkpoint["ts"])
                
                if latest_timestamp is None or timestamp > latest_timestamp:
                    latest_timestamp = timestamp
                    latest_key = obj
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.warning(f"Skipping malformed checkpoint {obj}: {str(e)}")
                continue
        if latest_key is None:
            return None
        try:
            data = self.store.get_object(latest_key)
            if data is None:
                logger.debug(f"Checkpoint disappeared: {latest_key}")
                return None
                
            checkpoint_data = json.loads(data.decode('utf-8'))
            checkpoint = checkpoint_data["checkpoint"]
            checkpoint.setdefault("step", 0)
            return checkpoint
        except Exception as e:
            raise RuntimeError(f"Failed to load checkpoint {latest_key}: {str(e)}") from e
    

    def list_versions(self, thread_id: str, checkpoint_id: str) -> List[str]:
        """List all versions of a checkpoint.
        
        Args:
            thread_id: The ID of the thread
            checkpoint_id: The ID of the checkpoint
            
        Returns:
            List of version strings
        """
        prefix = f"{self.prefix}/{thread_id}/{checkpoint_id}/"
        objects = self.store.list_objects(prefix)
        
        versions = []
        for obj in objects:
            if obj.endswith("checkpoint.json"):
                parts = obj.split("/")
                if len(parts) >= 5:  # prefix/thread_id/checkpoint_id/version/checkpoint.json
                    version = parts[-2]
                    versions.append(version)
        
        return sorted(versions, reverse=True)
    
    def list(self, config: RunnableConfig) -> List[str]:
        """List all checkpoints for a thread.
        
        Args:
            config: The config containing the thread ID
            
        Returns:
            List of checkpoint IDs
        """
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        
        # Use _get_key to construct the prefix, but without the checkpoint_id
        prefix = self._get_key(thread_id, "", checkpoint_ns)
        
        try:
            # List all objects with the prefix
            objects = self.store.list_objects(prefix)
            
            # Extract checkpoint IDs from object names
            checkpoint_ids = set()
            for obj in objects:
                if obj.endswith("checkpoint.json"):
                    # Split path and get the component before checkpoint.json
                    parts = obj.split("/")
                    if len(parts) >= 2:
                        checkpoint_ids.add(parts[-2])
            
            return sorted(list(checkpoint_ids))
        except Exception as e:
            logger.error(f"Error listing checkpoints: {e}")
            return []
    
    def delete_thread(self, thread_id: str) -> None:
        """Delete all checkpoints and writes associated with a specific thread ID.
        
        Args:
            thread_id: The thread ID whose checkpoints should be deleted
        """
        # Construct prefix for the thread
        prefix = f"{self.prefix}/{thread_id}"
        
        try:
            # List all objects under the thread prefix
            objects = self.store.list_objects(prefix)
            
            # Delete each object
            for obj in objects:
                self.store.delete_object(obj)
                
        except Exception as e:
            logger.error(f"Error deleting thread {thread_id}: {e}")
            raise
    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Get a checkpoint tuple from the config.
        
        Args:
            config: The runnable config containing checkpoint info
            
        Returns:
            A CheckpointTuple if found, None otherwise
        """
        # Extract necessary information from config
        thread_id = config.get("configurable", {}).get("thread_id")
        checkpoint_id = config.get("configurable", {}).get("checkpoint_id")
        checkpoint_ns = config.get("configurable", {}).get("checkpoint_ns", "")
        
        if not thread_id:
            logger.debug("Missing required thread_id parameter")
            return None
            
        # Get the checkpoint
        checkpoint = None
        if checkpoint_id:
            # Get specific checkpoint
            key = self._get_key(thread_id, checkpoint_id, checkpoint_ns)
            try:
                data = self.store.get_object(key)
                if not data:
                    return None
                checkpoint_data = json.loads(data.decode('utf-8'))
                checkpoint = checkpoint_data["checkpoint"]
            except Exception as e:
                logger.error(f"Error getting checkpoint: {e}")
                return None
        else:
            # Get latest checkpoint
            checkpoint = self.get_latest(thread_id)
        if checkpoint is None:
            logger.debug("No checkpoint found")
            return None
            
        # Get parent checkpoint info if available
        parent_checkpoint_id = checkpoint.get("parent_checkpoint_id")
        parent_config = None
        if parent_checkpoint_id:
            parent_config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": parent_checkpoint_id,
                }
            }
            
        # Get channel values
        channel_values = self.get_channel_values(
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            checkpoint_id=checkpoint_id
        )
        
        # Get pending sends from parent checkpoint if available
        pending_sends = []
        if parent_checkpoint_id:
            pending_sends = self._load_pending_sends(
                thread_id=thread_id,
                checkpoint_ns=checkpoint_ns,
                parent_checkpoint_id=parent_checkpoint_id
            )
            
        # Process metadata
        raw_metadata = checkpoint.get("metadata", "{}")
        metadata_dict = (
            json.loads(raw_metadata) if isinstance(raw_metadata, str) else raw_metadata
        )
        
        # Sanitize metadata and ensure step key exists
        sanitized_metadata = {
            k.replace("\u0000", ""): (
                v.replace("\u0000", "") if isinstance(v, str) else v
            )
            for k, v in metadata_dict.items()
        }
        # Add step if not present
        if "step" not in sanitized_metadata:
            sanitized_metadata["step"] = 0
            
        metadata = cast(CheckpointMetadata, sanitized_metadata)
        
        # Get pending writes
        pending_writes = self._load_pending_writes(
            thread_id=thread_id,
            checkpoint_id=checkpoint_id
        )
        
        # Create config parameter
        config_param: RunnableConfig = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            }
        }
        
        # Load checkpoint with channel values and pending sends
        checkpoint_param = self._load_checkpoint(
            checkpoint,
            channel_values,
            pending_sends
        )
            
        return CheckpointTuple(
            config=config_param,
            checkpoint=checkpoint_param,
            metadata=metadata,
            parent_config=parent_config,
            pending_writes=pending_writes
        )
    
    def get_version(
        self,
        config: RunnableConfig,
        channel: str,
        version: str
    ) -> Optional[Any]:
        """Get a specific version of a checkpoint.
        
        Args:
            config: The config containing the thread ID
            channel: The channel to get the version for
            version: The version to get
            
        Returns:
            The checkpoint version if found, None otherwise
        """
        if isinstance(config, str):
            try:
                config = json.loads(config)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON string passed to get_version()")
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = config["configurable"].get("checkpoint_id", "")
        
        key = f"{self.prefix}/{thread_id}/{checkpoint_ns}/{checkpoint_id}/channels/{channel}/{version}/blob.json"
        try:
            data = self.store.get(
                namespace=tuple(key.split("/")[:-1]),
                key=key.split("/")[-1]
            )
            if not data:
                return None
            return data.get("value")
        except Exception as e:
            logger.error(f"Error getting version: {e}")
            return None
    
    def get_channel_values(
        self, thread_id: str, checkpoint_ns: str = "", checkpoint_id: str = ""
    ) -> dict[str, Any]:
        """Retrieve channel_values dictionary with properly constructed message objects.
        
        Args:
            thread_id: The thread ID
            checkpoint_ns: The checkpoint namespace
            checkpoint_id: The checkpoint ID
            
        Returns:
            Dictionary of channel values
        """
        # Construct the key for the checkpoint
        key = f"{self.prefix}/{thread_id}/{checkpoint_ns}/{checkpoint_id}/checkpoint.json"
        
        try:
            # Get the checkpoint data
            data = self.store.get_object(key)
            if data is None:
                return {}
                
            checkpoint_data = json.loads(data.decode('utf-8'))
            checkpoint = checkpoint_data["checkpoint"]
            channel_versions = checkpoint.get("channel_versions", {})
            if not channel_versions:
                return {}
                
            channel_values = {}
            for channel, version in channel_versions.items():
                # Construct the key for the channel blob
                blob_key = f"{self.prefix}/{thread_id}/{checkpoint_ns}/{checkpoint_id}/channels/{channel}/{version}/blob.json"
                
                # Get the channel blob
                blob_data = self.store.get_object(blob_key)
                if blob_data is None:
                    continue
                    
                blob = json.loads(blob_data.decode('utf-8'))
                blob_type = blob.get("type")
                blob_value = blob.get("value")
                
                if blob_value and blob_type != "empty":
                    channel_values[channel] = self.serde.loads_typed((blob_type, blob_value))
                    
            return channel_values
            
        except Exception as e:
            logger.error(f"Error getting channel values: {e}")
            return {}
            
    def _load_pending_sends(
        self,
        thread_id: str,
        checkpoint_ns: str = "",
        parent_checkpoint_id: str = ""
    ) -> List[PendingSend]:
        """Load pending sends for a parent checkpoint."""
        prefix = f"{self.prefix}/{thread_id}/{checkpoint_ns}/{parent_checkpoint_id}/pending_sends/"
        
        try:
            objects = self.store.list_objects(prefix)
            if not objects:
                return []
                
            sorted_objects = sorted(objects)
            
            pending_sends: List[PendingSend] = []
            for obj in sorted_objects:
                data = self.store.get_object(obj)
                if data is None:
                    continue
                    
                pending_send = json.loads(data.decode('utf-8'))
                pending_sends.append((pending_send["type"], pending_send["blob"].encode('utf-8')))
                
            return pending_sends
            
        except Exception as e:
            logging.error(f"Error loading pending sends: {e}")
            return []
            
    def _load_pending_writes(
        self,
        thread_id: str,
        checkpoint_id: str,
        task_id: str = "",
        task_path: Optional[str] = None,
    ) -> List[PendingWrite]:
        """Load pending writes for a checkpoint with caching."""
        if not checkpoint_id:
            logger.warning("No checkpoint_id provided, returning empty list")
            return []

        # Construct the prefix for pending writes
        prefix = self._get_checkpoint_write_key(
            thread_id=thread_id,
            checkpoint_id=checkpoint_id,
            task_id=task_id,
            task_path=task_path,
            channel=""  # Empty channel to match all channels
        )
        
        try:
            # List all objects with the prefix
            objects = self.store.list_objects(prefix)            
            if not objects:
                logger.debug(f"No pending writes found for prefix: {prefix}")
                return []
                
            # Sort objects by path to maintain order
            sorted_objects = sorted(objects)
            logger.debug(f"Found {len(sorted_objects)} pending write objects")
            
            pending_writes = []
            for obj in sorted_objects:
                logger.debug(f"Loading writes from {obj}")
                # Get the pending write data (using cached version if available)
                data = self.store.get_object(obj)
                if data is None:
                    continue
                    
                try:
                    # Parse the batched writes
                    batched_writes = json.loads(data.decode('utf-8'))
                    
                    for channel, write_data in batched_writes.items():
                        value_type = write_data.get("type")
                        value = write_data.get("value")
                        
                        # Deserialize based on type
                        if value_type in ("int", "float", "bool", "str"):
                            deserialized_value = eval(f"{value_type}({repr(value)})")
                        elif value_type == "json":
                            deserialized_value = value
                        elif value_type == "pickle":
                            deserialized_value = pickle.loads(bytes.fromhex(value))
                        else:
                            deserialized_value = value  # Keep as string for unknown types
                            
                        pending_writes.append(
                            (
                                task_id,
                                channel,
                                deserialized_value
                            )
                        )
                        logger.debug(f"Loaded write for channel {channel} with type {value_type}")
                except Exception as e:
                    logger.error(f"Failed to deserialize writes from {obj}: {e}")
                    continue
                
            return pending_writes
            
        except Exception as e:
            logger.error(f"Error loading pending writes: {e}")
            return []

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[Tuple[str, Any]],
        task_id: str,
        task_path: Optional[str] = None,
    ) -> None:
        """Save pending writes to MinIO with optimized batching."""
        # Get checkpoint_id from configurable
        checkpoint_id = config["configurable"].get("checkpoint_id")
        if not checkpoint_id:
            logger.warning("No checkpoint_id provided, skipping writes")
            return

        # Get thread_id from configurable
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        
        # Group writes by channel to minimize duplicates
        channel_writes = {}
        for channel, value in writes:
            if channel not in channel_writes:
                channel_writes[channel] = value
            else:
                # If channel already exists, keep the latest value
                channel_writes[channel] = value

        # Prepare metadata once
        metadata = {
            "thread_id": thread_id,
            "checkpoint_id": checkpoint_id,
            "checkpoint_ns": checkpoint_ns,
            "created_at": str(datetime.now().timestamp()),
            "task_id": task_id,
            "task_path": task_path or "",
            "write_count": str(len(channel_writes))
        }

        # Batch all writes into a single object with optimized serialization
        batched_writes = {}
        for channel, value in channel_writes.items():
            # Optimize serialization based on value type
            if isinstance(value, (str, int, float, bool)):
                batched_writes[channel] = {
                    "type": type(value).__name__,
                    "value": value
                }
            elif isinstance(value, (dict, list)):
                batched_writes[channel] = {
                    "type": "json",
                    "value": value
                }
            else:
                # For complex types, use pickle for better serialization
                batched_writes[channel] = {
                    "type": "pickle",
                    "value": pickle.dumps(value).hex()
                }

        # Store all writes in a single object with optimized key
        key = self._get_checkpoint_write_key(
            thread_id=thread_id,
            checkpoint_id=checkpoint_id,
            task_id=task_id,
            task_path=task_path,
            channel="",  # Empty channel to store all writes
        )
        
        try:
            # Use the store's put_object which includes buffering
            self.store.put_object(
                key=key,
                data=json.dumps(batched_writes).encode(),
                metadata=metadata
            )
            logger.debug(f"Buffered {len(channel_writes)} writes to {key}")
        except Exception as e:
            logger.error(f"Failed to buffer writes to {key}: {e}")

    def get_next_version(
        self,
        current: Optional[Union[int, float, str]],
        channel: Any  # Using Any since ChannelProtocol is not available
    ) -> Union[int, float, str]:
        """Generate the next version ID for a channel.
        
        Args:
            current: The current version identifier (int, float, or str)
            channel: The channel being versioned
            
        Returns:
            The next version identifier, which must be increasing
        """
        if current is None:
            # Start with version 1 if no current version
            return 1
            
        if isinstance(current, int):
            # For integers, increment by 1
            return current + 1
            
        if isinstance(current, float):
            # For floats, increment by 1.0
            return current + 1.0
            
        if isinstance(current, str):
            try:
                # Try to parse as number first
                if "." in current:
                    return float(current) + 1.0
                else:
                    return int(current) + 1
            except ValueError:
                # If not a number, append a counter
                if "_" in current:
                    base, counter = current.rsplit("_", 1)
                    try:
                        counter = int(counter)
                        return f"{base}_{counter + 1}"
                    except ValueError:
                        pass
                return f"{current}_1"
                

    def _clear_pending_writes(self, thread_id: str, checkpoint_ns: str, checkpoint_id: str) -> None:
        """Clear all pending writes for a checkpoint.
        
        Args:
            thread_id: The thread ID
            checkpoint_ns: The checkpoint namespace
            checkpoint_id: The checkpoint ID
        """
        try:
            prefix = self._get_checkpoint_write_key(thread_id, checkpoint_ns, checkpoint_id, "")
            objects = self.store.list_objects(prefix)
            for obj in objects:
                self.store.delete_object(obj)
        except Exception as e:
            logger.error(f"Error clearing pending writes: {e}")

    def flush_pending_writes(
        self,
        config: RunnableConfig,
        checkpoint_id: str,
        force: bool = False,
    ) -> None:
        """Flush pending writes to Minio with optimized batching."""
        # First, flush the write buffer to ensure all writes are persisted
        if hasattr(self.store, '_flush_buffer'):
            self.store._flush_buffer()
        
        # Then proceed with the normal flush operation
        super().flush_pending_writes(config, checkpoint_id, force)
