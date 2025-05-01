from typing import Dict, Any, List, Optional, Sequence, Tuple, Union, Iterator, cast
import asyncio
from langgraph_minio.checkpoint.base import Checkpoint
from langgraph_minio.store.aio import AsyncMinioStore
from langgraph_minio.checkpoint.base import BaseMinioSaver
from langgraph.checkpoint.base import (
    RunnableConfig,
    CheckpointMetadata,
    CheckpointTuple,
)
import logging
import pickle
import json
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class AsyncMinioSaver(BaseMinioSaver[AsyncMinioStore]):
    """An async checkpoint saver that uses MinIO as the backend storage."""
    
    def __init__(self, store: AsyncMinioStore):
        """Initialize the checkpoint saver.
        
        Args:
            store: The async MinIO store to use
        """
        super().__init__(store)

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: Optional[Dict[str, Any]] = None,
        new_versions: Optional[Dict[str, Any]] = None,
    ) -> RunnableConfig:
        """Async version of put()"""
        thread_id = config["configurable"]["thread_id"]
        checkpoint_id = checkpoint["id"]  # Get ID from checkpoint
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        
        # Update config with checkpoint_id
        config["configurable"]["checkpoint_id"] = checkpoint_id
        
        # Get sanitized key
        key = self._get_key(thread_id, checkpoint_id, checkpoint_ns)
        
        # Convert checkpoint to bytes
        checkpoint_bytes = pickle.dumps(checkpoint)
        
        # Format metadata for MinIO
        minio_metadata = {
            "thread_id": thread_id,
            "checkpoint_id": checkpoint_id,
            "version": "1.0",
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        if checkpoint_ns:
            minio_metadata["checkpoint_ns"] = checkpoint_ns
            
        # Get TTL from metadata if present
        if metadata and "ttl" in metadata:
            ttl = metadata["ttl"]
            # Convert TTL from minutes to seconds
            ttl = ttl * 60 if ttl else None
            # Add TTL to metadata
            minio_metadata["ttl"] = str(ttl) if ttl else "None"
            minio_metadata["last_accessed"] = datetime.now(timezone.utc).isoformat()
            
        # Store checkpoint with metadata
        await self.store.aput_object(
            key=key,
            data=checkpoint_bytes,
            metadata=minio_metadata
        )
        
        # Store new versions if provided
        if new_versions:
            for channel, version in new_versions.items():
                version_path = f"{key}/channels/{channel}/{version}/blob.json"
                version_data = {
                    "type": "json",
                    "value": version
                }
                await self.store.aput_object(
                    key=version_path,
                    data=json.dumps(version_data).encode('utf-8'),
                    metadata=minio_metadata
                )
        
        return config
    
    async def aget_latest(self, thread_id: str) -> Optional[Checkpoint]:
        """Async version of get_latest()"""
        try:
            # Use _get_key to construct the prefix, but without the checkpoint_id
            prefix = self._get_key(thread_id, "", "")
            
            # List all checkpoints for this thread
            objects = await self.store.alist_objects(prefix)
            if not objects:
                return None
            
            # Find the latest version
            latest_key = None
            latest_timestamp = None
            for obj in objects:
                if obj.endswith("checkpoint.json"):
                    try:
                        # Get the checkpoint data
                        data = await self.store.aget_object(obj)
                        if data is None:
                            continue
                            
                        # Parse the checkpoint
                        checkpoint = pickle.loads(data)
                        if not isinstance(checkpoint, dict):
                            checkpoint = checkpoint.to_dict()
                        timestamp = datetime.fromisoformat(checkpoint["ts"])
                        
                        # Update latest if this is newer
                        if latest_timestamp is None or timestamp > latest_timestamp:
                            latest_timestamp = timestamp
                            latest_key = obj
                    except (pickle.PickleError, KeyError, ValueError) as e:
                        logger.error(f"Error parsing checkpoint {obj}: {e}")
                        continue
            
            if latest_key is None:
                return None
            
            # Load the latest checkpoint
            data = await self.store.aget_object(latest_key)
            if data is None:
                logger.debug(f"Failed to load checkpoint from key={latest_key}")
                return None
            
            checkpoint = pickle.loads(data)
            if not isinstance(checkpoint, dict):
                checkpoint = checkpoint.to_dict()
            return checkpoint
        except Exception as e:
            logger.error(f"Error getting latest checkpoint: {e}")
            return None

    async def aget(self, config: RunnableConfig) -> Optional[Checkpoint]:
        """Async version of get()"""
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = config["configurable"].get("checkpoint_id", "")
        
        # Get the key for this checkpoint
        key = self._get_key(thread_id, checkpoint_id, checkpoint_ns)
        
        try:
            # Get the checkpoint data with metadata
            result = await self.store.aget_object(key, include_metadata=True)
            if result is None:
                return None
                
            # Unpack data and metadata
            data, metadata = result
                
            # Parse the checkpoint data
            checkpoint = pickle.loads(data)
            
            # Check TTL expiration
            if metadata and 'ttl' in metadata and metadata['ttl'] != 'None':
                try:
                    ttl_seconds = float(metadata['ttl'])
                    last_accessed = datetime.fromisoformat(metadata['last_accessed'])
                    now = datetime.now(timezone.utc)
                    
                    # If TTL has expired, delete the checkpoint and return None
                    if (now - last_accessed).total_seconds() > ttl_seconds:
                        logger.debug(f"Checkpoint {key} has expired (TTL: {ttl_seconds} seconds, Last accessed: {last_accessed})")
                        await self.store.adelete_object(key)
                        return None
                        
                    # Update last_accessed
                    metadata['last_accessed'] = now.isoformat()
                    await self.store.aput_object(key, data, metadata=metadata)
                    
                except (ValueError, TypeError) as e:
                    logger.error(f"Error processing TTL for checkpoint {key}: {e}")
            
            return checkpoint
            
        except Exception as e:
            logger.error(f"Error getting checkpoint: {e}")
            return None

    async def aget_version(
        self,
        config: RunnableConfig,
        channel: str,
        version: str
    ) -> Optional[Any]:
        """Async version of get_version()"""
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = config["configurable"].get("checkpoint_id", "")
        
        key = f"{self.prefix}/{thread_id}/{checkpoint_ns}/{checkpoint_id}/channels/{channel}/{version}/blob.json"
        try:
            data = await self.store.aget_object(key)
            if not data:
                return None
            return json.loads(data.decode()).get("value")
        except Exception as e:
            logger.error(f"Error getting version: {e}")
            return None

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[Tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Async wrapper around put_writes()"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            self.put_writes,
            config,
            writes,
            task_id,
            task_path
        )

    async def aget_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        """Async wrapper around get_tuple()"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.get_tuple,
            config
        )

    async def alist_versions(self, thread_id: str, checkpoint_id: str) -> List[str]:
        """Async wrapper around list_versions()"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.list_versions,
            thread_id,
            checkpoint_id
        )

    async def alist(
        self,
        config: Optional[RunnableConfig] = None,
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None
    ) -> Iterator[CheckpointTuple]:
        """Async wrapper around list()"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.list,
            config,
            filter,
            before,
            limit
        )

    async def adelete_thread(self, thread_id: str) -> None:
        """Async wrapper around delete_thread()"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            self.delete_thread,
            thread_id
        )

    async def aget_channel_values(
        self, thread_id: str, checkpoint_ns: str = "", checkpoint_id: str = ""
    ) -> dict[str, Any]:
        """Async wrapper around get_channel_values()"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.get_channel_values,
            thread_id,
            checkpoint_ns,
            checkpoint_id
        )