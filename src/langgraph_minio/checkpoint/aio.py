from typing import Dict, Any, List, Optional, Sequence, Tuple, Iterator
import asyncio
from langgraph_minio.checkpoint.base import Checkpoint
from langgraph_minio.store.aio import AsyncMinioStore
from langgraph_minio.checkpoint.base import MinioSaver
from langgraph.checkpoint.base import (
    RunnableConfig,
    CheckpointTuple,
)
import logging
logger = logging.getLogger(__name__)


class AsyncMinioSaver(MinioSaver[AsyncMinioStore]):
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
        """Async wrapper around put()"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.put,
            config,
            checkpoint,
            metadata,
            new_versions
        )
    
    async def aget_latest(self, thread_id: str) -> Optional[Checkpoint]:
        """Async wrapper around get_latest()"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.get_latest,
            thread_id
        )

    async def aget(self, config: RunnableConfig) -> Optional[Checkpoint]:
        """Async wrapper around get()"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.get,
            config
        )

    async def aget_version(
        self,
        config: RunnableConfig,
        channel: str,
        version: str
    ) -> Optional[Any]:
        """Async wrapper around get_version()"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.get_version,
            config,
            channel,
            version
        )

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