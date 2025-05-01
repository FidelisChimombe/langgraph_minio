import pickle
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List, Tuple, Union, Iterable
import logging
import asyncio
from functools import partial
import aiohttp
from langgraph.store.base import (
    SearchItem,
    GetOp,
    PutOp,
    SearchOp,
    ListNamespacesOp,
    Op,
    Result
)
from .base import MinioStore
from minio.error import S3Error
from minio import Minio

logger = logging.getLogger(__name__)

class AsyncMinioStore(MinioStore):
    """Async wrapper around MinioStore."""

    def __init__(self, *args, **kwargs):
        """Initialize the async store without calling ensure_bucket_exists."""
        # Skip _ensure_bucket_exists in parent class
        self.client = None
        self.bucket_name = None
        
        # Get the endpoint_url, access_key, secret_key from args or kwargs
        if args:
            endpoint_url = args[0]
            access_key = args[1] if len(args) > 1 else kwargs.get('access_key')
            secret_key = args[2] if len(args) > 2 else kwargs.get('secret_key')
            bucket_name = args[3] if len(args) > 3 else kwargs.get('bucket_name')
            client = args[4] if len(args) > 4 else kwargs.get('client')
        else:
            endpoint_url = kwargs.get('endpoint_url')
            access_key = kwargs.get('access_key')
            secret_key = kwargs.get('secret_key')
            bucket_name = kwargs.get('bucket_name')
            client = kwargs.get('client')

        if not client and not (endpoint_url and access_key and secret_key):
            raise ValueError("Either client or endpoint_url, access_key, and secret_key must be provided")
        
        self.client = client or Minio(
            endpoint_url.replace("http://", "").replace("https://", ""),
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        self.bucket_name = bucket_name or "default"

    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_bucket_exists()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        pass

    async def _ensure_bucket_exists(self) -> None:
        """Ensure the bucket exists."""
        try:
            loop = asyncio.get_event_loop()
            exists = await loop.run_in_executor(None, self.client.bucket_exists, self.bucket_name)
            if not exists:
                await loop.run_in_executor(None, self.client.make_bucket, self.bucket_name)
                logger.debug(f"Created bucket {self.bucket_name}")
            else:
                logger.debug(f"Bucket {self.bucket_name} already exists")
        except Exception as e:
            logger.error(f"Failed to ensure bucket exists: {e}")
            raise RuntimeError(f"Failed to ensure bucket exists: {e}")

    async def alist_objects(self, prefix: str = "") -> List[str]:
        """Async version of list_objects."""
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self.list_objects, prefix)
        except Exception as e:
            logger.error(f"Failed to list objects: {e}")
            raise RuntimeError(f"Failed to list objects: {e}")

    async def aget_object(self, key: str, include_metadata: bool = False, refresh_ttl: bool = False) -> Optional[Union[bytes, Tuple[bytes, Dict[str, str]]]]:
        """Async version of get_object."""
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self.get_object, key, include_metadata, refresh_ttl)
        except Exception as e:
            logger.error(f"Failed to get object: {e}")
            raise

    async def aput_object(self, key: str, data: bytes, metadata: Optional[Dict[str, str]] = None) -> None:
        """Async version of put_object."""
        try:
            loop = asyncio.get_event_loop()
            func = partial(self.put_object, key, data, metadata=metadata)
            await loop.run_in_executor(None, func)
        except Exception as e:
            logger.error(f"Failed to put object: {e}")
            raise

    async def adelete_object(self, key: str) -> None:
        """Async version of delete_object."""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.delete_object, key)
        except Exception as e:
            logger.error(f"Failed to delete object: {e}")
            raise

    async def aget(
        self,
        namespace: Tuple[str, ...],
        key: str,
        *,
        refresh_ttl: Optional[bool] = None
    ) -> Optional[Dict[str, Any]]:
        """Async version of get."""
        try:
            loop = asyncio.get_event_loop()
            func = partial(self.get, namespace, key, refresh_ttl=refresh_ttl)
            return await loop.run_in_executor(None, func)
        except Exception as e:
            logger.error(f"Failed to get item: {e}")
            raise

    async def aput(
        self,
        namespace: Tuple[str, ...],
        key: str,
        data: Dict[str, Any],
        *,
        ttl: Optional[float] = None
    ) -> None:
        """Async version of put."""
        try:
            loop = asyncio.get_event_loop()
            func = partial(self.put, namespace, key, data, ttl=ttl)
            await loop.run_in_executor(None, func)
        except Exception as e:
            logger.error(f"Failed to put item: {e}")
            raise

    async def adelete(self, namespace: Tuple[str, ...], key: str) -> None:
        """Async version of delete."""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.delete, namespace, key)
        except Exception as e:
            logger.error(f"Failed to delete item: {e}")
            raise

    async def asearch(
        self,
        namespace_prefix: Tuple[str, ...],
        *,
        query: Optional[str] = None,
        filter: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        refresh_ttl: Optional[bool] = None
    ) -> List[SearchItem]:
        """Async version of search."""
        try:
            loop = asyncio.get_event_loop()
            func = partial(
                self.search,
                namespace_prefix,
                query=query,
                filter=filter,
                limit=limit,
                offset=offset,
                refresh_ttl=refresh_ttl
            )
            return await loop.run_in_executor(None, func)
        except Exception as e:
            logger.error(f"Failed to search: {e}")
            raise

    async def alist_namespaces(
        self,
        prefix: Tuple[str, ...] = (),
        *,
        max_depth: Optional[int] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[str]:
        """Async version of list_namespaces."""
        try:
            loop = asyncio.get_event_loop()
            func = partial(
                self.list_namespaces,
                max_depth=max_depth,
                limit=limit,
                offset=offset
            )
            if prefix:
                func = partial(func, prefix=prefix)
            return await loop.run_in_executor(None, func)
        except Exception as e:
            logger.error(f"Failed to list namespaces: {e}")
            raise

    async def abatch(self, ops: Iterable[Op]) -> List[Result]:
        """Async version of batch."""
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self.batch, ops)
        except Exception as e:
            logger.error(f"Failed to execute batch operations: {e}")
            raise
