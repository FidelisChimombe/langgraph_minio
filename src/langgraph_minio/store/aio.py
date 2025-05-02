from typing import Dict, Any, Optional, List, Tuple, Union, Iterable
import logging
import asyncio
from functools import partial
import aiohttp
from langgraph.store.base import (
    SearchItem,
    Op,
    Result
)
from .base import MinioStore
from minio.error import S3Error
from minio import Minio
import json
from datetime import datetime, timezone
import io
import threading
import time
from collections import defaultdict
import pickle

logger = logging.getLogger(__name__)

class AsyncMinioStore(MinioStore):
    """Async wrapper around MinioStore."""

    def __init__(self, endpoint_url=None, access_key=None, secret_key=None, bucket_name=None, client=None):
        """Initialize the async store."""
        if not bucket_name:
            raise ValueError("bucket_name is required")
            
        try:
            if client:
                super().__init__(bucket_name=bucket_name, client=client)
            else:
                if not endpoint_url:
                    raise ValueError("endpoint_url is required")
                if not access_key:
                    raise ValueError("access_key is required") 
                if not secret_key:
                    raise ValueError("secret_key is required")
                    
                super().__init__(endpoint_url=endpoint_url, access_key=access_key, 
                               secret_key=secret_key, bucket_name=bucket_name)
                
            # Test connection by checking bucket exists
            self.client.bucket_exists(bucket_name)
            
        except Exception as e:
            raise RuntimeError(f"Failed to initialize MinIO client: {str(e)}")
            
        self.loop = asyncio.get_event_loop()
            
    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_bucket_exists()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        pass

    async def _ensure_bucket_exists(self):
        """Ensure the bucket exists."""
        try:
            exists = await self.loop.run_in_executor(
                None,
                self.client.bucket_exists,
                self.bucket_name
            )
            if not exists:
                await self.loop.run_in_executor(
                    None,
                    self.client.make_bucket,
                    self.bucket_name
                )
        except Exception as e:
            raise RuntimeError(f"Failed to ensure bucket exists: {str(e)}")

    async def alist_objects(self, prefix: str = "") -> List[str]:
        """Async wrapper around list_objects."""
        await self._ensure_bucket_exists()
        return await self.loop.run_in_executor(None, super().list_objects, prefix)

    async def aget_object(self, key: str, include_metadata: bool = False, refresh_ttl: bool = False) -> Union[Optional[bytes], Optional[Tuple[bytes, Dict[str, str]]]]:
        """Async wrapper around get_object."""
        await self._ensure_bucket_exists()
        return await self.loop.run_in_executor(None, super().get_object, key, include_metadata, refresh_ttl)

    async def aput_object(self, key: str, data: bytes, metadata: Optional[Dict[str, str]] = None) -> None:
        """Async wrapper around put_object."""
        await self._ensure_bucket_exists()
        await self.loop.run_in_executor(None, super().put_object, key, data, metadata)

    async def adelete_object(self, key: str) -> None:
        """Async wrapper around delete_object."""
        await self._ensure_bucket_exists()
        await self.loop.run_in_executor(None, super().delete_object, key)

    async def aget(self, namespace, key, refresh_ttl=False):
        """Get an object asynchronously."""
        await self._ensure_bucket_exists()
        return await self.loop.run_in_executor(
            None,
            lambda: self.get(namespace, key, refresh_ttl=refresh_ttl)
        )

    async def aput(self, namespace, key, data, ttl=None):
        print("***********putting --xxx", namespace, key, data, ttl)
        """Put an object asynchronously."""
        await self._ensure_bucket_exists()
        await self.loop.run_in_executor(
            None,
            lambda: self.put(namespace, key, data, ttl=ttl)
        )

    async def adelete(self, namespace: Tuple[str, ...], key: str) -> None:
        """Async wrapper around delete."""
        await self._ensure_bucket_exists()
        await self.loop.run_in_executor(None, super().delete, namespace, key)

    async def asearch(self, namespace_prefix, query=None, filter=None, limit=None, offset=None):
        """Search for objects asynchronously."""
        await self._ensure_bucket_exists()
        return await self.loop.run_in_executor(
            None,
            lambda: self.search(namespace_prefix, query=query, filter=filter, limit=limit, offset=offset)
        )

    async def alist_namespaces(self, prefix=None, max_depth=None, limit=100, offset=0):
        """List namespaces asynchronously."""
        await self._ensure_bucket_exists()
        return await self.loop.run_in_executor(
            None,
            lambda: self.list_namespaces(prefix=prefix, max_depth=max_depth, limit=limit, offset=offset)
        )

    async def abatch(self, ops: Iterable[Op]) -> List[Result]:
        """Async wrapper around batch."""
        await self._ensure_bucket_exists()
        return await self.loop.run_in_executor(None, super().batch, ops)
