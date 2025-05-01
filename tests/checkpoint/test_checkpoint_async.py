import pytest
import pytest_asyncio
import asyncio
from datetime import datetime, timezone, UTC
from minio import Minio
from langgraph_minio.checkpoint.base import Checkpoint, CheckpointMetadata
from langgraph_minio.store.aio import AsyncMinioStore
from langgraph_minio.checkpoint.aio import AsyncMinioSaver
import logging
from typing import Dict, Any, Optional, List, Tuple

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@pytest.fixture
def minio_client():
    """Create a MinIO client fixture."""
    client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    
    # Ensure test bucket exists
    bucket_name = "test-bucket"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    
    yield client
    
    # Cleanup: Remove all objects in the bucket
    # objects = client.list_objects(bucket_name, recursive=True)
    # for obj in objects:
    #     client.remove_object(bucket_name, obj.object_name)

@pytest_asyncio.fixture
async def async_store():
    """Create an AsyncMinioStore fixture."""
    return AsyncMinioStore(
        endpoint_url="http://localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket_name="test-bucket"
    )

@pytest_asyncio.fixture
async def async_checkpointer(async_store):
    """Create an AsyncMinioSaver fixture."""
    return AsyncMinioSaver(async_store)

@pytest.fixture
def sample_data() -> Dict[str, Any]:
    """Create sample data fixture."""
    return {
        "test_key": "test_value",
        "nested": {
            "key": "value"
        },
        "list": [1, 2, 3]
    }

@pytest.fixture
def sample_checkpoint(sample_data) -> Checkpoint:
    """Create a sample checkpoint fixture."""
    return Checkpoint(
        v=1,
        id="test-checkpoint",
        ts=datetime.now(UTC).isoformat(),
        channel_values=sample_data,
        channel_versions={},
        versions_seen={},
        pending_sends=[]
    )

@pytest.fixture
def sample_metadata() -> CheckpointMetadata:
    """Create a sample metadata fixture."""
    return {
        "checkpoint_id": "test-checkpoint",
        "timestamp": datetime.now(UTC).isoformat(),
        "version": "v1",
        "description": "Test metadata",
        "tags": ["test", "checkpoint"],
        "metadata": {"key": "value"}
    }

@pytest.mark.asyncio
async def test_async_get_latest(async_checkpointer, sample_checkpoint):
    """Test getting the latest checkpoint asynchronously."""
    thread_id = "test-thread"
    
    # Put a checkpoint first
    config = {"configurable": {"thread_id": thread_id}}
    await async_checkpointer.aput(config, sample_checkpoint, {})
    
    # Get the latest checkpoint
    latest = await async_checkpointer.aget_latest(thread_id)
    assert latest is not None
    assert latest["id"] == sample_checkpoint["id"]
    assert latest["channel_values"] == sample_checkpoint["channel_values"]

@pytest.mark.asyncio
async def test_async_concurrent_operations(async_checkpointer):
    """Test concurrent operations."""
    thread_id = "test-thread"
    checkpoints = []
    
    # Create multiple checkpoints
    for i in range(5):
        checkpoint = Checkpoint.from_prosci(
            checkpoint_id=f"concurrent-checkpoint-{i}",
            data={"test": f"data-{i+1}"},
            description=f"Concurrent test {i}"
        )
        checkpoints.append(checkpoint)
    
    # Put checkpoints concurrently
    config = {"configurable": {"thread_id": thread_id}}
    tasks = []
    for checkpoint in checkpoints:
        tasks.append(async_checkpointer.aput(config, checkpoint, {}))
    await asyncio.gather(*tasks)
    
    # Verify all checkpoints were stored
    for checkpoint in checkpoints:
        config["configurable"]["checkpoint_id"] = checkpoint["id"]
        retrieved = await async_checkpointer.aget(config)
        assert retrieved is not None
        assert retrieved["channel_values"] == checkpoint["channel_values"]
        assert retrieved["id"] == checkpoint["id"]

@pytest.mark.asyncio
async def test_async_error_handling_get_version(async_checkpointer):
    """Test error handling for async get_version."""
    config = {"configurable": {"thread_id": "test-thread"}}
    
    # Test invalid channel
    result = await async_checkpointer.aget_version(config, "invalid-channel", "v1")
    assert result is None

    # Test invalid version
    result = await async_checkpointer.aget_version(config, "test-channel", "invalid-version")
    assert result is None 