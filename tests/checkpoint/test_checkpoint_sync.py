import pytest
from datetime import datetime, timezone, UTC
from minio import Minio
from langgraph_minio.checkpoint.base import MinioSaver, Checkpoint, CheckpointMetadata
from langgraph_minio.store.base import MinioStore
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

@pytest.fixture
def store(minio_client):
    """Create a MinioStore fixture."""
    return MinioStore(
        endpoint_url="http://localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket_name="test-bucket"
    )

@pytest.fixture
def checkpointer(store):
    """Create a MinioSaver fixture."""
    return MinioSaver(store)

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

# Basic Operations Tests
def test_put_get_checkpoint(checkpointer, sample_checkpoint, sample_metadata):
    """Test putting and getting a checkpoint."""
    # Put checkpoint
    config = {"configurable": {"thread_id": "test-thread"}}
    result = checkpointer.put(config, sample_checkpoint, sample_metadata)
    assert result is not None
    
    # Get checkpoint
    retrieved = checkpointer.get(config)
    assert retrieved is not None
    assert retrieved["id"] == sample_checkpoint["id"]
    assert retrieved["channel_values"] == sample_checkpoint["channel_values"]

def test_list_checkpoints(checkpointer, sample_metadata):
    """Test listing checkpoints."""
    # Put multiple checkpoints
    config = {"configurable": {"thread_id": "test-thread"}}
    for i in range(3):
        checkpoint = Checkpoint(
            v=1,
            id=f"test-checkpoint-{i}",
            ts=datetime.now(UTC).isoformat(),
            channel_values={"test": f"data-{i}"},
            channel_versions={},
            versions_seen={},
            pending_sends=[]
        )
        checkpointer.put(config, checkpoint, sample_metadata)
    
    # List checkpoints
    checkpoints = checkpointer.list(config)
    assert len(checkpoints) >= 3
    assert all(f"test-checkpoint-{i}" in checkpoints for i in range(3))

def test_delete_checkpoint(checkpointer, sample_checkpoint, sample_metadata):
    """Test deleting a checkpoint."""
    # Put checkpoint
    config = {"configurable": {"thread_id": "test-thread"}}
    checkpointer.put(config, sample_checkpoint, sample_metadata)
    
    # Delete checkpoint
    checkpointer.delete_thread("test-thread")
    
    # Verify deletion
    retrieved = checkpointer.get(config)
    assert retrieved is None

# Version Management Tests
def test_version_management(checkpointer, sample_checkpoint, sample_metadata):
    """Test checkpoint version management."""
    config = {"configurable": {"thread_id": "test-thread"}}
    
    # Put initial version
    checkpointer.put(config, sample_checkpoint, sample_metadata)
    
    # Update checkpoint with new version
    updated_checkpoint = Checkpoint(
        v=1,
        id=sample_checkpoint["id"],
        ts=datetime.now(UTC).isoformat(),
        channel_values={"test": "updated-data"},
        channel_versions={},
        versions_seen={},
        pending_sends=[]
    )
    checkpointer.put(config, updated_checkpoint, sample_metadata)
    
    # Get latest version
    latest = checkpointer.get(config)
    assert latest is not None
    assert latest["channel_values"]["test"] == "updated-data"

# Edge Cases Tests
def test_nonexistent_checkpoint(checkpointer):
    """Test handling of nonexistent checkpoints."""
    config = {"configurable": {"thread_id": "nonexistent-thread"}}
    result = checkpointer.get(config)
    assert result is None

def test_invalid_inputs(checkpointer):
    """Test handling of invalid inputs."""
    # Test with empty config
    with pytest.raises(KeyError):
        checkpointer.get({})
    
    # Test with invalid thread_id
    with pytest.raises(KeyError):
        checkpointer.get({"configurable": {}})

def test_concurrent_operations(checkpointer, sample_metadata):
    """Test concurrent operations."""
    config = {"configurable": {"thread_id": "test-thread"}}
    
    # Create multiple checkpoints concurrently
    checkpoints = []
    for i in range(5):
        checkpoint = Checkpoint(
            v=1,
            id=f"concurrent-checkpoint-{i}",
            ts=datetime.now(UTC).isoformat(),
            channel_values={"test": f"data-{i}"},
            channel_versions={},
            versions_seen={},
            pending_sends=[]
        )
        checkpoints.append(checkpoint)
    
    # Put checkpoints
    for checkpoint in checkpoints:
        checkpointer.put(config, checkpoint, sample_metadata)
    
    # Verify all checkpoints were stored
    stored_checkpoints = checkpointer.list(config)
    assert len(stored_checkpoints) >= 5
    assert all(f"concurrent-checkpoint-{i}" in stored_checkpoints for i in range(5))

# Large Data Tests
def test_large_checkpoint(checkpointer, sample_metadata):
    """Test handling of large checkpoints."""
    # Create large data
    large_data = "x" * (5 * 1024 * 1024)  # 5MB of data
    checkpoint = Checkpoint(
        v=1,
        id="large-checkpoint",
        ts=datetime.now(UTC).isoformat(),
        channel_values={"large": large_data},
        channel_versions={},
        versions_seen={},
        pending_sends=[]
    )
    
    # Put and get large checkpoint
    config = {"configurable": {"thread_id": "test-thread"}}
    checkpointer.put(config, checkpoint, sample_metadata)
    retrieved = checkpointer.get(config)
    
    assert retrieved is not None
    assert len(retrieved["channel_values"]["large"]) == len(large_data)
    assert retrieved["channel_values"]["large"] == large_data

# Metadata and TTL Tests
def test_metadata_handling(checkpointer, sample_checkpoint, sample_metadata):
    """Test metadata handling."""
    config = {"configurable": {"thread_id": "test-thread"}}
    
    # Put checkpoint with metadata
    checkpointer.put(config, sample_checkpoint, sample_metadata)
    
    # Get checkpoint and verify metadata
    retrieved = checkpointer.get(config)
    assert retrieved is not None
    assert retrieved["id"] == sample_checkpoint["id"]
    assert retrieved["channel_values"] == sample_checkpoint["channel_values"]

def test_ttl_expiration(checkpointer, sample_checkpoint, sample_metadata):
    """Test TTL expiration."""
    config = {"configurable": {"thread_id": "test-thread"}}
    
    # Put checkpoint with TTL of 1/60 minutes (1 second)
    metadata = {**sample_metadata, "ttl": 1/60}  # 1 second TTL in minutes
    logger.debug("Putting checkpoint with TTL=1/60 minutes (1 second)")
    checkpointer.put(config, sample_checkpoint, metadata)
    
    # Get checkpoint immediately and verify TTL is set
    logger.debug("Getting checkpoint immediately")
    retrieved = checkpointer.get(config)
    assert retrieved is not None
    logger.debug(f"Retrieved checkpoint: {retrieved}")
    
    # Get the object directly from MinIO to verify TTL metadata
    key = checkpointer._get_checkpoint_key("test-thread", "", sample_checkpoint["id"])
    result = checkpointer.store.get_object(key, include_metadata=True)
    assert result is not None
    data, metadata = result
    assert metadata is not None
    assert "ttl" in metadata
    assert metadata["ttl"] == "1.0"  # 1 second in seconds
    logger.debug(f"TTL metadata: {metadata}")
    
    # Wait for TTL to expire
    logger.debug("Waiting for TTL to expire")
    import time
    time.sleep(1.1)  # Wait for 1.1 seconds to ensure TTL expires
    
    # Try to get expired checkpoint
    logger.debug("Getting expired checkpoint")
    expired = checkpointer.get(config)
    logger.debug(f"Expired checkpoint: {expired}")
    assert expired is None
    
    # Verify the object was deleted from MinIO
    result = checkpointer.store.get_object(key, include_metadata=True)
    assert result is None
    logger.debug("Verified object was deleted from MinIO")

# Error Handling Tests
def test_error_handling(checkpointer):
    """Test error handling."""
    # Test with invalid store
    with pytest.raises(ValueError):
        MinioSaver(None)
    
    # Test with invalid bucket
    with pytest.raises(RuntimeError):
        MinioStore(
            endpoint_url="http://invalid-endpoint:9000",
            access_key="invalid",
            secret_key="invalid",
            bucket_name="nonexistent-bucket"
        )

# Batch Operations Tests
def test_batch_operations(checkpointer, sample_metadata):
    """Test batch operations."""
    config = {"configurable": {"thread_id": "test-thread"}}
    
    # Create multiple checkpoints
    checkpoints = []
    for i in range(3):
        checkpoint = Checkpoint(
            v=1,
            id=f"batch-checkpoint-{i}",
            ts=datetime.now(UTC).isoformat(),
            channel_values={"test": f"data-{i}"},
            channel_versions={},
            versions_seen={},
            pending_sends=[]
        )
        checkpoints.append(checkpoint)
    
    # Put checkpoints in batch
    for checkpoint in checkpoints:
        checkpointer.put(config, checkpoint, sample_metadata)
    
    # List checkpoints
    stored_checkpoints = checkpointer.list(config)
    assert len(stored_checkpoints) >= 3
    assert all(f"batch-checkpoint-{i}" in stored_checkpoints for i in range(3))

# Latest Version Tests
def test_get_latest(checkpointer, sample_checkpoint):
    """Test getting the latest checkpoint version."""
    thread_id = "test-thread"
    
    # First put a checkpoint
    config = {"configurable": {"thread_id": thread_id}}
    checkpointer.put(config, sample_checkpoint)
    
    # Now get the latest version
    latest = checkpointer.get_latest(thread_id)
    assert latest is not None
    assert latest["id"] == sample_checkpoint["id"]
    assert latest["channel_values"] == sample_checkpoint["channel_values"]

# Error Handling Tests
def test_error_handling_get_version(checkpointer):
    """Test error handling for get_version."""
    config = {"configurable": {"thread_id": "test-thread"}}
    
    # Test invalid channel
    result = checkpointer.get_version(config, "", "v1")
    assert result is None

    # Test invalid version
    result = checkpointer.get_version(config, "test-channel", "")
    assert result is None 