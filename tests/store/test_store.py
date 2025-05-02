import pytest
from datetime import datetime, timezone
from minio import Minio
from langgraph_minio.store.base import MinioStore
from langgraph.store.base import GetOp, PutOp, SearchOp, ListNamespacesOp, NotProvided, NOT_PROVIDED
import json
import logging
import time
from typing import Dict, Any, Optional, List, Tuple
from minio.error import S3Error
import requests.exceptions
from urllib3.exceptions import HTTPError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
TEST_BUCKET = "test-bucket"

@pytest.fixture(scope="session")
def minio_client():
    """Create a MinIO client fixture."""
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    # Ensure test bucket exists
    if not client.bucket_exists(TEST_BUCKET):
        client.make_bucket(TEST_BUCKET)
    
    yield client
    
    # Cleanup: Remove all objects in the bucket
    objects = client.list_objects(TEST_BUCKET, recursive=True)
    for obj in objects:
        client.remove_object(TEST_BUCKET, obj.object_name)

@pytest.fixture
def store(minio_client):
    """Create a MinioStore fixture."""
    return MinioStore(
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        bucket_name=TEST_BUCKET
    )

@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return {
        "nested": {
            "key": "value",
            "list": [1, 2, 3],
            "dict": {"a": 1, "b": 2}
        },
        "test_key": "test_value",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@pytest.fixture
def sample_namespace_data():
    """Create sample data with namespaces for testing."""
    return {
        "test": {
            "ns1": {
                "key1": {"value": 1},
                "key2": {"value": 2}
            },
            "ns2": {
                "key3": {"value": 3},
                "key4": {"value": 4}
            }
        }
    }

@pytest.fixture(autouse=True)
def cleanup(minio_client):
    """Clean up test objects before each test."""
    # Remove all objects in the bucket
    objects = minio_client.list_objects(TEST_BUCKET, recursive=True)
    for obj in objects:
        minio_client.remove_object(TEST_BUCKET, obj.object_name)
    yield

# Basic Store Operations Tests
def test_store_initialization(store):
    """Test store initialization and bucket existence."""
    assert store.bucket_name == TEST_BUCKET
    assert store.client is not None

def test_put_get_object(store, sample_data):
    """Test putting and getting an object."""
    # Serialize data
    data = json.dumps(sample_data).encode('utf-8')
    
    # Put object
    store.put_object("test-key", data)
    
    # Get object and deserialize
    result = store.get_object("test-key")
    assert result is not None
    assert json.loads(result.decode('utf-8')) == sample_data

def test_put_get_object_with_metadata(store, sample_data):
    """Test putting and getting an object with metadata."""
    # Add metadata
    metadata = {
        "content-type": "application/json",
        "custom-meta": "test"
    }
    # Serialize data
    data = json.dumps(sample_data).encode('utf-8')
    # Put object with metadata
    store.put_object("test-key-meta", data, metadata=metadata)

    # Get object and verify metadata
    result = store.get_object("test-key-meta", include_metadata=True)
    assert result is not None
    assert isinstance(result, tuple)
    data, result_metadata = result
    assert json.loads(data.decode('utf-8')) == sample_data
    assert result_metadata.get("content-type") == "application/json"
    assert result_metadata.get("custom-meta") == "test"

def test_list_objects(store, sample_data):
    """Test listing objects with various filters."""
    # Put multiple objects
    data = json.dumps(sample_data).encode('utf-8')
    test_keys = [f"test-key-{i}" for i in range(3)]
    for key in test_keys:
        store.put_object(key, data)

    # List objects with prefix
    objects = store.list_objects("test-key-")
    assert len(objects) == 3
    assert all(obj in test_keys for obj in objects)

def test_delete_object(store, sample_data):
    """Test deleting objects."""
    # Put object
    data = json.dumps(sample_data).encode('utf-8')
    store.put_object("test-key", data)
    
    # Verify object exists
    assert store.get_object("test-key") is not None
    
    # Delete object
    store.delete_object("test-key")
    
    # Verify deletion
    assert store.get_object("test-key") is None

# Namespace Operations Tests
def test_put_get_with_namespace(store, sample_namespace_data):
    """Test putting and getting objects with namespaces."""
    # Put objects in different namespaces
    for ns, data in sample_namespace_data["test"].items():
        for key, value in data.items():
            store.put(("test", ns), key, value)
    
    # Get objects from namespaces
    for ns, data in sample_namespace_data["test"].items():
        for key, value in data.items():
            result = store.get(("test", ns), key)
            assert result == value

def test_list_namespaces(store, sample_namespace_data):
    """Test listing namespaces."""
    # Put objects in different namespaces
    for ns, data in sample_namespace_data["test"].items():
        for key, value in data.items():
            store.put(("test", ns), key, value)

    # List namespaces
    namespaces = store.list_namespaces(prefix=("test",))
    assert len(namespaces) >= 2
    assert ("test", "ns1") in namespaces
    assert ("test", "ns2") in namespaces

# Search Operations Tests
def test_search(store, sample_namespace_data):
    """Test searching objects."""
    # Put objects in different namespaces
    for ns, data in sample_namespace_data["test"].items():
        for key, value in data.items():
            store.put(("test", ns), key, value)

    # Search with filter
    results = store.search(
        ("test", "ns1"),
        filter={"value": {"$eq": 1}}
    )
    assert len(results) == 1
    assert results[0].value["value"] == 1

# Batch Operations Tests
def test_batch_operations(store):
    """Test batch operations."""
    # Create batch operations
    ops = [
        PutOp(namespace=("test", "ns"), key="key1", value={"test": 1}),
        PutOp(namespace=("test", "ns"), key="key2", value={"test": 2}),
        GetOp(namespace=("test", "ns"), key="key1"),
        GetOp(namespace=("test", "ns"), key="key2"),
        SearchOp(namespace_prefix=("test", "ns"), filter={"test": {"$eq": 1}}),
        ListNamespacesOp(match_conditions=("test",))
    ]

    results = store.batch(ops)
    assert len(results) == 6
    assert results[0] is None  # PutOp result
    assert results[1] is None  # PutOp result
    assert results[2] == {"test": 1}  # GetOp result
    assert results[3] == {"test": 2}  # GetOp result
    assert len(results[4]) == 1  # SearchOp result
    assert len(results[5]) >= 1  # ListNamespacesOp result

# Large Data Tests
def test_large_object(store):
    """Test handling large objects."""
    # Create 5MB of data
    large_data = {"large": "x" * (5 * 1024 * 1024)}
    data = json.dumps(large_data).encode('utf-8')
    
    # Put and get large object
    store.put_object("large-key", data)
    result = store.get_object("large-key")
    
    assert result is not None
    assert json.loads(result.decode('utf-8')) == large_data

# TTL Tests
def test_basic_ttl(store, sample_data):
    """Test basic TTL functionality."""
    try:
        # Put object with 1 second TTL
        store.put(("test",), "key1", sample_data, ttl=0.05)
        
        # Object should be available immediately
        result = store.get(("test",), "key1")
        assert result == sample_data
        
        # Wait for TTL to expire
        time.sleep(3.1)
        
        # Object should be deleted
        result = store.get(("test",), "key1")
        assert result is None
    except S3Error as e:
        pytest.fail(f"S3Error occurred: {str(e)}")

def test_ttl_refresh(store, sample_data):
    """Test TTL refresh functionality."""
    try:
        # Put object with 3 seconds TTL
        store.put(("test",), "key1", sample_data, ttl=0.05)
        
        # Object should be available immediately
        result = store.get(("test",), "key1", refresh_ttl=True)  # This should reset the TTL
        assert result is not None
        assert result.pop('timestamp')  # Remove timestamp before comparison
        sample_data_no_ts = sample_data.copy()
        sample_data_no_ts.pop('timestamp')
        assert result == sample_data_no_ts
        
        # Wait 2 seconds
        time.sleep(2)
        
        # Object should still be available and refresh TTL again
        result = store.get(("test",), "key1", refresh_ttl=True)  # This should reset the TTL again
        assert result is not None
        assert result.pop('timestamp')  # Remove timestamp before comparison
        assert result == sample_data_no_ts
        
        # Wait another 2 seconds
        time.sleep(2)
        
        # Object should still be available because we refreshed the TTL
        result = store.get(("test",), "key1")
        assert result is not None
        assert result.pop('timestamp')  # Remove timestamp before comparison
        assert result == sample_data_no_ts
        
        # Wait 3 seconds without refresh (TTL should expire)
        time.sleep(3)
        
        # Object should be deleted
        result = store.get(("test",), "key1")
        assert result is None
    except S3Error as e:
        pytest.fail(f"S3Error occurred: {str(e)}")

def test_batch_ttl_operations(store, sample_data):
    """Test batch operations with TTL."""
    try:
        # Create batch operations with TTL
        sample_data_no_ts = sample_data.copy()
        sample_data_no_ts.pop('timestamp')
        
        # First put the objects
        ops = [
            PutOp(namespace=("test",), key="key1", value=sample_data, ttl=0.05),
            PutOp(namespace=("test",), key="key2", value=sample_data, ttl=0.1)
        ]
        
        # Execute put operations
        results = store.batch(ops)
        assert len(results) == 2
        assert results[0] is None  # PutOp result
        assert results[1] is None  # PutOp result
        
        # Then get the objects to verify they were stored
        ops = [
            GetOp(namespace=("test",), key="key1"),
            GetOp(namespace=("test",), key="key2")
        ]
        
        results = store.batch(ops)
        assert len(results) == 2
        
        # Check GetOp results without timestamps
        result1 = results[0]
        result1.pop('timestamp')
        assert result1 == sample_data_no_ts
        
        result2 = results[1]
        result2.pop('timestamp')
        assert result2 == sample_data_no_ts
        
        # Wait for first TTL to expire
        time.sleep(3.1)
        # First object should be deleted
        assert store.get(("test",), "key1") is None
        
        # Second object should still be available
        result = store.get(("test",), "key2")
        assert result is not None
        result.pop('timestamp')
        assert result == sample_data_no_ts
        
        # Wait for second TTL to expire
        time.sleep(3.1)
        
        # Second object should now be deleted
        assert store.get(("test",), "key2") is None
        
    except S3Error as e:
        pytest.fail(f"S3Error occurred: {str(e)}")
