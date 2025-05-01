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
import time

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
        "x-amz-meta-content-type": "application/json",
        "x-amz-meta-custom-meta": "test"
    }
    # Serialize data
    data = json.dumps(sample_data).encode('utf-8')
    # Put object with metadata
    store.put_object("test-key-meta", data, metadata=metadata)
    
    # Get object and verify metadata
    result, result_metadata = store.get_object("test-key-meta", include_metadata=True)
    assert result is not None
    assert json.loads(result.decode('utf-8')) == sample_data
    #MinIO (and S3 in general) strips the x-amz-meta- prefix when it returns user-defined metadata.
    assert result_metadata['content-type'] == "application/json"
    assert result_metadata['custom-meta'] == "test"

def test_list_objects(store, sample_data):
    """Test listing objects with various filters."""
    # Put multiple objects
    data = json.dumps(sample_data).encode('utf-8')
    for i in range(3):
        store.put_object(f"test-key-{i}", data)
    
    # List objects with prefix
    objects = store.list_objects("test-key")
    assert len(objects) == 3
    assert all(obj.startswith("test-key-") for obj in objects)
    
    # List objects with namespace
    store.put_object("namespace/test-key", data)
    objects = store.list_objects("namespace/")
    assert len(objects) == 1
    assert objects[0] == "namespace/test-key"

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
    namespaces = store.list_namespaces()
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
    assert ("test", "ns") in results[5]  # ListNamespacesOp result

# Error Handling Tests
import pytest
from minio.error import S3Error
from urllib3.exceptions import HTTPError
import time

import pytest
from minio.error import S3Error
import time

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
    # Put object with 1 second TTL
    store.put(("test",), "key1", sample_data, ttl=1)
    
    # Object should be available immediately
    result = store.get(("test",), "key1")
    assert result == sample_data
    
    # Wait for TTL to expire
    time.sleep(1.1)
    
    # Object should be deleted
    result = store.get(("test",), "key1")
    assert result is None

def test_ttl_refresh(store, sample_data):
    """Test TTL refresh functionality."""
    # Put object with 2 second TTL
    store.put(("test",), "key1", sample_data, ttl=2)
    
    # Object should be available immediately
    result = store.get(("test",), "key1", refresh_ttl=True)  # This should reset the TTL
    assert result is not None
    assert result.pop('timestamp')  # Remove timestamp before comparison
    sample_data_no_ts = sample_data.copy()
    sample_data_no_ts.pop('timestamp')
    assert result == sample_data_no_ts
    
    # Wait 1 second
    time.sleep(1)
    
    # Object should still be available and refresh TTL again
    result = store.get(("test",), "key1", refresh_ttl=True)  # This should reset the TTL again
    assert result is not None
    assert result.pop('timestamp')  # Remove timestamp before comparison
    assert result == sample_data_no_ts
    
    # Wait another 1.1 seconds (original TTL would have expired)
    time.sleep(1.1)
    
    # Object should still be available because we refreshed the TTL
    result = store.get(("test",), "key1")
    assert result is not None
    assert result.pop('timestamp')  # Remove timestamp before comparison
    assert result == sample_data_no_ts
    
    # Wait 2.1 seconds without refresh (TTL should expire)
    time.sleep(2.1)
    
    # Object should be deleted
    result = store.get(("test",), "key1")
    assert result is None

def test_batch_ttl_operations(store, sample_data):
    """Test batch operations with TTL."""
    # Create batch operations with TTL
    sample_data_no_ts = sample_data.copy()
    sample_data_no_ts.pop('timestamp')
    
    ops = [
        PutOp(namespace=("test",), key="key1", value=sample_data, ttl=1),
        PutOp(namespace=("test",), key="key2", value=sample_data, ttl=2),
        GetOp(namespace=("test",), key="key1", refresh_ttl=True),
        GetOp(namespace=("test",), key="key2", refresh_ttl=True)
    ]
    
    # Execute batch operations
    results = store.batch(ops)
    assert len(results) == 4
    assert results[0] is None  # PutOp result
    assert results[1] is None  # PutOp result
    
    # Check GetOp results without timestamps
    result2 = results[2]
    result2.pop('timestamp')
    assert result2 == sample_data_no_ts  # GetOp result
    
    result3 = results[3]
    result3.pop('timestamp')
    assert result3 == sample_data_no_ts  # GetOp result
    
    # Wait for first TTL to expire
    time.sleep(1.1)
    
    # First object should be deleted
    assert store.get(("test",), "key1") is None
    
    # Second object should still be available
    result = store.get(("test",), "key2")
    result.pop('timestamp')
    assert result == sample_data_no_ts

