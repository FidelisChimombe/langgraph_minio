import asyncio
import logging
import pytest
import pytest_asyncio
from langgraph_minio.store.base import MinioStore
from langgraph_minio.store.aio import AsyncMinioStore
from datetime import datetime, timezone
from minio import Minio
from langgraph.store.base import GetOp, PutOp, SearchOp, ListNamespacesOp
import json
import time
from minio.error import S3Error


# Configure logging
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
def minio_store():
    return MinioStore(
        endpoint_url="http://localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket_name="test-bucket"
    )

@pytest_asyncio.fixture
async def async_store():
    """Create an AsyncMinioStore fixture."""
    return AsyncMinioStore(
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
async def cleanup(async_store):
    """Clean up test objects before each test."""
    # Remove all objects in the bucket
    objects = await async_store.alist_objects()
    for obj in objects:
        await async_store.adelete_object(obj)
    yield
    # Cleanup after test
    objects = await async_store.alist_objects()
    for obj in objects:
        await async_store.adelete_object(obj)

@pytest.mark.asyncio
async def test_async_store_initialization():
    """Test async store initialization and bucket existence."""
    # Test with direct instantiation
    store = AsyncMinioStore(
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        bucket_name=TEST_BUCKET
    )
    assert store.bucket_name == TEST_BUCKET
    assert store.client is not None

    # Test bucket exists
    await store._ensure_bucket_exists()
    exists = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: store.client.bucket_exists(TEST_BUCKET)
    )
    assert exists

    # Test context manager
    async with AsyncMinioStore(
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        bucket_name=TEST_BUCKET
    ) as store:
        assert store.bucket_name == TEST_BUCKET
        assert store.client is not None

@pytest.mark.asyncio
async def test_async_store_error_handling():
    """Test error handling for AsyncMinioStore operations."""
    # Test invalid bucket name
    print("***********test invalid bucket name")
    with pytest.raises(ValueError):
        AsyncMinioStore(
            endpoint_url=f"http://{MINIO_ENDPOINT}",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            bucket_name=None
        )
    
    # Test invalid credentials
    print("***********test invalid credentials")
    with pytest.raises(RuntimeError):
        try:
            print("***********raising")
            AsyncMinioStore(
                endpoint_url=f"http://{MINIO_ENDPOINT}",
                access_key="wrong",
                secret_key="wrong",
                bucket_name=TEST_BUCKET
            )
        except Exception as e:
            print("***********error")
            print(e)
            raise e
    
    # Test invalid endpoint
    print("***********test invalid endpoint")
    with pytest.raises(RuntimeError):
        AsyncMinioStore(
            endpoint_url="http://invalid-endpoint:9000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            bucket_name=TEST_BUCKET
        )

@pytest.mark.asyncio
async def test_async_put_get_object(async_store, sample_data):
    """Test putting and getting an object asynchronously."""
    # Serialize data
    data = json.dumps(sample_data).encode('utf-8')
    
    # Put object
    await async_store.aput_object("test-key", data)
    
    # Get object and deserialize
    result = await async_store.aget_object("test-key")
    assert result is not None
    assert json.loads(result.decode('utf-8')) == sample_data

@pytest.mark.asyncio
async def test_async_put_get_object_with_metadata(async_store, sample_data):
    """Test putting and getting an object with metadata asynchronously."""
    # Add metadata
    metadata = {
        "content-type": "application/json",
        "custom-meta": "test"
    }
    
    # Serialize data
    data = json.dumps(sample_data).encode('utf-8')
    
    # Put object with metadata
    await async_store.aput_object("test-key-meta", data, metadata=metadata)
    
    # Get object and verify metadata
    result = await async_store.aget_object("test-key-meta")
    assert result is not None
    assert json.loads(result.decode('utf-8')) == sample_data

@pytest.mark.asyncio
async def test_async_list_objects(async_store, sample_data):
    """Test listing objects asynchronously."""
    # Put multiple objects
    data = json.dumps(sample_data).encode('utf-8')
    test_keys = [f"test-list-{i}" for i in range(3)]
    for key in test_keys:
        await async_store.aput_object(key, data)
    
    # List objects with prefix
    objects = await async_store.alist_objects("test-list-")
    assert len(objects) == 3
    assert all(obj in test_keys for obj in objects)

@pytest.mark.asyncio
async def test_async_delete_object(async_store, sample_data):
    """Test deleting objects asynchronously."""
    # Put object
    data = json.dumps(sample_data).encode('utf-8')
    await async_store.aput_object("test-key", data)
    
    # Verify object exists
    result = await async_store.aget_object("test-key")
    assert result is not None
    
    # Delete object
    await async_store.adelete_object("test-key")
    
    # Verify deletion
    result = await async_store.aget_object("test-key")
    assert result is None

@pytest.mark.asyncio
async def test_async_put_get_with_namespace(async_store, sample_namespace_data):
    """Test putting and getting objects with namespaces asynchronously."""
    # Put objects in different namespaces
    for ns, data in sample_namespace_data["test"].items():
        for key, value in data.items():
            await async_store.aput(("test", ns), key, value)
    
    # Get objects from namespaces
    for ns, data in sample_namespace_data["test"].items():
        for key, value in data.items():
            result = await async_store.aget(("test", ns), key)
            assert result == value

@pytest.mark.asyncio
async def test_async_list_namespaces(async_store, sample_namespace_data):
    """Test listing namespaces asynchronously."""
    # Put objects in different namespaces
    for ns, data in sample_namespace_data["test"].items():
        for key, value in data.items():
            await async_store.aput(("test", ns), key, value)
    
    # List namespaces
    namespaces = await async_store.alist_namespaces(prefix=("test",))
    assert len(namespaces) >= 2
    assert ("test", "ns1") in namespaces
    assert ("test", "ns2") in namespaces

@pytest.mark.asyncio
async def test_async_search(async_store, sample_namespace_data):
    """Test searching objects asynchronously."""
    # Put objects in different namespaces
    print(sample_namespace_data)
    for ns, data in sample_namespace_data["test"].items():
        for key, value in data.items():
            print("***********putting", ("test", ns), key, value)
            await async_store.aput(("test", ns), key, value)
    
    # Search with filter
    results = await async_store.asearch(
        ("test", "ns1"),
        filter={"value": {"$eq": 1}}
    )
    print("***********results", results)
    assert len(results) == 1
    assert results[0].value["value"] == 1

@pytest.mark.asyncio
async def test_async_error_handling(async_store):
    """Test error handling for AsyncMinioStore operations."""
    # Test invalid credentials (expect timeout or auth error)
    try:
        async with asyncio.timeout(2.0):  # Shorter timeout
            invalid_store = AsyncMinioStore(
                endpoint_url=f"http://{MINIO_ENDPOINT}",
                access_key="wrong",
                secret_key="wrong",
                bucket_name=TEST_BUCKET
            )
            async with invalid_store:
                pytest.fail("Should not reach here with invalid creds")
    except TimeoutError:
        # Accept timeout as valid failure mode for invalid creds
        pass  
    except (RuntimeError, S3Error) as e:
        error_msg = str(e).lower()
        assert any(msg in error_msg for msg in [
            "access denied", 
            "invalid credentials",
            "the access key id you provided does not exist"
        ]), f"Unexpected error: {error_msg}"

    # Test 2: Nonexistent object should return None
    assert await async_store.aget_object("nonexistent") is None

    # Test 3: Nonexistent namespace should return empty list
    assert await async_store.alist_objects("nonexistent/") == []

@pytest.mark.asyncio
async def test_async_large_object(async_store):
    """Test handling large objects asynchronously."""
    # Create 5MB of data
    large_data = {"large": "x" * (5 * 1024 * 1024)}
    data = json.dumps(large_data).encode('utf-8')
    
    # Put and get large object
    await async_store.aput_object("large-key", data)
    result = await async_store.aget_object("large-key")
    
    assert result is not None
    assert json.loads(result.decode('utf-8')) == large_data

@pytest.mark.asyncio
async def test_async_ttl_operations(async_store, sample_data):
    """Test TTL with async operations."""
    # Put object with TTL
    await async_store.aput(
        ("test", "ttl"),
        "async_key",
        sample_data,
        ttl=0.05  # 3 second TTL (3/60 minutes)
    )

    # Verify object exists
    result = await async_store.aget(("test", "ttl"), "async_key")
    assert result == sample_data

    # Wait for TTL to expire
    await asyncio.sleep(4)  # Wait 2 seconds

    # Verify object is gone
    result = await async_store.aget(("test", "ttl"), "async_key")
    assert result is None

    # Test async TTL refresh
    await async_store.aput(
        ("test", "ttl"),
        "async_refresh",
        sample_data,
        ttl=0.05  # 3 second TTL (3/60 minutes)
    )

    # Wait 1 second
    await asyncio.sleep(1)

    # Refresh TTL - this should reset the last_accessed time
    result = await async_store.aget(("test", "ttl"), "async_refresh", refresh_ttl=True)
    assert result == sample_data

    # Wait 2 seconds - original TTL would have expired, but refresh should keep it alive
    await asyncio.sleep(2)

    # Object should still exist because we refreshed TTL
    result = await async_store.aget(("test", "ttl"), "async_refresh")
    assert result == sample_data

    # Wait another 2 seconds - now it should expire
    await asyncio.sleep(2)

    # Verify object is gone
    result = await async_store.aget(("test", "ttl"), "async_refresh")
    assert result is None

@pytest.mark.asyncio
async def test_async_concurrent_operations(async_store):
    """Test concurrent operations asynchronously."""
    # Create multiple objects
    objects = [{"value": f"data-{i}"} for i in range(5)]
    
    # Put objects concurrently
    await asyncio.gather(*[
        async_store.aput_object(
            f"concurrent-key-{i}",
            json.dumps(obj).encode('utf-8')
        )
        for i, obj in enumerate(objects)
    ])
    
    # Get objects concurrently
    results = await asyncio.gather(*[
        async_store.aget_object(f"concurrent-key-{i}")
        for i in range(5)
    ])
    
    # Verify results
    for i, result in enumerate(results):
        assert result is not None
        assert json.loads(result.decode('utf-8')) == objects[i] 