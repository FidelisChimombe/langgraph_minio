# ProSciKit - MinIO Checkpointing for LangGraph

A custom LangGraph-compatible checkpointing system using MinIO as the backend. This implementation provides both synchronous and asynchronous interfaces for storing and retrieving checkpoints, with support for versioning and atomic writes.

## Features

- ✅ Synchronous and asynchronous interfaces
- ✅ Checkpoint versioning and history
- ✅ Atomic writes with `put_writes`
- ✅ Timestamp-aware key ordering
- ✅ LangGraph's `serde` for checkpoint (de)serialization
- ✅ Clean package layout using Poetry
- ✅ Channel value management
- ✅ Pending writes and sends tracking

## Installation

```bash
poetry install
```

## Usage

### Synchronous Usage

```python
from langgraph_minio.store.base import MinioStore
from langgraph_minio.checkpoint.base import BaseMinioSaver
from langgraph.checkpoint.base import Checkpoint

# Initialize the store
store = MinioStore(
    endpoint_url="http://localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    bucket_name="checkpoints",
)

# Initialize the checkpoint saver
saver = BaseMinioSaver(store)

# Create a checkpoint with channel values and versions
checkpoint = Checkpoint(
    v=1,  # Version of checkpoint format
    id="step-1",
    ts=datetime.now(timezone.utc).isoformat(),
    channel_values={
        "state": "running",
        "data": {"key": "value"}
    },
    channel_versions={
        "state": "1",
        "data": "1"
    },
    versions_seen={
        "node1": {
            "state": "1",
            "data": "1"
        }
    },
    pending_sends=[],
    next_tasks=[],
    metadata={}
)

# Save the checkpoint
saver.put({"configurable": {"thread_id": "my-graph"}}, checkpoint)

# Save checkpoint with additional writes
additional_writes = [
    ("metadata.json", b'{"status": "completed"}'),
    ("results.csv", b"id,value\n1,42\n2,84"),
]
saver.put_writes(
    {"configurable": {"thread_id": "my-graph"}},
    additional_writes,
    task_id="task1"
)

# Get the latest checkpoint
latest = saver.get_latest("my-graph")

# Get a specific version
versions = saver.list_versions("my-graph", "step-1")
version = versions[0]
specific = saver.get_version(
    {"configurable": {"thread_id": "my-graph"}},
    "state",
    version
)

# List all checkpoints
all_checkpoints = saver.list({"configurable": {"thread_id": "my-graph"}})

# Get channel values
channel_values = saver.get_channel_values("my-graph")
```

### Asynchronous Usage

```python
import asyncio
from langgraph_minio.store.aio import AsyncMinioStore
from langgraph_minio.checkpoint.aio import AsyncMinioSaver
from langgraph.checkpoint.base import Checkpoint

async def main():
    # Initialize the store
    async with AsyncMinioStore(
        endpoint_url="http://localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket_name="checkpoints",
    ) as store:
        # Initialize the checkpoint saver
        saver = AsyncMinioSaver(store)

        # Create a checkpoint
        checkpoint = Checkpoint(
            v=1,
            id="step-1",
            ts=datetime.now(timezone.utc).isoformat(),
            channel_values={
                "state": "running",
                "data": {"key": "value"}
            },
            channel_versions={
                "state": "1",
                "data": "1"
            },
            versions_seen={
                "node1": {
                    "state": "1",
                    "data": "1"
                }
            },
            pending_sends=[],
            next_tasks=[],
            metadata={}
        )

        # Save the checkpoint
        await saver.put({"configurable": {"thread_id": "my-graph"}}, checkpoint)

        # Save checkpoint with additional writes
        additional_writes = [
            ("metadata.json", b'{"status": "completed"}'),
            ("results.csv", b"id,value\n1,42\n2,84"),
        ]
        await saver.put_writes(
            {"configurable": {"thread_id": "my-graph"}},
            additional_writes,
            task_id="task1"
        )

        # Get the latest checkpoint
        latest = await saver.get_latest("my-graph")

        # Get a specific version
        versions = await saver.list_versions("my-graph", "step-1")
        version = versions[0]
        specific = await saver.get_version(
            {"configurable": {"thread_id": "my-graph"}},
            "state",
            version
        )

        # List all checkpoints
        all_checkpoints = await saver.list({"configurable": {"thread_id": "my-graph"}})

        # Get channel values
        channel_values = await saver.get_channel_values("my-graph")

asyncio.run(main())
```

## Development

### Running Tests

```bash
poetry run pytest
```

### Code Formatting

```bash
poetry run black .
poetry run isort .
```

### Type Checking

```bash
poetry run mypy .
```

## Key Features Explained

### Versioning

Checkpoints are stored with timestamps in their keys, allowing for version history:

```
checkpoints/my-graph/step-1-2024-01-01T00:00:00.json
checkpoints/my-graph/step-1-2024-01-02T00:00:00.json
```

### Channel Management

The checkpoint system supports managing multiple channels with their own versions:

```python
checkpoint = Checkpoint(
    v=1,
    id="step-1",
    channel_values={
        "state": "running",
        "data": {"key": "value"}
    },
    channel_versions={
        "state": "1",
        "data": "1"
    },
    # ...
)
```

### Atomic Writes

The `put_writes` method allows saving a checkpoint along with additional data atomically:

```python
saver.put_writes(
    {"configurable": {"thread_id": "my-graph"}},
    [
        ("metadata.json", b'{"status": "completed"}'),
        ("results.csv", b"id,value\n1,42\n2,84"),
    ],
    task_id="task1"
)
```

### Timestamp-aware Ordering

The `get_tuple` method returns both the value and its last modified timestamp:

```python
value, timestamp = saver.get_tuple({"configurable": {"thread_id": "my-graph"}})
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request
