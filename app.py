from datetime import datetime
from fastapi import FastAPI
from langgraph.checkpoint.base import CheckpointTuple
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.base import RunnableConfig
from typing import Dict, TypedDict, Any, List, Callable



from typing import Annotated
from langgraph.graph import StateGraph, END
from operator import add
from langgraph_minio.store.aio import AsyncMinioStore
from langgraph_minio.store.base import MinioStore
from langgraph_minio.checkpoint.base import MinioSaver
from langgraph_minio.checkpoint.aio import AsyncMinioSaver

from minio import Minio




from pydantic import BaseModel
from langgraph.checkpoint.base import CheckpointTuple
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.base import RunnableConfig
from typing import Dict, TypedDict, Any, List
import json

from typing import Annotated
from langgraph.graph import StateGraph, END
from operator import add
import time

app = FastAPI()

connection_args = {"decode_responses": True}
store = MinioStore(
    bucket_name="pro-sci-kit-test",
    client=Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
)

async_store = AsyncMinioStore(
    bucket_name="pro-sci-kit-test-async",
    endpoint_url="http://localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
)
saver = MinioSaver(
    store=store,
)

saver = AsyncMinioSaver(
    store=async_store,
)


class CheckpointData(BaseModel):
    checkpoint_id: str
    config: Dict[str, Any]
    checkpoint: Dict[str, Any]
    metadata: Dict[str, Any]

write_config = {"configurable": {"thread_id": "1", "checkpoint_ns": "test-minio-namespace"}}
read_config = {"configurable": {"thread_id": "1ef4f797-8335-6428-8001-8a1503f9b876", "checkpoint_id": "1ef4f797-8335-6428-8001-8a1503f9b875", "checkpoint_ns": "test-minio-namespace"}}
checkpoint = {
        "v": 1,
        "ts": "2024-07-31T20:14:19.804150+00:00",
        "id": "1ef4f797-8335-6428-8001-8a1503f9b875",
        "channel_values": {
            "my_key": "meow",
            "node": "node"
        },
        "channel_versions": {
            "__start__": 2,
            "my_key": 3,
            "start:node": 3,
            "node": 3
        },
        "versions_seen": {
            "__input__": {},
            "__start__": {
                "__start__": 1
            },
            "node": {
                "start:node": 2
            }
        },
        "pending_sends": []
    }



@app.post("/save-checkpoint")
async def save_checkpoint(data: CheckpointData):
    config = {
        "configurable": {
            "thread_id": data.checkpoint_id,
            "checkpoint_ns": "test-minio-namespace",
            "checkpoint_id": data.checkpoint_id
        }
    }
    saver.put(
        config=config,
        checkpoint=data.checkpoint,
        metadata=data.metadata,
        new_versions={}  # Empty dict for new versions since we're not tracking versions
        # write_config, checkpoint, {}, {}
    )
    return {"status": "saved"}

@app.get("/load-checkpoint/{checkpoint_id}")
async def load_checkpoint(checkpoint_id: str):
    read_config["configurable"]["checkpoint_id"] = checkpoint_id
    checkpoint = saver.get_tuple(read_config)
    return checkpoint._asdict() if checkpoint else {"error": "not found"}


class StoreItem(BaseModel):
    key: str
    value: str

@app.post("/store")
async def store_item(item: StoreItem):
    store.put(("test-minio-namespace",), item.key,  item.value)
    return {"status": "stored"}

@app.get("/store/{key}")
async def get_item(key: str):
    value = store.get(("test-minio-namespace",), key)
    return {"key": key, "value": value}

@app.post("/async-store")
async def store_item(item: StoreItem):
    await async_store.aput(("test-minio-namespace",), item.key,  item.value)
    return {"status": "stored"}

@app.get("/async-store/{key}")
async def get_item(key: str):
    value = await async_store.aget(("test-minio-namespace",), key)
    return {"key": key, "value": value}

import random
import json
import string

def generate_random_string(length: int) -> str:
    """Generate a random string of given length."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_large_json(size_kb: int) -> dict:
    """Generate a JSON object of approximately given size in KB."""
    result = {
        "id": str(random.randint(1000, 9999)),
        "timestamp": datetime.now().isoformat(),
        "data": []
    }
    
    # Each entry is roughly 100 bytes
    num_entries = (size_kb * 1024) // 100
    
    for i in range(num_entries):
        entry = {
            "key": generate_random_string(10),
            "value": generate_random_string(50),
            "index": i
        }
        result["data"].append(entry)
        
    return result

@app.post("/store-5kb")
async def store_5kb(item: dict):
    key = item["key"]
    print(f"*****************Storing 5kb file with key: {key}")
    data = generate_large_json(5)
    store.put(("test-minio-namespace",), key, json.dumps(data))
    return {"key": key, "size": "5kb"}

@app.post("/store-10kb") 
async def store_10kb(item: dict):
    key = item["key"]
    print(f"**************Storing 10kb file with key: {key}")
    data = generate_large_json(10)
    store.put(("test-minio-namespace",), key, json.dumps(data))
    return {"key": key, "size": "10kb"}

@app.get("/large-file/{key}")
async def get_large_file(key: str):
    value = store.get(("test-minio-namespace",), key)
    if value:
        return json.loads(value)
    return {"error": "not found"}

@app.get("/list-large-files")
async def list_large_files():
    # List objects with prefix "large_file"
    keys = store.list_objects("large_file")
    return {"files": keys}


# Annotated with Reducer
class GraphState(TypedDict):
    steps: Annotated[list[str], add]
    value: Annotated[int, add]
    checkpoints: Annotated[list[Dict[str, Any]], add]  # Changed to use a list of checkpoints

# LangGraph steps
def step1(state: GraphState) -> GraphState:
    state["steps"].append("step1")
    state["value"] += 1
    state["checkpoints"].append({
        "step": "step1",
        "steps": state["steps"].copy(),
        "value": state["value"]
    })
    return state

def step2a(state: GraphState) -> GraphState:
    state["steps"].append("step2a") 
    state["value"] += 2
    state["checkpoints"].append({
        "step": "step2a",
        "steps": state["steps"].copy(),
        "value": state["value"]
    })
    return state

def step2b(state: GraphState) -> GraphState:
    state["steps"].append("step2b")
    state["value"] += 3
    state["checkpoints"].append({
        "step": "step2b",
        "steps": state["steps"].copy(),
        "value": state["value"]
    })
    return state

def merge(state: GraphState) -> GraphState:
    state["steps"].append("merge")
    state["value"] *= 2
    state["checkpoints"].append({
        "step": "merge",
        "steps": state["steps"].copy(),
        "value": state["value"]
    })
    return state

# Build the graph
workflow = StateGraph(GraphState)
workflow.add_node("step1", step1)
workflow.add_node("step2a", step2a) 
workflow.add_node("step2b", step2b)
workflow.add_node("merge", merge)

workflow.set_entry_point("step1")
workflow.add_edge("step1", "step2a")
workflow.add_edge("step1", "step2b") 
workflow.add_edge("step2a", "merge")
workflow.add_edge("step2b", "merge")
workflow.add_edge("merge", END)

# Compile with checkpointer
app_graph = workflow.compile(checkpointer=saver)

@app.post("/run-graph/{checkpoint_id}")
async def run_graph(checkpoint_id: str):
    input_state: GraphState = {
        "steps": [], 
        "value": 0,
        "checkpoints": []  # Initialize as empty list
    }
    
    # Run graph and collect checkpoints
    final_state = app_graph.invoke(
        input_state,
        config={
            "configurable": {
                "checkpoint_id": checkpoint_id,
                "thread_id": checkpoint_id,
                "checkpoint_ns": "graph-execution"
            }
        }
    )
    
    # Return both final state and checkpoint data
    return {
        "final_state": {
            "steps": final_state["steps"],
            "value": final_state["value"]
        },
        "checkpoints": final_state["checkpoints"]
    }