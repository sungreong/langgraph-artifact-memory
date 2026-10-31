# langgraph-artifact-memory

Keep heavy values outside LangGraph checkpoints and store compact `ArtifactRef` values in state.

The package externalizes configured state channels before the first checkpoint, hydrates only the
subset required by a wrapped node, and can keep a bounded message window while archiving evicted
messages.

## Why

Long-running graphs can repeatedly serialize growing document lists, tool results, and messages.
This increases checkpoint size, storage usage, and checkpoint read/write latency.

```text
heavy value -> ArtifactStore -> ArtifactRef in graph state
ArtifactRef -> bounded hydrate -> real value inside one node
```

In a local benchmark with 10 concurrent threads and 1,000 total turns, compact references reduced
the maximum checkpoint from 2.35 MB to 112 KB and total runtime from 164.2 seconds to 49.2 seconds.
Results depend on workload, backend, durability mode, and infrastructure.

## Installation

Core package with in-memory and local filesystem stores:

```bash
pip install langgraph-artifact-memory
```

Add the shared Redis backend:

```bash
pip install "langgraph-artifact-memory[redis]"
```

## Backends

| Backend | Persistent | Shared between processes or pods | Extra dependency |
|---|---:|---:|---:|
| `InMemoryArtifactStore` | No | No | No |
| `LocalArtifactStore` | Yes | No | No |
| `RedisArtifactStore` | Yes | Yes | `redis` |

`LocalArtifactStore` uses content-addressed files for payloads and a SQLite WAL database for archive
ordering. A container needs a persistent volume if values must survive pod replacement.

## Basic usage

```python
import operator
from typing import Annotated, Any

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from langgraph_artifact_memory import (
    ArtifactMemory,
    ArtifactRef,
    Externalize,
    Hydrate,
    InMemoryArtifactStore,
)


class State(TypedDict, total=False):
    artifacts: Annotated[list[dict[str, Any] | ArtifactRef], operator.add]
    answer: str


memory = ArtifactMemory(
    InMemoryArtifactStore(),
    externalize={"artifacts": Externalize(itemized=True)},
)


def answer_node(state: State) -> dict[str, str]:
    # The wrapper resolves the selected refs before this function runs.
    artifacts = state["artifacts"]
    return {"answer": f"retrieved={len(artifacts)}"}


graph = (
    StateGraph(State)
    .add_node(
        "answer",
        memory.wrap(
            answer_node,
            hydrate={"artifacts": Hydrate(last_n=5, max_bytes=1_000_000)},
        ),
    )
    .add_edge(START, "answer")
    .add_edge("answer", END)
    .compile(checkpointer=InMemorySaver())
)

config = {"configurable": {"thread_id": "conversation-123"}}
result = await memory.ainvoke(
    graph,
    {"artifacts": new_artifacts},
    config,
)
```

Use `memory.ainvoke()`, not `graph.ainvoke()`, for inputs containing configured externalized
channels. This ensures heavy input is replaced before LangGraph writes its first checkpoint.

## Choose a store

### In-memory

```python
from langgraph_artifact_memory import InMemoryArtifactStore

store = InMemoryArtifactStore()
```

Use for unit tests and short-lived processes. Values disappear when the process exits.

### Local filesystem

```python
from langgraph_artifact_memory import LocalArtifactStore

store = LocalArtifactStore("./.artifacts")
```

Use for local development or a single server. It is persistent but is not a shared distributed
store. In Kubernetes, mount the root on a persistent volume if required.

### Redis

```python
from langgraph_artifact_memory.redis import RedisArtifactStore

store = RedisArtifactStore(
    "redis://localhost:6379/0",
    prefix="my-artifacts",
)

# On shutdown:
await store.aclose()
```

Use when multiple workers or pods need the same artifacts.

## Message window

```python
from langgraph_artifact_memory import MessageWindow

memory = ArtifactMemory(
    store,
    externalize={"artifacts": Externalize(itemized=True)},
    message_window=MessageWindow(max_messages=20),
)

old_messages = await memory.aget_archive("conversation-123", limit=20)
```

A wrapped node that returns `messages` keeps the latest messages in graph state using LangGraph's
`Overwrite` type and archives evicted messages in the configured `ArtifactStore`.

## Metrics

```python
print(memory.metrics.snapshot())
```

The snapshot contains put/get/archive counts, logical bytes, and in-process operation durations.

## Compatibility

| Component | Supported in 0.1.x |
|---|---|
| Python | CPython 3.10-3.13 |
| LangGraph | `>=1.2,<2` |
| Execution | Async |
| Node return type | Mapping / partial state update |
| Wrapped node signature | `node(state)` or `node(state, config)` |
| Artifact values | Values supported by LangGraph `JsonPlusSerializer` |
| Checkpointers | Checkpointer-independent; tested with InMemory and Redis |

LangGraph 1.2 is the minimum because message-window replacement uses `langgraph.types.Overwrite`.

## Build and verify a wheel

```bash
python -m build
python -m twine check dist/*
python -m pip install --force-reinstall dist/langgraph_artifact_memory-0.1.0-py3-none-any.whl
python -c "from langgraph_artifact_memory import ArtifactMemory, InMemoryArtifactStore"
```

## Benchmark stores

```bash
python benchmarks/backend_benchmark.py \
  --operations 1000 \
  --payload-bytes 4096 \
  --concurrency 20
```

Include Redis:

```bash
python benchmarks/backend_benchmark.py \
  --redis-url redis://localhost:6379/0
```

Backend microbenchmarks measure serialization and storage operations. The checkpoint reduction shown
above comes from an end-to-end LangGraph benchmark, not only raw store throughput.

## Lifecycle

Artifact data has a separate lifecycle from checkpoints. Applications should coordinate checkpoint
retention with `store.adelete_scope(thread_id)` and should not delete referenced values while replay,
time travel, or fault recovery still needs them.

## Roadmap

S3 and MinIO support is planned after the MVP. See [ROADMAP.md](ROADMAP.md). The initial release ships
only implemented and tested backends: in-memory, local filesystem, and Redis.

## License

MIT
