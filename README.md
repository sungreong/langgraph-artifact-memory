# langgraph-artifact-memory

[한국어 문서](README.kr.md) · [Background and benchmark story (한국어)](docs/blog/introducing-langgraph-artifact-memory-ko.md)

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

## Measured impact

The following end-to-end benchmark used the same Redis service, `durability="sync"`, 10 concurrent
users, and 100 turns per user. Each turn added five 4 KB artifacts and one 1 KB user message. The
answer node hydrated five artifacts, calculated a digest, and produced a 1 KB AI message.

| Metric | Inline state | `ArtifactRef` + bounded messages | Improvement |
|---|---:|---:|---:|
| Total runtime for 1,000 turns | 164.2 s | 49.2 s | 70.1% lower |
| Throughput | 6.09 turns/s | 20.33 turns/s | 3.34x |
| Average turn latency | 1,634 ms | 490 ms | 70.0% lower |
| p95 turn latency | 3,211 ms | 722 ms | 77.5% lower |
| Last 10 turns per user, average | 3,205 ms | 698 ms | 78.2% lower |
| Maximum checkpoint size | 2,346 KB | 112 KB | 95.2% lower |
| Artifact channel size | 2,104 KB | 85 KB | 96.0% lower |
| Message channel size | 242 KB | 25 KB | 89.5% lower |
| Checkpoint `aget_tuple` average | 926 ms | 89 ms | 90.4% lower |
| Checkpoint `aput` average | 118 ms | 40 ms | 65.7% lower |
| Redis peak memory | 507 MB | 382 MB | 24.6% lower |
| Redis workload memory increase | 404 MB | 279 MB | 30.9% lower |

These are local benchmark results, not production guarantees. The optimized Redis peak includes
5,000 stored artifact bodies and 1,800 archived messages; it does not hide external-storage cost.
The comparison measures the complete recommended configuration—artifact externalization plus a
bounded message window—rather than attributing every improvement to artifact externalization alone.
Results depend on payload size, graph shape, checkpointer, durability mode, network latency, Redis
configuration, and hydration policy.

### Where it helps

| Workload characteristic | Inline state behavior | With this package |
|---|---|---|
| Documents or Tool results accumulate in lists | Every checkpoint repeatedly serializes growing payloads | Checkpoints retain small references; bodies are written once |
| A conversation runs for many turns | Message state and checkpoint reads grow continuously | Recent messages stay in state; evicted messages remain retrievable from the archive |
| Several workers or pods share a thread | Large checkpoint reads/writes amplify Redis and network load | Redis-backed references are shared while per-node hydration remains bounded |
| Only one node needs a heavy value | Every node receives the heavy checkpoint state | `hydrate` restores only the configured slice for the wrapped node |
| Reducers repeatedly merge large lists | Copying, merging, and serialization cost grows with full payload size | Reducers merge compact `ArtifactRef` dictionaries instead of the original bodies |

### When it may not help

| Situation | Trade-off |
|---|---|
| Short conversations with small state | Artifact put/get calls can cost more than the checkpoint savings |
| Every node needs every artifact | Repeated full hydration can move the bottleneck from checkpoints to the artifact store |
| Hundreds of MB are hydrated at once | Pod memory can still grow; use `last_n`, `max_bytes`, or application-level retrieval |
| Artifact and checkpoint retention are not coordinated | Checkpoints may reference deleted data, or unused artifacts may accumulate |
| An in-memory or local store is used across multiple pods | References are not shareable; use Redis or another shared backend |

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
    ArtifactIOPolicy,
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
    io_policy=ArtifactIOPolicy(max_concurrency=16),
    state_schema=State,
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

## State schema validation

Pass the same schema used by `StateGraph` to detect configuration mistakes before the graph starts:

```python
memory = ArtifactMemory(
    store,
    externalize={"artifacts": Externalize(itemized=True)},
    state_schema=State,
    validation="strict",
)
```

Strict validation checks that:

- every `externalize`, `hydrate`, and `message_window` key exists in the state schema;
- `Externalize(itemized=True)` targets a `list[...]` channel whose item type can hold
  `ArtifactRef`;
- `Externalize(itemized=False)` targets a channel whose type can hold `ArtifactRef`;
- a message window targets a list channel;
- input values and wrapped node updates for itemized channels are actually lists at runtime.

A typo such as `"artfacts"` raises `ArtifactConfigurationError` with
`Did you mean 'artifacts'?`. A node returning the wrong runtime shape raises
`ArtifactStateTypeError` before the value is written to the artifact store or checkpoint.

The mode can be changed to `validation="warn"` during migration, which emits
`ArtifactValidationWarning` and preserves the previous behavior, or to `validation="off"` to skip
validation. Static key and annotation checks require `state_schema`; runtime itemized-value checks
still apply in strict mode without one. `TypedDict` annotations describe valid state but do not
perform runtime validation by themselves, which is why the package performs both checks.

## Runnable examples

Every example contains assertions and is executed by the test suite as a standalone script.

| Example | Demonstrates |
|---|---|
| `examples/basic.py` | Basic graph input externalization and bounded hydration |
| `examples/document_pipeline.py` | Collect, externalize, analyze, and answer pipeline |
| `examples/parallel_tool_workflow.py` | Parallel `Send` workers with externalized Tool results |
| `examples/conditional_hydration.py` | Quick and deep graph routes with different hydrate limits |
| `examples/multi_thread_isolation.py` | Separate checkpoints and archives for multiple thread IDs |
| `examples/multiple_channels.py` | Different policies for document and tool-result state keys |
| `examples/message_keep.py` | Human, AI, System, and Tool messages with keep and archive |
| `examples/long_conversation.py` | A 100-turn conversation bounded to recent and kept messages |
| `examples/local_persistence.py` | Reopening a Local store after a simulated process restart |
| `examples/resilient_io.py` | Transient failures, retries, and resilience metrics |
| `examples/redis_shared.py` | Two workers sharing artifacts through Redis |
| `examples/state_validation.py` | Schema typos and invalid node/input shapes caught early |

Run the examples from the repository root:

```bash
python examples/basic.py
python examples/document_pipeline.py
python examples/parallel_tool_workflow.py
python examples/conditional_hydration.py
python examples/multi_thread_isolation.py
python examples/message_keep.py
python examples/long_conversation.py
python examples/local_persistence.py
python examples/multiple_channels.py
python examples/resilient_io.py
python examples/state_validation.py
```

The Redis example requires the optional dependency and a URL:

```bash
pip install -e ".[redis]"
REDIS_URL=redis://localhost:6379/0 python examples/redis_shared.py
```

In PowerShell, set the environment variable with
`$env:REDIS_URL = "redis://localhost:6379/0"` before running the same Python command.

## Bounded async I/O and resilience

Artifact lists are stored and hydrated concurrently while preserving their original order. A
shared semaphore bounds all store I/O performed by one `ArtifactMemory` instance, including calls
from concurrent graph threads.

```python
from langgraph_artifact_memory import ArtifactIOPolicy, ArtifactMemory

memory = ArtifactMemory(
    store,
    externalize={"artifacts": Externalize(itemized=True)},
    io_policy=ArtifactIOPolicy(
        max_concurrency=16,
        get_timeout=2.0,
        put_timeout=5.0,
        archive_timeout=3.0,
        acquire_timeout=1.0,
        max_retries=2,
        initial_backoff=0.1,
        max_backoff=2.0,
        jitter=0.1,
    ),
)
```

- Timeouts and transient connection failures use bounded exponential-backoff retries.
- Missing artifacts and serialization or validation errors are not retried.
- `put`, `get`, and archive reads are safe to retry with the shipped content-addressed stores.
- Archive append is timed out but is not automatically retried because an ambiguous write could
  otherwise duplicate archive entries.
- `acquire_timeout=None` waits for I/O capacity. Set a positive value to reject excess work with
  `ArtifactCapacityError` instead of allowing an unbounded wait.

Terminal timeouts raise `ArtifactTimeoutError`; exhausted transient failures raise
`ArtifactUnavailableError`. These errors are allowed to leave the node so LangGraph can apply its
node-level retry or failure policy. If a wrapped node also has a LangGraph `RetryPolicy`, keep both
attempt limits small: store-level attempts and node-level attempts multiply.

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
from langgraph_artifact_memory import MessageWindow, mark_message_kept

memory = ArtifactMemory(
    store,
    externalize={"artifacts": Externalize(itemized=True)},
    message_window=MessageWindow(
        max_messages=20,
        keep_message_types=("system",),
        keep_metadata_key="keep",
        max_kept_messages=50,
    ),
)

old_messages = await memory.aget_archive("conversation-123", limit=20)
```

A wrapped node that returns `messages` keeps the latest messages in graph state using LangGraph's
`Overwrite` type and archives evicted messages in the configured `ArtifactStore`.

Mark an individual message for bounded keep retention with ordinary LangChain message metadata:

```python
from langchain_core.messages import HumanMessage

preference = HumanMessage(
    content="Always answer me in Korean.",
    additional_kwargs={"keep": True},
)

# Existing messages can be copied without mutation and toggled later.
kept_preference = mark_message_kept(preference)
ordinary_preference = mark_message_kept(kept_preference, kept=False)
```

`keep_message_types` applies the same behavior to every message of a configured type, such as
`system`. `max_messages` counts recent non-kept messages and `max_kept_messages` separately bounds
kept messages, so state contains at most their sum. Both lists preserve chronological order.
Messages that exceed either bound are archived rather than deleted, including old kept messages.
`HumanMessage`, `AIMessage`, `SystemMessage`, and `ToolMessage` use the same window and archive path.
Kept messages remain in checkpoint state, so keep small instructions, preferences, and decisions;
do not mark large tool payloads as kept. Store large payloads in an externalized artifact channel and
keep only their compact reference or summary in the message.

## Metrics

```python
print(memory.metrics.snapshot())
```

The snapshot contains put/get/archive counts, logical bytes, and in-process operation durations.
It also reports retries, failures, timeouts, capacity rejections, queue wait, and peak in-flight
artifact I/O. Duration metrics use running aggregates rather than retaining one sample per call.

## Compatibility

| Component | Supported in 0.4.x |
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
python -m pip install --force-reinstall dist/langgraph_artifact_memory-0.4.0-py3-none-any.whl
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

Compare the bounded I/O policy at several concurrency levels:

```bash
python benchmarks/io_policy_benchmark.py \
  --operations 1000 \
  --concurrency 1,4,16 \
  --redis-url redis://localhost:6379/0
```

On one Windows/Python 3.12 run with 1,000 4 KB values, Redis concurrency 4 increased put
throughput from 658 to 3,529 operations/second, while concurrency 16 increased get throughput from
756 to 3,828 operations/second. Treat these as local measurements rather than production capacity
guarantees; tune concurrency against the real Redis connection pool and latency distribution.

## Lifecycle

Artifact data has a separate lifecycle from checkpoints. Applications should coordinate checkpoint
retention with `store.adelete_scope(thread_id)` and should not delete referenced values while replay,
time travel, or fault recovery still needs them.

## Roadmap

S3 and MinIO support is planned after the MVP. See [ROADMAP.md](ROADMAP.md). The initial release ships
only implemented and tested backends: in-memory, local filesystem, and Redis.

## License

MIT
