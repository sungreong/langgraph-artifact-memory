from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

from langgraph_artifact_memory import (
    ArtifactIOPolicy,
    ArtifactMemory,
    Externalize,
    Hydrate,
    LocalArtifactStore,
)
from langgraph_artifact_memory.stores.base import ArtifactStore


async def benchmark_policy(
    backend: str,
    store: ArtifactStore,
    *,
    operations: int,
    payload_bytes: int,
    concurrency: int,
) -> dict[str, Any]:
    memory = ArtifactMemory(
        store,
        externalize={"documents": Externalize(itemized=True)},
        io_policy=ArtifactIOPolicy(
            max_concurrency=concurrency,
            get_timeout=30,
            put_timeout=30,
            max_retries=0,
        ),
    )
    scope = f"io-policy-benchmark-{uuid4().hex}"
    config = {"configurable": {"thread_id": scope}}
    seed = "artifact-payload-"
    content = (seed * ((payload_bytes // len(seed)) + 1))[:payload_bytes]
    values = [{"id": index, "content": content} for index in range(operations)]

    started = perf_counter()
    prepared = await memory.prepare_input({"documents": values}, config)
    put_seconds = perf_counter() - started

    started = perf_counter()
    hydrated = await memory.hydrate_state(
        prepared,
        {"documents": Hydrate()},
    )
    get_seconds = perf_counter() - started

    assert hydrated["documents"] == values
    await store.adelete_scope(scope)
    metrics = memory.metrics.snapshot()
    return {
        "backend": backend,
        "operations": operations,
        "payload_bytes": payload_bytes,
        "max_concurrency": concurrency,
        "put_seconds": round(put_seconds, 6),
        "put_operations_per_second": round(operations / put_seconds, 2),
        "get_seconds": round(get_seconds, 6),
        "get_operations_per_second": round(operations / get_seconds, 2),
        "observed_max_in_flight": metrics["concurrency"]["max_in_flight"],
    }


async def run(args: argparse.Namespace) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    concurrency_values = [int(value) for value in args.concurrency.split(",")]

    with tempfile.TemporaryDirectory(prefix="langgraph-artifact-policy-") as directory:
        local_store = LocalArtifactStore(directory)
        for concurrency in concurrency_values:
            results.append(
                await benchmark_policy(
                    "local",
                    local_store,
                    operations=args.operations,
                    payload_bytes=args.payload_bytes,
                    concurrency=concurrency,
                )
            )

    if args.redis_url:
        from langgraph_artifact_memory.redis import RedisArtifactStore

        redis_store = RedisArtifactStore(
            args.redis_url,
            prefix=f"lgartifact_policy_benchmark_{uuid4().hex}",
        )
        try:
            for concurrency in concurrency_values:
                results.append(
                    await benchmark_policy(
                        "redis",
                        redis_store,
                        operations=args.operations,
                        payload_bytes=args.payload_bytes,
                        concurrency=concurrency,
                    )
                )
        finally:
            await redis_store.aclose()

    return {"results": results}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark ArtifactIOPolicy concurrency")
    parser.add_argument("--operations", type=int, default=1000)
    parser.add_argument("--payload-bytes", type=int, default=4096)
    parser.add_argument("--concurrency", default="1,4,16")
    parser.add_argument("--redis-url")
    parser.add_argument("--output")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = asyncio.run(run(args))
    payload = json.dumps(result, indent=2)
    print(payload)
    if args.output:
        Path(args.output).write_text(payload + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
