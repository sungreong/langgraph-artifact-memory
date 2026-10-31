from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

from langgraph_artifact_memory import InMemoryArtifactStore, LocalArtifactStore
from langgraph_artifact_memory.stores.base import ArtifactStore


async def _bounded_map(limit: int, function: Any, values: list[Any]) -> list[Any]:
    semaphore = asyncio.Semaphore(limit)

    async def invoke(value: Any) -> Any:
        async with semaphore:
            return await function(value)

    return await asyncio.gather(*(invoke(value) for value in values))


async def benchmark_store(
    name: str,
    store: ArtifactStore,
    *,
    operations: int,
    payload_bytes: int,
    concurrency: int,
) -> dict[str, Any]:
    scope = f"benchmark-{uuid4().hex}"
    seed = "artifact-payload-"
    content = (seed * ((payload_bytes // len(seed)) + 1))[:payload_bytes]
    values = [{"id": index, "content": content} for index in range(operations)]

    started = perf_counter()
    refs = await _bounded_map(
        concurrency,
        lambda value: store.aput(scope, "documents", value),
        values,
    )
    put_seconds = perf_counter() - started

    started = perf_counter()
    restored = await _bounded_map(concurrency, store.aget, refs)
    get_seconds = perf_counter() - started

    assert restored == values
    await store.adelete_scope(scope)
    return {
        "backend": name,
        "operations": operations,
        "payload_bytes": payload_bytes,
        "concurrency": concurrency,
        "put_seconds": round(put_seconds, 6),
        "put_operations_per_second": round(operations / put_seconds, 2),
        "get_seconds": round(get_seconds, 6),
        "get_operations_per_second": round(operations / get_seconds, 2),
    }


async def run(args: argparse.Namespace) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    results.append(
        await benchmark_store(
            "memory",
            InMemoryArtifactStore(),
            operations=args.operations,
            payload_bytes=args.payload_bytes,
            concurrency=args.concurrency,
        )
    )

    with tempfile.TemporaryDirectory(prefix="langgraph-artifact-benchmark-") as directory:
        results.append(
            await benchmark_store(
                "local",
                LocalArtifactStore(directory),
                operations=args.operations,
                payload_bytes=args.payload_bytes,
                concurrency=args.concurrency,
            )
        )

    if args.redis_url:
        from langgraph_artifact_memory.redis import RedisArtifactStore

        redis_store = RedisArtifactStore(
            args.redis_url,
            prefix=f"lgartifact_benchmark_{uuid4().hex}",
        )
        try:
            results.append(
                await benchmark_store(
                    "redis",
                    redis_store,
                    operations=args.operations,
                    payload_bytes=args.payload_bytes,
                    concurrency=args.concurrency,
                )
            )
        finally:
            await redis_store.aclose()

    return {"results": results}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark ArtifactStore backends")
    parser.add_argument("--operations", type=int, default=1000)
    parser.add_argument("--payload-bytes", type=int, default=4096)
    parser.add_argument("--concurrency", type=int, default=20)
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
