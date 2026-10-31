from __future__ import annotations

import asyncio
from typing import Any

from langgraph_artifact_memory import (
    ArtifactIOPolicy,
    ArtifactMemory,
    Externalize,
    InMemoryArtifactStore,
)
from langgraph_artifact_memory.models import ArtifactRef


class FlakyStore(InMemoryArtifactStore):
    def __init__(self, failures: int) -> None:
        super().__init__()
        self.failures = failures
        self.put_calls = 0

    async def aput(self, scope: str, channel: str, value: Any) -> ArtifactRef:
        self.put_calls += 1
        if self.failures:
            self.failures -= 1
            raise ConnectionError("temporary backend failure")
        return await super().aput(scope, channel, value)


async def main() -> None:
    store = FlakyStore(failures=2)
    memory = ArtifactMemory(
        store,
        externalize={"documents": Externalize(itemized=True)},
        io_policy=ArtifactIOPolicy(
            max_concurrency=1,
            max_retries=2,
            initial_backoff=0,
            max_backoff=0,
            jitter=0,
        ),
    )
    compact_state = await memory.prepare_input(
        {"documents": [{"content": "eventually stored"}]},
        {"configurable": {"thread_id": "resilient-io-example"}},
    )
    metrics = memory.metrics.snapshot()

    assert len(compact_state["documents"]) == 1
    assert store.put_calls == 3
    assert metrics["retries"]["put"] == 2
    print("metrics:", metrics)
    print("resilient I/O example passed")


if __name__ == "__main__":
    asyncio.run(main())
