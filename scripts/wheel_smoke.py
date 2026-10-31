from __future__ import annotations

import asyncio
import importlib.util
import tempfile
from pathlib import Path

from langgraph_artifact_memory import (
    ArtifactIOPolicy,
    InMemoryArtifactStore,
    LocalArtifactStore,
)


async def main() -> None:
    assert importlib.util.find_spec("redis") is None
    assert ArtifactIOPolicy().max_concurrency == 16

    memory_store = InMemoryArtifactStore()
    memory_ref = await memory_store.aput("smoke", "documents", {"value": [1, 2, 3]})
    assert await memory_store.aget(memory_ref) == {"value": [1, 2, 3]}

    with tempfile.TemporaryDirectory() as temporary_directory:
        local_store = LocalArtifactStore(Path(temporary_directory))
        local_ref = await local_store.aput("smoke", "documents", b"wheel-roundtrip")
        assert await local_store.aget(local_ref) == b"wheel-roundtrip"

    print("wheel smoke test passed without the Redis extra")


if __name__ == "__main__":
    asyncio.run(main())
