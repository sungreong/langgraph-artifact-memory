from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

from langgraph_artifact_memory import (
    ArtifactMemory,
    Externalize,
    Hydrate,
    LocalArtifactStore,
)


async def main() -> None:
    documents = [
        {"name": "design.md", "content": "A" * 4096},
        {"name": "notes.md", "content": "B" * 4096},
    ]
    config = {"configurable": {"thread_id": "local-persistence-example"}}

    with tempfile.TemporaryDirectory(prefix="artifact-memory-example-") as directory:
        root = Path(directory)
        writer = ArtifactMemory(
            LocalArtifactStore(root),
            externalize={"documents": Externalize(itemized=True)},
        )
        compact_state = await writer.prepare_input({"documents": documents}, config)

        # Simulate a process restart by constructing a new store and memory instance.
        reader_store = LocalArtifactStore(root)
        reader = ArtifactMemory(
            reader_store,
            externalize={"documents": Externalize(itemized=True)},
        )
        restored = await reader.hydrate_state(
            compact_state,
            {"documents": Hydrate()},
        )

        assert restored["documents"] == documents
        assert reader_store.stats()["artifacts"] == 2
        print("local stats:", reader_store.stats())
        print("local persistence example passed")


if __name__ == "__main__":
    asyncio.run(main())
