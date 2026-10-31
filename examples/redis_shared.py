from __future__ import annotations

import asyncio
import os
from uuid import uuid4

from langgraph_artifact_memory import ArtifactMemory, Externalize, Hydrate
from langgraph_artifact_memory.redis import RedisArtifactStore


async def main() -> None:
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        raise SystemExit("Set REDIS_URL, for example redis://localhost:6379/0")

    prefix = f"lgartifact_example_{uuid4().hex}"
    thread_id = f"redis-shared-{uuid4().hex}"
    config = {"configurable": {"thread_id": thread_id}}
    writer_store = RedisArtifactStore(redis_url, prefix=prefix)
    reader_store = RedisArtifactStore(redis_url, prefix=prefix)
    writer = ArtifactMemory(
        writer_store,
        externalize={"documents": Externalize(itemized=True)},
    )
    reader = ArtifactMemory(
        reader_store,
        externalize={"documents": Externalize(itemized=True)},
    )
    documents = [{"content": "shared between workers" * 100}]

    try:
        compact_state = await writer.prepare_input({"documents": documents}, config)
        restored = await reader.hydrate_state(
            compact_state,
            {"documents": Hydrate()},
        )
        assert restored["documents"] == documents
        print("redis shared-store example passed")
    finally:
        await writer_store.adelete_scope(thread_id)
        await writer_store.aclose()
        await reader_store.aclose()


if __name__ == "__main__":
    asyncio.run(main())
