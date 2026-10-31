from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest


@pytest.mark.integration
def test_redis_store_round_trip() -> None:
    redis_url = os.getenv("TEST_REDIS_URL")
    if not redis_url:
        pytest.skip("TEST_REDIS_URL is not set")

    from langgraph_artifact_memory.redis import RedisArtifactStore

    async def scenario() -> None:
        store = RedisArtifactStore(redis_url, prefix=f"lgartifact_test_{uuid4().hex}")
        scope = f"thread-{uuid4().hex}"
        try:
            ref = await store.aput(scope, "documents", {"content": "redis-value"})
            await store.aappend_archive(scope, "history", [ref])

            assert await store.aget(ref) == {"content": "redis-value"}
            assert await store.alist_archive(scope, "history") == [ref]
        finally:
            await store.adelete_scope(scope)
            await store.aclose()

    asyncio.run(scenario())
