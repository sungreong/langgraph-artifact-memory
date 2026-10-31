from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest
from langchain_core.messages import HumanMessage, ToolMessage


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
            value = [
                HumanMessage(content="redis-value", additional_kwargs={"keep": True}),
                ToolMessage(content="tool-result", tool_call_id="redis-tool"),
            ]
            ref = await store.aput(scope, "documents", value)
            await store.aappend_archive(scope, "history", [ref])

            assert await store.aget(ref) == value
            assert await store.alist_archive(scope, "history") == [ref]
        finally:
            await store.adelete_scope(scope)
            await store.aclose()

    asyncio.run(scenario())
