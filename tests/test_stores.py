from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from langgraph_artifact_memory import InMemoryArtifactStore, LocalArtifactStore


@pytest.mark.parametrize("backend", ["memory", "local"])
def test_store_round_trip_archive_and_scope_delete(tmp_path: Path, backend: str) -> None:
    async def scenario() -> None:
        store = (
            InMemoryArtifactStore()
            if backend == "memory"
            else LocalArtifactStore(tmp_path / "artifacts")
        )
        value = {"content": "large-value" * 100, "metadata": {"kind": "text"}}

        first = await store.aput("thread-1", "documents", value)
        second = await store.aput("thread-1", "documents", value)
        await store.aappend_archive("thread-1", "history", [first, second])

        assert first == second
        assert await store.aget(first) == value
        assert await store.alist_archive("thread-1", "history", limit=1) == [second]
        assert store.stats()["artifacts"] == 1

        await store.adelete_scope("thread-1")

        assert await store.alist_archive("thread-1", "history") == []
        with pytest.raises(KeyError):
            await store.aget(first)

    asyncio.run(scenario())


def test_local_store_survives_new_instance(tmp_path: Path) -> None:
    async def scenario() -> None:
        root = tmp_path / "persistent-artifacts"
        first_store = LocalArtifactStore(root)
        ref = await first_store.aput("thread-1", "documents", ["persistent", "value"])

        reopened_store = LocalArtifactStore(root)

        assert await reopened_store.aget(ref) == ["persistent", "value"]

    asyncio.run(scenario())


def test_local_store_rejects_reference_outside_root(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = LocalArtifactStore(tmp_path / "artifacts")

        with pytest.raises(ValueError, match="escapes"):
            await store.aget({"artifact_key": "local:../outside.blob", "size_bytes": 1})

    asyncio.run(scenario())
