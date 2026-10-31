from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import Any

from langgraph_artifact_memory.models import ArtifactRef
from langgraph_artifact_memory.stores._serde import ArtifactSerializer, scope_hash


class InMemoryArtifactStore:
    """Ephemeral artifact store for tests and short-lived local processes."""

    def __init__(self) -> None:
        self._serde = ArtifactSerializer()
        self._values: dict[str, tuple[str, bytes]] = {}
        self._archives: dict[tuple[str, str], list[ArtifactRef]] = defaultdict(list)

    async def aput(self, scope: str, channel: str, value: Any) -> ArtifactRef:
        type_name, payload, digest, size_bytes = self._serde.dumps(value)
        key = f"memory:artifact:{scope_hash(scope)}:{channel}:{digest}"
        self._values[key] = (type_name, payload)
        return {"artifact_key": key, "size_bytes": size_bytes}

    async def aget(self, ref: ArtifactRef) -> Any:
        try:
            type_name, payload = self._values[ref["artifact_key"]]
        except KeyError as error:
            raise KeyError(f"artifact not found: {ref['artifact_key']}") from error
        return self._serde.loads(type_name, payload)

    async def aappend_archive(self, scope: str, channel: str, refs: Sequence[ArtifactRef]) -> None:
        self._archives[(scope_hash(scope), channel)].extend(refs)

    async def alist_archive(
        self, scope: str, channel: str, *, limit: int | None = None
    ) -> list[ArtifactRef]:
        refs = self._archives[(scope_hash(scope), channel)]
        return list(refs[-limit:] if limit is not None else refs)

    async def adelete_scope(self, scope: str) -> None:
        hashed_scope = scope_hash(scope)
        scope_token = f":{hashed_scope}:"
        for key in [key for key in self._values if scope_token in key]:
            del self._values[key]
        for key in [key for key in self._archives if key[0] == hashed_scope]:
            del self._archives[key]

    def stats(self) -> dict[str, int]:
        return {
            "artifacts": len(self._values),
            "archive_refs": sum(len(refs) for refs in self._archives.values()),
        }
