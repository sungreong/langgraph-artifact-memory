from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from langgraph_artifact_memory.models import ArtifactRef


class ArtifactStore(Protocol):
    """Storage contract used by ArtifactMemory."""

    async def aput(self, scope: str, channel: str, value: Any) -> ArtifactRef: ...

    async def aget(self, ref: ArtifactRef) -> Any: ...

    async def aappend_archive(
        self, scope: str, channel: str, refs: Sequence[ArtifactRef]
    ) -> None: ...

    async def alist_archive(
        self, scope: str, channel: str, *, limit: int | None = None
    ) -> list[ArtifactRef]: ...

    async def adelete_scope(self, scope: str) -> None: ...
