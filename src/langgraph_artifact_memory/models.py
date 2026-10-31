from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypedDict


class ArtifactRef(TypedDict):
    """Compact reference stored in LangGraph state instead of a heavy value."""

    artifact_key: str
    size_bytes: int


def is_artifact_ref(value: Any) -> bool:
    """Return whether a value has the compact ArtifactRef shape."""

    return (
        isinstance(value, dict)
        and isinstance(value.get("artifact_key"), str)
        and isinstance(value.get("size_bytes"), int)
    )


@dataclass(frozen=True, slots=True)
class Externalize:
    """Configure how one state channel is replaced by ArtifactRef values."""

    itemized: bool = True


@dataclass(frozen=True, slots=True)
class Hydrate:
    """Bound how much external state a wrapped node restores."""

    last_n: int | None = None
    max_bytes: int | None = None

    def __post_init__(self) -> None:
        if self.last_n is not None and self.last_n < 1:
            raise ValueError("last_n must be at least 1")
        if self.max_bytes is not None and self.max_bytes < 1:
            raise ValueError("max_bytes must be at least 1")


@dataclass(frozen=True, slots=True)
class MessageWindow:
    """Keep recent messages in state and archive evicted messages externally."""

    key: str = "messages"
    max_messages: int = 20
    archive_channel: str = "messages_archive"

    def __post_init__(self) -> None:
        if self.max_messages < 1:
            raise ValueError("max_messages must be at least 1")
