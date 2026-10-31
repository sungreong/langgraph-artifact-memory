from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, TypedDict, TypeVar, cast

TMessage = TypeVar("TMessage")


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


def mark_message_kept(
    message: TMessage,
    *,
    kept: bool = True,
    metadata_key: str = "keep",
) -> TMessage:
    """Copy a LangChain message with bounded keep metadata set or cleared."""

    if not metadata_key:
        raise ValueError("metadata_key must not be empty")
    model_copy = getattr(message, "model_copy", None)
    if not callable(model_copy):
        raise TypeError("message must provide model_copy()")
    metadata = dict(getattr(message, "additional_kwargs", {}) or {})
    metadata[metadata_key] = kept
    return cast(TMessage, model_copy(update={"additional_kwargs": metadata}))


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
    """Keep bounded recent and selected messages, archiving everything evicted."""

    key: str = "messages"
    max_messages: int = 20
    archive_channel: str = "messages_archive"
    keep_metadata_key: str | None = "keep"
    keep_message_types: tuple[str, ...] = ()
    max_kept_messages: int = 50

    def __post_init__(self) -> None:
        if self.max_messages < 1:
            raise ValueError("max_messages must be at least 1")
        if self.max_kept_messages < 1:
            raise ValueError("max_kept_messages must be at least 1")
        if self.keep_metadata_key is not None and not self.keep_metadata_key:
            raise ValueError("keep_metadata_key must not be empty")
        if any(not message_type for message_type in self.keep_message_types):
            raise ValueError("keep_message_types must not contain empty values")

    def is_kept(self, message: Any) -> bool:
        """Return whether a message is selected for bounded keep retention."""

        if getattr(message, "type", None) in self.keep_message_types:
            return True
        if self.keep_metadata_key is None:
            return False
        metadata = getattr(message, "additional_kwargs", None)
        return isinstance(metadata, Mapping) and metadata.get(self.keep_metadata_key) is True

    def select(self, messages: Sequence[Any]) -> tuple[list[Any], list[Any]]:
        """Return retained and evicted messages while preserving chronological order."""

        keep_flags = [self.is_kept(message) for message in messages]
        kept_indices = [index for index, kept in enumerate(keep_flags) if kept][
            -self.max_kept_messages :
        ]
        recent_indices = [index for index, kept in enumerate(keep_flags) if not kept][
            -self.max_messages :
        ]
        retained_indices = set(kept_indices + recent_indices)
        retained = [message for index, message in enumerate(messages) if index in retained_indices]
        evicted = [
            message for index, message in enumerate(messages) if index not in retained_indices
        ]
        return retained, evicted
