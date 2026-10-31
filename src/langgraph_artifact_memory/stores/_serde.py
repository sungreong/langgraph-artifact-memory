from __future__ import annotations

import hashlib
from typing import Any

from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer


class ArtifactSerializer:
    """Small adapter around LangGraph's checkpoint-compatible serializer."""

    def __init__(self) -> None:
        self._serde = JsonPlusSerializer()

    def dumps(self, value: Any) -> tuple[str, bytes, str, int]:
        type_name, payload = self._serde.dumps_typed(value)
        type_bytes = type_name.encode()
        digest = hashlib.sha256(type_bytes + payload).hexdigest()
        return type_name, payload, digest, len(type_bytes) + len(payload)

    def loads(self, type_name: str, payload: bytes) -> Any:
        return self._serde.loads_typed((type_name, payload))


def scope_hash(scope: str) -> str:
    return hashlib.sha256(scope.encode()).hexdigest()[:20]


def channel_hash(channel: str) -> str:
    return hashlib.sha256(channel.encode()).hexdigest()[:16]
