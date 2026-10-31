from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from typing import Any

try:
    from redis.asyncio import Redis
except ImportError as error:  # pragma: no cover - exercised by wheel smoke tests
    raise ImportError(
        "RedisArtifactStore requires the redis extra: "
        "pip install 'langgraph-artifact-memory[redis]'"
    ) from error

from langgraph_artifact_memory.models import ArtifactRef
from langgraph_artifact_memory.stores._serde import ArtifactSerializer, scope_hash


class RedisArtifactStore:
    """Shared content-addressed artifact store backed by Redis."""

    def __init__(self, redis_url: str, *, prefix: str = "lgartifact") -> None:
        self.client = Redis.from_url(redis_url, decode_responses=False)
        self.prefix = prefix
        self._serde = ArtifactSerializer()

    def _artifact_key(self, scope: str, channel: str, digest: str) -> str:
        channel_token = hashlib.sha256(channel.encode()).hexdigest()[:16]
        return f"{self.prefix}:artifact:{scope_hash(scope)}:{channel_token}:{digest}"

    def _archive_key(self, scope: str, channel: str) -> str:
        channel_token = hashlib.sha256(channel.encode()).hexdigest()[:16]
        return f"{self.prefix}:archive:{scope_hash(scope)}:{channel_token}"

    async def aput(self, scope: str, channel: str, value: Any) -> ArtifactRef:
        type_name, payload, digest, size_bytes = self._serde.dumps(value)
        key = self._artifact_key(scope, channel, digest)
        await self.client.hset(key, mapping={b"type": type_name.encode(), b"payload": payload})
        return {"artifact_key": key, "size_bytes": size_bytes}

    async def aget(self, ref: ArtifactRef) -> Any:
        values = await self.client.hmget(ref["artifact_key"], b"type", b"payload")
        if values[0] is None or values[1] is None:
            raise KeyError(f"artifact not found: {ref['artifact_key']}")
        return self._serde.loads(values[0].decode(), values[1])

    async def aappend_archive(self, scope: str, channel: str, refs: Sequence[ArtifactRef]) -> None:
        if refs:
            await self.client.rpush(
                self._archive_key(scope, channel),
                *(json.dumps(ref, separators=(",", ":")).encode() for ref in refs),
            )

    async def alist_archive(
        self, scope: str, channel: str, *, limit: int | None = None
    ) -> list[ArtifactRef]:
        start = -limit if limit is not None else 0
        values = await self.client.lrange(self._archive_key(scope, channel), start, -1)
        return [json.loads(value) for value in values]

    async def adelete_scope(self, scope: str) -> None:
        hashed_scope = scope_hash(scope)
        keys: list[bytes] = []
        for pattern in (
            f"{self.prefix}:artifact:{hashed_scope}:*",
            f"{self.prefix}:archive:{hashed_scope}:*",
        ):
            keys.extend([key async for key in self.client.scan_iter(match=pattern, count=1000)])
        if keys:
            await self.client.unlink(*keys)

    async def aclose(self) -> None:
        await self.client.aclose()
