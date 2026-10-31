from __future__ import annotations

import asyncio
import json
import os
import shutil
import sqlite3
import struct
import tempfile
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from langgraph_artifact_memory.models import ArtifactRef
from langgraph_artifact_memory.stores._serde import (
    ArtifactSerializer,
    channel_hash,
    scope_hash,
)


class LocalArtifactStore:
    """Persistent content-addressed store backed by files and SQLite."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser().resolve()
        self.artifacts_root = self.root / "artifacts"
        self.database_path = self.root / "index.sqlite3"
        self._serde = ArtifactSerializer()
        self.artifacts_root.mkdir(parents=True, exist_ok=True)
        self._initialize_database()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _initialize_database(self) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS archive_refs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scope_hash TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    ref_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS archive_refs_scope_channel_id
                ON archive_refs(scope_hash, channel, id)
                """
            )

    def _relative_path(self, scope: str, channel: str, digest: str) -> Path:
        return Path("artifacts") / scope_hash(scope) / channel_hash(channel) / f"{digest}.blob"

    def _path_from_ref(self, ref: ArtifactRef) -> Path:
        key = ref["artifact_key"]
        if not key.startswith("local:"):
            raise ValueError(f"not a local artifact reference: {key}")
        relative = Path(key.removeprefix("local:"))
        target = (self.root / relative).resolve()
        try:
            target.relative_to(self.artifacts_root)
        except ValueError as error:
            raise ValueError("artifact reference escapes the configured root") from error
        return target

    @staticmethod
    def _encode(type_name: str, payload: bytes) -> bytes:
        type_bytes = type_name.encode()
        return struct.pack(">I", len(type_bytes)) + type_bytes + payload

    @staticmethod
    def _decode(blob: bytes) -> tuple[str, bytes]:
        if len(blob) < 4:
            raise ValueError("invalid artifact blob")
        type_size = struct.unpack(">I", blob[:4])[0]
        type_end = 4 + type_size
        if len(blob) < type_end:
            raise ValueError("invalid artifact blob header")
        return blob[4:type_end].decode(), blob[type_end:]

    def _put_sync(self, scope: str, channel: str, value: Any) -> ArtifactRef:
        type_name, payload, digest, size_bytes = self._serde.dumps(value)
        relative = self._relative_path(scope, channel, digest)
        target = self.root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            descriptor, temporary_name = tempfile.mkstemp(dir=target.parent, suffix=".tmp")
            try:
                with os.fdopen(descriptor, "wb") as handle:
                    handle.write(self._encode(type_name, payload))
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary_name, target)
            finally:
                if os.path.exists(temporary_name):
                    os.unlink(temporary_name)
        return {
            "artifact_key": f"local:{relative.as_posix()}",
            "size_bytes": size_bytes,
        }

    async def aput(self, scope: str, channel: str, value: Any) -> ArtifactRef:
        return await asyncio.to_thread(self._put_sync, scope, channel, value)

    def _get_sync(self, ref: ArtifactRef) -> Any:
        path = self._path_from_ref(ref)
        try:
            blob = path.read_bytes()
        except FileNotFoundError as error:
            raise KeyError(f"artifact not found: {ref['artifact_key']}") from error
        type_name, payload = self._decode(blob)
        return self._serde.loads(type_name, payload)

    async def aget(self, ref: ArtifactRef) -> Any:
        return await asyncio.to_thread(self._get_sync, ref)

    def _append_archive_sync(self, scope: str, channel: str, refs: Sequence[ArtifactRef]) -> None:
        if not refs:
            return
        rows = [
            (scope_hash(scope), channel, json.dumps(ref, separators=(",", ":"))) for ref in refs
        ]
        with self._connection() as connection:
            connection.executemany(
                "INSERT INTO archive_refs(scope_hash, channel, ref_json) VALUES (?, ?, ?)",
                rows,
            )

    async def aappend_archive(self, scope: str, channel: str, refs: Sequence[ArtifactRef]) -> None:
        await asyncio.to_thread(self._append_archive_sync, scope, channel, refs)

    def _list_archive_sync(self, scope: str, channel: str, limit: int | None) -> list[ArtifactRef]:
        query = (
            "SELECT ref_json FROM archive_refs "
            "WHERE scope_hash = ? AND channel = ? ORDER BY id DESC"
        )
        parameters: list[Any] = [scope_hash(scope), channel]
        if limit is not None:
            query += " LIMIT ?"
            parameters.append(limit)
        with self._connection() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [json.loads(row[0]) for row in reversed(rows)]

    async def alist_archive(
        self, scope: str, channel: str, *, limit: int | None = None
    ) -> list[ArtifactRef]:
        return await asyncio.to_thread(self._list_archive_sync, scope, channel, limit)

    def _delete_scope_sync(self, scope: str) -> None:
        hashed_scope = scope_hash(scope)
        target = (self.artifacts_root / hashed_scope).resolve()
        if target.parent != self.artifacts_root:
            raise ValueError("refusing to delete outside the artifacts root")
        if target.exists():
            shutil.rmtree(target)
        with self._connection() as connection:
            connection.execute("DELETE FROM archive_refs WHERE scope_hash = ?", (hashed_scope,))

    async def adelete_scope(self, scope: str) -> None:
        await asyncio.to_thread(self._delete_scope_sync, scope)

    def stats(self) -> dict[str, int]:
        artifact_files = list(self.artifacts_root.rglob("*.blob"))
        with self._connection() as connection:
            archive_refs = int(
                connection.execute("SELECT COUNT(*) FROM archive_refs").fetchone()[0]
            )
        return {
            "artifacts": len(artifact_files),
            "artifact_disk_bytes": sum(path.stat().st_size for path in artifact_files),
            "archive_refs": archive_refs,
        }
