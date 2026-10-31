from __future__ import annotations

from collections import defaultdict
from time import perf_counter
from typing import Any


class ArtifactMemoryMetrics:
    """Low-overhead in-process counters for artifact operations."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = defaultdict(int)
        self.durations_ms: dict[str, list[float]] = defaultdict(list)
        self.bytes: dict[str, int] = defaultdict(int)

    def record(self, operation: str, started: float, *, size_bytes: int = 0) -> None:
        self.counts[operation] += 1
        self.durations_ms[operation].append((perf_counter() - started) * 1000)
        self.bytes[operation] += size_bytes

    def snapshot(self) -> dict[str, Any]:
        return {
            "counts": dict(self.counts),
            "bytes": dict(self.bytes),
            "duration_ms": {
                operation: {
                    "average": round(sum(values) / len(values), 3),
                    "max": round(max(values), 3),
                }
                for operation, values in self.durations_ms.items()
                if values
            },
        }
