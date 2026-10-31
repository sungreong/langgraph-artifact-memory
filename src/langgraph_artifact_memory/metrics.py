from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from time import perf_counter
from typing import Any


@dataclass(slots=True)
class _Duration:
    count: int = 0
    total: float = 0.0
    maximum: float = 0.0

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value
        self.maximum = max(self.maximum, value)


class ArtifactMemoryMetrics:
    """Bounded-memory in-process counters for artifact operations."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = defaultdict(int)
        self._durations: dict[str, _Duration] = defaultdict(_Duration)
        self.bytes: dict[str, int] = defaultdict(int)
        self.retries: dict[str, int] = defaultdict(int)
        self.failures: dict[str, int] = defaultdict(int)
        self.timeouts: dict[str, int] = defaultdict(int)
        self.capacity_rejections: dict[str, int] = defaultdict(int)
        self._queue_wait: dict[str, _Duration] = defaultdict(_Duration)
        self._in_flight = 0
        self._max_in_flight = 0

    def record(self, operation: str, started: float, *, size_bytes: int = 0) -> None:
        self.counts[operation] += 1
        self._durations[operation].add((perf_counter() - started) * 1000)
        self.bytes[operation] += size_bytes

    def record_retry(self, operation: str) -> None:
        self.retries[operation] += 1

    def record_failure(self, operation: str) -> None:
        self.failures[operation] += 1

    def record_timeout(self, operation: str) -> None:
        self.timeouts[operation] += 1

    def record_capacity_rejection(self, operation: str) -> None:
        self.capacity_rejections[operation] += 1

    def record_queue_wait(self, operation: str, started: float) -> None:
        self._queue_wait[operation].add((perf_counter() - started) * 1000)

    def operation_started(self) -> None:
        self._in_flight += 1
        self._max_in_flight = max(self._max_in_flight, self._in_flight)

    def operation_finished(self) -> None:
        self._in_flight -= 1

    @staticmethod
    def _duration_snapshot(values: dict[str, _Duration]) -> dict[str, dict[str, float]]:
        return {
            operation: {
                "average": round(value.total / value.count, 3),
                "max": round(value.maximum, 3),
            }
            for operation, value in values.items()
            if value.count
        }

    def snapshot(self) -> dict[str, Any]:
        return {
            "counts": dict(self.counts),
            "bytes": dict(self.bytes),
            "duration_ms": self._duration_snapshot(self._durations),
            "retries": dict(self.retries),
            "failures": dict(self.failures),
            "timeouts": dict(self.timeouts),
            "capacity_rejections": dict(self.capacity_rejections),
            "queue_wait_ms": self._duration_snapshot(self._queue_wait),
            "concurrency": {
                "in_flight": self._in_flight,
                "max_in_flight": self._max_in_flight,
            },
        }
