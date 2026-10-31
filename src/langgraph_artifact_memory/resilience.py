from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from time import perf_counter
from typing import TypeVar, cast

from langgraph_artifact_memory.errors import (
    ArtifactCapacityError,
    ArtifactTimeoutError,
    ArtifactUnavailableError,
)
from langgraph_artifact_memory.metrics import ArtifactMemoryMetrics

T = TypeVar("T")
R = TypeVar("R")

_RETRYABLE_REDIS_ERRORS = {
    "BusyLoadingError",
    "ClusterDownError",
    "ConnectionError",
    "TimeoutError",
    "TryAgainError",
}


@dataclass(frozen=True, slots=True)
class ArtifactIOPolicy:
    """Bound artifact I/O concurrency, latency, and transient retries."""

    max_concurrency: int = 16
    get_timeout: float | None = 5.0
    put_timeout: float | None = 10.0
    archive_timeout: float | None = 5.0
    acquire_timeout: float | None = None
    max_retries: int = 2
    initial_backoff: float = 0.1
    max_backoff: float = 2.0
    jitter: float = 0.1
    retryable_exceptions: tuple[type[Exception], ...] = (ConnectionError, OSError)

    def __post_init__(self) -> None:
        if self.max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")
        for name in ("get_timeout", "put_timeout", "archive_timeout", "acquire_timeout"):
            value = getattr(self, name)
            if value is not None and value <= 0:
                raise ValueError(f"{name} must be greater than 0 or None")
        if self.max_retries < 0:
            raise ValueError("max_retries must be at least 0")
        if self.initial_backoff < 0:
            raise ValueError("initial_backoff must be at least 0")
        if self.max_backoff < self.initial_backoff:
            raise ValueError("max_backoff must be greater than or equal to initial_backoff")
        if self.jitter < 0:
            raise ValueError("jitter must be at least 0")

    def is_retryable(self, error: Exception) -> bool:
        if isinstance(error, self.retryable_exceptions):
            return True
        error_type = type(error)
        return (
            error_type.__module__.startswith("redis.")
            and error_type.__name__ in _RETRYABLE_REDIS_ERRORS
        )


class ArtifactIOExecutor:
    """Internal bounded executor shared by all operations in ArtifactMemory."""

    def __init__(self, policy: ArtifactIOPolicy, metrics: ArtifactMemoryMetrics) -> None:
        self.policy = policy
        self.metrics = metrics
        self._semaphore = asyncio.Semaphore(policy.max_concurrency)

    async def _acquire(self, operation: str) -> None:
        started = perf_counter()
        try:
            if self.policy.acquire_timeout is None:
                await self._semaphore.acquire()
            else:
                await asyncio.wait_for(
                    self._semaphore.acquire(),
                    timeout=self.policy.acquire_timeout,
                )
        except (asyncio.TimeoutError, TimeoutError) as error:
            self.metrics.record_capacity_rejection(operation)
            raise ArtifactCapacityError(operation) from error
        finally:
            self.metrics.record_queue_wait(operation, started)

    def _backoff(self, retry_index: int) -> float:
        base = min(
            self.policy.initial_backoff * (2 ** max(0, retry_index - 1)),
            self.policy.max_backoff,
        )
        return base + random.uniform(0, base * self.policy.jitter)

    async def run(
        self,
        operation: str,
        factory: Callable[[], Awaitable[T]],
        *,
        timeout: float | None,
        retry: bool = True,
    ) -> T:
        attempts = 0
        while True:
            attempts += 1
            last_error: Exception
            await self._acquire(operation)
            self.metrics.operation_started()
            try:
                try:
                    awaitable = factory()
                    if timeout is None:
                        return await awaitable
                    return await asyncio.wait_for(awaitable, timeout=timeout)
                except (asyncio.TimeoutError, TimeoutError) as error:
                    last_error = error
                    self.metrics.record_timeout(operation)
                except Exception as error:
                    if not self.policy.is_retryable(error):
                        self.metrics.record_failure(operation)
                        raise
                    last_error = error
            finally:
                self.metrics.operation_finished()
                self._semaphore.release()

            if not retry or attempts > self.policy.max_retries:
                self.metrics.record_failure(operation)
                if isinstance(last_error, (asyncio.TimeoutError, TimeoutError)):
                    raise ArtifactTimeoutError(operation, attempts) from last_error
                raise ArtifactUnavailableError(operation, attempts) from last_error

            self.metrics.record_retry(operation)
            delay = self._backoff(attempts)
            if delay:
                await asyncio.sleep(delay)

    async def map(
        self,
        values: Sequence[T],
        function: Callable[[T], Awaitable[R]],
    ) -> list[R]:
        """Map with stable ordering and at most max_concurrency worker tasks."""

        if not values:
            return []

        results: list[R | None] = [None] * len(values)
        next_index = 0

        async def worker() -> None:
            nonlocal next_index
            while next_index < len(values):
                index = next_index
                next_index += 1
                results[index] = await function(values[index])

        workers = [
            asyncio.create_task(worker())
            for _ in range(min(len(values), self.policy.max_concurrency))
        ]
        try:
            await asyncio.gather(*workers)
        except BaseException:
            for worker_task in workers:
                worker_task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise
        return cast(list[R], results)
