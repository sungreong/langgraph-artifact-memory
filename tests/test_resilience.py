from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from langgraph_artifact_memory import (
    ArtifactCapacityError,
    ArtifactIOPolicy,
    ArtifactMemory,
    ArtifactTimeoutError,
    ArtifactUnavailableError,
    Externalize,
    Hydrate,
    InMemoryArtifactStore,
    MessageWindow,
)
from langgraph_artifact_memory.models import ArtifactRef


class ControlledStore(InMemoryArtifactStore):
    def __init__(
        self,
        *,
        delay: float = 0.0,
        fail_puts: int = 0,
        put_error: type[Exception] = ConnectionError,
    ) -> None:
        super().__init__()
        self.delay = delay
        self.fail_puts = fail_puts
        self.put_error = put_error
        self.put_calls = 0
        self.active = 0
        self.max_active = 0

    async def _enter(self) -> None:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        if self.delay:
            await asyncio.sleep(self.delay)

    def _exit(self) -> None:
        self.active -= 1

    async def aput(self, scope: str, channel: str, value: Any) -> ArtifactRef:
        self.put_calls += 1
        await self._enter()
        try:
            if self.fail_puts:
                self.fail_puts -= 1
                raise self.put_error("controlled put failure")
            return await super().aput(scope, channel, value)
        finally:
            self._exit()

    async def aget(self, ref: ArtifactRef) -> Any:
        await self._enter()
        try:
            return await super().aget(ref)
        finally:
            self._exit()


class FailingArchiveStore(InMemoryArtifactStore):
    def __init__(self) -> None:
        super().__init__()
        self.append_calls = 0

    async def aappend_archive(
        self,
        scope: str,
        channel: str,
        refs: Sequence[ArtifactRef],
    ) -> None:
        self.append_calls += 1
        raise ConnectionError("ambiguous archive append")


def test_bounded_parallel_io_preserves_input_order() -> None:
    async def scenario() -> None:
        store = ControlledStore(delay=0.01)
        memory = ArtifactMemory(
            store,
            externalize={"documents": Externalize()},
            io_policy=ArtifactIOPolicy(
                max_concurrency=3,
                max_retries=0,
                initial_backoff=0,
                max_backoff=0,
                jitter=0,
            ),
        )
        config = {"configurable": {"thread_id": "parallel"}}
        documents = [{"index": index} for index in range(9)]

        prepared = await memory.prepare_input({"documents": documents}, config)
        hydrated = await memory.hydrate_state(
            prepared,
            {"documents": Hydrate()},
        )

        assert hydrated["documents"] == documents
        assert store.max_active == 3
        assert memory.metrics.snapshot()["concurrency"]["max_in_flight"] == 3

    asyncio.run(scenario())


def test_transient_failure_is_retried_then_succeeds() -> None:
    async def scenario() -> None:
        store = ControlledStore(fail_puts=2)
        memory = ArtifactMemory(
            store,
            externalize={"documents": Externalize()},
            io_policy=ArtifactIOPolicy(
                max_concurrency=1,
                max_retries=2,
                initial_backoff=0,
                max_backoff=0,
                jitter=0,
            ),
        )

        prepared = await memory.prepare_input(
            {"documents": ["eventually-stored"]},
            {"configurable": {"thread_id": "retry"}},
        )
        metrics = memory.metrics.snapshot()

        assert len(prepared["documents"]) == 1
        assert store.put_calls == 3
        assert metrics["counts"]["put"] == 1
        assert metrics["retries"]["put"] == 2
        assert metrics["failures"] == {}

    asyncio.run(scenario())


def test_timeout_is_retried_and_raised_as_artifact_error() -> None:
    async def scenario() -> None:
        store = ControlledStore(delay=0.05)
        memory = ArtifactMemory(
            store,
            externalize={"documents": Externalize()},
            io_policy=ArtifactIOPolicy(
                max_concurrency=1,
                put_timeout=0.005,
                max_retries=1,
                initial_backoff=0,
                max_backoff=0,
                jitter=0,
            ),
        )

        with pytest.raises(ArtifactTimeoutError) as captured:
            await memory.prepare_input(
                {"documents": ["too-slow"]},
                {"configurable": {"thread_id": "timeout"}},
            )

        metrics = memory.metrics.snapshot()
        assert captured.value.operation == "put"
        assert captured.value.attempts == 2
        assert store.put_calls == 2
        assert metrics["retries"]["put"] == 1
        assert metrics["timeouts"]["put"] == 2
        assert metrics["failures"]["put"] == 1
        assert metrics["concurrency"]["in_flight"] == 0

    asyncio.run(scenario())


def test_non_transient_failure_is_not_retried() -> None:
    async def scenario() -> None:
        store = ControlledStore(fail_puts=1, put_error=ValueError)
        memory = ArtifactMemory(
            store,
            externalize={"documents": Externalize()},
            io_policy=ArtifactIOPolicy(
                max_retries=5,
                initial_backoff=0,
                max_backoff=0,
                jitter=0,
            ),
        )

        with pytest.raises(ValueError, match="controlled"):
            await memory.prepare_input(
                {"documents": ["invalid"]},
                {"configurable": {"thread_id": "invalid"}},
            )

        metrics = memory.metrics.snapshot()
        assert store.put_calls == 1
        assert metrics["retries"] == {}
        assert metrics["failures"]["put"] == 1

    asyncio.run(scenario())


def test_archive_append_is_not_retried_automatically() -> None:
    async def scenario() -> None:
        store = FailingArchiveStore()
        memory = ArtifactMemory(
            store,
            externalize={},
            message_window=MessageWindow(max_messages=1),
            io_policy=ArtifactIOPolicy(
                max_retries=5,
                initial_backoff=0,
                max_backoff=0,
                jitter=0,
            ),
        )

        wrapped = memory.wrap(lambda state: {"messages": [AIMessage(content="new")]})
        with pytest.raises(ArtifactUnavailableError) as captured:
            await wrapped(
                {"messages": [HumanMessage(content="old")]},
                {"configurable": {"thread_id": "archive"}},
            )

        assert captured.value.operation == "archive_append"
        assert captured.value.attempts == 1
        assert store.append_calls == 1
        assert memory.metrics.snapshot()["retries"] == {}

    asyncio.run(scenario())


def test_acquire_timeout_applies_backpressure() -> None:
    async def scenario() -> None:
        store = ControlledStore(delay=0.05)
        memory = ArtifactMemory(
            store,
            externalize={"documents": Externalize()},
            io_policy=ArtifactIOPolicy(
                max_concurrency=1,
                acquire_timeout=0.005,
                put_timeout=1,
                max_retries=0,
            ),
        )
        config = {"configurable": {"thread_id": "capacity"}}

        first = asyncio.create_task(memory.prepare_input({"documents": ["first"]}, config))
        await asyncio.sleep(0)
        second = asyncio.create_task(memory.prepare_input({"documents": ["second"]}, config))
        results = await asyncio.gather(first, second, return_exceptions=True)

        assert sum(isinstance(result, ArtifactCapacityError) for result in results) == 1
        assert memory.metrics.snapshot()["capacity_rejections"]["put"] == 1

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_concurrency", 0),
        ("put_timeout", 0),
        ("max_retries", -1),
        ("initial_backoff", -0.1),
        ("jitter", -0.1),
    ],
)
def test_io_policy_rejects_invalid_values(field: str, value: Any) -> None:
    with pytest.raises(ValueError):
        ArtifactIOPolicy(**{field: value})
