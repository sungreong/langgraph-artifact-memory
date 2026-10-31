from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping, Sequence
from time import perf_counter
from typing import Any, TypeVar, cast

from langchain_core.runnables import RunnableConfig
from langgraph.graph.message import add_messages
from langgraph.types import Overwrite

from langgraph_artifact_memory.metrics import ArtifactMemoryMetrics
from langgraph_artifact_memory.models import (
    ArtifactRef,
    Externalize,
    Hydrate,
    MessageWindow,
    is_artifact_ref,
)
from langgraph_artifact_memory.resilience import ArtifactIOExecutor, ArtifactIOPolicy
from langgraph_artifact_memory.stores.base import ArtifactStore
from langgraph_artifact_memory.validation import StateSchemaValidator, ValidationMode

NodeResult = Mapping[str, Any]
Node = Callable[..., NodeResult | Awaitable[NodeResult]]
T = TypeVar("T")


class ArtifactMemory:
    """Externalize heavy LangGraph values and hydrate them only where needed."""

    def __init__(
        self,
        store: ArtifactStore,
        *,
        externalize: Mapping[str, Externalize],
        message_window: MessageWindow | None = None,
        io_policy: ArtifactIOPolicy | None = None,
        state_schema: type[Any] | None = None,
        validation: ValidationMode = "strict",
    ) -> None:
        self.store = store
        self.externalize = dict(externalize)
        self.message_window = message_window
        self.io_policy = io_policy or ArtifactIOPolicy()
        self.state_schema = state_schema
        self.validation = validation
        self._validator = StateSchemaValidator(state_schema, validation)
        self._validator.validate_configuration(self.externalize, self.message_window)
        self.metrics = ArtifactMemoryMetrics()
        self._io = ArtifactIOExecutor(self.io_policy, self.metrics)

    @staticmethod
    def _thread_id(config: RunnableConfig) -> str:
        thread_id = config.get("configurable", {}).get("thread_id")
        if not thread_id:
            raise ValueError("ArtifactMemory requires configurable.thread_id")
        return str(thread_id)

    async def _put(self, scope: str, channel: str, value: Any) -> ArtifactRef:
        started = perf_counter()
        ref = await self._io.run(
            "put",
            lambda: self.store.aput(scope, channel, value),
            timeout=self.io_policy.put_timeout,
        )
        self.metrics.record("put", started, size_bytes=ref["size_bytes"])
        return ref

    async def _get(self, ref: ArtifactRef) -> Any:
        started = perf_counter()
        value = await self._io.run(
            "get",
            lambda: self.store.aget(ref),
            timeout=self.io_policy.get_timeout,
        )
        self.metrics.record("get", started, size_bytes=ref["size_bytes"])
        return value

    async def _externalize_value(
        self, scope: str, channel: str, value: Any, policy: Externalize
    ) -> Any:
        self._validator.validate_externalized_value(channel, value, policy)
        if policy.itemized and isinstance(value, list):

            async def externalize_item(item: Any) -> Any:
                if is_artifact_ref(item):
                    return item
                return await self._put(scope, channel, item)

            return await self._io.map(value, externalize_item)
        if is_artifact_ref(value):
            return value
        return await self._put(scope, channel, value)

    async def prepare_input(
        self, state: Mapping[str, Any], config: RunnableConfig
    ) -> dict[str, Any]:
        """Replace configured input values before the first graph checkpoint."""

        scope = self._thread_id(config)
        prepared = dict(state)
        for channel, policy in self.externalize.items():
            if channel in prepared:
                prepared[channel] = await self._externalize_value(
                    scope, channel, prepared[channel], policy
                )
        return prepared

    @staticmethod
    def _select(values: Sequence[Any], spec: Hydrate) -> list[Any]:
        selected = list(values[-spec.last_n :] if spec.last_n is not None else values)
        if spec.max_bytes is None:
            return selected
        kept_reversed: list[Any] = []
        total = 0
        for value in reversed(selected):
            size = value["size_bytes"] if is_artifact_ref(value) else 0
            if total + size > spec.max_bytes:
                continue
            kept_reversed.append(value)
            total += size
        return list(reversed(kept_reversed))

    async def _hydrate_value(self, value: Any, spec: Hydrate) -> Any:
        values = self._select(value, spec) if isinstance(value, list) else [value]

        async def hydrate_item(item: Any) -> Any:
            if not is_artifact_ref(item):
                return item
            return await self._get(item)

        hydrated = await self._io.map(values, hydrate_item)
        return hydrated if isinstance(value, list) else hydrated[0]

    async def _hydrate_state(
        self, state: Mapping[str, Any], hydrate: Mapping[str, Hydrate]
    ) -> dict[str, Any]:
        result = dict(state)
        for channel, spec in hydrate.items():
            if channel in result:
                result[channel] = await self._hydrate_value(result[channel], spec)
        return result

    async def hydrate_state(
        self, state: Mapping[str, Any], hydrate: Mapping[str, Hydrate]
    ) -> dict[str, Any]:
        """Return a node-local state copy with selected references resolved."""

        hydrate = dict(hydrate)
        self._validator.validate_hydrate(hydrate)
        return await self._hydrate_state(state, hydrate)

    async def _apply_message_window(
        self, scope: str, state: Mapping[str, Any], updates: dict[str, Any]
    ) -> None:
        policy = self.message_window
        if policy is None or policy.key not in updates:
            return
        current = list(state.get(policy.key, []))
        combined = list(add_messages(current, updates[policy.key]))
        retained, evicted = policy.select(combined)
        if evicted:
            refs = await self._io.map(
                evicted,
                lambda message: self._put(scope, policy.archive_channel, message),
            )
            await self._io.run(
                "archive_append",
                lambda: self.store.aappend_archive(scope, policy.archive_channel, refs),
                timeout=self.io_policy.archive_timeout,
                retry=False,
            )
            self.metrics.counts["archive"] += len(refs)
        updates[policy.key] = Overwrite(retained)

    def wrap(
        self,
        node: Node,
        *,
        hydrate: Mapping[str, Hydrate] | None = None,
    ) -> Callable[[Mapping[str, Any], RunnableConfig], Awaitable[dict[str, Any]]]:
        """Wrap a state-only or state-plus-config node with artifact hydration."""

        hydrate = dict(hydrate or {})
        self._validator.validate_hydrate(hydrate)
        parameter_count = len(inspect.signature(node).parameters)

        async def wrapped(state: Mapping[str, Any], config: RunnableConfig) -> dict[str, Any]:
            scope = self._thread_id(config)
            hydrated = await self._hydrate_state(state, hydrate)
            value = node(hydrated, config) if parameter_count >= 2 else node(hydrated)
            result = await value if inspect.isawaitable(value) else value
            updates = dict(cast(NodeResult, result))
            for channel, policy in self.externalize.items():
                if channel in updates:
                    updates[channel] = await self._externalize_value(
                        scope, channel, updates[channel], policy
                    )
            await self._apply_message_window(scope, state, updates)
            return updates

        return wrapped

    async def ainvoke(
        self,
        graph: Any,
        state: Mapping[str, Any],
        config: RunnableConfig,
        **kwargs: Any,
    ) -> Any:
        """Externalize input before invoking a compiled LangGraph."""

        prepared = await self.prepare_input(state, config)
        return await graph.ainvoke(prepared, config, **kwargs)

    async def aget(self, ref: ArtifactRef) -> Any:
        return await self._get(ref)

    async def aget_archive(self, thread_id: str, *, limit: int | None = None) -> list[Any]:
        if self.message_window is None:
            return []
        refs = await self._io.run(
            "archive_list",
            lambda: self.store.alist_archive(
                thread_id,
                self.message_window.archive_channel,
                limit=limit,
            ),
            timeout=self.io_policy.archive_timeout,
        )
        return await self._io.map(refs, self._get)
