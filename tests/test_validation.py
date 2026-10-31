from __future__ import annotations

import asyncio
import operator
from typing import Annotated, Any

import pytest
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict

from langgraph_artifact_memory import (
    ArtifactConfigurationError,
    ArtifactMemory,
    ArtifactRef,
    ArtifactStateTypeError,
    ArtifactValidationWarning,
    Externalize,
    Hydrate,
    InMemoryArtifactStore,
    MessageWindow,
    is_artifact_ref,
)


class ValidState(TypedDict, total=False):
    artifacts: Annotated[list[dict[str, Any] | ArtifactRef], operator.add]
    messages: Annotated[list[BaseMessage], add_messages]
    answer: str


class ScalarArtifactState(TypedDict, total=False):
    artifact: dict[str, Any] | ArtifactRef


class WrongItemizedShapeState(TypedDict, total=False):
    artifacts: str | ArtifactRef


class MissingReferenceState(TypedDict, total=False):
    artifacts: list[dict[str, Any]]


class WrongMessageState(TypedDict, total=False):
    messages: str


def make_memory(**kwargs: Any) -> ArtifactMemory:
    return ArtifactMemory(
        InMemoryArtifactStore(),
        externalize={"artifacts": Externalize(itemized=True)},
        state_schema=ValidState,
        **kwargs,
    )


def test_valid_state_schema_configuration_is_accepted() -> None:
    memory = make_memory(message_window=MessageWindow())

    memory.wrap(lambda state: {}, hydrate={"artifacts": Hydrate(last_n=2)})


def test_externalize_typo_has_channel_suggestion() -> None:
    with pytest.raises(ArtifactConfigurationError, match="Did you mean 'artifacts'"):
        ArtifactMemory(
            InMemoryArtifactStore(),
            externalize={"artfacts": Externalize()},
            state_schema=ValidState,
        )


def test_itemized_externalize_requires_list_channel() -> None:
    with pytest.raises(ArtifactConfigurationError, match=r"expected list\[\.\.\.\]"):
        ArtifactMemory(
            InMemoryArtifactStore(),
            externalize={"artifacts": Externalize(itemized=True)},
            state_schema=WrongItemizedShapeState,
        )


def test_itemized_channel_type_must_include_artifact_ref() -> None:
    with pytest.raises(ArtifactConfigurationError, match="does not include ArtifactRef"):
        ArtifactMemory(
            InMemoryArtifactStore(),
            externalize={"artifacts": Externalize(itemized=True)},
            state_schema=MissingReferenceState,
        )


def test_whole_value_externalize_requires_and_accepts_artifact_ref() -> None:
    ArtifactMemory(
        InMemoryArtifactStore(),
        externalize={"artifact": Externalize(itemized=False)},
        state_schema=ScalarArtifactState,
    )

    with pytest.raises(ArtifactConfigurationError, match="type does not include ArtifactRef"):
        ArtifactMemory(
            InMemoryArtifactStore(),
            externalize={"answer": Externalize(itemized=False)},
            state_schema=ValidState,
        )


def test_message_window_requires_list_channel() -> None:
    with pytest.raises(ArtifactConfigurationError, match=r"must be annotated as list\[\.\.\.\]"):
        ArtifactMemory(
            InMemoryArtifactStore(),
            externalize={},
            message_window=MessageWindow(),
            state_schema=WrongMessageState,
        )


def test_hydrate_typo_is_rejected_when_wrapping_node() -> None:
    memory = make_memory()

    with pytest.raises(ArtifactConfigurationError, match="Did you mean 'artifacts'"):
        memory.wrap(lambda state: {}, hydrate={"artfacts": Hydrate()})


def test_direct_hydrate_typo_is_rejected() -> None:
    async def scenario() -> None:
        memory = make_memory()
        with pytest.raises(ArtifactConfigurationError, match="unknown state channel"):
            await memory.hydrate_state({}, {"missing": Hydrate()})

    asyncio.run(scenario())


def test_runtime_input_shape_is_rejected_before_store_write() -> None:
    async def scenario() -> None:
        memory = make_memory()
        with pytest.raises(ArtifactStateTypeError, match="received str; expected list"):
            await memory.prepare_input(
                {"artifacts": "not-a-list"},
                {"configurable": {"thread_id": "validation"}},
            )
        assert memory.metrics.snapshot()["counts"].get("put", 0) == 0

    asyncio.run(scenario())


def test_runtime_node_update_shape_is_rejected() -> None:
    async def scenario() -> None:
        memory = make_memory()
        wrapped = memory.wrap(lambda state: {"artifacts": "not-a-list"})

        with pytest.raises(ArtifactStateTypeError, match="received str; expected list"):
            await wrapped({}, {"configurable": {"thread_id": "validation"}})

    asyncio.run(scenario())


def test_warn_mode_reports_problem_and_preserves_legacy_behavior() -> None:
    async def scenario() -> None:
        with pytest.warns(ArtifactValidationWarning, match="unknown state channel"):
            memory = ArtifactMemory(
                InMemoryArtifactStore(),
                externalize={"artfacts": Externalize()},
                state_schema=ValidState,
                validation="warn",
            )

        with pytest.warns(ArtifactValidationWarning, match="received str; expected list"):
            prepared = await memory.prepare_input(
                {"artfacts": "legacy-scalar"},
                {"configurable": {"thread_id": "warn"}},
            )
        assert is_artifact_ref(prepared["artfacts"])

    asyncio.run(scenario())


def test_off_mode_skips_static_and_runtime_validation() -> None:
    async def scenario() -> None:
        memory = ArtifactMemory(
            InMemoryArtifactStore(),
            externalize={"artfacts": Externalize()},
            state_schema=ValidState,
            validation="off",
        )
        prepared = await memory.prepare_input(
            {"artfacts": "legacy-scalar"},
            {"configurable": {"thread_id": "off"}},
        )
        assert is_artifact_ref(prepared["artfacts"])

    asyncio.run(scenario())


def test_invalid_validation_mode_is_rejected() -> None:
    with pytest.raises(ValueError, match="validation must be"):
        ArtifactMemory(
            InMemoryArtifactStore(),
            externalize={},
            validation="unknown",  # type: ignore[arg-type]
        )
