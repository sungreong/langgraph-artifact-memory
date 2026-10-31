from __future__ import annotations

import asyncio
import operator
from typing import Annotated, Any

import pytest
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict

from langgraph_artifact_memory import (
    ArtifactMemory,
    Externalize,
    Hydrate,
    InMemoryArtifactStore,
    MessageWindow,
    is_artifact_ref,
    mark_message_kept,
)


class State(TypedDict, total=False):
    artifacts: Annotated[list[Any], operator.add]
    messages: Annotated[list[BaseMessage], add_messages]
    retrieved: list[str]


def test_artifact_memory_externalizes_hydrates_and_archives_messages() -> None:
    async def scenario() -> None:
        store = InMemoryArtifactStore()
        memory = ArtifactMemory(
            store,
            externalize={"artifacts": Externalize(itemized=True)},
            message_window=MessageWindow(max_messages=4),
        )

        def answer(state: State) -> dict[str, Any]:
            return {
                "retrieved": [item["content"] for item in state["artifacts"]],
                "messages": [AIMessage(content="ok")],
            }

        graph = (
            StateGraph(State)
            .add_node(
                "answer",
                memory.wrap(
                    answer,
                    hydrate={"artifacts": Hydrate(last_n=2, max_bytes=10_000)},
                ),
            )
            .add_edge(START, "answer")
            .add_edge("answer", END)
            .compile(checkpointer=InMemorySaver())
        )
        config = {"configurable": {"thread_id": "artifact-thread"}}

        for turn in range(5):
            result = await memory.ainvoke(
                graph,
                {
                    "artifacts": [{"content": f"document-{turn}" * 100}],
                    "messages": [HumanMessage(content=f"question-{turn}")],
                },
                config,
            )

        state = await graph.aget_state(config)
        refs = state.values["artifacts"]
        archived = await memory.aget_archive("artifact-thread")

        assert len(refs) == 5
        assert all(is_artifact_ref(ref) for ref in refs)
        assert len(result["retrieved"]) == 2
        assert result["retrieved"][-1].startswith("document-4")
        assert len(state.values["messages"]) == 4
        assert len(archived) == 6
        assert memory.metrics.snapshot()["counts"] == {
            "put": 11,
            "get": 15,
            "archive": 6,
        }

    asyncio.run(scenario())


def test_artifact_memory_requires_thread_id() -> None:
    async def scenario() -> None:
        memory = ArtifactMemory(
            InMemoryArtifactStore(),
            externalize={"artifacts": Externalize()},
        )

        with pytest.raises(ValueError, match="thread_id"):
            await memory.prepare_input({"artifacts": ["large"]}, {})

    asyncio.run(scenario())


def test_message_window_keeps_selected_messages_and_archives_other_types() -> None:
    async def scenario() -> None:
        store = InMemoryArtifactStore()
        memory = ArtifactMemory(
            store,
            externalize={},
            message_window=MessageWindow(
                max_messages=2,
                keep_message_types=("system",),
                max_kept_messages=2,
            ),
        )
        answer_count = 0

        def answer(state: State) -> dict[str, Any]:
            nonlocal answer_count
            answer_count += 1
            return {"messages": [AIMessage(content=f"answer-{answer_count}")]}

        graph = (
            StateGraph(State)
            .add_node("answer", memory.wrap(answer))
            .add_edge(START, "answer")
            .add_edge("answer", END)
            .compile(checkpointer=InMemorySaver())
        )
        config = {"configurable": {"thread_id": "kept-messages"}}

        await memory.ainvoke(
            graph,
            {"messages": [SystemMessage(content="system-rules")]},
            config,
        )
        await memory.ainvoke(
            graph,
            {
                "messages": [
                    HumanMessage(
                        content="user-preference",
                        additional_kwargs={"keep": True},
                    )
                ]
            },
            config,
        )
        await memory.ainvoke(
            graph,
            {"messages": [ToolMessage(content="tool-result", tool_call_id="tool-call-1")]},
            config,
        )
        await memory.ainvoke(
            graph,
            {"messages": [HumanMessage(content="latest-question")]},
            config,
        )

        state = await graph.aget_state(config)
        archived = await memory.aget_archive("kept-messages")

        assert [message.content for message in state.values["messages"]] == [
            "system-rules",
            "user-preference",
            "latest-question",
            "answer-4",
        ]
        assert [message.content for message in archived] == [
            "answer-1",
            "answer-2",
            "tool-result",
            "answer-3",
        ]
        assert {message.type for message in archived} == {"ai", "tool"}

    asyncio.run(scenario())


def test_message_window_bounds_kept_messages_and_archives_keep_overflow() -> None:
    async def scenario() -> None:
        store = InMemoryArtifactStore()
        policy = MessageWindow(
            max_messages=1,
            keep_message_types=("system",),
            max_kept_messages=1,
        )
        memory = ArtifactMemory(store, externalize={}, message_window=policy)

        graph = (
            StateGraph(State)
            .add_node(
                "answer",
                memory.wrap(lambda state: {"messages": [AIMessage(content="answer")]}),
            )
            .add_edge(START, "answer")
            .add_edge("answer", END)
            .compile(checkpointer=InMemorySaver())
        )
        config = {"configurable": {"thread_id": "keep-overflow"}}

        await memory.ainvoke(
            graph,
            {
                "messages": [
                    SystemMessage(content="old-system"),
                    SystemMessage(content="new-system"),
                ]
            },
            config,
        )

        state = await graph.aget_state(config)
        archived = await memory.aget_archive("keep-overflow")

        assert [message.content for message in state.values["messages"]] == [
            "new-system",
            "answer",
        ]
        assert [message.content for message in archived] == ["old-system"]

    asyncio.run(scenario())


def test_message_window_keep_policy_validation_and_detection() -> None:
    policy = MessageWindow(keep_message_types=("system",))
    original = HumanMessage(
        id="preference-1",
        content="preference",
        additional_kwargs={"source": "user"},
    )
    kept = mark_message_kept(original)
    unkept = mark_message_kept(kept, kept=False)

    assert policy.is_kept(SystemMessage(content="system"))
    assert policy.is_kept(kept)
    assert not policy.is_kept(unkept)
    assert kept.id == original.id
    assert kept.additional_kwargs == {"source": "user", "keep": True}
    assert original.additional_kwargs == {"source": "user"}

    with pytest.raises(ValueError, match="max_kept_messages"):
        MessageWindow(max_kept_messages=0)
    with pytest.raises(ValueError, match="keep_metadata_key"):
        MessageWindow(keep_metadata_key="")
    with pytest.raises(ValueError, match="metadata_key"):
        mark_message_kept(original, metadata_key="")


def test_message_window_stays_bounded_across_one_hundred_turns() -> None:
    async def scenario() -> None:
        store = InMemoryArtifactStore()
        policy = MessageWindow(max_messages=5, max_kept_messages=3)
        memory = ArtifactMemory(store, externalize={}, message_window=policy)
        answer_count = 0

        def answer(state: State) -> dict[str, Any]:
            nonlocal answer_count
            message = AIMessage(content=f"ai-{answer_count}")
            answer_count += 1
            return {"messages": [message]}

        graph = (
            StateGraph(State)
            .add_node("answer", memory.wrap(answer))
            .add_edge(START, "answer")
            .add_edge("answer", END)
            .compile(checkpointer=InMemorySaver())
        )
        config = {"configurable": {"thread_id": "one-hundred-turns"}}

        for turn in range(100):
            message = HumanMessage(
                content=f"human-{turn}",
                additional_kwargs={"keep": turn % 25 == 0},
            )
            await memory.ainvoke(graph, {"messages": [message]}, config)

        state = await graph.aget_state(config)
        archived = await memory.aget_archive("one-hundred-turns")
        retained = state.values["messages"]

        assert len(retained) == 8
        assert sum(policy.is_kept(message) for message in retained) == 3
        assert len(archived) == 192
        assert len(retained) + len(archived) == 200

    asyncio.run(scenario())
