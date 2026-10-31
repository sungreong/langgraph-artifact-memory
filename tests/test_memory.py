from __future__ import annotations

import asyncio
import operator
from typing import Annotated, Any

import pytest
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
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
            "get": 9,
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
