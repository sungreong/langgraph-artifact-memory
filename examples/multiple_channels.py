from __future__ import annotations

import asyncio
import operator
from typing import Annotated, Any

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from langgraph_artifact_memory import (
    ArtifactMemory,
    Externalize,
    Hydrate,
    InMemoryArtifactStore,
    is_artifact_ref,
)


class State(TypedDict, total=False):
    documents: Annotated[list[Any], operator.add]
    tool_results: Annotated[list[Any], operator.add]
    summary: str


async def main() -> None:
    memory = ArtifactMemory(
        InMemoryArtifactStore(),
        externalize={
            "documents": Externalize(itemized=True),
            "tool_results": Externalize(itemized=True),
        },
    )

    def analyze(state: State) -> dict[str, str]:
        return {
            "summary": (
                f"documents={len(state['documents'])}, tool_results={len(state['tool_results'])}"
            )
        }

    graph = (
        StateGraph(State)
        .add_node(
            "analyze",
            memory.wrap(
                analyze,
                hydrate={
                    "documents": Hydrate(last_n=2, max_bytes=20_000),
                    "tool_results": Hydrate(last_n=1, max_bytes=20_000),
                },
            ),
        )
        .add_edge(START, "analyze")
        .add_edge("analyze", END)
        .compile(checkpointer=InMemorySaver())
    )
    config = {"configurable": {"thread_id": "multiple-channels-example"}}

    for turn in range(3):
        result = await memory.ainvoke(
            graph,
            {
                "documents": [{"turn": turn, "text": "document" * 500}],
                "tool_results": [{"turn": turn, "result": "tool" * 500}],
            },
            config,
        )

    state = await graph.aget_state(config)
    assert result["summary"] == "documents=2, tool_results=1"
    assert all(is_artifact_ref(value) for value in state.values["documents"])
    assert all(is_artifact_ref(value) for value in state.values["tool_results"])
    print(result["summary"])
    print("multiple channels example passed")


if __name__ == "__main__":
    asyncio.run(main())
