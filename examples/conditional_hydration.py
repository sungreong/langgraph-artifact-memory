from __future__ import annotations

import asyncio
import operator
from typing import Annotated, Any, Literal

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from langgraph_artifact_memory import (
    ArtifactMemory,
    Externalize,
    Hydrate,
    InMemoryArtifactStore,
)


class State(TypedDict, total=False):
    mode: Literal["quick", "deep"]
    documents: Annotated[list[Any], operator.add]
    answer: str


async def main() -> None:
    memory = ArtifactMemory(
        InMemoryArtifactStore(),
        externalize={"documents": Externalize(itemized=True)},
    )

    def route(state: State) -> Literal["quick", "deep"]:
        return state["mode"]

    def answer(state: State) -> dict[str, str]:
        return {"answer": f"used {len(state['documents'])} documents"}

    graph = (
        StateGraph(State)
        .add_node(
            "quick",
            memory.wrap(answer, hydrate={"documents": Hydrate(last_n=1)}),
        )
        .add_node(
            "deep",
            memory.wrap(answer, hydrate={"documents": Hydrate(last_n=3)}),
        )
        .add_conditional_edges(START, route, ["quick", "deep"])
        .add_edge("quick", END)
        .add_edge("deep", END)
        .compile(checkpointer=InMemorySaver())
    )
    documents = [{"id": index, "content": "document" * 1000} for index in range(5)]

    quick = await memory.ainvoke(
        graph,
        {"mode": "quick", "documents": documents},
        {"configurable": {"thread_id": "quick-thread"}},
    )
    deep = await memory.ainvoke(
        graph,
        {"mode": "deep", "documents": documents},
        {"configurable": {"thread_id": "deep-thread"}},
    )

    assert quick["answer"] == "used 1 documents"
    assert deep["answer"] == "used 3 documents"
    print(quick["answer"], "/", deep["answer"])
    print("conditional hydration example passed")


if __name__ == "__main__":
    asyncio.run(main())
