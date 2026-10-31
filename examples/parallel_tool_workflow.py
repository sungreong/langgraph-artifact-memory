from __future__ import annotations

import asyncio
import operator
from typing import Annotated, Any

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Send
from typing_extensions import TypedDict

from langgraph_artifact_memory import (
    ArtifactMemory,
    Externalize,
    Hydrate,
    InMemoryArtifactStore,
    is_artifact_ref,
)


class State(TypedDict, total=False):
    queries: list[str]
    query: str
    tool_results: Annotated[list[Any], operator.add]
    answer: str


async def main() -> None:
    memory = ArtifactMemory(
        InMemoryArtifactStore(),
        externalize={"tool_results": Externalize(itemized=True)},
        state_schema=State,
    )

    def fan_out(state: State) -> list[Send]:
        return [Send("tool", {"query": query}) for query in state["queries"]]

    async def tool(state: State) -> dict[str, Any]:
        await asyncio.sleep(0.001)
        return {"tool_results": [{"query": state["query"], "payload": state["query"] * 1000}]}

    def synthesize(state: State) -> dict[str, str]:
        queries = sorted(result["query"] for result in state["tool_results"])
        return {"answer": f"combined: {','.join(queries)}"}

    graph = (
        StateGraph(State)
        .add_node("tool", memory.wrap(tool))
        .add_node(
            "synthesize",
            memory.wrap(
                synthesize,
                hydrate={"tool_results": Hydrate(last_n=3, max_bytes=100_000)},
            ),
        )
        .add_conditional_edges(START, fan_out, ["tool"])
        .add_edge("tool", "synthesize")
        .add_edge("synthesize", END)
        .compile(checkpointer=InMemorySaver())
    )
    config = {"configurable": {"thread_id": "parallel-tool-example"}}
    result = await memory.ainvoke(
        graph,
        {"queries": ["weather", "price", "inventory"]},
        config,
    )

    assert result["answer"] == "combined: inventory,price,weather"
    assert len(result["tool_results"]) == 3
    assert all(is_artifact_ref(value) for value in result["tool_results"])
    print(result["answer"])
    print("parallel tool workflow example passed")


if __name__ == "__main__":
    asyncio.run(main())
