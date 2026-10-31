from __future__ import annotations

import asyncio
import operator
from typing import Annotated, Any

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from langgraph_artifact_memory import (
    ArtifactIOPolicy,
    ArtifactMemory,
    ArtifactRef,
    Externalize,
    Hydrate,
    InMemoryArtifactStore,
)


class State(TypedDict, total=False):
    artifacts: Annotated[list[dict[str, Any] | ArtifactRef], operator.add]
    answer: str


async def main() -> None:
    memory = ArtifactMemory(
        InMemoryArtifactStore(),
        externalize={"artifacts": Externalize(itemized=True)},
        io_policy=ArtifactIOPolicy(max_concurrency=8),
        state_schema=State,
    )

    def answer(state: State) -> dict[str, str]:
        return {"answer": f"hydrated {len(state['artifacts'])} artifacts"}

    graph = (
        StateGraph(State)
        .add_node(
            "answer",
            memory.wrap(answer, hydrate={"artifacts": Hydrate(last_n=2)}),
        )
        .add_edge(START, "answer")
        .add_edge("answer", END)
        .compile(checkpointer=InMemorySaver())
    )

    result = await memory.ainvoke(
        graph,
        {"artifacts": [{"content": "A" * 4096}, {"content": "B" * 4096}]},
        {"configurable": {"thread_id": "example-thread"}},
    )
    assert result["answer"] == "hydrated 2 artifacts"
    assert all("artifact_key" in ref for ref in result["artifacts"])
    print(result["answer"])
    print(result["artifacts"])
    print("basic example passed")


if __name__ == "__main__":
    asyncio.run(main())
