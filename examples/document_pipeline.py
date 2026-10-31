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
    query: str
    documents: Annotated[list[Any], operator.add]
    analysis: str
    answer: str


async def main() -> None:
    memory = ArtifactMemory(
        InMemoryArtifactStore(),
        externalize={"documents": Externalize(itemized=True)},
    )

    def collect(state: State) -> dict[str, Any]:
        return {
            "documents": [
                {"title": f"source-{index}", "content": state["query"] * 1000} for index in range(3)
            ]
        }

    def analyze(state: State) -> dict[str, str]:
        titles = [document["title"] for document in state["documents"]]
        return {"analysis": f"analyzed: {','.join(titles)}"}

    def answer(state: State) -> dict[str, str]:
        return {"answer": f"answer from {state['analysis']}"}

    graph = (
        StateGraph(State)
        .add_node("collect", memory.wrap(collect))
        .add_node(
            "analyze",
            memory.wrap(analyze, hydrate={"documents": Hydrate(last_n=2)}),
        )
        .add_node("answer", answer)
        .add_edge(START, "collect")
        .add_edge("collect", "analyze")
        .add_edge("analyze", "answer")
        .add_edge("answer", END)
        .compile(checkpointer=InMemorySaver())
    )
    config = {"configurable": {"thread_id": "document-pipeline-example"}}
    result = await memory.ainvoke(graph, {"query": "artifact-memory"}, config)

    assert result["analysis"] == "analyzed: source-1,source-2"
    assert result["answer"].endswith("source-1,source-2")
    assert all(is_artifact_ref(document) for document in result["documents"])
    print(result["answer"])
    print("document pipeline example passed")


if __name__ == "__main__":
    asyncio.run(main())
