from __future__ import annotations

import asyncio
from typing import Annotated, Any

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict

from langgraph_artifact_memory import ArtifactMemory, InMemoryArtifactStore, MessageWindow


class State(TypedDict, total=False):
    messages: Annotated[list[BaseMessage], add_messages]


async def main() -> None:
    policy = MessageWindow(max_messages=5, max_kept_messages=3)
    memory = ArtifactMemory(
        InMemoryArtifactStore(),
        externalize={},
        message_window=policy,
        state_schema=State,
    )
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
    config = {"configurable": {"thread_id": "long-conversation-example"}}

    for turn in range(100):
        message = HumanMessage(
            content=f"human-{turn}",
            additional_kwargs={"keep": turn % 25 == 0},
        )
        await memory.ainvoke(graph, {"messages": [message]}, config)

    state = await graph.aget_state(config)
    retained = state.values["messages"]
    archived = await memory.aget_archive("long-conversation-example")

    assert len(retained) == 8
    assert sum(policy.is_kept(message) for message in retained) == 3
    assert len(archived) == 192
    print(f"state={len(retained)}, archive={len(archived)}, total=200")
    print("long conversation example passed")


if __name__ == "__main__":
    asyncio.run(main())
