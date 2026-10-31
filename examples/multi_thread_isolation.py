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
    memory = ArtifactMemory(
        InMemoryArtifactStore(),
        externalize={},
        message_window=MessageWindow(max_messages=2, max_kept_messages=1),
        state_schema=State,
    )

    def answer(state: State) -> dict[str, Any]:
        latest = state["messages"][-1]
        return {"messages": [AIMessage(content=f"reply:{latest.content}")]}

    graph = (
        StateGraph(State)
        .add_node("answer", memory.wrap(answer))
        .add_edge(START, "answer")
        .add_edge("answer", END)
        .compile(checkpointer=InMemorySaver())
    )

    for user in ("alice", "bob"):
        config = {"configurable": {"thread_id": user}}
        for turn in range(3):
            await memory.ainvoke(
                graph,
                {
                    "messages": [
                        HumanMessage(
                            content=f"{user}-{turn}",
                            additional_kwargs={"keep": turn == 0},
                        )
                    ]
                },
                config,
            )

    alice_state = await graph.aget_state({"configurable": {"thread_id": "alice"}})
    bob_state = await graph.aget_state({"configurable": {"thread_id": "bob"}})
    alice_archive = await memory.aget_archive("alice")
    bob_archive = await memory.aget_archive("bob")

    assert all("alice" in str(message.content) for message in alice_state.values["messages"])
    assert all("bob" in str(message.content) for message in bob_state.values["messages"])
    assert all("alice" in str(message.content) for message in alice_archive)
    assert all("bob" in str(message.content) for message in bob_archive)
    print(f"alice archive={len(alice_archive)}, bob archive={len(bob_archive)}")
    print("multi-thread isolation example passed")


if __name__ == "__main__":
    asyncio.run(main())
