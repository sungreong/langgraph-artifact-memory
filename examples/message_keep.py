from __future__ import annotations

import asyncio
from typing import Annotated, Any

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
    InMemoryArtifactStore,
    MessageWindow,
    mark_message_kept,
)


class State(TypedDict, total=False):
    messages: Annotated[list[BaseMessage], add_messages]


async def main() -> None:
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
    config = {"configurable": {"thread_id": "message-keep-example"}}
    preference = mark_message_kept(HumanMessage(content="Always answer in Korean"))

    inputs = [
        SystemMessage(content="System rules"),
        preference,
        ToolMessage(content="Large tool result summary", tool_call_id="tool-1"),
        HumanMessage(content="Latest question"),
    ]
    for message in inputs:
        await memory.ainvoke(graph, {"messages": [message]}, config)

    state = await graph.aget_state(config)
    retained = state.values["messages"]
    archived = await memory.aget_archive("message-keep-example")

    assert [message.content for message in retained] == [
        "System rules",
        "Always answer in Korean",
        "Latest question",
        "answer-4",
    ]
    assert len(archived) == 4
    print("retained:", [message.content for message in retained])
    print("archived:", [message.content for message in archived])
    print("message keep example passed")


if __name__ == "__main__":
    asyncio.run(main())
