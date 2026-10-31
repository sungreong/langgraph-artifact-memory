from __future__ import annotations

import asyncio
import operator
from typing import Annotated, Any

from typing_extensions import TypedDict

from langgraph_artifact_memory import (
    ArtifactConfigurationError,
    ArtifactMemory,
    ArtifactRef,
    ArtifactStateTypeError,
    Externalize,
    Hydrate,
    InMemoryArtifactStore,
)


class State(TypedDict, total=False):
    artifacts: Annotated[list[dict[str, Any] | ArtifactRef], operator.add]
    answer: str


async def main() -> None:
    try:
        ArtifactMemory(
            InMemoryArtifactStore(),
            externalize={"artfacts": Externalize()},
            state_schema=State,
        )
    except ArtifactConfigurationError as error:
        assert "Did you mean 'artifacts'?" in str(error)
    else:
        raise AssertionError("a misspelled state channel must fail at construction time")

    memory = ArtifactMemory(
        InMemoryArtifactStore(),
        externalize={"artifacts": Externalize(itemized=True)},
        state_schema=State,
    )
    memory.wrap(lambda state: {}, hydrate={"artifacts": Hydrate(last_n=2)})

    try:
        await memory.prepare_input(
            {"artifacts": "not-a-list"},
            {"configurable": {"thread_id": "validation-example"}},
        )
    except ArtifactStateTypeError as error:
        assert "expected list" in str(error)
    else:
        raise AssertionError("an invalid runtime channel value must fail before storage")

    print("state validation example passed")


if __name__ == "__main__":
    asyncio.run(main())
