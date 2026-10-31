from __future__ import annotations

import types
import warnings
from difflib import get_close_matches
from typing import Annotated, Any, Literal, Union, get_args, get_origin, get_type_hints

from langgraph_artifact_memory.errors import (
    ArtifactConfigurationError,
    ArtifactStateTypeError,
    ArtifactValidationWarning,
)
from langgraph_artifact_memory.models import ArtifactRef, Externalize, Hydrate, MessageWindow

ValidationMode = Literal["strict", "warn", "off"]


def _unwrap_annotated(annotation: Any) -> Any:
    while get_origin(annotation) is Annotated:
        annotation = get_args(annotation)[0]
    return annotation


def _contains_artifact_ref(annotation: Any) -> bool:
    annotation = _unwrap_annotated(annotation)
    if annotation is Any or annotation is ArtifactRef:
        return True
    origin = get_origin(annotation)
    if origin in (Union, types.UnionType):
        return any(_contains_artifact_ref(value) for value in get_args(annotation))
    return False


class StateSchemaValidator:
    """Validate ArtifactMemory configuration against a Python state schema."""

    def __init__(self, state_schema: type[Any] | None, mode: ValidationMode) -> None:
        if mode not in ("strict", "warn", "off"):
            raise ValueError("validation must be 'strict', 'warn', or 'off'")
        self.mode = mode
        self.state_schema = state_schema
        self.annotations: dict[str, Any] | None = None
        if state_schema is not None and mode != "off":
            try:
                self.annotations = get_type_hints(state_schema, include_extras=True)
            except Exception as error:
                self._issue(
                    ArtifactConfigurationError(
                        f"could not inspect state_schema {state_schema!r}: {error}"
                    )
                )
                self.annotations = {}
            if not self.annotations:
                self._issue(
                    ArtifactConfigurationError("state_schema must define annotated state channels")
                )

    def _issue(self, error: ArtifactConfigurationError | ArtifactStateTypeError) -> None:
        if self.mode == "off":
            return
        if self.mode == "warn":
            warnings.warn(str(error), ArtifactValidationWarning, stacklevel=3)
            return
        raise error

    def _unknown_channel(self, channel: str, purpose: str) -> None:
        if self.annotations is None or channel in self.annotations:
            return
        available = sorted(self.annotations)
        match = get_close_matches(channel, available, n=1, cutoff=0.5)
        suggestion = f" Did you mean '{match[0]}'?" if match else ""
        self._issue(
            ArtifactConfigurationError(
                f"unknown state channel '{channel}' in {purpose}.{suggestion} "
                f"Available channels: {', '.join(available) or '(none)'}"
            )
        )

    def _validate_externalize_annotation(
        self,
        channel: str,
        policy: Externalize,
    ) -> None:
        if self.annotations is None or channel not in self.annotations:
            return
        annotation = _unwrap_annotated(self.annotations[channel])
        if annotation is Any:
            return

        if policy.itemized:
            if annotation is not list and get_origin(annotation) is not list:
                self._issue(
                    ArtifactConfigurationError(
                        f"state channel '{channel}' uses Externalize(itemized=True) "
                        f"but is annotated as {annotation!r}; expected list[...]"
                    )
                )
                return
            item_types = get_args(annotation)
            if item_types and not _contains_artifact_ref(item_types[0]):
                self._issue(
                    ArtifactConfigurationError(
                        f"state channel '{channel}' is externalized item-by-item but its "
                        "list item type does not include ArtifactRef"
                    )
                )
            return

        if not _contains_artifact_ref(annotation):
            self._issue(
                ArtifactConfigurationError(
                    f"state channel '{channel}' uses Externalize(itemized=False) but its "
                    "type does not include ArtifactRef"
                )
            )

    def validate_configuration(
        self,
        externalize: dict[str, Externalize],
        message_window: MessageWindow | None,
    ) -> None:
        if self.mode == "off":
            return
        for channel, policy in externalize.items():
            self._unknown_channel(channel, "externalize")
            self._validate_externalize_annotation(channel, policy)
        if message_window is not None:
            self._unknown_channel(message_window.key, "message_window")
            if self.annotations is not None and message_window.key in self.annotations:
                annotation = _unwrap_annotated(self.annotations[message_window.key])
                if (
                    annotation is not Any
                    and annotation is not list
                    and get_origin(annotation) is not list
                ):
                    self._issue(
                        ArtifactConfigurationError(
                            f"message window channel '{message_window.key}' must be annotated "
                            "as list[...]"
                        )
                    )

    def validate_hydrate(self, hydrate: dict[str, Hydrate]) -> None:
        if self.mode == "off":
            return
        for channel in hydrate:
            self._unknown_channel(channel, "hydrate")

    def validate_externalized_value(
        self,
        channel: str,
        value: Any,
        policy: Externalize,
    ) -> None:
        if self.mode == "off" or not policy.itemized or isinstance(value, list):
            return
        self._issue(
            ArtifactStateTypeError(
                f"state channel '{channel}' uses Externalize(itemized=True) but received "
                f"{type(value).__name__}; expected list"
            )
        )
