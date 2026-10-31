from __future__ import annotations


class ArtifactError(Exception):
    """Base class for artifact I/O failures raised by this package."""


class ArtifactValidationError(ArtifactError):
    """Base class for state schema and runtime value validation failures."""


class ArtifactConfigurationError(ArtifactValidationError):
    """ArtifactMemory configuration does not match the provided state schema."""


class ArtifactStateTypeError(ArtifactValidationError):
    """A runtime state value does not match its externalization policy."""


class ArtifactValidationWarning(UserWarning):
    """Non-fatal validation problem emitted when validation='warn'."""


class ArtifactOperationError(ArtifactError):
    """Base class for failures associated with one store operation."""

    def __init__(self, operation: str, attempts: int, message: str) -> None:
        self.operation = operation
        self.attempts = attempts
        super().__init__(message)


class ArtifactTimeoutError(ArtifactOperationError):
    """An artifact operation exceeded its timeout after all attempts."""

    def __init__(self, operation: str, attempts: int) -> None:
        super().__init__(
            operation,
            attempts,
            f"artifact {operation} timed out after {attempts} attempt(s)",
        )


class ArtifactUnavailableError(ArtifactOperationError):
    """A transient backend failure remained after all retry attempts."""

    def __init__(self, operation: str, attempts: int) -> None:
        super().__init__(
            operation,
            attempts,
            f"artifact backend unavailable during {operation} after {attempts} attempt(s)",
        )


class ArtifactCapacityError(ArtifactOperationError):
    """The configured I/O concurrency capacity was not available in time."""

    def __init__(self, operation: str) -> None:
        super().__init__(
            operation,
            0,
            f"artifact I/O capacity unavailable for {operation}",
        )
