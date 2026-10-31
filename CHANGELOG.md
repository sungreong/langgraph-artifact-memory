# Changelog

## 0.2.0

- Add bounded, order-preserving concurrent artifact put/get operations.
- Add configurable operation and capacity timeouts.
- Add bounded exponential-backoff retries for transient failures.
- Add typed timeout, availability, and capacity errors.
- Add retry, failure, timeout, queue-wait, and peak-concurrency metrics.
- Keep metric memory usage bounded with running duration aggregates.
- Keep archive append non-retrying until idempotent archive writes are available.

## 0.1.0

- Add compact `ArtifactRef` values for heavy LangGraph state.
- Add `ArtifactMemory.ainvoke()` input externalization.
- Add bounded per-node hydration with `last_n` and `max_bytes`.
- Add `MessageWindow` with external archive support.
- Add in-memory, local filesystem, and optional Redis artifact stores.
- Add operation metrics for artifact put/get/archive activity.
