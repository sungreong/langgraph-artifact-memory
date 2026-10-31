# Changelog

## 0.3.0

- Add bounded keep retention for messages marked with `additional_kwargs={"keep": True}`.
- Add message-type keep rules, such as retaining system messages.
- Add `mark_message_kept()` for immutable keep/unkeep metadata updates.
- Add a separate `max_kept_messages` bound so pinned content cannot grow state without limit.
- Preserve chronological order across kept and recent messages.
- Archive keep overflow and support Human, AI, System, and Tool messages through one policy.
- Add standalone, assertion-backed examples for pipelines, parallel Tools, conditional hydration,
  thread isolation, retention, persistence, resilience, and Redis.

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
