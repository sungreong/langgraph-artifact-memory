# Changelog

## 0.1.0

- Add compact `ArtifactRef` values for heavy LangGraph state.
- Add `ArtifactMemory.ainvoke()` input externalization.
- Add bounded per-node hydration with `last_n` and `max_bytes`.
- Add `MessageWindow` with external archive support.
- Add in-memory, local filesystem, and optional Redis artifact stores.
- Add operation metrics for artifact put/get/archive activity.
