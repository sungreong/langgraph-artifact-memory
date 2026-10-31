# Roadmap

## Planned after the 0.1 MVP

- `S3ArtifactStore` compatible with AWS S3 and S3-compatible services such as MinIO.
  - Multipart upload for large values.
  - Configurable bucket, prefix, endpoint URL, region, and server-side encryption.
  - Conditional writes for content-addressed deduplication.
  - Lifecycle and retention integration.
- Batched Redis and S3 put/get operations.
- Synchronous API equivalents.
- Keyword and vector retrieval policies in addition to `last_n` and `max_bytes`.
- Manifest/page compaction for state containing very large ArtifactRef lists.
- Transactional cleanup and garbage collection coordinated with checkpoint retention.
- Additional compatibility tests for Postgres and SQLite checkpointers.

MinIO/S3 is intentionally a TODO for 0.1.0. The MVP publishes only backends that are
implemented and covered by tests: in-memory, local filesystem, and Redis.
