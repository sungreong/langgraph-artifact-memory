# Roadmap

## Planned after the 0.3 message retention release

- `S3ArtifactStore` compatible with AWS S3 and S3-compatible services such as MinIO.
  - Multipart upload for large values.
  - Configurable bucket, prefix, endpoint URL, region, and server-side encryption.
  - Conditional writes for content-addressed deduplication.
  - Lifecycle and retention integration.
- Batched Redis and S3 put/get operations.
- Idempotent archive append operations that can be retried safely.
- Explicit fallback routing between shared durable stores.
- Synchronous API equivalents.
- Keyword and vector retrieval policies in addition to `last_n` and `max_bytes`.
- Manifest/page compaction for state containing very large ArtifactRef lists.
- Transactional cleanup and garbage collection coordinated with checkpoint retention.
- Additional compatibility tests for Postgres and SQLite checkpointers.

MinIO/S3 and cross-backend fallback are intentionally deferred. The package currently ships only
implemented and covered backends: in-memory, local filesystem, and Redis. Local or in-memory
fallback is not safe for multi-pod recovery because a later request may run on another pod.
