# S3 Backend (`aws_sdk_s3`) Guarantees and Limits

This backend is an object-storage persistence backend for Helios. It is intentionally focused on storage, versioning/history, and bulk workflows, not advanced FHIR query execution.

## Scope and Role

- Primary responsibilities:
  - CRUD persistence of resources
  - Versioning (`vread`, `list_versions`, optimistic conflict checks)
  - Instance/type/system history via immutable history objects plus history index events
  - Batch bundles and best-effort transaction bundles (non-atomic with compensating rollback attempts)
  - Bulk export (NDJSON objects + manifest/progress state in S3)
  - Bulk submit (ingest + raw artifact persistence + rollback change log)
  - Tenant isolation through:
    - `PrefixPerTenant`
    - `BucketPerTenant` (explicit tenant→bucket map)

- Explicit non-goals for this backend:
  - Advanced FHIR search semantics as the primary query engine (`date/number/quantity` comparison semantics, full chained query planning, `_has`, include/revinclude fanout planning, full cursor keyset query engine)

For query-heavy production deployments, run a DB/search backend as primary query engine and use S3 for bulk/history/archive responsibilities.

## Object Model

Resource objects:

- Current pointer: `.../resources/{type}/{id}/current.json`
- Immutable history version: `.../resources/{type}/{id}/_history/{version}.json`
- Type history event: `.../history/type/{type}/{ts}_{id}_{version}_{suffix}.json`
- System history event: `.../history/system/{ts}_{type}_{id}_{version}_{suffix}.json`

Bulk export:

- `.../bulk/export/jobs/{job_id}/state.json`
- `.../bulk/export/jobs/{job_id}/progress/{type}.json`
- `.../bulk/export/jobs/{job_id}/output/{type}/part-{n}.ndjson`
- `.../bulk/export/jobs/{job_id}/manifest.json`

Bulk submit:

- `.../bulk/submit/{submitter}/{submission_id}/state.json`
- `.../bulk/submit/{submitter}/{submission_id}/manifests/{manifest_id}.json`
- `.../bulk/submit/{submitter}/{submission_id}/raw/{manifest_id}/line-{line}.ndjson`
- `.../bulk/submit/{submitter}/{submission_id}/results/{manifest_id}/line-{line}.json`
- `.../bulk/submit/{submitter}/{submission_id}/changes/{change_id}.json`

## Consistency and Transaction Notes

- The backend never creates buckets.
- Startup/runtime bucket checks use `HeadBucket` only.
- Optimistic locking relies on version checks plus S3 preconditions (`If-Match`, `If-None-Match`) where applicable.
- Transaction bundle behavior is best-effort:
  - Entries are applied sequentially.
  - On failure, rollback is attempted in reverse order.
  - Rollback is not guaranteed under concurrent writes or partial failures.

## AWS Credentials and Region

- Uses AWS SDK for Rust (`aws_sdk_s3`) with standard provider chain.
- Region may be provided in config or via `AWS_REGION`.
- Environment credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`) are supported by provider chain behavior.

## Endpoint Modes

### AWS S3 mode

Use this mode when connecting to AWS-managed S3 endpoints.

- `endpoint_url = None`
- `allow_http = false` (default)
- `force_path_style = false` (default)

```rust
use helios_persistence::backends::s3::{S3BackendConfig, S3TenancyMode};

let config = S3BackendConfig {
    tenancy_mode: S3TenancyMode::PrefixPerTenant {
        bucket: "my-aws-bucket".to_string(),
    },
    endpoint_url: None,
    force_path_style: false,
    allow_http: false,
    ..Default::default()
};
```

### S3-compatible endpoint mode (MinIO, etc.)

Use this mode for custom endpoints.

- set `endpoint_url`
- set `allow_http=true` for local `http://...` endpoints
- path-style is defaulted on when endpoint mode is active
- region falls back to `us-east-1` when not provided

```rust
use helios_persistence::backends::s3::{S3BackendConfig, S3TenancyMode};

let config = S3BackendConfig {
    tenancy_mode: S3TenancyMode::PrefixPerTenant {
        bucket: "minio-bucket".to_string(),
    },
    endpoint_url: Some("http://127.0.0.1:9000".to_string()),
    allow_http: true,
    force_path_style: true,
    ..Default::default()
};
```

Notes:

- `http://` endpoints are rejected unless `allow_http=true`.
- AWS behavior is unchanged when `endpoint_url` is not set.
- Buckets are never created by production backend code; startup validation uses `HeadBucket`.

## MinIO Integration Tests

MinIO parity tests live in:

- `crates/persistence/tests/minio_s3_tests.rs`

The suite is opt-in and env-gated:

- `RUN_MINIO_S3_TESTS=1`

Optional overrides:

- `MINIO_IMAGE` (default: `minio/minio`)
- `MINIO_TAG` (default: `RELEASE.2025-02-28T09-55-16Z`)
- `MINIO_ROOT_USER` (default: `minioadmin`)
- `MINIO_ROOT_PASSWORD` (default: `minioadmin`)
- `HFS_MINIO_TEST_BUCKET` (if unset, tests auto-generate a unique bucket)

Example:

```bash
RUN_MINIO_S3_TESTS=1 \
cargo test -p helios-persistence --features s3 --test minio_s3_tests
```

```bash
RUN_AWS_S3_TESTS=1 \
cargo test -p helios-persistence --features s3 --test s3_tests
```
