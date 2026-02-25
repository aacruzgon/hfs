# S3 Primary Backend Plan v1

> **Scope:** Implement an S3 backend as a **primary datastore alone** (no S3 + Elasticsearch in this effort), **without changing any public APIs** and **without adding new core traits**. S3 is object storage, so it is expected that many capability-matrix features will be unsupported; all unsupported behavior must return **explicit typed errors** (no silent no-ops).

---

## 1. Inputs and anchors

This plan is grounded in the current `hfs` repository (main branch) and uses these artifacts as sources of truth:

- Architecture discussion: `crates/persistence/docs/design_discussion_28.md`
- Persistence README and capability matrix: `crates/persistence/README.md`
- Core trait contracts:
  - `crates/persistence/src/core/backend.rs`
  - `crates/persistence/src/core/storage.rs`
  - `crates/persistence/src/core/versioned.rs`
  - `crates/persistence/src/core/history.rs`
  - `crates/persistence/src/core/search.rs`
  - `crates/persistence/src/core/transaction.rs`
  - `crates/persistence/src/core/bulk_export.rs`
  - `crates/persistence/src/core/bulk_submit.rs`
- Reference semantics (behavioral parity where applicable): SQLite/Postgres backends under `crates/persistence/src/backends/sqlite` and `crates/persistence/src/backends/postgres`.

---

## 2. Scope lock (capabilities)

### 2.1 Must support (this effort)

S3 backend must support the following capabilities as a **primary** store:

- **CRUD** (`ResourceStorage`)
- **Versioning / vread** (`VersionedStorage::vread`, and any repo-defined version listing helper)
- **Instance history** (instance-level history provider methods)
- **Batch bundles** (batch-only; transaction bundles unsupported)
- **Multitenancy**
  - Shared Schema → tenant isolation by prefix in a shared bucket
  - Database-per-Tenant → bucket-per-tenant (or tenant-resolved bucket)
- **Minimal SearchProvider subset**
  - Parameter types: **Date, Number, Quantity, URI**
  - Modifiers: **`:exact`, `:not`, `:missing`**
  - **Cursor (keyset)** pagination only
- **Bulk Export** (bulk export trait family)
- **Bulk Submit** (bulk submit trait family; include streaming + rollback traits if required by the crate)

### 2.2 Must be explicitly unsupported (typed errors + tests)

Features that must return explicit typed errors (no partial/implicit behavior):

- Transactions / transaction bundles
- Optimistic locking via If-Match variants (e.g., `update_with_match`, `delete_with_match` if present)
- Conditional operations and conditional patch
- Type history and system history
- Delete-history APIs (e.g., delete instance history / delete version) unless explicitly required by scope
- Search types: String, Token, Reference, Composite
- Search features: chaining, `_has`, `_include`, `_revinclude`, `_filter`, `_text`, `_content`
- Terminology modifiers: `:above`, `:below`, `:in`, `:not-in`, `:of-type`, `:text-advanced`
- Offset pagination
- Sorting (single/multi-field)
- Schema-per-tenant, row-level security

---

## 3. Trait implementation checklist

> **Important:** Do not add new traits. Implement existing traits exactly. Where the S3 backend cannot support a method, implement it and return a typed unsupported error **only if** the S3 backend type is required to implement that trait in the code path; otherwise omit implementing the trait entirely.

### 3.1 Backend (core)

Implement the repo’s backend abstraction (see `core/backend.rs`):

- Kind/name identification
- Capability reporting (`supports`, `capabilities`)
- Lifecycle (`initialize`, `migrate`)
- Health check
- Acquire/release if required by the trait

Reference patterns: SQLite/Postgres backend implementations.

### 3.2 ResourceStorage (CRUD)

Implement all required `ResourceStorage` methods (see `core/storage.rs`), including:

- `create`
- `create_or_update`
- `read`
- `update`
- `delete` (soft delete / tombstone semantics)
- `count`
- Any required batching helper behavior (e.g., `read_batch`) if part of trait/defaults

### 3.3 VersionedStorage

Implement version-aware operations (see `core/versioned.rs`):

- **Supported:** `vread` (read a specific version)
- **Supported:** version listing if defined in this repo’s trait surface
- **Unsupported (typed error):** match-based optimistic locking variants (e.g., `update_with_match`, `delete_with_match`) if present and out of scope

> **Version IDs:** Follow the repo’s current `VersionedStorage` contract for `version_id` semantics. If the trait specifies monotonic numeric strings (common in this repo), follow that. If the repo later changes to opaque IDs, internal ordering must not assume lexical properties.

### 3.4 History providers

Support **instance-level history** only (see `core/history.rs`).

- **Supported:** `history_instance`, `history_instance_count`
- **Unsupported (typed error):** type/system history if in trait surface but out of scope

### 3.5 Bundle support

Support **batch bundles** and explicitly reject transaction bundles.

- **Supported:** `process_batch`
- **Unsupported (typed error):** `process_transaction`

Trait definitions live under `core/transaction.rs`.

### 3.6 SearchProvider (bounded subset)

Implement `SearchProvider` (see `core/search.rs`) with strict feature gating:

- Allowed parameter types: Date, Number, Quantity, URI
- Allowed modifiers: `:exact`, `:not`, `:missing`
- Cursor (keyset) pagination only

Reject everything else with explicit typed errors.

### 3.7 Bulk export

Implement the bulk export trait family defined in `core/bulk_export.rs`, including system/patient/group export provider traits if present.

### 3.8 Bulk submit

Implement the bulk submit trait family in `core/bulk_submit.rs`, including streaming ingest and rollback traits if present.

---

## 4. S3 object model and key layout

### 4.1 Tenant isolation mapping

Define a `tenant_scope` base:

- **Shared Schema mode:** single bucket, tenant prefix isolation
  - `v1/tenants/{tenant_id}/...`
- **Database-per-tenant mode:** bucket-per-tenant, fixed prefix
  - `v1/...`

Schema-per-tenant is rejected with `UnsupportedCapability`.

### 4.2 Canonical keys (per resource)

Under `{tenant_scope}/resources/{resource_type}/{id}/`:

| Key | Content | Full body? |
|---|---|---|
| `state.json` | current pointer, tombstone, and minimal search metadata for current version | No |
| `versions/by_seq/{seq_20}.json` | immutable version snapshot: full resource JSON + metadata + version_id | Yes |
| `versions/by_vid/{sha256(version_id)}.json` | pointer `{ seq, version_key, version_id }` for vread by version_id | No |
| `locks/{id}.lock` | lease lock payload `{ owner, expires_at, attempt }` | No |

### 4.3 Cursor/index keys (tenant-scoped)

Under `{tenant_scope}/cursor/by_updated/{resource_type}/`:

- `{rev_updated_ms_20}_{id}.json` → record `{ id, seq, last_updated_ms, deleted }`

Use newest-first ordering by storing `rev_updated_ms_20 = (u64::MAX - updated_ms)` padded to fixed width.

This cursor index is **eventually consistent** with `state.json`. Drift must never corrupt `read`/`vread`; it may only affect traversal visibility until repaired.

---

## 5. Concurrency and correctness model

S3 has no transactions. To avoid split-brain between `state.json` and version objects, serialize writes per resource.

### 5.1 Preferred path (conditional write, when supported)

If the object store supports conditional writes / put preconditions:

1. Read `state.json` (capture version/etag if available)
2. Write `versions/by_seq/{seq}.json` with create-only semantics
3. Write `versions/by_vid/{hash}.json` with create-only semantics
4. Update `state.json` with a conditional write (If-Match / precondition on prior state)
5. Write/update the cursor record

On precondition failure, retry with bounded backoff.

### 5.2 Fallback path (lease lock object)

If conditional updates are not available but create-only writes are:

1. Create lock `locks/{id}.lock` using create-only put (fail if exists)
2. Treat expired locks as stealable
3. Re-read `state.json`
4. Write version object and version_id pointer
5. Overwrite `state.json`
6. Release lock only if owner matches

Lock acquisition timeout returns `ConcurrencyError::LockTimeout`.

### 5.3 Initialization failure

If neither conditional update nor create-only lock is supported, backend initialization fails with `BackendError::UnsupportedCapability`.

---

## 6. Minimal search metadata (Date/Number/Quantity/URI)

Search metadata is stored in `state.json` only.

### 6.1 `state.json` schema (v1)

```json
{
  "schema_version": 1,
  "current": {
    "seq": 42,
    "version_id": "1",
    "last_updated": "2026-02-24T12:34:56Z",
    "last_updated_ms": 1708778096000,
    "deleted": false
  },
  "search": {
    "params": {
      "<param_name>": {
        "type": "date|number|quantity|uri",
        "values": [
          { "exact": "...", "raw": "..." }
        ]
      }
    }
  }
}
```

### 6.2 Canonicalization rules

- **Date**: store precision and a UTC range `[start,end)`; `exact` preserves the original precision form.
- **Number**: normalize to decimal string (no exponent); store scale; store equality range if needed for eq semantics.
- **Quantity**: normalize numeric + preserve system/code/unit; `exact` token is `value|system|code_or_unit`.
- **URI**: strict string equality; no case folding.

Extraction should reuse the repo’s existing search parameter registry/extractor infrastructure and retain only these supported types.

### 6.3 Query semantics (bounded)

- AND across parameters; OR across values per parameter
- `:missing=true` means param absent/empty; `:missing=false` means present with at least one value
- `:exact` compares `exact` token
- `:not` is resource-level negation of the positive predicate; ensure behavior matches SQLite/Postgres tests regarding missing
- Cursor-only pagination; offset/sorting rejected

---

## 7. Error semantics

### 7.1 Core resource errors

- Create conflict: `AlreadyExists`
- Missing resource on update/delete: `NotFound`
- Read of deleted resource: `Gone` (match SQL backends)
- vread missing version: `Ok(None)`

### 7.2 Unsupported behavior

Return explicit typed errors for unsupported features, using `BackendError::UnsupportedCapability { backend_name: "s3", capability: "..." }` (or the repo’s equivalent).

For search, prefer specialized search error variants when available; otherwise use `UnsupportedCapability`.

### 7.3 Lock timeout

Lock acquisition timeout returns `ConcurrencyError::LockTimeout`.

### 7.4 Transport failures

After bounded retries, return `BackendError::Unavailable` or `BackendError::Internal` (no infinite retry loops).

---

## 8. Test strategy (MinIO + parity)

### 8.1 Harness

- Integration tests gated by `--features s3`
- Use MinIO for S3-compatible testing
- Use per-test unique tenant prefixes and/or per-tenant buckets

### 8.2 Test groups (supported scope)

- CRUD: create/read/update/delete/count/create_or_update + tenant isolation
- Versioning: vread, version increments, list versions ordering (by seq)
- Instance history: history_instance, history_instance_count, cursor stability
- Batch bundles: batch behavior; transaction bundle unsupported
- Multitenancy: shared prefix isolation; bucket-per-tenant isolation; schema-per-tenant rejected
- Minimal search:
  - Date/Number/Quantity/URI positive and negative cases
  - :exact/:not/:missing
  - cursor paging stability
  - explicit rejection tests for unsupported search features
- Bulk export: job lifecycle + stable keyset traversal
- Bulk submit: lifecycle + streaming ingest + rollback behavior

### 8.3 Unsupported tests

Explicit tests for:
- optimistic locking match APIs unsupported
- conditional ops/patch unsupported
- type/system history unsupported
- transaction bundles unsupported
- unsupported search families/features
- offset pagination + sorting unsupported

### 8.4 MinIO testcontainers modules (full context)

All S3 integration tests must run against a local, reproducible S3-compatible service using **MinIO via testcontainers**. This provides deterministic behavior in CI and avoids relying on real AWS infrastructure.

#### Test harness module layout

Add a dedicated, reusable MinIO harness under:

- `crates/persistence/tests/common/minio.rs`

This module should expose:

- `MinioHarness::shared() -> &'static MinioHarness` (global singleton)
- `MinioHarness::client_shared_bucket(&self, bucket: &str) -> S3TestClient`
- `MinioHarness::client_bucket_per_tenant(&self, bucket_prefix: &str) -> S3TestClient`
- helpers for generating unique test tenant IDs/prefixes

Use a `OnceCell`/`LazyLock` singleton pattern so MinIO is started **once per test run**, not per test.

#### Container configuration

Start MinIO using the official image (example):

- Image: `minio/minio`
- Command: `server /data --console-address :9001`
- Expose ports: `9000` (S3 API), `9001` (console, optional)
- Credentials: fixed test credentials (e.g., `MINIO_ROOT_USER=minioadmin`, `MINIO_ROOT_PASSWORD=minioadmin`)

Wait strategy / readiness:

- Prefer a readiness check that verifies the S3 API is reachable (HTTP 200/403 on the service endpoint) before running tests.
- As a secondary check, verify that a bucket can be created and listed.

#### S3 client wiring (object_store)

Create an `object_store::aws::AmazonS3Builder` (or repo-standard constructor) configured for MinIO:

- Endpoint: `http://127.0.0.1:{mapped_port}`
- Region: a fixed test region (e.g., `us-east-1`)
- Access key / secret: MinIO credentials
- **Path-style addressing enabled** (required for many MinIO setups)
- HTTP allowed (no TLS) for local tests

The harness should provide:

- `create_bucket_if_missing(bucket)`
- `purge_prefix(bucket, prefix)` for cleanup between tests (or use unique prefixes to avoid cleanup)

#### Two tenancy modes to test

Every integration test must be runnable in both modes:

1) **Shared Schema mode** (single bucket, tenant prefix isolation)
   - One shared bucket per test run (e.g., `hfs-test`)
   - Each test uses a unique tenant root prefix:
     - `v1/tenants/{tenant_id}/...`

2) **Database-per-tenant mode** (bucket-per-tenant)
   - Bucket name derived from tenant root:
     - `hfs-tenant-{tenant_root}-{random_suffix}`
   - Fixed prefix inside bucket:
     - `v1/...`

Provide harness helpers so tests can easily instantiate the S3 backend in either mode.

#### Required smoke tests (Phase 0 gate)

Before implementing higher-level semantics, Phase 0 must include a small set of “pipe is real” tests:

- `s3_minio_put_get_delete_roundtrip`
- `s3_minio_tenant_prefix_isolation`
- `s3_minio_bucket_per_tenant_isolation`

Each test should:

- create a unique tenant context
- write a small object
- read it back
- assert isolation rules (wrong tenant cannot read)
- delete and assert not-found behavior

#### CI considerations

- Tests must be gated by `--features s3`.
- Ensure container startup is robust and does not require Docker-in-Docker privileges beyond normal testcontainers usage.
- Avoid global bucket cleanup races by using unique per-test tenant prefixes (preferred) or prefix purge helpers.
- Keep timeouts conservative to avoid flakiness (container start + readiness check).

---

## 9. PR-sized phased implementation plan

Each phase must end with `cargo test -p helios-persistence --features s3` passing.

1. **Phase 0**: scaffold backend, config, key builder, MinIO smoke tests (put/get/delete + tenant isolation)
2. **Phase 1**: implement `Backend` + `ResourceStorage` CRUD core
3. **Phase 2**: implement `VersionedStorage` supported methods (`vread`, list versions) + explicit unsupported match methods
4. **Phase 3**: implement instance history provider methods
5. **Phase 4**: implement batch bundles; reject transaction bundles
6. **Phase 5**: implement bounded `SearchProvider` subset + cursor-only
7. **Phase 6**: implement bulk export trait family
8. **Phase 7**: implement bulk submit trait family (including streaming/rollback traits if required)
9. **Phase 8**: harden unsupported coverage + documentation updates

---

## 10. Codex execution rule

All Codex implementation prompts must begin with:

> “Follow `crates/persistence/docs/s3_backend_plan.md` exactly. Do not expand scope. Implement only the requested phase.”

This prevents scope drift and prevents losing the plan to chat context.