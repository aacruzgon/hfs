# helios-persistence

Polyglot persistence layer for the Helios FHIR Server.

## Overview

Traditional FHIR server implementations force all resources into a single database technology, creating inevitable trade-offs. A patient lookup by identifier, a population cohort query, relationship traversals through care teams, and semantic similarity searches for clinical trial matching all have fundamentally different performance characteristics, yet they're typically crammed into one system optimized for none of them.

**Polyglot persistence** is an architectural approach where different types of data and operations are routed to the storage technologies best suited for how that data will be accessed. Rather than accepting compromise, this pattern leverages specialized storage systems optimized for specific workloads:

| Workload | Optimal Technology | Why |
|----------|-------------------|-----|
| ACID transactions | PostgreSQL | Strong consistency guarantees |
| Document storage | MongoDB | Natural alignment with FHIR's resource model |
| Relationship traversal | Neo4j | Efficient graph queries for references |
| Full-text search | Elasticsearch | Optimized inverted indexes |
| Semantic search | Vector databases | Embedding similarity for clinical matching |
| Bulk analytics & ML | Object Storage | Cost-effective columnar storage |

This crate implements the polyglot persistence layer described in [Discussion #28: Polyglot Persistence Architecture](https://github.com/HeliosSoftware/hfs/discussions/28).

## Polyglot Query Example

Consider a complex clinical query that combines multiple access patterns:

```
GET /Observation?patient.name:contains=smith&_text=cardiac&code:below=http://loinc.org|8867-4&_include=Observation:patient
```

This query requires:
1. **Chained search** (`patient.name:contains=smith`) - Find observations where the referenced patient's name contains "smith"
2. **Full-text search** (`_text=cardiac`) - Search narrative text for "cardiac"
3. **Terminology subsumption** (`code:below=LOINC|8867-4`) - Find codes that are descendants of heart rate
4. **Reference resolution** (`_include=Observation:patient`) - Include the referenced Patient resources

In a polyglot architecture, the `CompositeStorage` routes each component to its optimal backend:

```rust
// Conceptual flow - CompositeStorage coordinates backends
async fn search(&self, query: SearchQuery) -> SearchResult {
    // 1. Route chained search to graph database (efficient traversal)
    let patient_refs = self.neo4j.find_patients_by_name("smith").await?;

    // 2. Route full-text to Elasticsearch (optimized inverted index)
    let text_matches = self.elasticsearch.text_search("cardiac").await?;

    // 3. Route terminology query to terminology service + primary store
    let code_matches = self.postgres.codes_below("8867-4").await?;

    // 4. Intersect results and fetch from primary storage
    let observation_ids = intersect(patient_refs, text_matches, code_matches);
    let observations = self.postgres.batch_read(observation_ids).await?;

    // 5. Resolve _include from primary storage
    let patients = self.postgres.resolve_references(&observations, "patient").await?;

    SearchResult { resources: observations, included: patients }
}
```

No single database excels at all four operations. PostgreSQL would struggle with the graph traversal, Neo4j isn't optimized for full-text search, and Elasticsearch can't efficiently handle terminology hierarchies. Polyglot persistence lets each system do what it does best.

## Architecture

```
helios-persistence/
├── src/
│   ├── lib.rs           # Main entry point and re-exports
│   ├── error.rs         # Comprehensive error types
│   ├── tenant/          # Multitenancy support
│   │   ├── id.rs        # Hierarchical TenantId
│   │   ├── context.rs   # TenantContext (required for all operations)
│   │   ├── permissions.rs # Fine-grained TenantPermissions
│   │   └── tenancy.rs   # TenancyModel configuration
│   ├── types/           # Core domain types
│   │   ├── stored_resource.rs  # Resource with persistence metadata
│   │   ├── search_params.rs    # Full FHIR search parameter model
│   │   └── pagination.rs       # Cursor and offset pagination
│   ├── core/            # Storage trait hierarchy
│   │   ├── backend.rs      # Backend abstraction with capabilities
│   │   ├── storage.rs      # ResourceStorage (CRUD)
│   │   ├── versioned.rs    # VersionedStorage (vread, If-Match)
│   │   ├── history.rs      # History providers (instance/type/system)
│   │   ├── search.rs       # Search providers (basic, chained, include)
│   │   ├── transaction.rs  # ACID transactions with bundle support
│   │   └── capabilities.rs # Runtime capability discovery
│   ├── strategy/        # Tenancy isolation strategies
│   │   ├── shared_schema.rs       # tenant_id column + optional RLS
│   │   ├── schema_per_tenant.rs   # PostgreSQL search_path isolation
│   │   └── database_per_tenant.rs # Complete database isolation
│   ├── backends/        # Backend implementations
│   │   └── sqlite/      # Reference implementation (complete)
│   └── composite/       # Multi-backend routing (planned)
```

### Trait Hierarchy

The storage layer uses a progressive trait hierarchy inspired by Diesel:

```
Backend (connection management, capabilities)
    │
    ├── ResourceStorage (create, read, update, delete)
    │       │
    │       └── VersionedStorage (vread, update_with_match)
    │               │
    │               └── HistoryProvider (instance, type, system history)
    │
    ├── SearchProvider (search, search_count)
    │       │
    │       ├── IncludeProvider (_include resolution)
    │       ├── RevincludeProvider (_revinclude resolution)
    │       └── ChainedSearchProvider (chained parameters, _has)
    │
    └── TransactionProvider (begin, commit, rollback)
```

## Features

- **Multiple Backends**: SQLite, PostgreSQL, Cassandra, MongoDB, Neo4j, Elasticsearch, S3
- **Multitenancy**: Three isolation strategies with type-level enforcement
- **Full FHIR Search**: All parameter types, modifiers, chaining, _include/_revinclude
- **Versioning**: Complete resource history with optimistic locking
- **Transactions**: ACID transactions with FHIR bundle support
- **Capability Discovery**: Runtime introspection of backend capabilities

## Multitenancy

All storage operations require a `TenantContext`, ensuring tenant isolation at the type level. There is no way to bypass this requirement—the compiler enforces it.

### Tenancy Strategies

| Strategy | Isolation | Use Case |
|----------|-----------|----------|
| **Shared Schema** | `tenant_id` column + optional RLS | Multi-tenant SaaS with shared infrastructure |
| **Schema-per-Tenant** | PostgreSQL schemas | Logical isolation with shared database |
| **Database-per-Tenant** | Separate databases | Complete isolation for compliance |

### Hierarchical Tenants

```rust
use helios_persistence::tenant::TenantId;

let parent = TenantId::new("acme");
let child = TenantId::new("acme/research");
let grandchild = TenantId::new("acme/research/oncology");

assert!(child.is_descendant_of(&parent));
assert!(grandchild.is_descendant_of(&parent));
assert_eq!(grandchild.root().as_str(), "acme");
```

### Permission Control

```rust
use helios_persistence::tenant::{TenantPermissions, Operation};

// Read-only access
let read_only = TenantPermissions::read_only();

// Custom permissions with compartment restrictions
let custom = TenantPermissions::builder()
    .allow_operations(vec![Operation::Read, Operation::Search])
    .allow_resource_types(vec!["Patient", "Observation"])
    .restrict_to_compartment("Patient", "123")
    .build();
```

## Search

Build search queries with full FHIR search support:

```rust
use helios_persistence::types::{
    SearchQuery, SearchParameter, SearchParamType, SearchValue,
    SearchModifier, SortDirective, IncludeDirective, IncludeType,
};

// Simple search
let query = SearchQuery::new("Patient")
    .with_parameter(SearchParameter {
        name: "name".to_string(),
        param_type: SearchParamType::String,
        modifier: Some(SearchModifier::Contains),
        values: vec![SearchValue::eq("smith")],
        chain: vec![],
    })
    .with_sort(SortDirective::parse("-_lastUpdated"))
    .with_count(20);

// With _include
let query_with_include = SearchQuery::new("Observation")
    .with_include(IncludeDirective {
        include_type: IncludeType::Include,
        source_type: "Observation".to_string(),
        search_param: "patient".to_string(),
        target_type: Some("Patient".to_string()),
        iterate: false,
    });
```

## Backend Capability Matrix

The matrix below shows which FHIR operations each backend supports. This reflects the actual implementation status, not aspirational goals.

> **Note:** Documentation links reference [build.fhir.org](https://build.fhir.org), which contains the current FHIR development version. Some features marked as planned are new and may be labeled "Trial Use" in the specification.

**Legend:** ✓ Implemented | ◐ Partial | ○ Planned | ✗ Not planned | † Requires external service

| Feature | SQLite | PostgreSQL | MongoDB | Cassandra | Neo4j | Elasticsearch | S3 |
|---------|--------|------------|---------|-----------|-------|---------------|-----|
| **Core Operations** |
| [CRUD](https://build.fhir.org/http.html#crud) | ✓ | ○ | ○ | ○ | ○ | ○ | ○ |
| [Versioning (vread)](https://build.fhir.org/http.html#vread) | ✓ | ○ | ○ | ○ | ○ | ○ | ○ |
| [Optimistic Locking](https://build.fhir.org/http.html#concurrency) | ✓ | ○ | ○ | ○ | ○ | ✗ | ✗ |
| [Instance History](https://build.fhir.org/http.html#history) | ✓ | ○ | ○ | ○ | ○ | ○ | ○ |
| [Type History](https://build.fhir.org/http.html#history) | ✓ | ○ | ○ | ✗ | ○ | ○ | ✗ |
| [System History](https://build.fhir.org/http.html#history) | ✓ | ○ | ○ | ✗ | ○ | ○ | ✗ |
| [Transactions](https://build.fhir.org/http.html#transaction) | ✓ | ○ | ○ | ✗ | ○ | ✗ | ✗ |
| [Conditional Operations](https://build.fhir.org/http.html#cond-update) | ✓ | ○ | ○ | ✗ | ○ | ○ | ✗ |
| [Conditional Patch](https://build.fhir.org/http.html#patch) | ✓ | ○ | ○ | ✗ | ○ | ○ | ✗ |
| [Delete History](https://build.fhir.org/http.html#delete) | ✓ | ○ | ○ | ✗ | ○ | ✗ | ✗ |
| **Multitenancy** |
| Shared Schema | ✓ | ○ | ○ | ○ | ○ | ○ | ○ |
| Schema-per-Tenant | ✗ | ○ | ○ | ✗ | ✗ | ○ | ✗ |
| Database-per-Tenant | ✓ | ○ | ○ | ○ | ○ | ○ | ○ |
| Row-Level Security | ✗ | ○ | ✗ | ✗ | ✗ | ✗ | ✗ |
| **[Search Parameters](https://build.fhir.org/search.html#ptypes)** |
| [String](https://build.fhir.org/search.html#string) | ✓ | ○ | ○ | ✗ | ○ | ○ | ✗ |
| [Token](https://build.fhir.org/search.html#token) | ✓ | ○ | ○ | ○ | ○ | ○ | ✗ |
| [Reference](https://build.fhir.org/search.html#reference) | ✓ | ○ | ○ | ✗ | ○ | ○ | ✗ |
| [Date](https://build.fhir.org/search.html#date) | ✓ | ○ | ○ | ○ | ○ | ○ | ○ |
| [Number](https://build.fhir.org/search.html#number) | ✓ | ○ | ○ | ✗ | ○ | ○ | ○ |
| [Quantity](https://build.fhir.org/search.html#quantity) | ✓ | ○ | ○ | ✗ | ✗ | ○ | ○ |
| [URI](https://build.fhir.org/search.html#uri) | ✓ | ○ | ○ | ○ | ○ | ○ | ○ |
| [Composite](https://build.fhir.org/search.html#composite) | ✓ | ○ | ○ | ✗ | ○ | ○ | ✗ |
| **[Search Modifiers](https://build.fhir.org/search.html#modifiers)** |
| [:exact](https://build.fhir.org/search.html#modifiers) | ✓ | ○ | ○ | ○ | ○ | ○ | ○ |
| [:contains](https://build.fhir.org/search.html#modifiers) | ✓ | ○ | ○ | ✗ | ○ | ○ | ✗ |
| [:text](https://build.fhir.org/search.html#modifiers) (full-text) | ✓ | ○ | ○ | ✗ | ✗ | ○ | ✗ |
| [:not](https://build.fhir.org/search.html#modifiers) | ✓ | ○ | ○ | ✗ | ○ | ○ | ○ |
| [:missing](https://build.fhir.org/search.html#modifiers) | ✓ | ○ | ○ | ✗ | ○ | ○ | ○ |
| [:above / :below](https://build.fhir.org/search.html#modifiers) | ✗ | †○ | †○ | ✗ | ○ | †○ | ✗ |
| [:in / :not-in](https://build.fhir.org/search.html#modifiers) | ✗ | †○ | †○ | ✗ | ○ | †○ | ✗ |
| [:of-type](https://build.fhir.org/search.html#modifiers) | ✓ | ○ | ○ | ✗ | ○ | ○ | ✗ |
| [:text-advanced](https://build.fhir.org/search.html#modifiertextadvanced) | ✓ | †○ | †○ | ✗ | ✗ | †○ | ✗ |
| **[Special Parameters](https://build.fhir.org/search.html#all)** |
| [_text](https://build.fhir.org/search.html#_text) (narrative search) | ✓ | ○ | ○ | ✗ | ✗ | ○ | ✗ |
| [_content](https://build.fhir.org/search.html#_content) (full content) | ✓ | ○ | ○ | ✗ | ✗ | ○ | ✗ |
| [_filter](https://build.fhir.org/search.html#_filter) (advanced filtering) | ✓ | ○ | ○ | ✗ | ○ | ○ | ✗ |
| **Advanced Search** |
| [Chained Parameters](https://build.fhir.org/search.html#chaining) | ✓ | ○ | ○ | ✗ | ○ | ✗ | ✗ |
| [Reverse Chaining (_has)](https://build.fhir.org/search.html#has) | ✓ | ○ | ○ | ✗ | ○ | ✗ | ✗ |
| [_include](https://build.fhir.org/search.html#include) | ✓ | ○ | ○ | ✗ | ○ | ○ | ✗ |
| [_revinclude](https://build.fhir.org/search.html#revinclude) | ✓ | ○ | ○ | ✗ | ○ | ○ | ✗ |
| **[Pagination](https://build.fhir.org/http.html#paging)** |
| Offset | ✓ | ○ | ○ | ✗ | ○ | ○ | ✗ |
| Cursor (keyset) | ✓ | ○ | ○ | ○ | ○ | ○ | ○ |
| **[Sorting](https://build.fhir.org/search.html#sort)** |
| Single field | ✓ | ○ | ○ | ✗ | ○ | ○ | ✗ |
| Multiple fields | ✓ | ○ | ○ | ✗ | ○ | ○ | ✗ |
| **[Bulk Operations](https://hl7.org/fhir/uv/bulkdata/)** |
| [Bulk Export](https://hl7.org/fhir/uv/bulkdata/export.html) | ○ | ○ | ○ | ○ | ○ | ○ | ○ |
| Bulk Import | ○ | ○ | ○ | ○ | ○ | ○ | ○ |

### Backend Selection Guide

| Use Case | Recommended Backend | Rationale |
|----------|---------------------|-----------|
| Development & Testing | SQLite | Zero configuration, in-memory mode |
| Production OLTP | PostgreSQL | ACID transactions, JSONB, mature ecosystem |
| Document-centric | MongoDB | Natural FHIR alignment, flexible schema |
| Graph queries | Neo4j | Efficient relationship traversal |
| Full-text search | Elasticsearch | Optimized inverted indexes, analyzers |
| Bulk analytics | S3 + Parquet | Cost-effective, columnar, ML-ready |
| High write throughput | Cassandra | Distributed writes, eventual consistency |

### Feature Flags

| Feature | Description | Driver |
|---------|-------------|--------|
| `sqlite` (default) | SQLite (in-memory and file) | rusqlite |
| `postgres` | PostgreSQL with JSONB | tokio-postgres |
| `cassandra` | Apache Cassandra | cdrs-tokio |
| `mongodb` | MongoDB document store | mongodb |
| `neo4j` | Neo4j graph database | neo4rs |
| `elasticsearch` | Elasticsearch search | elasticsearch |
| `s3` | AWS S3 object storage | object_store |

## Implementation Status

### Phase 1: Core Types ✓
- [x] Error types with comprehensive variants
- [x] Tenant types (TenantId, TenantContext, TenantPermissions)
- [x] Stored resource types with versioning metadata
- [x] Search parameter types (all FHIR parameter types)
- [x] Pagination types (cursor and offset)

### Phase 2: Core Traits ✓
- [x] Backend trait with capability discovery
- [x] ResourceStorage trait (CRUD operations)
- [x] VersionedStorage trait (vread, If-Match)
- [x] History provider traits (instance, type, system)
- [x] Search provider traits (basic, chained, _include, terminology)
- [x] Transaction traits (ACID, bundles)
- [x] Capabilities trait (CapabilityStatement generation)

### Phase 3: Tenancy Strategies ✓
- [x] Shared schema strategy with RLS support
- [x] Schema-per-tenant strategy with PostgreSQL search_path
- [x] Database-per-tenant strategy with pool management

### Phase 4: SQLite Backend ✓
- [x] Connection pooling (r2d2)
- [x] Schema migrations
- [x] ResourceStorage implementation
- [x] VersionedStorage implementation
- [x] History providers (instance, type, system)
- [x] TransactionProvider implementation
- [x] Conditional operations (conditional create/update/delete)

#### SQLite Search Implementation ✓

The SQLite backend includes a complete FHIR search implementation using pre-computed indexes:

**Search Parameter Registry & Extraction:**
- [x] `SearchParameterRegistry` - In-memory cache of active SearchParameter definitions
- [x] `SearchParameterLoader` - Loads embedded R4 standard parameters at startup
- [x] `SearchParameterExtractor` - FHIRPath-based value extraction using `helios-fhirpath`
- [x] Dynamic SearchParameter handling - POST/PUT/DELETE to SearchParameter updates the registry

**Search Index & Query:**
- [x] Pre-computed `search_index` table for fast queries
- [x] All 8 parameter type handlers (string, token, date, number, quantity, reference, URI, composite)
- [x] Modifier support (:exact, :contains, :missing, :not, :identifier, :below, :above)
- [x] Prefix support for date/number/quantity (eq, ne, gt, lt, ge, le, sa, eb, ap)
- [x] `_include` and `_revinclude` resolution
- [x] Cursor-based and offset pagination
- [x] Single-field sorting

**Full-Text Search (FTS5):**
- [x] `resource_fts` FTS5 virtual table for full-text indexing
- [x] Narrative text extraction from `text.div` with HTML stripping
- [x] Full content extraction from all resource string values
- [x] `_text` parameter - searches narrative content
- [x] `_content` parameter - searches all resource text
- [x] `:text-advanced` modifier - advanced FTS5-based search with:
  - Porter stemming (e.g., "run" matches "running")
  - Boolean operators (AND, OR, NOT)
  - Phrase matching ("heart failure")
  - Prefix search (cardio*)
  - Proximity matching (NEAR operator)
- [x] Porter stemmer tokenization for improved search quality
- [x] Automatic FTS indexing on resource create/update/delete

**Chained Parameters & Reverse Chaining:**
- [x] N-level forward chains (e.g., `Observation?subject.organization.name=Hospital`)
- [x] Nested reverse chains / `_has` (e.g., `Patient?_has:Observation:subject:code=1234-5`)
- [x] Type modifiers for ambiguous references (e.g., `subject:Patient.name=Smith`)
- [x] SQL-based chain resolution using efficient nested subqueries
- [x] Registry-based type inference with fallback heuristics
- [x] Configurable depth limits (default: 4, max: 8)

**Reindexing:**
- [x] `ReindexableStorage` trait for backend-agnostic reindexing
- [x] `ReindexOperation` with background task execution
- [x] Progress tracking and cancellation support
- [ ] `$reindex` HTTP endpoint (planned for server layer)

**Capability Reporting:**
- [x] `SearchCapabilityProvider` implementation
- [x] Runtime capability discovery from registry

### Phase 5+: Additional Backends (Planned)
- [ ] PostgreSQL backend (JSONB, GIN indexes, RLS)
- [ ] Cassandra backend (wide-column, partition keys)
- [ ] MongoDB backend (document storage, aggregation)
- [ ] Neo4j backend (graph queries, Cypher)
- [ ] Elasticsearch backend (full-text, analyzers)
- [ ] S3 backend (bulk export, object storage)

### Phase 6: Composite Storage (Planned)
- [ ] Query analysis and routing
- [ ] Multi-backend coordination
- [ ] Cost-based optimization

## License

MIT
