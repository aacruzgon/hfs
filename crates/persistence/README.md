# helios-persistence

Polyglot persistence layer for the Helios FHIR Server.

## Overview

This crate provides a flexible, multi-backend persistence layer for storing and retrieving FHIR resources. It supports multiple database backends via feature flags and provides configurable multitenancy with full FHIR search capabilities.

## Features

- **Multiple Backends**: SQLite, PostgreSQL, Cassandra, MongoDB, Neo4j, Elasticsearch, S3
- **Multitenancy**: Three isolation strategies available from day one
- **Full FHIR Search**: All parameter types, modifiers, chaining, _include/_revinclude
- **Versioning**: Full resource history with optimistic locking
- **Transactions**: ACID transactions with FHIR bundle support

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
helios-persistence = { version = "0.1", features = ["postgres", "R4"] }
```

## Feature Flags

### Database Backends

| Feature | Description | Driver |
|---------|-------------|--------|
| `sqlite` (default) | SQLite (in-memory and file) | rusqlite |
| `postgres` | PostgreSQL with JSONB | tokio-postgres |
| `cassandra` | Apache Cassandra | cdrs-tokio |
| `mongodb` | MongoDB document store | mongodb |
| `neo4j` | Neo4j graph database | neo4rs |
| `elasticsearch` | Elasticsearch search | elasticsearch |
| `s3` | AWS S3 object storage | object_store |

### FHIR Versions

| Feature | Description |
|---------|-------------|
| `R4` | FHIR R4 (4.0.1) |
| `R4B` | FHIR R4B (4.3.0) |
| `R5` | FHIR R5 (5.0.0) |
| `R6` | FHIR R6 (preview) |

## Quick Start

```rust
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use helios_persistence::types::StoredResource;
use serde_json::json;

// Create a tenant context (required for all operations)
let tenant = TenantContext::new(
    TenantId::new("my-organization"),
    TenantPermissions::full_access(),
);

// Create a stored resource
let resource = StoredResource::new(
    "Patient",
    "patient-123",
    tenant.tenant_id().clone(),
    json!({
        "resourceType": "Patient",
        "id": "patient-123",
        "name": [{"family": "Smith", "given": ["John"]}]
    }),
);

// The resource includes persistence metadata
assert_eq!(resource.version_id(), "1");
assert_eq!(resource.url(), "Patient/patient-123");
```

## Multitenancy

All storage operations require a `TenantContext`, ensuring tenant isolation at the type level. There is no way to bypass this requirement.

### Tenancy Strategies

1. **Shared Schema**: All tenants share tables with `tenant_id` column
2. **Schema-per-Tenant**: Separate PostgreSQL schema per tenant
3. **Database-per-Tenant**: Complete isolation with separate databases

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

// Custom permissions
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

### Search Parameter Types

- `String` - Text search with prefix matching
- `Token` - Code/identifier search
- `Reference` - Resource reference search
- `Date` - Date/DateTime search with prefixes
- `Number` - Numeric search with prefixes
- `Quantity` - Quantity search with units
- `URI` - URI search
- `Composite` - Combined parameters

### Search Modifiers

- `:exact` - Exact string match
- `:contains` - Substring match
- `:text` - Full-text search
- `:not` - Negation
- `:missing` - Missing value check
- `:above/:below` - Hierarchy navigation
- `:in/:not-in` - Value set membership
- `:identifier` - Identifier on reference
- `:[type]` - Type filter on reference

## Pagination

Cursor-based pagination (recommended):

```rust
use helios_persistence::types::{Pagination, PageCursor, CursorValue};

// Create pagination request
let pagination = Pagination::cursor().with_count(50);

// Create cursor for next page
let cursor = PageCursor::new(
    vec![CursorValue::from("2024-01-15T10:30:00Z")],
    "resource-id",
);
let encoded = cursor.encode();

// Parse cursor from request
let decoded = PageCursor::decode(&encoded).unwrap();
```

## Architecture

```
helios-persistence/
├── src/
│   ├── lib.rs           # Main entry point
│   ├── error.rs         # Error types
│   ├── tenant/          # Multitenancy support
│   │   ├── id.rs        # TenantId
│   │   ├── context.rs   # TenantContext
│   │   ├── permissions.rs # TenantPermissions
│   │   └── tenancy.rs   # TenancyModel
│   ├── types/           # Core types
│   │   ├── stored_resource.rs
│   │   ├── search_params.rs
│   │   └── pagination.rs
│   ├── core/            # Storage traits
│   │   ├── backend.rs   # Backend abstraction
│   │   ├── storage.rs   # ResourceStorage trait
│   │   ├── versioned.rs # VersionedStorage trait
│   │   ├── history.rs   # History providers
│   │   ├── search.rs    # Search providers
│   │   ├── transaction.rs # Transaction support
│   │   └── capabilities.rs # Capability discovery
│   ├── strategy/        # Tenancy strategies
│   │   ├── mod.rs           # TenancyStrategy enum
│   │   ├── shared_schema.rs # Shared schema strategy
│   │   ├── schema_per_tenant.rs # Schema-per-tenant strategy
│   │   └── database_per_tenant.rs # Database-per-tenant strategy
│   ├── backends/        # Backend implementations (future)
│   └── composite/       # Composite storage (future)
```

## Implementation Status

### Phase 1: Core Types (Complete)
- [x] Error types
- [x] Tenant types (TenantId, TenantContext, TenantPermissions)
- [x] Stored resource types
- [x] Search parameter types
- [x] Pagination types

### Phase 2: Core Traits (Complete)
- [x] Backend trait with capability discovery
- [x] ResourceStorage trait (CRUD operations)
- [x] VersionedStorage trait (vread, If-Match)
- [x] History provider traits (instance, type, system)
- [x] Search provider traits (basic, chained, _include, terminology)
- [x] Transaction traits (ACID, bundles)
- [x] Capabilities trait (CapabilityStatement generation)

### Phase 3: Tenancy Strategies (Complete)
- [x] Shared schema strategy with RLS support
- [x] Schema-per-tenant strategy with PostgreSQL search_path
- [x] Database-per-tenant strategy with pool management

### Phase 4+: Backend Implementations (Planned)
- [ ] SQLite backend
- [ ] PostgreSQL backend
- [ ] Cassandra backend
- [ ] MongoDB backend
- [ ] Neo4j backend
- [ ] Elasticsearch backend
- [ ] S3 backend

## License

MIT
