# Building a Modern FHIR Persistence Layer - Architectural Considerations for Healthcare's AI Era

## Introduction

As I write this in early 2026, I don't think it is an understatement to say that the opportunities and impact that are upon us with AI in healthcare feels like a Cambrian Explosion moment.  Healthcare professionals, administrators, and patients alike will be increasingly chatting with, talking directly to, and collaborating with artificial intelligence software systems in entirely new ways.  This will need to be [done safely and carefully](https://hvp.global/).  

What worked five years ago, or even two years ago, is increasingly inadequate for the demands of clinical AI, population health analytics, and real-time decision support. For technical architects navigating this shift, the challenge isn't just scaling storage; it's rethinking the entire data architecture.

This discussion document shares my thoughts about an approach to persistence for the Helios FHIR Server.

This document is an architecture strategy document.  In other words, it describes the main motivating direction, building blocks, and key technology ingredients that will makeup the persistence design for the Helios FHIR Server.  It is not intended to be a comprehensive set of requirements and design, but instead contains enough of a starting point such that readers can understand our approach to persistence, and understand why we decided to make the decisions that we did. 
## Who should read this?

The Helios FHIR Server is open source software, and is being developed in the open.  If you have some interest in persistence design for healthcare software - this document is for you!

My hope is that you will think about the contents of this document, comment and provide feedback!
### AI Is Driving New Requirements on Data

AI workloads have upended traditional assumptions about data access patterns. Training models demand sustained high-throughput reads across massive datasets, while inference requires low-latency access to distributed data sources. In healthcare, this is compounded by the explosive growth of unstructured data.  Radiology images, pathology slides, genomic sequences, clinical notes, and waveform data from monitoring devices to name a few. Structured EHR data, once the center of gravity, is increasingly extracted from the EMR and compared with other external data sources. Architectures optimized for transactional workloads simply cannot deliver the performance AI pipelines require, and retrofitting them is often a losing battle.
### Separation of Storage and Compute

Decoupling storage from compute has moved from a cloud-native best practice to an architectural necessity, yet many FHIR server implementations haven't caught up. While cloud-based analytics platforms routinely embrace this separation, transactional FHIR servers often remain tightly coupled to their persistence layers, treating database and application as an inseparable unit. This creates painful trade-offs: over-provisioning compute to get adequate storage, or vice versa. A modern FHIR server must separate these concerns as a core architectural principle, allowing the API layer to scale horizontally for request throughput while the persistence layer scales independently for capacity and query performance. In healthcare AI workloads, this separation is especially critical.  Spin up GPU clusters for model training without provisioning redundant storage, or expand storage for imaging archives without paying for idle compute.  The persistence layer becomes a service with its own scaling characteristics rather than a monolithic dependency. This separation is now expected as a defining characteristic of production-ready FHIR infrastructure.

### Medallion Architecture Within FHIR Persistence

We have seen our largest petabyte-scale customers transition to a [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture) strategy for their FHIR data. The bronze layer represents resources as received, preserving original payloads, source system identifiers, and ingestion metadata for auditability and replay. The silver layer applies normalization: terminology mapping, reference resolution, deduplication of resources that represent the same clinical entity, and enforcement of business rules that go beyond FHIR validation. The gold layer materializes optimized views for specific consumers, denormalized patient summaries for clinical applications, flattened tabular projections for analytics, or pre-computed feature sets for ML pipelines. 

### Hybrid and Multi-Cloud Architectures

The reality for most health IT systems is a hybrid footprint: on-premises data centers housing legacy systems and sensitive workloads, cloud platforms providing elastic compute for AI and analytics, and edge infrastructure at clinical sites. Multi-cloud strategies add another dimension, whether driven by M&A activity, best-of-breed vendor selection, or risk diversification.

### Security-First and Zero-Trust Patterns in FHIR Persistence

The persistence layer is where FHIR data lives at rest, making it the most critical surface for security enforcement. Zero-trust principles must be embedded in the persistence design itself, not just the API layer above it. This means encryption at rest as a baseline, but also fine-grained access control at the resource, compartment or even finer-grained levels - ensuring that database-level access cannot bypass FHIR authorization semantics. Audit logging must capture all persistence operations with sufficient detail for HIPAA accounting-of-disclosures requirements.  This typically means persisting AuditEvent resources to a separately controlled store. Consent enforcement, particularly for sensitive resource types like mental health or substance abuse records under [42 CFR Part 2](https://www.ecfr.gov/current/title-42/chapter-I/subchapter-A/part-2), often requires persistence-layer support through segmentation, tagging, or dynamic filtering. Treating security as an API-layer concern while leaving the persistence layer permissive creates unacceptable risk.

### Data Retention, Tiering, and Cost Optimization

FHIR persistence layers accumulate data over years and decades.  Version history, provenance records, and audit logs all create significant cost pressure. Intelligent tiering within the persistence layer moves older resource versions and infrequently accessed resources to lower-cost storage classes while keeping current data on performant storage. The architectural challenge is maintaining query semantics across tiers: a search that spans active and archived resources should work transparently, even if archived retrieval is slower. Retention policies must account for regulatory requirements that vary by resource type.  Imaging studies may have different retention mandates than clinical notes. A well-designed persistence layer makes tiering a configuration concern rather than an architectural constraint.

## Different Data Technologies for Different Problems

A FHIR persistence layer that commits to a single storage technology is making a bet that one tool can serve all masters. This is a bet that rarely pays off as requirements evolve. The reality is that different access patterns, query types, and workloads have fundamentally different performance characteristics, and no single database technology optimizes for all of them. A patient lookup by identifier, a population-level cohort query, a graph traversal of care team relationships, and a semantic similarity search for clinical trial matching across different terminology code systems are all legitimate operations against FHIR data, yet each performs best on a different underlying technology.

>  Modern FHIR persistence architectures increasingly embrace [polyglot persistence](https://en.wikipedia.org/wiki/Polyglot_persistence), which means routing data to the storage technology best suited for how that data will be accessed, while maintaining a unified FHIR API layer above. 

- **Relational Databases** remain the workhorse for transactional FHIR operations, offering ACID guarantees, mature tooling, and well-understood query optimization for structured data with predictable access patterns.

- **NoSQL Databases** - particularly document stores - align naturally with FHIR's resource model, persisting resources as complete documents without the impedance mismatch of relational decomposition, and scaling horizontally for high-throughput ingestion. Additionally, Cassandra has been exceptional at handling web-scale data requirements without breaking the bank.

- **Data Lakes** provide cost-effective, schema-flexible storage for raw FHIR resources and bulk exports, serving as the foundation for large-scale analytics and ML training pipelines that need to process millions of resources.

- **Data Warehouses** deliver optimized analytical query performance over structured, transformed FHIR data, enabling population health analytics, quality measure computation, and business intelligence workloads that would overwhelm transactional systems.

- **Graph Databases** excel at traversing relationships.  Patient to provider to organization to care team is an example relationship pathway that are represented as references in FHIR but are expensive to navigate through recursive joins in relational systems.

- **Vector Databases** enable semantic search and similarity matching over embedded representations of clinical text, supporting AI use cases like similar-patient retrieval, terminology matching, and contextual search that go beyond keyword-based FHIR queries.

- **Block Storage** provides the high-performance, low-latency foundation for database engines themselves, while also serving large binary attachments, imaging data, scanned documents, and waveforms that are referenced by FHIR resources but impractical to store within the resource payload.

- **Object Storage** (S3, Azure Blob, GCS) offers virtually unlimited capacity with pay-per-use economics, making it ideal for storing raw FHIR resources, bulk exports, and serving as the authoritative record in architectures that separate storage from indexing. Object stores can serve as the primary persistence layer with separate indexing technology supporting search, enabling almost database-less designs for certain workloads.

The architectural discipline is not choosing one technology but designing the abstraction layer that routes FHIR operations to the appropriate backend while maintaining consistency, security, and a coherent developer experience.

## Positioning the Helios FHIR Server in the FHIR Server Landscape

<img alt="matrix-diagram" src="https://github.com/user-attachments/assets/7ad4d331-ba85-44aa-98c2-cb03c0b3f716" />

The FHIR server landscape can be understood along two architectural dimensions: how tightly the implementation is coupled to its storage technology, and whether the system supports multiple specialized data stores or requires a single backend.

The vertical axis distinguishes between servers with **tightly-coupled persistence** where the implementation is deeply intertwined with a specific database technology, and those offering an **extensible interface layer** that abstracts storage concerns behind well-defined interfaces. A FHIR Server built directly on JPA (Java Persistence API) is such an example, meaning its data access patterns, query capabilities, and performance characteristics are fundamentally shaped by relational database assumptions. In contrast, an extensible interface layer defines traits or interfaces that can be implemented for any storage technology, allowing the same FHIR API to sit atop different backends without rewriting core logic.

The horizontal axis captures the difference between **single storage backend** architectures and **polyglot persistence**. Polyglot persistence is an architectural pattern where different types of data are routed to the storage technologies best suited for how that data will be accessed. For example, a polyglot system might store clinical documents in an object store optimized for large binary content, maintain patient relationships in a graph database for efficient traversal, and keep structured observations in a columnar store for fast analytical queries all while presenting a unified FHIR API to consuming applications. Most existing FHIR servers force all resources into a single database, sacrificing performance and flexibility for implementation simplicity.

The Helios FHIR Server occupies the upper-right quadrant: it combines a trait-based, open-source interface layer built in Rust with native support for polyglot persistence. This architecture allows organizations to optimize storage decisions for their specific access patterns while maintaining full FHIR compliance at the API layer.

## Decomposing the FHIR Specification: Separation of Concerns in Persistence Design

The FHIR specification is vast. It defines resource structures, REST interactions, search semantics, terminology operations, versioning behavior, and much more. A monolithic interface, or trait that attempts to capture all of this becomes unwieldy, difficult to implement, and impossible to optimize for specific storage technologies. The Helios FHIR Server persistence design takes a different approach: decompose the specification into cohesive concerns, express each as a focused trait, and compose them to build complete storage backends.
### Learning from Diesel: Type-Safe Database Abstractions

Before diving into our trait design, it's worth examining what we can learn from [Diesel](https://docs.diesel.rs/main/diesel/index.html), Rust's most mature database abstraction layer. Diesel has solved many of the problems we face - multi-backend support, compile-time query validation, extensibility, and its design choices offer valuable lessons.

**Backend Abstraction via Traits, Not Enums**: Diesel defines a `Backend` trait that captures the differences between database systems (PostgreSQL, MySQL, SQLite) without coupling to specific implementations. The `Backend` trait specifies how SQL is generated, how bind parameters are collected, and how types are mapped. This allows new backends to be added without modifying core code. This is exactly what we need for polyglot FHIR persistence.

**QueryFragment for Composable SQL Generation**: Diesel's `QueryFragment` trait represents any piece of SQL that can be rendered. A WHERE clause, a JOIN, an entire SELECT statement all implement `QueryFragment`. This composability lets complex queries be built from simple pieces. For FHIR search, we can adopt a similar pattern: each search parameter modifier becomes a fragment that can be composed into complete queries.

**Type-Level Query Validation**: Diesel catches many errors at compile time by encoding schema information in the type system. While we can't achieve the same level of compile-time validation for dynamic FHIR queries, we can use Rust's type system to ensure that storage backends only claim to support operations they actually implement.

**MultiConnection for Runtime Backend Selection**: Diesel's `#[derive(MultiConnection)]` generates an enum that wraps multiple connection types, dispatching operations to the appropriate backend at runtime. This pattern directly applies to polyglot persistence.  We can route FHIR operations to different backends based on query characteristics.

**Extensibility via sql_function! and Custom Types**: Diesel makes it trivial to add custom SQL functions and types. For FHIR, this translates to extensibility for custom search parameters, terminology operations, and backend-specific optimizations.

### The Core Resource Storage Trait

At the foundation is the `ResourceStorage` trait, which handles the fundamental persistence of FHIR resources. This trait intentionally knows nothing about search, nothing about REST semantics, nothing about transactions. It simply stores and retrieves resources by type and identifier.

Multitenancy is not optional in this design. Every operation requires a `TenantContext`, making it impossible at the type level to accidentally execute a query without tenant scoping. There is no "escape hatch" that bypasses tenant isolation.

```rust
use async_trait::async_trait;
use serde_json::Value;

/// Represents a stored FHIR resource with metadata.
pub struct StoredResource {
    pub resource_type: String,
    pub id: String,
    pub version_id: String,
    pub last_updated: chrono::DateTime<chrono::Utc>,
    pub tenant_id: TenantId,
    pub resource: Value,
}

/// Core trait for resource storage operations.
/// 
/// All operations are tenant-scoped. There is no non-tenant code path - 
/// the type system enforces that tenant context is always provided.
#[async_trait]
pub trait ResourceStorage: Send + Sync {
    /// Creates a new resource within a tenant's scope, assigning an ID if not provided.
    async fn create(
        &self,
        tenant: &TenantContext,
        resource: &Value,
    ) -> Result<StoredResource, StorageError>;

    /// Reads the current version of a resource within a tenant's scope.
    /// Returns NotFound if the resource exists but belongs to a different tenant.
    async fn read(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> Result<Option<StoredResource>, StorageError>;

    /// Updates a resource within a tenant's scope, returning the new version.
    async fn update(
        &self,
        tenant: &TenantContext,
        resource: &Value,
    ) -> Result<StoredResource, StorageError>;

    /// Deletes a resource within a tenant's scope (soft delete preserving history where supported).
    async fn delete(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> Result<(), StorageError>;

    /// Returns the storage backend identifier for logging and diagnostics.
    fn backend_name(&self) -> &'static str;
}
```

Notice what's absent: there's no `if_match` parameter for optimistic concurrency, no version-specific reads, no history. Those capabilities belong to separate traits that extend the base functionality. A storage backend that doesn't support versioning simply doesn't implement the versioning trait.
### Multitenancy: A Cross-Cutting Concern

Multitenancy has downstream implications for every layer of a FHIR server, from indexing strategy to reference validation to search semantics. By requiring tenant context at the lowest storage layer, we ensure that isolation guarantees propagate upward through the entire system.

**Isolation Strategies**

There are three fundamental approaches to tenant isolation, each with different trade-offs:

- **Database-per-tenant**: Strongest isolation, simplest security model, easier compliance story. The downside is operational overhead that grows linearly with tenants.  Connection pool management becomes complex, and schema migrations are painful at scale.

- **Schema-per-tenant**: Good isolation within a single database instance, allows tenant-specific indexing. PostgreSQL handles this well. Still has schema migration coordination challenges.

- **Shared schema with tenant discriminator**: Most operationally efficient at scale, single migration path. The risk is that every query must include tenant filtering.  One missed WHERE clause and you have a data breach.

For SQL-backed FHIR persistence, the shared schema approach with a `tenant_id` discriminator is pragmatic, but the enforcement layer must be airtight - you literally cannot construct a storage operation without providing tenant context.

**Tenant Context as a Type-Level Guarantee**

Borrowing from Diesel's approach to type safety, we can make tenant context explicit in the type system. Rather than passing tenant IDs as strings that might be forgotten, we create a wrapper type that must be present for any storage operation:

```rust
/// A validated tenant context. Operations that access tenant data
/// require this type, making it impossible to forget tenant filtering.
#[derive(Debug, Clone)]
pub struct TenantContext {
    tenant_id: TenantId,
    /// Permissions determine what operations are allowed
    permissions: TenantPermissions,
    /// Whether this context can access shared/system resources
    can_access_shared: bool,
}

/// The system tenant for shared resources (terminology, conformance)
pub const SYSTEM_TENANT: TenantId = TenantId::system();

/// Marker trait for operations that are tenant-scoped
pub trait TenantScoped {
    fn tenant(&self) -> &TenantContext;
}
```

**TenantId: Flexible Identifier Support**

The `TenantId` type is intentionally opaque to the storage layer, supporting both simple IDs and hierarchical namespaces:

```rust
/// A tenant identifier. Opaque to the storage layer—interpretation
/// of structure (flat vs. hierarchical) is left to the application.
///
/// # Examples
///
/// Simple flat identifiers:
/// ```
/// let tenant = TenantId::new("acme-health");
/// ```
///
/// Hierarchical namespaces:
/// ```
/// let tenant = TenantId::new("organization_id/hospital_id");
/// let tenant = TenantId::new("region:us-east/org:12345/facility:main");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TenantId(String);

impl TenantId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The system tenant, used for shared resources (terminology, conformance).
    pub const fn system() -> Self {
        Self(String::new())  // Or a sentinel like "__system__"
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns true if this is the system tenant.
    pub fn is_system(&self) -> bool {
        self.0.is_empty()  // Or check for sentinel
    }
}

impl From<&str> for TenantId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for TenantId {
    fn from(s: String) -> Self {
        Self(s)
    }
}
```

This approach keeps `TenantId` opaque at the storage layer while supporting simple string IDs, hierarchical namespaces like `org/hospital`, and leaving interpretation to the application layer. Index design may still care about hierarchy for prefix queries, but that's an implementation detail for specific backends.

**Shared Resources and the System Tenant**

CodeSystems, ValueSets, StructureDefinitions, and other conformance resources are typically shared across tenants. We designate a "system" tenant that holds these shared resources:

```rust
/// Determines whether a resource type should be tenant-specific or shared.
pub trait ResourceTenancy {
    /// Returns the tenancy model for a resource type.
    fn tenancy_model(&self, resource_type: &str) -> TenancyModel;
}

pub enum TenancyModel {
    /// Resource is always tenant-specific (e.g., Patient, Observation)
    TenantScoped,
    /// Resource is always shared (e.g., CodeSystem, ValueSet)
    Shared,
    /// Resource can be either, determined by business rules
    Configurable,
}
```

**Index Design for Multitenancy**

Search performance in a multitenant system depends critically on index design. The `tenant_id` must be the leading column in composite indexes:

```sql
-- Good: tenant_id leads, enabling efficient tenant-scoped queries
CREATE INDEX idx_patient_identifier ON patient (tenant_id, identifier_system, identifier_value);

-- Bad: tenant_id not leading, will scan all tenants
CREATE INDEX idx_patient_identifier ON patient (identifier_system, identifier_value, tenant_id);
```

### Versioning as a Separate Concern

FHIR's versioning model is sophisticated: every update creates a new version, version IDs are opaque strings, and the `vread` interaction retrieves historical versions. Not all storage backends can efficiently support this. An append-only data lake handles versioning naturally; a key-value store might not.

```rust
/// Adds version-aware operations to base storage.
#[async_trait]
pub trait VersionedStorage: ResourceStorage {
    /// Reads a specific version of a resource within a tenant's scope.
    async fn vread(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        version_id: &str,
    ) -> Result<Option<StoredResource>, StorageError>;

    /// Updates with optimistic concurrency control.
    /// Fails with VersionConflict if current version doesn't match expected.
    async fn update_with_match(
        &self,
        tenant: &TenantContext,
        resource: &Value,
        expected_version: &str,
    ) -> Result<StoredResource, StorageError>;
}

```

### History: Building on Versioning

History access naturally extends versioning. If a backend can read specific versions, it can also enumerate them. We decompose history into progressively broader scopes:

```rust
/// Instance-level history only.
#[async_trait]
pub trait InstanceHistoryProvider: VersionedStorage {
    /// Returns the history of a specific resource within a tenant's scope.
    async fn history_instance(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        params: &HistoryParams,
    ) -> Result<HistoryBundle, StorageError>;
}

/// Adds type-level history.
#[async_trait]
pub trait TypeHistoryProvider: InstanceHistoryProvider {
    /// Returns the history of all resources of a type within a tenant's scope.
    async fn history_type(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        params: &HistoryParams,
    ) -> Result<HistoryBundle, StorageError>;
}

/// Adds system-level history.
#[async_trait]
pub trait SystemHistoryProvider: TypeHistoryProvider {
    /// Returns the history of all resources within a tenant's scope.
    async fn history_system(
        &self,
        tenant: &TenantContext,
        params: &HistoryParams,
    ) -> Result<HistoryBundle, StorageError>;
}

/// Parameters for history queries, matching FHIR's _since, _at, _count parameters.
pub struct HistoryParams {
    pub since: Option<chrono::DateTime<chrono::Utc>>,
    pub at: Option<chrono::DateTime<chrono::Utc>>,
    pub count: Option<usize>,
}
```

The trait hierarchy forms a progression: `SystemHistoryProvider: TypeHistoryProvider: InstanceHistoryProvider: VersionedStorage: ResourceStorage`. A backend that supports system-level history automatically supports all narrower scopes. A simpler backend might only implement `InstanceHistoryProvider`, indicating it can return history for individual resources but not enumerate all versions across a type or the entire system.

### The Search Abstraction: Decomposing FHIR's Query Model

Search is where the FHIR specification becomes genuinely complex. There are eight search parameter types (number, date, string, token, reference, quantity, uri, composite), sixteen modifiers (`:exact`, `:contains`, `:not`, `:missing`, `:above`, `:below`, `:in`, `:not-in`, `:of-type`, `:identifier`, `:text`, `:code-text`, `:text-advanced`, `:iterate`, plus resource type modifiers on references), six comparison prefixes (`eq`, `ne`, `lt`, `le`, `gt`, `ge`, `sa`, `eb`, `ap`), chained parameters, reverse chaining (`_has`), `_include` and `_revinclude` directives, and advanced filtering via `_filter`. A single search query can combine all of these all while respecting tenant boundaries.

Modeling search as a single trait would be a mistake. Instead, we decompose it into layers - and here, Diesel's `QueryFragment` pattern proves invaluable.

#### The SearchFragment Pattern (Inspired by Diesel's QueryFragment)

Diesel's `QueryFragment` trait allows any piece of SQL to be composable. We adapt this pattern for FHIR search, creating fragments that can be combined into complete search queries:

```rust
/// A fragment of a FHIR search that can be rendered to a backend-specific query.
/// Inspired by Diesel's QueryFragment pattern.
pub trait SearchFragment<B: SearchBackend> {
    /// Renders this fragment to the backend's query representation.
    fn apply(&self, builder: &mut B::QueryBuilder) -> Result<(), SearchError>;
    
    /// Whether this fragment can be efficiently evaluated by the backend.
    /// Returns false if the backend would need to do post-filtering.
    fn is_native(&self, backend: &B) -> bool;
    
    /// Estimated cost of evaluating this fragment (for query planning).
    fn estimated_cost(&self, backend: &B) -> QueryCost;
}

/// A search backend that can evaluate SearchFragments.
pub trait SearchBackend: Send + Sync {
    type QueryBuilder;
    type QueryResult;

    /// Creates a query builder for one or more resource types.
    /// If `resource_types` is None, searches all resource types.
    /// If `resource_types` is Some with multiple types, parameters must be common across all.
    fn query_builder(&self, resource_types: Option<&[&str]>) -> Self::QueryBuilder;

    /// Executes a built query.
    async fn execute(&self, query: Self::QueryBuilder) -> Result<Self::QueryResult, SearchError>;
}
```

Each search modifier becomes a fragment that knows how to render itself:

```rust
/// Fragment for the :exact modifier on string parameters.
pub struct ExactStringMatch {
    pub parameter: String,
    pub path: FhirPath,
    pub value: String,
}

impl<B: SearchBackend> SearchFragment<B> for ExactStringMatch 
where
    B: SupportsExactMatch,
{
    fn apply(&self, builder: &mut B::QueryBuilder) -> Result<(), SearchError> {
        builder.add_exact_string_match(&self.path, &self.value)
    }
    
    fn is_native(&self, _backend: &B) -> bool {
        true  // Most backends support exact string matching natively
    }
    
    fn estimated_cost(&self, backend: &B) -> QueryCost {
        backend.cost_for_exact_match(&self.path)
    }
}

/// Fragment for the :above modifier on token parameters (terminology subsumption).
pub struct SubsumesMatch {
    pub parameter: String,
    pub path: FhirPath,
    pub system: String,
    pub code: String,
}

impl<B: SearchBackend> SearchFragment<B> for SubsumesMatch 
where
    B: SupportsTerminologySearch,
{
    fn apply(&self, builder: &mut B::QueryBuilder) -> Result<(), SearchError> {
        builder.add_subsumes_match(&self.path, &self.system, &self.code)
    }
    
    fn is_native(&self, backend: &B) -> bool {
        // Only native if the backend has integrated terminology support
        backend.has_native_terminology()
    }
    
    fn estimated_cost(&self, backend: &B) -> QueryCost {
        if self.is_native(backend) {
            backend.cost_for_subsumption(&self.path)
        } else {
            QueryCost::RequiresExpansion  // Will need to expand the code set first
        }
    }
}
```

#### Search Parameter Types

First, we model the search parameter types and their associated matching logic:

```rust
/// The type of a search parameter, determining matching semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchParamType {
    Number,
    Date,
    String,
    Token,
    Reference,
    Quantity,
    Uri,
    Composite,
    Special,
}

/// Comparison prefixes for ordered types (number, date, quantity).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SearchPrefix {
    #[default]
    Eq,  // equals (default)
    Ne,  // not equals
    Lt,  // less than
    Le,  // less than or equals
    Gt,  // greater than
    Ge,  // greater than or equals
    Sa,  // starts after
    Eb,  // ends before
    Ap,  // approximately
}

/// Modifiers that alter search behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchModifier {
    Exact,           // String: case-sensitive, full match
    Contains,        // String: substring match
    Text,            // Token/Reference: search display text
    TextAdvanced,    // Token/Reference: advanced text search
    CodeText,        // Token: search code text
    Not,             // Token: negation
    Missing(bool),   // All: test for presence/absence
    Above,           // Token/Reference/Uri: hierarchical above
    Below,           // Token/Reference/Uri: hierarchical below
    In,              // Token: value set membership
    NotIn,           // Token: value set non-membership
    OfType,          // Token (Identifier): type-qualified search
    Identifier,      // Reference: search by identifier
    Type(String),    // Reference: restrict to resource type
    Iterate,         // _include/_revinclude: recursive inclusion
}
```

#### The Core Search Trait

The base search trait handles fundamental query execution without advanced features:

```rust
/// A parsed search parameter with its value and modifiers.
#[derive(Debug, Clone)]
pub struct SearchParameter {
    pub name: String,
    pub param_type: SearchParamType,
    pub modifier: Option<SearchModifier>,
    pub prefix: Option<SearchPrefix>,
    pub values: Vec<String>,  // Multiple values = OR
}

/// A complete search query with all parameters.
#[derive(Debug, Clone, Default)]
pub struct SearchQuery {
    /// Filter parameters (AND-joined)
    pub parameters: Vec<SearchParameter>,
    /// Sort specifications
    pub sort: Vec<SortSpec>,
    /// Pagination (cursor-based preferred, offset for compatibility)
    pub pagination: Option<Pagination>,
    /// Result modifiers
    pub summary: Option<SummaryMode>,
    pub elements: Option<Vec<String>>,
    /// Include directives
    pub include: Vec<IncludeSpec>,
    pub revinclude: Vec<IncludeSpec>,
}

/// Pagination strategy for search results.
#[derive(Debug, Clone)]
pub enum Pagination {
    /// Offset-based (supported for compatibility, discouraged for large result sets).
    Offset { count: u32, offset: u32 },

    /// Cursor-based (preferred). The cursor is opaque to clients.
    Cursor { count: u32, cursor: Option<PageCursor> },
}

/// An opaque pagination cursor. Internal structure is backend-specific.
///
/// For Cassandra, this wraps the paging state.
/// For PostgreSQL, this might encode a keyset (e.g., last seen `_lastUpdated` + `_id`).
/// For Elasticsearch, this wraps a search_after value or scroll ID.
#[derive(Debug, Clone)]
pub struct PageCursor(Vec<u8>);

impl PageCursor {
    pub fn new(data: impl Into<Vec<u8>>) -> Self {
        Self(data.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Encodes the cursor for inclusion in a URL (base64).
    pub fn encode(&self) -> String {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
        URL_SAFE_NO_PAD.encode(&self.0)
    }

    /// Decodes a cursor from a URL parameter.
    pub fn decode(s: &str) -> Result<Self, PaginationError> {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
        let bytes = URL_SAFE_NO_PAD.decode(s)
            .map_err(|_| PaginationError::InvalidCursor)?;
        Ok(Self(bytes))
    }
}

/// Search results with pagination support.
pub struct SearchBundle {
    pub matches: Vec<StoredResource>,
    pub included: Vec<StoredResource>,
    pub total: Option<u64>,
    /// Cursor for the next page, if more results exist.
    pub next_cursor: Option<PageCursor>,
}

Cursor-based pagination avoids the O(N) complexity and data drift issues of offset-based pagination. Instead of "skip N rows," the server returns an opaque continuation token encoding the current position. The client passes this token to fetch the next page, and the database seeks directly to that position—O(1) regardless of depth.

| Backend | Cursor Contents |
|---------|-----------------|
| **Cassandra** | Native paging state from the driver |
| **PostgreSQL** | Keyset values: `(_lastUpdated, _id)` of last row |
| **MongoDB** | `_id` of last document, or resume token |
| **Elasticsearch** | `search_after` values, or scroll ID for deep pagination |
| **S3/Parquet** | Continuation token from `ListObjectsV2` |

**Keyset Pagination for SQL Backends**

For PostgreSQL and other relational databases, keyset pagination (also called "seek method") provides efficient cursor-based paging:

```sql
-- First page
SELECT * FROM observation
WHERE tenant_id = $1
ORDER BY last_updated DESC, id ASC
LIMIT 100;

-- Subsequent pages (cursor contains last_updated and id of final row)
SELECT * FROM observation
WHERE tenant_id = $1
  AND (last_updated, id) < ($last_updated, $last_id)
ORDER BY last_updated DESC, id ASC
LIMIT 100;
```

This requires a stable sort order with a unique tiebreaker (typically `_id`). The cursor encodes the sort key values of the last row returned.

The `_cursor` parameter appears in FHIR Bundle `next` links and is a server-specific extension. Clients should treat pagination URLs as opaque and simply follow the `next` link, as described in the [FHIR spec](https://fhir.hl7.org/fhir/http.html#paging).

```rust
/// Core search: execute a query, return matches.
#[async_trait]
pub trait SearchProvider: ResourceStorage {
    /// Executes a search query against a resource type within a tenant's scope.
    async fn search(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        query: &SearchQuery,
    ) -> Result<SearchBundle, StorageError>;
}

/// Adds multi-type search capability.
#[async_trait]
pub trait MultiTypeSearchProvider: SearchProvider {
    /// Searches multiple resource types (or all if None).
    /// Parameters must be common across all searched types.
    async fn search_multi(
        &self,
        tenant: &TenantContext,
        resource_types: Option<&[&str]>,
        query: &SearchQuery,
    ) -> Result<SearchBundle, StorageError>;
}

/// Adds _include support.
#[async_trait]
pub trait IncludeProvider: SearchProvider {
    /// Resolves _include directives for a set of matched resources.
    async fn resolve_includes(
        &self,
        tenant: &TenantContext,
        matches: &[StoredResource],
        includes: &[IncludeSpec],
    ) -> Result<Vec<StoredResource>, StorageError>;
}

/// Adds _revinclude support.
#[async_trait]
pub trait RevincludeProvider: SearchProvider {
    /// Resolves _revinclude directives for a set of matched resources.
    async fn resolve_revincludes(
        &self,
        tenant: &TenantContext,
        matches: &[StoredResource],
        revincludes: &[IncludeSpec],
    ) -> Result<Vec<StoredResource>, StorageError>;
}
```

#### Advanced Search Capabilities as Extension Traits

Not every storage backend can support every search feature. A relational database might handle token searches efficiently but struggle with subsumption queries that require terminology reasoning. A vector database might excel at text search but lack native support for date range queries. We model these variations as extension traits.
##### Chained Search Provider:

```rust
/// Adds support for chained parameter searches.
/// 
/// Chaining allows searching by properties of referenced resources,
/// e.g., `Observation?patient.name=Smith`. This typically requires
/// join operations or graph traversal and must respect tenant boundaries
/// when following references.
#[async_trait]
pub trait ChainedSearchProvider: SearchProvider {
    /// Executes a search with chained parameters within a tenant's scope.
    /// 
    /// The implementation must ensure that chained references do not
    /// cross tenant boundaries except for shared resources.
    async fn search_with_chain(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        chain: &ChainedParameter,
        terminal_condition: &SearchCondition,
    ) -> Result<SearchBundle, StorageError>;

    /// Executes a reverse chain (_has) search within a tenant's scope.
    /// 
    /// Finds resources that are referenced by other resources matching
    /// the given criteria, respecting tenant isolation.
    async fn search_reverse_chain(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        has_param: &HasParameter,
    ) -> Result<SearchBundle, StorageError>;

    /// Returns the maximum chain depth supported by this backend.
    fn max_chain_depth(&self) -> usize {
        4 // Reasonable default; deep chains are expensive
    }
}

/// Represents a chained parameter like `patient.organization.name`
pub struct ChainedParameter {
    /// The chain of reference parameters to follow
    pub chain: Vec<ChainLink>,
}

pub struct ChainLink {
    /// The search parameter name (must be a reference type)
    pub parameter: String,
    /// Optional type restriction for polymorphic references
    pub target_type: Option<String>,
}

/// Represents a _has parameter for reverse chaining
pub struct HasParameter {
    /// The resource type that references us
    pub referencing_type: String,
    /// The reference parameter on that type pointing to us
    pub reference_param: String,
    /// The condition to apply to the referencing resource
    pub condition: SearchCondition,
    /// Nested _has for multi-level reverse chains
    pub nested: Option<Box<HasParameter>>,
}
```

##### Terminology Search Provider:

```rust
/// Adds terminology-aware search capabilities.
/// 
/// Supports the `:above`, `:below`, `:in`, and `:not-in` modifiers
/// which require understanding of code system hierarchies and value set
/// membership. Terminology resources are typically shared across tenants,
/// but the search itself is tenant-scoped.
#[async_trait]
pub trait TerminologySearchProvider: SearchProvider {
    /// Expands a code using `:below` semantics (descendants) within tenant scope.
    /// 
    /// Returns all codes subsumed by the given code. These codes are then
    /// used to filter resources belonging to the specified tenant.
    async fn expand_below(
        &self,
        tenant: &TenantContext,
        system: &str,
        code: &str,
    ) -> Result<Vec<ExpandedCode>, StorageError>;

    /// Expands a code using `:above` semantics (ancestors) within tenant scope.
    async fn expand_above(
        &self,
        tenant: &TenantContext,
        system: &str,
        code: &str,
    ) -> Result<Vec<ExpandedCode>, StorageError>;

    /// Checks value set membership for `:in` modifier within tenant scope.
    /// 
    /// The value set itself may be shared or tenant-specific; the implementation
    /// must resolve the correct value set based on tenant context.
    async fn check_membership(
        &self,
        tenant: &TenantContext,
        valueset_url: &str,
        system: &str,
        code: &str,
    ) -> Result<bool, StorageError>;

    /// Expands a value set to all member codes within tenant scope.
    /// 
    /// Used for `:in` searches when the backend can efficiently filter
    /// by an expanded code list.
    async fn expand_valueset(
        &self,
        tenant: &TenantContext,
        valueset_url: &str,
    ) -> Result<Vec<ExpandedCode>, StorageError>;
}

pub struct ExpandedCode {
    pub system: String,
    pub code: String,
    pub display: Option<String>,
}
```

##### Text Search Provider:

```rust
/// Adds full-text search capabilities.
/// 
/// Supports `_text` (narrative search) and `_content` (full resource search)
/// parameters, as well as the `:text` modifier on string parameters.
/// All searches are scoped to the specified tenant.
#[async_trait]
pub trait TextSearchProvider: SearchProvider {
    /// Searches resource narratives within a tenant's scope.
    /// 
    /// Matches against the XHTML content in `Resource.text.div`.
    async fn search_text(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        text_query: &str,
        additional_params: &SearchQuery,
    ) -> Result<SearchBundle, StorageError>;

    /// Searches full resource content within a tenant's scope.
    /// 
    /// Matches against all string content in the resource JSON.
    async fn search_content(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        content_query: &str,
        additional_params: &SearchQuery,
    ) -> Result<SearchBundle, StorageError>;

    /// Executes a text search on a specific parameter within tenant scope.
    /// 
    /// Used for the `:text` modifier on string and token parameters,
    /// e.g., `Condition?code:text=heart attack`.
    async fn search_parameter_text(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        parameter: &str,
        text_query: &str,
    ) -> Result<SearchBundle, StorageError>;
}
```

This decomposition has practical consequences. When configuring a polyglot persistence layer, we can route terminology-aware searches to a backend that integrates with a terminology server (perhaps backed by a graph database), while directing simple token matches to a faster document store. The trait system makes these routing decisions explicit and type-safe.

### Transactions: When Atomicity Matters

FHIR defines batch and transaction bundles. A batch processes entries independently; a transaction either succeeds completely or fails entirely with no partial effects. This all-or-nothing semantics requires database-level transaction support - something not all storage technologies provide natively.

```rust
/// Locking strategy for a transaction.
#[derive(Debug, Clone, Copy, Default)]
pub enum LockingStrategy {
    /// Pessimistic: acquire locks on read, hold until commit.
    /// Guarantees success at commit time but may cause contention.
    #[default]
    Pessimistic,

    /// Optimistic: track read versions, verify at commit.
    /// Better concurrency but may fail at commit time.
    Optimistic,
}

#[derive(Debug, Clone, Default)]
pub struct TransactionOptions {
    pub locking: LockingStrategy,
    pub isolation: IsolationLevel,
    /// Timeout for acquiring locks (pessimistic) or total transaction duration.
    pub timeout: Option<std::time::Duration>,
}

/// Provides ACID transaction support.
///
/// Transactions group multiple operations into an atomic unit. All
/// operations within a transaction are tenant-scoped; a single transaction
/// cannot span multiple tenants.
#[async_trait]
pub trait TransactionProvider: ResourceStorage {
    /// Begins a transaction with the default (pessimistic) locking strategy.
    async fn begin_transaction(
        &self,
        tenant: &TenantContext,
    ) -> Result<Box<dyn Transaction>, StorageError> {
        self.begin_transaction_with_options(tenant, TransactionOptions::default()).await
    }

    /// Begins a transaction with explicit options.
    async fn begin_transaction_with_options(
        &self,
        tenant: &TenantContext,
        options: TransactionOptions,
    ) -> Result<Box<dyn Transaction>, StorageError>;
}

/// Core transaction capability: atomic CRUD operations.
///
/// Operations within a transaction see their own uncommitted changes
/// but are isolated from concurrent transactions.
#[async_trait]
pub trait Transaction: Send + Sync {
    /// Returns the tenant context for this transaction.
    fn tenant(&self) -> &TenantContext;

    /// Creates a resource within this transaction.
    async fn create(&mut self, resource: &Value) -> Result<StoredResource, StorageError>;

    /// Reads a resource within this transaction (sees uncommitted changes).
    async fn read(
        &self,
        resource_type: &str,
        id: &str,
    ) -> Result<Option<StoredResource>, StorageError>;

    /// Updates a resource within this transaction.
    async fn update(&mut self, resource: &Value) -> Result<StoredResource, StorageError>;

    /// Deletes a resource within this transaction.
    async fn delete(&mut self, resource_type: &str, id: &str) -> Result<(), StorageError>;

    /// Commits all operations in this transaction atomically.
    async fn commit(self: Box<Self>) -> Result<(), StorageError>;

    /// Rolls back all operations in this transaction.
    async fn rollback(self: Box<Self>) -> Result<(), StorageError>;
}

/// Indicates this transaction uses optimistic locking.
///
/// Reads automatically track versions; commit fails if any
/// tracked resource was modified by another transaction.
pub trait OptimisticTransaction: Transaction {
    fn locking_strategy(&self) -> LockingStrategy {
        LockingStrategy::Optimistic
    }
}

/// Adds conditional operations to transactions.
#[async_trait]
pub trait ConditionalTransaction: Transaction {
    /// Conditional create: creates only if search returns no matches.
    async fn create_if_none_exist(
        &mut self,
        resource: &Value,
        search_params: &[SearchParameter],
    ) -> Result<ConditionalOutcome, StorageError>;

    /// Conditional update: updates the resource matching search criteria.
    async fn update_conditional(
        &mut self,
        resource: &Value,
        search_params: &[SearchParameter],
    ) -> Result<ConditionalOutcome, StorageError>;

    /// Conditional delete: deletes resources matching search criteria.
    async fn delete_conditional(
        &mut self,
        resource_type: &str,
        search_params: &[SearchParameter],
    ) -> Result<ConditionalDeleteOutcome, StorageError>;
}

#[derive(Debug)]
pub enum ConditionalOutcome {
    /// Resource was created.
    Created(StoredResource),
    /// Resource already existed (for conditional create).
    Existed { id: String, version: String },
    /// Multiple matches found (error condition).
    MultipleMatches { count: usize },
}

#[derive(Debug)]
pub enum ConditionalDeleteOutcome {
    /// No resources matched (not an error).
    NoneMatched,
    /// Single resource deleted.
    Deleted { id: String },
    /// Multiple resources deleted (if server policy allows).
    DeletedMultiple { count: usize },
}

#[derive(Debug, Clone, Copy, Default)]
pub enum IsolationLevel {
    #[default]
    ReadCommitted,
    RepeatableRead,
    Serializable,
}
```

**Optimistic vs. Pessimistic Locking**

The base `Transaction` trait supports both locking strategies:

| Scenario | Recommended Strategy |
|----------|---------------------|
| Short-lived transactions, high contention | Pessimistic |
| Long-lived transactions, rare conflicts | Optimistic |
| FHIR transaction bundles (typically small) | Pessimistic |
| User-facing "edit and save" workflows | Optimistic |
| Batch processing with known non-overlapping data | Either |
| Distributed/multi-region deployments | Optimistic (locks don't span regions well) |

With optimistic locking, the transaction tracks which resources were read and their versions. At commit time, it verifies they haven't changed. If any resource was modified by another transaction, the commit fails with `StorageError::OptimisticLockFailure` and the client retries.

**Error Handling for Optimistic Lock Failures**

When an optimistic transaction fails at commit time, the client needs enough information to retry intelligently:

```rust
#[derive(Debug)]
pub enum StorageError {
    // ... other variants ...

    /// Optimistic lock failure—a tracked resource was modified.
    OptimisticLockFailure {
        /// Resources that changed since they were read.
        conflicts: Vec<ConflictInfo>,
    },

    /// Version conflict on a single resource update.
    VersionConflict {
        resource_type: String,
        id: String,
        expected_version: String,
        actual_version: String,
    },
}

#[derive(Debug)]
pub struct ConflictInfo {
    pub resource_type: String,
    pub id: String,
    pub read_version: String,
    pub current_version: String,
}
```

**Interaction with FHIR Conditional Operations**

FHIR defines conditional create, update, and delete operations that use search criteria rather than explicit IDs. These interact with transaction locking:

| Operation | Pessimistic | Optimistic |
|-----------|-------------|------------|
| **Simple CRUD** | Lock acquired on access | Version tracked on read, verified on commit |
| **Conditional Create** | Lock search space | Assert "no matches" at commit |
| **Conditional Update** | Lock matched resource | Assert same match + version at commit |
| **Conditional Delete** | Lock matched resources | Assert same match set at commit |
| **If-Match on entry** | Lock + verify version | Verify version at commit |
| **Commit failure** | Rare (deadlock, timeout) | Expected under contention |
| **Retry strategy** | Usually unnecessary | Built into application logic |

**Version-Aware Updates in Transaction Bundles**

FHIR transaction bundles support `If-Match` on individual entries for optimistic concurrency:

```json
{
  "resourceType": "Bundle",
  "type": "transaction",
  "entry": [
    {
      "resource": { "resourceType": "Patient", "id": "123", ... },
      "request": {
        "method": "PUT",
        "url": "Patient/123",
        "ifMatch": "W/\"2\""
      }
    }
  ]
}
```

The transaction processor must respect these per-entry version constraints regardless of the overall transaction locking strategy:

```rust
/// Request metadata for a transaction bundle entry.
#[derive(Debug, Clone)]
pub struct TransactionEntryRequest {
    pub method: TransactionMethod,
    pub url: String,
    /// If-Match header value for optimistic concurrency on this entry.
    pub if_match: Option<String>,
    /// If-None-Match header value (for conditional create).
    pub if_none_match: Option<String>,
    /// If-None-Exist search parameters (for conditional create).
    pub if_none_exist: Option<Vec<SearchParameter>>,
}

#[derive(Debug, Clone, Copy)]
pub enum TransactionMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}
```

**Implementation Considerations**

1. **Search result stability**: For conditional operations with optimistic locking, the implementation must track not just "which resources matched" but enough information to verify the search would return the same results at commit time. This is complex for queries with sorting or pagination.

2. **Phantom reads**: Even with optimistic locking, a conditional delete might miss a resource that was created after the search but before commit. The isolation level determines whether this is acceptable.

3. **Backend capabilities**: Not all backends can efficiently implement optimistic conditional operations. The capability system should expose this:

```rust
pub trait StorageCapabilities {
    // ... existing methods ...

    /// Returns supported locking strategies.
    fn supported_locking_strategies(&self) -> Vec<LockingStrategy>;

    /// Returns whether conditional operations are supported within transactions.
    fn supports_conditional_in_transaction(&self) -> bool;

    /// Returns whether optimistic locking on conditional operations is supported.
    fn supports_optimistic_conditional(&self) -> bool;
}
```

4. **FHIR transaction bundle semantics**: The FHIR spec requires that a transaction bundle either fully succeeds or fully fails with no partial effects. Both locking strategies satisfy this—pessimistic by holding locks, optimistic by aborting on conflict. The choice affects throughput and failure modes, not correctness.

A storage backend that doesn't support transactions can still handle batch operations.  It simply processes each entry independently, accepting that failures may leave partial results. The trait separation makes this distinction clear: code that requires atomicity takes `&dyn TransactionProvider`, while code that can tolerate partial failures takes `&dyn ResourceStorage`.

### Audit Events: A Separated Persistence Store

AuditEvent resources should be ideally stored separately from clinical data. This isn't just a security concern, it's also an architectural one. Audit logs have different access patterns (append-heavy, rarely queried except during investigations), different retention requirements (often longer than clinical data), and different security constraints (must be tamper-evident, may require separate access controls).

```rust
/// Specialized storage for audit events.
/// 
/// Audit storage is intentionally separate from clinical data storage.
/// It typically has different characteristics:
/// - Append-only or append-heavy workload
/// - Different retention policies
/// - Tamper-evident storage requirements
/// - Separate access control
#[async_trait]
pub trait AuditStorage: Send + Sync {
    /// Records an audit event. This operation should be highly available
    /// and should not fail clinical operations if audit storage is degraded.
    async fn record(&self, tenant: &TenantContext, event: &AuditEvent) -> Result<String, AuditError>;

    /// Queries audit events within a time range.
    async fn query(
        &self,
        criteria: &AuditQuery,
    ) -> Result<Vec<AuditEvent>, AuditError>;

    /// Retrieves audit events for a specific resource (accounting of disclosures).
    async fn disclosures_for_resource(
        &self,
        resource_type: &str,
        resource_id: &str,
        period: &DateRange,
    ) -> Result<Vec<AuditEvent>, AuditError>;
}

/// Audit query criteria supporting HIPAA accounting requirements.
#[derive(Debug, Clone, Default)]
pub struct AuditQuery {
    pub patient_id: Option<String>,
    pub agent_id: Option<String>,
    pub action: Option<AuditAction>,
    pub period: Option<DateRange>,
    pub resource_type: Option<String>,
    pub outcome: Option<AuditOutcome>,
}
```

The separation of `AuditStorage` from `ResourceStorage` enables critical architectural flexibility. Audit events can flow to a dedicated time-series database optimized for append-only writes, or to an immutable ledger for tamper evidence, or to a separate cloud account for security isolation.

### The REST Layer: Mapping HTTP to Storage

The FHIR REST API defines interactions (read, vread, update, create, delete, search, etc.) that map HTTP verbs and URL patterns to operations. This mapping is a separate concern from storage. The same storage backend might be accessed via REST, GraphQL, messaging, or bulk export.

```rust
/// Result of a read operation.
pub struct ReadOutcome {
    pub resource: Option<StoredResource>,
    pub etag: Option<String>,
    pub last_modified: Option<chrono::DateTime<chrono::Utc>>,
}

/// Result of a write operation.
pub struct WriteOutcome {
    pub resource: StoredResource,
    pub created: bool,  // true for create, false for update
    pub etag: String,
    pub last_modified: chrono::DateTime<chrono::Utc>,
    pub location: Option<String>,
}

/// Result of a delete operation.
pub struct DeleteOutcome {
    pub deleted: bool,
    pub version_id: Option<String>,
}

/// Result of a search operation.
pub struct SearchOutcome {
    pub bundle: SearchBundle,
    pub self_link: String,
    pub next_link: Option<String>,
}

/// Result of a history operation.
pub struct HistoryOutcome {
    pub bundle: HistoryBundle,
    pub self_link: String,
    pub next_link: Option<String>,
}

/// Handles FHIR REST interactions by coordinating storage traits.
///
/// This trait maps HTTP semantics to storage operations. Implementations
/// handle concerns like ETag generation, Location headers, and
/// OperationOutcome construction.
#[async_trait]
pub trait RestHandler: Send + Sync {
    // === Instance-level interactions ===

    /// Reads the current version of a resource.
    /// Maps to: GET [base]/[type]/[id]
    async fn read(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> Result<ReadOutcome, RestError>;

    /// Reads a specific version of a resource.
    /// Maps to: GET [base]/[type]/[id]/_history/[vid]
    async fn vread(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        version_id: &str,
    ) -> Result<ReadOutcome, RestError>;

    /// Updates a resource, optionally with version matching.
    /// Maps to: PUT [base]/[type]/[id]
    async fn update(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        resource: &Value,
        if_match: Option<&str>,
    ) -> Result<WriteOutcome, RestError>;

    /// Patches a resource using JSON Patch, JSON Merge Patch, or FHIRPath Patch.
    /// Maps to: PATCH [base]/[type]/[id]
    async fn patch(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        patch: &Patch,
        if_match: Option<&str>,
    ) -> Result<WriteOutcome, RestError>;

    /// Deletes a resource.
    /// Maps to: DELETE [base]/[type]/[id]
    async fn delete(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> Result<DeleteOutcome, RestError>;

    /// Returns the history of a specific resource.
    /// Maps to: GET [base]/[type]/[id]/_history
    async fn history_instance(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        params: &HistoryParams,
    ) -> Result<HistoryOutcome, RestError>;

    // === Type-level interactions ===

    /// Creates a new resource with server-assigned ID.
    /// Maps to: POST [base]/[type]
    async fn create(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        resource: &Value,
        if_none_exist: Option<&[SearchParameter]>,
    ) -> Result<WriteOutcome, RestError>;

    /// Searches for resources of a given type.
    /// Maps to: GET [base]/[type]?params or POST [base]/[type]/_search
    async fn search(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        query: &SearchQuery,
    ) -> Result<SearchOutcome, RestError>;

    /// Returns the history of all resources of a type.
    /// Maps to: GET [base]/[type]/_history
    async fn history_type(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        params: &HistoryParams,
    ) -> Result<HistoryOutcome, RestError>;

    // === Conditional interactions ===

    /// Conditional update: updates based on search criteria.
    /// Maps to: PUT [base]/[type]?search-params
    async fn update_conditional(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        resource: &Value,
        search_params: &[SearchParameter],
        if_match: Option<&str>,
    ) -> Result<WriteOutcome, RestError>;

    /// Conditional delete: deletes based on search criteria.
    /// Maps to: DELETE [base]/[type]?search-params
    async fn delete_conditional(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        search_params: &[SearchParameter],
    ) -> Result<DeleteOutcome, RestError>;

    // === System-level interactions ===

    /// Searches across all resource types.
    /// Maps to: GET [base]?params
    async fn search_system(
        &self,
        tenant: &TenantContext,
        resource_types: Option<&[&str]>,
        query: &SearchQuery,
    ) -> Result<SearchOutcome, RestError>;

    /// Returns the history of all resources.
    /// Maps to: GET [base]/_history
    async fn history_system(
        &self,
        tenant: &TenantContext,
        params: &HistoryParams,
    ) -> Result<HistoryOutcome, RestError>;

    /// Returns the server's capability statement.
    /// Maps to: GET [base]/metadata
    async fn capabilities(&self) -> Result<Value, RestError>;

    // === Bundle interactions ===

    /// Processes a batch bundle (independent operations).
    /// Maps to: POST [base] with Bundle.type = "batch"
    async fn batch(
        &self,
        tenant: &TenantContext,
        bundle: &Value,
    ) -> Result<Value, RestError>;

    /// Processes a transaction bundle (atomic operations).
    /// Maps to: POST [base] with Bundle.type = "transaction"
    async fn transaction(
        &self,
        tenant: &TenantContext,
        bundle: &Value,
    ) -> Result<Value, RestError>;
}
```

The `RestHandler` is a coordination layer that combines multiple storage traits to implement FHIR REST semantics. A read interaction needs only `ResourceStorage`. A vread needs `VersionedStorage`. A search with `_include` needs both `SearchProvider` and `ResourceStorage`. The REST handler composes these capabilities based on what the request requires and what the storage backend provides.

### Capability Statements: Documenting What Storage Supports

The FHIR specification requires servers to publish a CapabilityStatement declaring which interactions, resources, and search parameters they support. When storage backends have different capabilities, this statement must accurately reflect the union of what's available and identify gaps.

Diesel solves a similar problem with its type system.  Operations that aren't supported simply don't compile. For FHIR, we need runtime capability discovery because queries are dynamic. We model storage capabilities as a queryable trait that can generate CapabilityStatement fragments:

```rust
/// Declares the capabilities of a storage backend.
/// Inspired by Diesel's approach to backend-specific features.
pub trait StorageCapabilities {
    /// Returns supported interactions for a resource type.
    fn supported_interactions(&self, resource_type: &str) -> Vec<Interaction>;

    /// Returns supported search parameters for a resource type.
    fn supported_search_params(&self, resource_type: &str) -> Vec<SearchParamCapability>;

    /// Returns supported search modifiers for a parameter type.
    fn supported_modifiers(&self, param_type: SearchParamType) -> Vec<SearchModifier>;

    /// Returns whether chained search is supported.
    fn supports_chaining(&self) -> bool;

    /// Returns whether reverse chaining (_has) is supported.
    fn supports_reverse_chaining(&self) -> bool;

    /// Returns whether _include is supported.
    fn supports_include(&self) -> bool;

    /// Returns whether _revinclude is supported.
    fn supports_revinclude(&self) -> bool;

    /// Returns supported transaction isolation levels.
    fn supported_isolation_levels(&self) -> Vec<IsolationLevel>;

    /// Generates a FHIR CapabilityStatement fragment for this backend.
    fn to_capability_statement(&self) -> Value;
}

/// Describes support for a specific search parameter.
#[derive(Debug, Clone)]
pub struct SearchParamCapability {
    pub name: String,
    pub param_type: SearchParamType,
    pub modifiers: Vec<SearchModifier>,
    pub prefixes: Vec<SearchPrefix>,
    pub documentation: Option<String>,
}

/// Marker traits for optional capabilities, enabling compile-time 
/// capability checking where possible (similar to Diesel's backend features).
pub trait SupportsExactMatch: SearchBackend {}
pub trait SupportsContainsMatch: SearchBackend {}
pub trait SupportsTerminologySearch: SearchBackend {}
pub trait SupportsFullTextSearch: SearchBackend {}
pub trait SupportsChainedSearch: SearchBackend {}
pub trait SupportsReverseChaining: SearchBackend {}
```

#### Dynamic Capability Checking

For operations that can't be checked at compile time, we provide runtime capability checking that fails fast with clear error messages:

```rust
/// Validates that a search query can be executed by this backend.
pub trait QueryValidator: StorageCapabilities {
    /// Checks if all features required by the query are supported.
    fn validate_query(&self, query: &SearchQuery) -> Result<(), UnsupportedFeature>;
    
    /// Returns which parts of a query would need post-processing.
    fn requires_post_processing(&self, query: &SearchQuery) -> Vec<PostProcessingStep>;
}

#[derive(Debug)]
pub struct UnsupportedFeature {
    pub feature: String,
    pub parameter: Option<String>,
    pub suggestion: Option<String>,
}

impl std::fmt::Display for UnsupportedFeature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unsupported feature: {}", self.feature)?;
        if let Some(ref param) = self.parameter {
            write!(f, " on parameter '{}'", param)?;
        }
        if let Some(ref suggestion) = self.suggestion {
            write!(f, ". Suggestion: {}", suggestion)?;
        }
        Ok(())
    }
}
```

### The Feature Support Matrix

Different storage technologies have different strengths. A key deliverable of the Helios FHIR Server's persistence design is a clear feature support matrix that documents what each storage backend provides. This (example, work-in-progress) matrix drives both the CapabilityStatement generation and helps operators choose the right backend for their workload.

| Feature | PostgreSQL | MongoDB | Cassandra | Neo4j | Elasticsearch | Object Storage |
|---------|-----------|---------|-----------|-------|---------------|----------------|
| **Basic CRUD** | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Versioning** | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| **History** | ✓ | ✓ | Limited | ✓ | ✓ | ✓ |
| **Transactions** | ✓ | ✓ | Limited | ✓ | ✗ | ✗ |
| **Multitenancy** | RLS + App | Collection/DB | Keyspace | Labels | Index per tenant | Prefix-based |
| **Search: String** | ✓ | ✓ | Limited | ✓ | ✓ | ✓ |
| **Search: Token** | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Search: Reference** | ✓ | ✓ | Limited | ✓ | ✓ | ✓ |
| **Search: Date Range** | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Search: Quantity** | ✓ | ✓ | Limited | Limited | ✓ | ✓ |
| **Modifier: :exact** | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Modifier: :contains** | ✓ | ✓ | ✗ | ✓ | ✓ | ✗ |
| **Modifier: :not** | ✓ | ✓ | ✗ | ✓ | ✓ | ✓ |
| **Modifier: :missing** | ✓ | ✓ | Limited | ✓ | ✓ | ✓ |
| **Modifier: :above/:below** | With terminology | With terminology | ✗ | ✓ | With terminology | ✗ |
| **Modifier: :in/:not-in** | With terminology | With terminology | ✗ | ✓ | With terminology | ✗ |
| **Chained Parameters** | ✓ | ✓ | ✗ | ✓ | Limited | ✗ |
| **Reverse Chaining (_has)** | ✓ | ✓ | ✗ | ✓ | ✗ | ✗ |
| **_include** | ✓ | ✓ | ✗ | ✓ | ✓ | ✗ |
| **_revinclude** | ✓ | ✓ | ✗ | ✓ | ✓ | ✗ |
| **Full-text Search** | ✓ | ✓ | ✗ | ✗ | ✓ | ✗ |
| **Bulk Export** | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |

This matrix isn't static. It's generated from the `StorageCapabilities` implementations. When a new storage backend is added or an existing one gains features, the matrix updates automatically.

**Hybrid Storage Patterns**

The polyglot architecture explicitly supports hybrid storage patterns. For example:

- **Internal database storage (JSONB in PostgreSQL)**: Resources stored directly in the database with embedded indexing.
- **Object Store with separate indexing**: Raw FHIR resources stored in S3/GCS/Azure Blob as the authoritative record, with a relational or search database providing indexing for search queries.

This hybrid approach can provide the cost benefits of object storage for long-term data retention while maintaining fast search capabilities through dedicated indexing infrastructure. The design permits either approach—or mixing them based on resource type, access patterns, or retention requirements.

### Composing Storage Backends (Inspired by Diesel's MultiConnection)

Diesel's `MultiConnection` derive macro generates an enum that wraps multiple connection types, dispatching to the appropriate backend at runtime. We adapt this pattern for polyglot FHIR persistence, but with intelligent routing based on query characteristics:

```rust
/// Routes operations to appropriate storage backends based on capabilities
/// and query characteristics. Similar to Diesel's MultiConnection but with
/// query-aware routing.
pub struct CompositeStorage {
    /// Primary transactional store for CRUD operations
    primary: Arc<dyn ResourceStorage>,
    
    /// Search-optimized store (may be the same as primary)
    search: Arc<dyn SearchProvider>,
    
    /// Terminology service for subsumption queries
    terminology: Arc<dyn TerminologySearchProvider>,
    
    /// Graph store for relationship traversal
    graph: Option<Arc<dyn ChainedSearchProvider>>,
    
    /// Full-text search engine
    text: Option<Arc<dyn TextSearchProvider>>,
    
    /// Audit log store (always separate)
    audit: Arc<dyn AuditStorage>,
    
    /// Bulk export store
    bulk: Arc<dyn BulkExportProvider>,
    
    /// Query cost estimator for routing decisions
    cost_estimator: Arc<dyn QueryCostEstimator>,
}
```

The routing logic becomes explicit policy that considers both capabilities and cost:

```rust

impl CompositeStorage {
    async fn route_search(
        &self, 
        tenant: &TenantContext,
        query: &SearchQuery,
    ) -> Result<SearchBundle, StorageError> {
        // If query contains _text or _content, route to text search
        if query.has_text_search() {
            if let Some(ref text) = self.text {
                return text.search(tenant, &query.resource_type, query).await;
            }
            return Err(StorageError::UnsupportedFeature(UnsupportedFeature {
                feature: "full-text search".into(),
                parameter: query.text_param_name(),
                suggestion: Some("Remove _text/_content parameters or enable Elasticsearch backend".into()),
            }));
        }

        // If query contains :above or :below modifiers, involve terminology
        if query.has_terminology_modifiers() {
            return self.search_with_terminology(tenant, query).await;
        }

        // If query contains chained parameters, prefer graph store
        if query.has_chaining() {
            if let Some(ref graph) = self.graph {
                let graph_cost = self.cost_estimator.estimate_cost(query, graph.as_ref());
                let primary_cost = self.cost_estimator.estimate_cost(query, self.search.as_ref());
                
                if graph_cost < primary_cost {
                    return graph.search(tenant, &query.resource_type, query).await;
                }
            }
        }

        // Default to primary search
        self.search.search(tenant, &query.resource_type, query).await
    }
    
    /// Ensures _include and _revinclude respect tenant boundaries.
    async fn apply_includes(
        &self,
        tenant: &TenantContext,
        matches: Vec<StoredResource>,
        query: &SearchQuery,
    ) -> Result<SearchBundle, StorageError> {
        let mut included = Vec::new();
        
        // Process _include directives
        for include in &query.include {
            let resolved = self.search.resolve_includes(tenant, &matches, &[include.clone()]).await?;
            included.extend(resolved);
        }
        
        // Process _revinclude directives  
        for revinclude in &query.revinclude {
            let resolved = self.search.resolve_revincludes(tenant, &matches, &[revinclude.clone()]).await?;
            included.extend(resolved);
        }
        
        Ok(SearchBundle {
            matches,
            included,
            total: None,
        })
    }
}

/// Estimates the cost of executing a query on different backends.
pub trait QueryCostEstimator: Send + Sync {
    fn estimate_cost(
        &self,
        query: &SearchQuery,
        backend: &dyn StorageCapabilities,
    ) -> QueryCost;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum QueryCost {
    /// Query can be executed efficiently
    Optimal(u64),
    /// Query requires some post-processing
    Acceptable(u64),
    /// Query will be slow, consider alternatives
    Expensive(u64),
    /// Query requires expanding a code set first
    RequiresExpansion,
    /// Backend cannot execute this query
    Unsupported,
}
```

### Binary and Unstructured Data Storage

Clinical AI increasingly depends on unstructured data: DICOM images, pathology slides, genomic sequences, waveforms, and scanned documents. While the current SQL-on-FHIR pipeline focuses on transforming structured FHIR resources into tabular outputs for analytics and bulk export, the polyglot architecture must also address storage and retrieval of large binary content.

**FHIR's Approach to Binary Content**

FHIR handles binary content through `DocumentReference` and `ImagingStudy` resources, which hold metadata and references to the binary content rather than embedding it. The actual DICOM data or document content lives in object storage (S3, Azure Blob, GCS, or on-premises equivalents), with the FHIR resources containing URLs or attachment references pointing to that store.

**Future: BinaryStorage Trait**

A dedicated trait for binary content management would fit naturally alongside the other storage traits:

```rust
/// Specialized storage for large binary content.
///
/// Handles uploads, chunked transfers, content-addressable storage,
/// and tiering policies for large objects like imaging data and documents.
#[async_trait]
pub trait BinaryStorage: Send + Sync {
    /// Stores binary content, returning a reference URL.
    async fn store(
        &self,
        tenant: &TenantContext,
        content: &[u8],
        content_type: &str,
        metadata: &BinaryMetadata,
    ) -> Result<BinaryReference, StorageError>;

    /// Retrieves binary content by reference.
    async fn retrieve(
        &self,
        tenant: &TenantContext,
        reference: &BinaryReference,
    ) -> Result<Vec<u8>, StorageError>;

    /// Initiates a chunked upload for large content.
    async fn begin_chunked_upload(
        &self,
        tenant: &TenantContext,
        content_type: &str,
        total_size: Option<u64>,
    ) -> Result<ChunkedUpload, StorageError>;

    /// Deletes binary content (respecting retention policies).
    async fn delete(
        &self,
        tenant: &TenantContext,
        reference: &BinaryReference,
    ) -> Result<(), StorageError>;
}

pub struct BinaryMetadata {
    pub patient_reference: Option<String>,
    pub security_labels: Vec<String>,
    pub retention_class: Option<RetentionClass>,
}

pub enum RetentionClass {
    Hot,      // Frequently accessed, high-performance storage
    Warm,     // Occasionally accessed
    Cold,     // Rarely accessed, archival storage
    Archive,  // Long-term retention, retrieval may be slow
}
```

This trait would support:

- **Metadata extraction and indexing**: For images, extract relevant metadata (modality, body part, study date) and potentially embeddings for vector search on imaging features.
- **Tiered storage**: Large binaries have different lifecycle needs. Hot data stays on fast storage while older content moves to cheaper archival tiers.
- **Content-addressable storage**: Deduplication of identical content across patients or studies.
- **Security and tenancy**: Extending `TenantContext` and audit logging to cover binary storage with appropriate encryption and access controls.

### The Path Forward

This trait-based decomposition provides a foundation for building a FHIR persistence layer that can evolve with requirements. When AI workloads demand vector similarity search, we add a `VectorSearchProvider` trait and plug in a vector database. When regulatory requirements demand immutable audit trails, we implement `AuditStorage` against an append-only ledger. When performance analysis reveals that graph traversals are bottlenecking population health queries, we route those operations to a dedicated graph database.

**Extensibility Following Diesel's Model**: Just as Diesel's `sql_function!` macro makes it trivial to add custom SQL functions, our design should make it easy to add custom search parameters and modifiers. A healthcare organization might need a custom `:phonetic` modifier for patient name matching, or a `:geo-near` modifier for location-based searches. The `SearchFragment` pattern enables this:

```rust
// Adding a custom phonetic search modifier is straightforward
pub struct PhoneticMatch {
    pub parameter: String,
    pub path: FhirPath,
    pub value: String,
    pub algorithm: PhoneticAlgorithm,
}

impl<B: SearchBackend> SearchFragment<B> for PhoneticMatch 
where
    B: SupportsPhoneticSearch,  // Custom capability marker
{
    fn apply(&self, builder: &mut B::QueryBuilder) -> Result<(), SearchError> {
        builder.add_phonetic_match(&self.path, &self.value, &self.algorithm)
    }
    
    fn is_native(&self, backend: &B) -> bool {
        backend.has_phonetic_support(&self.algorithm)
    }
    
    fn estimated_cost(&self, backend: &B) -> QueryCost {
        backend.cost_for_phonetic(&self.path)
    }
}
```

This is what it means to build FHIR persistence for the AI era: not a monolithic database adapter, but a composable system of specialized capabilities that can be assembled to meet the specific needs of each deployment with tenant isolation, search routing, and extensibility built into the architecture from the start.

## Thank you!

I very much look forward to your thoughts on these ideas and to the discussions that follow.

Sincerely,
-Steve
