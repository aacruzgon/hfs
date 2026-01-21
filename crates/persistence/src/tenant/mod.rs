//! Tenant management for multi-tenant FHIR storage.
//!
//! This module provides the core types for multi-tenant support in the persistence layer.
//! All storage operations require a [`TenantContext`] to ensure proper tenant isolation.
//!
//! # Core Types
//!
//! - [`TenantId`] - Opaque tenant identifier with hierarchical namespace support
//! - [`TenantContext`] - Validated context required for all storage operations
//! - [`TenantPermissions`] - Defines what operations a tenant can perform
//! - [`TenancyModel`] - Determines how resources are isolated between tenants
//!
//! # Design Philosophy
//!
//! The persistence layer enforces tenant isolation at the type level. Every storage
//! operation requires a `TenantContext`, making it impossible to accidentally bypass
//! tenant boundaries. This is a deliberate design choice - there is no escape hatch.
//!
//! # Tenancy Models
//!
//! Three isolation strategies are supported:
//!
//! 1. **Shared Schema** - All tenants in one database with tenant_id column
//! 2. **Schema-per-Tenant** - Separate database schema for each tenant
//! 3. **Database-per-Tenant** - Separate database for each tenant
//!
//! # Examples
//!
//! ## Creating a Tenant Context
//!
//! ```
//! use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
//!
//! // Full access context
//! let ctx = TenantContext::new(
//!     TenantId::new("acme-corp"),
//!     TenantPermissions::full_access(),
//! );
//!
//! // Read-only context
//! let read_ctx = TenantContext::new(
//!     TenantId::new("acme-corp"),
//!     TenantPermissions::read_only(),
//! );
//!
//! // System tenant for shared resources
//! let system_ctx = TenantContext::system();
//! ```
//!
//! ## Hierarchical Tenants
//!
//! ```
//! use helios_persistence::tenant::TenantId;
//!
//! let parent = TenantId::new("acme");
//! let child = TenantId::new("acme/research");
//! let grandchild = TenantId::new("acme/research/oncology");
//!
//! assert!(child.is_descendant_of(&parent));
//! assert!(grandchild.is_descendant_of(&parent));
//! assert_eq!(grandchild.root().as_str(), "acme");
//! ```
//!
//! ## Custom Permissions
//!
//! ```
//! use helios_persistence::tenant::{TenantPermissions, Operation};
//!
//! let perms = TenantPermissions::builder()
//!     .allow_operations(vec![Operation::Read, Operation::Search])
//!     .allow_resource_types(vec!["Patient", "Observation"])
//!     .restrict_to_compartment("Patient", "123")
//!     .build();
//! ```

mod context;
mod id;
mod permissions;
mod tenancy;

pub use context::{TenantContext, TenantContextBuilder};
pub use id::{TenantId, SYSTEM_TENANT};
pub use permissions::{CompartmentRestriction, Operation, TenantPermissions, TenantPermissionsBuilder};
pub use tenancy::{CustomResourceTenancy, DefaultResourceTenancy, ResourceTenancy, TenancyModel};
