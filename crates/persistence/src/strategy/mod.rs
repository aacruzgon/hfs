//! Multitenancy strategy implementations.
//!
//! This module provides three tenancy isolation strategies:
//!
//! - [`SharedSchemaStrategy`] - All tenants in one schema with tenant_id column
//! - [`SchemaPerTenantStrategy`] - Separate database schema per tenant
//! - [`DatabasePerTenantStrategy`] - Separate database per tenant
//!
//! # Choosing a Strategy
//!
//! | Strategy | Isolation | Performance | Scalability | Complexity |
//! |----------|-----------|-------------|-------------|------------|
//! | Shared Schema | Low | High | Medium | Low |
//! | Schema-per-Tenant | Medium | Medium | Medium | Medium |
//! | Database-per-Tenant | High | Low | High | High |
//!
//! ## Shared Schema
//!
//! Best for:
//! - Many small tenants with similar data patterns
//! - Simple deployment and maintenance
//! - Cost-sensitive environments
//!
//! Considerations:
//! - All tenants share resources (connections, indexes)
//! - Requires careful index design (tenant_id should be leading)
//! - Row-Level Security can add additional protection
//!
//! ## Schema-per-Tenant
//!
//! Best for:
//! - Medium number of tenants
//! - Need for logical isolation
//! - Tenant-specific customizations
//!
//! Considerations:
//! - PostgreSQL-specific (uses schemas)
//! - Shared connection pool with search_path switching
//! - Simpler backup/restore per tenant
//!
//! ## Database-per-Tenant
//!
//! Best for:
//! - Enterprise customers requiring complete isolation
//! - Regulatory requirements (data residency)
//! - Tenants with very different usage patterns
//!
//! Considerations:
//! - Highest resource usage (connection pools per tenant)
//! - Most complex operations (migrations across databases)
//! - Best data isolation and portability
//!
//! # Example
//!
//! ```
//! use helios_persistence::strategy::{
//!     TenancyStrategy, SharedSchemaConfig, SchemaPerTenantConfig, DatabasePerTenantConfig
//! };
//!
//! // Shared schema (simplest)
//! let shared = TenancyStrategy::SharedSchema(SharedSchemaConfig {
//!     use_row_level_security: true,
//!     tenant_column: "tenant_id".to_string(),
//!     ..Default::default()
//! });
//!
//! // Schema per tenant (PostgreSQL)
//! let schema_per = TenancyStrategy::SchemaPerTenant(SchemaPerTenantConfig {
//!     schema_prefix: "tenant_".to_string(),
//!     shared_schema: "shared".to_string(),
//!     auto_create_schema: true,
//!     ..Default::default()
//! });
//!
//! // Database per tenant (maximum isolation)
//! let db_per = TenancyStrategy::DatabasePerTenant(DatabasePerTenantConfig {
//!     connection_template: "postgres://user:pass@{host}/{tenant}_db".to_string(),
//!     pool_per_tenant: true,
//!     max_pools: Some(100),
//!     ..Default::default()
//! });
//! ```

mod database_per_tenant;
mod schema_per_tenant;
mod shared_schema;

pub use database_per_tenant::{DatabasePerTenantConfig, DatabasePerTenantStrategy};
pub use schema_per_tenant::{SchemaPerTenantConfig, SchemaPerTenantStrategy};
pub use shared_schema::{SharedSchemaConfig, SharedSchemaStrategy};

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::tenant::TenantId;

/// The tenancy strategy configuration.
///
/// This enum defines how tenant isolation is implemented at the database level.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TenancyStrategy {
    /// All tenants share the same schema with a tenant_id column.
    SharedSchema(SharedSchemaConfig),

    /// Each tenant has a separate database schema.
    SchemaPerTenant(SchemaPerTenantConfig),

    /// Each tenant has a separate database.
    DatabasePerTenant(DatabasePerTenantConfig),
}

impl Default for TenancyStrategy {
    fn default() -> Self {
        TenancyStrategy::SharedSchema(SharedSchemaConfig::default())
    }
}

impl fmt::Display for TenancyStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TenancyStrategy::SharedSchema(_) => write!(f, "shared-schema"),
            TenancyStrategy::SchemaPerTenant(_) => write!(f, "schema-per-tenant"),
            TenancyStrategy::DatabasePerTenant(_) => write!(f, "database-per-tenant"),
        }
    }
}

impl TenancyStrategy {
    /// Returns the isolation level of this strategy.
    pub fn isolation_level(&self) -> IsolationLevel {
        match self {
            TenancyStrategy::SharedSchema(_) => IsolationLevel::Logical,
            TenancyStrategy::SchemaPerTenant(_) => IsolationLevel::Schema,
            TenancyStrategy::DatabasePerTenant(_) => IsolationLevel::Physical,
        }
    }

    /// Returns true if this strategy uses a shared connection pool.
    pub fn uses_shared_pool(&self) -> bool {
        match self {
            TenancyStrategy::SharedSchema(_) => true,
            TenancyStrategy::SchemaPerTenant(_) => true,
            TenancyStrategy::DatabasePerTenant(config) => !config.pool_per_tenant,
        }
    }
}

/// Level of tenant isolation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IsolationLevel {
    /// Logical isolation via tenant_id column.
    Logical,
    /// Schema-level isolation.
    Schema,
    /// Physical isolation via separate databases.
    Physical,
}

impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IsolationLevel::Logical => write!(f, "logical"),
            IsolationLevel::Schema => write!(f, "schema"),
            IsolationLevel::Physical => write!(f, "physical"),
        }
    }
}

/// Trait for tenant resolution in different strategies.
///
/// Implementations convert tenant IDs to database-specific identifiers
/// (schema names, database names, connection strings, etc.).
pub trait TenantResolver: Send + Sync {
    /// Resolves a tenant ID to the appropriate database identifier.
    fn resolve(&self, tenant_id: &TenantId) -> TenantResolution;

    /// Validates that a tenant ID is valid for this strategy.
    fn validate(&self, tenant_id: &TenantId) -> Result<(), TenantValidationError>;

    /// Returns the system tenant resolution.
    fn system_tenant(&self) -> TenantResolution;
}

/// Result of tenant resolution.
#[derive(Debug, Clone)]
pub enum TenantResolution {
    /// Use shared schema with tenant_id filter.
    SharedSchema {
        /// The tenant ID to filter by.
        tenant_id: String,
    },

    /// Use a specific schema.
    Schema {
        /// The schema name to use.
        schema_name: String,
    },

    /// Use a specific database.
    Database {
        /// The connection string or database name.
        connection: String,
    },
}

/// Error when validating a tenant ID.
#[derive(Debug, Clone)]
pub struct TenantValidationError {
    /// The invalid tenant ID.
    pub tenant_id: String,
    /// The reason for validation failure.
    pub reason: String,
}

impl fmt::Display for TenantValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid tenant '{}': {}", self.tenant_id, self.reason)
    }
}

impl std::error::Error for TenantValidationError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenancy_strategy_display() {
        let shared = TenancyStrategy::SharedSchema(SharedSchemaConfig::default());
        assert_eq!(shared.to_string(), "shared-schema");

        let schema = TenancyStrategy::SchemaPerTenant(SchemaPerTenantConfig::default());
        assert_eq!(schema.to_string(), "schema-per-tenant");

        let db = TenancyStrategy::DatabasePerTenant(DatabasePerTenantConfig::default());
        assert_eq!(db.to_string(), "database-per-tenant");
    }

    #[test]
    fn test_isolation_level() {
        let shared = TenancyStrategy::SharedSchema(SharedSchemaConfig::default());
        assert_eq!(shared.isolation_level(), IsolationLevel::Logical);

        let schema = TenancyStrategy::SchemaPerTenant(SchemaPerTenantConfig::default());
        assert_eq!(schema.isolation_level(), IsolationLevel::Schema);

        let db = TenancyStrategy::DatabasePerTenant(DatabasePerTenantConfig::default());
        assert_eq!(db.isolation_level(), IsolationLevel::Physical);
    }

    #[test]
    fn test_uses_shared_pool() {
        let shared = TenancyStrategy::SharedSchema(SharedSchemaConfig::default());
        assert!(shared.uses_shared_pool());

        let db_shared_pool = TenancyStrategy::DatabasePerTenant(DatabasePerTenantConfig {
            pool_per_tenant: false,
            ..Default::default()
        });
        assert!(db_shared_pool.uses_shared_pool());

        let db_per_pool = TenancyStrategy::DatabasePerTenant(DatabasePerTenantConfig {
            pool_per_tenant: true,
            ..Default::default()
        });
        assert!(!db_per_pool.uses_shared_pool());
    }

    #[test]
    fn test_tenant_validation_error_display() {
        let err = TenantValidationError {
            tenant_id: "bad-tenant".to_string(),
            reason: "contains invalid characters".to_string(),
        };
        assert!(err.to_string().contains("bad-tenant"));
        assert!(err.to_string().contains("invalid characters"));
    }
}
