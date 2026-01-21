//! History provider traits.
//!
//! This module defines a progressive trait hierarchy for history operations:
//! - [`InstanceHistoryProvider`] - History for a single resource instance
//! - [`TypeHistoryProvider`] - History for all resources of a type
//! - [`SystemHistoryProvider`] - History across all resource types
//!
//! Backends implement the levels they support, with each level extending the previous.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::StorageResult;
use crate::tenant::TenantContext;
use crate::types::{Page, Pagination, StoredResource};

use super::versioned::VersionedStorage;

/// Parameters for history queries.
#[derive(Debug, Clone, Default)]
pub struct HistoryParams {
    /// Only include versions created/updated since this time.
    pub since: Option<DateTime<Utc>>,

    /// Only include versions created/updated before this time.
    pub before: Option<DateTime<Utc>>,

    /// Pagination settings.
    pub pagination: Pagination,

    /// If true, include deleted versions.
    pub include_deleted: bool,
}

impl HistoryParams {
    /// Creates new history parameters with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the since filter.
    pub fn since(mut self, since: DateTime<Utc>) -> Self {
        self.since = Some(since);
        self
    }

    /// Sets the before filter.
    pub fn before(mut self, before: DateTime<Utc>) -> Self {
        self.before = Some(before);
        self
    }

    /// Sets the count limit.
    pub fn count(mut self, count: u32) -> Self {
        self.pagination = self.pagination.with_count(count);
        self
    }

    /// Sets whether to include deleted versions.
    pub fn include_deleted(mut self, include: bool) -> Self {
        self.include_deleted = include;
        self
    }
}

/// A single entry in a history bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// The resource at this version.
    pub resource: StoredResource,

    /// The HTTP method that created this version.
    pub method: HistoryMethod,

    /// When this version was created.
    pub timestamp: DateTime<Utc>,
}

/// HTTP method that created a history entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HistoryMethod {
    /// Resource was created (POST).
    Post,
    /// Resource was updated (PUT).
    Put,
    /// Resource was patched (PATCH).
    Patch,
    /// Resource was deleted (DELETE).
    Delete,
}

impl std::fmt::Display for HistoryMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HistoryMethod::Post => write!(f, "POST"),
            HistoryMethod::Put => write!(f, "PUT"),
            HistoryMethod::Patch => write!(f, "PATCH"),
            HistoryMethod::Delete => write!(f, "DELETE"),
        }
    }
}

/// A page of history entries.
pub type HistoryPage = Page<HistoryEntry>;

/// Provider for instance-level history.
///
/// This trait provides the history for a single resource instance,
/// corresponding to the FHIR history interaction:
/// `GET [base]/[type]/[id]/_history`
///
/// # Example
///
/// ```ignore
/// use helios_persistence::core::InstanceHistoryProvider;
///
/// async fn get_patient_history<S: InstanceHistoryProvider>(
///     storage: &S,
///     tenant: &TenantContext,
/// ) -> Result<(), StorageError> {
///     let params = HistoryParams::new()
///         .since(Utc::now() - Duration::days(30))
///         .count(10);
///
///     let history = storage.history_instance(
///         tenant,
///         "Patient",
///         "123",
///         &params,
///     ).await?;
///
///     for entry in history.items {
///         println!("Version {}: {} at {}",
///             entry.resource.version_id(),
///             entry.method,
///             entry.timestamp
///         );
///     }
///
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait InstanceHistoryProvider: VersionedStorage {
    /// Gets the history for a specific resource instance.
    ///
    /// # Arguments
    ///
    /// * `tenant` - The tenant context for this operation
    /// * `resource_type` - The FHIR resource type
    /// * `id` - The resource's logical ID
    /// * `params` - History query parameters
    ///
    /// # Returns
    ///
    /// A page of history entries in reverse chronological order (newest first).
    async fn history_instance(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        params: &HistoryParams,
    ) -> StorageResult<HistoryPage>;

    /// Gets the total number of versions for a resource.
    async fn history_instance_count(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<u64>;
}

/// Provider for type-level history.
///
/// This trait provides the history for all resources of a given type,
/// corresponding to the FHIR history interaction:
/// `GET [base]/[type]/_history`
///
/// This extends [`InstanceHistoryProvider`] as backends that support type-level
/// history also support instance-level history.
#[async_trait]
pub trait TypeHistoryProvider: InstanceHistoryProvider {
    /// Gets the history for all resources of a type.
    ///
    /// # Arguments
    ///
    /// * `tenant` - The tenant context for this operation
    /// * `resource_type` - The FHIR resource type
    /// * `params` - History query parameters
    ///
    /// # Returns
    ///
    /// A page of history entries in reverse chronological order.
    async fn history_type(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        params: &HistoryParams,
    ) -> StorageResult<HistoryPage>;

    /// Gets the total number of history entries for a resource type.
    async fn history_type_count(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
    ) -> StorageResult<u64>;
}

/// Provider for system-level history.
///
/// This trait provides the history across all resource types,
/// corresponding to the FHIR history interaction:
/// `GET [base]/_history`
///
/// This extends [`TypeHistoryProvider`] as backends that support system-level
/// history also support type-level and instance-level history.
#[async_trait]
pub trait SystemHistoryProvider: TypeHistoryProvider {
    /// Gets the history for all resources in the system.
    ///
    /// # Arguments
    ///
    /// * `tenant` - The tenant context for this operation
    /// * `params` - History query parameters
    ///
    /// # Returns
    ///
    /// A page of history entries in reverse chronological order.
    async fn history_system(
        &self,
        tenant: &TenantContext,
        params: &HistoryParams,
    ) -> StorageResult<HistoryPage>;

    /// Gets the total number of history entries in the system.
    async fn history_system_count(
        &self,
        tenant: &TenantContext,
    ) -> StorageResult<u64>;
}

/// Extension trait for history providers that support differential queries.
///
/// Differential queries return only resources that have changed since a given point,
/// which is more efficient for synchronization use cases.
#[async_trait]
pub trait DifferentialHistoryProvider: TypeHistoryProvider {
    /// Gets resources modified since a given timestamp.
    ///
    /// This is more efficient than full history for sync scenarios as it returns
    /// only the current version of each modified resource, not all versions.
    ///
    /// # Arguments
    ///
    /// * `tenant` - The tenant context for this operation
    /// * `resource_type` - The FHIR resource type (or None for all types)
    /// * `since` - Only include resources modified after this time
    /// * `pagination` - Pagination settings
    ///
    /// # Returns
    ///
    /// A page of current resource versions that were modified since the given time.
    async fn modified_since(
        &self,
        tenant: &TenantContext,
        resource_type: Option<&str>,
        since: DateTime<Utc>,
        pagination: &Pagination,
    ) -> StorageResult<Page<StoredResource>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_history_params_builder() {
        let now = Utc::now();
        let params = HistoryParams::new()
            .since(now)
            .count(50)
            .include_deleted(true);

        assert!(params.since.is_some());
        assert_eq!(params.pagination.count, 50);
        assert!(params.include_deleted);
    }

    #[test]
    fn test_history_method_display() {
        assert_eq!(HistoryMethod::Post.to_string(), "POST");
        assert_eq!(HistoryMethod::Put.to_string(), "PUT");
        assert_eq!(HistoryMethod::Patch.to_string(), "PATCH");
        assert_eq!(HistoryMethod::Delete.to_string(), "DELETE");
    }

    #[test]
    fn test_history_entry_creation() {
        let resource = StoredResource::new(
            "Patient",
            "123",
            crate::tenant::TenantId::new("t1"),
            serde_json::json!({}),
        );

        let entry = HistoryEntry {
            resource,
            method: HistoryMethod::Post,
            timestamp: Utc::now(),
        };

        assert_eq!(entry.method, HistoryMethod::Post);
    }
}
