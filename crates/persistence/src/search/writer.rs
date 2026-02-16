//! Search Index Writer Trait.
//!
//! Defines the interface for writing extracted search values to a backend's
//! search index. Each backend implements this trait according to its storage model.

use async_trait::async_trait;

use crate::error::StorageResult;

use super::extractor::ExtractedValue;

/// Trait for writing search parameter values to an index.
///
/// This trait abstracts the storage of extracted search values, allowing
/// different backends to implement their own indexing strategies.
#[async_trait]
pub trait SearchIndexWriter: Send + Sync {
    /// Writes extracted values for a resource to the search index.
    ///
    /// This typically inserts multiple rows in a search_index table,
    /// one for each extracted value.
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - The tenant identifier
    /// * `resource_type` - The resource type (e.g., "Patient")
    /// * `resource_id` - The resource's logical ID
    /// * `values` - The extracted search values to index
    ///
    /// # Returns
    ///
    /// The number of index entries created.
    async fn write_entries(
        &self,
        tenant_id: &str,
        resource_type: &str,
        resource_id: &str,
        values: Vec<ExtractedValue>,
    ) -> StorageResult<usize>;

    /// Deletes all search index entries for a resource.
    ///
    /// Called when a resource is updated (before re-indexing) or deleted.
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - The tenant identifier
    /// * `resource_type` - The resource type
    /// * `resource_id` - The resource's logical ID
    ///
    /// # Returns
    ///
    /// The number of index entries deleted.
    async fn delete_entries(
        &self,
        tenant_id: &str,
        resource_type: &str,
        resource_id: &str,
    ) -> StorageResult<usize>;

    /// Deletes all search index entries for a specific parameter.
    ///
    /// Used when a SearchParameter is deleted or disabled.
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - The tenant identifier
    /// * `param_url` - The SearchParameter's canonical URL
    ///
    /// # Returns
    ///
    /// The number of index entries deleted.
    async fn delete_entries_for_param(
        &self,
        tenant_id: &str,
        param_url: &str,
    ) -> StorageResult<usize>;

    /// Clears all search index entries for a tenant.
    ///
    /// Used during full reindexing or tenant cleanup.
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - The tenant identifier
    ///
    /// # Returns
    ///
    /// The number of index entries deleted.
    async fn clear_all(&self, tenant_id: &str) -> StorageResult<usize>;

    /// Returns the number of index entries for a tenant.
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - The tenant identifier
    ///
    /// # Returns
    ///
    /// The total number of index entries.
    async fn count_entries(&self, tenant_id: &str) -> StorageResult<u64>;

    /// Returns the number of index entries for a specific resource.
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - The tenant identifier
    /// * `resource_type` - The resource type
    /// * `resource_id` - The resource's logical ID
    ///
    /// # Returns
    ///
    /// The number of index entries for this resource.
    async fn count_resource_entries(
        &self,
        tenant_id: &str,
        resource_type: &str,
        resource_id: &str,
    ) -> StorageResult<u64>;
}

/// Options for index writing operations.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// Whether to replace existing entries (vs. append).
    pub replace: bool,

    /// Whether to skip validation (for bulk operations).
    pub skip_validation: bool,

    /// Batch size for bulk operations.
    pub batch_size: Option<usize>,
}

impl WriteOptions {
    /// Creates new write options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the replace flag.
    pub fn replace(mut self) -> Self {
        self.replace = true;
        self
    }

    /// Sets the skip_validation flag.
    pub fn skip_validation(mut self) -> Self {
        self.skip_validation = true;
        self
    }

    /// Sets the batch size.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = Some(size);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_options() {
        let opts = WriteOptions::new().replace().with_batch_size(100);

        assert!(opts.replace);
        assert_eq!(opts.batch_size, Some(100));
    }
}
