//! Search provider traits.
//!
//! This module defines a hierarchy of search provider traits:
//! - [`SearchProvider`] - Basic single-type search
//! - [`MultiTypeSearchProvider`] - Search across multiple resource types
//! - [`IncludeProvider`] - Support for _include
//! - [`RevincludeProvider`] - Support for _revinclude
//! - [`ChainedSearchProvider`] - Chained parameters and _has
//! - [`TerminologySearchProvider`] - :above, :below, :in, :not-in
//! - [`TextSearchProvider`] - Full-text search (_text, _content, :text)

use async_trait::async_trait;

use crate::error::StorageResult;
use crate::tenant::TenantContext;
use crate::types::{
    IncludeDirective, Page, ReverseChainedParameter, SearchBundle, SearchQuery, StoredResource,
};

use super::storage::ResourceStorage;

/// Result of a search operation.
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// The matching resources.
    pub resources: Page<StoredResource>,

    /// Included resources (from _include/_revinclude).
    pub included: Vec<StoredResource>,

    /// Total count of matches (if requested via _total).
    pub total: Option<u64>,
}

impl SearchResult {
    /// Creates a new search result.
    pub fn new(resources: Page<StoredResource>) -> Self {
        Self {
            resources,
            included: Vec::new(),
            total: None,
        }
    }

    /// Adds included resources.
    pub fn with_included(mut self, included: Vec<StoredResource>) -> Self {
        self.included = included;
        self
    }

    /// Sets the total count.
    pub fn with_total(mut self, total: u64) -> Self {
        self.total = Some(total);
        self
    }

    /// Converts the result to a FHIR SearchBundle.
    pub fn to_bundle(&self, base_url: &str, self_link: &str) -> SearchBundle {
        use crate::types::{BundleEntry, SearchBundle};

        let mut bundle = SearchBundle::new()
            .with_self_link(self_link);

        if let Some(total) = self.total {
            bundle = bundle.with_total(total);
        }

        // Add next link if there's more data
        if let Some(ref cursor) = self.resources.page_info.next_cursor {
            bundle = bundle.with_next_link(format!("{}?_cursor={}", self_link, cursor));
        }

        // Add matching resources
        for resource in &self.resources.items {
            let full_url = format!("{}/{}", base_url, resource.url());
            bundle = bundle.with_entry(BundleEntry::match_entry(full_url, resource.content().clone()));
        }

        // Add included resources
        for resource in &self.included {
            let full_url = format!("{}/{}", base_url, resource.url());
            bundle = bundle.with_entry(BundleEntry::include_entry(full_url, resource.content().clone()));
        }

        bundle
    }
}

/// Basic search provider for single resource type queries.
///
/// This trait provides search functionality for a single resource type,
/// corresponding to the FHIR search interaction:
/// `GET [base]/[type]?[parameters]`
///
/// # Example
///
/// ```ignore
/// use helios_persistence::core::SearchProvider;
/// use helios_persistence::types::{SearchQuery, SearchParameter, SearchParamType, SearchValue};
///
/// async fn search_patients<S: SearchProvider>(
///     storage: &S,
///     tenant: &TenantContext,
/// ) -> Result<(), StorageError> {
///     let query = SearchQuery::new("Patient")
///         .with_parameter(SearchParameter {
///             name: "name".to_string(),
///             param_type: SearchParamType::String,
///             modifier: None,
///             values: vec![SearchValue::eq("Smith")],
///             chain: vec![],
///         })
///         .with_count(20);
///
///     let result = storage.search(tenant, &query).await?;
///
///     for resource in result.resources.items {
///         println!("Found: {}", resource.url());
///     }
///
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait SearchProvider: ResourceStorage {
    /// Searches for resources matching the query.
    ///
    /// # Arguments
    ///
    /// * `tenant` - The tenant context for this operation
    /// * `query` - The search query with parameters
    ///
    /// # Returns
    ///
    /// A search result with matching resources and pagination info.
    ///
    /// # Errors
    ///
    /// * `StorageError::Validation` - If the query contains invalid parameters
    /// * `StorageError::Search` - If a search feature is not supported
    /// * `StorageError::Tenant` - If the tenant doesn't have search permission
    async fn search(
        &self,
        tenant: &TenantContext,
        query: &SearchQuery,
    ) -> StorageResult<SearchResult>;

    /// Counts resources matching the query without returning them.
    ///
    /// This is more efficient than search when you only need the count.
    async fn search_count(
        &self,
        tenant: &TenantContext,
        query: &SearchQuery,
    ) -> StorageResult<u64>;
}

/// Search provider that supports searching across multiple resource types.
///
/// This extends [`SearchProvider`] to support system-level search:
/// `GET [base]?[parameters]`
#[async_trait]
pub trait MultiTypeSearchProvider: SearchProvider {
    /// Searches across multiple resource types.
    ///
    /// # Arguments
    ///
    /// * `tenant` - The tenant context for this operation
    /// * `resource_types` - The resource types to search (empty = all types)
    /// * `query` - The search query
    ///
    /// # Returns
    ///
    /// A search result with matching resources from all specified types.
    async fn search_multi(
        &self,
        tenant: &TenantContext,
        resource_types: &[&str],
        query: &SearchQuery,
    ) -> StorageResult<SearchResult>;
}

/// Search provider that supports _include.
///
/// _include adds referenced resources to the search results.
#[async_trait]
pub trait IncludeProvider: SearchProvider {
    /// Resolves _include directives for search results.
    ///
    /// # Arguments
    ///
    /// * `tenant` - The tenant context for this operation
    /// * `resources` - The primary search results
    /// * `includes` - The include directives to resolve
    ///
    /// # Returns
    ///
    /// Resources referenced by the primary results according to the include directives.
    async fn resolve_includes(
        &self,
        tenant: &TenantContext,
        resources: &[StoredResource],
        includes: &[IncludeDirective],
    ) -> StorageResult<Vec<StoredResource>>;
}

/// Search provider that supports _revinclude.
///
/// _revinclude adds resources that reference the search results.
#[async_trait]
pub trait RevincludeProvider: SearchProvider {
    /// Resolves _revinclude directives for search results.
    ///
    /// # Arguments
    ///
    /// * `tenant` - The tenant context for this operation
    /// * `resources` - The primary search results
    /// * `revincludes` - The revinclude directives to resolve
    ///
    /// # Returns
    ///
    /// Resources that reference the primary results according to the revinclude directives.
    async fn resolve_revincludes(
        &self,
        tenant: &TenantContext,
        resources: &[StoredResource],
        revincludes: &[IncludeDirective],
    ) -> StorageResult<Vec<StoredResource>>;
}

/// Search provider that supports chained parameters and _has.
///
/// Chained parameters search on referenced resources:
/// `Observation?patient.name=Smith`
///
/// _has searches for resources referenced by other resources:
/// `Patient?_has:Observation:patient:code=1234-5`
#[async_trait]
pub trait ChainedSearchProvider: SearchProvider {
    /// Evaluates a chained search and returns matching resource IDs.
    ///
    /// This is used internally to resolve chains before the main search.
    ///
    /// # Arguments
    ///
    /// * `tenant` - The tenant context for this operation
    /// * `base_type` - The base resource type being searched
    /// * `chain` - The chain path (e.g., "patient.organization.name")
    /// * `value` - The value to match
    ///
    /// # Returns
    ///
    /// IDs of base resources that match the chain condition.
    async fn resolve_chain(
        &self,
        tenant: &TenantContext,
        base_type: &str,
        chain: &str,
        value: &str,
    ) -> StorageResult<Vec<String>>;

    /// Evaluates a reverse chain (_has) and returns matching resource IDs.
    ///
    /// # Arguments
    ///
    /// * `tenant` - The tenant context for this operation
    /// * `base_type` - The base resource type being searched
    /// * `reverse_chain` - The reverse chain parameters
    ///
    /// # Returns
    ///
    /// IDs of base resources that are referenced by matching resources.
    async fn resolve_reverse_chain(
        &self,
        tenant: &TenantContext,
        base_type: &str,
        reverse_chain: &ReverseChainedParameter,
    ) -> StorageResult<Vec<String>>;
}

/// Search provider that supports terminology-aware modifiers.
///
/// These modifiers require integration with a terminology service:
/// - `:above` - Match codes above in the hierarchy
/// - `:below` - Match codes below in the hierarchy
/// - `:in` - Match codes in a value set
/// - `:not-in` - Match codes not in a value set
#[async_trait]
pub trait TerminologySearchProvider: SearchProvider {
    /// Expands a value set and returns member codes.
    ///
    /// # Arguments
    ///
    /// * `value_set_url` - The canonical URL of the value set
    ///
    /// # Returns
    ///
    /// A list of (system, code) pairs in the value set.
    async fn expand_value_set(
        &self,
        value_set_url: &str,
    ) -> StorageResult<Vec<(String, String)>>;

    /// Gets codes above the given code in the hierarchy.
    ///
    /// # Arguments
    ///
    /// * `system` - The code system URL
    /// * `code` - The code to find ancestors for
    ///
    /// # Returns
    ///
    /// Codes that are ancestors of the given code (including the code itself).
    async fn codes_above(
        &self,
        system: &str,
        code: &str,
    ) -> StorageResult<Vec<String>>;

    /// Gets codes below the given code in the hierarchy.
    ///
    /// # Arguments
    ///
    /// * `system` - The code system URL
    /// * `code` - The code to find descendants for
    ///
    /// # Returns
    ///
    /// Codes that are descendants of the given code (including the code itself).
    async fn codes_below(
        &self,
        system: &str,
        code: &str,
    ) -> StorageResult<Vec<String>>;
}

/// Search provider that supports full-text search.
///
/// Full-text search operations:
/// - `_text` - Search in the narrative
/// - `_content` - Search in the entire resource content
/// - `:text` modifier - Full-text search on token parameters
#[async_trait]
pub trait TextSearchProvider: SearchProvider {
    /// Performs a full-text search on resource narratives.
    ///
    /// # Arguments
    ///
    /// * `tenant` - The tenant context for this operation
    /// * `resource_type` - The resource type to search
    /// * `text` - The text to search for
    /// * `pagination` - Pagination settings
    ///
    /// # Returns
    ///
    /// Resources with matching narrative text, ordered by relevance.
    async fn search_text(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        text: &str,
        pagination: &crate::types::Pagination,
    ) -> StorageResult<SearchResult>;

    /// Performs a full-text search on entire resource content.
    ///
    /// # Arguments
    ///
    /// * `tenant` - The tenant context for this operation
    /// * `resource_type` - The resource type to search
    /// * `content` - The content to search for
    /// * `pagination` - Pagination settings
    ///
    /// # Returns
    ///
    /// Resources with matching content, ordered by relevance.
    async fn search_content(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        content: &str,
        pagination: &crate::types::Pagination,
    ) -> StorageResult<SearchResult>;
}

/// Marker trait for search providers that support all advanced features.
///
/// This is a convenience trait that combines all search capabilities.
pub trait FullSearchProvider:
    SearchProvider
    + MultiTypeSearchProvider
    + IncludeProvider
    + RevincludeProvider
    + ChainedSearchProvider
{
}

// Blanket implementation for types that implement all required traits
impl<T> FullSearchProvider for T where
    T: SearchProvider
        + MultiTypeSearchProvider
        + IncludeProvider
        + RevincludeProvider
        + ChainedSearchProvider
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PageInfo;

    #[test]
    fn test_search_result_creation() {
        let page = Page::new(Vec::new(), PageInfo::end());
        let result = SearchResult::new(page);
        assert!(result.included.is_empty());
        assert!(result.total.is_none());
    }

    #[test]
    fn test_search_result_with_included() {
        let page = Page::new(Vec::new(), PageInfo::end());
        let result = SearchResult::new(page)
            .with_included(vec![StoredResource::new(
                "Patient",
                "123",
                crate::tenant::TenantId::new("t1"),
                serde_json::json!({}),
            )])
            .with_total(100);

        assert_eq!(result.included.len(), 1);
        assert_eq!(result.total, Some(100));
    }

    #[test]
    fn test_search_result_to_bundle() {
        let resource = StoredResource::new(
            "Patient",
            "123",
            crate::tenant::TenantId::new("t1"),
            serde_json::json!({"resourceType": "Patient", "id": "123"}),
        );

        let page = Page::new(vec![resource], PageInfo::end());
        let result = SearchResult::new(page).with_total(1);

        let bundle = result.to_bundle("http://example.com/fhir", "http://example.com/fhir/Patient");

        assert_eq!(bundle.total, Some(1));
        assert_eq!(bundle.entry.len(), 1);
    }
}
