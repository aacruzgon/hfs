//! Search interaction handler.
//!
//! Implements the FHIR [search interaction](https://hl7.org/fhir/http.html#search):
//! - `GET [base]/[type]?params` - Type-level search
//! - `POST [base]/[type]/_search` - Type-level search (POST)
//! - `GET [base]?params` - System-level search (all types)
//!
//! The search handler connects to the persistence layer's SearchProvider trait
//! to execute searches against the storage backend.

use axum::{
    Form, Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use helios_persistence::core::{MultiTypeSearchProvider, ResourceStorage, SearchProvider};
use helios_persistence::types::SearchBundle;
use serde::Deserialize;
use std::collections::HashMap;
use tracing::{debug, warn};

use crate::error::{RestError, RestResult};
use crate::extractors::{TenantExtractor, build_search_query_from_map};
use crate::responses::subsetting::{SummaryMode, apply_elements, apply_summary};
use crate::state::AppState;

/// Query parameters for search (used in the SearchQuery struct in handlers).
#[derive(Debug, Deserialize, Default)]
#[allow(dead_code)]
pub struct SearchQueryParams {
    /// The page size (_count parameter).
    #[serde(rename = "_count")]
    pub count: Option<usize>,

    /// The page offset for pagination.
    #[serde(rename = "_offset")]
    pub offset: Option<usize>,

    /// Include total count in response.
    #[serde(rename = "_total")]
    pub total: Option<String>,

    /// Sort order.
    #[serde(rename = "_sort")]
    pub sort: Option<String>,

    /// Summary mode (_summary parameter).
    #[serde(rename = "_summary")]
    pub summary: Option<String>,

    /// Elements to include (_elements parameter).
    #[serde(rename = "_elements")]
    pub elements: Option<String>,

    /// Resources to include (_include parameter).
    #[serde(rename = "_include")]
    pub include: Option<Vec<String>>,

    /// Resources to reverse include (_revinclude parameter).
    #[serde(rename = "_revinclude")]
    pub revinclude: Option<Vec<String>>,
}

/// Handler for GET search.
///
/// Searches for resources of a specific type.
///
/// # HTTP Request
///
/// `GET [base]/[type]?params`
///
/// # Response
///
/// Returns a Bundle of type "searchset".
pub async fn search_get_handler<S>(
    State(state): State<AppState<S>>,
    Path(resource_type): Path<String>,
    tenant: TenantExtractor,
    Query(params): Query<HashMap<String, String>>,
) -> RestResult<Response>
where
    S: ResourceStorage + SearchProvider + Send + Sync,
{
    debug!(
        resource_type = %resource_type,
        tenant = %tenant.tenant_id(),
        params = ?params,
        "Processing search GET request"
    );

    execute_search(&state, tenant, &resource_type, params).await
}

/// Handler for POST search.
///
/// Searches for resources using form-encoded parameters.
///
/// # HTTP Request
///
/// `POST [base]/[type]/_search`
///
/// This is useful when search parameters are too long for a GET URL.
pub async fn search_post_handler<S>(
    State(state): State<AppState<S>>,
    Path(resource_type): Path<String>,
    tenant: TenantExtractor,
    Form(params): Form<HashMap<String, String>>,
) -> RestResult<Response>
where
    S: ResourceStorage + SearchProvider + Send + Sync,
{
    debug!(
        resource_type = %resource_type,
        tenant = %tenant.tenant_id(),
        params = ?params,
        "Processing search POST request"
    );

    execute_search(&state, tenant, &resource_type, params).await
}

/// Handler for system-level search.
///
/// Searches across all resource types.
///
/// # HTTP Request
///
/// `GET [base]?params`
pub async fn search_system_handler<S>(
    State(state): State<AppState<S>>,
    tenant: TenantExtractor,
    Query(params): Query<HashMap<String, String>>,
) -> RestResult<Response>
where
    S: ResourceStorage + MultiTypeSearchProvider + Send + Sync,
{
    debug!(
        tenant = %tenant.tenant_id(),
        params = ?params,
        "Processing system-level search request"
    );

    execute_system_search(&state, tenant, params).await
}

/// Executes a type-level search and returns a Bundle response.
async fn execute_search<S>(
    state: &AppState<S>,
    tenant: TenantExtractor,
    resource_type: &str,
    params: HashMap<String, String>,
) -> RestResult<Response>
where
    S: ResourceStorage + SearchProvider + Send + Sync,
{
    // Apply pagination limits from config
    let mut params = params;
    apply_pagination_limits(
        &mut params,
        state.default_page_size(),
        state.max_page_size(),
    );

    // Convert REST params to persistence SearchQuery
    let query = build_search_query_from_map(resource_type, &params)?;

    // Execute the search
    // Note: The search provider is responsible for resolving _include/_revinclude
    // directives that are part of the query. The result already contains included resources.
    let result = state
        .storage()
        .search(tenant.context(), &query)
        .await
        .map_err(|e| {
            warn!(error = %e, "Search failed");
            RestError::from(e)
        })?;

    // Build the self link URL
    let self_link = build_search_url(state.base_url(), resource_type, &params);

    // Convert result to FHIR Bundle
    let bundle = result.to_bundle(state.base_url(), &self_link);

    // Parse subsetting parameters
    let summary_mode = params.get("_summary").and_then(|v| SummaryMode::parse(v));
    let elements: Option<Vec<&str>> = params
        .get("_elements")
        .map(|v| v.split(',').map(|s| s.trim()).collect());

    debug!(
        resource_type = %resource_type,
        results = result.resources.len(),
        included = result.included.len(),
        summary = ?summary_mode,
        elements = ?elements,
        "Search completed"
    );

    Ok((
        StatusCode::OK,
        Json(bundle_to_json_with_subsetting(
            bundle,
            summary_mode,
            elements.as_deref(),
        )),
    )
        .into_response())
}

/// Executes a system-level search across all resource types.
#[allow(dead_code)]
async fn execute_system_search<S>(
    state: &AppState<S>,
    tenant: TenantExtractor,
    params: HashMap<String, String>,
) -> RestResult<Response>
where
    S: ResourceStorage + MultiTypeSearchProvider + Send + Sync,
{
    // Apply pagination limits from config
    let mut params = params;
    apply_pagination_limits(
        &mut params,
        state.default_page_size(),
        state.max_page_size(),
    );

    // Get resource types from _type parameter (if specified)
    let resource_types: Vec<&str> = params
        .get("_type")
        .map(|t| t.split(',').collect())
        .unwrap_or_default();

    // Build a search query (resource type doesn't matter much for system search)
    let query = build_search_query_from_map("Resource", &params)?;

    // Execute the multi-type search
    let result = state
        .storage()
        .search_multi(tenant.context(), &resource_types, &query)
        .await
        .map_err(|e| {
            warn!(error = %e, "System-level search failed");
            RestError::from(e)
        })?;

    // Build the self link URL
    let self_link = build_system_search_url(state.base_url(), &params);

    // Convert result to FHIR Bundle
    let bundle = result.to_bundle(state.base_url(), &self_link);

    // Parse subsetting parameters
    let summary_mode = params.get("_summary").and_then(|v| SummaryMode::parse(v));
    let elements: Option<Vec<&str>> = params
        .get("_elements")
        .map(|v| v.split(',').map(|s| s.trim()).collect());

    debug!(
        results = result.resources.len(),
        summary = ?summary_mode,
        elements = ?elements,
        "System-level search completed"
    );

    Ok((
        StatusCode::OK,
        Json(bundle_to_json_with_subsetting(
            bundle,
            summary_mode,
            elements.as_deref(),
        )),
    )
        .into_response())
}

/// Applies pagination limits from configuration to the params.
fn apply_pagination_limits(
    params: &mut HashMap<String, String>,
    default_page_size: usize,
    max_page_size: usize,
) {
    // Parse and limit _count
    let count = params
        .get("_count")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default_page_size)
        .min(max_page_size);

    params.insert("_count".to_string(), count.to_string());
}

/// Builds a type-level search URL from base URL and parameters.
fn build_search_url(
    base_url: &str,
    resource_type: &str,
    params: &HashMap<String, String>,
) -> String {
    if params.is_empty() {
        format!("{}/{}", base_url, resource_type)
    } else {
        let query: String = params
            .iter()
            .map(|(k, v)| format!("{}={}", k, urlencoding::encode(v)))
            .collect::<Vec<_>>()
            .join("&");
        format!("{}/{}?{}", base_url, resource_type, query)
    }
}

/// Builds a system-level search URL from base URL and parameters.
fn build_system_search_url(base_url: &str, params: &HashMap<String, String>) -> String {
    if params.is_empty() {
        base_url.to_string()
    } else {
        let query: String = params
            .iter()
            .map(|(k, v)| format!("{}={}", k, urlencoding::encode(v)))
            .collect::<Vec<_>>()
            .join("&");
        format!("{}?{}", base_url, query)
    }
}

/// Converts a SearchBundle to a serde_json::Value for response with optional subsetting.
fn bundle_to_json_with_subsetting(
    bundle: SearchBundle,
    summary_mode: Option<SummaryMode>,
    elements: Option<&[&str]>,
) -> serde_json::Value {
    // Handle _summary=count specially - only return count, no entries
    if summary_mode == Some(SummaryMode::Count) {
        return serde_json::json!({
            "resourceType": "Bundle",
            "type": bundle.bundle_type,
            "total": bundle.total
        });
    }

    serde_json::json!({
        "resourceType": "Bundle",
        "type": bundle.bundle_type,
        "total": bundle.total,
        "link": bundle.link.iter().map(|l| {
            serde_json::json!({
                "relation": l.relation,
                "url": l.url
            })
        }).collect::<Vec<_>>(),
        "entry": bundle.entry.iter().map(|e| {
            let mut entry = serde_json::json!({});
            if let Some(ref full_url) = e.full_url {
                entry["fullUrl"] = serde_json::Value::String(full_url.clone());
            }
            if let Some(ref resource) = e.resource {
                // Apply subsetting to the resource
                let subsetted = apply_subsetting(resource, summary_mode, elements);
                entry["resource"] = subsetted;
            }
            if let Some(ref search) = e.search {
                entry["search"] = serde_json::json!({
                    "mode": match search.mode {
                        helios_persistence::types::SearchEntryMode::Match => "match",
                        helios_persistence::types::SearchEntryMode::Include => "include",
                        helios_persistence::types::SearchEntryMode::Outcome => "outcome",
                    }
                });
            }
            entry
        }).collect::<Vec<_>>()
    })
}

/// Applies subsetting to a resource based on _summary and _elements parameters.
fn apply_subsetting(
    resource: &serde_json::Value,
    summary_mode: Option<SummaryMode>,
    elements: Option<&[&str]>,
) -> serde_json::Value {
    let mut result = resource.clone();

    // Apply _summary if specified
    if let Some(mode) = summary_mode {
        result = apply_summary(&result, mode);
    }

    // Apply _elements if specified (takes precedence over _summary for element selection)
    if let Some(elem_list) = elements {
        result = apply_elements(&result, elem_list);
    }

    result
}

// URL encoding helper
mod urlencoding {
    pub fn encode(s: &str) -> String {
        url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_search_url_no_params() {
        let url = build_search_url("http://example.com/fhir", "Patient", &HashMap::new());
        assert_eq!(url, "http://example.com/fhir/Patient");
    }

    #[test]
    fn test_build_search_url_with_params() {
        let mut params = HashMap::new();
        params.insert("name".to_string(), "Smith".to_string());
        params.insert("_count".to_string(), "10".to_string());

        let url = build_search_url("http://example.com/fhir", "Patient", &params);
        assert!(url.starts_with("http://example.com/fhir/Patient?"));
        assert!(url.contains("name=Smith"));
        assert!(url.contains("_count=10"));
    }

    #[test]
    fn test_build_system_search_url() {
        let mut params = HashMap::new();
        params.insert("_type".to_string(), "Patient,Observation".to_string());

        let url = build_system_search_url("http://example.com/fhir", &params);
        assert!(url.starts_with("http://example.com/fhir?"));
        assert!(url.contains("_type="));
    }

    #[test]
    fn test_apply_pagination_limits() {
        let mut params = HashMap::new();
        params.insert("_count".to_string(), "1000".to_string());

        apply_pagination_limits(&mut params, 20, 100);

        assert_eq!(params.get("_count"), Some(&"100".to_string()));
    }

    #[test]
    fn test_apply_pagination_limits_default() {
        let mut params = HashMap::new();

        apply_pagination_limits(&mut params, 20, 100);

        assert_eq!(params.get("_count"), Some(&"20".to_string()));
    }
}
