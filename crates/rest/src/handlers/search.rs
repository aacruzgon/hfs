//! Search interaction handler.
//!
//! Implements the FHIR [search interaction](https://hl7.org/fhir/http.html#search):
//! - `GET [base]/[type]?params` - Type-level search
//! - `POST [base]/[type]/_search` - Type-level search (POST)
//! - `GET [base]?params` - System-level search (all types)
//!
//! Note: Full search functionality requires a backend that implements
//! the SearchProvider trait. This module provides basic search support
//! that can be extended by specific backends.

use axum::{
    Form, Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use helios_persistence::core::ResourceStorage;
use serde::Deserialize;
use std::collections::HashMap;
use tracing::debug;

use crate::error::{RestError, RestResult};
use crate::extractors::TenantExtractor;
use crate::state::AppState;

/// Query parameters for search.
#[derive(Debug, Deserialize, Default)]
#[allow(dead_code)]
pub struct SearchQuery {
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
    S: ResourceStorage + Send + Sync,
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
    S: ResourceStorage + Send + Sync,
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
#[allow(dead_code)]
pub async fn search_system_handler<S>(
    State(_state): State<AppState<S>>,
    tenant: TenantExtractor,
    Query(params): Query<HashMap<String, String>>,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    debug!(
        tenant = %tenant.tenant_id(),
        params = ?params,
        "Processing system-level search request"
    );

    // System-level search is not yet fully implemented
    Err(RestError::NotImplemented {
        feature: "System-level search".to_string(),
    })
}

/// Executes a search and returns a Bundle response.
///
/// Note: This is a simplified implementation. Full search requires
/// a backend that implements SearchProvider.
async fn execute_search<S>(
    state: &AppState<S>,
    _tenant: TenantExtractor,
    resource_type: &str,
    params: HashMap<String, String>,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    // Extract pagination parameters
    let count = params
        .get("_count")
        .and_then(|v| v.parse().ok())
        .unwrap_or(state.default_page_size())
        .min(state.max_page_size());

    let offset = params
        .get("_offset")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    // For now, return an empty searchset bundle
    // Full implementation would use SearchProvider trait
    let bundle = build_empty_search_bundle(resource_type, state.base_url(), &params, count, offset);

    debug!(
        resource_type = %resource_type,
        "Search completed (basic implementation)"
    );

    Ok((StatusCode::OK, Json(bundle)).into_response())
}

/// Builds an empty searchset Bundle.
fn build_empty_search_bundle(
    resource_type: &str,
    base_url: &str,
    params: &HashMap<String, String>,
    _count: usize,
    _offset: usize,
) -> serde_json::Value {
    // Build self link
    let self_url = build_search_url(base_url, resource_type, params);

    serde_json::json!({
        "resourceType": "Bundle",
        "type": "searchset",
        "total": 0,
        "link": [{
            "relation": "self",
            "url": self_url
        }],
        "entry": []
    })
}

/// Builds a search URL from base URL and parameters.
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

// Add urlencoding as a simple function for now
mod urlencoding {
    pub fn encode(s: &str) -> String {
        url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
    }
}
