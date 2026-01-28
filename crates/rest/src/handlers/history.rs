//! History interaction handlers.
//!
//! Implements the FHIR [history interaction](https://hl7.org/fhir/http.html#history):
//! - Instance history: `GET [base]/[type]/[id]/_history`
//! - Type history: `GET [base]/[type]/_history`
//! - System history: `GET [base]/_history`
//!
//! Note: Full history functionality requires a backend that implements
//! the InstanceHistoryProvider, TypeHistoryProvider, and SystemHistoryProvider traits.

use axum::{
    extract::{Path, Query, State},
    response::Response,
};
use helios_persistence::core::ResourceStorage;
use serde::Deserialize;
use tracing::debug;

use crate::error::{RestError, RestResult};
use crate::extractors::TenantExtractor;
use crate::state::AppState;

/// Query parameters for history requests.
#[derive(Debug, Deserialize, Default)]
pub struct HistoryQuery {
    /// The page size.
    #[serde(rename = "_count")]
    pub count: Option<usize>,

    /// Only include versions created since this time.
    #[serde(rename = "_since")]
    pub since: Option<String>,

    /// Only include versions created before this time.
    #[serde(rename = "_at")]
    #[allow(dead_code)]
    pub at: Option<String>,
}

/// Handler for instance history.
///
/// Returns the version history for a specific resource instance.
///
/// # HTTP Request
///
/// `GET [base]/[type]/[id]/_history`
///
/// # Query Parameters
///
/// - `_count` - Page size
/// - `_since` - Only versions since this time
///
/// # Response
///
/// Returns a Bundle of type "history".
pub async fn history_instance_handler<S>(
    State(state): State<AppState<S>>,
    Path((resource_type, id)): Path<(String, String)>,
    tenant: TenantExtractor,
    Query(params): Query<HistoryQuery>,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    debug!(
        resource_type = %resource_type,
        id = %id,
        tenant = %tenant.tenant_id(),
        "Processing instance history request"
    );

    let _count = params.count.unwrap_or(state.default_page_size());
    let _since = params.since.as_deref();

    // For now, return a not implemented error
    // Full implementation requires InstanceHistoryProvider
    Err(RestError::NotImplemented {
        feature: format!("Instance history for {}/{}", resource_type, id),
    })
}

/// Handler for type history.
///
/// Returns the version history for all resources of a type.
///
/// # HTTP Request
///
/// `GET [base]/[type]/_history`
pub async fn history_type_handler<S>(
    State(state): State<AppState<S>>,
    Path(resource_type): Path<String>,
    tenant: TenantExtractor,
    Query(params): Query<HistoryQuery>,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    debug!(
        resource_type = %resource_type,
        tenant = %tenant.tenant_id(),
        "Processing type history request"
    );

    let _count = params.count.unwrap_or(state.default_page_size());
    let _since = params.since.as_deref();

    // For now, return a not implemented error
    // Full implementation requires TypeHistoryProvider
    Err(RestError::NotImplemented {
        feature: format!("Type history for {}", resource_type),
    })
}

/// Handler for system history.
///
/// Returns the version history for all resources.
///
/// # HTTP Request
///
/// `GET [base]/_history`
pub async fn history_system_handler<S>(
    State(state): State<AppState<S>>,
    tenant: TenantExtractor,
    Query(params): Query<HistoryQuery>,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    debug!(
        tenant = %tenant.tenant_id(),
        "Processing system history request"
    );

    let _count = params.count.unwrap_or(state.default_page_size());
    let _since = params.since.as_deref();

    // For now, return a not implemented error
    // Full implementation requires SystemHistoryProvider
    Err(RestError::NotImplemented {
        feature: "System history".to_string(),
    })
}

/// Builds a history Bundle from history entries.
#[allow(dead_code)]
fn build_history_bundle(entries: &[HistoryBundleEntry], base_url: &str) -> serde_json::Value {
    let bundle_entries: Vec<serde_json::Value> = entries
        .iter()
        .map(|e| {
            let mut entry = serde_json::json!({
                "fullUrl": format!(
                    "{}/{}/{}/_history/{}",
                    base_url, e.resource_type, e.id, e.version_id
                ),
            });

            // Add request info based on method
            let request = match e.method.as_str() {
                "POST" => serde_json::json!({
                    "method": "POST",
                    "url": e.resource_type
                }),
                "PUT" => serde_json::json!({
                    "method": "PUT",
                    "url": format!("{}/{}", e.resource_type, e.id)
                }),
                "DELETE" => serde_json::json!({
                    "method": "DELETE",
                    "url": format!("{}/{}", e.resource_type, e.id)
                }),
                _ => serde_json::json!({
                    "method": e.method,
                    "url": format!("{}/{}", e.resource_type, e.id)
                }),
            };
            entry["request"] = request;

            // Add response info
            let response = serde_json::json!({
                "status": if e.method == "DELETE" { "204" } else { "200" },
                "etag": format!("W/\"{}\"", e.version_id),
                "lastModified": e.timestamp
            });
            entry["response"] = response;

            // Add resource if not deleted
            if e.method != "DELETE" {
                if let Some(content) = &e.content {
                    entry["resource"] = content.clone();
                }
            }

            entry
        })
        .collect();

    serde_json::json!({
        "resourceType": "Bundle",
        "type": "history",
        "total": bundle_entries.len(),
        "entry": bundle_entries
    })
}

/// A history bundle entry for internal use.
#[derive(Debug)]
#[allow(dead_code)]
struct HistoryBundleEntry {
    resource_type: String,
    id: String,
    version_id: String,
    method: String,
    timestamp: String,
    content: Option<serde_json::Value>,
}
