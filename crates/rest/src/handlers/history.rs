//! History interaction handlers.
//!
//! Implements the FHIR [history interaction](https://hl7.org/fhir/http.html#history):
//! - Instance history: `GET [base]/[type]/[id]/_history`
//! - Type history: `GET [base]/[type]/_history`
//! - System history: `GET [base]/_history`
//!
//! Also implements FHIR v6.0.0 Trial Use delete history operations:
//! - Delete instance history: `DELETE [base]/[type]/[id]/_history`
//! - Delete specific version: `DELETE [base]/[type]/[id]/_history/[vid]`
//!
//! Note: Full history functionality requires a backend that implements
//! the InstanceHistoryProvider, TypeHistoryProvider, and SystemHistoryProvider traits.

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use helios_persistence::core::{InstanceHistoryProvider, ResourceStorage};
use serde::Deserialize;
use tracing::{debug, warn};

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

/// Handler for deleting instance history.
///
/// Deletes all historical versions of a resource, preserving only the current version.
/// This is a FHIR v6.0.0 Trial Use feature.
///
/// # HTTP Request
///
/// `DELETE [base]/[type]/[id]/_history`
///
/// # Response
///
/// - `200 OK` with OperationOutcome indicating number of versions deleted
/// - `404 Not Found` if the resource doesn't exist
/// - `501 Not Implemented` if the backend doesn't support this operation
pub async fn delete_instance_history_handler<S>(
    State(state): State<AppState<S>>,
    Path((resource_type, id)): Path<(String, String)>,
    tenant: TenantExtractor,
) -> RestResult<Response>
where
    S: ResourceStorage + InstanceHistoryProvider + Send + Sync,
{
    debug!(
        resource_type = %resource_type,
        id = %id,
        tenant = %tenant.tenant_id(),
        "Processing delete instance history request"
    );

    let deleted_count = state
        .storage()
        .delete_instance_history(tenant.context(), &resource_type, &id)
        .await
        .map_err(|e| {
            warn!(error = %e, "Delete instance history failed");
            RestError::from(e)
        })?;

    debug!(
        resource_type = %resource_type,
        id = %id,
        deleted_count = deleted_count,
        "Instance history deleted successfully"
    );

    // Return OperationOutcome with success message
    let outcome = serde_json::json!({
        "resourceType": "OperationOutcome",
        "issue": [{
            "severity": "information",
            "code": "informational",
            "diagnostics": format!(
                "Deleted {} historical version(s) of {}/{}. Current version preserved.",
                deleted_count, resource_type, id
            )
        }]
    });

    Ok((StatusCode::OK, Json(outcome)).into_response())
}

/// Handler for deleting a specific version from history.
///
/// Deletes a specific historical version of a resource. Cannot delete the current version.
/// This is a FHIR v6.0.0 Trial Use feature.
///
/// # HTTP Request
///
/// `DELETE [base]/[type]/[id]/_history/[vid]`
///
/// # Response
///
/// - `204 No Content` on successful deletion
/// - `404 Not Found` if the resource or version doesn't exist
/// - `400 Bad Request` if attempting to delete the current version
/// - `501 Not Implemented` if the backend doesn't support this operation
pub async fn delete_version_handler<S>(
    State(state): State<AppState<S>>,
    Path((resource_type, id, version_id)): Path<(String, String, String)>,
    tenant: TenantExtractor,
) -> RestResult<Response>
where
    S: ResourceStorage + InstanceHistoryProvider + Send + Sync,
{
    debug!(
        resource_type = %resource_type,
        id = %id,
        version_id = %version_id,
        tenant = %tenant.tenant_id(),
        "Processing delete version request"
    );

    state
        .storage()
        .delete_version(tenant.context(), &resource_type, &id, &version_id)
        .await
        .map_err(|e| {
            warn!(error = %e, "Delete version failed");
            RestError::from(e)
        })?;

    debug!(
        resource_type = %resource_type,
        id = %id,
        version_id = %version_id,
        "Version deleted successfully"
    );

    Ok(StatusCode::NO_CONTENT.into_response())
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
