//! Batch and transaction processing handler.
//!
//! Implements the FHIR [batch/transaction interaction](https://hl7.org/fhir/http.html#transaction):
//! `POST [base]` with a Bundle of type "batch" or "transaction"

use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use helios_persistence::core::ResourceStorage;
use serde_json::Value;
use tracing::{debug, warn};

use crate::error::{RestError, RestResult};
use crate::extractors::TenantExtractor;
use crate::state::AppState;

/// Handler for batch/transaction processing.
///
/// Processes a Bundle of type "batch" or "transaction".
///
/// # HTTP Request
///
/// `POST [base]`
///
/// # Request Body
///
/// A Bundle resource with type "batch" or "transaction" containing entries
/// with request information.
///
/// # Response
///
/// Returns a Bundle of type "batch-response" or "transaction-response"
/// with the results of each operation.
///
/// # Batch vs Transaction
///
/// - **Batch**: Each entry is processed independently. Failures don't affect other entries.
/// - **Transaction**: All entries are processed atomically. Any failure rolls back all changes.
pub async fn batch_handler<S>(
    State(state): State<AppState<S>>,
    tenant: TenantExtractor,
    Json(bundle): Json<Value>,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    // Validate it's a Bundle
    let resource_type = bundle
        .get("resourceType")
        .and_then(|v| v.as_str())
        .ok_or_else(|| RestError::BadRequest {
            message: "Request must be a Bundle resource".to_string(),
        })?;

    if resource_type != "Bundle" {
        return Err(RestError::BadRequest {
            message: format!("Expected Bundle, got {}", resource_type),
        });
    }

    // Get Bundle type
    let bundle_type =
        bundle
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| RestError::BadRequest {
                message: "Bundle must have a type".to_string(),
            })?;

    match bundle_type {
        "batch" => process_batch(&state, tenant, &bundle).await,
        "transaction" => process_transaction(&state, tenant, &bundle).await,
        _ => Err(RestError::BadRequest {
            message: format!(
                "Bundle type must be 'batch' or 'transaction', got '{}'",
                bundle_type
            ),
        }),
    }
}

/// Processes a batch Bundle.
async fn process_batch<S>(
    state: &AppState<S>,
    tenant: TenantExtractor,
    bundle: &Value,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    debug!(
        tenant = %tenant.tenant_id(),
        "Processing batch request"
    );

    let entries = bundle
        .get("entry")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let mut response_entries = Vec::with_capacity(entries.len());

    for (index, entry) in entries.iter().enumerate() {
        let result = process_batch_entry(state, &tenant, entry, index).await;
        response_entries.push(result);
    }

    let response_bundle = serde_json::json!({
        "resourceType": "Bundle",
        "type": "batch-response",
        "entry": response_entries
    });

    debug!(
        entries = response_entries.len(),
        "Batch processing completed"
    );

    Ok((StatusCode::OK, Json(response_bundle)).into_response())
}

/// Processes a transaction Bundle.
async fn process_transaction<S>(
    _state: &AppState<S>,
    tenant: TenantExtractor,
    _bundle: &Value,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    debug!(
        tenant = %tenant.tenant_id(),
        "Processing transaction request"
    );

    // Transaction processing requires database transaction support
    // This is a placeholder that will be implemented with proper transaction support
    Err(RestError::NotImplemented {
        feature: "Transaction bundles".to_string(),
    })
}

/// Processes a single batch entry.
async fn process_batch_entry<S>(
    state: &AppState<S>,
    tenant: &TenantExtractor,
    entry: &Value,
    index: usize,
) -> Value
where
    S: ResourceStorage + Send + Sync,
{
    let request = match entry.get("request") {
        Some(r) => r,
        None => {
            return create_error_entry("400", &format!("Entry {} missing request", index));
        }
    };

    let method = request.get("method").and_then(|v| v.as_str()).unwrap_or("");
    let url = request.get("url").and_then(|v| v.as_str()).unwrap_or("");

    // Parse the URL to extract resource type and ID
    let (resource_type, id) = match parse_request_url(url) {
        Ok(parsed) => parsed,
        Err(e) => {
            return create_error_entry("400", &e);
        }
    };

    match method {
        "GET" => {
            // Read operation
            match state
                .storage()
                .read(tenant.context(), &resource_type, &id)
                .await
            {
                Ok(Some(stored)) => {
                    serde_json::json!({
                        "resource": stored.content(),
                        "response": {
                            "status": "200 OK",
                            "etag": format!("W/\"{}\"", stored.version_id())
                        }
                    })
                }
                Ok(None) => create_error_entry("404", "Resource not found"),
                Err(e) => create_error_entry("500", &e.to_string()),
            }
        }
        "POST" => {
            // Create operation
            let resource = match entry.get("resource") {
                Some(r) => r.clone(),
                None => {
                    return create_error_entry("400", "POST entry missing resource");
                }
            };

            match state
                .storage()
                .create(tenant.context(), &resource_type, resource)
                .await
            {
                Ok(stored) => {
                    serde_json::json!({
                        "resource": stored.content(),
                        "response": {
                            "status": "201 Created",
                            "location": format!("{}/{}", resource_type, stored.id()),
                            "etag": format!("W/\"{}\"", stored.version_id())
                        }
                    })
                }
                Err(e) => create_error_entry("400", &e.to_string()),
            }
        }
        "PUT" => {
            // Update operation
            let resource = match entry.get("resource") {
                Some(r) => r.clone(),
                None => {
                    return create_error_entry("400", "PUT entry missing resource");
                }
            };

            match state
                .storage()
                .create_or_update(tenant.context(), &resource_type, &id, resource)
                .await
            {
                Ok((stored, created)) => {
                    let status = if created { "201 Created" } else { "200 OK" };
                    serde_json::json!({
                        "resource": stored.content(),
                        "response": {
                            "status": status,
                            "etag": format!("W/\"{}\"", stored.version_id())
                        }
                    })
                }
                Err(e) => create_error_entry("400", &e.to_string()),
            }
        }
        "DELETE" => {
            // Delete operation
            match state
                .storage()
                .delete(tenant.context(), &resource_type, &id)
                .await
            {
                Ok(()) => {
                    serde_json::json!({
                        "response": {
                            "status": "204 No Content"
                        }
                    })
                }
                Err(e) => create_error_entry("404", &e.to_string()),
            }
        }
        _ => {
            warn!(method = method, "Unsupported batch method");
            create_error_entry("405", &format!("Unsupported method: {}", method))
        }
    }
}

/// Parses a request URL to extract resource type and optional ID.
fn parse_request_url(url: &str) -> Result<(String, String), String> {
    let parts: Vec<&str> = url.trim_start_matches('/').split('/').collect();

    match parts.len() {
        0 => Err("Empty URL".to_string()),
        1 => Ok((parts[0].to_string(), String::new())),
        2 => Ok((parts[0].to_string(), parts[1].to_string())),
        _ => {
            // Handle URLs like Patient/123/_history/1
            Ok((parts[0].to_string(), parts[1].to_string()))
        }
    }
}

/// Creates an error response entry.
fn create_error_entry(status: &str, message: &str) -> Value {
    serde_json::json!({
        "response": {
            "status": format!("{} {}", status, status_text(status)),
            "outcome": {
                "resourceType": "OperationOutcome",
                "issue": [{
                    "severity": "error",
                    "code": "processing",
                    "details": {
                        "text": message
                    }
                }]
            }
        }
    })
}

/// Returns HTTP status text for a status code.
fn status_text(code: &str) -> &'static str {
    match code {
        "200" => "OK",
        "201" => "Created",
        "204" => "No Content",
        "400" => "Bad Request",
        "404" => "Not Found",
        "405" => "Method Not Allowed",
        "409" => "Conflict",
        "412" => "Precondition Failed",
        "500" => "Internal Server Error",
        _ => "Unknown",
    }
}
