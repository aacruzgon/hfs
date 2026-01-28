//! Read interaction handler.
//!
//! Implements the FHIR [read interaction](https://hl7.org/fhir/http.html#read):
//! `GET [base]/[type]/[id]`

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use helios_persistence::core::ResourceStorage;
use tracing::debug;

use crate::error::{RestError, RestResult};
use crate::extractors::TenantExtractor;
use crate::middleware::conditional::ConditionalHeaders;
use crate::responses::headers::ResourceHeaders;
use crate::state::AppState;

/// Handler for the read interaction.
///
/// Reads a resource by type and ID, returning the current version.
///
/// # HTTP Request
///
/// `GET [base]/[type]/[id]`
///
/// # Headers
///
/// - `Accept` - Content type negotiation (default: application/fhir+json)
/// - `If-None-Match` - Return 304 Not Modified if ETag matches
/// - `If-Modified-Since` - Return 304 Not Modified if not modified since date
///
/// # Response
///
/// - `200 OK` - Resource found, returns the resource
/// - `304 Not Modified` - Resource unchanged (conditional read)
/// - `404 Not Found` - Resource does not exist
/// - `410 Gone` - Resource was deleted
///
/// # Example
///
/// ```http
/// GET /Patient/123 HTTP/1.1
/// Host: fhir.example.com
/// Accept: application/fhir+json
/// ```
pub async fn read_handler<S>(
    State(state): State<AppState<S>>,
    Path((resource_type, id)): Path<(String, String)>,
    tenant: TenantExtractor,
    conditional: ConditionalHeaders,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    debug!(
        resource_type = %resource_type,
        id = %id,
        tenant = %tenant.tenant_id(),
        "Processing read request"
    );

    // Read the resource
    let resource = state
        .storage()
        .read(tenant.context(), &resource_type, &id)
        .await?;

    match resource {
        Some(stored) => {
            // Check conditional headers (If-None-Match)
            if let Some(etag) = conditional.if_none_match() {
                let resource_etag = format!("W/\"{}\"", stored.version_id());
                if etag == resource_etag || etag == "*" {
                    debug!(etag = %resource_etag, "Returning 304 Not Modified");
                    return Ok(StatusCode::NOT_MODIFIED.into_response());
                }
            }

            // Check If-Modified-Since
            if let Some(since) = conditional.if_modified_since() {
                let last_modified = stored.last_modified();
                if last_modified <= since {
                    debug!("Resource not modified since {}", since);
                    return Ok(StatusCode::NOT_MODIFIED.into_response());
                }
            }

            // Build response headers
            let headers = ResourceHeaders::from_stored(&stored, &state);

            // Return the resource
            debug!(
                resource_type = %resource_type,
                id = %id,
                version = %stored.version_id(),
                "Returning resource"
            );

            Ok((
                StatusCode::OK,
                headers.to_header_map(),
                Json(stored.content().clone()),
            )
                .into_response())
        }
        None => {
            debug!(
                resource_type = %resource_type,
                id = %id,
                "Resource not found"
            );
            Err(RestError::NotFound { resource_type, id })
        }
    }
}

#[cfg(test)]
mod tests {
    // Tests will be added in the integration test suite
}
