//! Version read (vread) interaction handler.
//!
//! Implements the FHIR [vread interaction](https://hl7.org/fhir/http.html#vread):
//! `GET [base]/[type]/[id]/_history/[vid]`
//!
//! Note: Full vread functionality requires a backend that implements
//! the VersionedStorage trait.

use axum::{
    extract::{Path, State},
    response::Response,
};
use helios_persistence::core::ResourceStorage;
use tracing::debug;

use crate::error::{RestError, RestResult};
use crate::extractors::TenantExtractor;
use crate::state::AppState;

/// Handler for the vread interaction.
///
/// Reads a specific version of a resource.
///
/// # HTTP Request
///
/// `GET [base]/[type]/[id]/_history/[vid]`
///
/// # Response
///
/// - `200 OK` - Version found, returns the resource
/// - `404 Not Found` - Resource or version does not exist
///
/// # Example
///
/// ```http
/// GET /Patient/123/_history/2 HTTP/1.1
/// Host: fhir.example.com
/// Accept: application/fhir+json
/// ```
pub async fn vread_handler<S>(
    State(_state): State<AppState<S>>,
    Path((resource_type, id, version_id)): Path<(String, String, String)>,
    tenant: TenantExtractor,
) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    debug!(
        resource_type = %resource_type,
        id = %id,
        version_id = %version_id,
        tenant = %tenant.tenant_id(),
        "Processing vread request"
    );

    // For now, return a not implemented error
    // Full implementation requires VersionedStorage trait
    Err(RestError::NotImplemented {
        feature: format!(
            "Version read for {}/{}/_history/{}",
            resource_type, id, version_id
        ),
    })
}
