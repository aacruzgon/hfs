//! Create interaction handler.
//!
//! Implements the FHIR [create interaction](https://hl7.org/fhir/http.html#create):
//! `POST [base]/[type]`

use axum::{
    Json,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use helios_persistence::core::{ConditionalStorage, ResourceStorage};
use serde_json::Value;
use tracing::debug;

use crate::error::{RestError, RestResult};
use crate::extractors::TenantExtractor;
use crate::middleware::conditional::ConditionalHeaders;
use crate::middleware::prefer::PreferHeader;
use crate::responses::headers::ResourceHeaders;
use crate::state::AppState;

/// Handler for the create interaction.
///
/// Creates a new resource. The server assigns the resource ID.
///
/// # HTTP Request
///
/// `POST [base]/[type]`
///
/// # Headers
///
/// - `Content-Type` - Must be application/fhir+json or application/fhir+xml
/// - `If-None-Exist` - Conditional create search parameters
/// - `Prefer` - Response preference (return=minimal, return=representation, return=OperationOutcome)
///
/// # Response
///
/// - `201 Created` - Resource created successfully
/// - `200 OK` - Conditional create matched existing resource
/// - `400 Bad Request` - Invalid resource
/// - `412 Precondition Failed` - Conditional create matched multiple resources
///
/// # Example
///
/// ```http
/// POST /Patient HTTP/1.1
/// Host: fhir.example.com
/// Content-Type: application/fhir+json
/// Prefer: return=representation
///
/// {"resourceType": "Patient", "name": [{"family": "Smith"}]}
/// ```
pub async fn create_handler<S>(
    State(state): State<AppState<S>>,
    Path(resource_type): Path<String>,
    tenant: TenantExtractor,
    conditional: ConditionalHeaders,
    prefer: PreferHeader,
    Json(resource): Json<Value>,
) -> RestResult<Response>
where
    S: ResourceStorage + ConditionalStorage + Send + Sync,
{
    debug!(
        resource_type = %resource_type,
        tenant = %tenant.tenant_id(),
        conditional = ?conditional.if_none_exist(),
        "Processing create request"
    );

    // Validate resourceType in body matches URL
    if let Some(body_type) = resource.get("resourceType").and_then(|v| v.as_str()) {
        if body_type != resource_type {
            return Err(RestError::BadRequest {
                message: format!(
                    "Resource type in body ({}) does not match URL ({})",
                    body_type, resource_type
                ),
            });
        }
    } else {
        return Err(RestError::BadRequest {
            message: "Resource must contain resourceType".to_string(),
        });
    }

    // Check for conditional create
    if let Some(search_params) = conditional.if_none_exist() {
        debug!(search_params = %search_params, "Processing conditional create");

        let result = state
            .storage()
            .conditional_create(tenant.context(), &resource_type, resource, &search_params)
            .await?;

        use helios_persistence::core::ConditionalCreateResult;
        return match result {
            ConditionalCreateResult::Created(stored) => {
                let headers = ResourceHeaders::from_stored(&stored, &state);
                let location = format!("{}/{}/{}", state.base_url(), resource_type, stored.id());

                debug!(
                    resource_type = %resource_type,
                    id = %stored.id(),
                    "Resource created (conditional)"
                );

                build_create_response(StatusCode::CREATED, &stored, headers, &location, &prefer)
            }
            ConditionalCreateResult::Exists(stored) => {
                let headers = ResourceHeaders::from_stored(&stored, &state);

                debug!(
                    resource_type = %resource_type,
                    id = %stored.id(),
                    "Existing resource matched conditional create"
                );

                // Return 200 OK with the existing resource
                build_existing_response(&stored, headers, &prefer)
            }
            ConditionalCreateResult::MultipleMatches(count) => Err(RestError::MultipleMatches {
                operation: "create".to_string(),
                count,
            }),
        };
    }

    // Standard create
    let stored = state
        .storage()
        .create(tenant.context(), &resource_type, resource)
        .await?;

    let headers = ResourceHeaders::from_stored(&stored, &state);
    let location = format!("{}/{}/{}", state.base_url(), resource_type, stored.id());

    debug!(
        resource_type = %resource_type,
        id = %stored.id(),
        "Resource created"
    );

    build_create_response(StatusCode::CREATED, &stored, headers, &location, &prefer)
}

/// Builds the response for a successful create.
fn build_create_response(
    status: StatusCode,
    stored: &helios_persistence::types::StoredResource,
    headers: ResourceHeaders,
    location: &str,
    prefer: &PreferHeader,
) -> RestResult<Response> {
    let mut header_map = headers.to_header_map();
    header_map.insert(header::LOCATION, location.parse().unwrap());

    match prefer.return_preference() {
        Some("minimal") => Ok((status, header_map).into_response()),
        Some("OperationOutcome") => {
            let outcome = serde_json::json!({
                "resourceType": "OperationOutcome",
                "issue": [{
                    "severity": "information",
                    "code": "informational",
                    "details": {
                        "text": format!("Resource created: {}", location)
                    }
                }]
            });
            Ok((status, header_map, Json(outcome)).into_response())
        }
        _ => {
            // Default: return=representation
            Ok((status, header_map, Json(stored.content().clone())).into_response())
        }
    }
}

/// Builds the response for an existing resource (conditional create match).
fn build_existing_response(
    stored: &helios_persistence::types::StoredResource,
    headers: ResourceHeaders,
    prefer: &PreferHeader,
) -> RestResult<Response> {
    let header_map = headers.to_header_map();

    match prefer.return_preference() {
        Some("minimal") => Ok((StatusCode::OK, header_map).into_response()),
        _ => Ok((StatusCode::OK, header_map, Json(stored.content().clone())).into_response()),
    }
}
