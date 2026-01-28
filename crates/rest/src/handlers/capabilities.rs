//! Capabilities (CapabilityStatement) handler.
//!
//! Implements the FHIR [capabilities interaction](https://hl7.org/fhir/http.html#capabilities):
//! `GET [base]/metadata`

use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use helios_persistence::core::ResourceStorage;
use tracing::debug;

use crate::error::RestResult;
use crate::fhir_types::{get_fhir_version, get_resource_type_names};
use crate::state::AppState;

/// Handler for the capabilities interaction.
///
/// Returns a CapabilityStatement describing the server's capabilities.
///
/// # HTTP Request
///
/// `GET [base]/metadata`
///
/// # Response
///
/// Returns a CapabilityStatement resource (200 OK).
pub async fn capabilities_handler<S>(State(state): State<AppState<S>>) -> RestResult<Response>
where
    S: ResourceStorage + Send + Sync,
{
    debug!("Processing capabilities request");

    let capability_statement = build_capability_statement(&state);

    Ok((StatusCode::OK, Json(capability_statement)).into_response())
}

/// Builds a CapabilityStatement describing server capabilities.
fn build_capability_statement<S>(state: &AppState<S>) -> serde_json::Value
where
    S: ResourceStorage,
{
    let base_url = state.base_url();
    let backend_name = state.storage().backend_name();

    // Get resource types from generated FHIR models
    let resource_types = get_resource_type_names();

    let resources: Vec<serde_json::Value> = resource_types
        .iter()
        .map(|rt| build_resource_capability(rt))
        .collect();

    serde_json::json!({
        "resourceType": "CapabilityStatement",
        "status": "active",
        "date": chrono::Utc::now().to_rfc3339(),
        "kind": "instance",
        "fhirVersion": get_fhir_version(),
        "format": ["json", "application/fhir+json"],
        "implementation": {
            "description": format!("Helios FHIR Server ({})", backend_name),
            "url": base_url
        },
        "rest": [{
            "mode": "server",
            "documentation": "Helios FHIR RESTful API",
            "security": {
                "cors": state.config().enable_cors,
                "description": "This server supports CORS for cross-origin requests"
            },
            "resource": resources,
            "interaction": [
                { "code": "transaction" },
                { "code": "batch" },
                { "code": "history-system" },
                { "code": "search-system" }
            ],
            "operation": [
                {
                    "name": "validate",
                    "definition": "http://hl7.org/fhir/OperationDefinition/Resource-validate"
                }
            ]
        }]
    })
}

/// Builds the capability entry for a resource type.
fn build_resource_capability(resource_type: &str) -> serde_json::Value {
    serde_json::json!({
        "type": resource_type,
        "profile": format!("http://hl7.org/fhir/StructureDefinition/{}", resource_type),
        "interaction": [
            { "code": "read" },
            { "code": "vread" },
            { "code": "update" },
            { "code": "patch" },
            { "code": "delete" },
            { "code": "history-instance" },
            { "code": "history-type" },
            { "code": "create" },
            { "code": "search-type" }
        ],
        "versioning": "versioned",
        "readHistory": true,
        "updateCreate": true,
        "conditionalCreate": true,
        "conditionalRead": "full-support",
        "conditionalUpdate": true,
        "conditionalDelete": "single",
        "searchInclude": ["*"],
        "searchRevInclude": ["*"],
        "searchParam": build_common_search_params()
    })
}

/// Builds common search parameters supported by all resources.
fn build_common_search_params() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "name": "_id",
            "type": "token",
            "documentation": "Logical id of this artifact"
        }),
        serde_json::json!({
            "name": "_lastUpdated",
            "type": "date",
            "documentation": "When the resource version last changed"
        }),
        serde_json::json!({
            "name": "_tag",
            "type": "token",
            "documentation": "Tags applied to this resource"
        }),
        serde_json::json!({
            "name": "_profile",
            "type": "uri",
            "documentation": "Profiles this resource claims to conform to"
        }),
        serde_json::json!({
            "name": "_security",
            "type": "token",
            "documentation": "Security Labels applied to this resource"
        }),
        serde_json::json!({
            "name": "_text",
            "type": "string",
            "documentation": "Search on the narrative of the resource"
        }),
        serde_json::json!({
            "name": "_content",
            "type": "string",
            "documentation": "Search on the entire content of the resource"
        }),
    ]
}
