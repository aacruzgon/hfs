//! Compartment search handler.
//!
//! Implements FHIR [compartment search](https://hl7.org/fhir/compartmentdefinition.html):
//! `GET [base]/[compartment-type]/[id]/[resource-type]?params`
//!
//! Compartment search allows finding all resources related to a specific resource,
//! such as all Observations for a specific Patient.

use std::collections::HashMap;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use helios_persistence::core::{ResourceStorage, SearchProvider};
use tracing::debug;

use crate::error::{RestError, RestResult};
use crate::extractors::{TenantExtractor, build_search_query_from_map};
use crate::state::AppState;

/// Compartment definitions mapping compartment types to their reference parameters.
///
/// For example, the Patient compartment includes resources that reference Patient
/// via parameters like "patient", "subject", "performer", etc.
fn get_compartment_param(compartment_type: &str, target_type: &str) -> Option<&'static str> {
    // Common compartment membership parameters based on FHIR compartment definitions
    // See: https://hl7.org/fhir/compartmentdefinition-patient.html
    match (compartment_type, target_type) {
        // Patient compartment
        ("Patient", "Observation") => Some("subject"),
        ("Patient", "Condition") => Some("subject"),
        ("Patient", "Procedure") => Some("subject"),
        ("Patient", "Encounter") => Some("subject"),
        ("Patient", "DiagnosticReport") => Some("subject"),
        ("Patient", "MedicationRequest") => Some("subject"),
        ("Patient", "MedicationStatement") => Some("subject"),
        ("Patient", "Immunization") => Some("patient"),
        ("Patient", "AllergyIntolerance") => Some("patient"),
        ("Patient", "CarePlan") => Some("subject"),
        ("Patient", "CareTeam") => Some("subject"),
        ("Patient", "Claim") => Some("patient"),
        ("Patient", "Coverage") => Some("beneficiary"),
        ("Patient", "DocumentReference") => Some("subject"),
        ("Patient", "Goal") => Some("subject"),
        ("Patient", "ServiceRequest") => Some("subject"),
        ("Patient", "Appointment") => Some("actor"),
        ("Patient", "Communication") => Some("subject"),
        ("Patient", "Consent") => Some("patient"),
        ("Patient", "Device") => Some("patient"),
        ("Patient", "FamilyMemberHistory") => Some("patient"),
        ("Patient", "Flag") => Some("subject"),
        ("Patient", "ImagingStudy") => Some("subject"),
        ("Patient", "List") => Some("subject"),
        ("Patient", "MeasureReport") => Some("subject"),
        ("Patient", "NutritionOrder") => Some("patient"),
        ("Patient", "QuestionnaireResponse") => Some("subject"),
        ("Patient", "RelatedPerson") => Some("patient"),
        ("Patient", "RiskAssessment") => Some("subject"),
        ("Patient", "Schedule") => Some("actor"),
        ("Patient", "Specimen") => Some("subject"),
        ("Patient", "SupplyDelivery") => Some("patient"),
        ("Patient", "SupplyRequest") => Some("subject"),
        ("Patient", "VisionPrescription") => Some("patient"),

        // Encounter compartment
        ("Encounter", "Observation") => Some("encounter"),
        ("Encounter", "Condition") => Some("encounter"),
        ("Encounter", "Procedure") => Some("encounter"),
        ("Encounter", "DiagnosticReport") => Some("encounter"),
        ("Encounter", "MedicationRequest") => Some("encounter"),
        ("Encounter", "DocumentReference") => Some("encounter"),
        ("Encounter", "Communication") => Some("encounter"),
        ("Encounter", "Composition") => Some("encounter"),

        // Practitioner compartment
        ("Practitioner", "Appointment") => Some("actor"),
        ("Practitioner", "Encounter") => Some("participant"),
        ("Practitioner", "Observation") => Some("performer"),
        ("Practitioner", "Procedure") => Some("performer"),
        ("Practitioner", "DiagnosticReport") => Some("performer"),
        ("Practitioner", "MedicationRequest") => Some("requester"),
        ("Practitioner", "CarePlan") => Some("author"),
        ("Practitioner", "CareTeam") => Some("participant"),
        ("Practitioner", "Communication") => Some("sender"),

        // RelatedPerson compartment
        ("RelatedPerson", "Observation") => Some("performer"),
        ("RelatedPerson", "Procedure") => Some("performer"),
        ("RelatedPerson", "Appointment") => Some("actor"),

        // Device compartment
        ("Device", "Observation") => Some("device"),
        ("Device", "Procedure") => Some("device"),
        ("Device", "DiagnosticReport") => Some("device"),

        _ => None,
    }
}

/// Handler for compartment search.
///
/// Searches for resources within a specific compartment.
///
/// # HTTP Request
///
/// `GET [base]/[compartment-type]/[id]/[resource-type]?params`
///
/// # Examples
///
/// - `GET /Patient/123/Observation?code=8867-4` - Observations for patient 123
/// - `GET /Patient/123/Condition` - All conditions for patient 123
/// - `GET /Encounter/456/Procedure` - Procedures for encounter 456
///
/// # Response
///
/// Returns a Bundle of type "searchset" containing matching resources.
pub async fn compartment_search_handler<S>(
    State(state): State<AppState<S>>,
    Path((compartment_type, compartment_id, target_type)): Path<(String, String, String)>,
    tenant: TenantExtractor,
    Query(mut params): Query<HashMap<String, String>>,
) -> RestResult<Response>
where
    S: ResourceStorage + SearchProvider + Send + Sync,
{
    debug!(
        compartment_type = %compartment_type,
        compartment_id = %compartment_id,
        target_type = %target_type,
        tenant = %tenant.tenant_id(),
        params = ?params,
        "Processing compartment search request"
    );

    // Get the reference parameter for this compartment/target combination
    let ref_param = get_compartment_param(&compartment_type, &target_type).ok_or_else(|| {
        RestError::BadRequest {
            message: format!(
                "Resource type '{}' is not a member of the '{}' compartment",
                target_type, compartment_type
            ),
        }
    })?;

    // Build the compartment reference
    let compartment_ref = format!("{}/{}", compartment_type, compartment_id);

    // Add the compartment reference to the search parameters
    params.insert(ref_param.to_string(), compartment_ref);

    // Apply pagination limits
    apply_pagination_limits(
        &mut params,
        state.default_page_size(),
        state.max_page_size(),
    );

    // Convert REST params to persistence SearchQuery
    let query = build_search_query_from_map(&target_type, &params)?;

    // Execute the search
    let result = state
        .storage()
        .search(tenant.context(), &query)
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, "Compartment search failed");
            RestError::from(e)
        })?;

    // Build the self link URL
    let self_link = build_compartment_search_url(
        state.base_url(),
        &compartment_type,
        &compartment_id,
        &target_type,
        &params,
    );

    // Convert result to FHIR Bundle
    let bundle = result.to_bundle(state.base_url(), &self_link);

    debug!(
        compartment_type = %compartment_type,
        compartment_id = %compartment_id,
        target_type = %target_type,
        results = result.resources.len(),
        "Compartment search completed"
    );

    Ok((StatusCode::OK, Json(bundle_to_json(bundle))).into_response())
}

/// Handler for compartment search across all types.
///
/// Returns all resources in a compartment.
///
/// # HTTP Request
///
/// `GET [base]/[compartment-type]/[id]/*`
///
/// Note: This is a less common operation and returns resources of various types.
#[allow(dead_code)]
pub async fn compartment_search_all_handler<S>(
    State(_state): State<AppState<S>>,
    Path((compartment_type, compartment_id)): Path<(String, String)>,
    _tenant: TenantExtractor,
    Query(_params): Query<HashMap<String, String>>,
) -> RestResult<Response>
where
    S: ResourceStorage + SearchProvider + Send + Sync,
{
    debug!(
        compartment_type = %compartment_type,
        compartment_id = %compartment_id,
        "Processing compartment search all request"
    );

    // For now, return an error - full implementation would search multiple types
    // and combine results
    Err(RestError::BadRequest {
        message: format!(
            "Searching all types in compartment '{}' is not yet implemented. \
             Please specify a resource type: GET /{}/{}/[type]",
            compartment_type, compartment_type, compartment_id
        ),
    })
}

/// Applies pagination limits from configuration to the params.
fn apply_pagination_limits(
    params: &mut HashMap<String, String>,
    default_page_size: usize,
    max_page_size: usize,
) {
    let count = params
        .get("_count")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default_page_size)
        .min(max_page_size);

    params.insert("_count".to_string(), count.to_string());
}

/// Builds a compartment search URL.
fn build_compartment_search_url(
    base_url: &str,
    compartment_type: &str,
    compartment_id: &str,
    target_type: &str,
    params: &HashMap<String, String>,
) -> String {
    let path = format!(
        "{}/{}/{}/{}",
        base_url, compartment_type, compartment_id, target_type
    );

    if params.is_empty() {
        path
    } else {
        let query: String = params
            .iter()
            .map(|(k, v)| format!("{}={}", k, urlencoding::encode(v)))
            .collect::<Vec<_>>()
            .join("&");
        format!("{}?{}", path, query)
    }
}

/// Converts a SearchBundle to a serde_json::Value for response.
fn bundle_to_json(bundle: helios_persistence::types::SearchBundle) -> serde_json::Value {
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
                entry["resource"] = resource.clone();
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
    fn test_get_compartment_param_patient_observation() {
        assert_eq!(
            get_compartment_param("Patient", "Observation"),
            Some("subject")
        );
    }

    #[test]
    fn test_get_compartment_param_patient_immunization() {
        assert_eq!(
            get_compartment_param("Patient", "Immunization"),
            Some("patient")
        );
    }

    #[test]
    fn test_get_compartment_param_encounter_procedure() {
        assert_eq!(
            get_compartment_param("Encounter", "Procedure"),
            Some("encounter")
        );
    }

    #[test]
    fn test_get_compartment_param_unknown() {
        assert_eq!(get_compartment_param("Patient", "UnknownType"), None);
    }

    #[test]
    fn test_build_compartment_search_url_no_params() {
        let url = build_compartment_search_url(
            "http://example.com/fhir",
            "Patient",
            "123",
            "Observation",
            &HashMap::new(),
        );
        assert_eq!(url, "http://example.com/fhir/Patient/123/Observation");
    }

    #[test]
    fn test_build_compartment_search_url_with_params() {
        let mut params = HashMap::new();
        params.insert("code".to_string(), "8867-4".to_string());

        let url = build_compartment_search_url(
            "http://example.com/fhir",
            "Patient",
            "123",
            "Observation",
            &params,
        );

        assert!(url.starts_with("http://example.com/fhir/Patient/123/Observation?"));
        assert!(url.contains("code=8867-4"));
    }
}
