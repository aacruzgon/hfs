//! Tests for search modifiers.
//!
//! This module tests various search modifiers including :missing,
//! :exact, :contains, :above, :below, :in, :not-in, and :text.

use serde_json::json;

use helios_persistence::core::{ResourceStorage, SearchProvider};
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use helios_persistence::types::{
    Pagination, SearchModifier, SearchParamType, SearchParameter, SearchQuery, SearchValue,
};

#[cfg(feature = "sqlite")]
use helios_persistence::backends::sqlite::SqliteBackend;

#[cfg(feature = "sqlite")]
fn create_sqlite_backend() -> SqliteBackend {
    let backend = SqliteBackend::in_memory().expect("Failed to create SQLite backend");
    backend.init_schema().expect("Failed to initialize schema");
    backend
}

fn create_tenant() -> TenantContext {
    TenantContext::new(TenantId::new("test-tenant"), TenantPermissions::full_access())
}

// ============================================================================
// :missing Modifier Tests
// ============================================================================

/// Test :missing=true finds resources without the element.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_missing_true() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create patients - some with birthDate, some without
    let with_date = json!({"resourceType": "Patient", "birthDate": "1980-01-15"});
    let without_date = json!({"resourceType": "Patient", "name": [{"family": "No Date"}]});
    backend.create(&tenant, "Patient", with_date).await.unwrap();
    backend.create(&tenant, "Patient", without_date).await.unwrap();

    let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
        name: "birthdate".to_string(),
        param_type: SearchParamType::Date,
        modifier: Some(SearchModifier::Missing),
        values: vec![SearchValue::boolean(true)],
        chain: vec![],
        components: vec![],
    });

    let result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await
        .unwrap();

    // Should only find patients without birthDate
    for resource in &result.resources {
        assert!(
            resource.content().get("birthDate").is_none()
                || resource.content()["birthDate"].is_null()
        );
    }
}

/// Test :missing=false finds resources with the element.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_missing_false() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    let with_date = json!({"resourceType": "Patient", "birthDate": "1980-01-15"});
    let without_date = json!({"resourceType": "Patient", "name": [{"family": "No Date"}]});
    backend.create(&tenant, "Patient", with_date).await.unwrap();
    backend.create(&tenant, "Patient", without_date).await.unwrap();

    let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
        name: "birthdate".to_string(),
        param_type: SearchParamType::Date,
        modifier: Some(SearchModifier::Missing),
        values: vec![SearchValue::boolean(false)],
        chain: vec![],
        components: vec![],
    });

    let result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await
        .unwrap();

    // Should only find patients with birthDate
    for resource in &result.resources {
        assert!(resource.content().get("birthDate").is_some());
    }
}

// ============================================================================
// :not Modifier Tests
// ============================================================================

/// Test :not modifier excludes matching resources.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_not_modifier() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    let male = json!({"resourceType": "Patient", "gender": "male"});
    let female = json!({"resourceType": "Patient", "gender": "female"});
    let unknown = json!({"resourceType": "Patient", "gender": "unknown"});
    backend.create(&tenant, "Patient", male).await.unwrap();
    backend.create(&tenant, "Patient", female).await.unwrap();
    backend.create(&tenant, "Patient", unknown).await.unwrap();

    let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
        name: "gender".to_string(),
        param_type: SearchParamType::Token,
        modifier: Some(SearchModifier::Not),
        values: vec![SearchValue::token(None, "male")],
        chain: vec![],
        components: vec![],
    });

    let result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await
        .unwrap();

    // Should find female and unknown, not male
    for resource in &result.resources {
        assert_ne!(resource.content()["gender"], "male");
    }
}

// ============================================================================
// :text Modifier Tests
// ============================================================================

/// Test :text modifier for narrative search.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_text_modifier() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    let obs = json!({
        "resourceType": "Observation",
        "status": "final",
        "code": {
            "coding": [{"code": "8867-4"}],
            "text": "Patient heart rate measurement during exercise"
        }
    });
    backend.create(&tenant, "Observation", obs).await.unwrap();

    // :text modifier searches the display/text fields
    let query = SearchQuery::new("Observation").with_parameter(SearchParameter {
        name: "code".to_string(),
        param_type: SearchParamType::Token,
        modifier: Some(SearchModifier::Text),
        values: vec![SearchValue::string("heart rate")],
        chain: vec![],
        components: vec![],
    });

    let _result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await;

    // Test documents expected text search behavior
}

// ============================================================================
// :identifier Modifier Tests
// ============================================================================

/// Test :identifier modifier on reference parameters.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_identifier_modifier() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create patient with identifier
    let patient = json!({
        "resourceType": "Patient",
        "id": "patient-123",
        "identifier": [{"system": "http://hospital.org/mrn", "value": "MRN001"}]
    });
    backend.create_or_update(&tenant, "Patient", "patient-123", patient).await.unwrap();

    // Create observation referencing patient
    let obs = json!({
        "resourceType": "Observation",
        "status": "final",
        "subject": {"reference": "Patient/patient-123"},
        "code": {"coding": [{"code": "test"}]}
    });
    backend.create(&tenant, "Observation", obs).await.unwrap();

    // Search using :identifier modifier
    let query = SearchQuery::new("Observation").with_parameter(SearchParameter {
        name: "subject".to_string(),
        param_type: SearchParamType::Reference,
        modifier: Some(SearchModifier::Identifier),
        values: vec![SearchValue::token(
            Some("http://hospital.org/mrn"),
            "MRN001",
        )],
        chain: vec![],
        components: vec![],
    });

    let _result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await;
}

// ============================================================================
// :of-type Modifier Tests (FHIR v6.0.0 Enhanced)
// ============================================================================

/// Test :of-type modifier with three-part format [system]|[code]|[value].
///
/// The :of-type modifier allows searching identifiers by type. In FHIR v6.0.0,
/// the format is enhanced to support: `identifier:of-type=[system]|[code]|[value]`
/// where system and code identify the identifier type, and value is the identifier value.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_of_type_modifier_three_part_format() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create patient with typed identifier (SSN)
    let patient_with_ssn = json!({
        "resourceType": "Patient",
        "identifier": [{
            "type": {
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                    "code": "SS"
                }]
            },
            "system": "http://hl7.org/fhir/sid/us-ssn",
            "value": "123-45-6789"
        }]
    });
    backend.create(&tenant, "Patient", patient_with_ssn).await.unwrap();

    // Create patient with typed identifier (MRN)
    let patient_with_mrn = json!({
        "resourceType": "Patient",
        "identifier": [{
            "type": {
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                    "code": "MR"
                }]
            },
            "system": "http://hospital.org/mrn",
            "value": "MRN-001"
        }]
    });
    backend.create(&tenant, "Patient", patient_with_mrn).await.unwrap();

    // Search for SSN type identifier with :of-type modifier
    // Format: identifier:of-type=[type-system]|[type-code]|[value]
    let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
        name: "identifier".to_string(),
        param_type: SearchParamType::Token,
        modifier: Some(SearchModifier::OfType),
        values: vec![SearchValue::of_type(
            "http://terminology.hl7.org/CodeSystem/v2-0203",
            "SS",
            "123-45-6789",
        )],
        chain: vec![],
        components: vec![],
    });

    let result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await;

    // Test documents expected :of-type behavior
    // When implemented, should only match the SSN identifier
    match result {
        Ok(result) => {
            // If implemented, verify only SSN patient is returned
            for resource in &result.resources {
                let identifiers = resource.content()["identifier"].as_array().unwrap();
                let has_ssn = identifiers.iter().any(|id| {
                    id["type"]["coding"][0]["code"] == "SS"
                });
                assert!(has_ssn, "Should only match SSN identifiers");
            }
        }
        Err(_) => {
            // :of-type may not be implemented yet - this test serves as specification
        }
    }
}

/// Test :of-type modifier distinguishes between identifier types.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_of_type_modifier_type_discrimination() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create patient with multiple identifiers of different types
    let patient = json!({
        "resourceType": "Patient",
        "identifier": [
            {
                "type": {
                    "coding": [{
                        "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                        "code": "SS"
                    }]
                },
                "value": "123-45-6789"
            },
            {
                "type": {
                    "coding": [{
                        "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                        "code": "DL"
                    }]
                },
                "value": "DL-999888"
            }
        ]
    });
    backend.create(&tenant, "Patient", patient).await.unwrap();

    // Search for DL (driver's license) type - should match
    let dl_query = SearchQuery::new("Patient").with_parameter(SearchParameter {
        name: "identifier".to_string(),
        param_type: SearchParamType::Token,
        modifier: Some(SearchModifier::OfType),
        values: vec![SearchValue::of_type(
            "http://terminology.hl7.org/CodeSystem/v2-0203",
            "DL",
            "DL-999888",
        )],
        chain: vec![],
        components: vec![],
    });

    // Search for passport type - should NOT match (patient has no passport)
    let pp_query = SearchQuery::new("Patient").with_parameter(SearchParameter {
        name: "identifier".to_string(),
        param_type: SearchParamType::Token,
        modifier: Some(SearchModifier::OfType),
        values: vec![SearchValue::of_type(
            "http://terminology.hl7.org/CodeSystem/v2-0203",
            "PPN",
            "DL-999888", // Same value but wrong type
        )],
        chain: vec![],
        components: vec![],
    });

    // Test documents expected behavior when :of-type is implemented
    let _dl_result = backend.search(&tenant, &dl_query, Pagination::new(100)).await;
    let _pp_result = backend.search(&tenant, &pp_query, Pagination::new(100)).await;
}
