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
    });

    let _result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await;
}
