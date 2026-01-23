//! Tests for chained search parameters.
//!
//! This module tests chained search parameters (e.g., patient.name)
//! and reverse chaining (_has).

use serde_json::json;

use helios_persistence::core::{ResourceStorage, SearchProvider};
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use helios_persistence::types::{
    ChainedParameter, Pagination, SearchParamType, SearchParameter, SearchQuery, SearchValue,
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

#[cfg(feature = "sqlite")]
async fn seed_chained_data(backend: &SqliteBackend, tenant: &TenantContext) {
    // Create patients
    let patient1 = json!({
        "resourceType": "Patient",
        "id": "patient-smith",
        "name": [{"family": "Smith", "given": ["John"]}]
    });
    let patient2 = json!({
        "resourceType": "Patient",
        "id": "patient-jones",
        "name": [{"family": "Jones", "given": ["Jane"]}]
    });
    backend.create_or_update(tenant, "Patient", "patient-smith", patient1).await.unwrap();
    backend.create_or_update(tenant, "Patient", "patient-jones", patient2).await.unwrap();

    // Create observations for patients
    let obs1 = json!({
        "resourceType": "Observation",
        "status": "final",
        "subject": {"reference": "Patient/patient-smith"},
        "code": {"coding": [{"system": "http://loinc.org", "code": "8867-4"}]}
    });
    let obs2 = json!({
        "resourceType": "Observation",
        "status": "final",
        "subject": {"reference": "Patient/patient-smith"},
        "code": {"coding": [{"system": "http://loinc.org", "code": "8310-5"}]}
    });
    let obs3 = json!({
        "resourceType": "Observation",
        "status": "final",
        "subject": {"reference": "Patient/patient-jones"},
        "code": {"coding": [{"system": "http://loinc.org", "code": "8867-4"}]}
    });
    backend.create(tenant, "Observation", obs1).await.unwrap();
    backend.create(tenant, "Observation", obs2).await.unwrap();
    backend.create(tenant, "Observation", obs3).await.unwrap();
}

// ============================================================================
// Chained Search Tests
// ============================================================================

/// Test chained search: Observation?subject.name=Smith
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_chained_search_subject_name() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();
    seed_chained_data(&backend, &tenant).await;

    // Search for observations where patient name is Smith
    let query = SearchQuery::new("Observation").with_parameter(SearchParameter {
        name: "subject".to_string(),
        param_type: SearchParamType::Reference,
        modifier: None,
        values: vec![SearchValue::eq("Smith")],
        chain: vec![ChainedParameter {
            resource_type: Some("Patient".to_string()),
            parameter: "name".to_string(),
        }],
    });

    let result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await
        .unwrap();

    // Should find observations for patient Smith
    for resource in &result.resources {
        assert_eq!(
            resource.content()["subject"]["reference"],
            "Patient/patient-smith"
        );
    }
}

/// Test chained search with type hint: Observation?subject:Patient.name=Smith
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_chained_search_with_type_hint() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();
    seed_chained_data(&backend, &tenant).await;

    let query = SearchQuery::new("Observation").with_parameter(SearchParameter {
        name: "subject".to_string(),
        param_type: SearchParamType::Reference,
        modifier: None,
        values: vec![SearchValue::eq("Smith")],
        chain: vec![ChainedParameter {
            resource_type: Some("Patient".to_string()),
            parameter: "name".to_string(),
        }],
    });

    let result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await
        .unwrap();

    // Should find Smith's observations
    assert!(!result.resources.is_empty());
}

/// Test chained search with no results.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_chained_search_no_results() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();
    seed_chained_data(&backend, &tenant).await;

    let query = SearchQuery::new("Observation").with_parameter(SearchParameter {
        name: "subject".to_string(),
        param_type: SearchParamType::Reference,
        modifier: None,
        values: vec![SearchValue::eq("Nonexistent")],
        chain: vec![ChainedParameter {
            resource_type: Some("Patient".to_string()),
            parameter: "name".to_string(),
        }],
    });

    let result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await
        .unwrap();

    assert!(result.resources.is_empty());
}

// ============================================================================
// Reverse Chaining (_has) Tests
// ============================================================================

/// Test reverse chaining: Patient?_has:Observation:subject:code=8867-4
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_reverse_chaining() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();
    seed_chained_data(&backend, &tenant).await;

    // Find patients that have observations with code 8867-4
    // This is expressed as: Patient?_has:Observation:subject:code=8867-4
    let query = SearchQuery::new("Patient").with_has(
        "Observation",
        "subject",
        SearchParameter {
            name: "code".to_string(),
            param_type: SearchParamType::Token,
            modifier: None,
            values: vec![SearchValue::token(Some("http://loinc.org"), "8867-4")],
            chain: vec![],
        },
    );

    let result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await
        .unwrap();

    // Should find both patients (both have 8867-4 observations)
    assert!(!result.resources.is_empty());
}

/// Test reverse chaining with no matches.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_reverse_chaining_no_matches() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();
    seed_chained_data(&backend, &tenant).await;

    // Find patients with nonexistent observation code
    let query = SearchQuery::new("Patient").with_has(
        "Observation",
        "subject",
        SearchParameter {
            name: "code".to_string(),
            param_type: SearchParamType::Token,
            modifier: None,
            values: vec![SearchValue::token(Some("http://loinc.org"), "NONEXISTENT")],
            chain: vec![],
        },
    );

    let result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await
        .unwrap();

    assert!(result.resources.is_empty());
}
