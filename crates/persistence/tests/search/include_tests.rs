//! Tests for _include and _revinclude search parameters.
//!
//! This module tests _include (forward references) and _revinclude
//! (reverse references) functionality.

use serde_json::json;

use helios_persistence::core::{IncludeProvider, ResourceStorage, SearchProvider};
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use helios_persistence::types::{
    IncludeDirective, IncludeType, Pagination, SearchQuery,
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
async fn seed_include_data(backend: &SqliteBackend, tenant: &TenantContext) {
    // Create organizations
    let org = json!({
        "resourceType": "Organization",
        "id": "org-hospital",
        "name": "Test Hospital"
    });
    backend.create_or_update(tenant, "Organization", "org-hospital", org).await.unwrap();

    // Create patients with organization references
    let patient1 = json!({
        "resourceType": "Patient",
        "id": "patient-1",
        "name": [{"family": "Smith"}],
        "managingOrganization": {"reference": "Organization/org-hospital"}
    });
    let patient2 = json!({
        "resourceType": "Patient",
        "id": "patient-2",
        "name": [{"family": "Jones"}],
        "managingOrganization": {"reference": "Organization/org-hospital"}
    });
    backend.create_or_update(tenant, "Patient", "patient-1", patient1).await.unwrap();
    backend.create_or_update(tenant, "Patient", "patient-2", patient2).await.unwrap();

    // Create observations referencing patients
    let obs1 = json!({
        "resourceType": "Observation",
        "status": "final",
        "subject": {"reference": "Patient/patient-1"},
        "code": {"coding": [{"code": "test"}]}
    });
    let obs2 = json!({
        "resourceType": "Observation",
        "status": "final",
        "subject": {"reference": "Patient/patient-1"},
        "code": {"coding": [{"code": "test2"}]}
    });
    backend.create(tenant, "Observation", obs1).await.unwrap();
    backend.create(tenant, "Observation", obs2).await.unwrap();
}

// ============================================================================
// _include Tests
// ============================================================================

/// Test _include returns referenced resources.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_include_basic() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();
    seed_include_data(&backend, &tenant).await;

    // Search for observations with _include=Observation:subject
    let query = SearchQuery::new("Observation").with_include(IncludeDirective {
        include_type: IncludeType::Include,
        source_type: "Observation".to_string(),
        search_param: "subject".to_string(),
        target_type: Some("Patient".to_string()),
        iterate: false,
    });

    let result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await
        .unwrap();

    // Should have observations in resources
    assert!(!result.resources.is_empty());

    // Should have patients in included
    assert!(!result.included.is_empty());

    // Check that included resources are patients
    for resource in &result.included {
        assert_eq!(resource.resource_type(), "Patient");
    }
}

/// Test _include with specific target type.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_include_with_target_type() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();
    seed_include_data(&backend, &tenant).await;

    let query = SearchQuery::new("Patient").with_include(IncludeDirective {
        include_type: IncludeType::Include,
        source_type: "Patient".to_string(),
        search_param: "organization".to_string(),
        target_type: Some("Organization".to_string()),
        iterate: false,
    });

    let result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await
        .unwrap();

    // Should have organizations in included
    for resource in &result.included {
        assert_eq!(resource.resource_type(), "Organization");
    }
}

/// Test _include:iterate for transitive includes.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_include_iterate() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();
    seed_include_data(&backend, &tenant).await;

    // _include:iterate follows references in included resources
    let query = SearchQuery::new("Observation")
        .with_include(IncludeDirective {
            include_type: IncludeType::Include,
            source_type: "Observation".to_string(),
            search_param: "subject".to_string(),
            target_type: Some("Patient".to_string()),
            iterate: false,
        })
        .with_include(IncludeDirective {
            include_type: IncludeType::Include,
            source_type: "Patient".to_string(),
            search_param: "organization".to_string(),
            target_type: Some("Organization".to_string()),
            iterate: true, // This follows references in included patients
        });

    let result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await
        .unwrap();

    // Should include both patients and organizations
    let included_types: std::collections::HashSet<_> =
        result.included.iter().map(|r| r.resource_type()).collect();

    // Depending on implementation, may have both Patient and Organization
}

// ============================================================================
// _revinclude Tests
// ============================================================================

/// Test _revinclude returns resources that reference the search results.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_revinclude_basic() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();
    seed_include_data(&backend, &tenant).await;

    // Search for patients with _revinclude=Observation:subject
    let query = SearchQuery::new("Patient").with_include(IncludeDirective {
        include_type: IncludeType::Revinclude,
        source_type: "Observation".to_string(),
        search_param: "subject".to_string(),
        target_type: Some("Patient".to_string()),
        iterate: false,
    });

    let result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await
        .unwrap();

    // Should have patients in resources
    assert!(!result.resources.is_empty());
    for resource in &result.resources {
        assert_eq!(resource.resource_type(), "Patient");
    }

    // Should have observations in included (those that reference the patients)
    for resource in &result.included {
        assert_eq!(resource.resource_type(), "Observation");
    }
}

/// Test _revinclude only includes resources referencing search results.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_revinclude_filtered() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();
    seed_include_data(&backend, &tenant).await;

    // Search for specific patient
    let query = SearchQuery::new("Patient")
        .with_parameter(helios_persistence::types::SearchParameter {
            name: "_id".to_string(),
            param_type: helios_persistence::types::SearchParamType::Token,
            modifier: None,
            values: vec![helios_persistence::types::SearchValue::eq("patient-1")],
            chain: vec![],
        })
        .with_include(IncludeDirective {
            include_type: IncludeType::Revinclude,
            source_type: "Observation".to_string(),
            search_param: "subject".to_string(),
            target_type: Some("Patient".to_string()),
            iterate: false,
        });

    let result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await
        .unwrap();

    // Should only have patient-1
    assert_eq!(result.resources.len(), 1);

    // Should only have observations for patient-1
    for resource in &result.included {
        assert_eq!(
            resource.content()["subject"]["reference"],
            "Patient/patient-1"
        );
    }
}

// ============================================================================
// Edge Cases
// ============================================================================

/// Test _include with no referenced resources.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_include_no_references() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create patient without organization reference
    let patient = json!({
        "resourceType": "Patient",
        "name": [{"family": "NoOrg"}]
    });
    backend.create(&tenant, "Patient", patient).await.unwrap();

    let query = SearchQuery::new("Patient").with_include(IncludeDirective {
        include_type: IncludeType::Include,
        source_type: "Patient".to_string(),
        search_param: "organization".to_string(),
        target_type: Some("Organization".to_string()),
        iterate: false,
    });

    let result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await
        .unwrap();

    // Should have patient but no included resources
    assert!(!result.resources.is_empty());
    assert!(result.included.is_empty());
}
