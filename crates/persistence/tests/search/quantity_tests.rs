//! Tests for quantity search parameters.
//!
//! This module tests quantity-type search parameters including
//! value|system|code format and unit-aware comparisons.

use serde_json::json;

use helios_persistence::core::{ResourceStorage, SearchProvider};
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use helios_persistence::types::{
    Pagination, SearchParamType, SearchParameter, SearchPrefix, SearchQuery, SearchValue,
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
async fn seed_observations(backend: &SqliteBackend, tenant: &TenantContext) {
    let observations = vec![
        json!({
            "resourceType": "Observation",
            "status": "final",
            "code": {"coding": [{"code": "29463-7", "display": "Body weight"}]},
            "valueQuantity": {
                "value": 70,
                "unit": "kg",
                "system": "http://unitsofmeasure.org",
                "code": "kg"
            }
        }),
        json!({
            "resourceType": "Observation",
            "status": "final",
            "code": {"coding": [{"code": "29463-7", "display": "Body weight"}]},
            "valueQuantity": {
                "value": 154,
                "unit": "lb",
                "system": "http://unitsofmeasure.org",
                "code": "[lb_av]"
            }
        }),
        json!({
            "resourceType": "Observation",
            "status": "final",
            "code": {"coding": [{"code": "8302-2", "display": "Body height"}]},
            "valueQuantity": {
                "value": 175,
                "unit": "cm",
                "system": "http://unitsofmeasure.org",
                "code": "cm"
            }
        }),
    ];

    for obs in observations {
        backend.create(tenant, "Observation", obs).await.unwrap();
    }
}

/// Test quantity search with value only.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_quantity_search_value_only() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();
    seed_observations(&backend, &tenant).await;

    let query = SearchQuery::new("Observation").with_parameter(SearchParameter {
        name: "value-quantity".to_string(),
        param_type: SearchParamType::Quantity,
        modifier: None,
        values: vec![SearchValue::quantity(SearchPrefix::Eq, 70.0, None, None)],
        chain: vec![],
        components: vec![],
    });

    let result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await
        .unwrap();

    // Should find observation with value 70
    for resource in &result.resources {
        if let Some(value) = resource.content()["valueQuantity"]["value"].as_f64() {
            assert!((value - 70.0).abs() < 0.1);
        }
    }
}

/// Test quantity search with value and unit.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_quantity_search_value_and_unit() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();
    seed_observations(&backend, &tenant).await;

    let query = SearchQuery::new("Observation").with_parameter(SearchParameter {
        name: "value-quantity".to_string(),
        param_type: SearchParamType::Quantity,
        modifier: None,
        values: vec![SearchValue::quantity(
            SearchPrefix::Eq,
            70.0,
            Some("http://unitsofmeasure.org"),
            Some("kg"),
        )],
        chain: vec![],
        components: vec![],
    });

    let result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await
        .unwrap();

    // Should find the 70 kg observation
    assert!(!result.resources.is_empty());
}

/// Test quantity search with comparison.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_quantity_search_gt() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();
    seed_observations(&backend, &tenant).await;

    let query = SearchQuery::new("Observation").with_parameter(SearchParameter {
        name: "value-quantity".to_string(),
        param_type: SearchParamType::Quantity,
        modifier: None,
        values: vec![SearchValue::quantity(SearchPrefix::Gt, 100.0, None, None)],
        chain: vec![],
        components: vec![],
    });

    let result = backend
        .search(&tenant, &query, Pagination::new(100))
        .await
        .unwrap();

    // Should find observations with value > 100 (154 lb, 175 cm)
    for resource in &result.resources {
        if let Some(value) = resource.content()["valueQuantity"]["value"].as_f64() {
            assert!(value > 100.0);
        }
    }
}
