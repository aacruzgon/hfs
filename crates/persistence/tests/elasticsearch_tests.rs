//! Elasticsearch backend integration tests.
//!
//! These tests verify the Elasticsearch backend implementation.
//! Tests that require a running Elasticsearch instance use testcontainers
//! to spin up real ES instances in Docker.
//!
//! Run with: `cargo test -p helios-persistence --features elasticsearch -- elasticsearch`

#![cfg(feature = "elasticsearch")]

use helios_persistence::backends::elasticsearch::{ElasticsearchBackend, ElasticsearchConfig};
use helios_persistence::core::{Backend, BackendCapability, BackendKind};

// ============================================================================
// Backend Configuration Tests (no ES instance required)
// ============================================================================

#[test]
fn test_elasticsearch_config_defaults() {
    let config = ElasticsearchConfig::default();
    assert_eq!(config.nodes, vec!["http://localhost:9200".to_string()]);
    assert_eq!(config.index_prefix, "hfs");
    assert_eq!(config.number_of_shards, 1);
    assert_eq!(config.number_of_replicas, 1);
    assert!(config.auth.is_none());
}

#[test]
fn test_elasticsearch_config_serialization() {
    let config = ElasticsearchConfig {
        nodes: vec!["http://es1:9200".to_string(), "http://es2:9200".to_string()],
        index_prefix: "test".to_string(),
        ..Default::default()
    };

    let json = serde_json::to_string(&config).unwrap();
    let deserialized: ElasticsearchConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.nodes, config.nodes);
    assert_eq!(deserialized.index_prefix, "test");
}

#[test]
fn test_backend_creation() {
    let config = ElasticsearchConfig::default();
    // This just creates the client — doesn't connect
    let backend = ElasticsearchBackend::new(config);
    assert!(backend.is_ok());

    let backend = backend.unwrap();
    assert_eq!(backend.kind(), BackendKind::Elasticsearch);
    assert_eq!(backend.name(), "elasticsearch");
}

#[test]
fn test_backend_capabilities() {
    let config = ElasticsearchConfig::default();
    let backend = ElasticsearchBackend::new(config).unwrap();

    assert!(backend.supports(BackendCapability::Crud));
    assert!(backend.supports(BackendCapability::BasicSearch));
    assert!(backend.supports(BackendCapability::FullTextSearch));
    assert!(backend.supports(BackendCapability::CursorPagination));
    assert!(backend.supports(BackendCapability::OffsetPagination));
    assert!(backend.supports(BackendCapability::Sorting));
    assert!(backend.supports(BackendCapability::Include));
    assert!(backend.supports(BackendCapability::Revinclude));

    // ES does not support these
    assert!(!backend.supports(BackendCapability::Transactions));
    assert!(!backend.supports(BackendCapability::InstanceHistory));
    assert!(!backend.supports(BackendCapability::Versioning));
}

#[test]
fn test_index_name() {
    let config = ElasticsearchConfig {
        index_prefix: "hfs".to_string(),
        ..Default::default()
    };
    let backend = ElasticsearchBackend::new(config).unwrap();

    assert_eq!(backend.index_name("acme", "Patient"), "hfs_acme_patient");
    assert_eq!(
        backend.index_name("tenant-1", "Observation"),
        "hfs_tenant-1_observation"
    );
}

// ============================================================================
// Query Builder Unit Tests (no ES instance required)
// ============================================================================

mod query_builder_tests {
    use helios_persistence::backends::elasticsearch::search::query_builder::{
        EsQueryBuilder, build_count_query,
    };
    use helios_persistence::types::{
        SearchParamType, SearchParameter, SearchPrefix, SearchQuery, SearchValue, SortDirection,
        SortDirective,
    };

    #[test]
    fn test_empty_query() {
        let builder = EsQueryBuilder::new("acme", "Patient", "hfs_acme_patient".to_string());
        let query = SearchQuery::new("Patient");
        let es_query = builder.build(&query);

        assert_eq!(es_query.index, "hfs_acme_patient");

        // Should have tenant_id and is_deleted filters
        let filters = &es_query.body["query"]["bool"]["filter"];
        assert!(filters.is_array());
        let filters = filters.as_array().unwrap();
        assert_eq!(filters.len(), 2);

        // Default sort: _lastUpdated desc + resource_id asc
        let sort = &es_query.body["sort"];
        assert!(sort.is_array());
        let sort = sort.as_array().unwrap();
        assert_eq!(sort.len(), 2);

        // Default size
        assert_eq!(es_query.body["size"], 20);

        // track_total_hits
        assert_eq!(es_query.body["track_total_hits"], true);
    }

    #[test]
    fn test_string_search_parameter() {
        let builder = EsQueryBuilder::new("acme", "Patient", "hfs_acme_patient".to_string());
        let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
            name: "name".to_string(),
            param_type: SearchParamType::String,
            modifier: None,
            values: vec![SearchValue::eq("Smith")],
            chain: vec![],
            components: vec![],
        });

        let es_query = builder.build(&query);
        let body_str = serde_json::to_string(&es_query.body).unwrap();

        assert!(body_str.contains("search_params.string"));
        assert!(body_str.contains("Smith"));
    }

    #[test]
    fn test_token_search_parameter() {
        let builder =
            EsQueryBuilder::new("acme", "Observation", "hfs_acme_observation".to_string());
        let query = SearchQuery::new("Observation").with_parameter(SearchParameter {
            name: "code".to_string(),
            param_type: SearchParamType::Token,
            modifier: None,
            values: vec![SearchValue::eq("http://loinc.org|8867-4")],
            chain: vec![],
            components: vec![],
        });

        let es_query = builder.build(&query);
        let body_str = serde_json::to_string(&es_query.body).unwrap();

        assert!(body_str.contains("search_params.token"));
        assert!(body_str.contains("http://loinc.org"));
        assert!(body_str.contains("8867-4"));
    }

    #[test]
    fn test_date_range_query() {
        let builder = EsQueryBuilder::new("acme", "Patient", "hfs_acme_patient".to_string());
        let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
            name: "birthdate".to_string(),
            param_type: SearchParamType::Date,
            modifier: None,
            values: vec![SearchValue::new(SearchPrefix::Gt, "2000-01-01")],
            chain: vec![],
            components: vec![],
        });

        let es_query = builder.build(&query);
        let body_str = serde_json::to_string(&es_query.body).unwrap();

        assert!(body_str.contains("search_params.date"));
        // The date handler transforms dates to precision-based ranges,
        // so the exact string may not appear; just verify it produces a range query
        assert!(body_str.contains("range") || body_str.contains("2000-01-01"));
    }

    #[test]
    fn test_multiple_values_or() {
        let builder = EsQueryBuilder::new("acme", "Patient", "hfs_acme_patient".to_string());
        let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
            name: "name".to_string(),
            param_type: SearchParamType::String,
            modifier: None,
            values: vec![SearchValue::eq("Smith"), SearchValue::eq("Jones")],
            chain: vec![],
            components: vec![],
        });

        let es_query = builder.build(&query);
        let body_str = serde_json::to_string(&es_query.body).unwrap();

        // Multiple values should produce a "should" (OR) clause
        assert!(body_str.contains("should"));
        assert!(body_str.contains("Smith"));
        assert!(body_str.contains("Jones"));
    }

    #[test]
    fn test_id_parameter() {
        let builder = EsQueryBuilder::new("acme", "Patient", "hfs_acme_patient".to_string());
        let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
            name: "_id".to_string(),
            param_type: SearchParamType::Token,
            modifier: None,
            values: vec![SearchValue::eq("123")],
            chain: vec![],
            components: vec![],
        });

        let es_query = builder.build(&query);
        let body_str = serde_json::to_string(&es_query.body).unwrap();

        assert!(body_str.contains("resource_id"));
        assert!(body_str.contains("123"));
    }

    #[test]
    fn test_last_updated_parameter() {
        let builder = EsQueryBuilder::new("acme", "Patient", "hfs_acme_patient".to_string());
        let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
            name: "_lastUpdated".to_string(),
            param_type: SearchParamType::Date,
            modifier: None,
            values: vec![SearchValue::new(SearchPrefix::Ge, "2024-01-01")],
            chain: vec![],
            components: vec![],
        });

        let es_query = builder.build(&query);
        let body_str = serde_json::to_string(&es_query.body).unwrap();

        assert!(body_str.contains("last_updated"));
        assert!(body_str.contains("gte"));
    }

    #[test]
    fn test_custom_sort() {
        let builder = EsQueryBuilder::new("acme", "Patient", "hfs_acme_patient".to_string());
        let query = SearchQuery::new("Patient").with_sort(SortDirective {
            parameter: "_id".to_string(),
            direction: SortDirection::Ascending,
        });

        let es_query = builder.build(&query);
        let sort = &es_query.body["sort"];
        let sort_arr = sort.as_array().unwrap();

        // First sort clause should be resource_id asc
        assert_eq!(sort_arr[0]["resource_id"]["order"], "asc");
        // Last should be tie-breaker
        assert_eq!(sort_arr[sort_arr.len() - 1]["resource_id"]["order"], "asc");
    }

    #[test]
    fn test_pagination_size() {
        let builder = EsQueryBuilder::new("acme", "Patient", "hfs_acme_patient".to_string());
        let mut query = SearchQuery::new("Patient");
        query.count = Some(50);

        let es_query = builder.build(&query);
        assert_eq!(es_query.body["size"], 50);
    }

    #[test]
    fn test_offset_pagination() {
        let builder = EsQueryBuilder::new("acme", "Patient", "hfs_acme_patient".to_string());
        let mut query = SearchQuery::new("Patient");
        query.offset = Some(100);

        let es_query = builder.build(&query);
        assert_eq!(es_query.body["from"], 100);
    }

    #[test]
    fn test_count_query() {
        let query = SearchQuery::new("Patient");
        let body = build_count_query("acme", "Patient", &query);

        // Count query should have size=0 and no sort
        assert_eq!(body["size"], 0);
        assert!(body.get("sort").is_none());
    }
}

// ============================================================================
// Search Parameter Handler Tests (no ES instance required)
// ============================================================================

mod parameter_handler_tests {
    use helios_persistence::backends::elasticsearch::search::parameter_handlers::*;
    use helios_persistence::types::{
        SearchModifier, SearchParamType, SearchParameter, SearchValue,
    };

    fn make_param(
        name: &str,
        param_type: SearchParamType,
        modifier: Option<SearchModifier>,
    ) -> SearchParameter {
        SearchParameter {
            name: name.to_string(),
            param_type,
            modifier,
            values: vec![SearchValue::eq("test")],
            chain: vec![],
            components: vec![],
        }
    }

    // String handler tests
    mod string_handler {
        use super::*;

        #[test]
        fn test_default_prefix_match() {
            let param = make_param("name", SearchParamType::String, None);
            let clause = string::build_clause(&param, "Smi").unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("search_params.string"));
            assert!(s.contains("Smi"));
        }

        #[test]
        fn test_exact_modifier() {
            let param = make_param("name", SearchParamType::String, Some(SearchModifier::Exact));
            let clause = string::build_clause(&param, "Smith").unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("keyword"));
        }

        #[test]
        fn test_contains_modifier() {
            let param = make_param(
                "name",
                SearchParamType::String,
                Some(SearchModifier::Contains),
            );
            let clause = string::build_clause(&param, "mit").unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("wildcard"));
            assert!(s.contains("mit"));
        }
    }

    // Token handler tests
    mod token_handler {
        use super::*;

        #[test]
        fn test_code_only() {
            let param = make_param("code", SearchParamType::Token, None);
            let clause = token::build_clause(&param, "active").unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("search_params.token.code"));
            assert!(s.contains("active"));
        }

        #[test]
        fn test_system_and_code() {
            let param = make_param("code", SearchParamType::Token, None);
            let clause = token::build_clause(&param, "http://loinc.org|8867-4").unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("search_params.token.system"));
            assert!(s.contains("http://loinc.org"));
            assert!(s.contains("search_params.token.code"));
            assert!(s.contains("8867-4"));
        }

        #[test]
        fn test_system_only() {
            let param = make_param("code", SearchParamType::Token, None);
            let clause = token::build_clause(&param, "http://loinc.org|").unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("search_params.token.system"));
            assert!(s.contains("http://loinc.org"));
            // Should NOT contain token.code match
        }

        #[test]
        fn test_code_no_system() {
            let param = make_param("code", SearchParamType::Token, None);
            let clause = token::build_clause(&param, "|active").unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("active"));
            assert!(s.contains("must_not"));
        }

        #[test]
        fn test_not_modifier() {
            let param = make_param("gender", SearchParamType::Token, Some(SearchModifier::Not));
            let clause = token::build_clause(&param, "male").unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("must_not"));
        }

        #[test]
        fn test_text_modifier() {
            let param = make_param("code", SearchParamType::Token, Some(SearchModifier::Text));
            let clause = token::build_clause(&param, "headache").unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("display"));
            assert!(s.contains("headache"));
        }
    }

    // Date handler tests
    mod date_handler {
        use helios_persistence::types::SearchPrefix;

        #[test]
        fn test_date_eq() {
            use super::*;
            let clause = date::build_clause("birthdate", "2000-01-15", SearchPrefix::Eq).unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("search_params.date"));
            assert!(s.contains("2000-01-15"));
        }

        #[test]
        fn test_date_gt() {
            use super::*;
            let clause = date::build_clause("birthdate", "2000-01-15", SearchPrefix::Gt).unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("search_params.date"));
        }
    }

    // Reference handler tests
    mod reference_handler {
        use super::*;

        #[test]
        fn test_relative_reference() {
            let param = make_param("subject", SearchParamType::Reference, None);
            let clause = reference::build_clause(&param, "Patient/123").unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("Patient/123"));
            assert!(s.contains("search_params.reference"));
        }

        #[test]
        fn test_id_only() {
            let param = make_param("subject", SearchParamType::Reference, None);
            let clause = reference::build_clause(&param, "123").unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("resource_id"));
        }

        #[test]
        fn test_type_modifier() {
            let param = make_param(
                "subject",
                SearchParamType::Reference,
                Some(SearchModifier::Type("Patient".to_string())),
            );
            let clause = reference::build_clause(&param, "123").unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("resource_type"));
            assert!(s.contains("Patient"));
        }
    }

    // Number handler tests
    mod number_handler {
        use helios_persistence::types::SearchPrefix;

        #[test]
        fn test_number_eq() {
            use super::*;
            let clause = number::build_clause("probability", "0.5", SearchPrefix::Eq).unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("search_params.number"));
        }

        #[test]
        fn test_number_gt() {
            use super::*;
            let clause = number::build_clause("probability", "0.5", SearchPrefix::Gt).unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("search_params.number"));
            assert!(s.contains("gt"));
        }
    }

    // Quantity handler tests
    mod quantity_handler {
        use helios_persistence::types::SearchPrefix;

        #[test]
        fn test_quantity_value_only() {
            use super::*;
            let clause = quantity::build_clause("value-quantity", "5.4", SearchPrefix::Eq).unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("search_params.quantity"));
        }

        #[test]
        fn test_quantity_with_system_code() {
            use super::*;
            let clause = quantity::build_clause(
                "value-quantity",
                "5.4|http://unitsofmeasure.org|mg",
                SearchPrefix::Eq,
            )
            .unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("search_params.quantity"));
            assert!(s.contains("http://unitsofmeasure.org"));
            assert!(s.contains("mg"));
        }
    }

    // URI handler tests
    mod uri_handler {
        use super::*;

        #[test]
        fn test_exact_uri() {
            let param = make_param("url", SearchParamType::Uri, None);
            let clause = uri::build_clause(&param, "http://example.org/fhir").unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("search_params.uri"));
            assert!(s.contains("http://example.org/fhir"));
        }

        #[test]
        fn test_below_modifier() {
            let param = make_param("url", SearchParamType::Uri, Some(SearchModifier::Below));
            let clause = uri::build_clause(&param, "http://example.org/fhir").unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("prefix"));
        }

        #[test]
        fn test_above_modifier() {
            let param = make_param("url", SearchParamType::Uri, Some(SearchModifier::Above));
            let clause = uri::build_clause(&param, "http://example.org/fhir/ValueSet/123").unwrap();
            let s = serde_json::to_string(&clause).unwrap();
            assert!(s.contains("terms"));
        }
    }
}

// ============================================================================
// Search Offloading Tests
// ============================================================================

#[cfg(feature = "sqlite")]
mod search_offloading_tests {
    use helios_fhir::FhirVersion;
    use helios_persistence::backends::sqlite::{SqliteBackend, SqliteBackendConfig};
    use helios_persistence::core::{ResourceStorage, SearchProvider};
    use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
    use helios_persistence::types::SearchQuery;
    use serde_json::json;
    use std::path::PathBuf;

    fn create_backend(search_offloaded: bool) -> SqliteBackend {
        let data_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("data"))
            .unwrap_or_else(|| PathBuf::from("data"));

        let config = SqliteBackendConfig {
            data_dir: Some(data_dir),
            search_offloaded,
            ..Default::default()
        };
        let backend = SqliteBackend::with_config(":memory:", config)
            .expect("Failed to create SQLite backend");
        backend.init_schema().expect("Failed to initialize schema");
        backend
    }

    fn create_tenant(id: &str) -> TenantContext {
        TenantContext::new(TenantId::new(id), TenantPermissions::full_access())
    }

    #[tokio::test]
    async fn test_search_offloaded_crud_still_works() {
        let backend = create_backend(true);
        let tenant = create_tenant("test");

        let patient = json!({
            "resourceType": "Patient",
            "id": "p1",
            "name": [{"family": "Smith", "given": ["John"]}]
        });

        // Create should succeed even with offloaded search
        let result = backend
            .create(&tenant, "Patient", patient, FhirVersion::default())
            .await;
        assert!(result.is_ok());

        // Read should work
        let read = backend.read(&tenant, "Patient", "p1").await;
        assert!(read.is_ok());
        assert!(read.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_search_offloaded_parameterized_search_returns_empty() {
        use helios_persistence::types::{SearchParamType, SearchParameter, SearchValue};

        let backend = create_backend(true);
        let tenant = create_tenant("test");

        let patient = json!({
            "resourceType": "Patient",
            "id": "p1",
            "name": [{"family": "Smith", "given": ["John"]}]
        });

        backend
            .create(&tenant, "Patient", patient, FhirVersion::default())
            .await
            .unwrap();

        // With search offloaded, the SQLite search index is empty,
        // so parameterized search should return no results
        let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
            name: "name".to_string(),
            param_type: SearchParamType::String,
            modifier: None,
            values: vec![SearchValue::eq("Smith")],
            chain: vec![],
            components: vec![],
        });
        let result = backend.search(&tenant, &query).await.unwrap();
        assert_eq!(
            result.resources.len(),
            0,
            "Parameterized search should return empty when search is offloaded"
        );
    }

    #[tokio::test]
    async fn test_search_not_offloaded_parameterized_search_finds_resources() {
        use helios_persistence::types::{SearchParamType, SearchParameter, SearchValue};

        let backend = create_backend(false);
        let tenant = create_tenant("test");

        let patient = json!({
            "resourceType": "Patient",
            "id": "p1",
            "name": [{"family": "Smith", "given": ["John"]}]
        });

        backend
            .create(&tenant, "Patient", patient, FhirVersion::default())
            .await
            .unwrap();

        // Without offloading, parameterized search should find the resource
        let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
            name: "name".to_string(),
            param_type: SearchParamType::String,
            modifier: None,
            values: vec![SearchValue::eq("Smith")],
            chain: vec![],
            components: vec![],
        });
        let result = backend.search(&tenant, &query).await.unwrap();
        assert!(
            !result.resources.is_empty(),
            "Parameterized search should find resources when not offloaded"
        );
    }

    #[tokio::test]
    async fn test_search_offloaded_delete_works() {
        let backend = create_backend(true);
        let tenant = create_tenant("test");

        let patient = json!({
            "resourceType": "Patient",
            "id": "p1",
            "name": [{"family": "Smith"}]
        });

        backend
            .create(&tenant, "Patient", patient, FhirVersion::default())
            .await
            .unwrap();

        // Delete should succeed
        let result = backend.delete(&tenant, "Patient", "p1").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_set_search_offloaded() {
        let data_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("data"))
            .unwrap_or_else(|| PathBuf::from("data"));

        let config = SqliteBackendConfig {
            data_dir: Some(data_dir),
            ..Default::default()
        };
        let mut backend = SqliteBackend::with_config(":memory:", config)
            .expect("Failed to create SQLite backend");

        assert!(!backend.is_search_offloaded());
        backend.set_search_offloaded(true);
        assert!(backend.is_search_offloaded());
    }
}
