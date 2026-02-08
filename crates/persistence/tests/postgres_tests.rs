//! PostgreSQL backend integration tests.
//!
//! These tests verify the PostgreSQL backend implementation.
//! Tests that require a running PostgreSQL instance use testcontainers
//! to spin up real PostgreSQL instances in Docker.
//!
//! Run with: `cargo test -p helios-persistence --features postgres -- postgres`

#![cfg(feature = "postgres")]

use helios_persistence::backends::postgres::PostgresConfig;
use helios_persistence::core::{BackendCapability, BackendKind};

// ============================================================================
// Backend Configuration Tests (no PostgreSQL instance required)
// ============================================================================

#[test]
fn test_postgres_config_defaults() {
    let config = PostgresConfig::default();
    assert_eq!(config.host, "localhost");
    assert_eq!(config.port, 5432);
    assert_eq!(config.dbname, "helios");
    assert_eq!(config.user, "helios");
    assert!(config.password.is_none());
    assert_eq!(config.max_connections, 10);
    assert_eq!(config.connect_timeout_secs, 5);
    assert_eq!(config.statement_timeout_ms, 30000);
    assert!(!config.search_offloaded);
    assert!(config.schema_name.is_none());
}

#[test]
fn test_postgres_config_serialization() {
    let config = PostgresConfig {
        host: "pg-server".to_string(),
        port: 5433,
        dbname: "test_db".to_string(),
        user: "test_user".to_string(),
        password: Some("secret".to_string()),
        ..Default::default()
    };

    let json = serde_json::to_string(&config).unwrap();
    let deserialized: PostgresConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.host, "pg-server");
    assert_eq!(deserialized.port, 5433);
    assert_eq!(deserialized.dbname, "test_db");
    assert_eq!(deserialized.user, "test_user");
    assert_eq!(deserialized.password, Some("secret".to_string()));
}

// ============================================================================
// Backend Capability Tests (no PostgreSQL instance required)
// ============================================================================

// NOTE: These tests verify capability declarations. They cannot construct a
// PostgresBackend without a real database (the constructor connects immediately).
// We verify via config + trait bounds instead.

#[test]
fn test_postgres_config_backend_kind() {
    // Verify BackendKind::Postgres exists and is usable
    let kind = BackendKind::Postgres;
    assert_eq!(format!("{}", kind), "postgres");
}

#[test]
fn test_postgres_expected_capabilities() {
    // Verify the capability enum variants that PostgreSQL should support exist
    let expected = [
        BackendCapability::Crud,
        BackendCapability::Versioning,
        BackendCapability::InstanceHistory,
        BackendCapability::TypeHistory,
        BackendCapability::SystemHistory,
        BackendCapability::BasicSearch,
        BackendCapability::DateSearch,
        BackendCapability::ReferenceSearch,
        BackendCapability::FullTextSearch,
        BackendCapability::Sorting,
        BackendCapability::OffsetPagination,
        BackendCapability::CursorPagination,
        BackendCapability::Transactions,
        BackendCapability::OptimisticLocking,
        BackendCapability::Include,
        BackendCapability::Revinclude,
        BackendCapability::SharedSchema,
        BackendCapability::SchemaPerTenant,
        BackendCapability::DatabasePerTenant,
    ];
    // This is a compile-time check: all variants exist
    assert!(!expected.is_empty());
}

// ============================================================================
// Query Builder Unit Tests (no PostgreSQL instance required)
// ============================================================================

mod query_builder_tests {
    use helios_persistence::backends::postgres::search::query_builder::{
        PostgresQueryBuilder, SqlParam,
    };
    use helios_persistence::types::{
        SearchParamType, SearchParameter, SearchPrefix, SearchQuery, SearchValue,
    };

    #[test]
    fn test_empty_query_returns_none() {
        let query = SearchQuery::new("Patient");
        let result = PostgresQueryBuilder::build_search_query(&query, 2);
        assert!(result.is_none());
    }

    #[test]
    fn test_id_parameter() {
        let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
            name: "_id".to_string(),
            param_type: SearchParamType::Token,
            modifier: None,
            values: vec![SearchValue::eq("123")],
            chain: vec![],
            components: vec![],
        });

        let result = PostgresQueryBuilder::build_search_query(&query, 2);
        assert!(result.is_some());
        let fragment = result.unwrap();
        assert!(fragment.sql.contains("id = $"));
        assert_eq!(fragment.params.len(), 1);
        match &fragment.params[0] {
            SqlParam::Text(s) => assert_eq!(s, "123"),
            _ => panic!("Expected Text param"),
        }
    }

    #[test]
    fn test_string_parameter_default() {
        let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
            name: "name".to_string(),
            param_type: SearchParamType::String,
            modifier: None,
            values: vec![SearchValue::eq("Smith")],
            chain: vec![],
            components: vec![],
        });

        let result = PostgresQueryBuilder::build_search_query(&query, 2);
        assert!(result.is_some());
        let fragment = result.unwrap();
        // Default string search is starts-with (case-insensitive via ILIKE)
        assert!(fragment.sql.contains("ILIKE"));
        assert!(fragment.sql.contains("param_name = 'name'"));
        // Parameter should be "Smith%"
        match &fragment.params[0] {
            SqlParam::Text(s) => assert!(s.ends_with('%')),
            _ => panic!("Expected Text param"),
        }
    }

    #[test]
    fn test_string_parameter_exact() {
        use helios_persistence::types::SearchModifier;

        let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
            name: "name".to_string(),
            param_type: SearchParamType::String,
            modifier: Some(SearchModifier::Exact),
            values: vec![SearchValue::eq("Smith")],
            chain: vec![],
            components: vec![],
        });

        let result = PostgresQueryBuilder::build_search_query(&query, 2);
        assert!(result.is_some());
        let fragment = result.unwrap();
        // Exact match should use = not ILIKE
        assert!(fragment.sql.contains("value_string = $"));
    }

    #[test]
    fn test_string_parameter_contains() {
        use helios_persistence::types::SearchModifier;

        let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
            name: "name".to_string(),
            param_type: SearchParamType::String,
            modifier: Some(SearchModifier::Contains),
            values: vec![SearchValue::eq("mit")],
            chain: vec![],
            components: vec![],
        });

        let result = PostgresQueryBuilder::build_search_query(&query, 2);
        assert!(result.is_some());
        let fragment = result.unwrap();
        assert!(fragment.sql.contains("ILIKE"));
        // Parameter should be "%mit%"
        match &fragment.params[0] {
            SqlParam::Text(s) => {
                assert!(s.starts_with('%'));
                assert!(s.ends_with('%'));
            }
            _ => panic!("Expected Text param"),
        }
    }

    #[test]
    fn test_token_system_and_code() {
        let query = SearchQuery::new("Observation").with_parameter(SearchParameter {
            name: "code".to_string(),
            param_type: SearchParamType::Token,
            modifier: None,
            values: vec![SearchValue::eq("http://loinc.org|8867-4")],
            chain: vec![],
            components: vec![],
        });

        let result = PostgresQueryBuilder::build_search_query(&query, 2);
        assert!(result.is_some());
        let fragment = result.unwrap();
        assert!(fragment.sql.contains("value_token_system"));
        assert!(fragment.sql.contains("value_token_code"));
        assert_eq!(fragment.params.len(), 2);
    }

    #[test]
    fn test_token_code_only() {
        let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
            name: "gender".to_string(),
            param_type: SearchParamType::Token,
            modifier: None,
            values: vec![SearchValue::eq("male")],
            chain: vec![],
            components: vec![],
        });

        let result = PostgresQueryBuilder::build_search_query(&query, 2);
        assert!(result.is_some());
        let fragment = result.unwrap();
        assert!(fragment.sql.contains("value_token_code"));
        assert_eq!(fragment.params.len(), 1);
    }

    #[test]
    fn test_token_system_only() {
        let query = SearchQuery::new("Observation").with_parameter(SearchParameter {
            name: "code".to_string(),
            param_type: SearchParamType::Token,
            modifier: None,
            values: vec![SearchValue::eq("http://loinc.org|")],
            chain: vec![],
            components: vec![],
        });

        let result = PostgresQueryBuilder::build_search_query(&query, 2);
        assert!(result.is_some());
        let fragment = result.unwrap();
        assert!(fragment.sql.contains("value_token_system"));
        assert!(!fragment.sql.contains("value_token_code"));
        assert_eq!(fragment.params.len(), 1);
    }

    #[test]
    fn test_date_parameter() {
        let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
            name: "birthdate".to_string(),
            param_type: SearchParamType::Date,
            modifier: None,
            values: vec![SearchValue::new(SearchPrefix::Gt, "2000-01-01")],
            chain: vec![],
            components: vec![],
        });

        let result = PostgresQueryBuilder::build_search_query(&query, 2);
        assert!(result.is_some());
        let fragment = result.unwrap();
        assert!(fragment.sql.contains("value_date"));
        assert!(fragment.sql.contains("::timestamptz"));
        assert!(fragment.sql.contains("> $"));
    }

    #[test]
    fn test_number_parameter() {
        let query = SearchQuery::new("RiskAssessment").with_parameter(SearchParameter {
            name: "probability".to_string(),
            param_type: SearchParamType::Number,
            modifier: None,
            values: vec![SearchValue::new(SearchPrefix::Ge, "0.5")],
            chain: vec![],
            components: vec![],
        });

        let result = PostgresQueryBuilder::build_search_query(&query, 2);
        assert!(result.is_some());
        let fragment = result.unwrap();
        assert!(fragment.sql.contains("value_number"));
        assert!(fragment.sql.contains(">= $"));
        match &fragment.params[0] {
            SqlParam::Float(f) => assert!((f - 0.5).abs() < f64::EPSILON),
            _ => panic!("Expected Float param"),
        }
    }

    #[test]
    fn test_quantity_parameter() {
        let query = SearchQuery::new("Observation").with_parameter(SearchParameter {
            name: "value-quantity".to_string(),
            param_type: SearchParamType::Quantity,
            modifier: None,
            values: vec![SearchValue::eq("5.4||mg")],
            chain: vec![],
            components: vec![],
        });

        let result = PostgresQueryBuilder::build_search_query(&query, 2);
        assert!(result.is_some());
        let fragment = result.unwrap();
        assert!(fragment.sql.contains("value_quantity_value"));
        assert!(fragment.sql.contains("value_quantity_unit"));
    }

    #[test]
    fn test_reference_parameter() {
        let query = SearchQuery::new("Observation").with_parameter(SearchParameter {
            name: "subject".to_string(),
            param_type: SearchParamType::Reference,
            modifier: None,
            values: vec![SearchValue::eq("Patient/123")],
            chain: vec![],
            components: vec![],
        });

        let result = PostgresQueryBuilder::build_search_query(&query, 2);
        assert!(result.is_some());
        let fragment = result.unwrap();
        assert!(fragment.sql.contains("value_reference"));
    }

    #[test]
    fn test_uri_parameter() {
        let query = SearchQuery::new("ValueSet").with_parameter(SearchParameter {
            name: "url".to_string(),
            param_type: SearchParamType::Uri,
            modifier: None,
            values: vec![SearchValue::eq("http://example.org/fhir/ValueSet/123")],
            chain: vec![],
            components: vec![],
        });

        let result = PostgresQueryBuilder::build_search_query(&query, 2);
        assert!(result.is_some());
        let fragment = result.unwrap();
        assert!(fragment.sql.contains("value_uri"));
    }

    #[test]
    fn test_uri_below_modifier() {
        use helios_persistence::types::SearchModifier;

        let query = SearchQuery::new("ValueSet").with_parameter(SearchParameter {
            name: "url".to_string(),
            param_type: SearchParamType::Uri,
            modifier: Some(SearchModifier::Below),
            values: vec![SearchValue::eq("http://example.org/fhir")],
            chain: vec![],
            components: vec![],
        });

        let result = PostgresQueryBuilder::build_search_query(&query, 2);
        assert!(result.is_some());
        let fragment = result.unwrap();
        assert!(fragment.sql.contains("LIKE"));
    }

    #[test]
    fn test_last_updated_parameter() {
        let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
            name: "_lastUpdated".to_string(),
            param_type: SearchParamType::Date,
            modifier: None,
            values: vec![SearchValue::new(SearchPrefix::Ge, "2024-01-01")],
            chain: vec![],
            components: vec![],
        });

        let result = PostgresQueryBuilder::build_search_query(&query, 2);
        assert!(result.is_some());
        let fragment = result.unwrap();
        assert!(fragment.sql.contains("last_updated"));
        assert!(fragment.sql.contains(">= $"));
    }

    #[test]
    fn test_multiple_values_or() {
        let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
            name: "_id".to_string(),
            param_type: SearchParamType::Token,
            modifier: None,
            values: vec![SearchValue::eq("123"), SearchValue::eq("456")],
            chain: vec![],
            components: vec![],
        });

        let result = PostgresQueryBuilder::build_search_query(&query, 2);
        assert!(result.is_some());
        let fragment = result.unwrap();
        // Multiple _id values should be OR'd
        assert!(fragment.sql.contains("OR"));
        assert_eq!(fragment.params.len(), 2);
    }

    #[test]
    fn test_multiple_parameters_and() {
        let query = SearchQuery::new("Patient")
            .with_parameter(SearchParameter {
                name: "name".to_string(),
                param_type: SearchParamType::String,
                modifier: None,
                values: vec![SearchValue::eq("Smith")],
                chain: vec![],
                components: vec![],
            })
            .with_parameter(SearchParameter {
                name: "gender".to_string(),
                param_type: SearchParamType::Token,
                modifier: None,
                values: vec![SearchValue::eq("male")],
                chain: vec![],
                components: vec![],
            });

        let result = PostgresQueryBuilder::build_search_query(&query, 2);
        assert!(result.is_some());
        let fragment = result.unwrap();
        // Different parameters should be AND'd
        assert!(fragment.sql.contains("AND"));
    }

    #[test]
    fn test_prefix_operators() {
        // Test all prefix-to-operator mappings by using date search
        let prefixes_and_ops = vec![
            (SearchPrefix::Eq, "="),
            (SearchPrefix::Ne, "!="),
            (SearchPrefix::Gt, ">"),
            (SearchPrefix::Lt, "<"),
            (SearchPrefix::Ge, ">="),
            (SearchPrefix::Le, "<="),
        ];

        for (prefix, expected_op) in prefixes_and_ops {
            let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
                name: "_lastUpdated".to_string(),
                param_type: SearchParamType::Date,
                modifier: None,
                values: vec![SearchValue::new(prefix, "2024-01-01")],
                chain: vec![],
                components: vec![],
            });

            let result = PostgresQueryBuilder::build_search_query(&query, 0);
            assert!(result.is_some(), "Failed for prefix {:?}", prefix);
            let fragment = result.unwrap();
            assert!(
                fragment
                    .sql
                    .contains(&format!("last_updated {} $", expected_op)),
                "Expected operator '{}' for prefix {:?}, got SQL: {}",
                expected_op,
                prefix,
                fragment.sql
            );
        }
    }
}

// ============================================================================
// Integration Tests (requires Docker for testcontainers)
// ============================================================================

/// Integration tests that require a real PostgreSQL instance via testcontainers.
///
/// These tests are behind `#[cfg(feature = "postgres")]` and require Docker.
/// They mirror the patterns in sqlite_tests.rs.
///
/// Run with:
///   cargo test -p helios-persistence --features postgres -- postgres_integration
///
/// Skip if no Docker:
///   cargo test -p helios-persistence --features postgres -- --skip postgres_integration
#[cfg(test)]
mod postgres_integration {
    use std::path::PathBuf;

    use helios_fhir::FhirVersion;
    use serde_json::json;

    use helios_persistence::backends::postgres::{PostgresBackend, PostgresConfig};
    use helios_persistence::core::history::{HistoryParams, InstanceHistoryProvider};
    use helios_persistence::core::{Backend, BackendCapability, BackendKind, ResourceStorage};
    use helios_persistence::error::{ResourceError, StorageError};
    use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};

    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::postgres::Postgres;

    /// Creates a PostgresBackend connected to a testcontainers PostgreSQL instance.
    ///
    /// Returns the backend and the container handle (must be kept alive for the
    /// duration of the test).
    async fn create_backend() -> (PostgresBackend, testcontainers::ContainerAsync<Postgres>) {
        let container = Postgres::default()
            .start()
            .await
            .expect("Failed to start PostgreSQL container");

        let host_port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("Failed to get host port");

        let host = container
            .get_host()
            .await
            .expect("Failed to get host")
            .to_string();

        let data_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("data"))
            .unwrap_or_else(|| PathBuf::from("data"));

        let config = PostgresConfig {
            host,
            port: host_port,
            dbname: "postgres".to_string(),
            user: "postgres".to_string(),
            password: Some("postgres".to_string()),
            max_connections: 5,
            data_dir: Some(data_dir),
            ..Default::default()
        };

        let backend = PostgresBackend::new(config)
            .await
            .expect("Failed to create PostgresBackend");

        backend
            .init_schema()
            .await
            .expect("Failed to initialize schema");

        (backend, container)
    }

    fn create_tenant(id: &str) -> TenantContext {
        TenantContext::new(TenantId::new(id), TenantPermissions::full_access())
    }

    // ========================================================================
    // CRUD Tests
    // ========================================================================

    #[tokio::test]
    async fn postgres_integration_create_resource() {
        let (backend, _container) = create_backend().await;
        let tenant = create_tenant("test-tenant");

        let patient = json!({
            "resourceType": "Patient",
            "name": [{"family": "Smith", "given": ["John"]}]
        });

        let result = backend
            .create(&tenant, "Patient", patient, FhirVersion::default())
            .await;
        assert!(result.is_ok(), "Create failed: {:?}", result.err());

        let created = result.unwrap();
        assert_eq!(created.resource_type(), "Patient");
        assert!(!created.id().is_empty());
        assert_eq!(created.version_id(), "1");
    }

    #[tokio::test]
    async fn postgres_integration_create_with_id() {
        let (backend, _container) = create_backend().await;
        let tenant = create_tenant("test-tenant");

        let patient = json!({
            "resourceType": "Patient",
            "id": "patient-123",
            "name": [{"family": "Jones"}]
        });

        let created = backend
            .create(&tenant, "Patient", patient, FhirVersion::default())
            .await
            .unwrap();
        assert_eq!(created.id(), "patient-123");
    }

    #[tokio::test]
    async fn postgres_integration_create_duplicate_fails() {
        let (backend, _container) = create_backend().await;
        let tenant = create_tenant("test-tenant");

        let patient = json!({
            "resourceType": "Patient",
            "id": "duplicate-id"
        });

        backend
            .create(&tenant, "Patient", patient.clone(), FhirVersion::default())
            .await
            .unwrap();

        let result = backend
            .create(&tenant, "Patient", patient, FhirVersion::default())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn postgres_integration_read_resource() {
        let (backend, _container) = create_backend().await;
        let tenant = create_tenant("test-tenant");

        let patient = json!({
            "resourceType": "Patient",
            "name": [{"family": "ReadTest"}]
        });

        let created = backend
            .create(&tenant, "Patient", patient, FhirVersion::default())
            .await
            .unwrap();

        let read = backend
            .read(&tenant, "Patient", created.id())
            .await
            .unwrap();
        assert!(read.is_some());

        let resource = read.unwrap();
        assert_eq!(resource.id(), created.id());
        assert_eq!(resource.content()["name"][0]["family"], "ReadTest");
    }

    #[tokio::test]
    async fn postgres_integration_read_nonexistent() {
        let (backend, _container) = create_backend().await;
        let tenant = create_tenant("test-tenant");

        let read = backend
            .read(&tenant, "Patient", "does-not-exist")
            .await
            .unwrap();
        assert!(read.is_none());
    }

    #[tokio::test]
    async fn postgres_integration_exists() {
        let (backend, _container) = create_backend().await;
        let tenant = create_tenant("test-tenant");

        let patient = json!({"resourceType": "Patient"});
        let created = backend
            .create(&tenant, "Patient", patient, FhirVersion::default())
            .await
            .unwrap();

        assert!(
            backend
                .exists(&tenant, "Patient", created.id())
                .await
                .unwrap()
        );
        assert!(
            !backend
                .exists(&tenant, "Patient", "nonexistent")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn postgres_integration_update_resource() {
        let (backend, _container) = create_backend().await;
        let tenant = create_tenant("test-tenant");

        let patient = json!({
            "resourceType": "Patient",
            "name": [{"family": "Original"}]
        });

        let created = backend
            .create(&tenant, "Patient", patient, FhirVersion::default())
            .await
            .unwrap();

        let updated_content = json!({
            "resourceType": "Patient",
            "name": [{"family": "Updated"}]
        });

        let updated = backend
            .update(&tenant, &created, updated_content)
            .await
            .unwrap();

        assert_eq!(updated.version_id(), "2");
        assert_eq!(updated.content()["name"][0]["family"], "Updated");
    }

    #[tokio::test]
    async fn postgres_integration_create_or_update() {
        let (backend, _container) = create_backend().await;
        let tenant = create_tenant("test-tenant");

        // Create via upsert
        let patient = json!({"resourceType": "Patient", "name": [{"family": "First"}]});
        let (resource, was_created) = backend
            .create_or_update(
                &tenant,
                "Patient",
                "upsert-id",
                patient,
                FhirVersion::default(),
            )
            .await
            .unwrap();

        assert!(was_created);
        assert_eq!(resource.id(), "upsert-id");

        // Update via upsert
        let patient2 = json!({"resourceType": "Patient", "name": [{"family": "Second"}]});
        let (resource2, was_created2) = backend
            .create_or_update(
                &tenant,
                "Patient",
                "upsert-id",
                patient2,
                FhirVersion::default(),
            )
            .await
            .unwrap();

        assert!(!was_created2);
        assert_eq!(resource2.content()["name"][0]["family"], "Second");
    }

    #[tokio::test]
    async fn postgres_integration_delete_resource() {
        let (backend, _container) = create_backend().await;
        let tenant = create_tenant("test-tenant");

        let patient = json!({"resourceType": "Patient"});
        let created = backend
            .create(&tenant, "Patient", patient, FhirVersion::default())
            .await
            .unwrap();

        backend
            .delete(&tenant, "Patient", created.id())
            .await
            .unwrap();

        let read_result = backend.read(&tenant, "Patient", created.id()).await;
        match read_result {
            Err(StorageError::Resource(ResourceError::Gone { .. })) => {}
            Ok(None) => {}
            other => {
                panic!("Expected Gone error or None, got: {:?}", other);
            }
        }
    }

    #[tokio::test]
    async fn postgres_integration_delete_nonexistent_fails() {
        let (backend, _container) = create_backend().await;
        let tenant = create_tenant("test-tenant");

        let result = backend.delete(&tenant, "Patient", "nonexistent").await;
        assert!(result.is_err());
    }

    // ========================================================================
    // Tenant Isolation Tests
    // ========================================================================

    #[tokio::test]
    async fn postgres_integration_tenant_isolation() {
        let (backend, _container) = create_backend().await;
        let tenant_a = create_tenant("tenant-a");
        let tenant_b = create_tenant("tenant-b");

        let patient = json!({"resourceType": "Patient"});
        let created = backend
            .create(&tenant_a, "Patient", patient, FhirVersion::default())
            .await
            .unwrap();

        // Tenant A can see it
        assert!(
            backend
                .exists(&tenant_a, "Patient", created.id())
                .await
                .unwrap()
        );

        // Tenant B cannot see it
        assert!(
            !backend
                .exists(&tenant_b, "Patient", created.id())
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn postgres_integration_same_id_different_tenants() {
        let (backend, _container) = create_backend().await;
        let tenant_a = create_tenant("tenant-a");
        let tenant_b = create_tenant("tenant-b");

        let patient_a = json!({"resourceType": "Patient", "name": [{"family": "A"}]});
        let patient_b = json!({"resourceType": "Patient", "name": [{"family": "B"}]});

        backend
            .create_or_update(
                &tenant_a,
                "Patient",
                "shared-id",
                patient_a,
                FhirVersion::default(),
            )
            .await
            .unwrap();
        backend
            .create_or_update(
                &tenant_b,
                "Patient",
                "shared-id",
                patient_b,
                FhirVersion::default(),
            )
            .await
            .unwrap();

        let read_a = backend
            .read(&tenant_a, "Patient", "shared-id")
            .await
            .unwrap()
            .unwrap();
        let read_b = backend
            .read(&tenant_b, "Patient", "shared-id")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(read_a.content()["name"][0]["family"], "A");
        assert_eq!(read_b.content()["name"][0]["family"], "B");
    }

    // ========================================================================
    // Version Tests
    // ========================================================================

    #[tokio::test]
    async fn postgres_integration_version_increments() {
        let (backend, _container) = create_backend().await;
        let tenant = create_tenant("test-tenant");

        let patient = json!({"resourceType": "Patient"});
        let v1 = backend
            .create(&tenant, "Patient", patient, FhirVersion::default())
            .await
            .unwrap();
        assert_eq!(v1.version_id(), "1");

        let v2 = backend
            .update(&tenant, &v1, json!({"resourceType": "Patient"}))
            .await
            .unwrap();
        assert_eq!(v2.version_id(), "2");

        let v3 = backend
            .update(&tenant, &v2, json!({"resourceType": "Patient"}))
            .await
            .unwrap();
        assert_eq!(v3.version_id(), "3");
    }

    // ========================================================================
    // Count Tests
    // ========================================================================

    #[tokio::test]
    async fn postgres_integration_count_resources() {
        let (backend, _container) = create_backend().await;
        let tenant = create_tenant("test-tenant");

        for i in 0..5 {
            let patient = json!({"resourceType": "Patient", "id": format!("p{}", i)});
            backend
                .create(&tenant, "Patient", patient, FhirVersion::default())
                .await
                .unwrap();
        }

        let count = backend.count(&tenant, Some("Patient")).await.unwrap();
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn postgres_integration_count_by_tenant() {
        let (backend, _container) = create_backend().await;
        let tenant_a = create_tenant("tenant-a");
        let tenant_b = create_tenant("tenant-b");

        for _ in 0..3 {
            let patient = json!({"resourceType": "Patient"});
            backend
                .create(&tenant_a, "Patient", patient, FhirVersion::default())
                .await
                .unwrap();
        }

        for _ in 0..2 {
            let patient = json!({"resourceType": "Patient"});
            backend
                .create(&tenant_b, "Patient", patient, FhirVersion::default())
                .await
                .unwrap();
        }

        assert_eq!(backend.count(&tenant_a, Some("Patient")).await.unwrap(), 3);
        assert_eq!(backend.count(&tenant_b, Some("Patient")).await.unwrap(), 2);
    }

    // ========================================================================
    // Batch Read Tests
    // ========================================================================

    #[tokio::test]
    async fn postgres_integration_read_batch() {
        let (backend, _container) = create_backend().await;
        let tenant = create_tenant("test-tenant");

        let ids: Vec<String> = (0..3).map(|i| format!("batch-{}", i)).collect();
        for id in &ids {
            let patient = json!({"resourceType": "Patient"});
            backend
                .create_or_update(&tenant, "Patient", id, patient, FhirVersion::default())
                .await
                .unwrap();
        }

        let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
        let batch = backend
            .read_batch(&tenant, "Patient", &id_refs)
            .await
            .unwrap();

        assert_eq!(batch.len(), 3);
    }

    // ========================================================================
    // History Tests
    // ========================================================================

    #[tokio::test]
    async fn postgres_integration_instance_history() {
        let (backend, _container) = create_backend().await;
        let tenant = create_tenant("test-tenant");

        let patient = json!({"resourceType": "Patient", "name": [{"family": "V1"}]});
        let v1 = backend
            .create(&tenant, "Patient", patient, FhirVersion::default())
            .await
            .unwrap();

        let v2 = backend
            .update(
                &tenant,
                &v1,
                json!({"resourceType": "Patient", "name": [{"family": "V2"}]}),
            )
            .await
            .unwrap();

        let _v3 = backend
            .update(
                &tenant,
                &v2,
                json!({"resourceType": "Patient", "name": [{"family": "V3"}]}),
            )
            .await
            .unwrap();

        let history = backend
            .history_instance(&tenant, "Patient", v1.id(), &HistoryParams::default())
            .await
            .unwrap();

        assert!(
            history.items.len() >= 3,
            "Expected at least 3 history entries, got {}",
            history.items.len()
        );
    }

    // ========================================================================
    // Content Preservation Tests
    // ========================================================================

    #[tokio::test]
    async fn postgres_integration_content_preserved() {
        let (backend, _container) = create_backend().await;
        let tenant = create_tenant("test-tenant");

        let patient = json!({
            "resourceType": "Patient",
            "name": [{"family": "Smith", "given": ["John", "Jacob"]}],
            "birthDate": "1990-01-15",
            "gender": "male",
            "active": true,
            "identifier": [{
                "system": "http://example.org/mrn",
                "value": "MRN-001"
            }],
            "address": [{
                "city": "Springfield",
                "state": "IL"
            }]
        });

        let created = backend
            .create(&tenant, "Patient", patient, FhirVersion::default())
            .await
            .unwrap();

        let read = backend
            .read(&tenant, "Patient", created.id())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(read.content()["name"][0]["family"], "Smith");
        assert_eq!(read.content()["name"][0]["given"][0], "John");
        assert_eq!(read.content()["name"][0]["given"][1], "Jacob");
        assert_eq!(read.content()["birthDate"], "1990-01-15");
        assert_eq!(read.content()["gender"], "male");
        assert_eq!(read.content()["active"], true);
        assert_eq!(read.content()["identifier"][0]["value"], "MRN-001");
        assert_eq!(read.content()["address"][0]["city"], "Springfield");
    }

    // ========================================================================
    // Search Tests
    // ========================================================================

    #[tokio::test]
    async fn postgres_integration_search_by_name() {
        use helios_persistence::core::SearchProvider;
        use helios_persistence::types::{
            SearchParamType, SearchParameter, SearchQuery, SearchValue,
        };

        let (backend, _container) = create_backend().await;
        let tenant = create_tenant("test-tenant");

        let patient = json!({
            "resourceType": "Patient",
            "id": "p1",
            "name": [{"family": "Smith", "given": ["John"]}]
        });

        backend
            .create(&tenant, "Patient", patient, FhirVersion::default())
            .await
            .unwrap();

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
            !result.resources.items.is_empty(),
            "Search by name should find the patient"
        );
        assert_eq!(result.resources.items[0].id(), "p1");
    }

    #[tokio::test]
    async fn postgres_integration_search_by_token() {
        use helios_persistence::core::SearchProvider;
        use helios_persistence::types::{
            SearchParamType, SearchParameter, SearchQuery, SearchValue,
        };

        let (backend, _container) = create_backend().await;
        let tenant = create_tenant("test-tenant");

        let patient = json!({
            "resourceType": "Patient",
            "id": "p1",
            "gender": "male"
        });

        backend
            .create(&tenant, "Patient", patient, FhirVersion::default())
            .await
            .unwrap();

        let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
            name: "gender".to_string(),
            param_type: SearchParamType::Token,
            modifier: None,
            values: vec![SearchValue::eq("male")],
            chain: vec![],
            components: vec![],
        });

        let result = backend.search(&tenant, &query).await.unwrap();
        assert!(
            !result.resources.items.is_empty(),
            "Search by gender should find the patient"
        );
    }

    // ========================================================================
    // Backend Health Check Tests
    // ========================================================================

    #[tokio::test]
    async fn postgres_integration_health_check() {
        let (backend, _container) = create_backend().await;

        let result = backend.health_check().await;
        assert!(result.is_ok(), "Health check failed: {:?}", result.err());
    }

    #[tokio::test]
    async fn postgres_integration_backend_kind() {
        let (backend, _container) = create_backend().await;

        assert_eq!(backend.kind(), BackendKind::Postgres);
        assert_eq!(backend.name(), "postgres");
    }

    #[tokio::test]
    async fn postgres_integration_capabilities() {
        let (backend, _container) = create_backend().await;

        assert!(backend.supports(BackendCapability::Crud));
        assert!(backend.supports(BackendCapability::Versioning));
        assert!(backend.supports(BackendCapability::InstanceHistory));
        assert!(backend.supports(BackendCapability::BasicSearch));
        assert!(backend.supports(BackendCapability::Transactions));
        assert!(backend.supports(BackendCapability::Include));
        assert!(backend.supports(BackendCapability::Revinclude));
    }
}
