//! REST API test harness.
//!
//! Provides infrastructure for testing the REST API endpoints.

use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum_test::TestServer;
use helios_persistence::core::ResourceStorage;
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use serde_json::Value;

use helios_rest::{create_app_with_config, AppState, ServerConfig};

use super::fixtures::TestFixtures;

/// Test harness for REST API testing.
///
/// Provides a configured test server with a backend ready for testing.
///
/// # Example
///
/// ```rust,ignore
/// use helios_rest_tests::common::harness::RestTestHarness;
///
/// #[tokio::test]
/// async fn test_read() {
///     let harness = RestTestHarness::new_sqlite().await;
///
///     // Seed test data
///     harness.seed_patient("123", "Smith").await;
///
///     // Make request
///     let response = harness.get("/Patient/123").await;
///
///     // Assert
///     assert_eq!(response.status(), 200);
/// }
/// ```
pub struct RestTestHarness<S>
where
    S: ResourceStorage + Send + Sync + 'static,
{
    /// The test server instance.
    pub server: TestServer,

    /// The storage backend.
    pub backend: Arc<S>,

    /// Test fixtures.
    pub fixtures: TestFixtures,

    /// Tenant context for test operations.
    pub tenant: TenantContext,

    /// Saved resources for reference in tests.
    pub saved: HashMap<String, Value>,

    /// Server configuration.
    pub config: ServerConfig,
}

impl<S> RestTestHarness<S>
where
    S: ResourceStorage + Send + Sync + Clone + 'static,
{
    /// Creates a new test harness with the given backend.
    pub async fn new(backend: S) -> Self {
        let config = ServerConfig::for_testing();
        let backend = Arc::new(backend);

        // Create the app with the backend
        // Note: We need to work around the trait bounds here
        // For now, we'll create a minimal test setup
        let state = AppState::new(Arc::clone(&backend), config.clone());

        // Create a minimal router for testing
        let router = Router::new()
            .route("/health", axum::routing::get(|| async { "OK" }))
            .with_state(state);

        let server = TestServer::new(router).expect("Failed to create test server");

        let tenant = TenantContext::new(
            TenantId::new("test-tenant"),
            TenantPermissions::full_access(),
        );

        Self {
            server,
            backend,
            fixtures: TestFixtures::default(),
            tenant,
            saved: HashMap::new(),
            config,
        }
    }

    /// Returns the tenant context.
    pub fn tenant(&self) -> &TenantContext {
        &self.tenant
    }

    /// Seeds a patient resource.
    pub async fn seed_patient(&mut self, id: &str, family: &str) -> &Value {
        let patient = serde_json::json!({
            "resourceType": "Patient",
            "id": id,
            "name": [{ "family": family }]
        });

        self.backend
            .create(&self.tenant, "Patient", patient.clone())
            .await
            .expect("Failed to seed patient");

        self.saved.insert(format!("Patient/{}", id), patient);
        self.saved.get(&format!("Patient/{}", id)).unwrap()
    }

    /// Seeds multiple resources from fixtures.
    pub async fn seed_fixtures(&mut self) {
        for (resource_type, id, resource) in self.fixtures.clone().all_resources() {
            self.backend
                .create(&self.tenant, resource_type, resource.clone())
                .await
                .expect("Failed to seed fixture");
            self.saved.insert(format!("{}/{}", resource_type, id), resource);
        }
    }

    /// Makes a GET request.
    pub async fn get(&self, path: &str) -> axum_test::TestResponse {
        self.server
            .get(path)
            .add_header("X-Tenant-ID".parse().unwrap(), self.tenant.tenant_id().as_str().parse().unwrap())
            .await
    }

    /// Makes a POST request with JSON body.
    pub async fn post(&self, path: &str, body: Value) -> axum_test::TestResponse {
        self.server
            .post(path)
            .add_header("X-Tenant-ID".parse().unwrap(), self.tenant.tenant_id().as_str().parse().unwrap())
            .add_header("Content-Type".parse().unwrap(), "application/fhir+json".parse().unwrap())
            .json(&body)
            .await
    }

    /// Makes a PUT request with JSON body.
    pub async fn put(&self, path: &str, body: Value) -> axum_test::TestResponse {
        self.server
            .put(path)
            .add_header("X-Tenant-ID".parse().unwrap(), self.tenant.tenant_id().as_str().parse().unwrap())
            .add_header("Content-Type".parse().unwrap(), "application/fhir+json".parse().unwrap())
            .json(&body)
            .await
    }

    /// Makes a DELETE request.
    pub async fn delete(&self, path: &str) -> axum_test::TestResponse {
        self.server
            .delete(path)
            .add_header("X-Tenant-ID".parse().unwrap(), self.tenant.tenant_id().as_str().parse().unwrap())
            .await
    }

    /// Makes a PATCH request with JSON body.
    pub async fn patch(&self, path: &str, body: Value) -> axum_test::TestResponse {
        self.server
            .patch(path)
            .add_header("X-Tenant-ID".parse().unwrap(), self.tenant.tenant_id().as_str().parse().unwrap())
            .add_header("Content-Type".parse().unwrap(), "application/json-patch+json".parse().unwrap())
            .json(&body)
            .await
    }

    /// Gets a saved resource.
    pub fn get_saved(&self, key: &str) -> Option<&Value> {
        self.saved.get(key)
    }
}

/// Result type for test operations.
pub type TestResult<T> = Result<T, TestError>;

/// Error type for test operations.
#[derive(Debug)]
pub enum TestError {
    /// HTTP request failed.
    RequestFailed(String),
    /// Assertion failed.
    AssertionFailed(String),
    /// Setup failed.
    SetupFailed(String),
    /// Backend error.
    BackendError(String),
}

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestError::RequestFailed(msg) => write!(f, "Request failed: {}", msg),
            TestError::AssertionFailed(msg) => write!(f, "Assertion failed: {}", msg),
            TestError::SetupFailed(msg) => write!(f, "Setup failed: {}", msg),
            TestError::BackendError(msg) => write!(f, "Backend error: {}", msg),
        }
    }
}

impl std::error::Error for TestError {}
