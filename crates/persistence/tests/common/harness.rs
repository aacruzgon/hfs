//! Test harness infrastructure for backend testing.
//!
//! This module provides the [`TestableBackend`] trait and [`TestContext`] struct
//! for running tests against different storage backends.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use helios_persistence::core::{Backend, BackendCapability, BackendKind, ResourceStorage};
use helios_persistence::error::StorageResult;
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};

use super::fixtures::TestFixtures;

/// Trait that storage backends must implement to be testable.
///
/// This extends the standard [`ResourceStorage`] trait with test-specific
/// operations like reset and seed.
///
/// # Example
///
/// ```ignore
/// impl TestableBackend for SqliteBackend {
///     fn backend_kind(&self) -> BackendKind {
///         BackendKind::Sqlite
///     }
///
///     fn supported_capabilities(&self) -> HashSet<BackendCapability> {
///         self.capabilities().into_iter().collect()
///     }
///
///     async fn reset(&self) -> StorageResult<()> {
///         // Clear all data from the backend
///     }
///
///     async fn seed(&self, fixtures: &TestFixtures) -> StorageResult<()> {
///         // Insert fixture data
///     }
/// }
/// ```
#[async_trait]
pub trait TestableBackend: ResourceStorage + Send + Sync + 'static {
    /// Returns the kind of backend being tested.
    fn backend_kind(&self) -> BackendKind;

    /// Returns the set of capabilities this backend supports.
    fn supported_capabilities(&self) -> HashSet<BackendCapability>;

    /// Resets the backend to a clean state.
    ///
    /// This should clear all data while preserving the schema.
    async fn reset(&self) -> StorageResult<()>;

    /// Seeds the backend with test fixtures.
    ///
    /// # Arguments
    ///
    /// * `fixtures` - The test fixtures to seed
    async fn seed(&self, fixtures: &TestFixtures) -> StorageResult<()>;

    /// Checks if this backend supports the given capability.
    fn supports(&self, capability: BackendCapability) -> bool {
        self.supported_capabilities().contains(&capability)
    }

    /// Checks if this backend supports all the given capabilities.
    fn supports_all(&self, capabilities: &[BackendCapability]) -> bool {
        let supported = self.supported_capabilities();
        capabilities.iter().all(|c| supported.contains(c))
    }
}

/// Context for running backend tests.
///
/// Provides access to the backend, fixtures, and tenant contexts needed
/// for comprehensive testing.
///
/// # Type Parameters
///
/// * `B` - The backend type being tested
pub struct TestContext<B: TestableBackend> {
    /// The backend instance being tested.
    pub backend: Arc<B>,

    /// Test fixtures that have been seeded.
    pub fixtures: TestFixtures,

    /// Primary tenant context for tests.
    pub tenant: TenantContext,

    /// Secondary tenant context for isolation tests.
    pub secondary_tenant: TenantContext,

    /// System tenant context for shared resources.
    pub system_tenant: TenantContext,
}

impl<B: TestableBackend> TestContext<B> {
    /// Creates a new test context with the given backend.
    ///
    /// # Arguments
    ///
    /// * `backend` - The backend instance to test
    pub fn new(backend: B) -> Self {
        Self {
            backend: Arc::new(backend),
            fixtures: TestFixtures::default(),
            tenant: TenantContext::new(
                TenantId::new("test-tenant-1"),
                TenantPermissions::full_access(),
            ),
            secondary_tenant: TenantContext::new(
                TenantId::new("test-tenant-2"),
                TenantPermissions::full_access(),
            ),
            system_tenant: TenantContext::system(),
        }
    }

    /// Creates a new test context with custom fixtures.
    ///
    /// # Arguments
    ///
    /// * `backend` - The backend instance to test
    /// * `fixtures` - Custom fixtures to use
    pub fn with_fixtures(backend: B, fixtures: TestFixtures) -> Self {
        Self {
            backend: Arc::new(backend),
            fixtures,
            tenant: TenantContext::new(
                TenantId::new("test-tenant-1"),
                TenantPermissions::full_access(),
            ),
            secondary_tenant: TenantContext::new(
                TenantId::new("test-tenant-2"),
                TenantPermissions::full_access(),
            ),
            system_tenant: TenantContext::system(),
        }
    }

    /// Resets the backend and re-seeds with fixtures.
    pub async fn reset_and_seed(&self) -> StorageResult<()> {
        self.backend.reset().await?;
        self.backend.seed(&self.fixtures).await
    }

    /// Creates a tenant context with read-only permissions.
    pub fn read_only_tenant(&self) -> TenantContext {
        TenantContext::new(
            self.tenant.tenant_id().clone(),
            TenantPermissions::read_only(),
        )
    }

    /// Creates a tenant context with custom permissions.
    pub fn tenant_with_permissions(&self, permissions: TenantPermissions) -> TenantContext {
        TenantContext::new(self.tenant.tenant_id().clone(), permissions)
    }

    /// Returns a reference to the backend.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Checks if the backend supports a capability.
    pub fn supports(&self, capability: BackendCapability) -> bool {
        self.backend.supports(capability)
    }
}

/// Builder for creating test contexts with custom configuration.
pub struct TestContextBuilder<B: TestableBackend> {
    backend: B,
    fixtures: Option<TestFixtures>,
    tenant_id: Option<String>,
    secondary_tenant_id: Option<String>,
}

impl<B: TestableBackend> TestContextBuilder<B> {
    /// Creates a new builder with the given backend.
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            fixtures: None,
            tenant_id: None,
            secondary_tenant_id: None,
        }
    }

    /// Sets custom fixtures.
    pub fn fixtures(mut self, fixtures: TestFixtures) -> Self {
        self.fixtures = Some(fixtures);
        self
    }

    /// Sets the primary tenant ID.
    pub fn tenant_id(mut self, id: impl Into<String>) -> Self {
        self.tenant_id = Some(id.into());
        self
    }

    /// Sets the secondary tenant ID.
    pub fn secondary_tenant_id(mut self, id: impl Into<String>) -> Self {
        self.secondary_tenant_id = Some(id.into());
        self
    }

    /// Builds the test context.
    pub fn build(self) -> TestContext<B> {
        let tenant_id = self
            .tenant_id
            .unwrap_or_else(|| "test-tenant-1".to_string());
        let secondary_tenant_id = self
            .secondary_tenant_id
            .unwrap_or_else(|| "test-tenant-2".to_string());

        TestContext {
            backend: Arc::new(self.backend),
            fixtures: self.fixtures.unwrap_or_default(),
            tenant: TenantContext::new(TenantId::new(&tenant_id), TenantPermissions::full_access()),
            secondary_tenant: TenantContext::new(
                TenantId::new(&secondary_tenant_id),
                TenantPermissions::full_access(),
            ),
            system_tenant: TenantContext::system(),
        }
    }
}

/// Result type for test skip decisions.
#[derive(Debug, Clone)]
pub enum TestDecision {
    /// Run the test.
    Run,
    /// Skip the test with a reason.
    Skip(String),
    /// Run with partial expectations (some assertions may be relaxed).
    Partial(String),
}

/// Determines whether to run a test based on backend capabilities.
///
/// # Arguments
///
/// * `backend` - The backend being tested
/// * `required` - Capabilities required for the test
/// * `matrix` - The capability matrix for support level lookup
///
/// # Returns
///
/// A [`TestDecision`] indicating whether to run, skip, or run with partial expectations.
pub fn should_run_test<B: TestableBackend>(
    backend: &B,
    required: &[BackendCapability],
    matrix: &super::capabilities::CapabilityMatrix,
) -> TestDecision {
    use super::capabilities::SupportLevel;

    let kind = backend.backend_kind();

    for &cap in required {
        match matrix.support_level(kind, cap) {
            SupportLevel::Implemented => continue,
            SupportLevel::Partial => {
                return TestDecision::Partial(format!(
                    "Capability {:?} is only partially implemented",
                    cap
                ));
            }
            SupportLevel::Planned => {
                return TestDecision::Skip(format!(
                    "Capability {:?} is planned but not yet implemented",
                    cap
                ));
            }
            SupportLevel::NotPlanned => {
                return TestDecision::Skip(format!(
                    "Capability {:?} is not planned for this backend",
                    cap
                ));
            }
            SupportLevel::RequiresExternalService => {
                return TestDecision::Skip(format!(
                    "Capability {:?} requires an external service",
                    cap
                ));
            }
        }
    }

    TestDecision::Run
}

/// Macro to define a test that runs against multiple backends.
///
/// This macro generates test functions for each enabled backend, checking
/// capabilities before running.
///
/// # Example
///
/// ```ignore
/// backend_test!(
///     create_patient_resource,
///     &[BackendCapability::Crud],
///     |ctx: &TestContext<_>| async move {
///         let patient = json!({"resourceType": "Patient", "name": [{"family": "Smith"}]});
///         let result = ctx.backend.create(&ctx.tenant, "Patient", patient).await;
///         assert!(result.is_ok());
///     }
/// );
/// ```
#[macro_export]
macro_rules! backend_test {
    ($test_name:ident, $capabilities:expr, $test_fn:expr) => {
        paste::paste! {
            #[cfg(feature = "sqlite")]
            #[tokio::test]
            async fn [<sqlite_ $test_name>]() {
                use $crate::common::harness::{should_run_test, TestDecision};
                use $crate::common::capabilities::CapabilityMatrix;
                use helios_persistence::backends::sqlite::SqliteBackend;

                let backend = SqliteBackend::in_memory().expect("Failed to create SQLite backend");
                backend.init_schema().expect("Failed to initialize schema");

                let matrix = CapabilityMatrix::default();
                let caps: &[helios_persistence::core::BackendCapability] = $capabilities;

                match should_run_test(&backend, caps, &matrix) {
                    TestDecision::Run => {
                        let ctx = $crate::common::harness::TestContext::new(backend);
                        let test_fn: fn(&$crate::common::harness::TestContext<_>) -> _ = $test_fn;
                        test_fn(&ctx).await;
                    }
                    TestDecision::Skip(reason) => {
                        println!("Skipping test: {}", reason);
                    }
                    TestDecision::Partial(reason) => {
                        println!("Running with partial expectations: {}", reason);
                        let ctx = $crate::common::harness::TestContext::new(backend);
                        let test_fn: fn(&$crate::common::harness::TestContext<_>) -> _ = $test_fn;
                        test_fn(&ctx).await;
                    }
                }
            }
        }
    };
}

/// Macro to skip a test with a message.
#[macro_export]
macro_rules! skip_test {
    ($reason:expr) => {{
        println!("SKIPPED: {}", $reason);
        return;
    }};
}

/// Macro to run a test only if a capability is supported.
#[macro_export]
macro_rules! require_capability {
    ($ctx:expr, $cap:expr) => {
        if !$ctx.supports($cap) {
            $crate::skip_test!(format!("Backend does not support capability: {:?}", $cap));
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_test_context_creation() {
        // This is a compile-time test to verify the API
    }

    #[test]
    fn test_test_decision_variants() {
        let run = TestDecision::Run;
        let skip = TestDecision::Skip("reason".to_string());
        let partial = TestDecision::Partial("reason".to_string());

        match run {
            TestDecision::Run => {}
            _ => panic!("Expected Run"),
        }

        match skip {
            TestDecision::Skip(r) => assert_eq!(r, "reason"),
            _ => panic!("Expected Skip"),
        }

        match partial {
            TestDecision::Partial(r) => assert_eq!(r, "reason"),
            _ => panic!("Expected Partial"),
        }
    }
}
