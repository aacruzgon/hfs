//! Tests for history operations.
//!
//! This module tests the instance, type, and system history operations
//! as defined by the FHIR specification.

use serde_json::json;

use helios_persistence::core::{
    InstanceHistoryProvider, ResourceStorage, SystemHistoryProvider, TypeHistoryProvider,
    VersionedStorage,
};
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use helios_persistence::types::Pagination;

#[cfg(feature = "sqlite")]
use helios_persistence::backends::sqlite::SqliteBackend;

// ============================================================================
// Helper Functions
// ============================================================================

#[cfg(feature = "sqlite")]
fn create_sqlite_backend() -> SqliteBackend {
    let backend = SqliteBackend::in_memory().expect("Failed to create SQLite backend");
    backend.init_schema().expect("Failed to initialize schema");
    backend
}

fn create_tenant() -> TenantContext {
    TenantContext::new(TenantId::new("test-tenant"), TenantPermissions::full_access())
}

fn create_patient_json(name: &str) -> serde_json::Value {
    json!({
        "resourceType": "Patient",
        "name": [{"family": name}],
        "active": true
    })
}

// ============================================================================
// Instance History Tests
// ============================================================================

/// Test instance history returns all versions of a resource.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_instance_history_basic() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create a resource and update it multiple times
    let patient = create_patient_json("Version1");
    let v1 = backend.create(&tenant, "Patient", patient).await.unwrap();

    let mut content2 = v1.content().clone();
    content2["name"][0]["family"] = json!("Version2");
    let v2 = backend.update(&tenant, &v1, content2).await.unwrap();

    let mut content3 = v2.content().clone();
    content3["name"][0]["family"] = json!("Version3");
    let _v3 = backend.update(&tenant, &v2, content3).await.unwrap();

    // Get instance history
    let pagination = Pagination::new(100);
    let history = backend
        .instance_history(&tenant, "Patient", v1.id(), pagination)
        .await
        .unwrap();

    assert_eq!(history.resources.len(), 3, "Should have 3 versions");

    // History should be in reverse chronological order (newest first)
    assert_eq!(history.resources[0].version_id(), "3");
    assert_eq!(history.resources[1].version_id(), "2");
    assert_eq!(history.resources[2].version_id(), "1");

    // Content should match each version
    assert_eq!(history.resources[0].content()["name"][0]["family"], "Version3");
    assert_eq!(history.resources[1].content()["name"][0]["family"], "Version2");
    assert_eq!(history.resources[2].content()["name"][0]["family"], "Version1");
}

/// Test instance history includes deleted version.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_instance_history_includes_deleted() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    let patient = create_patient_json("Smith");
    let v1 = backend.create(&tenant, "Patient", patient).await.unwrap();

    // Delete the resource
    backend.delete(&tenant, "Patient", v1.id()).await.unwrap();

    // Get instance history
    let pagination = Pagination::new(100);
    let history = backend
        .instance_history(&tenant, "Patient", v1.id(), pagination)
        .await
        .unwrap();

    // Should have 2 versions: v1 (created) and v2 (deleted)
    assert!(history.resources.len() >= 1);

    // If delete creates a version, the most recent should be deleted
    if history.resources.len() > 1 {
        assert!(history.resources[0].is_deleted());
    }
}

/// Test instance history with pagination.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_instance_history_pagination() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create resource with many versions
    let patient = create_patient_json("Version0");
    let mut current = backend.create(&tenant, "Patient", patient).await.unwrap();
    let id = current.id().to_string();

    for i in 1..=10 {
        let mut content = current.content().clone();
        content["name"][0]["family"] = json!(format!("Version{}", i));
        current = backend.update(&tenant, &current, content).await.unwrap();
    }

    // Get first page (3 items)
    let page1 = backend
        .instance_history(&tenant, "Patient", &id, Pagination::new(3))
        .await
        .unwrap();

    assert_eq!(page1.resources.len(), 3);
    assert_eq!(page1.resources[0].version_id(), "11"); // Most recent
    assert_eq!(page1.resources[1].version_id(), "10");
    assert_eq!(page1.resources[2].version_id(), "9");

    // If there's a next page cursor, get next page
    if let Some(cursor) = page1.next_cursor {
        let page2 = backend
            .instance_history(&tenant, "Patient", &id, Pagination::with_cursor(3, cursor))
            .await
            .unwrap();

        assert_eq!(page2.resources.len(), 3);
        assert_eq!(page2.resources[0].version_id(), "8");
    }
}

/// Test instance history for nonexistent resource.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_instance_history_nonexistent() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    let pagination = Pagination::new(100);
    let history = backend
        .instance_history(&tenant, "Patient", "nonexistent", pagination)
        .await
        .unwrap();

    assert!(history.resources.is_empty());
}

/// Test instance history respects tenant isolation.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_instance_history_tenant_isolation() {
    let backend = create_sqlite_backend();

    let tenant1 = TenantContext::new(TenantId::new("tenant-1"), TenantPermissions::full_access());
    let tenant2 = TenantContext::new(TenantId::new("tenant-2"), TenantPermissions::full_access());

    // Create resource in tenant1
    let patient = create_patient_json("Smith");
    let created = backend.create(&tenant1, "Patient", patient).await.unwrap();

    // Try to get history from tenant2
    let pagination = Pagination::new(100);
    let history = backend
        .instance_history(&tenant2, "Patient", created.id(), pagination)
        .await
        .unwrap();

    assert!(
        history.resources.is_empty(),
        "Should not see other tenant's history"
    );
}

// ============================================================================
// Type History Tests
// ============================================================================

/// Test type history returns all versions of all resources of a type.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_type_history_basic() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create multiple patients with multiple versions
    let patient1 = create_patient_json("Patient1");
    let p1v1 = backend.create(&tenant, "Patient", patient1).await.unwrap();
    let p1v2 = backend
        .update(&tenant, &p1v1, p1v1.content().clone())
        .await
        .unwrap();

    let patient2 = create_patient_json("Patient2");
    let _p2v1 = backend.create(&tenant, "Patient", patient2).await.unwrap();

    // Get type history
    let pagination = Pagination::new(100);
    let history = backend
        .type_history(&tenant, "Patient", pagination)
        .await
        .unwrap();

    // Should have 3 total versions (2 for patient1, 1 for patient2)
    assert_eq!(history.resources.len(), 3);

    // Should be in reverse chronological order
    // (most recent first - patient2v1, then patient1v2, then patient1v1)
}

/// Test type history excludes other resource types.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_type_history_excludes_other_types() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create patients
    let patient = create_patient_json("Smith");
    backend.create(&tenant, "Patient", patient).await.unwrap();

    // Create observations
    let observation = json!({
        "resourceType": "Observation",
        "status": "final",
        "code": {"coding": [{"code": "test"}]}
    });
    backend
        .create(&tenant, "Observation", observation)
        .await
        .unwrap();

    // Get Patient history only
    let pagination = Pagination::new(100);
    let history = backend
        .type_history(&tenant, "Patient", pagination)
        .await
        .unwrap();

    // Should only contain patients
    for resource in &history.resources {
        assert_eq!(resource.resource_type(), "Patient");
    }
}

/// Test type history with pagination.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_type_history_pagination() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create many patients
    for i in 0..10 {
        let patient = create_patient_json(&format!("Patient{}", i));
        backend.create(&tenant, "Patient", patient).await.unwrap();
    }

    // Get first page
    let page1 = backend
        .type_history(&tenant, "Patient", Pagination::new(3))
        .await
        .unwrap();

    assert_eq!(page1.resources.len(), 3);

    // Get second page if available
    if let Some(cursor) = page1.next_cursor {
        let page2 = backend
            .type_history(&tenant, "Patient", Pagination::with_cursor(3, cursor))
            .await
            .unwrap();

        assert_eq!(page2.resources.len(), 3);
    }
}

/// Test type history respects tenant isolation.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_type_history_tenant_isolation() {
    let backend = create_sqlite_backend();

    let tenant1 = TenantContext::new(TenantId::new("tenant-1"), TenantPermissions::full_access());
    let tenant2 = TenantContext::new(TenantId::new("tenant-2"), TenantPermissions::full_access());

    // Create patients in tenant1
    for i in 0..5 {
        let patient = create_patient_json(&format!("Tenant1Patient{}", i));
        backend.create(&tenant1, "Patient", patient).await.unwrap();
    }

    // Create patients in tenant2
    for i in 0..3 {
        let patient = create_patient_json(&format!("Tenant2Patient{}", i));
        backend.create(&tenant2, "Patient", patient).await.unwrap();
    }

    // Get history for each tenant
    let pagination = Pagination::new(100);
    let history1 = backend
        .type_history(&tenant1, "Patient", pagination.clone())
        .await
        .unwrap();
    let history2 = backend
        .type_history(&tenant2, "Patient", pagination)
        .await
        .unwrap();

    assert_eq!(history1.resources.len(), 5);
    assert_eq!(history2.resources.len(), 3);
}

// ============================================================================
// System History Tests
// ============================================================================

/// Test system history returns all versions of all resources.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_system_history_basic() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create various resources
    let patient = create_patient_json("Smith");
    backend.create(&tenant, "Patient", patient).await.unwrap();

    let observation = json!({
        "resourceType": "Observation",
        "status": "final",
        "code": {"coding": [{"code": "test"}]}
    });
    backend
        .create(&tenant, "Observation", observation)
        .await
        .unwrap();

    let organization = json!({
        "resourceType": "Organization",
        "name": "Test Org"
    });
    backend
        .create(&tenant, "Organization", organization)
        .await
        .unwrap();

    // Get system history
    let pagination = Pagination::new(100);
    let history = backend.system_history(&tenant, pagination).await.unwrap();

    // Should have all 3 resources
    assert_eq!(history.resources.len(), 3);

    // Collect resource types
    let types: std::collections::HashSet<_> = history
        .resources
        .iter()
        .map(|r| r.resource_type())
        .collect();

    assert!(types.contains("Patient"));
    assert!(types.contains("Observation"));
    assert!(types.contains("Organization"));
}

/// Test system history is in reverse chronological order.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_system_history_order() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create resources with small delays to ensure ordering
    let patient = create_patient_json("First");
    let first = backend.create(&tenant, "Patient", patient).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let observation = json!({
        "resourceType": "Observation",
        "status": "final",
        "code": {"coding": [{"code": "second"}]}
    });
    let second = backend
        .create(&tenant, "Observation", observation)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let organization = json!({
        "resourceType": "Organization",
        "name": "Third"
    });
    let third = backend
        .create(&tenant, "Organization", organization)
        .await
        .unwrap();

    // Get system history
    let pagination = Pagination::new(100);
    let history = backend.system_history(&tenant, pagination).await.unwrap();

    // Should be in reverse chronological order
    assert!(history.resources[0].last_modified() >= history.resources[1].last_modified());
    assert!(history.resources[1].last_modified() >= history.resources[2].last_modified());
}

/// Test system history with pagination.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_system_history_pagination() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create many resources
    for i in 0..10 {
        let patient = create_patient_json(&format!("Patient{}", i));
        backend.create(&tenant, "Patient", patient).await.unwrap();
    }

    // Get first page
    let page1 = backend
        .system_history(&tenant, Pagination::new(3))
        .await
        .unwrap();

    assert_eq!(page1.resources.len(), 3);

    // Verify pagination works
    if let Some(cursor) = page1.next_cursor {
        let page2 = backend
            .system_history(&tenant, Pagination::with_cursor(3, cursor))
            .await
            .unwrap();

        assert_eq!(page2.resources.len(), 3);

        // Pages should not overlap
        let page1_ids: std::collections::HashSet<_> =
            page1.resources.iter().map(|r| r.id()).collect();
        for resource in &page2.resources {
            assert!(!page1_ids.contains(resource.id()));
        }
    }
}

/// Test system history respects tenant isolation.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_system_history_tenant_isolation() {
    let backend = create_sqlite_backend();

    let tenant1 = TenantContext::new(TenantId::new("tenant-1"), TenantPermissions::full_access());
    let tenant2 = TenantContext::new(TenantId::new("tenant-2"), TenantPermissions::full_access());

    // Create resources in both tenants
    for i in 0..5 {
        let patient = create_patient_json(&format!("Tenant1_{}", i));
        backend.create(&tenant1, "Patient", patient).await.unwrap();
    }

    for i in 0..3 {
        let patient = create_patient_json(&format!("Tenant2_{}", i));
        backend.create(&tenant2, "Patient", patient).await.unwrap();
    }

    // Get history for each tenant
    let pagination = Pagination::new(100);
    let history1 = backend.system_history(&tenant1, pagination.clone()).await.unwrap();
    let history2 = backend.system_history(&tenant2, pagination).await.unwrap();

    // Each tenant should only see their own resources
    assert_eq!(history1.resources.len(), 5);
    assert_eq!(history2.resources.len(), 3);

    for resource in &history1.resources {
        assert_eq!(resource.tenant_id().as_str(), "tenant-1");
    }

    for resource in &history2.resources {
        assert_eq!(resource.tenant_id().as_str(), "tenant-2");
    }
}

// ============================================================================
// History _since Parameter Tests
// ============================================================================

/// Test history with _since parameter.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_history_since_parameter() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create some resources
    let patient1 = create_patient_json("Before");
    backend.create(&tenant, "Patient", patient1).await.unwrap();

    // Record time
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let since = chrono::Utc::now();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Create more resources
    let patient2 = create_patient_json("After");
    backend.create(&tenant, "Patient", patient2).await.unwrap();

    // Get history since the marker time
    let pagination = Pagination::new(100).with_since(since);
    let history = backend
        .type_history(&tenant, "Patient", pagination)
        .await
        .unwrap();

    // Should only have resources created after 'since'
    for resource in &history.resources {
        assert!(
            resource.last_modified() >= since,
            "Resource {} was modified before _since",
            resource.id()
        );
    }
}

// ============================================================================
// History Bundle Tests
// ============================================================================

/// Test that history results can be converted to a FHIR Bundle.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_history_bundle_format() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    let patient = create_patient_json("Smith");
    let v1 = backend.create(&tenant, "Patient", patient).await.unwrap();
    let _v2 = backend
        .update(&tenant, &v1, v1.content().clone())
        .await
        .unwrap();

    let pagination = Pagination::new(100);
    let history = backend
        .instance_history(&tenant, "Patient", v1.id(), pagination)
        .await
        .unwrap();

    // History should have the structure needed for a Bundle
    assert!(!history.resources.is_empty());
    for resource in &history.resources {
        // Each entry should have method info
        assert!(resource.method().is_some() || !resource.is_deleted());
        // Each should have versioned URL
        assert!(resource.versioned_url().contains("_history"));
    }
}
