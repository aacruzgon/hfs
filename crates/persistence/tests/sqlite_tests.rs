//! SQLite backend integration tests.
//!
//! These tests verify the SQLite backend implementation against the actual API.

use serde_json::json;

use helios_persistence::backends::sqlite::SqliteBackend;
use helios_persistence::core::ResourceStorage;
use helios_persistence::core::history::{
    HistoryMethod, HistoryParams, InstanceHistoryProvider, SystemHistoryProvider,
    TypeHistoryProvider,
};
use helios_persistence::error::{ResourceError, StorageError};
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};

fn create_backend() -> SqliteBackend {
    let backend = SqliteBackend::in_memory().expect("Failed to create SQLite backend");
    backend.init_schema().expect("Failed to initialize schema");
    backend
}

fn create_tenant(id: &str) -> TenantContext {
    TenantContext::new(TenantId::new(id), TenantPermissions::full_access())
}

// ============================================================================
// Create Tests
// ============================================================================

#[tokio::test]
async fn test_create_resource() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    let patient = json!({
        "resourceType": "Patient",
        "name": [{"family": "Smith", "given": ["John"]}]
    });

    let result = backend.create(&tenant, "Patient", patient).await;
    assert!(result.is_ok());

    let created = result.unwrap();
    assert_eq!(created.resource_type(), "Patient");
    assert!(!created.id().is_empty());
    assert_eq!(created.version_id(), "1");
}

#[tokio::test]
async fn test_create_with_id() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    let patient = json!({
        "resourceType": "Patient",
        "id": "patient-123",
        "name": [{"family": "Jones"}]
    });

    let created = backend.create(&tenant, "Patient", patient).await.unwrap();
    assert_eq!(created.id(), "patient-123");
}

#[tokio::test]
async fn test_create_duplicate_fails() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    let patient = json!({
        "resourceType": "Patient",
        "id": "duplicate-id"
    });

    // First create succeeds
    backend
        .create(&tenant, "Patient", patient.clone())
        .await
        .unwrap();

    // Second create with same ID fails
    let result = backend.create(&tenant, "Patient", patient).await;
    assert!(result.is_err());
}

// ============================================================================
// Read Tests
// ============================================================================

#[tokio::test]
async fn test_read_resource() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    let patient = json!({
        "resourceType": "Patient",
        "name": [{"family": "ReadTest"}]
    });

    let created = backend.create(&tenant, "Patient", patient).await.unwrap();

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
async fn test_read_nonexistent() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    let read = backend
        .read(&tenant, "Patient", "does-not-exist")
        .await
        .unwrap();
    assert!(read.is_none());
}

#[tokio::test]
async fn test_exists() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    let patient = json!({"resourceType": "Patient"});
    let created = backend.create(&tenant, "Patient", patient).await.unwrap();

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

// ============================================================================
// Update Tests
// ============================================================================

#[tokio::test]
async fn test_update_resource() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    let patient = json!({
        "resourceType": "Patient",
        "name": [{"family": "Original"}]
    });

    let created = backend.create(&tenant, "Patient", patient).await.unwrap();

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
async fn test_create_or_update_creates() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    let patient = json!({"resourceType": "Patient"});

    let (resource, was_created) = backend
        .create_or_update(&tenant, "Patient", "new-id", patient)
        .await
        .unwrap();

    assert!(was_created);
    assert_eq!(resource.id(), "new-id");
}

#[tokio::test]
async fn test_create_or_update_updates() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    // Create first
    let patient = json!({"resourceType": "Patient", "name": [{"family": "First"}]});
    backend
        .create_or_update(&tenant, "Patient", "upsert-id", patient)
        .await
        .unwrap();

    // Update
    let patient2 = json!({"resourceType": "Patient", "name": [{"family": "Second"}]});
    let (resource, was_created) = backend
        .create_or_update(&tenant, "Patient", "upsert-id", patient2)
        .await
        .unwrap();

    assert!(!was_created);
    assert_eq!(resource.content()["name"][0]["family"], "Second");
}

// ============================================================================
// Delete Tests
// ============================================================================

#[tokio::test]
async fn test_delete_resource() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    let patient = json!({"resourceType": "Patient"});
    let created = backend.create(&tenant, "Patient", patient).await.unwrap();

    // Delete
    backend
        .delete(&tenant, "Patient", created.id())
        .await
        .unwrap();

    // Read should return Gone error for deleted resources (soft delete behavior)
    let read_result = backend.read(&tenant, "Patient", created.id()).await;
    match read_result {
        Err(StorageError::Resource(ResourceError::Gone { .. })) => {
            // Expected: deleted resources return Gone error
        }
        Ok(None) => {
            // Also acceptable: deleted resource not found
        }
        other => {
            panic!("Expected Gone error or None, got: {:?}", other);
        }
    }
}

#[tokio::test]
async fn test_delete_nonexistent_fails() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    let result = backend.delete(&tenant, "Patient", "nonexistent").await;
    assert!(result.is_err());
}

// ============================================================================
// Tenant Isolation Tests
// ============================================================================

#[tokio::test]
async fn test_tenant_isolation_create() {
    let backend = create_backend();
    let tenant_a = create_tenant("tenant-a");
    let tenant_b = create_tenant("tenant-b");

    let patient = json!({"resourceType": "Patient"});
    let created = backend.create(&tenant_a, "Patient", patient).await.unwrap();

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
async fn test_tenant_isolation_read() {
    let backend = create_backend();
    let tenant_a = create_tenant("tenant-a");
    let tenant_b = create_tenant("tenant-b");

    let patient = json!({"resourceType": "Patient"});
    let created = backend.create(&tenant_a, "Patient", patient).await.unwrap();

    // Tenant A can read
    let read_a = backend
        .read(&tenant_a, "Patient", created.id())
        .await
        .unwrap();
    assert!(read_a.is_some());

    // Tenant B cannot read
    let read_b = backend
        .read(&tenant_b, "Patient", created.id())
        .await
        .unwrap();
    assert!(read_b.is_none());
}

#[tokio::test]
async fn test_same_id_different_tenants() {
    let backend = create_backend();
    let tenant_a = create_tenant("tenant-a");
    let tenant_b = create_tenant("tenant-b");

    let patient_a = json!({"resourceType": "Patient", "name": [{"family": "A"}]});
    let patient_b = json!({"resourceType": "Patient", "name": [{"family": "B"}]});

    // Create same ID in both tenants
    backend
        .create_or_update(&tenant_a, "Patient", "shared-id", patient_a)
        .await
        .unwrap();
    backend
        .create_or_update(&tenant_b, "Patient", "shared-id", patient_b)
        .await
        .unwrap();

    // Each tenant sees their own version
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

#[tokio::test]
async fn test_tenant_isolation_delete() {
    let backend = create_backend();
    let tenant_a = create_tenant("tenant-a");
    let tenant_b = create_tenant("tenant-b");

    let patient = json!({"resourceType": "Patient"});
    let created = backend.create(&tenant_a, "Patient", patient).await.unwrap();

    // Tenant B cannot delete tenant A's resource
    let result = backend.delete(&tenant_b, "Patient", created.id()).await;
    assert!(result.is_err());

    // Resource still exists for tenant A
    assert!(
        backend
            .exists(&tenant_a, "Patient", created.id())
            .await
            .unwrap()
    );
}

// ============================================================================
// Count Tests
// ============================================================================

#[tokio::test]
async fn test_count_resources() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    // Create several patients
    for i in 0..5 {
        let patient = json!({"resourceType": "Patient", "id": format!("p{}", i)});
        backend.create(&tenant, "Patient", patient).await.unwrap();
    }

    let count = backend.count(&tenant, Some("Patient")).await.unwrap();
    assert_eq!(count, 5);
}

#[tokio::test]
async fn test_count_by_tenant() {
    let backend = create_backend();
    let tenant_a = create_tenant("tenant-a");
    let tenant_b = create_tenant("tenant-b");

    // Create 3 in tenant A
    for i in 0..3 {
        let patient = json!({"resourceType": "Patient"});
        backend.create(&tenant_a, "Patient", patient).await.unwrap();
    }

    // Create 2 in tenant B
    for i in 0..2 {
        let patient = json!({"resourceType": "Patient"});
        backend.create(&tenant_b, "Patient", patient).await.unwrap();
    }

    assert_eq!(backend.count(&tenant_a, Some("Patient")).await.unwrap(), 3);
    assert_eq!(backend.count(&tenant_b, Some("Patient")).await.unwrap(), 2);
}

// ============================================================================
// Batch Read Tests
// ============================================================================

#[tokio::test]
async fn test_read_batch() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    // Create resources
    let ids: Vec<String> = (0..3)
        .map(|i| {
            let patient = json!({"resourceType": "Patient", "id": format!("batch-{}", i)});
            format!("batch-{}", i)
        })
        .collect();

    for id in &ids {
        let patient = json!({"resourceType": "Patient"});
        backend
            .create_or_update(&tenant, "Patient", id, patient)
            .await
            .unwrap();
    }

    // Batch read
    let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
    let batch = backend
        .read_batch(&tenant, "Patient", &id_refs)
        .await
        .unwrap();

    assert_eq!(batch.len(), 3);
}

#[tokio::test]
async fn test_read_batch_ignores_other_tenant() {
    let backend = create_backend();
    let tenant_a = create_tenant("tenant-a");
    let tenant_b = create_tenant("tenant-b");

    // Create in tenant A
    backend
        .create_or_update(
            &tenant_a,
            "Patient",
            "a-patient",
            json!({"resourceType": "Patient"}),
        )
        .await
        .unwrap();

    // Create in tenant B
    backend
        .create_or_update(
            &tenant_b,
            "Patient",
            "b-patient",
            json!({"resourceType": "Patient"}),
        )
        .await
        .unwrap();

    // Batch read from tenant A with both IDs
    let ids = ["a-patient", "b-patient"];
    let batch = backend
        .read_batch(&tenant_a, "Patient", &ids)
        .await
        .unwrap();

    // Should only return tenant A's resource
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].id(), "a-patient");
}

// ============================================================================
// Version Tests
// ============================================================================

#[tokio::test]
async fn test_version_increments() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    let patient = json!({"resourceType": "Patient"});
    let v1 = backend.create(&tenant, "Patient", patient).await.unwrap();
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

// ============================================================================
// Content Preservation Tests
// ============================================================================

#[tokio::test]
async fn test_content_preserved() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    let patient = json!({
        "resourceType": "Patient",
        "name": [{"family": "Smith", "given": ["John", "Michael"]}],
        "birthDate": "1985-06-15",
        "active": true,
        "multipleBirthInteger": 2
    });

    let created = backend
        .create(&tenant, "Patient", patient.clone())
        .await
        .unwrap();
    let read = backend
        .read(&tenant, "Patient", created.id())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(read.content()["name"][0]["family"], "Smith");
    assert_eq!(read.content()["name"][0]["given"][0], "John");
    assert_eq!(read.content()["name"][0]["given"][1], "Michael");
    assert_eq!(read.content()["birthDate"], "1985-06-15");
    assert_eq!(read.content()["active"], true);
    assert_eq!(read.content()["multipleBirthInteger"], 2);
}

#[tokio::test]
async fn test_unicode_content() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    let patient = json!({
        "resourceType": "Patient",
        "name": [{"family": "日本語", "given": ["名前"]}]
    });

    let created = backend.create(&tenant, "Patient", patient).await.unwrap();
    let read = backend
        .read(&tenant, "Patient", created.id())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(read.content()["name"][0]["family"], "日本語");
    assert_eq!(read.content()["name"][0]["given"][0], "名前");
}

// ============================================================================
// History Tests
// ============================================================================

#[tokio::test]
async fn test_history_instance() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    // Create a resource
    let patient = json!({"resourceType": "Patient", "name": [{"family": "Smith"}]});
    let created = backend.create(&tenant, "Patient", patient).await.unwrap();

    // Update twice
    let v2 = backend
        .update(
            &tenant,
            &created,
            json!({"resourceType": "Patient", "name": [{"family": "Jones"}]}),
        )
        .await
        .unwrap();
    let _v3 = backend
        .update(
            &tenant,
            &v2,
            json!({"resourceType": "Patient", "name": [{"family": "Brown"}]}),
        )
        .await
        .unwrap();

    // Get history
    let params = HistoryParams::new();
    let history = backend
        .history_instance(&tenant, "Patient", created.id(), &params)
        .await
        .unwrap();

    // Should have 3 versions, newest first
    assert_eq!(history.items.len(), 3);
    assert_eq!(history.items[0].resource.version_id(), "3");
    assert_eq!(history.items[1].resource.version_id(), "2");
    assert_eq!(history.items[2].resource.version_id(), "1");

    // Check methods
    assert_eq!(history.items[0].method, HistoryMethod::Put);
    assert_eq!(history.items[1].method, HistoryMethod::Put);
    assert_eq!(history.items[2].method, HistoryMethod::Post);

    // Check content is correct
    assert_eq!(
        history.items[0].resource.content()["name"][0]["family"],
        "Brown"
    );
    assert_eq!(
        history.items[2].resource.content()["name"][0]["family"],
        "Smith"
    );
}

#[tokio::test]
async fn test_history_instance_count() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    let patient = json!({"resourceType": "Patient"});
    let created = backend.create(&tenant, "Patient", patient).await.unwrap();
    let v2 = backend
        .update(&tenant, &created, json!({"resourceType": "Patient"}))
        .await
        .unwrap();
    let _v3 = backend
        .update(&tenant, &v2, json!({"resourceType": "Patient"}))
        .await
        .unwrap();

    let count = backend
        .history_instance_count(&tenant, "Patient", created.id())
        .await
        .unwrap();
    assert_eq!(count, 3);
}

#[tokio::test]
async fn test_history_with_delete() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    let patient = json!({"resourceType": "Patient", "id": "hist-patient"});
    let created = backend.create(&tenant, "Patient", patient).await.unwrap();
    let _v2 = backend
        .update(
            &tenant,
            &created,
            json!({"resourceType": "Patient", "id": "hist-patient"}),
        )
        .await
        .unwrap();
    backend
        .delete(&tenant, "Patient", "hist-patient")
        .await
        .unwrap();

    // Get history including deleted
    let params = HistoryParams::new().include_deleted(true);
    let history = backend
        .history_instance(&tenant, "Patient", "hist-patient", &params)
        .await
        .unwrap();

    assert_eq!(history.items.len(), 3);
    assert_eq!(history.items[0].method, HistoryMethod::Delete);
    assert_eq!(history.items[0].resource.version_id(), "3");
}

#[tokio::test]
async fn test_history_tenant_isolation() {
    let backend = create_backend();
    let tenant_a = create_tenant("tenant-a");
    let tenant_b = create_tenant("tenant-b");

    // Create in tenant A
    let patient = json!({"resourceType": "Patient", "id": "hist-shared"});
    let created = backend.create(&tenant_a, "Patient", patient).await.unwrap();
    let _v2 = backend
        .update(
            &tenant_a,
            &created,
            json!({"resourceType": "Patient", "id": "hist-shared"}),
        )
        .await
        .unwrap();

    // Tenant A sees history
    let history_a = backend
        .history_instance(&tenant_a, "Patient", "hist-shared", &HistoryParams::new())
        .await
        .unwrap();
    assert_eq!(history_a.items.len(), 2);

    // Tenant B sees nothing
    let history_b = backend
        .history_instance(&tenant_b, "Patient", "hist-shared", &HistoryParams::new())
        .await
        .unwrap();
    assert!(history_b.items.is_empty());
}

// ============================================================================
// Type History Tests
// ============================================================================

#[tokio::test]
async fn test_history_type() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    // Create multiple patients
    let p1 = backend
        .create(
            &tenant,
            "Patient",
            json!({"resourceType": "Patient", "id": "tp1"}),
        )
        .await
        .unwrap();
    let _p2 = backend
        .create(
            &tenant,
            "Patient",
            json!({"resourceType": "Patient", "id": "tp2"}),
        )
        .await
        .unwrap();

    // Update p1
    let _p1_v2 = backend
        .update(
            &tenant,
            &p1,
            json!({"resourceType": "Patient", "id": "tp1"}),
        )
        .await
        .unwrap();

    // Create an observation (different type)
    backend
        .create(
            &tenant,
            "Observation",
            json!({"resourceType": "Observation"}),
        )
        .await
        .unwrap();

    // Get Patient type history
    let history = backend
        .history_type(&tenant, "Patient", &HistoryParams::new())
        .await
        .unwrap();

    // Should have 3 entries for Patient (p1 v1, p1 v2, p2 v1)
    assert_eq!(history.items.len(), 3);

    // All should be Patient type
    for entry in &history.items {
        assert_eq!(entry.resource.resource_type(), "Patient");
    }
}

#[tokio::test]
async fn test_history_type_count() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    // Create patients with updates
    let p1 = backend
        .create(&tenant, "Patient", json!({"resourceType": "Patient"}))
        .await
        .unwrap();
    let _p1_v2 = backend
        .update(&tenant, &p1, json!({"resourceType": "Patient"}))
        .await
        .unwrap();
    let _p2 = backend
        .create(&tenant, "Patient", json!({"resourceType": "Patient"}))
        .await
        .unwrap();

    // Create observation
    backend
        .create(
            &tenant,
            "Observation",
            json!({"resourceType": "Observation"}),
        )
        .await
        .unwrap();

    // Count patient history
    let patient_count = backend
        .history_type_count(&tenant, "Patient")
        .await
        .unwrap();
    assert_eq!(patient_count, 3);

    // Count observation history
    let obs_count = backend
        .history_type_count(&tenant, "Observation")
        .await
        .unwrap();
    assert_eq!(obs_count, 1);
}

#[tokio::test]
async fn test_history_type_tenant_isolation() {
    let backend = create_backend();
    let tenant_a = create_tenant("tenant-a");
    let tenant_b = create_tenant("tenant-b");

    // Create patients in tenant A
    backend
        .create(&tenant_a, "Patient", json!({"resourceType": "Patient"}))
        .await
        .unwrap();
    backend
        .create(&tenant_a, "Patient", json!({"resourceType": "Patient"}))
        .await
        .unwrap();

    // Create patient in tenant B
    backend
        .create(&tenant_b, "Patient", json!({"resourceType": "Patient"}))
        .await
        .unwrap();

    // Tenant A sees only its history
    let history_a = backend
        .history_type(&tenant_a, "Patient", &HistoryParams::new())
        .await
        .unwrap();
    assert_eq!(history_a.items.len(), 2);

    // Tenant B sees only its history
    let history_b = backend
        .history_type(&tenant_b, "Patient", &HistoryParams::new())
        .await
        .unwrap();
    assert_eq!(history_b.items.len(), 1);
}

// ============================================================================
// System History Tests
// ============================================================================

#[tokio::test]
async fn test_history_system() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    // Create different resource types
    let p1 = backend
        .create(
            &tenant,
            "Patient",
            json!({"resourceType": "Patient", "id": "sp1"}),
        )
        .await
        .unwrap();
    backend
        .create(
            &tenant,
            "Observation",
            json!({"resourceType": "Observation", "id": "so1"}),
        )
        .await
        .unwrap();
    backend
        .create(
            &tenant,
            "Encounter",
            json!({"resourceType": "Encounter", "id": "se1"}),
        )
        .await
        .unwrap();

    // Update patient
    let _p1_v2 = backend
        .update(
            &tenant,
            &p1,
            json!({"resourceType": "Patient", "id": "sp1"}),
        )
        .await
        .unwrap();

    // Get system history
    let history = backend
        .history_system(&tenant, &HistoryParams::new())
        .await
        .unwrap();

    // Should have 4 entries total
    assert_eq!(history.items.len(), 4);

    // Should include all resource types
    let types: std::collections::HashSet<_> = history
        .items
        .iter()
        .map(|e| e.resource.resource_type())
        .collect();
    assert!(types.contains("Patient"));
    assert!(types.contains("Observation"));
    assert!(types.contains("Encounter"));
}

#[tokio::test]
async fn test_history_system_count() {
    let backend = create_backend();
    let tenant = create_tenant("test-tenant");

    // Create different resource types with updates
    let p1 = backend
        .create(&tenant, "Patient", json!({"resourceType": "Patient"}))
        .await
        .unwrap();
    let _p1_v2 = backend
        .update(&tenant, &p1, json!({"resourceType": "Patient"}))
        .await
        .unwrap();
    backend
        .create(
            &tenant,
            "Observation",
            json!({"resourceType": "Observation"}),
        )
        .await
        .unwrap();

    // Count all history
    let count = backend.history_system_count(&tenant).await.unwrap();
    assert_eq!(count, 3); // p1 v1, p1 v2, obs
}

#[tokio::test]
async fn test_history_system_tenant_isolation() {
    let backend = create_backend();
    let tenant_a = create_tenant("tenant-a");
    let tenant_b = create_tenant("tenant-b");

    // Create resources in tenant A
    backend
        .create(&tenant_a, "Patient", json!({"resourceType": "Patient"}))
        .await
        .unwrap();
    backend
        .create(
            &tenant_a,
            "Observation",
            json!({"resourceType": "Observation"}),
        )
        .await
        .unwrap();

    // Create resource in tenant B
    backend
        .create(&tenant_b, "Encounter", json!({"resourceType": "Encounter"}))
        .await
        .unwrap();

    // Tenant A sees only its history
    let history_a = backend
        .history_system(&tenant_a, &HistoryParams::new())
        .await
        .unwrap();
    assert_eq!(history_a.items.len(), 2);

    // Tenant B sees only its history
    let history_b = backend
        .history_system(&tenant_b, &HistoryParams::new())
        .await
        .unwrap();
    assert_eq!(history_b.items.len(), 1);

    // Counts should also be isolated
    assert_eq!(backend.history_system_count(&tenant_a).await.unwrap(), 2);
    assert_eq!(backend.history_system_count(&tenant_b).await.unwrap(), 1);
}
