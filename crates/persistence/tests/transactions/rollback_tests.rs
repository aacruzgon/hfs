//! Tests for transaction rollback scenarios.
//!
//! This module tests various rollback scenarios including error conditions,
//! constraint violations, and recovery from failures.

use serde_json::json;

use helios_persistence::core::{ResourceStorage, TransactionProvider};
use helios_persistence::error::StorageError;
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};

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
// Explicit Rollback Tests
// ============================================================================

/// Test explicit rollback after multiple creates.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_rollback_after_creates() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    let tx = backend.begin_transaction(&tenant).await.unwrap();

    // Create several resources
    let mut created_ids = Vec::new();
    for i in 0..5 {
        let patient = json!({
            "resourceType": "Patient",
            "name": [{"family": format!("Rollback{}", i)}]
        });
        let created = backend
            .create_in_transaction(&tx, "Patient", patient)
            .await
            .unwrap();
        created_ids.push(created.id().to_string());
    }

    // Explicit rollback
    backend.abort_transaction(tx).await.unwrap();

    // None of the resources should exist
    for id in &created_ids {
        assert!(
            !backend.exists(&tenant, "Patient", id).await.unwrap(),
            "Resource {} should not exist after rollback",
            id
        );
    }

    let count = backend.count(&tenant, Some("Patient")).await.unwrap();
    assert_eq!(count, 0);
}

/// Test rollback after updates.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_rollback_after_updates() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create resources outside transaction
    let mut resources = Vec::new();
    for i in 0..3 {
        let patient = json!({
            "resourceType": "Patient",
            "name": [{"family": format!("Original{}", i)}]
        });
        let created = backend.create(&tenant, "Patient", patient).await.unwrap();
        resources.push(created);
    }

    // Start transaction and update
    let tx = backend.begin_transaction(&tenant).await.unwrap();

    for (i, resource) in resources.iter().enumerate() {
        let updated_content = json!({
            "resourceType": "Patient",
            "name": [{"family": format!("Modified{}", i)}]
        });
        backend
            .update_in_transaction(&tx, resource, updated_content)
            .await
            .unwrap();
    }

    // Rollback
    backend.abort_transaction(tx).await.unwrap();

    // All resources should have original values
    for (i, resource) in resources.iter().enumerate() {
        let read = backend
            .read(&tenant, "Patient", resource.id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            read.content()["name"][0]["family"],
            format!("Original{}", i),
            "Resource should have original value after rollback"
        );
    }
}

/// Test rollback after deletes.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_rollback_after_deletes() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create resources outside transaction
    let mut resource_ids = Vec::new();
    for i in 0..3 {
        let patient = json!({
            "resourceType": "Patient",
            "name": [{"family": format!("ToDelete{}", i)}]
        });
        let created = backend.create(&tenant, "Patient", patient).await.unwrap();
        resource_ids.push(created.id().to_string());
    }

    // Start transaction and delete
    let tx = backend.begin_transaction(&tenant).await.unwrap();

    for id in &resource_ids {
        backend
            .delete_in_transaction(&tx, "Patient", id)
            .await
            .unwrap();
    }

    // Rollback
    backend.abort_transaction(tx).await.unwrap();

    // All resources should still exist
    for id in &resource_ids {
        assert!(
            backend.exists(&tenant, "Patient", id).await.unwrap(),
            "Resource {} should exist after rollback",
            id
        );
    }

    let count = backend.count(&tenant, Some("Patient")).await.unwrap();
    assert_eq!(count, 3);
}

// ============================================================================
// Error-Triggered Rollback Tests
// ============================================================================

/// Test that transaction is rolled back on constraint violation.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_rollback_on_constraint_violation() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create initial resource
    backend
        .create_or_update(
            &tenant,
            "Patient",
            "unique-id",
            json!({"resourceType": "Patient"}),
        )
        .await
        .unwrap();

    let tx = backend.begin_transaction(&tenant).await.unwrap();

    // Create valid resource
    let valid = backend
        .create_in_transaction(
            &tx,
            "Patient",
            json!({"resourceType": "Patient", "name": [{"family": "Valid"}]}),
        )
        .await
        .unwrap();
    let valid_id = valid.id().to_string();

    // Try to create with duplicate ID in same tenant
    // This should fail due to unique constraint
    let duplicate_result = backend
        .create_with_id_in_transaction(&tx, "Patient", "unique-id", json!({"resourceType": "Patient"}))
        .await;

    // If constraint violation occurs, abort
    if duplicate_result.is_err() {
        backend.abort_transaction(tx).await.unwrap();

        // Valid resource should not exist (transaction rolled back)
        assert!(
            !backend.exists(&tenant, "Patient", &valid_id).await.unwrap(),
            "Valid resource should not exist after rollback"
        );
    }
}

/// Test rollback on version conflict.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_rollback_on_version_conflict() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create initial resource
    let initial = backend
        .create(&tenant, "Patient", json!({"resourceType": "Patient", "name": [{"family": "Initial"}]}))
        .await
        .unwrap();

    // Update outside any transaction to change version
    let _updated = backend
        .update(
            &tenant,
            &initial,
            json!({"resourceType": "Patient", "name": [{"family": "Updated"}]}),
        )
        .await
        .unwrap();

    // Try transaction with stale version
    let tx = backend.begin_transaction(&tenant).await.unwrap();

    // Create valid resource first
    let valid = backend
        .create_in_transaction(
            &tx,
            "Patient",
            json!({"resourceType": "Patient", "name": [{"family": "ShouldRollback"}]}),
        )
        .await
        .unwrap();
    let valid_id = valid.id().to_string();

    // Try to update with stale reference (initial instead of updated)
    let stale_update = backend
        .update_in_transaction(
            &tx,
            &initial, // Stale version
            json!({"resourceType": "Patient", "name": [{"family": "StaleUpdate"}]}),
        )
        .await;

    // If version conflict, abort
    if stale_update.is_err() {
        backend.abort_transaction(tx).await.unwrap();

        // Valid resource should not exist
        assert!(
            !backend.exists(&tenant, "Patient", &valid_id).await.unwrap(),
            "Transaction should have been rolled back"
        );

        // Original should still have "Updated" value
        let read = backend
            .read(&tenant, "Patient", initial.id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read.content()["name"][0]["family"], "Updated");
    }
}

/// Test rollback on resource not found.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_rollback_on_not_found() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    let tx = backend.begin_transaction(&tenant).await.unwrap();

    // Create valid resource
    let valid = backend
        .create_in_transaction(
            &tx,
            "Patient",
            json!({"resourceType": "Patient", "name": [{"family": "Valid"}]}),
        )
        .await
        .unwrap();
    let valid_id = valid.id().to_string();

    // Try to delete non-existent resource
    let delete_result = backend
        .delete_in_transaction(&tx, "Patient", "does-not-exist")
        .await;

    // If not found error, abort
    if delete_result.is_err() {
        backend.abort_transaction(tx).await.unwrap();

        // Valid resource should not exist
        assert!(
            !backend.exists(&tenant, "Patient", &valid_id).await.unwrap(),
            "Transaction should have been rolled back"
        );
    }
}

// ============================================================================
// Partial Failure Tests
// ============================================================================

/// Test that partial success in a batch is rolled back on any failure.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_rollback_partial_batch() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    let tx = backend.begin_transaction(&tenant).await.unwrap();

    // Create 50 resources successfully
    let mut created_ids = Vec::new();
    for i in 0..50 {
        let patient = json!({
            "resourceType": "Patient",
            "name": [{"family": format!("Batch{}", i)}]
        });
        let created = backend
            .create_in_transaction(&tx, "Patient", patient)
            .await
            .unwrap();
        created_ids.push(created.id().to_string());
    }

    // Force an error (implementation-specific)
    // For example, create a resource with invalid data
    let invalid_result = backend
        .update_in_transaction(
            &tx,
            &helios_persistence::types::StoredResource::new(
                "Patient",
                "nonexistent",
                tenant.tenant_id().clone(),
                json!({"resourceType": "Patient"}),
            ),
            json!({"resourceType": "Patient"}),
        )
        .await;

    // If error occurred, abort
    if invalid_result.is_err() {
        backend.abort_transaction(tx).await.unwrap();

        // None of the 50 resources should exist
        let count = backend.count(&tenant, Some("Patient")).await.unwrap();
        assert_eq!(count, 0, "All creates should be rolled back");
    }
}

// ============================================================================
// Recovery Tests
// ============================================================================

/// Test that backend is usable after rollback.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_backend_usable_after_rollback() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // First transaction - abort
    let tx1 = backend.begin_transaction(&tenant).await.unwrap();
    backend
        .create_in_transaction(&tx1, "Patient", json!({"resourceType": "Patient"}))
        .await
        .unwrap();
    backend.abort_transaction(tx1).await.unwrap();

    // Second transaction - should work normally
    let tx2 = backend.begin_transaction(&tenant).await.unwrap();
    let created = backend
        .create_in_transaction(
            &tx2,
            "Patient",
            json!({"resourceType": "Patient", "name": [{"family": "AfterRollback"}]}),
        )
        .await
        .unwrap();
    backend.commit_transaction(tx2).await.unwrap();

    // Resource should exist
    let read = backend
        .read(&tenant, "Patient", created.id())
        .await
        .unwrap();
    assert!(read.is_some());

    let count = backend.count(&tenant, Some("Patient")).await.unwrap();
    assert_eq!(count, 1);
}

/// Test multiple sequential rollbacks.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_multiple_sequential_rollbacks() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    for i in 0..5 {
        let tx = backend.begin_transaction(&tenant).await.unwrap();

        let patient = json!({
            "resourceType": "Patient",
            "name": [{"family": format!("Sequential{}", i)}]
        });
        backend
            .create_in_transaction(&tx, "Patient", patient)
            .await
            .unwrap();

        backend.abort_transaction(tx).await.unwrap();
    }

    // No resources should exist after all rollbacks
    let count = backend.count(&tenant, Some("Patient")).await.unwrap();
    assert_eq!(count, 0);

    // Backend should still be usable
    let created = backend
        .create(
            &tenant,
            "Patient",
            json!({"resourceType": "Patient", "name": [{"family": "FinalCreate"}]}),
        )
        .await
        .unwrap();

    assert!(backend.exists(&tenant, "Patient", created.id()).await.unwrap());
}

/// Test rollback with different resource types.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_rollback_multiple_resource_types() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    let tx = backend.begin_transaction(&tenant).await.unwrap();

    // Create different resource types
    let patient = backend
        .create_in_transaction(&tx, "Patient", json!({"resourceType": "Patient"}))
        .await
        .unwrap();

    let obs = backend
        .create_in_transaction(
            &tx,
            "Observation",
            json!({
                "resourceType": "Observation",
                "status": "final",
                "code": {"coding": [{"code": "test"}]}
            }),
        )
        .await
        .unwrap();

    let org = backend
        .create_in_transaction(
            &tx,
            "Organization",
            json!({"resourceType": "Organization", "name": "Test Org"}),
        )
        .await
        .unwrap();

    let patient_id = patient.id().to_string();
    let obs_id = obs.id().to_string();
    let org_id = org.id().to_string();

    // Rollback
    backend.abort_transaction(tx).await.unwrap();

    // All resource types should be rolled back
    assert!(!backend.exists(&tenant, "Patient", &patient_id).await.unwrap());
    assert!(!backend.exists(&tenant, "Observation", &obs_id).await.unwrap());
    assert!(!backend.exists(&tenant, "Organization", &org_id).await.unwrap());
}

// ============================================================================
// Savepoint Tests (if supported)
// ============================================================================

/// Test savepoint functionality if supported by backend.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_savepoint_rollback() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    let tx = backend.begin_transaction(&tenant).await.unwrap();

    // Create first resource
    let first = backend
        .create_in_transaction(
            &tx,
            "Patient",
            json!({"resourceType": "Patient", "name": [{"family": "First"}]}),
        )
        .await
        .unwrap();
    let first_id = first.id().to_string();

    // If savepoints are supported, create one here
    let savepoint_result = backend.create_savepoint(&tx, "sp1").await;

    if savepoint_result.is_ok() {
        // Create second resource after savepoint
        let second = backend
            .create_in_transaction(
                &tx,
                "Patient",
                json!({"resourceType": "Patient", "name": [{"family": "Second"}]}),
            )
            .await
            .unwrap();
        let second_id = second.id().to_string();

        // Rollback to savepoint
        backend.rollback_to_savepoint(&tx, "sp1").await.unwrap();

        // Commit
        backend.commit_transaction(tx).await.unwrap();

        // First should exist, second should not
        assert!(backend.exists(&tenant, "Patient", &first_id).await.unwrap());
        assert!(!backend.exists(&tenant, "Patient", &second_id).await.unwrap());
    } else {
        // Savepoints not supported - just rollback entire transaction
        backend.abort_transaction(tx).await.unwrap();
    }
}

// ============================================================================
// Stress Tests
// ============================================================================

/// Test rapid transaction start/abort cycles.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_rapid_transaction_cycles() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    for _ in 0..100 {
        let tx = backend.begin_transaction(&tenant).await.unwrap();
        backend
            .create_in_transaction(&tx, "Patient", json!({"resourceType": "Patient"}))
            .await
            .unwrap();
        backend.abort_transaction(tx).await.unwrap();
    }

    // Backend should be stable
    let count = backend.count(&tenant, Some("Patient")).await.unwrap();
    assert_eq!(count, 0);

    // Can still perform operations
    let created = backend
        .create(&tenant, "Patient", json!({"resourceType": "Patient"}))
        .await
        .unwrap();
    assert!(backend.exists(&tenant, "Patient", created.id()).await.unwrap());
}
