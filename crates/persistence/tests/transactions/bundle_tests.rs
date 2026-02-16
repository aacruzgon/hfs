//! Tests for FHIR bundle transaction operations.
//!
//! This module tests FHIR transaction bundles including the various
//! HTTP method equivalents and conditional operations.

use serde_json::json;

use helios_persistence::core::{ResourceStorage, TransactionProvider};
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use helios_persistence::types::{BundleEntry, BundleRequest, TransactionBundle};

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
// Basic Bundle Tests
// ============================================================================

/// Test executing a simple transaction bundle with creates.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_bundle_create_entries() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    let bundle = TransactionBundle::new(vec![
        BundleEntry {
            full_url: Some("urn:uuid:patient-1".to_string()),
            resource: Some(json!({
                "resourceType": "Patient",
                "name": [{"family": "BundlePatient1"}]
            })),
            request: BundleRequest {
                method: "POST".to_string(),
                url: "Patient".to_string(),
                if_match: None,
                if_none_match: None,
                if_none_exist: None,
            },
        },
        BundleEntry {
            full_url: Some("urn:uuid:patient-2".to_string()),
            resource: Some(json!({
                "resourceType": "Patient",
                "name": [{"family": "BundlePatient2"}]
            })),
            request: BundleRequest {
                method: "POST".to_string(),
                url: "Patient".to_string(),
                if_match: None,
                if_none_match: None,
                if_none_exist: None,
            },
        },
    ]);

    let result = backend.execute_transaction(&tenant, bundle).await.unwrap();

    // Should have 2 response entries
    assert_eq!(result.entries.len(), 2);

    // Both should be successful creates
    for entry in &result.entries {
        assert_eq!(entry.response.status, "201 Created");
        assert!(entry.response.location.is_some());
    }

    // Verify resources exist
    let count = backend.count(&tenant, Some("Patient")).await.unwrap();
    assert_eq!(count, 2);
}

/// Test bundle with PUT (create or update).
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_bundle_put_entries() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    let bundle = TransactionBundle::new(vec![BundleEntry {
        full_url: Some("urn:uuid:patient-put".to_string()),
        resource: Some(json!({
            "resourceType": "Patient",
            "id": "patient-123",
            "name": [{"family": "PutPatient"}]
        })),
        request: BundleRequest {
            method: "PUT".to_string(),
            url: "Patient/patient-123".to_string(),
            if_match: None,
            if_none_match: None,
            if_none_exist: None,
        },
    }]);

    let result = backend.execute_transaction(&tenant, bundle).await.unwrap();

    assert_eq!(result.entries.len(), 1);
    assert!(
        result.entries[0].response.status == "201 Created"
            || result.entries[0].response.status == "200 OK"
    );

    // Verify resource
    let read = backend
        .read(&tenant, "Patient", "patient-123")
        .await
        .unwrap();
    assert!(read.is_some());
    assert_eq!(read.unwrap().content()["name"][0]["family"], "PutPatient");
}

/// Test bundle with DELETE.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_bundle_delete_entries() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // First create a resource
    backend
        .create_or_update(
            &tenant,
            "Patient",
            "to-delete",
            json!({"resourceType": "Patient"}),
        )
        .await
        .unwrap();

    let bundle = TransactionBundle::new(vec![BundleEntry {
        full_url: None,
        resource: None,
        request: BundleRequest {
            method: "DELETE".to_string(),
            url: "Patient/to-delete".to_string(),
            if_match: None,
            if_none_match: None,
            if_none_exist: None,
        },
    }]);

    let result = backend.execute_transaction(&tenant, bundle).await.unwrap();

    assert_eq!(result.entries.len(), 1);
    assert!(
        result.entries[0].response.status == "200 OK"
            || result.entries[0].response.status == "204 No Content"
    );

    // Verify deleted
    assert!(!backend.exists(&tenant, "Patient", "to-delete").await.unwrap());
}

// ============================================================================
// Mixed Operation Bundle Tests
// ============================================================================

/// Test bundle with mixed operations (CREATE, UPDATE, DELETE).
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_bundle_mixed_operations() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Pre-create resources for update and delete
    backend
        .create_or_update(
            &tenant,
            "Patient",
            "update-me",
            json!({"resourceType": "Patient", "name": [{"family": "Original"}]}),
        )
        .await
        .unwrap();
    backend
        .create_or_update(
            &tenant,
            "Patient",
            "delete-me",
            json!({"resourceType": "Patient"}),
        )
        .await
        .unwrap();

    let bundle = TransactionBundle::new(vec![
        // CREATE
        BundleEntry {
            full_url: Some("urn:uuid:new-patient".to_string()),
            resource: Some(json!({
                "resourceType": "Patient",
                "name": [{"family": "NewPatient"}]
            })),
            request: BundleRequest {
                method: "POST".to_string(),
                url: "Patient".to_string(),
                if_match: None,
                if_none_match: None,
                if_none_exist: None,
            },
        },
        // UPDATE
        BundleEntry {
            full_url: None,
            resource: Some(json!({
                "resourceType": "Patient",
                "id": "update-me",
                "name": [{"family": "Updated"}]
            })),
            request: BundleRequest {
                method: "PUT".to_string(),
                url: "Patient/update-me".to_string(),
                if_match: None,
                if_none_match: None,
                if_none_exist: None,
            },
        },
        // DELETE
        BundleEntry {
            full_url: None,
            resource: None,
            request: BundleRequest {
                method: "DELETE".to_string(),
                url: "Patient/delete-me".to_string(),
                if_match: None,
                if_none_match: None,
                if_none_exist: None,
            },
        },
    ]);

    let result = backend.execute_transaction(&tenant, bundle).await.unwrap();

    assert_eq!(result.entries.len(), 3);

    // Verify all operations succeeded
    let count = backend.count(&tenant, Some("Patient")).await.unwrap();
    assert_eq!(count, 2); // 1 pre-existing + 1 new - 1 deleted

    // Verify update
    let updated = backend
        .read(&tenant, "Patient", "update-me")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated.content()["name"][0]["family"], "Updated");

    // Verify delete
    assert!(!backend.exists(&tenant, "Patient", "delete-me").await.unwrap());
}

// ============================================================================
// Reference Resolution Tests
// ============================================================================

/// Test bundle with internal references (urn:uuid).
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_bundle_internal_references() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    let bundle = TransactionBundle::new(vec![
        // Create patient first
        BundleEntry {
            full_url: Some("urn:uuid:new-patient".to_string()),
            resource: Some(json!({
                "resourceType": "Patient",
                "name": [{"family": "ReferencedPatient"}]
            })),
            request: BundleRequest {
                method: "POST".to_string(),
                url: "Patient".to_string(),
                if_match: None,
                if_none_match: None,
                if_none_exist: None,
            },
        },
        // Create observation referencing patient by urn:uuid
        BundleEntry {
            full_url: Some("urn:uuid:new-observation".to_string()),
            resource: Some(json!({
                "resourceType": "Observation",
                "status": "final",
                "code": {"coding": [{"code": "test"}]},
                "subject": {"reference": "urn:uuid:new-patient"}
            })),
            request: BundleRequest {
                method: "POST".to_string(),
                url: "Observation".to_string(),
                if_match: None,
                if_none_match: None,
                if_none_exist: None,
            },
        },
    ]);

    let result = backend.execute_transaction(&tenant, bundle).await.unwrap();

    assert_eq!(result.entries.len(), 2);

    // Get the patient's assigned ID from the response
    let patient_location = result.entries[0].response.location.as_ref().unwrap();
    let patient_id = patient_location.split('/').last().unwrap();

    // Find the observation and verify reference was resolved
    let obs_location = result.entries[1].response.location.as_ref().unwrap();
    let obs_id = obs_location.split('/').last().unwrap();

    let observation = backend
        .read(&tenant, "Observation", obs_id)
        .await
        .unwrap()
        .unwrap();

    // Reference should be resolved to actual Patient ID
    let subject_ref = observation.content()["subject"]["reference"].as_str().unwrap();
    assert!(
        subject_ref.contains(patient_id),
        "Reference should be resolved to actual patient ID"
    );
}

// ============================================================================
// Conditional Bundle Tests
// ============================================================================

/// Test bundle with conditional create (if-none-exist).
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_bundle_conditional_create() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // First bundle - should create
    let bundle1 = TransactionBundle::new(vec![BundleEntry {
        full_url: Some("urn:uuid:conditional".to_string()),
        resource: Some(json!({
            "resourceType": "Patient",
            "identifier": [{"system": "http://example.org", "value": "12345"}],
            "name": [{"family": "Conditional"}]
        })),
        request: BundleRequest {
            method: "POST".to_string(),
            url: "Patient".to_string(),
            if_match: None,
            if_none_match: None,
            if_none_exist: Some("identifier=http://example.org|12345".to_string()),
        },
    }]);

    let result1 = backend.execute_transaction(&tenant, bundle1).await.unwrap();
    assert_eq!(result1.entries[0].response.status, "201 Created");

    // Second bundle with same condition - should return existing
    let bundle2 = TransactionBundle::new(vec![BundleEntry {
        full_url: Some("urn:uuid:conditional".to_string()),
        resource: Some(json!({
            "resourceType": "Patient",
            "identifier": [{"system": "http://example.org", "value": "12345"}],
            "name": [{"family": "ShouldNotCreate"}]
        })),
        request: BundleRequest {
            method: "POST".to_string(),
            url: "Patient".to_string(),
            if_match: None,
            if_none_match: None,
            if_none_exist: Some("identifier=http://example.org|12345".to_string()),
        },
    }]);

    let result2 = backend.execute_transaction(&tenant, bundle2).await.unwrap();

    // Should not create duplicate
    assert_ne!(result2.entries[0].response.status, "201 Created");

    // Only one patient should exist
    let count = backend.count(&tenant, Some("Patient")).await.unwrap();
    assert_eq!(count, 1);
}

/// Test bundle with conditional update (if-match).
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_bundle_conditional_update_if_match() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create initial resource
    let created = backend
        .create_or_update(
            &tenant,
            "Patient",
            "conditional-update",
            json!({"resourceType": "Patient", "name": [{"family": "Original"}]}),
        )
        .await
        .unwrap();

    let etag = format!("W/\"{}\"", created.version());

    // Update with correct ETag
    let bundle = TransactionBundle::new(vec![BundleEntry {
        full_url: None,
        resource: Some(json!({
            "resourceType": "Patient",
            "id": "conditional-update",
            "name": [{"family": "UpdatedWithMatch"}]
        })),
        request: BundleRequest {
            method: "PUT".to_string(),
            url: "Patient/conditional-update".to_string(),
            if_match: Some(etag),
            if_none_match: None,
            if_none_exist: None,
        },
    }]);

    let result = backend.execute_transaction(&tenant, bundle).await.unwrap();
    assert_eq!(result.entries[0].response.status, "200 OK");

    // Verify update
    let read = backend
        .read(&tenant, "Patient", "conditional-update")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.content()["name"][0]["family"], "UpdatedWithMatch");
}

/// Test bundle with if-match failure.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_bundle_if_match_failure() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Create initial resource
    backend
        .create_or_update(
            &tenant,
            "Patient",
            "version-conflict",
            json!({"resourceType": "Patient"}),
        )
        .await
        .unwrap();

    // Update with wrong ETag
    let bundle = TransactionBundle::new(vec![BundleEntry {
        full_url: None,
        resource: Some(json!({
            "resourceType": "Patient",
            "id": "version-conflict",
            "name": [{"family": "ShouldFail"}]
        })),
        request: BundleRequest {
            method: "PUT".to_string(),
            url: "Patient/version-conflict".to_string(),
            if_match: Some("W/\"wrong-version\"".to_string()),
            if_none_match: None,
            if_none_exist: None,
        },
    }]);

    let result = backend.execute_transaction(&tenant, bundle).await;

    // Should fail due to version mismatch
    assert!(result.is_err() || result.unwrap().entries[0].response.status.contains("409"));
}

// ============================================================================
// Bundle Atomicity Tests
// ============================================================================

/// Test that bundle is atomic - all succeed or all fail.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_bundle_atomicity() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    // Bundle with valid operation and invalid operation
    let bundle = TransactionBundle::new(vec![
        // Valid create
        BundleEntry {
            full_url: Some("urn:uuid:valid".to_string()),
            resource: Some(json!({
                "resourceType": "Patient",
                "name": [{"family": "Valid"}]
            })),
            request: BundleRequest {
                method: "POST".to_string(),
                url: "Patient".to_string(),
                if_match: None,
                if_none_match: None,
                if_none_exist: None,
            },
        },
        // Invalid - delete non-existent
        BundleEntry {
            full_url: None,
            resource: None,
            request: BundleRequest {
                method: "DELETE".to_string(),
                url: "Patient/non-existent-id".to_string(),
                if_match: None,
                if_none_match: None,
                if_none_exist: None,
            },
        },
    ]);

    let result = backend.execute_transaction(&tenant, bundle).await;

    // If transaction failed, no resources should be created
    if result.is_err() {
        let count = backend.count(&tenant, Some("Patient")).await.unwrap();
        assert_eq!(count, 0, "Transaction should be atomic - no partial commits");
    }
}

// ============================================================================
// Bundle Edge Cases
// ============================================================================

/// Test empty bundle.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_bundle_empty() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    let bundle = TransactionBundle::new(vec![]);
    let result = backend.execute_transaction(&tenant, bundle).await;

    // Empty bundle should succeed with empty response
    assert!(result.is_ok());
    assert!(result.unwrap().entries.is_empty());
}

/// Test bundle with single entry.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_bundle_single_entry() {
    let backend = create_sqlite_backend();
    let tenant = create_tenant();

    let bundle = TransactionBundle::new(vec![BundleEntry {
        full_url: Some("urn:uuid:single".to_string()),
        resource: Some(json!({"resourceType": "Patient"})),
        request: BundleRequest {
            method: "POST".to_string(),
            url: "Patient".to_string(),
            if_match: None,
            if_none_match: None,
            if_none_exist: None,
        },
    }]);

    let result = backend.execute_transaction(&tenant, bundle).await.unwrap();
    assert_eq!(result.entries.len(), 1);
    assert_eq!(result.entries[0].response.status, "201 Created");
}

/// Test bundle respects tenant isolation.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_bundle_tenant_isolation() {
    let backend = create_sqlite_backend();
    let tenant_a = TenantContext::new(TenantId::new("tenant-a"), TenantPermissions::full_access());
    let tenant_b = TenantContext::new(TenantId::new("tenant-b"), TenantPermissions::full_access());

    let bundle = TransactionBundle::new(vec![BundleEntry {
        full_url: Some("urn:uuid:tenant-patient".to_string()),
        resource: Some(json!({
            "resourceType": "Patient",
            "name": [{"family": "TenantA"}]
        })),
        request: BundleRequest {
            method: "POST".to_string(),
            url: "Patient".to_string(),
            if_match: None,
            if_none_match: None,
            if_none_exist: None,
        },
    }]);

    let result = backend.execute_transaction(&tenant_a, bundle).await.unwrap();
    let location = result.entries[0].response.location.as_ref().unwrap();
    let patient_id = location.split('/').last().unwrap();

    // Tenant A can see it
    assert!(backend.exists(&tenant_a, "Patient", patient_id).await.unwrap());

    // Tenant B cannot
    assert!(!backend.exists(&tenant_b, "Patient", patient_id).await.unwrap());
}
