//! ResourceStorage and VersionedStorage implementations for SQLite.

use async_trait::async_trait;
use chrono::Utc;
use rusqlite::params;
use serde_json::Value;

use crate::core::{ResourceStorage, VersionedStorage};
use crate::error::{BackendError, ConcurrencyError, ResourceError, StorageError, StorageResult};
use crate::tenant::TenantContext;
use crate::types::StoredResource;

use super::SqliteBackend;

fn internal_error(message: String) -> StorageError {
    StorageError::Backend(BackendError::Internal {
        backend_name: "sqlite".to_string(),
        message,
        source: None,
    })
}

fn serialization_error(message: String) -> StorageError {
    StorageError::Backend(BackendError::SerializationError { message })
}

#[async_trait]
impl ResourceStorage for SqliteBackend {
    fn backend_name(&self) -> &'static str {
        "sqlite"
    }

    async fn create(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        resource: Value,
    ) -> StorageResult<StoredResource> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        // Extract or generate ID
        let id = resource
            .get("id")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        // Check if resource already exists
        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM resources WHERE tenant_id = ?1 AND resource_type = ?2 AND id = ?3",
                params![tenant_id, resource_type, id],
                |_| Ok(true),
            )
            .unwrap_or(false);

        if exists {
            return Err(StorageError::Resource(ResourceError::AlreadyExists {
                resource_type: resource_type.to_string(),
                id: id.clone(),
            }));
        }

        // Ensure the resource has correct type and id
        let mut resource = resource;
        if let Some(obj) = resource.as_object_mut() {
            obj.insert("resourceType".to_string(), Value::String(resource_type.to_string()));
            obj.insert("id".to_string(), Value::String(id.clone()));
        }

        // Serialize the resource data
        let data = serde_json::to_vec(&resource)
            .map_err(|e| serialization_error(format!("Failed to serialize resource: {}", e)))?;

        let now = Utc::now();
        let last_updated = now.to_rfc3339();
        let version_id = "1";

        // Insert the resource
        conn.execute(
            "INSERT INTO resources (tenant_id, resource_type, id, version_id, data, last_updated, is_deleted)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0)",
            params![tenant_id, resource_type, id, version_id, data, last_updated],
        )
        .map_err(|e| internal_error(format!("Failed to insert resource: {}", e)))?;

        // Insert into history
        conn.execute(
            "INSERT INTO resource_history (tenant_id, resource_type, id, version_id, data, last_updated, is_deleted)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0)",
            params![tenant_id, resource_type, id, version_id, data, last_updated],
        )
        .map_err(|e| internal_error(format!("Failed to insert history: {}", e)))?;

        // Return the stored resource with updated metadata
        Ok(StoredResource::from_storage(
            resource_type,
            &id,
            version_id,
            tenant.tenant_id().clone(),
            resource,
            now,
            now,
            None,
        ))
    }

    async fn create_or_update(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        resource: Value,
    ) -> StorageResult<(StoredResource, bool)> {
        // Check if exists
        let existing = self.read(tenant, resource_type, id).await?;

        if let Some(current) = existing {
            // Update existing
            let updated = self.update(tenant, &current, resource).await?;
            Ok((updated, false))
        } else {
            // Create new with specific ID
            let mut resource = resource;
            if let Some(obj) = resource.as_object_mut() {
                obj.insert("id".to_string(), Value::String(id.to_string()));
            }
            let created = self.create(tenant, resource_type, resource).await?;
            Ok((created, true))
        }
    }

    async fn read(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<Option<StoredResource>> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        let result = conn.query_row(
            "SELECT version_id, data, last_updated, is_deleted, deleted_at
             FROM resources
             WHERE tenant_id = ?1 AND resource_type = ?2 AND id = ?3",
            params![tenant_id, resource_type, id],
            |row| {
                let version_id: String = row.get(0)?;
                let data: Vec<u8> = row.get(1)?;
                let last_updated: String = row.get(2)?;
                let is_deleted: i32 = row.get(3)?;
                let deleted_at: Option<String> = row.get(4)?;
                Ok((version_id, data, last_updated, is_deleted, deleted_at))
            },
        );

        match result {
            Ok((version_id, data, last_updated, is_deleted, deleted_at)) => {
                // If deleted, return Gone error
                if is_deleted != 0 {
                    let deleted_at = deleted_at.and_then(|s| {
                        chrono::DateTime::parse_from_rfc3339(&s)
                            .ok()
                            .map(|dt| dt.with_timezone(&Utc))
                    });
                    return Err(StorageError::Resource(ResourceError::Gone {
                        resource_type: resource_type.to_string(),
                        id: id.to_string(),
                        deleted_at,
                    }));
                }

                let json_data: serde_json::Value = serde_json::from_slice(&data)
                    .map_err(|e| serialization_error(format!("Failed to deserialize resource: {}", e)))?;

                let last_updated = chrono::DateTime::parse_from_rfc3339(&last_updated)
                    .map_err(|e| internal_error(format!("Failed to parse last_updated: {}", e)))?
                    .with_timezone(&Utc);

                Ok(Some(StoredResource::from_storage(
                    resource_type,
                    id,
                    version_id,
                    tenant.tenant_id().clone(),
                    json_data,
                    last_updated,
                    last_updated,
                    None,
                )))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(internal_error(format!("Failed to read resource: {}", e))),
        }
    }

    async fn update(
        &self,
        tenant: &TenantContext,
        current: &StoredResource,
        resource: Value,
    ) -> StorageResult<StoredResource> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();
        let resource_type = current.resource_type();
        let id = current.id();

        // Check that the resource still exists with the expected version
        let actual_version: Result<String, _> = conn.query_row(
            "SELECT version_id FROM resources
             WHERE tenant_id = ?1 AND resource_type = ?2 AND id = ?3 AND is_deleted = 0",
            params![tenant_id, resource_type, id],
            |row| row.get(0),
        );

        let actual_version = match actual_version {
            Ok(v) => v,
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                return Err(StorageError::Resource(ResourceError::NotFound {
                    resource_type: resource_type.to_string(),
                    id: id.to_string(),
                }));
            }
            Err(e) => {
                return Err(internal_error(format!("Failed to get current version: {}", e)));
            }
        };

        // Check version match
        if actual_version != current.version_id() {
            return Err(StorageError::Concurrency(ConcurrencyError::VersionConflict {
                resource_type: resource_type.to_string(),
                id: id.to_string(),
                expected_version: current.version_id().to_string(),
                actual_version,
            }));
        }

        // Calculate new version
        let new_version: u64 = actual_version.parse().unwrap_or(0) + 1;
        let new_version_str = new_version.to_string();

        // Ensure the resource has correct type and id
        let mut resource = resource;
        if let Some(obj) = resource.as_object_mut() {
            obj.insert("resourceType".to_string(), Value::String(resource_type.to_string()));
            obj.insert("id".to_string(), Value::String(id.to_string()));
        }

        // Serialize the resource data
        let data = serde_json::to_vec(&resource)
            .map_err(|e| serialization_error(format!("Failed to serialize resource: {}", e)))?;

        let now = Utc::now();
        let last_updated = now.to_rfc3339();

        // Update the resource
        conn.execute(
            "UPDATE resources SET version_id = ?1, data = ?2, last_updated = ?3
             WHERE tenant_id = ?4 AND resource_type = ?5 AND id = ?6",
            params![new_version_str, data, last_updated, tenant_id, resource_type, id],
        )
        .map_err(|e| internal_error(format!("Failed to update resource: {}", e)))?;

        // Insert into history
        conn.execute(
            "INSERT INTO resource_history (tenant_id, resource_type, id, version_id, data, last_updated, is_deleted)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0)",
            params![tenant_id, resource_type, id, new_version_str, data, last_updated],
        )
        .map_err(|e| internal_error(format!("Failed to insert history: {}", e)))?;

        Ok(StoredResource::from_storage(
            resource_type,
            id,
            new_version_str,
            tenant.tenant_id().clone(),
            resource,
            now,
            now,
            None,
        ))
    }

    async fn delete(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<()> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        // Check if resource exists
        let result: Result<(String, Vec<u8>), _> = conn.query_row(
            "SELECT version_id, data FROM resources
             WHERE tenant_id = ?1 AND resource_type = ?2 AND id = ?3 AND is_deleted = 0",
            params![tenant_id, resource_type, id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        );

        let (current_version, data) = match result {
            Ok(v) => v,
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                return Err(StorageError::Resource(ResourceError::NotFound {
                    resource_type: resource_type.to_string(),
                    id: id.to_string(),
                }));
            }
            Err(e) => {
                return Err(internal_error(format!("Failed to check resource: {}", e)));
            }
        };

        let now = Utc::now();
        let deleted_at = now.to_rfc3339();

        // Calculate new version for the deletion record
        let new_version: u64 = current_version.parse().unwrap_or(0) + 1;
        let new_version_str = new_version.to_string();

        // Soft delete the resource
        conn.execute(
            "UPDATE resources SET is_deleted = 1, deleted_at = ?1, version_id = ?2, last_updated = ?1
             WHERE tenant_id = ?3 AND resource_type = ?4 AND id = ?5",
            params![deleted_at, new_version_str, tenant_id, resource_type, id],
        )
        .map_err(|e| internal_error(format!("Failed to delete resource: {}", e)))?;

        // Insert deletion record into history
        conn.execute(
            "INSERT INTO resource_history (tenant_id, resource_type, id, version_id, data, last_updated, is_deleted)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)",
            params![tenant_id, resource_type, id, new_version_str, data, deleted_at],
        )
        .map_err(|e| internal_error(format!("Failed to insert deletion history: {}", e)))?;

        // Delete search index entries
        conn.execute(
            "DELETE FROM search_index WHERE tenant_id = ?1 AND resource_type = ?2 AND resource_id = ?3",
            params![tenant_id, resource_type, id],
        )
        .map_err(|e| internal_error(format!("Failed to delete search index: {}", e)))?;

        Ok(())
    }

    async fn count(
        &self,
        tenant: &TenantContext,
        resource_type: Option<&str>,
    ) -> StorageResult<u64> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        let count: i64 = if let Some(rt) = resource_type {
            conn.query_row(
                "SELECT COUNT(*) FROM resources WHERE tenant_id = ?1 AND resource_type = ?2 AND is_deleted = 0",
                params![tenant_id, rt],
                |row| row.get(0),
            )
        } else {
            conn.query_row(
                "SELECT COUNT(*) FROM resources WHERE tenant_id = ?1 AND is_deleted = 0",
                params![tenant_id],
                |row| row.get(0),
            )
        }
        .map_err(|e| internal_error(format!("Failed to count resources: {}", e)))?;

        Ok(count as u64)
    }
}

#[async_trait]
impl VersionedStorage for SqliteBackend {
    async fn vread(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        version_id: &str,
    ) -> StorageResult<Option<StoredResource>> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        let result = conn.query_row(
            "SELECT data, last_updated, is_deleted
             FROM resource_history
             WHERE tenant_id = ?1 AND resource_type = ?2 AND id = ?3 AND version_id = ?4",
            params![tenant_id, resource_type, id, version_id],
            |row| {
                let data: Vec<u8> = row.get(0)?;
                let last_updated: String = row.get(1)?;
                let is_deleted: i32 = row.get(2)?;
                Ok((data, last_updated, is_deleted))
            },
        );

        match result {
            Ok((data, last_updated, is_deleted)) => {
                let json_data: serde_json::Value = serde_json::from_slice(&data)
                    .map_err(|e| serialization_error(format!("Failed to deserialize resource: {}", e)))?;

                let last_updated = chrono::DateTime::parse_from_rfc3339(&last_updated)
                    .map_err(|e| internal_error(format!("Failed to parse last_updated: {}", e)))?
                    .with_timezone(&Utc);

                // For deleted versions, use last_updated as deleted_at
                let deleted_at = if is_deleted != 0 {
                    Some(last_updated)
                } else {
                    None
                };

                Ok(Some(StoredResource::from_storage(
                    resource_type,
                    id,
                    version_id,
                    tenant.tenant_id().clone(),
                    json_data,
                    last_updated,
                    last_updated,
                    deleted_at,
                )))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(internal_error(format!("Failed to read version: {}", e))),
        }
    }

    async fn update_with_match(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        expected_version: &str,
        resource: Value,
    ) -> StorageResult<StoredResource> {
        // Read current resource
        let current = self.read(tenant, resource_type, id).await?
            .ok_or_else(|| StorageError::Resource(ResourceError::NotFound {
                resource_type: resource_type.to_string(),
                id: id.to_string(),
            }))?;

        // Check version match
        if current.version_id() != expected_version {
            return Err(StorageError::Concurrency(ConcurrencyError::VersionConflict {
                resource_type: resource_type.to_string(),
                id: id.to_string(),
                expected_version: expected_version.to_string(),
                actual_version: current.version_id().to_string(),
            }));
        }

        // Perform update
        self.update(tenant, &current, resource).await
    }

    async fn delete_with_match(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        expected_version: &str,
    ) -> StorageResult<()> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        // Check version match
        let current_version: Result<String, _> = conn.query_row(
            "SELECT version_id FROM resources
             WHERE tenant_id = ?1 AND resource_type = ?2 AND id = ?3 AND is_deleted = 0",
            params![tenant_id, resource_type, id],
            |row| row.get(0),
        );

        let current_version = match current_version {
            Ok(v) => v,
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                return Err(StorageError::Resource(ResourceError::NotFound {
                    resource_type: resource_type.to_string(),
                    id: id.to_string(),
                }));
            }
            Err(e) => {
                return Err(internal_error(format!("Failed to get current version: {}", e)));
            }
        };

        if current_version != expected_version {
            return Err(StorageError::Concurrency(ConcurrencyError::VersionConflict {
                resource_type: resource_type.to_string(),
                id: id.to_string(),
                expected_version: expected_version.to_string(),
                actual_version: current_version,
            }));
        }

        // Perform delete
        self.delete(tenant, resource_type, id).await
    }

    async fn list_versions(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<Vec<String>> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        let mut stmt = conn
            .prepare(
                "SELECT version_id FROM resource_history
                 WHERE tenant_id = ?1 AND resource_type = ?2 AND id = ?3
                 ORDER BY CAST(version_id AS INTEGER) ASC",
            )
            .map_err(|e| internal_error(format!("Failed to prepare query: {}", e)))?;

        let versions = stmt
            .query_map(params![tenant_id, resource_type, id], |row| row.get(0))
            .map_err(|e| internal_error(format!("Failed to list versions: {}", e)))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(versions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant::{TenantId, TenantPermissions};
    use serde_json::json;

    fn create_test_backend() -> SqliteBackend {
        let backend = SqliteBackend::in_memory().unwrap();
        backend.init_schema().unwrap();
        backend
    }

    fn create_test_tenant() -> TenantContext {
        TenantContext::new(TenantId::new("test-tenant"), TenantPermissions::full_access())
    }

    #[tokio::test]
    async fn test_create_and_read() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        let resource = json!({
            "resourceType": "Patient",
            "name": [{"family": "Test", "given": ["User"]}]
        });

        // Create
        let created = backend.create(&tenant, "Patient", resource).await.unwrap();
        assert_eq!(created.resource_type(), "Patient");
        assert_eq!(created.version_id(), "1");

        // Read
        let read = backend.read(&tenant, "Patient", created.id()).await.unwrap();
        assert!(read.is_some());
        let read = read.unwrap();
        assert_eq!(read.version_id(), "1");
    }

    #[tokio::test]
    async fn test_create_with_id() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        let resource = json!({
            "resourceType": "Patient",
            "id": "patient-123",
            "name": [{"family": "Test"}]
        });

        let created = backend.create(&tenant, "Patient", resource).await.unwrap();
        assert_eq!(created.id(), "patient-123");
    }

    #[tokio::test]
    async fn test_create_duplicate_fails() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        let resource = json!({"id": "patient-1"});
        backend.create(&tenant, "Patient", resource.clone()).await.unwrap();

        let result = backend.create(&tenant, "Patient", resource).await;
        assert!(matches!(
            result,
            Err(StorageError::Resource(ResourceError::AlreadyExists { .. }))
        ));
    }

    #[tokio::test]
    async fn test_read_nonexistent() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        let result = backend.read(&tenant, "Patient", "nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_update() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create
        let resource = json!({"name": [{"family": "Original"}]});
        let created = backend.create(&tenant, "Patient", resource).await.unwrap();

        // Update
        let updated_content = json!({"name": [{"family": "Updated"}]});
        let updated = backend.update(&tenant, &created, updated_content).await.unwrap();
        assert_eq!(updated.version_id(), "2");

        // Verify
        let read = backend.read(&tenant, "Patient", created.id()).await.unwrap().unwrap();
        assert_eq!(read.content()["name"][0]["family"], "Updated");
    }

    #[tokio::test]
    async fn test_update_version_conflict() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create
        let resource = json!({});
        let created = backend.create(&tenant, "Patient", resource).await.unwrap();

        // Update once
        let _ = backend.update(&tenant, &created, json!({})).await.unwrap();

        // Try to update with stale version
        let result = backend.update(&tenant, &created, json!({})).await;
        assert!(matches!(
            result,
            Err(StorageError::Concurrency(ConcurrencyError::VersionConflict { .. }))
        ));
    }

    #[tokio::test]
    async fn test_delete() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create
        let resource = json!({});
        let created = backend.create(&tenant, "Patient", resource).await.unwrap();

        // Delete
        backend.delete(&tenant, "Patient", created.id()).await.unwrap();

        // Read should return Gone
        let result = backend.read(&tenant, "Patient", created.id()).await;
        assert!(matches!(
            result,
            Err(StorageError::Resource(ResourceError::Gone { .. }))
        ));
    }

    #[tokio::test]
    async fn test_create_or_update_new() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        let (resource, created) = backend
            .create_or_update(&tenant, "Patient", "new-id", json!({}))
            .await
            .unwrap();

        assert!(created);
        assert_eq!(resource.id(), "new-id");
        assert_eq!(resource.version_id(), "1");
    }

    #[tokio::test]
    async fn test_create_or_update_existing() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create first
        backend.create(&tenant, "Patient", json!({"id": "existing-id"})).await.unwrap();

        // Update via create_or_update
        let (resource, created) = backend
            .create_or_update(&tenant, "Patient", "existing-id", json!({}))
            .await
            .unwrap();

        assert!(!created);
        assert_eq!(resource.version_id(), "2");
    }

    #[tokio::test]
    async fn test_count() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Initially empty
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 0);

        // Create some resources
        backend.create(&tenant, "Patient", json!({})).await.unwrap();
        backend.create(&tenant, "Patient", json!({})).await.unwrap();
        backend.create(&tenant, "Observation", json!({})).await.unwrap();

        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 2);
        assert_eq!(backend.count(&tenant, Some("Observation")).await.unwrap(), 1);
        assert_eq!(backend.count(&tenant, None).await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_tenant_isolation() {
        let backend = create_test_backend();

        let tenant1 = TenantContext::new(TenantId::new("tenant-1"), TenantPermissions::full_access());
        let tenant2 = TenantContext::new(TenantId::new("tenant-2"), TenantPermissions::full_access());

        // Create in tenant 1
        let resource = json!({"id": "patient-1"});
        backend.create(&tenant1, "Patient", resource).await.unwrap();

        // Tenant 1 can read
        assert!(backend.read(&tenant1, "Patient", "patient-1").await.unwrap().is_some());

        // Tenant 2 cannot read
        assert!(backend.read(&tenant2, "Patient", "patient-1").await.unwrap().is_none());
    }
}
