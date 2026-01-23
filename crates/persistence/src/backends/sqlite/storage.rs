//! ResourceStorage and VersionedStorage implementations for SQLite.

use async_trait::async_trait;
use chrono::Utc;
use rusqlite::params;
use serde_json::Value;

use crate::core::history::{HistoryEntry, HistoryMethod, HistoryPage, HistoryParams, InstanceHistoryProvider, SystemHistoryProvider, TypeHistoryProvider};
use crate::core::{ResourceStorage, VersionedStorage};
use crate::error::{BackendError, ConcurrencyError, ResourceError, StorageError, StorageResult};
use crate::tenant::TenantContext;
use crate::types::{CursorValue, Page, PageCursor, PageInfo, StoredResource};

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

#[async_trait]
impl InstanceHistoryProvider for SqliteBackend {
    async fn history_instance(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        params: &HistoryParams,
    ) -> StorageResult<HistoryPage> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        // Build the query with filters
        let mut sql = String::from(
            "SELECT version_id, data, last_updated, is_deleted
             FROM resource_history
             WHERE tenant_id = ?1 AND resource_type = ?2 AND id = ?3"
        );

        // Apply deleted filter
        if !params.include_deleted {
            sql.push_str(" AND is_deleted = 0");
        }

        // Apply since filter
        if let Some(since) = &params.since {
            sql.push_str(&format!(" AND last_updated >= '{}'", since.to_rfc3339()));
        }

        // Apply before filter
        if let Some(before) = &params.before {
            sql.push_str(&format!(" AND last_updated < '{}'", before.to_rfc3339()));
        }

        // Apply cursor filter if present
        if let Some(cursor) = params.pagination.cursor_value() {
            // Cursor contains version_id for history pagination
            if let Some(CursorValue::String(version_str)) = cursor.sort_values().first() {
                // For reverse chronological order, get versions less than cursor
                sql.push_str(&format!(
                    " AND CAST(version_id AS INTEGER) < {}",
                    version_str.parse::<i64>().unwrap_or(i64::MAX)
                ));
            }
        }

        // Order by version descending (newest first) and limit
        sql.push_str(" ORDER BY CAST(version_id AS INTEGER) DESC");
        sql.push_str(&format!(" LIMIT {}", params.pagination.count + 1)); // +1 to detect if there are more

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| internal_error(format!("Failed to prepare history query: {}", e)))?;

        let rows = stmt
            .query_map(params![tenant_id, resource_type, id], |row| {
                let version_id: String = row.get(0)?;
                let data: Vec<u8> = row.get(1)?;
                let last_updated: String = row.get(2)?;
                let is_deleted: i32 = row.get(3)?;
                Ok((version_id, data, last_updated, is_deleted))
            })
            .map_err(|e| internal_error(format!("Failed to query history: {}", e)))?;

        let mut entries = Vec::new();
        let mut last_version: Option<String> = None;

        for row in rows {
            let (version_id, data, last_updated_str, is_deleted) =
                row.map_err(|e| internal_error(format!("Failed to read history row: {}", e)))?;

            // Stop if we've collected enough items (we fetched count+1 to detect more)
            if entries.len() >= params.pagination.count as usize {
                break;
            }

            let json_data: serde_json::Value = serde_json::from_slice(&data)
                .map_err(|e| serialization_error(format!("Failed to deserialize resource: {}", e)))?;

            let last_updated = chrono::DateTime::parse_from_rfc3339(&last_updated_str)
                .map_err(|e| internal_error(format!("Failed to parse last_updated: {}", e)))?
                .with_timezone(&Utc);

            let deleted_at = if is_deleted != 0 {
                Some(last_updated)
            } else {
                None
            };

            let resource = StoredResource::from_storage(
                resource_type,
                id,
                &version_id,
                tenant.tenant_id().clone(),
                json_data,
                last_updated,
                last_updated,
                deleted_at,
            );

            // Determine the method based on version and deletion status
            let method = if is_deleted != 0 {
                HistoryMethod::Delete
            } else if version_id == "1" {
                HistoryMethod::Post
            } else {
                HistoryMethod::Put
            };

            last_version = Some(version_id);

            entries.push(HistoryEntry {
                resource,
                method,
                timestamp: last_updated,
            });
        }

        // Determine if there are more results
        let has_more = stmt
            .query_map(params![tenant_id, resource_type, id], |_| Ok(()))
            .map_err(|e| internal_error(format!("Failed to check for more results: {}", e)))?
            .count()
            > params.pagination.count as usize;

        // Build page info
        let page_info = if has_more && last_version.is_some() {
            let cursor = PageCursor::new(
                vec![CursorValue::String(last_version.unwrap())],
                id.to_string(),
            );
            PageInfo::with_next(cursor)
        } else {
            PageInfo::end()
        };

        Ok(Page::new(entries, page_info))
    }

    async fn history_instance_count(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<u64> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_history
                 WHERE tenant_id = ?1 AND resource_type = ?2 AND id = ?3",
                params![tenant_id, resource_type, id],
                |row| row.get(0),
            )
            .map_err(|e| internal_error(format!("Failed to count history: {}", e)))?;

        Ok(count as u64)
    }
}

#[async_trait]
impl TypeHistoryProvider for SqliteBackend {
    async fn history_type(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        params: &HistoryParams,
    ) -> StorageResult<HistoryPage> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        // Build the query with filters
        let mut sql = String::from(
            "SELECT id, version_id, data, last_updated, is_deleted
             FROM resource_history
             WHERE tenant_id = ?1 AND resource_type = ?2"
        );

        // Apply deleted filter
        if !params.include_deleted {
            sql.push_str(" AND is_deleted = 0");
        }

        // Apply since filter
        if let Some(since) = &params.since {
            sql.push_str(&format!(" AND last_updated >= '{}'", since.to_rfc3339()));
        }

        // Apply before filter
        if let Some(before) = &params.before {
            sql.push_str(&format!(" AND last_updated < '{}'", before.to_rfc3339()));
        }

        // Apply cursor filter if present
        // For type history, cursor contains (last_updated, id, version_id) for proper ordering
        if let Some(cursor) = params.pagination.cursor_value() {
            let sort_values = cursor.sort_values();
            if sort_values.len() >= 2 {
                if let (Some(CursorValue::String(timestamp)), Some(CursorValue::String(resource_id))) =
                    (sort_values.first(), sort_values.get(1))
                {
                    // For reverse chronological order, get entries older than cursor
                    sql.push_str(&format!(
                        " AND (last_updated < '{}' OR (last_updated = '{}' AND id < '{}'))",
                        timestamp, timestamp, resource_id
                    ));
                }
            }
        }

        // Order by last_updated descending (newest first), then by id for consistency
        sql.push_str(" ORDER BY last_updated DESC, id DESC, CAST(version_id AS INTEGER) DESC");
        sql.push_str(&format!(" LIMIT {}", params.pagination.count + 1)); // +1 to detect if there are more

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| internal_error(format!("Failed to prepare type history query: {}", e)))?;

        let rows = stmt
            .query_map(params![tenant_id, resource_type], |row| {
                let id: String = row.get(0)?;
                let version_id: String = row.get(1)?;
                let data: Vec<u8> = row.get(2)?;
                let last_updated: String = row.get(3)?;
                let is_deleted: i32 = row.get(4)?;
                Ok((id, version_id, data, last_updated, is_deleted))
            })
            .map_err(|e| internal_error(format!("Failed to query type history: {}", e)))?;

        let mut entries = Vec::new();
        let mut last_entry: Option<(String, String)> = None; // (last_updated, id)

        for row in rows {
            let (id, version_id, data, last_updated_str, is_deleted) =
                row.map_err(|e| internal_error(format!("Failed to read type history row: {}", e)))?;

            // Stop if we've collected enough items (we fetched count+1 to detect more)
            if entries.len() >= params.pagination.count as usize {
                break;
            }

            let json_data: serde_json::Value = serde_json::from_slice(&data)
                .map_err(|e| serialization_error(format!("Failed to deserialize resource: {}", e)))?;

            let last_updated = chrono::DateTime::parse_from_rfc3339(&last_updated_str)
                .map_err(|e| internal_error(format!("Failed to parse last_updated: {}", e)))?
                .with_timezone(&Utc);

            let deleted_at = if is_deleted != 0 {
                Some(last_updated)
            } else {
                None
            };

            let resource = StoredResource::from_storage(
                resource_type,
                &id,
                &version_id,
                tenant.tenant_id().clone(),
                json_data,
                last_updated,
                last_updated,
                deleted_at,
            );

            // Determine the method based on version and deletion status
            let method = if is_deleted != 0 {
                HistoryMethod::Delete
            } else if version_id == "1" {
                HistoryMethod::Post
            } else {
                HistoryMethod::Put
            };

            last_entry = Some((last_updated_str.clone(), id));

            entries.push(HistoryEntry {
                resource,
                method,
                timestamp: last_updated,
            });
        }

        // Check if there are more results by seeing if we got more than count
        let total_fetched = entries.len();
        let has_more = {
            // Re-run query to check if there are more
            let check_sql = sql.replace(&format!(" LIMIT {}", params.pagination.count + 1), &format!(" LIMIT {}", params.pagination.count + 2));
            let mut check_stmt = conn
                .prepare(&check_sql)
                .map_err(|e| internal_error(format!("Failed to prepare check query: {}", e)))?;
            let check_count = check_stmt
                .query_map(params![tenant_id, resource_type], |_| Ok(()))
                .map_err(|e| internal_error(format!("Failed to check for more results: {}", e)))?
                .count();
            check_count > params.pagination.count as usize
        };

        // Build page info
        let page_info = if has_more && last_entry.is_some() {
            let (timestamp, id) = last_entry.unwrap();
            let cursor = PageCursor::new(
                vec![
                    CursorValue::String(timestamp),
                    CursorValue::String(id),
                ],
                resource_type.to_string(),
            );
            PageInfo::with_next(cursor)
        } else {
            PageInfo::end()
        };

        Ok(Page::new(entries, page_info))
    }

    async fn history_type_count(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
    ) -> StorageResult<u64> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_history
                 WHERE tenant_id = ?1 AND resource_type = ?2",
                params![tenant_id, resource_type],
                |row| row.get(0),
            )
            .map_err(|e| internal_error(format!("Failed to count type history: {}", e)))?;

        Ok(count as u64)
    }
}

#[async_trait]
impl SystemHistoryProvider for SqliteBackend {
    async fn history_system(
        &self,
        tenant: &TenantContext,
        params: &HistoryParams,
    ) -> StorageResult<HistoryPage> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        // Build the query with filters
        let mut sql = String::from(
            "SELECT resource_type, id, version_id, data, last_updated, is_deleted
             FROM resource_history
             WHERE tenant_id = ?1"
        );

        // Apply deleted filter
        if !params.include_deleted {
            sql.push_str(" AND is_deleted = 0");
        }

        // Apply since filter
        if let Some(since) = &params.since {
            sql.push_str(&format!(" AND last_updated >= '{}'", since.to_rfc3339()));
        }

        // Apply before filter
        if let Some(before) = &params.before {
            sql.push_str(&format!(" AND last_updated < '{}'", before.to_rfc3339()));
        }

        // Apply cursor filter if present
        // For system history, cursor contains (last_updated, resource_type, id) for proper ordering
        if let Some(cursor) = params.pagination.cursor_value() {
            let sort_values = cursor.sort_values();
            if sort_values.len() >= 3 {
                if let (
                    Some(CursorValue::String(timestamp)),
                    Some(CursorValue::String(res_type)),
                    Some(CursorValue::String(res_id)),
                ) = (sort_values.first(), sort_values.get(1), sort_values.get(2))
                {
                    // For reverse chronological order, get entries older than cursor
                    sql.push_str(&format!(
                        " AND (last_updated < '{}' OR (last_updated = '{}' AND (resource_type < '{}' OR (resource_type = '{}' AND id < '{}'))))",
                        timestamp, timestamp, res_type, res_type, res_id
                    ));
                }
            }
        }

        // Order by last_updated descending (newest first), then by resource_type and id for consistency
        sql.push_str(" ORDER BY last_updated DESC, resource_type DESC, id DESC, CAST(version_id AS INTEGER) DESC");
        sql.push_str(&format!(" LIMIT {}", params.pagination.count + 1)); // +1 to detect if there are more

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| internal_error(format!("Failed to prepare system history query: {}", e)))?;

        let rows = stmt
            .query_map(params![tenant_id], |row| {
                let resource_type: String = row.get(0)?;
                let id: String = row.get(1)?;
                let version_id: String = row.get(2)?;
                let data: Vec<u8> = row.get(3)?;
                let last_updated: String = row.get(4)?;
                let is_deleted: i32 = row.get(5)?;
                Ok((resource_type, id, version_id, data, last_updated, is_deleted))
            })
            .map_err(|e| internal_error(format!("Failed to query system history: {}", e)))?;

        let mut entries = Vec::new();
        let mut last_entry: Option<(String, String, String)> = None; // (last_updated, resource_type, id)

        for row in rows {
            let (resource_type, id, version_id, data, last_updated_str, is_deleted) =
                row.map_err(|e| internal_error(format!("Failed to read system history row: {}", e)))?;

            // Stop if we've collected enough items (we fetched count+1 to detect more)
            if entries.len() >= params.pagination.count as usize {
                break;
            }

            let json_data: serde_json::Value = serde_json::from_slice(&data)
                .map_err(|e| serialization_error(format!("Failed to deserialize resource: {}", e)))?;

            let last_updated = chrono::DateTime::parse_from_rfc3339(&last_updated_str)
                .map_err(|e| internal_error(format!("Failed to parse last_updated: {}", e)))?
                .with_timezone(&Utc);

            let deleted_at = if is_deleted != 0 {
                Some(last_updated)
            } else {
                None
            };

            let resource = StoredResource::from_storage(
                &resource_type,
                &id,
                &version_id,
                tenant.tenant_id().clone(),
                json_data,
                last_updated,
                last_updated,
                deleted_at,
            );

            // Determine the method based on version and deletion status
            let method = if is_deleted != 0 {
                HistoryMethod::Delete
            } else if version_id == "1" {
                HistoryMethod::Post
            } else {
                HistoryMethod::Put
            };

            last_entry = Some((last_updated_str.clone(), resource_type, id));

            entries.push(HistoryEntry {
                resource,
                method,
                timestamp: last_updated,
            });
        }

        // Check if there are more results
        let has_more = {
            let check_sql = sql.replace(
                &format!(" LIMIT {}", params.pagination.count + 1),
                &format!(" LIMIT {}", params.pagination.count + 2),
            );
            let mut check_stmt = conn
                .prepare(&check_sql)
                .map_err(|e| internal_error(format!("Failed to prepare check query: {}", e)))?;
            let check_count = check_stmt
                .query_map(params![tenant_id], |_| Ok(()))
                .map_err(|e| internal_error(format!("Failed to check for more results: {}", e)))?
                .count();
            check_count > params.pagination.count as usize
        };

        // Build page info
        let page_info = if has_more && last_entry.is_some() {
            let (timestamp, resource_type, id) = last_entry.unwrap();
            let cursor = PageCursor::new(
                vec![
                    CursorValue::String(timestamp),
                    CursorValue::String(resource_type),
                    CursorValue::String(id),
                ],
                "system".to_string(),
            );
            PageInfo::with_next(cursor)
        } else {
            PageInfo::end()
        };

        Ok(Page::new(entries, page_info))
    }

    async fn history_system_count(
        &self,
        tenant: &TenantContext,
    ) -> StorageResult<u64> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_history WHERE tenant_id = ?1",
                params![tenant_id],
                |row| row.get(0),
            )
            .map_err(|e| internal_error(format!("Failed to count system history: {}", e)))?;

        Ok(count as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::history::HistoryParams;
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

    // ========================================================================
    // History Tests
    // ========================================================================

    #[tokio::test]
    async fn test_history_instance_basic() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create a resource
        let resource = json!({"name": [{"family": "Smith"}]});
        let created = backend.create(&tenant, "Patient", resource).await.unwrap();

        // Update it twice
        let v2 = backend.update(&tenant, &created, json!({"name": [{"family": "Jones"}]})).await.unwrap();
        let _v3 = backend.update(&tenant, &v2, json!({"name": [{"family": "Brown"}]})).await.unwrap();

        // Get history
        let params = HistoryParams::new();
        let history = backend.history_instance(&tenant, "Patient", created.id(), &params).await.unwrap();

        // Should have 3 versions, newest first
        assert_eq!(history.items.len(), 3);
        assert_eq!(history.items[0].resource.version_id(), "3");
        assert_eq!(history.items[1].resource.version_id(), "2");
        assert_eq!(history.items[2].resource.version_id(), "1");

        // Check methods
        assert_eq!(history.items[0].method, HistoryMethod::Put);
        assert_eq!(history.items[1].method, HistoryMethod::Put);
        assert_eq!(history.items[2].method, HistoryMethod::Post);
    }

    #[tokio::test]
    async fn test_history_instance_count() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create and update
        let resource = json!({});
        let created = backend.create(&tenant, "Patient", resource).await.unwrap();
        let v2 = backend.update(&tenant, &created, json!({})).await.unwrap();
        let _v3 = backend.update(&tenant, &v2, json!({})).await.unwrap();

        let count = backend.history_instance_count(&tenant, "Patient", created.id()).await.unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_history_instance_with_delete() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create, update, then delete
        let resource = json!({"id": "p1"});
        let created = backend.create(&tenant, "Patient", resource).await.unwrap();
        let _v2 = backend.update(&tenant, &created, json!({"id": "p1"})).await.unwrap();
        backend.delete(&tenant, "Patient", "p1").await.unwrap();

        // Get history including deleted
        let params = HistoryParams::new().include_deleted(true);
        let history = backend.history_instance(&tenant, "Patient", "p1", &params).await.unwrap();

        assert_eq!(history.items.len(), 3);
        assert_eq!(history.items[0].method, HistoryMethod::Delete);
        assert_eq!(history.items[0].resource.version_id(), "3");
    }

    #[tokio::test]
    async fn test_history_instance_exclude_deleted() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create, update, then delete
        let resource = json!({"id": "p2"});
        let created = backend.create(&tenant, "Patient", resource).await.unwrap();
        let _v2 = backend.update(&tenant, &created, json!({"id": "p2"})).await.unwrap();
        backend.delete(&tenant, "Patient", "p2").await.unwrap();

        // Get history excluding deleted
        let params = HistoryParams::new().include_deleted(false);
        let history = backend.history_instance(&tenant, "Patient", "p2", &params).await.unwrap();

        // Should not include the delete version
        assert_eq!(history.items.len(), 2);
        assert_eq!(history.items[0].resource.version_id(), "2");
        assert_eq!(history.items[1].resource.version_id(), "1");
    }

    #[tokio::test]
    async fn test_history_instance_pagination() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create with multiple versions
        let resource = json!({});
        let mut current = backend.create(&tenant, "Patient", resource).await.unwrap();
        for _ in 0..4 {
            current = backend.update(&tenant, &current, json!({})).await.unwrap();
        }
        // Now have 5 versions

        // Get first page (2 items)
        let params = HistoryParams::new().count(2);
        let page1 = backend.history_instance(&tenant, "Patient", current.id(), &params).await.unwrap();

        assert_eq!(page1.items.len(), 2);
        assert_eq!(page1.items[0].resource.version_id(), "5");
        assert_eq!(page1.items[1].resource.version_id(), "4");
        assert!(page1.page_info.has_next);
    }

    #[tokio::test]
    async fn test_history_instance_nonexistent() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        let params = HistoryParams::new();
        let history = backend.history_instance(&tenant, "Patient", "nonexistent", &params).await.unwrap();

        assert!(history.items.is_empty());
    }

    #[tokio::test]
    async fn test_history_instance_tenant_isolation() {
        let backend = create_test_backend();
        let tenant1 = TenantContext::new(TenantId::new("tenant-1"), TenantPermissions::full_access());
        let tenant2 = TenantContext::new(TenantId::new("tenant-2"), TenantPermissions::full_access());

        // Create in tenant 1
        let resource = json!({"id": "shared-id"});
        let created = backend.create(&tenant1, "Patient", resource).await.unwrap();
        let _v2 = backend.update(&tenant1, &created, json!({"id": "shared-id"})).await.unwrap();

        // Tenant 1 sees history
        let history1 = backend.history_instance(&tenant1, "Patient", "shared-id", &HistoryParams::new()).await.unwrap();
        assert_eq!(history1.items.len(), 2);

        // Tenant 2 sees nothing
        let history2 = backend.history_instance(&tenant2, "Patient", "shared-id", &HistoryParams::new()).await.unwrap();
        assert!(history2.items.is_empty());
    }

    // ========================================================================
    // Type History Tests
    // ========================================================================

    #[tokio::test]
    async fn test_history_type_basic() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create multiple patients
        let p1 = backend.create(&tenant, "Patient", json!({"id": "p1"})).await.unwrap();
        let p2 = backend.create(&tenant, "Patient", json!({"id": "p2"})).await.unwrap();

        // Update p1
        let _p1_v2 = backend.update(&tenant, &p1, json!({"id": "p1"})).await.unwrap();

        // Get type history
        let params = HistoryParams::new();
        let history = backend.history_type(&tenant, "Patient", &params).await.unwrap();

        // Should have 3 entries total (p1 v1, p1 v2, p2 v1)
        assert_eq!(history.items.len(), 3);

        // All should be Patient type
        for entry in &history.items {
            assert_eq!(entry.resource.resource_type(), "Patient");
        }
    }

    #[tokio::test]
    async fn test_history_type_count() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create multiple patients with updates
        let p1 = backend.create(&tenant, "Patient", json!({})).await.unwrap();
        let _p1_v2 = backend.update(&tenant, &p1, json!({})).await.unwrap();
        let _p2 = backend.create(&tenant, "Patient", json!({})).await.unwrap();

        // Create an observation (different type)
        backend.create(&tenant, "Observation", json!({})).await.unwrap();

        // Count patient history
        let count = backend.history_type_count(&tenant, "Patient").await.unwrap();
        assert_eq!(count, 3); // p1 v1, p1 v2, p2 v1

        // Count observation history
        let obs_count = backend.history_type_count(&tenant, "Observation").await.unwrap();
        assert_eq!(obs_count, 1);
    }

    #[tokio::test]
    async fn test_history_type_filters_by_type() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create different resource types
        backend.create(&tenant, "Patient", json!({})).await.unwrap();
        backend.create(&tenant, "Observation", json!({})).await.unwrap();
        backend.create(&tenant, "Encounter", json!({})).await.unwrap();

        // Get only Patient history
        let history = backend.history_type(&tenant, "Patient", &HistoryParams::new()).await.unwrap();
        assert_eq!(history.items.len(), 1);
        assert_eq!(history.items[0].resource.resource_type(), "Patient");

        // Get only Observation history
        let obs_history = backend.history_type(&tenant, "Observation", &HistoryParams::new()).await.unwrap();
        assert_eq!(obs_history.items.len(), 1);
        assert_eq!(obs_history.items[0].resource.resource_type(), "Observation");
    }

    #[tokio::test]
    async fn test_history_type_includes_deleted() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create and delete a patient
        let p1 = backend.create(&tenant, "Patient", json!({"id": "del-p1"})).await.unwrap();
        backend.delete(&tenant, "Patient", "del-p1").await.unwrap();

        // Create another patient
        backend.create(&tenant, "Patient", json!({"id": "p2"})).await.unwrap();

        // Without including deleted
        let history = backend.history_type(&tenant, "Patient", &HistoryParams::new()).await.unwrap();
        assert_eq!(history.items.len(), 2); // p1 v1, p2 v1 (excludes delete)

        // Including deleted
        let history_with_deleted = backend.history_type(&tenant, "Patient", &HistoryParams::new().include_deleted(true)).await.unwrap();
        assert_eq!(history_with_deleted.items.len(), 3); // p1 v1, p1 delete, p2 v1
    }

    #[tokio::test]
    async fn test_history_type_tenant_isolation() {
        let backend = create_test_backend();
        let tenant1 = TenantContext::new(TenantId::new("tenant-1"), TenantPermissions::full_access());
        let tenant2 = TenantContext::new(TenantId::new("tenant-2"), TenantPermissions::full_access());

        // Create patients in tenant 1
        backend.create(&tenant1, "Patient", json!({})).await.unwrap();
        backend.create(&tenant1, "Patient", json!({})).await.unwrap();

        // Create patient in tenant 2
        backend.create(&tenant2, "Patient", json!({})).await.unwrap();

        // Tenant 1 sees only its history
        let history1 = backend.history_type(&tenant1, "Patient", &HistoryParams::new()).await.unwrap();
        assert_eq!(history1.items.len(), 2);

        // Tenant 2 sees only its history
        let history2 = backend.history_type(&tenant2, "Patient", &HistoryParams::new()).await.unwrap();
        assert_eq!(history2.items.len(), 1);
    }

    #[tokio::test]
    async fn test_history_type_pagination() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create several patients
        for i in 0..5 {
            backend.create(&tenant, "Patient", json!({"id": format!("p{}", i)})).await.unwrap();
        }

        // Get first page (2 items)
        let params = HistoryParams::new().count(2);
        let page1 = backend.history_type(&tenant, "Patient", &params).await.unwrap();

        assert_eq!(page1.items.len(), 2);
        assert!(page1.page_info.has_next);
    }

    #[tokio::test]
    async fn test_history_type_empty() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // No resources created
        let history = backend.history_type(&tenant, "Patient", &HistoryParams::new()).await.unwrap();
        assert!(history.items.is_empty());
        assert!(!history.page_info.has_next);
    }

    // ========================================================================
    // System History Tests
    // ========================================================================

    #[tokio::test]
    async fn test_history_system_basic() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create different resource types
        let p1 = backend.create(&tenant, "Patient", json!({"id": "p1"})).await.unwrap();
        backend.create(&tenant, "Observation", json!({"id": "o1"})).await.unwrap();
        backend.create(&tenant, "Encounter", json!({"id": "e1"})).await.unwrap();

        // Update patient
        let _p1_v2 = backend.update(&tenant, &p1, json!({"id": "p1"})).await.unwrap();

        // Get system history
        let history = backend.history_system(&tenant, &HistoryParams::new()).await.unwrap();

        // Should have 4 entries total
        assert_eq!(history.items.len(), 4);

        // Should include all resource types
        let types: std::collections::HashSet<_> = history.items.iter()
            .map(|e| e.resource.resource_type())
            .collect();
        assert!(types.contains("Patient"));
        assert!(types.contains("Observation"));
        assert!(types.contains("Encounter"));
    }

    #[tokio::test]
    async fn test_history_system_count() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create different resource types
        let p1 = backend.create(&tenant, "Patient", json!({})).await.unwrap();
        let _p1_v2 = backend.update(&tenant, &p1, json!({})).await.unwrap();
        backend.create(&tenant, "Observation", json!({})).await.unwrap();
        backend.create(&tenant, "Encounter", json!({})).await.unwrap();

        // Count all history
        let count = backend.history_system_count(&tenant).await.unwrap();
        assert_eq!(count, 4); // p1 v1, p1 v2, o1, e1
    }

    #[tokio::test]
    async fn test_history_system_includes_deleted() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create and delete a patient
        backend.create(&tenant, "Patient", json!({"id": "del-p1"})).await.unwrap();
        backend.delete(&tenant, "Patient", "del-p1").await.unwrap();

        // Create another resource
        backend.create(&tenant, "Observation", json!({})).await.unwrap();

        // Without including deleted
        let history = backend.history_system(&tenant, &HistoryParams::new()).await.unwrap();
        assert_eq!(history.items.len(), 2); // p1 v1, obs (excludes delete)

        // Including deleted
        let history_with_deleted = backend.history_system(&tenant, &HistoryParams::new().include_deleted(true)).await.unwrap();
        assert_eq!(history_with_deleted.items.len(), 3); // p1 v1, p1 delete, obs
    }

    #[tokio::test]
    async fn test_history_system_tenant_isolation() {
        let backend = create_test_backend();
        let tenant1 = TenantContext::new(TenantId::new("tenant-1"), TenantPermissions::full_access());
        let tenant2 = TenantContext::new(TenantId::new("tenant-2"), TenantPermissions::full_access());

        // Create resources in tenant 1
        backend.create(&tenant1, "Patient", json!({})).await.unwrap();
        backend.create(&tenant1, "Observation", json!({})).await.unwrap();

        // Create resource in tenant 2
        backend.create(&tenant2, "Encounter", json!({})).await.unwrap();

        // Tenant 1 sees only its history
        let history1 = backend.history_system(&tenant1, &HistoryParams::new()).await.unwrap();
        assert_eq!(history1.items.len(), 2);

        // Tenant 2 sees only its history
        let history2 = backend.history_system(&tenant2, &HistoryParams::new()).await.unwrap();
        assert_eq!(history2.items.len(), 1);

        // Counts should also be isolated
        assert_eq!(backend.history_system_count(&tenant1).await.unwrap(), 2);
        assert_eq!(backend.history_system_count(&tenant2).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_history_system_pagination() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create several resources of different types
        for i in 0..3 {
            backend.create(&tenant, "Patient", json!({"id": format!("p{}", i)})).await.unwrap();
        }
        for i in 0..2 {
            backend.create(&tenant, "Observation", json!({"id": format!("o{}", i)})).await.unwrap();
        }
        // Total: 5 entries

        // Get first page (2 items)
        let params = HistoryParams::new().count(2);
        let page1 = backend.history_system(&tenant, &params).await.unwrap();

        assert_eq!(page1.items.len(), 2);
        assert!(page1.page_info.has_next);
    }

    #[tokio::test]
    async fn test_history_system_empty() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // No resources created
        let history = backend.history_system(&tenant, &HistoryParams::new()).await.unwrap();
        assert!(history.items.is_empty());
        assert!(!history.page_info.has_next);

        assert_eq!(backend.history_system_count(&tenant).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_history_system_ordered_by_time() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create resources - they should be ordered by last_updated DESC
        backend.create(&tenant, "Patient", json!({"id": "first"})).await.unwrap();
        backend.create(&tenant, "Observation", json!({"id": "second"})).await.unwrap();
        backend.create(&tenant, "Encounter", json!({"id": "third"})).await.unwrap();

        let history = backend.history_system(&tenant, &HistoryParams::new()).await.unwrap();

        // Should be in reverse chronological order (newest first)
        assert_eq!(history.items.len(), 3);
        // The last created should be first in the list
        assert_eq!(history.items[0].resource.id(), "third");
        assert_eq!(history.items[1].resource.id(), "second");
        assert_eq!(history.items[2].resource.id(), "first");
    }
}
