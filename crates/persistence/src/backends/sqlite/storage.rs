//! ResourceStorage and VersionedStorage implementations for SQLite.

use async_trait::async_trait;
use chrono::Utc;
use rusqlite::{params, ToSql};
use serde_json::Value;

use crate::core::history::{
    DifferentialHistoryProvider, HistoryEntry, HistoryMethod, HistoryPage, HistoryParams,
    InstanceHistoryProvider, SystemHistoryProvider, TypeHistoryProvider,
};
use crate::core::transaction::{
    BundleEntry, BundleEntryResult, BundleMethod, BundleProvider, BundleResult, BundleType,
};
use crate::core::{
    ConditionalCreateResult, ConditionalDeleteResult, ConditionalStorage, ConditionalUpdateResult,
    PurgableStorage, ResourceStorage, VersionedStorage,
};
use crate::error::TransactionError;
use crate::types::Pagination;
use crate::error::{BackendError, ConcurrencyError, ResourceError, StorageError, StorageResult};
use crate::search::extractor::ExtractedValue;
use crate::search::reindex::{ReindexableStorage, ResourcePage};
use crate::tenant::TenantContext;
use crate::types::{CursorValue, Page, PageCursor, PageInfo, StoredResource};

use super::search::writer::SqliteSearchIndexWriter;
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
            obj.insert(
                "resourceType".to_string(),
                Value::String(resource_type.to_string()),
            );
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

        // Index the resource for search
        self.index_resource(&conn, tenant_id, resource_type, &id, &resource)?;

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

                let json_data: serde_json::Value = serde_json::from_slice(&data).map_err(|e| {
                    serialization_error(format!("Failed to deserialize resource: {}", e))
                })?;

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
                return Err(internal_error(format!(
                    "Failed to get current version: {}",
                    e
                )));
            }
        };

        // Check version match
        if actual_version != current.version_id() {
            return Err(StorageError::Concurrency(
                ConcurrencyError::VersionConflict {
                    resource_type: resource_type.to_string(),
                    id: id.to_string(),
                    expected_version: current.version_id().to_string(),
                    actual_version,
                },
            ));
        }

        // Calculate new version
        let new_version: u64 = actual_version.parse().unwrap_or(0) + 1;
        let new_version_str = new_version.to_string();

        // Ensure the resource has correct type and id
        let mut resource = resource;
        if let Some(obj) = resource.as_object_mut() {
            obj.insert(
                "resourceType".to_string(),
                Value::String(resource_type.to_string()),
            );
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
            params![
                new_version_str,
                data,
                last_updated,
                tenant_id,
                resource_type,
                id
            ],
        )
        .map_err(|e| internal_error(format!("Failed to update resource: {}", e)))?;

        // Insert into history
        conn.execute(
            "INSERT INTO resource_history (tenant_id, resource_type, id, version_id, data, last_updated, is_deleted)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0)",
            params![tenant_id, resource_type, id, new_version_str, data, last_updated],
        )
        .map_err(|e| internal_error(format!("Failed to insert history: {}", e)))?;

        // Re-index the resource (delete old entries, add new)
        self.delete_search_index(&conn, tenant_id, resource_type, id)?;
        self.index_resource(&conn, tenant_id, resource_type, id, &resource)?;

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

// Search Index Helpers
impl SqliteBackend {
    /// Index a resource for search.
    ///
    /// This method uses the SearchParameterExtractor to dynamically extract
    /// searchable values based on the configured SearchParameterRegistry.
    /// Falls back to hardcoded common parameter extraction if the registry
    /// extraction fails.
    pub(crate) fn index_resource(
        &self,
        conn: &rusqlite::Connection,
        tenant_id: &str,
        resource_type: &str,
        resource_id: &str,
        resource: &Value,
    ) -> StorageResult<()> {
        // Try dynamic extraction using the registry-driven extractor
        match self.index_resource_dynamic(conn, tenant_id, resource_type, resource_id, resource) {
            Ok(count) => {
                tracing::debug!(
                    "Dynamically indexed {} values for {}/{}",
                    count,
                    resource_type,
                    resource_id
                );
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    "Dynamic extraction failed for {}/{}, falling back to hardcoded: {}",
                    resource_type,
                    resource_id,
                    e
                );
                // Fall back to hardcoded extraction for common parameters
                self.index_common_params(conn, tenant_id, resource_type, resource_id, resource)
            }
        }
    }

    /// Index a resource using dynamic extraction from the SearchParameterRegistry.
    ///
    /// Returns the number of index entries created.
    fn index_resource_dynamic(
        &self,
        conn: &rusqlite::Connection,
        tenant_id: &str,
        resource_type: &str,
        resource_id: &str,
        resource: &Value,
    ) -> StorageResult<usize> {
        // Extract values using the registry-driven extractor
        let values = self
            .search_extractor()
            .extract(resource, resource_type)
            .map_err(|e| internal_error(format!("Search parameter extraction failed: {}", e)))?;

        let mut count = 0;
        for value in values {
            self.write_index_entry(conn, tenant_id, resource_type, resource_id, &value)?;
            count += 1;
        }

        Ok(count)
    }

    /// Writes a single ExtractedValue to the search_index table.
    fn write_index_entry(
        &self,
        conn: &rusqlite::Connection,
        tenant_id: &str,
        resource_type: &str,
        resource_id: &str,
        value: &ExtractedValue,
    ) -> StorageResult<()> {
        use crate::search::converters::IndexValue;

        // For date values, normalize the date format for consistent SQLite comparisons
        let normalized_value = match &value.value {
            IndexValue::Date { value: date_str, precision } => {
                let normalized_date = Self::normalize_date_for_sqlite(date_str);
                let mut normalized = value.clone();
                normalized.value = IndexValue::Date {
                    value: normalized_date,
                    precision: *precision,
                };
                Some(normalized)
            }
            _ => None,
        };

        let value_to_use = normalized_value.as_ref().unwrap_or(value);
        let sql_params = SqliteSearchIndexWriter::to_sql_params(
            tenant_id,
            resource_type,
            resource_id,
            value_to_use,
        );

        // Build parameter refs for rusqlite
        let param_refs: Vec<&dyn ToSql> = sql_params
            .iter()
            .map(|p| self.sql_value_to_ref(p))
            .collect();

        conn.execute(SqliteSearchIndexWriter::insert_sql(), param_refs.as_slice())
            .map_err(|e| internal_error(format!("Failed to insert search index entry: {}", e)))?;

        Ok(())
    }

    /// Normalizes a date string for SQLite comparisons.
    ///
    /// Ensures dates have a time component for consistent range comparisons.
    fn normalize_date_for_sqlite(value: &str) -> String {
        if value.contains('T') {
            value.to_string()
        } else if value.len() == 10 {
            // YYYY-MM-DD -> YYYY-MM-DDTHH:MM:SS
            format!("{}T00:00:00", value)
        } else if value.len() == 7 {
            // YYYY-MM -> YYYY-MM-01T00:00:00
            format!("{}-01T00:00:00", value)
        } else if value.len() == 4 {
            // YYYY -> YYYY-01-01T00:00:00
            format!("{}-01-01T00:00:00", value)
        } else {
            value.to_string()
        }
    }

    /// Converts a SqlValue to a rusqlite-compatible reference.
    fn sql_value_to_ref<'a>(&'a self, value: &'a super::search::writer::SqlValue) -> &'a dyn ToSql {
        use super::search::writer::SqlValue;
        match value {
            SqlValue::String(s) => s,
            SqlValue::OptString(opt) => opt,
            SqlValue::Int(i) => i,
            SqlValue::OptInt(opt) => opt,
            SqlValue::Float(f) => f,
            SqlValue::Null => &rusqlite::types::Null,
        }
    }

    /// Delete search index entries for a resource.
    pub(crate) fn delete_search_index(
        &self,
        conn: &rusqlite::Connection,
        tenant_id: &str,
        resource_type: &str,
        resource_id: &str,
    ) -> StorageResult<()> {
        conn.execute(
            "DELETE FROM search_index WHERE tenant_id = ?1 AND resource_type = ?2 AND resource_id = ?3",
            params![tenant_id, resource_type, resource_id],
        )
        .map_err(|e| internal_error(format!("Failed to delete search index: {}", e)))?;
        Ok(())
    }

    /// Index common search parameters that exist across most resources.
    fn index_common_params(
        &self,
        conn: &rusqlite::Connection,
        tenant_id: &str,
        resource_type: &str,
        resource_id: &str,
        resource: &Value,
    ) -> StorageResult<()> {
        // Index identifier (token)
        if let Some(identifiers) = resource.get("identifier").and_then(|v| v.as_array()) {
            for identifier in identifiers {
                let system = identifier.get("system").and_then(|v| v.as_str());
                let value = identifier.get("value").and_then(|v| v.as_str());
                if let Some(val) = value {
                    self.insert_token_index(
                        conn,
                        tenant_id,
                        resource_type,
                        resource_id,
                        "identifier",
                        system,
                        val,
                    )?;
                }
            }
        }

        // Index name for Patient, Practitioner, etc. (string)
        if let Some(names) = resource.get("name").and_then(|v| v.as_array()) {
            for name in names {
                // Family name
                if let Some(family) = name.get("family").and_then(|v| v.as_str()) {
                    self.insert_string_index(
                        conn,
                        tenant_id,
                        resource_type,
                        resource_id,
                        "family",
                        family,
                    )?;
                    self.insert_string_index(
                        conn,
                        tenant_id,
                        resource_type,
                        resource_id,
                        "name",
                        family,
                    )?;
                }
                // Given names
                if let Some(given) = name.get("given").and_then(|v| v.as_array()) {
                    for g in given {
                        if let Some(gname) = g.as_str() {
                            self.insert_string_index(
                                conn,
                                tenant_id,
                                resource_type,
                                resource_id,
                                "given",
                                gname,
                            )?;
                            self.insert_string_index(
                                conn,
                                tenant_id,
                                resource_type,
                                resource_id,
                                "name",
                                gname,
                            )?;
                        }
                    }
                }
            }
        }

        // Index code/coding (token) - common in many resources
        if let Some(code) = resource.get("code") {
            self.index_codeable_concept(
                conn,
                tenant_id,
                resource_type,
                resource_id,
                "code",
                code,
            )?;
        }

        // Index status (token)
        if let Some(status) = resource.get("status").and_then(|v| v.as_str()) {
            self.insert_token_index(
                conn,
                tenant_id,
                resource_type,
                resource_id,
                "status",
                None,
                status,
            )?;
        }

        // Index subject/patient reference
        if let Some(subject) = resource.get("subject") {
            self.index_reference(
                conn,
                tenant_id,
                resource_type,
                resource_id,
                "subject",
                subject,
            )?;
        }
        if let Some(patient) = resource.get("patient") {
            self.index_reference(
                conn,
                tenant_id,
                resource_type,
                resource_id,
                "patient",
                patient,
            )?;
        }

        // Index date fields
        if let Some(date) = resource.get("date").and_then(|v| v.as_str()) {
            self.insert_date_index(conn, tenant_id, resource_type, resource_id, "date", date)?;
        }
        if let Some(date) = resource.get("birthDate").and_then(|v| v.as_str()) {
            self.insert_date_index(
                conn,
                tenant_id,
                resource_type,
                resource_id,
                "birthdate",
                date,
            )?;
        }

        Ok(())
    }

    /// Insert a string index entry.
    fn insert_string_index(
        &self,
        conn: &rusqlite::Connection,
        tenant_id: &str,
        resource_type: &str,
        resource_id: &str,
        param_name: &str,
        value: &str,
    ) -> StorageResult<()> {
        conn.execute(
            "INSERT INTO search_index (tenant_id, resource_type, resource_id, param_name, value_string)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![tenant_id, resource_type, resource_id, param_name, value.to_lowercase()],
        )
        .map_err(|e| internal_error(format!("Failed to insert string index: {}", e)))?;
        Ok(())
    }

    /// Insert a token index entry.
    fn insert_token_index(
        &self,
        conn: &rusqlite::Connection,
        tenant_id: &str,
        resource_type: &str,
        resource_id: &str,
        param_name: &str,
        system: Option<&str>,
        code: &str,
    ) -> StorageResult<()> {
        conn.execute(
            "INSERT INTO search_index (tenant_id, resource_type, resource_id, param_name, value_token_system, value_token_code)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![tenant_id, resource_type, resource_id, param_name, system, code],
        )
        .map_err(|e| internal_error(format!("Failed to insert token index: {}", e)))?;
        Ok(())
    }

    /// Insert a date index entry.
    fn insert_date_index(
        &self,
        conn: &rusqlite::Connection,
        tenant_id: &str,
        resource_type: &str,
        resource_id: &str,
        param_name: &str,
        value: &str,
    ) -> StorageResult<()> {
        // Normalize date format: ensure we have at least YYYY-MM-DDTHH:MM:SS
        // This enables proper range comparisons in SQLite
        let normalized = if value.contains('T') {
            value.to_string()
        } else if value.len() == 10 {
            // YYYY-MM-DD -> YYYY-MM-DDTHH:MM:SS
            format!("{}T00:00:00", value)
        } else if value.len() == 7 {
            // YYYY-MM -> YYYY-MM-01T00:00:00
            format!("{}-01T00:00:00", value)
        } else if value.len() == 4 {
            // YYYY -> YYYY-01-01T00:00:00
            format!("{}-01-01T00:00:00", value)
        } else {
            value.to_string()
        };

        conn.execute(
            "INSERT INTO search_index (tenant_id, resource_type, resource_id, param_name, value_date)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![tenant_id, resource_type, resource_id, param_name, normalized],
        )
        .map_err(|e| internal_error(format!("Failed to insert date index: {}", e)))?;
        Ok(())
    }

    /// Insert a reference index entry.
    fn insert_reference_index(
        &self,
        conn: &rusqlite::Connection,
        tenant_id: &str,
        resource_type: &str,
        resource_id: &str,
        param_name: &str,
        reference: &str,
    ) -> StorageResult<()> {
        conn.execute(
            "INSERT INTO search_index (tenant_id, resource_type, resource_id, param_name, value_reference)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![tenant_id, resource_type, resource_id, param_name, reference],
        )
        .map_err(|e| internal_error(format!("Failed to insert reference index: {}", e)))?;
        Ok(())
    }

    /// Index a CodeableConcept or Coding.
    fn index_codeable_concept(
        &self,
        conn: &rusqlite::Connection,
        tenant_id: &str,
        resource_type: &str,
        resource_id: &str,
        param_name: &str,
        value: &Value,
    ) -> StorageResult<()> {
        // Check for coding array (CodeableConcept)
        if let Some(codings) = value.get("coding").and_then(|v| v.as_array()) {
            for coding in codings {
                let system = coding.get("system").and_then(|v| v.as_str());
                let code = coding.get("code").and_then(|v| v.as_str());
                if let Some(c) = code {
                    self.insert_token_index(
                        conn,
                        tenant_id,
                        resource_type,
                        resource_id,
                        param_name,
                        system,
                        c,
                    )?;
                }
            }
        }
        // Check for direct Coding
        else if let (Some(code), system) = (
            value.get("code").and_then(|v| v.as_str()),
            value.get("system").and_then(|v| v.as_str()),
        ) {
            self.insert_token_index(
                conn,
                tenant_id,
                resource_type,
                resource_id,
                param_name,
                system,
                code,
            )?;
        }
        Ok(())
    }

    /// Index a Reference.
    fn index_reference(
        &self,
        conn: &rusqlite::Connection,
        tenant_id: &str,
        resource_type: &str,
        resource_id: &str,
        param_name: &str,
        value: &Value,
    ) -> StorageResult<()> {
        if let Some(reference) = value.get("reference").and_then(|v| v.as_str()) {
            self.insert_reference_index(
                conn,
                tenant_id,
                resource_type,
                resource_id,
                param_name,
                reference,
            )?;
        }
        Ok(())
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
                let json_data: serde_json::Value = serde_json::from_slice(&data).map_err(|e| {
                    serialization_error(format!("Failed to deserialize resource: {}", e))
                })?;

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
        let current = self.read(tenant, resource_type, id).await?.ok_or_else(|| {
            StorageError::Resource(ResourceError::NotFound {
                resource_type: resource_type.to_string(),
                id: id.to_string(),
            })
        })?;

        // Check version match
        if current.version_id() != expected_version {
            return Err(StorageError::Concurrency(
                ConcurrencyError::VersionConflict {
                    resource_type: resource_type.to_string(),
                    id: id.to_string(),
                    expected_version: expected_version.to_string(),
                    actual_version: current.version_id().to_string(),
                },
            ));
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
                return Err(internal_error(format!(
                    "Failed to get current version: {}",
                    e
                )));
            }
        };

        if current_version != expected_version {
            return Err(StorageError::Concurrency(
                ConcurrencyError::VersionConflict {
                    resource_type: resource_type.to_string(),
                    id: id.to_string(),
                    expected_version: expected_version.to_string(),
                    actual_version: current_version,
                },
            ));
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
             WHERE tenant_id = ?1 AND resource_type = ?2 AND id = ?3",
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

            let json_data: serde_json::Value = serde_json::from_slice(&data).map_err(|e| {
                serialization_error(format!("Failed to deserialize resource: {}", e))
            })?;

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
             WHERE tenant_id = ?1 AND resource_type = ?2",
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
                if let (
                    Some(CursorValue::String(timestamp)),
                    Some(CursorValue::String(resource_id)),
                ) = (sort_values.first(), sort_values.get(1))
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

            let json_data: serde_json::Value = serde_json::from_slice(&data).map_err(|e| {
                serialization_error(format!("Failed to deserialize resource: {}", e))
            })?;

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
            let check_sql = sql.replace(
                &format!(" LIMIT {}", params.pagination.count + 1),
                &format!(" LIMIT {}", params.pagination.count + 2),
            );
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
                vec![CursorValue::String(timestamp), CursorValue::String(id)],
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
             WHERE tenant_id = ?1",
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

        let mut stmt = conn.prepare(&sql).map_err(|e| {
            internal_error(format!("Failed to prepare system history query: {}", e))
        })?;

        let rows = stmt
            .query_map(params![tenant_id], |row| {
                let resource_type: String = row.get(0)?;
                let id: String = row.get(1)?;
                let version_id: String = row.get(2)?;
                let data: Vec<u8> = row.get(3)?;
                let last_updated: String = row.get(4)?;
                let is_deleted: i32 = row.get(5)?;
                Ok((
                    resource_type,
                    id,
                    version_id,
                    data,
                    last_updated,
                    is_deleted,
                ))
            })
            .map_err(|e| internal_error(format!("Failed to query system history: {}", e)))?;

        let mut entries = Vec::new();
        let mut last_entry: Option<(String, String, String)> = None; // (last_updated, resource_type, id)

        for row in rows {
            let (resource_type, id, version_id, data, last_updated_str, is_deleted) = row
                .map_err(|e| internal_error(format!("Failed to read system history row: {}", e)))?;

            // Stop if we've collected enough items (we fetched count+1 to detect more)
            if entries.len() >= params.pagination.count as usize {
                break;
            }

            let json_data: serde_json::Value = serde_json::from_slice(&data).map_err(|e| {
                serialization_error(format!("Failed to deserialize resource: {}", e))
            })?;

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

    async fn history_system_count(&self, tenant: &TenantContext) -> StorageResult<u64> {
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

#[async_trait]
impl PurgableStorage for SqliteBackend {
    async fn purge(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<()> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        // Check if resource exists (in any state)
        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM resources WHERE tenant_id = ?1 AND resource_type = ?2 AND id = ?3",
                params![tenant_id, resource_type, id],
                |_| Ok(true),
            )
            .unwrap_or(false);

        if !exists {
            // Also check history in case it was already purged from main table
            let history_exists: bool = conn
                .query_row(
                    "SELECT 1 FROM resource_history WHERE tenant_id = ?1 AND resource_type = ?2 AND id = ?3",
                    params![tenant_id, resource_type, id],
                    |_| Ok(true),
                )
                .unwrap_or(false);

            if !history_exists {
                return Err(StorageError::Resource(ResourceError::NotFound {
                    resource_type: resource_type.to_string(),
                    id: id.to_string(),
                }));
            }
        }

        // Delete from resources table
        conn.execute(
            "DELETE FROM resources WHERE tenant_id = ?1 AND resource_type = ?2 AND id = ?3",
            params![tenant_id, resource_type, id],
        )
        .map_err(|e| internal_error(format!("Failed to purge resource: {}", e)))?;

        // Delete from history table
        conn.execute(
            "DELETE FROM resource_history WHERE tenant_id = ?1 AND resource_type = ?2 AND id = ?3",
            params![tenant_id, resource_type, id],
        )
        .map_err(|e| internal_error(format!("Failed to purge resource history: {}", e)))?;

        // Delete from search index
        conn.execute(
            "DELETE FROM search_index WHERE tenant_id = ?1 AND resource_type = ?2 AND resource_id = ?3",
            params![tenant_id, resource_type, id],
        )
        .map_err(|e| internal_error(format!("Failed to purge search index: {}", e)))?;

        Ok(())
    }

    async fn purge_all(&self, tenant: &TenantContext, resource_type: &str) -> StorageResult<u64> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        // Count how many we're about to delete
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(DISTINCT id) FROM resources WHERE tenant_id = ?1 AND resource_type = ?2",
                params![tenant_id, resource_type],
                |row| row.get(0),
            )
            .unwrap_or(0);

        // Delete from resources table
        conn.execute(
            "DELETE FROM resources WHERE tenant_id = ?1 AND resource_type = ?2",
            params![tenant_id, resource_type],
        )
        .map_err(|e| internal_error(format!("Failed to purge resources: {}", e)))?;

        // Delete from history table
        conn.execute(
            "DELETE FROM resource_history WHERE tenant_id = ?1 AND resource_type = ?2",
            params![tenant_id, resource_type],
        )
        .map_err(|e| internal_error(format!("Failed to purge resource history: {}", e)))?;

        // Delete from search index
        conn.execute(
            "DELETE FROM search_index WHERE tenant_id = ?1 AND resource_type = ?2",
            params![tenant_id, resource_type],
        )
        .map_err(|e| internal_error(format!("Failed to purge search index: {}", e)))?;

        Ok(count as u64)
    }
}

#[async_trait]
impl DifferentialHistoryProvider for SqliteBackend {
    async fn modified_since(
        &self,
        tenant: &TenantContext,
        resource_type: Option<&str>,
        since: chrono::DateTime<Utc>,
        pagination: &Pagination,
    ) -> StorageResult<Page<StoredResource>> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();
        let since_str = since.to_rfc3339();

        // Build query for current versions of resources modified since timestamp
        let mut sql = String::from(
            "SELECT resource_type, id, version_id, data, last_updated
             FROM resources
             WHERE tenant_id = ?1 AND last_updated > ?2 AND is_deleted = 0"
        );

        // Filter by resource type if specified
        if let Some(rt) = resource_type {
            sql.push_str(&format!(" AND resource_type = '{}'", rt));
        }

        // Apply cursor filter if present
        if let Some(cursor) = pagination.cursor_value() {
            let sort_values = cursor.sort_values();
            if sort_values.len() >= 2 {
                if let (
                    Some(CursorValue::String(timestamp)),
                    Some(CursorValue::String(res_id)),
                ) = (sort_values.first(), sort_values.get(1))
                {
                    sql.push_str(&format!(
                        " AND (last_updated > '{}' OR (last_updated = '{}' AND id > '{}'))",
                        timestamp, timestamp, res_id
                    ));
                }
            }
        }

        // Order by last_updated ascending (oldest first for sync)
        sql.push_str(" ORDER BY last_updated ASC, id ASC");
        sql.push_str(&format!(" LIMIT {}", pagination.count + 1));

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| internal_error(format!("Failed to prepare modified_since query: {}", e)))?;

        let rows = stmt
            .query_map(params![tenant_id, since_str], |row| {
                let resource_type: String = row.get(0)?;
                let id: String = row.get(1)?;
                let version_id: String = row.get(2)?;
                let data: Vec<u8> = row.get(3)?;
                let last_updated: String = row.get(4)?;
                Ok((resource_type, id, version_id, data, last_updated))
            })
            .map_err(|e| internal_error(format!("Failed to query modified resources: {}", e)))?;

        let mut resources = Vec::new();
        let mut last_entry: Option<(String, String)> = None; // (last_updated, id)

        for row in rows {
            let (resource_type, id, version_id, data, last_updated_str) =
                row.map_err(|e| internal_error(format!("Failed to read row: {}", e)))?;

            // Stop if we've collected enough items
            if resources.len() >= pagination.count as usize {
                break;
            }

            let json_data: serde_json::Value = serde_json::from_slice(&data)
                .map_err(|e| serialization_error(format!("Failed to deserialize resource: {}", e)))?;

            let last_updated = chrono::DateTime::parse_from_rfc3339(&last_updated_str)
                .map_err(|e| internal_error(format!("Failed to parse last_updated: {}", e)))?
                .with_timezone(&Utc);

            let resource = StoredResource::from_storage(
                &resource_type,
                &id,
                &version_id,
                tenant.tenant_id().clone(),
                json_data,
                last_updated,
                last_updated,
                None,
            );

            last_entry = Some((last_updated_str, id));
            resources.push(resource);
        }

        // Check if there are more results
        let has_more = {
            let check_sql = sql.replace(
                &format!(" LIMIT {}", pagination.count + 1),
                &format!(" LIMIT {}", pagination.count + 2),
            );
            let mut check_stmt = conn
                .prepare(&check_sql)
                .map_err(|e| internal_error(format!("Failed to prepare check query: {}", e)))?;
            let check_count = check_stmt
                .query_map(params![tenant_id, since_str], |_| Ok(()))
                .map_err(|e| internal_error(format!("Failed to check for more results: {}", e)))?
                .count();
            check_count > pagination.count as usize
        };

        // Build page info
        let page_info = if has_more && last_entry.is_some() {
            let (timestamp, id) = last_entry.unwrap();
            let cursor = PageCursor::new(
                vec![
                    CursorValue::String(timestamp),
                    CursorValue::String(id),
                ],
                "modified_since".to_string(),
            );
            PageInfo::with_next(cursor)
        } else {
            PageInfo::end()
        };

        Ok(Page::new(resources, page_info))
    }
}

// Helper function to parse simple search parameters
// Supports basic formats like: identifier=X, _id=Y, name=Z
fn parse_simple_search_params(params: &str) -> Vec<(String, String)> {
    params
        .split('&')
        .filter_map(|pair| {
            let parts: Vec<&str> = pair.splitn(2, '=').collect();
            if parts.len() == 2 {
                Some((parts[0].to_string(), parts[1].to_string()))
            } else {
                None
            }
        })
        .collect()
}

#[async_trait]
impl ConditionalStorage for SqliteBackend {
    async fn conditional_create(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        resource: Value,
        search_params: &str,
    ) -> StorageResult<ConditionalCreateResult> {
        // Find matching resources based on search parameters
        let matches = self
            .find_matching_resources(tenant, resource_type, search_params)
            .await?;

        match matches.len() {
            0 => {
                // No match - create the resource
                let created = self.create(tenant, resource_type, resource).await?;
                Ok(ConditionalCreateResult::Created(created))
            }
            1 => {
                // Exactly one match - return the existing resource
                Ok(ConditionalCreateResult::Exists(matches.into_iter().next().unwrap()))
            }
            n => {
                // Multiple matches - error condition
                Ok(ConditionalCreateResult::MultipleMatches(n))
            }
        }
    }

    async fn conditional_update(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        resource: Value,
        search_params: &str,
        upsert: bool,
    ) -> StorageResult<ConditionalUpdateResult> {
        // Find matching resources based on search parameters
        let matches = self
            .find_matching_resources(tenant, resource_type, search_params)
            .await?;

        match matches.len() {
            0 => {
                if upsert {
                    // No match, but upsert is true - create new resource
                    let created = self.create(tenant, resource_type, resource).await?;
                    Ok(ConditionalUpdateResult::Created(created))
                } else {
                    // No match and no upsert
                    Ok(ConditionalUpdateResult::NoMatch)
                }
            }
            1 => {
                // Exactly one match - update it
                let existing = matches.into_iter().next().unwrap();
                let updated = self.update(tenant, &existing, resource).await?;
                Ok(ConditionalUpdateResult::Updated(updated))
            }
            n => {
                // Multiple matches - error condition
                Ok(ConditionalUpdateResult::MultipleMatches(n))
            }
        }
    }

    async fn conditional_delete(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        search_params: &str,
    ) -> StorageResult<ConditionalDeleteResult> {
        // Find matching resources based on search parameters
        let matches = self
            .find_matching_resources(tenant, resource_type, search_params)
            .await?;

        match matches.len() {
            0 => {
                // No match
                Ok(ConditionalDeleteResult::NoMatch)
            }
            1 => {
                // Exactly one match - delete it
                let existing = matches.into_iter().next().unwrap();
                self.delete(tenant, resource_type, existing.id()).await?;
                Ok(ConditionalDeleteResult::Deleted)
            }
            n => {
                // Multiple matches - error condition
                Ok(ConditionalDeleteResult::MultipleMatches(n))
            }
        }
    }
}

impl SqliteBackend {
    /// Find resources matching the given search parameters.
    /// This is a simplified implementation that supports basic parameters.
    async fn find_matching_resources(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        search_params: &str,
    ) -> StorageResult<Vec<StoredResource>> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        // Parse search parameters
        let params = parse_simple_search_params(search_params);

        // Build the WHERE clause
        let mut conditions = vec![
            "tenant_id = ?1".to_string(),
            "resource_type = ?2".to_string(),
            "is_deleted = 0".to_string(),
        ];

        // Handle common search parameters
        for (name, value) in &params {
            match name.as_str() {
                "_id" => {
                    // Direct ID match
                    conditions.push(format!("id = '{}'", value.replace('\'', "''")));
                }
                "identifier" => {
                    // Search in JSON for identifier value
                    // Handle format: system|value or just value
                    if value.contains('|') {
                        let parts: Vec<&str> = value.splitn(2, '|').collect();
                        let system = parts[0].replace('\'', "''");
                        let id_value = parts[1].replace('\'', "''");
                        conditions.push(format!(
                            "json_extract(data, '$.identifier') LIKE '%\"system\":\"{}\"%' AND json_extract(data, '$.identifier') LIKE '%\"value\":\"{}\"%'",
                            system, id_value
                        ));
                    } else {
                        let escaped_value = value.replace('\'', "''");
                        conditions.push(format!(
                            "json_extract(data, '$.identifier') LIKE '%\"value\":\"{}\"%'",
                            escaped_value
                        ));
                    }
                }
                _ => {
                    // For other parameters, try a simple JSON path lookup
                    let escaped_value = value.replace('\'', "''");
                    conditions.push(format!(
                        "(json_extract(data, '$.{}') = '{}' OR json_extract(data, '$.{}') LIKE '%{}%')",
                        name, escaped_value, name, escaped_value
                    ));
                }
            }
        }

        let sql = format!(
            "SELECT id, version_id, data, last_updated FROM resources WHERE {}",
            conditions.join(" AND ")
        );

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| internal_error(format!("Failed to prepare conditional query: {}", e)))?;

        let rows = stmt
            .query_map(params![tenant_id, resource_type], |row| {
                let id: String = row.get(0)?;
                let version_id: String = row.get(1)?;
                let data: Vec<u8> = row.get(2)?;
                let last_updated: String = row.get(3)?;
                Ok((id, version_id, data, last_updated))
            })
            .map_err(|e| internal_error(format!("Failed to execute conditional query: {}", e)))?;

        let mut resources = Vec::new();
        for row in rows {
            let (id, version_id, data, last_updated_str) =
                row.map_err(|e| internal_error(format!("Failed to read row: {}", e)))?;

            let json_data: serde_json::Value = serde_json::from_slice(&data)
                .map_err(|e| serialization_error(format!("Failed to deserialize resource: {}", e)))?;

            let last_updated = chrono::DateTime::parse_from_rfc3339(&last_updated_str)
                .map_err(|e| internal_error(format!("Failed to parse last_updated: {}", e)))?
                .with_timezone(&Utc);

            let resource = StoredResource::from_storage(
                resource_type,
                &id,
                &version_id,
                tenant.tenant_id().clone(),
                json_data,
                last_updated,
                last_updated,
                None,
            );

            resources.push(resource);
        }

        Ok(resources)
    }
}

#[async_trait]
impl BundleProvider for SqliteBackend {
    async fn process_transaction(
        &self,
        tenant: &TenantContext,
        entries: Vec<BundleEntry>,
    ) -> Result<BundleResult, TransactionError> {
        use crate::core::transaction::{Transaction, TransactionOptions, TransactionProvider};

        // Start a transaction
        let mut tx = self
            .begin_transaction(tenant, TransactionOptions::new())
            .await
            .map_err(|e| TransactionError::RolledBack {
                reason: format!("Failed to begin transaction: {}", e),
            })?;

        let mut results = Vec::with_capacity(entries.len());
        let mut error_info: Option<(usize, String)> = None;

        // Process each entry within the transaction
        for (idx, entry) in entries.iter().enumerate() {
            let result = self.process_bundle_entry_tx(&mut tx, entry).await;

            match result {
                Ok(entry_result) => {
                    // Check for error status codes
                    if entry_result.status >= 400 {
                        error_info = Some((
                            idx,
                            format!("Entry failed with status {}", entry_result.status),
                        ));
                        break;
                    }
                    results.push(entry_result);
                }
                Err(e) => {
                    error_info = Some((idx, format!("Entry processing failed: {}", e)));
                    break;
                }
            }
        }

        // Handle error or commit
        if let Some((index, message)) = error_info {
            let _ = Box::new(tx).rollback().await;
            return Err(TransactionError::BundleError { index, message });
        }

        // Commit the transaction
        Box::new(tx)
            .commit()
            .await
            .map_err(|e| TransactionError::RolledBack {
                reason: format!("Commit failed: {}", e),
            })?;

        Ok(BundleResult {
            bundle_type: BundleType::Transaction,
            entries: results,
        })
    }

    async fn process_batch(
        &self,
        tenant: &TenantContext,
        entries: Vec<BundleEntry>,
    ) -> StorageResult<BundleResult> {
        let mut results = Vec::with_capacity(entries.len());

        // Process each entry independently
        for entry in &entries {
            let result = self.process_batch_entry(tenant, entry).await;
            results.push(result);
        }

        Ok(BundleResult {
            bundle_type: BundleType::Batch,
            entries: results,
        })
    }
}

impl SqliteBackend {
    /// Process a single bundle entry within a transaction.
    async fn process_bundle_entry_tx(
        &self,
        tx: &mut crate::backends::sqlite::transaction::SqliteTransaction,
        entry: &BundleEntry,
    ) -> StorageResult<BundleEntryResult> {
        use crate::core::transaction::Transaction;

        match entry.method {
            BundleMethod::Get => {
                // Parse resource type and ID from URL
                let (resource_type, id) = self.parse_url(&entry.url)?;
                match tx.read(&resource_type, &id).await? {
                    Some(resource) => Ok(BundleEntryResult::ok(resource)),
                    None => Ok(BundleEntryResult::error(
                        404,
                        serde_json::json!({
                            "resourceType": "OperationOutcome",
                            "issue": [{"severity": "error", "code": "not-found"}]
                        }),
                    )),
                }
            }
            BundleMethod::Post => {
                // Create new resource
                let resource = entry.resource.clone().ok_or_else(|| {
                    StorageError::Validation(crate::error::ValidationError::MissingRequiredField {
                        field: "resource".to_string(),
                    })
                })?;

                let resource_type = resource
                    .get("resourceType")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| {
                        StorageError::Validation(
                            crate::error::ValidationError::MissingRequiredField {
                                field: "resourceType".to_string(),
                            },
                        )
                    })?;

                let created = tx.create(&resource_type, resource).await?;
                Ok(BundleEntryResult::created(created))
            }
            BundleMethod::Put => {
                // Update or create resource
                let resource = entry.resource.clone().ok_or_else(|| {
                    StorageError::Validation(crate::error::ValidationError::MissingRequiredField {
                        field: "resource".to_string(),
                    })
                })?;

                let (resource_type, id) = self.parse_url(&entry.url)?;

                // Check if resource exists
                match tx.read(&resource_type, &id).await? {
                    Some(existing) => {
                        // Check If-Match if provided
                        if let Some(ref if_match) = entry.if_match {
                            let current_etag = existing.etag();
                            if current_etag != if_match.as_str() {
                                return Ok(BundleEntryResult::error(
                                    412,
                                    serde_json::json!({
                                        "resourceType": "OperationOutcome",
                                        "issue": [{"severity": "error", "code": "conflict", "diagnostics": "ETag mismatch"}]
                                    }),
                                ));
                            }
                        }
                        let updated = tx.update(&existing, resource).await?;
                        Ok(BundleEntryResult::ok(updated))
                    }
                    None => {
                        // Create new resource with specified ID
                        let mut resource_with_id = resource;
                        resource_with_id["id"] = serde_json::json!(id);
                        let created = tx.create(&resource_type, resource_with_id).await?;
                        Ok(BundleEntryResult::created(created))
                    }
                }
            }
            BundleMethod::Delete => {
                let (resource_type, id) = self.parse_url(&entry.url)?;
                tx.delete(&resource_type, &id).await?;
                Ok(BundleEntryResult::deleted())
            }
            BundleMethod::Patch => {
                // PATCH is not fully implemented yet
                Ok(BundleEntryResult::error(
                    501,
                    serde_json::json!({
                        "resourceType": "OperationOutcome",
                        "issue": [{"severity": "error", "code": "not-supported", "diagnostics": "PATCH not implemented"}]
                    }),
                ))
            }
        }
    }

    /// Process a single batch entry (independent, no transaction).
    async fn process_batch_entry(
        &self,
        tenant: &TenantContext,
        entry: &BundleEntry,
    ) -> BundleEntryResult {
        match self.process_batch_entry_inner(tenant, entry).await {
            Ok(result) => result,
            Err(e) => BundleEntryResult::error(
                500,
                serde_json::json!({
                    "resourceType": "OperationOutcome",
                    "issue": [{"severity": "error", "code": "exception", "diagnostics": e.to_string()}]
                }),
            ),
        }
    }

    async fn process_batch_entry_inner(
        &self,
        tenant: &TenantContext,
        entry: &BundleEntry,
    ) -> StorageResult<BundleEntryResult> {
        match entry.method {
            BundleMethod::Get => {
                let (resource_type, id) = self.parse_url(&entry.url)?;
                match self.read(tenant, &resource_type, &id).await? {
                    Some(resource) => Ok(BundleEntryResult::ok(resource)),
                    None => Ok(BundleEntryResult::error(
                        404,
                        serde_json::json!({
                            "resourceType": "OperationOutcome",
                            "issue": [{"severity": "error", "code": "not-found"}]
                        }),
                    )),
                }
            }
            BundleMethod::Post => {
                let resource = entry.resource.clone().ok_or_else(|| {
                    StorageError::Validation(crate::error::ValidationError::MissingRequiredField {
                        field: "resource".to_string(),
                    })
                })?;

                let resource_type = resource
                    .get("resourceType")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| {
                        StorageError::Validation(
                            crate::error::ValidationError::MissingRequiredField {
                                field: "resourceType".to_string(),
                            },
                        )
                    })?;

                let created = self.create(tenant, &resource_type, resource).await?;
                Ok(BundleEntryResult::created(created))
            }
            BundleMethod::Put => {
                let resource = entry.resource.clone().ok_or_else(|| {
                    StorageError::Validation(crate::error::ValidationError::MissingRequiredField {
                        field: "resource".to_string(),
                    })
                })?;

                let (resource_type, id) = self.parse_url(&entry.url)?;
                let (stored, _created) = self
                    .create_or_update(tenant, &resource_type, &id, resource)
                    .await?;
                Ok(BundleEntryResult::ok(stored))
            }
            BundleMethod::Delete => {
                let (resource_type, id) = self.parse_url(&entry.url)?;
                match self.delete(tenant, &resource_type, &id).await {
                    Ok(()) => Ok(BundleEntryResult::deleted()),
                    Err(StorageError::Resource(ResourceError::NotFound { .. })) => {
                        Ok(BundleEntryResult::deleted()) // Idempotent delete
                    }
                    Err(e) => Err(e),
                }
            }
            BundleMethod::Patch => Ok(BundleEntryResult::error(
                501,
                serde_json::json!({
                    "resourceType": "OperationOutcome",
                    "issue": [{"severity": "error", "code": "not-supported", "diagnostics": "PATCH not implemented"}]
                }),
            )),
        }
    }

    /// Parse a FHIR URL into resource type and ID.
    fn parse_url(&self, url: &str) -> StorageResult<(String, String)> {
        // Handle formats like:
        // - Patient/123
        // - /Patient/123
        // - http://example.com/fhir/Patient/123
        let path = url
            .strip_prefix("http://")
            .or_else(|| url.strip_prefix("https://"))
            .map(|s| {
                // Find the path part after the host
                s.find('/').map(|i| &s[i..]).unwrap_or(s)
            })
            .unwrap_or(url);

        let path = path.trim_start_matches('/');
        let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

        // Take the last two parts (resource type and ID)
        // This handles URLs like /fhir/Patient/123 where we want Patient/123
        if parts.len() >= 2 {
            let len = parts.len();
            Ok((parts[len - 2].to_string(), parts[len - 1].to_string()))
        } else {
            Err(StorageError::Validation(
                crate::error::ValidationError::InvalidReference {
                    reference: url.to_string(),
                    message: "URL must be in format ResourceType/id".to_string(),
                },
            ))
        }
    }
}

// ReindexableStorage implementation for SQLite backend.
#[async_trait]
impl ReindexableStorage for SqliteBackend {
    async fn list_resource_types(&self, tenant: &TenantContext) -> StorageResult<Vec<String>> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str().to_string();

        let mut stmt = conn
            .prepare(
                "SELECT DISTINCT resource_type FROM resources WHERE tenant_id = ?1 AND is_deleted = 0",
            )
            .map_err(|e| internal_error(format!("Failed to prepare statement: {}", e)))?;

        let types: Vec<String> = stmt
            .query_map([&tenant_id], |row| row.get(0))
            .map_err(|e| internal_error(format!("Failed to query resource types: {}", e)))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(types)
    }

    async fn count_resources(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
    ) -> StorageResult<u64> {
        self.count(tenant, Some(resource_type)).await
    }

    async fn fetch_resources_page(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> StorageResult<ResourcePage> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str().to_string();

        // Parse cursor if provided (format: "last_updated|id")
        let (cursor_ts, cursor_id) = if let Some(c) = cursor {
            let parts: Vec<&str> = c.split('|').collect();
            if parts.len() == 2 {
                (Some(parts[0].to_string()), Some(parts[1].to_string()))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        // Build query based on whether we have a cursor
        let (sql, params): (String, Vec<Box<dyn ToSql>>) = if let (Some(ts), Some(id)) =
            (&cursor_ts, &cursor_id)
        {
            (
                "SELECT id, version_id, data, last_updated FROM resources \
                 WHERE tenant_id = ?1 AND resource_type = ?2 AND is_deleted = 0 \
                 AND (last_updated > ?3 OR (last_updated = ?3 AND id > ?4)) \
                 ORDER BY last_updated ASC, id ASC LIMIT ?5"
                    .to_string(),
                vec![
                    Box::new(tenant_id.clone()) as Box<dyn ToSql>,
                    Box::new(resource_type.to_string()),
                    Box::new(ts.clone()),
                    Box::new(id.clone()),
                    Box::new(limit as i64),
                ],
            )
        } else {
            (
                "SELECT id, version_id, data, last_updated FROM resources \
                 WHERE tenant_id = ?1 AND resource_type = ?2 AND is_deleted = 0 \
                 ORDER BY last_updated ASC, id ASC LIMIT ?3"
                    .to_string(),
                vec![
                    Box::new(tenant_id.clone()) as Box<dyn ToSql>,
                    Box::new(resource_type.to_string()),
                    Box::new(limit as i64),
                ],
            )
        };

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| internal_error(format!("Failed to prepare statement: {}", e)))?;

        let param_refs: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();

        let resources: Vec<StoredResource> = stmt
            .query_map(param_refs.as_slice(), |row| {
                let id: String = row.get(0)?;
                let version_id: String = row.get(1)?;
                let data: Vec<u8> = row.get(2)?;
                let last_updated: String = row.get(3)?;

                Ok((id, version_id, data, last_updated))
            })
            .map_err(|e| internal_error(format!("Failed to query resources: {}", e)))?
            .filter_map(|r| r.ok())
            .filter_map(|(id, version_id, data, last_updated)| {
                let content: Value = serde_json::from_slice(&data).ok()?;
                let last_modified = chrono::DateTime::parse_from_rfc3339(&last_updated)
                    .ok()?
                    .with_timezone(&Utc);
                Some(StoredResource::from_storage(
                    resource_type.to_string(),
                    id,
                    version_id,
                    tenant.tenant_id().clone(),
                    content,
                    last_modified, // created_at (use last_modified as approximation)
                    last_modified,
                    None, // not deleted
                ))
            })
            .collect();

        // Determine next cursor
        let next_cursor = if resources.len() == limit as usize {
            resources.last().map(|r| {
                format!(
                    "{}|{}",
                    r.last_modified().to_rfc3339(),
                    r.id()
                )
            })
        } else {
            None
        };

        Ok(ResourcePage {
            resources,
            next_cursor,
        })
    }

    async fn delete_search_entries(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        resource_id: &str,
    ) -> StorageResult<()> {
        let conn = self.get_connection()?;
        self.delete_search_index(&conn, tenant.tenant_id().as_str(), resource_type, resource_id)
    }

    async fn write_search_entries(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        resource_id: &str,
        resource: &Value,
    ) -> StorageResult<usize> {
        let conn = self.get_connection()?;

        // Use the dynamic extraction
        let values = self
            .search_extractor()
            .extract(resource, resource_type)
            .map_err(|e| internal_error(format!("Search parameter extraction failed: {}", e)))?;

        let mut count = 0;
        for value in values {
            self.write_index_entry(
                &conn,
                tenant.tenant_id().as_str(),
                resource_type,
                resource_id,
                &value,
            )?;
            count += 1;
        }

        Ok(count)
    }

    async fn clear_search_index(&self, tenant: &TenantContext) -> StorageResult<u64> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        let deleted = conn
            .execute(
                "DELETE FROM search_index WHERE tenant_id = ?1",
                params![tenant_id],
            )
            .map_err(|e| internal_error(format!("Failed to clear search index: {}", e)))?;

        Ok(deleted as u64)
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
        TenantContext::new(
            TenantId::new("test-tenant"),
            TenantPermissions::full_access(),
        )
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
        let read = backend
            .read(&tenant, "Patient", created.id())
            .await
            .unwrap();
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
        backend
            .create(&tenant, "Patient", resource.clone())
            .await
            .unwrap();

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

        let result = backend
            .read(&tenant, "Patient", "nonexistent")
            .await
            .unwrap();
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
        let updated = backend
            .update(&tenant, &created, updated_content)
            .await
            .unwrap();
        assert_eq!(updated.version_id(), "2");

        // Verify
        let read = backend
            .read(&tenant, "Patient", created.id())
            .await
            .unwrap()
            .unwrap();
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
            Err(StorageError::Concurrency(
                ConcurrencyError::VersionConflict { .. }
            ))
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
        backend
            .delete(&tenant, "Patient", created.id())
            .await
            .unwrap();

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
        backend
            .create(&tenant, "Patient", json!({"id": "existing-id"}))
            .await
            .unwrap();

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
        backend
            .create(&tenant, "Observation", json!({}))
            .await
            .unwrap();

        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 2);
        assert_eq!(
            backend.count(&tenant, Some("Observation")).await.unwrap(),
            1
        );
        assert_eq!(backend.count(&tenant, None).await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_tenant_isolation() {
        let backend = create_test_backend();

        let tenant1 =
            TenantContext::new(TenantId::new("tenant-1"), TenantPermissions::full_access());
        let tenant2 =
            TenantContext::new(TenantId::new("tenant-2"), TenantPermissions::full_access());

        // Create in tenant 1
        let resource = json!({"id": "patient-1"});
        backend.create(&tenant1, "Patient", resource).await.unwrap();

        // Tenant 1 can read
        assert!(
            backend
                .read(&tenant1, "Patient", "patient-1")
                .await
                .unwrap()
                .is_some()
        );

        // Tenant 2 cannot read
        assert!(
            backend
                .read(&tenant2, "Patient", "patient-1")
                .await
                .unwrap()
                .is_none()
        );
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
        let v2 = backend
            .update(&tenant, &created, json!({"name": [{"family": "Jones"}]}))
            .await
            .unwrap();
        let _v3 = backend
            .update(&tenant, &v2, json!({"name": [{"family": "Brown"}]}))
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

        let count = backend
            .history_instance_count(&tenant, "Patient", created.id())
            .await
            .unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_history_instance_with_delete() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create, update, then delete
        let resource = json!({"id": "p1"});
        let created = backend.create(&tenant, "Patient", resource).await.unwrap();
        let _v2 = backend
            .update(&tenant, &created, json!({"id": "p1"}))
            .await
            .unwrap();
        backend.delete(&tenant, "Patient", "p1").await.unwrap();

        // Get history including deleted
        let params = HistoryParams::new().include_deleted(true);
        let history = backend
            .history_instance(&tenant, "Patient", "p1", &params)
            .await
            .unwrap();

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
        let _v2 = backend
            .update(&tenant, &created, json!({"id": "p2"}))
            .await
            .unwrap();
        backend.delete(&tenant, "Patient", "p2").await.unwrap();

        // Get history excluding deleted
        let params = HistoryParams::new().include_deleted(false);
        let history = backend
            .history_instance(&tenant, "Patient", "p2", &params)
            .await
            .unwrap();

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
        let page1 = backend
            .history_instance(&tenant, "Patient", current.id(), &params)
            .await
            .unwrap();

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
        let history = backend
            .history_instance(&tenant, "Patient", "nonexistent", &params)
            .await
            .unwrap();

        assert!(history.items.is_empty());
    }

    #[tokio::test]
    async fn test_history_instance_tenant_isolation() {
        let backend = create_test_backend();
        let tenant1 =
            TenantContext::new(TenantId::new("tenant-1"), TenantPermissions::full_access());
        let tenant2 =
            TenantContext::new(TenantId::new("tenant-2"), TenantPermissions::full_access());

        // Create in tenant 1
        let resource = json!({"id": "shared-id"});
        let created = backend.create(&tenant1, "Patient", resource).await.unwrap();
        let _v2 = backend
            .update(&tenant1, &created, json!({"id": "shared-id"}))
            .await
            .unwrap();

        // Tenant 1 sees history
        let history1 = backend
            .history_instance(&tenant1, "Patient", "shared-id", &HistoryParams::new())
            .await
            .unwrap();
        assert_eq!(history1.items.len(), 2);

        // Tenant 2 sees nothing
        let history2 = backend
            .history_instance(&tenant2, "Patient", "shared-id", &HistoryParams::new())
            .await
            .unwrap();
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
        let p1 = backend
            .create(&tenant, "Patient", json!({"id": "p1"}))
            .await
            .unwrap();
        let p2 = backend
            .create(&tenant, "Patient", json!({"id": "p2"}))
            .await
            .unwrap();

        // Update p1
        let _p1_v2 = backend
            .update(&tenant, &p1, json!({"id": "p1"}))
            .await
            .unwrap();

        // Get type history
        let params = HistoryParams::new();
        let history = backend
            .history_type(&tenant, "Patient", &params)
            .await
            .unwrap();

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
        backend
            .create(&tenant, "Observation", json!({}))
            .await
            .unwrap();

        // Count patient history
        let count = backend
            .history_type_count(&tenant, "Patient")
            .await
            .unwrap();
        assert_eq!(count, 3); // p1 v1, p1 v2, p2 v1

        // Count observation history
        let obs_count = backend
            .history_type_count(&tenant, "Observation")
            .await
            .unwrap();
        assert_eq!(obs_count, 1);
    }

    #[tokio::test]
    async fn test_history_type_filters_by_type() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create different resource types
        backend.create(&tenant, "Patient", json!({})).await.unwrap();
        backend
            .create(&tenant, "Observation", json!({}))
            .await
            .unwrap();
        backend
            .create(&tenant, "Encounter", json!({}))
            .await
            .unwrap();

        // Get only Patient history
        let history = backend
            .history_type(&tenant, "Patient", &HistoryParams::new())
            .await
            .unwrap();
        assert_eq!(history.items.len(), 1);
        assert_eq!(history.items[0].resource.resource_type(), "Patient");

        // Get only Observation history
        let obs_history = backend
            .history_type(&tenant, "Observation", &HistoryParams::new())
            .await
            .unwrap();
        assert_eq!(obs_history.items.len(), 1);
        assert_eq!(obs_history.items[0].resource.resource_type(), "Observation");
    }

    #[tokio::test]
    async fn test_history_type_includes_deleted() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create and delete a patient
        let p1 = backend
            .create(&tenant, "Patient", json!({"id": "del-p1"}))
            .await
            .unwrap();
        backend.delete(&tenant, "Patient", "del-p1").await.unwrap();

        // Create another patient
        backend
            .create(&tenant, "Patient", json!({"id": "p2"}))
            .await
            .unwrap();

        // Without including deleted
        let history = backend
            .history_type(&tenant, "Patient", &HistoryParams::new())
            .await
            .unwrap();
        assert_eq!(history.items.len(), 2); // p1 v1, p2 v1 (excludes delete)

        // Including deleted
        let history_with_deleted = backend
            .history_type(
                &tenant,
                "Patient",
                &HistoryParams::new().include_deleted(true),
            )
            .await
            .unwrap();
        assert_eq!(history_with_deleted.items.len(), 3); // p1 v1, p1 delete, p2 v1
    }

    #[tokio::test]
    async fn test_history_type_tenant_isolation() {
        let backend = create_test_backend();
        let tenant1 =
            TenantContext::new(TenantId::new("tenant-1"), TenantPermissions::full_access());
        let tenant2 =
            TenantContext::new(TenantId::new("tenant-2"), TenantPermissions::full_access());

        // Create patients in tenant 1
        backend
            .create(&tenant1, "Patient", json!({}))
            .await
            .unwrap();
        backend
            .create(&tenant1, "Patient", json!({}))
            .await
            .unwrap();

        // Create patient in tenant 2
        backend
            .create(&tenant2, "Patient", json!({}))
            .await
            .unwrap();

        // Tenant 1 sees only its history
        let history1 = backend
            .history_type(&tenant1, "Patient", &HistoryParams::new())
            .await
            .unwrap();
        assert_eq!(history1.items.len(), 2);

        // Tenant 2 sees only its history
        let history2 = backend
            .history_type(&tenant2, "Patient", &HistoryParams::new())
            .await
            .unwrap();
        assert_eq!(history2.items.len(), 1);
    }

    #[tokio::test]
    async fn test_history_type_pagination() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create several patients
        for i in 0..5 {
            backend
                .create(&tenant, "Patient", json!({"id": format!("p{}", i)}))
                .await
                .unwrap();
        }

        // Get first page (2 items)
        let params = HistoryParams::new().count(2);
        let page1 = backend
            .history_type(&tenant, "Patient", &params)
            .await
            .unwrap();

        assert_eq!(page1.items.len(), 2);
        assert!(page1.page_info.has_next);
    }

    #[tokio::test]
    async fn test_history_type_empty() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // No resources created
        let history = backend
            .history_type(&tenant, "Patient", &HistoryParams::new())
            .await
            .unwrap();
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
        let p1 = backend
            .create(&tenant, "Patient", json!({"id": "p1"}))
            .await
            .unwrap();
        backend
            .create(&tenant, "Observation", json!({"id": "o1"}))
            .await
            .unwrap();
        backend
            .create(&tenant, "Encounter", json!({"id": "e1"}))
            .await
            .unwrap();

        // Update patient
        let _p1_v2 = backend
            .update(&tenant, &p1, json!({"id": "p1"}))
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
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create different resource types
        let p1 = backend.create(&tenant, "Patient", json!({})).await.unwrap();
        let _p1_v2 = backend.update(&tenant, &p1, json!({})).await.unwrap();
        backend
            .create(&tenant, "Observation", json!({}))
            .await
            .unwrap();
        backend
            .create(&tenant, "Encounter", json!({}))
            .await
            .unwrap();

        // Count all history
        let count = backend.history_system_count(&tenant).await.unwrap();
        assert_eq!(count, 4); // p1 v1, p1 v2, o1, e1
    }

    #[tokio::test]
    async fn test_history_system_includes_deleted() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create and delete a patient
        backend
            .create(&tenant, "Patient", json!({"id": "del-p1"}))
            .await
            .unwrap();
        backend.delete(&tenant, "Patient", "del-p1").await.unwrap();

        // Create another resource
        backend
            .create(&tenant, "Observation", json!({}))
            .await
            .unwrap();

        // Without including deleted
        let history = backend
            .history_system(&tenant, &HistoryParams::new())
            .await
            .unwrap();
        assert_eq!(history.items.len(), 2); // p1 v1, obs (excludes delete)

        // Including deleted
        let history_with_deleted = backend
            .history_system(&tenant, &HistoryParams::new().include_deleted(true))
            .await
            .unwrap();
        assert_eq!(history_with_deleted.items.len(), 3); // p1 v1, p1 delete, obs
    }

    #[tokio::test]
    async fn test_history_system_tenant_isolation() {
        let backend = create_test_backend();
        let tenant1 =
            TenantContext::new(TenantId::new("tenant-1"), TenantPermissions::full_access());
        let tenant2 =
            TenantContext::new(TenantId::new("tenant-2"), TenantPermissions::full_access());

        // Create resources in tenant 1
        backend
            .create(&tenant1, "Patient", json!({}))
            .await
            .unwrap();
        backend
            .create(&tenant1, "Observation", json!({}))
            .await
            .unwrap();

        // Create resource in tenant 2
        backend
            .create(&tenant2, "Encounter", json!({}))
            .await
            .unwrap();

        // Tenant 1 sees only its history
        let history1 = backend
            .history_system(&tenant1, &HistoryParams::new())
            .await
            .unwrap();
        assert_eq!(history1.items.len(), 2);

        // Tenant 2 sees only its history
        let history2 = backend
            .history_system(&tenant2, &HistoryParams::new())
            .await
            .unwrap();
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
            backend
                .create(&tenant, "Patient", json!({"id": format!("p{}", i)}))
                .await
                .unwrap();
        }
        for i in 0..2 {
            backend
                .create(&tenant, "Observation", json!({"id": format!("o{}", i)}))
                .await
                .unwrap();
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
        let history = backend
            .history_system(&tenant, &HistoryParams::new())
            .await
            .unwrap();
        assert!(history.items.is_empty());
        assert!(!history.page_info.has_next);

        assert_eq!(backend.history_system_count(&tenant).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_history_system_ordered_by_time() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create resources - they should be ordered by last_updated DESC
        backend
            .create(&tenant, "Patient", json!({"id": "first"}))
            .await
            .unwrap();
        backend
            .create(&tenant, "Observation", json!({"id": "second"}))
            .await
            .unwrap();
        backend
            .create(&tenant, "Encounter", json!({"id": "third"}))
            .await
            .unwrap();

        let history = backend
            .history_system(&tenant, &HistoryParams::new())
            .await
            .unwrap();

        // Should be in reverse chronological order (newest first)
        assert_eq!(history.items.len(), 3);
        // The last created should be first in the list
        assert_eq!(history.items[0].resource.id(), "third");
        assert_eq!(history.items[1].resource.id(), "second");
        assert_eq!(history.items[2].resource.id(), "first");
    }

    // ========================================================================
    // PurgableStorage Tests
    // ========================================================================

    #[tokio::test]
    async fn test_purge_single_resource() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create a resource with multiple versions
        let p1 = backend
            .create(&tenant, "Patient", json!({"id": "p1"}))
            .await
            .unwrap();
        let _p1_v2 = backend
            .update(&tenant, &p1, json!({"id": "p1"}))
            .await
            .unwrap();

        // Purge the resource
        backend.purge(&tenant, "Patient", "p1").await.unwrap();

        // Resource should be gone
        let read_result = backend.read(&tenant, "Patient", "p1").await.unwrap();
        assert!(read_result.is_none());

        // History should also be gone
        let history = backend
            .history_instance(&tenant, "Patient", "p1", &HistoryParams::new())
            .await
            .unwrap();
        assert!(history.items.is_empty());
    }

    #[tokio::test]
    async fn test_purge_deleted_resource() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create and delete a resource
        backend
            .create(&tenant, "Patient", json!({"id": "del-p1"}))
            .await
            .unwrap();
        backend.delete(&tenant, "Patient", "del-p1").await.unwrap();

        // Purge the deleted resource
        backend.purge(&tenant, "Patient", "del-p1").await.unwrap();

        // History should be completely gone
        let history = backend
            .history_instance(
                &tenant,
                "Patient",
                "del-p1",
                &HistoryParams::new().include_deleted(true),
            )
            .await
            .unwrap();
        assert!(history.items.is_empty());
    }

    #[tokio::test]
    async fn test_purge_nonexistent_resource() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Purging a nonexistent resource should fail
        let result = backend.purge(&tenant, "Patient", "nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_purge_tenant_isolation() {
        let backend = create_test_backend();
        let tenant1 =
            TenantContext::new(TenantId::new("tenant-1"), TenantPermissions::full_access());
        let tenant2 =
            TenantContext::new(TenantId::new("tenant-2"), TenantPermissions::full_access());

        // Create resource in tenant 1
        backend
            .create(&tenant1, "Patient", json!({"id": "shared-id"}))
            .await
            .unwrap();

        // Create resource with same ID in tenant 2
        backend
            .create(&tenant2, "Patient", json!({"id": "shared-id"}))
            .await
            .unwrap();

        // Purge from tenant 1
        backend
            .purge(&tenant1, "Patient", "shared-id")
            .await
            .unwrap();

        // Tenant 2's resource should still exist
        let t2_read = backend.read(&tenant2, "Patient", "shared-id").await.unwrap();
        assert!(t2_read.is_some());

        // Tenant 1's resource should be gone
        let t1_read = backend.read(&tenant1, "Patient", "shared-id").await.unwrap();
        assert!(t1_read.is_none());
    }

    #[tokio::test]
    async fn test_purge_all_single_type() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create multiple patients
        for i in 0..5 {
            backend
                .create(&tenant, "Patient", json!({"id": format!("p{}", i)}))
                .await
                .unwrap();
        }

        // Create some observations too
        backend
            .create(&tenant, "Observation", json!({}))
            .await
            .unwrap();

        // Purge all patients
        let count = backend.purge_all(&tenant, "Patient").await.unwrap();
        assert_eq!(count, 5);

        // Patients should be gone
        let patient_history = backend
            .history_type(&tenant, "Patient", &HistoryParams::new())
            .await
            .unwrap();
        assert!(patient_history.items.is_empty());

        // Observations should still exist
        let obs_history = backend
            .history_type(&tenant, "Observation", &HistoryParams::new())
            .await
            .unwrap();
        assert_eq!(obs_history.items.len(), 1);
    }

    #[tokio::test]
    async fn test_purge_all_empty_type() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Purging empty type should return 0
        let count = backend.purge_all(&tenant, "Patient").await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_purge_all_tenant_isolation() {
        let backend = create_test_backend();
        let tenant1 =
            TenantContext::new(TenantId::new("tenant-1"), TenantPermissions::full_access());
        let tenant2 =
            TenantContext::new(TenantId::new("tenant-2"), TenantPermissions::full_access());

        // Create patients in both tenants
        for i in 0..3 {
            backend
                .create(&tenant1, "Patient", json!({"id": format!("t1-p{}", i)}))
                .await
                .unwrap();
        }
        for i in 0..2 {
            backend
                .create(&tenant2, "Patient", json!({"id": format!("t2-p{}", i)}))
                .await
                .unwrap();
        }

        // Purge all patients from tenant 1
        let count = backend.purge_all(&tenant1, "Patient").await.unwrap();
        assert_eq!(count, 3);

        // Tenant 2's patients should still exist
        let t2_history = backend
            .history_type(&tenant2, "Patient", &HistoryParams::new())
            .await
            .unwrap();
        assert_eq!(t2_history.items.len(), 2);
    }

    // ========================================================================
    // DifferentialHistoryProvider Tests
    // ========================================================================

    #[tokio::test]
    async fn test_modified_since_basic() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Capture time before creating resources
        let before_create = Utc::now();

        // Create some resources
        backend
            .create(&tenant, "Patient", json!({"id": "p1"}))
            .await
            .unwrap();
        backend
            .create(&tenant, "Patient", json!({"id": "p2"}))
            .await
            .unwrap();
        backend
            .create(&tenant, "Observation", json!({"id": "o1"}))
            .await
            .unwrap();

        // Query for all resources modified since before_create
        let pagination = Pagination::default();
        let result = backend
            .modified_since(&tenant, None, before_create, &pagination)
            .await
            .unwrap();

        // Should find all 3 resources
        assert_eq!(result.items.len(), 3);
    }

    #[tokio::test]
    async fn test_modified_since_with_type_filter() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        let before_create = Utc::now();

        // Create different resource types
        backend
            .create(&tenant, "Patient", json!({"id": "p1"}))
            .await
            .unwrap();
        backend
            .create(&tenant, "Patient", json!({"id": "p2"}))
            .await
            .unwrap();
        backend
            .create(&tenant, "Observation", json!({"id": "o1"}))
            .await
            .unwrap();

        // Query for only Patient resources
        let pagination = Pagination::default();
        let result = backend
            .modified_since(&tenant, Some("Patient"), before_create, &pagination)
            .await
            .unwrap();

        // Should find only 2 patients
        assert_eq!(result.items.len(), 2);
        for resource in &result.items {
            assert_eq!(resource.resource_type(), "Patient");
        }
    }

    #[tokio::test]
    async fn test_modified_since_excludes_older() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create a resource
        backend
            .create(&tenant, "Patient", json!({"id": "old"}))
            .await
            .unwrap();

        // Wait a tiny bit and capture time
        let after_first = Utc::now();

        // Create another resource
        backend
            .create(&tenant, "Patient", json!({"id": "new"}))
            .await
            .unwrap();

        // Query for resources modified after the first creation
        let pagination = Pagination::default();
        let result = backend
            .modified_since(&tenant, None, after_first, &pagination)
            .await
            .unwrap();

        // Should find only the newer resource
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].id(), "new");
    }

    #[tokio::test]
    async fn test_modified_since_tenant_isolation() {
        let backend = create_test_backend();
        let tenant1 =
            TenantContext::new(TenantId::new("tenant-1"), TenantPermissions::full_access());
        let tenant2 =
            TenantContext::new(TenantId::new("tenant-2"), TenantPermissions::full_access());

        let before_create = Utc::now();

        // Create resources in both tenants
        backend
            .create(&tenant1, "Patient", json!({"id": "t1-p1"}))
            .await
            .unwrap();
        backend
            .create(&tenant2, "Patient", json!({"id": "t2-p1"}))
            .await
            .unwrap();

        // Query tenant 1
        let pagination = Pagination::default();
        let result1 = backend
            .modified_since(&tenant1, None, before_create, &pagination)
            .await
            .unwrap();
        assert_eq!(result1.items.len(), 1);
        assert_eq!(result1.items[0].id(), "t1-p1");

        // Query tenant 2
        let result2 = backend
            .modified_since(&tenant2, None, before_create, &pagination)
            .await
            .unwrap();
        assert_eq!(result2.items.len(), 1);
        assert_eq!(result2.items[0].id(), "t2-p1");
    }

    #[tokio::test]
    async fn test_modified_since_excludes_deleted() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        let before_create = Utc::now();

        // Create and then delete a resource
        backend
            .create(&tenant, "Patient", json!({"id": "del-p1"}))
            .await
            .unwrap();
        backend.delete(&tenant, "Patient", "del-p1").await.unwrap();

        // Create another resource
        backend
            .create(&tenant, "Patient", json!({"id": "live-p1"}))
            .await
            .unwrap();

        // Query - deleted resources should be excluded
        let pagination = Pagination::default();
        let result = backend
            .modified_since(&tenant, None, before_create, &pagination)
            .await
            .unwrap();

        // Should only find the live resource
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].id(), "live-p1");
    }

    #[tokio::test]
    async fn test_modified_since_pagination() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        let before_create = Utc::now();

        // Create multiple resources
        for i in 0..5 {
            backend
                .create(&tenant, "Patient", json!({"id": format!("p{}", i)}))
                .await
                .unwrap();
        }

        // Get first page (2 items)
        let pagination = Pagination::cursor().with_count(2);
        let page1 = backend
            .modified_since(&tenant, None, before_create, &pagination)
            .await
            .unwrap();

        assert_eq!(page1.items.len(), 2);
        assert!(page1.page_info.has_next);
    }

    #[tokio::test]
    async fn test_modified_since_empty() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Query with no resources
        let pagination = Pagination::default();
        let result = backend
            .modified_since(&tenant, None, Utc::now(), &pagination)
            .await
            .unwrap();

        assert!(result.items.is_empty());
        assert!(!result.page_info.has_next);
    }

    #[tokio::test]
    async fn test_modified_since_returns_current_version() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        let before_create = Utc::now();

        // Create a resource and update it multiple times
        let p1 = backend
            .create(&tenant, "Patient", json!({"id": "p1", "name": "v1"}))
            .await
            .unwrap();
        let p1_v2 = backend
            .update(&tenant, &p1, json!({"id": "p1", "name": "v2"}))
            .await
            .unwrap();
        let _p1_v3 = backend
            .update(&tenant, &p1_v2, json!({"id": "p1", "name": "v3"}))
            .await
            .unwrap();

        // Query - should return only the current (latest) version
        let pagination = Pagination::default();
        let result = backend
            .modified_since(&tenant, None, before_create, &pagination)
            .await
            .unwrap();

        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].version_id(), "3");
    }

    // ========================================================================
    // ConditionalStorage Tests
    // ========================================================================

    #[tokio::test]
    async fn test_conditional_create_no_match() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create with no matching resources
        let result = backend
            .conditional_create(
                &tenant,
                "Patient",
                json!({"identifier": [{"value": "12345"}]}),
                "identifier=99999", // No match
            )
            .await
            .unwrap();

        match result {
            ConditionalCreateResult::Created(resource) => {
                assert_eq!(resource.resource_type(), "Patient");
            }
            _ => panic!("Expected Created result"),
        }
    }

    #[tokio::test]
    async fn test_conditional_create_single_match() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create an existing resource
        let existing = backend
            .create(
                &tenant,
                "Patient",
                json!({"id": "p1", "identifier": [{"value": "12345"}]}),
            )
            .await
            .unwrap();

        // Conditional create with matching identifier
        let result = backend
            .conditional_create(
                &tenant,
                "Patient",
                json!({"identifier": [{"value": "12345"}]}),
                "identifier=12345",
            )
            .await
            .unwrap();

        match result {
            ConditionalCreateResult::Exists(resource) => {
                assert_eq!(resource.id(), existing.id());
            }
            _ => panic!("Expected Exists result"),
        }
    }

    #[tokio::test]
    async fn test_conditional_create_by_id() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create an existing resource
        backend
            .create(&tenant, "Patient", json!({"id": "p1"}))
            .await
            .unwrap();

        // Conditional create with _id parameter
        let result = backend
            .conditional_create(&tenant, "Patient", json!({}), "_id=p1")
            .await
            .unwrap();

        match result {
            ConditionalCreateResult::Exists(resource) => {
                assert_eq!(resource.id(), "p1");
            }
            _ => panic!("Expected Exists result"),
        }
    }

    #[tokio::test]
    async fn test_conditional_update_single_match() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create an existing resource
        backend
            .create(
                &tenant,
                "Patient",
                json!({"id": "p1", "identifier": [{"value": "12345"}], "active": false}),
            )
            .await
            .unwrap();

        // Conditional update
        let result = backend
            .conditional_update(
                &tenant,
                "Patient",
                json!({"id": "p1", "identifier": [{"value": "12345"}], "active": true}),
                "identifier=12345",
                false,
            )
            .await
            .unwrap();

        match result {
            ConditionalUpdateResult::Updated(resource) => {
                assert_eq!(resource.version_id(), "2");
            }
            _ => panic!("Expected Updated result"),
        }
    }

    #[tokio::test]
    async fn test_conditional_update_no_match_no_upsert() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Conditional update with no match and upsert=false
        let result = backend
            .conditional_update(
                &tenant,
                "Patient",
                json!({"identifier": [{"value": "99999"}]}),
                "identifier=99999",
                false,
            )
            .await
            .unwrap();

        match result {
            ConditionalUpdateResult::NoMatch => {}
            _ => panic!("Expected NoMatch result"),
        }
    }

    #[tokio::test]
    async fn test_conditional_update_no_match_with_upsert() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Conditional update with no match and upsert=true
        let result = backend
            .conditional_update(
                &tenant,
                "Patient",
                json!({"identifier": [{"value": "new-id"}]}),
                "identifier=new-id",
                true,
            )
            .await
            .unwrap();

        match result {
            ConditionalUpdateResult::Created(resource) => {
                assert_eq!(resource.resource_type(), "Patient");
            }
            _ => panic!("Expected Created result"),
        }
    }

    #[tokio::test]
    async fn test_conditional_delete_single_match() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create a resource
        backend
            .create(&tenant, "Patient", json!({"id": "p1"}))
            .await
            .unwrap();

        // Conditional delete
        let result = backend
            .conditional_delete(&tenant, "Patient", "_id=p1")
            .await
            .unwrap();

        match result {
            ConditionalDeleteResult::Deleted => {
                // Verify resource is deleted (read returns Gone error or None)
                let read_result = backend.read(&tenant, "Patient", "p1").await;
                match read_result {
                    Ok(None) => {} // Resource not found
                    Err(StorageError::Resource(ResourceError::Gone { .. })) => {} // Soft deleted
                    other => panic!("Expected None or Gone, got {:?}", other),
                }
            }
            _ => panic!("Expected Deleted result"),
        }
    }

    #[tokio::test]
    async fn test_conditional_delete_no_match() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Conditional delete with no match
        let result = backend
            .conditional_delete(&tenant, "Patient", "_id=nonexistent")
            .await
            .unwrap();

        match result {
            ConditionalDeleteResult::NoMatch => {}
            _ => panic!("Expected NoMatch result"),
        }
    }

    #[tokio::test]
    async fn test_conditional_operations_tenant_isolation() {
        let backend = create_test_backend();
        let tenant1 =
            TenantContext::new(TenantId::new("tenant-1"), TenantPermissions::full_access());
        let tenant2 =
            TenantContext::new(TenantId::new("tenant-2"), TenantPermissions::full_access());

        // Create resource in tenant 1
        backend
            .create(&tenant1, "Patient", json!({"id": "shared-id"}))
            .await
            .unwrap();

        // Conditional create in tenant 2 should not find tenant 1's resource
        let result = backend
            .conditional_create(&tenant2, "Patient", json!({}), "_id=shared-id")
            .await
            .unwrap();

        match result {
            ConditionalCreateResult::Created(_) => {}
            _ => panic!("Expected Created result (tenant isolation)"),
        }
    }

    // ========================================================================
    // BundleProvider Tests
    // ========================================================================

    #[tokio::test]
    async fn test_batch_create_multiple() {
        use crate::core::transaction::BundleProvider;

        let backend = create_test_backend();
        let tenant = create_test_tenant();

        let entries = vec![
            BundleEntry {
                method: BundleMethod::Post,
                url: "Patient".to_string(),
                resource: Some(json!({"resourceType": "Patient", "id": "batch-p1"})),
                if_match: None,
                if_none_match: None,
                if_none_exist: None,
            },
            BundleEntry {
                method: BundleMethod::Post,
                url: "Patient".to_string(),
                resource: Some(json!({"resourceType": "Patient", "id": "batch-p2"})),
                if_match: None,
                if_none_match: None,
                if_none_exist: None,
            },
        ];

        let result = backend.process_batch(&tenant, entries).await.unwrap();

        assert_eq!(result.entries.len(), 2);
        assert_eq!(result.entries[0].status, 201);
        assert_eq!(result.entries[1].status, 201);
    }

    #[tokio::test]
    async fn test_batch_mixed_operations() {
        use crate::core::transaction::BundleProvider;

        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create a resource first
        backend
            .create(&tenant, "Patient", json!({"id": "existing"}))
            .await
            .unwrap();

        let entries = vec![
            // Read existing
            BundleEntry {
                method: BundleMethod::Get,
                url: "Patient/existing".to_string(),
                resource: None,
                if_match: None,
                if_none_match: None,
                if_none_exist: None,
            },
            // Create new
            BundleEntry {
                method: BundleMethod::Post,
                url: "Patient".to_string(),
                resource: Some(json!({"resourceType": "Patient", "id": "new"})),
                if_match: None,
                if_none_match: None,
                if_none_exist: None,
            },
            // Read nonexistent
            BundleEntry {
                method: BundleMethod::Get,
                url: "Patient/nonexistent".to_string(),
                resource: None,
                if_match: None,
                if_none_match: None,
                if_none_exist: None,
            },
        ];

        let result = backend.process_batch(&tenant, entries).await.unwrap();

        assert_eq!(result.entries.len(), 3);
        assert_eq!(result.entries[0].status, 200); // Read existing
        assert_eq!(result.entries[1].status, 201); // Create new
        assert_eq!(result.entries[2].status, 404); // Read nonexistent
    }

    #[tokio::test]
    async fn test_batch_delete() {
        use crate::core::transaction::BundleProvider;

        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create a resource
        backend
            .create(&tenant, "Patient", json!({"id": "to-delete"}))
            .await
            .unwrap();

        let entries = vec![BundleEntry {
            method: BundleMethod::Delete,
            url: "Patient/to-delete".to_string(),
            resource: None,
            if_match: None,
            if_none_match: None,
            if_none_exist: None,
        }];

        let result = backend.process_batch(&tenant, entries).await.unwrap();

        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].status, 204);

        // Verify deletion (read returns Gone error or None)
        let read_result = backend.read(&tenant, "Patient", "to-delete").await;
        match read_result {
            Ok(None) => {} // Resource not found
            Err(StorageError::Resource(ResourceError::Gone { .. })) => {} // Soft deleted
            other => panic!("Expected None or Gone, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_transaction_all_or_nothing() {
        use crate::core::transaction::BundleProvider;

        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create a resource first
        backend
            .create(&tenant, "Patient", json!({"id": "existing"}))
            .await
            .unwrap();

        let entries = vec![
            // This should succeed
            BundleEntry {
                method: BundleMethod::Post,
                url: "Patient".to_string(),
                resource: Some(json!({"resourceType": "Patient", "id": "tx-p1"})),
                if_match: None,
                if_none_match: None,
                if_none_exist: None,
            },
            // This should fail (duplicate ID)
            BundleEntry {
                method: BundleMethod::Post,
                url: "Patient".to_string(),
                resource: Some(json!({"resourceType": "Patient", "id": "existing"})),
                if_match: None,
                if_none_match: None,
                if_none_exist: None,
            },
        ];

        let result = backend.process_transaction(&tenant, entries).await;

        // Should fail
        assert!(result.is_err());

        // First resource should NOT have been created (rollback)
        let read = backend.read(&tenant, "Patient", "tx-p1").await.unwrap();
        assert!(read.is_none());
    }

    #[tokio::test]
    async fn test_transaction_success() {
        use crate::core::transaction::BundleProvider;

        let backend = create_test_backend();
        let tenant = create_test_tenant();

        let entries = vec![
            BundleEntry {
                method: BundleMethod::Post,
                url: "Patient".to_string(),
                resource: Some(json!({"resourceType": "Patient", "id": "tx-success-1"})),
                if_match: None,
                if_none_match: None,
                if_none_exist: None,
            },
            BundleEntry {
                method: BundleMethod::Post,
                url: "Observation".to_string(),
                resource: Some(json!({"resourceType": "Observation", "id": "tx-success-2"})),
                if_match: None,
                if_none_match: None,
                if_none_exist: None,
            },
        ];

        let result = backend
            .process_transaction(&tenant, entries)
            .await
            .unwrap();

        assert_eq!(result.entries.len(), 2);
        assert_eq!(result.entries[0].status, 201);
        assert_eq!(result.entries[1].status, 201);

        // Both resources should exist
        assert!(backend
            .read(&tenant, "Patient", "tx-success-1")
            .await
            .unwrap()
            .is_some());
        assert!(backend
            .read(&tenant, "Observation", "tx-success-2")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn test_parse_url_formats() {
        let backend = create_test_backend();

        // Simple format
        let (rt, id) = backend.parse_url("Patient/123").unwrap();
        assert_eq!(rt, "Patient");
        assert_eq!(id, "123");

        // With leading slash
        let (rt, id) = backend.parse_url("/Patient/456").unwrap();
        assert_eq!(rt, "Patient");
        assert_eq!(id, "456");

        // Full URL
        let (rt, id) = backend
            .parse_url("http://example.com/fhir/Patient/789")
            .unwrap();
        assert_eq!(rt, "Patient");
        assert_eq!(id, "789");

        // HTTPS URL
        let (rt, id) = backend
            .parse_url("https://example.com/fhir/Observation/obs-1")
            .unwrap();
        assert_eq!(rt, "Observation");
        assert_eq!(id, "obs-1");
    }
}
