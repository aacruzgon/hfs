//! Search implementation for SQLite backend.
//!
//! This module provides basic search functionality for the SQLite backend.
//! Full search support is a work in progress.

use async_trait::async_trait;
use chrono::Utc;
use rusqlite::params;

use crate::core::{SearchProvider, SearchResult};
use crate::error::{BackendError, StorageError, StorageResult};
use crate::tenant::TenantContext;
use crate::types::{Page, PageInfo, SearchQuery, StoredResource};

use super::SqliteBackend;

fn internal_error(message: String) -> StorageError {
    StorageError::Backend(BackendError::Internal {
        backend_name: "sqlite".to_string(),
        message,
        source: None,
    })
}

#[async_trait]
impl SearchProvider for SqliteBackend {
    async fn search(
        &self,
        tenant: &TenantContext,
        query: &SearchQuery,
    ) -> StorageResult<SearchResult> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();
        let resource_type = &query.resource_type;

        // Get count and offset with defaults
        let count = query.count.unwrap_or(100) as usize;
        let offset = query.offset.unwrap_or(0) as usize;

        // Basic implementation - just return all resources of the type
        // Full search parameter support is TODO
        let sql = format!(
            "SELECT id, version_id, data, last_updated FROM resources
             WHERE tenant_id = ?1 AND resource_type = ?2 AND is_deleted = 0
             ORDER BY last_updated DESC
             LIMIT {} OFFSET {}",
            count + 1, // Fetch one extra to check if there are more
            offset
        );

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| internal_error(format!("Failed to prepare search query: {}", e)))?;

        let rows = stmt
            .query_map(params![tenant_id, resource_type], |row| {
                let id: String = row.get(0)?;
                let version_id: String = row.get(1)?;
                let data: Vec<u8> = row.get(2)?;
                let last_updated: String = row.get(3)?;
                Ok((id, version_id, data, last_updated))
            })
            .map_err(|e| internal_error(format!("Failed to execute search: {}", e)))?;

        let mut resources = Vec::new();
        for row in rows {
            let (id, version_id, data, last_updated) =
                row.map_err(|e| internal_error(format!("Failed to read row: {}", e)))?;

            let json_data: serde_json::Value = serde_json::from_slice(&data)
                .map_err(|e| internal_error(format!("Failed to deserialize resource: {}", e)))?;

            let last_updated = chrono::DateTime::parse_from_rfc3339(&last_updated)
                .map_err(|e| internal_error(format!("Failed to parse last_updated: {}", e)))?
                .with_timezone(&Utc);

            let resource = StoredResource::from_storage(
                resource_type.clone(),
                id,
                version_id,
                tenant.tenant_id().clone(),
                json_data,
                last_updated, // Use last_updated as created_at (we don't track created_at separately)
                last_updated,
                None, // Not deleted
            );

            resources.push(resource);
        }

        // Check if there are more results (we fetched one extra)
        let has_next = resources.len() > count;
        if has_next {
            resources.pop(); // Remove the extra one
        }

        let page_info = PageInfo {
            next_cursor: None, // Cursor-based pagination TODO
            previous_cursor: None,
            total: None,
            has_next,
            has_previous: offset > 0,
        };

        let page = Page::new(resources, page_info);

        Ok(SearchResult {
            resources: page,
            included: Vec::new(),
            total: None,
        })
    }

    async fn search_count(
        &self,
        tenant: &TenantContext,
        query: &SearchQuery,
    ) -> StorageResult<u64> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();
        let resource_type = &query.resource_type;

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resources WHERE tenant_id = ?1 AND resource_type = ?2 AND is_deleted = 0",
                params![tenant_id, resource_type],
                |row| row.get(0),
            )
            .map_err(|e| internal_error(format!("Failed to count resources: {}", e)))?;

        Ok(count as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ResourceStorage;
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
    async fn test_search_empty() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        let query = SearchQuery::new("Patient");
        let result = backend.search(&tenant, &query).await.unwrap();

        assert!(result.resources.items.is_empty());
    }

    #[tokio::test]
    async fn test_search_returns_resources() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create some resources
        backend.create(&tenant, "Patient", json!({})).await.unwrap();
        backend.create(&tenant, "Patient", json!({})).await.unwrap();

        let query = SearchQuery::new("Patient");
        let result = backend.search(&tenant, &query).await.unwrap();

        assert_eq!(result.resources.items.len(), 2);
    }

    #[tokio::test]
    async fn test_search_count() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        backend.create(&tenant, "Patient", json!({})).await.unwrap();
        backend.create(&tenant, "Patient", json!({})).await.unwrap();
        backend
            .create(&tenant, "Observation", json!({}))
            .await
            .unwrap();

        let query = SearchQuery::new("Patient");
        let count = backend.search_count(&tenant, &query).await.unwrap();

        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_search_tenant_isolation() {
        let backend = create_test_backend();

        let tenant1 =
            TenantContext::new(TenantId::new("tenant-1"), TenantPermissions::full_access());
        let tenant2 =
            TenantContext::new(TenantId::new("tenant-2"), TenantPermissions::full_access());

        backend
            .create(&tenant1, "Patient", json!({}))
            .await
            .unwrap();
        backend
            .create(&tenant2, "Patient", json!({}))
            .await
            .unwrap();
        backend
            .create(&tenant2, "Patient", json!({}))
            .await
            .unwrap();

        let query = SearchQuery::new("Patient");

        let result1 = backend.search(&tenant1, &query).await.unwrap();
        assert_eq!(result1.resources.items.len(), 1);

        let result2 = backend.search(&tenant2, &query).await.unwrap();
        assert_eq!(result2.resources.items.len(), 2);
    }
}
