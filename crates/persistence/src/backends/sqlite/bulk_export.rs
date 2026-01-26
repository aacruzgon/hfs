//! Bulk export implementation for SQLite backend.

use async_trait::async_trait;
use chrono::Utc;
use rusqlite::params;
use serde_json::Value;

use crate::core::bulk_export::{
    BulkExportStorage, ExportDataProvider, ExportJobId, ExportLevel, ExportManifest,
    ExportOutputFile, ExportProgress, ExportRequest, ExportStatus, GroupExportProvider,
    NdjsonBatch, PatientExportProvider, TypeExportProgress,
};
use crate::error::{BackendError, BulkExportError, StorageError, StorageResult};
use crate::tenant::TenantContext;

use super::SqliteBackend;

fn internal_error(message: String) -> StorageError {
    StorageError::Backend(BackendError::Internal {
        backend_name: "sqlite".to_string(),
        message,
        source: None,
    })
}

#[async_trait]
impl BulkExportStorage for SqliteBackend {
    async fn start_export(
        &self,
        tenant: &TenantContext,
        request: ExportRequest,
    ) -> StorageResult<ExportJobId> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        // Check for too many concurrent exports (limit to 5 active exports per tenant)
        let active_count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM bulk_export_jobs
                 WHERE tenant_id = ?1 AND status IN ('accepted', 'in-progress')",
                params![tenant_id],
                |row| row.get(0),
            )
            .map_err(|e| internal_error(format!("Failed to count active exports: {}", e)))?;

        if active_count >= 5 {
            return Err(StorageError::BulkExport(
                BulkExportError::TooManyConcurrentExports { max_concurrent: 5 },
            ));
        }

        let job_id = ExportJobId::new();
        let now = Utc::now();
        let transaction_time = now.to_rfc3339();

        let level_str = match &request.level {
            ExportLevel::System => "system".to_string(),
            ExportLevel::Patient => "patient".to_string(),
            ExportLevel::Group { .. } => "group".to_string(),
        };

        let group_id = request.group_id().map(|s| s.to_string());

        let request_json = serde_json::to_string(&request)
            .map_err(|e| internal_error(format!("Failed to serialize request: {}", e)))?;

        conn.execute(
            "INSERT INTO bulk_export_jobs
             (id, tenant_id, status, level, group_id, request_json, transaction_time, created_at)
             VALUES (?1, ?2, 'accepted', ?3, ?4, ?5, ?6, ?7)",
            params![
                job_id.as_str(),
                tenant_id,
                level_str,
                group_id,
                request_json,
                transaction_time,
                transaction_time
            ],
        )
        .map_err(|e| internal_error(format!("Failed to create export job: {}", e)))?;

        Ok(job_id)
    }

    async fn get_export_status(
        &self,
        tenant: &TenantContext,
        job_id: &ExportJobId,
    ) -> StorageResult<ExportProgress> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        let (status_str, level_str, group_id, transaction_time, started_at, completed_at, error_message, current_type):
            (String, String, Option<String>, String, Option<String>, Option<String>, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT status, level, group_id, transaction_time, started_at, completed_at, error_message, current_type
                 FROM bulk_export_jobs
                 WHERE id = ?1 AND tenant_id = ?2",
                params![job_id.as_str(), tenant_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?,
                          row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?)),
            )
            .map_err(|e| {
                if matches!(e, rusqlite::Error::QueryReturnedNoRows) {
                    StorageError::BulkExport(BulkExportError::JobNotFound {
                        job_id: job_id.to_string(),
                    })
                } else {
                    internal_error(format!("Failed to get export status: {}", e))
                }
            })?;

        let status: ExportStatus = status_str
            .parse()
            .map_err(|_| internal_error(format!("Invalid status in database: {}", status_str)))?;

        let level = match level_str.as_str() {
            "system" => ExportLevel::System,
            "patient" => ExportLevel::Patient,
            "group" => ExportLevel::Group {
                group_id: group_id.unwrap_or_default(),
            },
            _ => {
                return Err(internal_error(format!(
                    "Invalid level in database: {}",
                    level_str
                )));
            }
        };

        let transaction_time = chrono::DateTime::parse_from_rfc3339(&transaction_time)
            .map_err(|e| internal_error(format!("Invalid transaction_time: {}", e)))?
            .with_timezone(&Utc);

        let started_at = started_at.and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(&s)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
        });

        let completed_at = completed_at.and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(&s)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
        });

        // Get per-type progress
        let mut stmt = conn
            .prepare(
                "SELECT resource_type, total_count, exported_count, error_count, cursor_state
                 FROM bulk_export_progress
                 WHERE job_id = ?1",
            )
            .map_err(|e| internal_error(format!("Failed to prepare progress query: {}", e)))?;

        let type_progress: Vec<TypeExportProgress> = stmt
            .query_map(params![job_id.as_str()], |row| {
                Ok(TypeExportProgress {
                    resource_type: row.get(0)?,
                    total_count: row.get::<_, Option<i64>>(1)?.map(|v| v as u64),
                    exported_count: row.get::<_, i64>(2)? as u64,
                    error_count: row.get::<_, i64>(3)? as u64,
                    cursor_state: row.get(4)?,
                })
            })
            .map_err(|e| internal_error(format!("Failed to query progress: {}", e)))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(ExportProgress {
            job_id: job_id.clone(),
            status,
            level,
            transaction_time,
            started_at,
            completed_at,
            type_progress,
            current_type,
            error_message,
        })
    }

    async fn cancel_export(
        &self,
        tenant: &TenantContext,
        job_id: &ExportJobId,
    ) -> StorageResult<()> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        // Check current status
        let current_status: String = conn
            .query_row(
                "SELECT status FROM bulk_export_jobs WHERE id = ?1 AND tenant_id = ?2",
                params![job_id.as_str(), tenant_id],
                |row| row.get(0),
            )
            .map_err(|e| {
                if matches!(e, rusqlite::Error::QueryReturnedNoRows) {
                    StorageError::BulkExport(BulkExportError::JobNotFound {
                        job_id: job_id.to_string(),
                    })
                } else {
                    internal_error(format!("Failed to get export status: {}", e))
                }
            })?;

        let status: ExportStatus = current_status.parse().map_err(|_| {
            internal_error(format!("Invalid status in database: {}", current_status))
        })?;

        if status.is_terminal() {
            return Err(StorageError::BulkExport(BulkExportError::InvalidJobState {
                job_id: job_id.to_string(),
                expected: "accepted or in-progress".to_string(),
                actual: current_status,
            }));
        }

        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE bulk_export_jobs SET status = 'cancelled', completed_at = ?1 WHERE id = ?2",
            params![now, job_id.as_str()],
        )
        .map_err(|e| internal_error(format!("Failed to cancel export: {}", e)))?;

        Ok(())
    }

    async fn delete_export(
        &self,
        tenant: &TenantContext,
        job_id: &ExportJobId,
    ) -> StorageResult<()> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        // Check exists
        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM bulk_export_jobs WHERE id = ?1 AND tenant_id = ?2",
                params![job_id.as_str(), tenant_id],
                |_| Ok(true),
            )
            .unwrap_or(false);

        if !exists {
            return Err(StorageError::BulkExport(BulkExportError::JobNotFound {
                job_id: job_id.to_string(),
            }));
        }

        // Delete job (cascades to progress and files due to foreign keys)
        conn.execute(
            "DELETE FROM bulk_export_jobs WHERE id = ?1 AND tenant_id = ?2",
            params![job_id.as_str(), tenant_id],
        )
        .map_err(|e| internal_error(format!("Failed to delete export: {}", e)))?;

        Ok(())
    }

    async fn get_export_manifest(
        &self,
        tenant: &TenantContext,
        job_id: &ExportJobId,
    ) -> StorageResult<ExportManifest> {
        let progress = self.get_export_status(tenant, job_id).await?;

        if progress.status != ExportStatus::Complete {
            return Err(StorageError::BulkExport(BulkExportError::InvalidJobState {
                job_id: job_id.to_string(),
                expected: "complete".to_string(),
                actual: progress.status.to_string(),
            }));
        }

        let conn = self.get_connection()?;

        // Get output files
        let mut stmt = conn
            .prepare(
                "SELECT resource_type, file_path, resource_count, file_type
                 FROM bulk_export_files
                 WHERE job_id = ?1
                 ORDER BY resource_type",
            )
            .map_err(|e| internal_error(format!("Failed to prepare files query: {}", e)))?;

        let mut output_files = Vec::new();
        let mut error_files = Vec::new();

        let rows = stmt
            .query_map(params![job_id.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<i64>>(2)?.map(|v| v as u64),
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(|e| internal_error(format!("Failed to query files: {}", e)))?;

        for row in rows {
            let (resource_type, file_path, count, file_type) =
                row.map_err(|e| internal_error(format!("Failed to read file row: {}", e)))?;

            let file = ExportOutputFile {
                resource_type,
                url: file_path,
                count,
            };

            if file_type == "error" {
                error_files.push(file);
            } else {
                output_files.push(file);
            }
        }

        Ok(ExportManifest {
            transaction_time: progress.transaction_time,
            request: format!("$export?job={}", job_id),
            requires_access_token: true,
            output: output_files,
            error: error_files,
            message: None,
            extension: None,
        })
    }

    async fn list_exports(
        &self,
        tenant: &TenantContext,
        include_completed: bool,
    ) -> StorageResult<Vec<ExportProgress>> {
        // Collect IDs first, then drop the connection before calling async methods
        let job_ids: Vec<String> = {
            let conn = self.get_connection()?;
            let tenant_id = tenant.tenant_id().as_str();

            let query = if include_completed {
                "SELECT id FROM bulk_export_jobs WHERE tenant_id = ?1 ORDER BY created_at DESC"
            } else {
                "SELECT id FROM bulk_export_jobs WHERE tenant_id = ?1 AND status IN ('accepted', 'in-progress') ORDER BY created_at DESC"
            };

            let mut stmt = conn
                .prepare(query)
                .map_err(|e| internal_error(format!("Failed to prepare list query: {}", e)))?;

            stmt.query_map(params![tenant_id], |row| row.get(0))
                .map_err(|e| internal_error(format!("Failed to query exports: {}", e)))?
                .filter_map(|r| r.ok())
                .collect()
        };

        let mut results = Vec::new();
        for id in job_ids {
            let job_id = ExportJobId::from_string(id);
            if let Ok(progress) = self.get_export_status(tenant, &job_id).await {
                results.push(progress);
            }
        }

        Ok(results)
    }
}

#[async_trait]
impl ExportDataProvider for SqliteBackend {
    async fn list_export_types(
        &self,
        tenant: &TenantContext,
        request: &ExportRequest,
    ) -> StorageResult<Vec<String>> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        // If specific types are requested, validate and return them
        if !request.resource_types.is_empty() {
            // Verify the types exist in the database
            let mut valid_types = Vec::new();
            for rt in &request.resource_types {
                let exists: bool = conn
                    .query_row(
                        "SELECT 1 FROM resources WHERE tenant_id = ?1 AND resource_type = ?2 AND is_deleted = 0 LIMIT 1",
                        params![tenant_id, rt],
                        |_| Ok(true),
                    )
                    .unwrap_or(false);
                if exists {
                    valid_types.push(rt.clone());
                }
            }
            return Ok(valid_types);
        }

        // Otherwise, get all types with data
        let mut stmt = conn
            .prepare(
                "SELECT DISTINCT resource_type FROM resources
                 WHERE tenant_id = ?1 AND is_deleted = 0
                 ORDER BY resource_type",
            )
            .map_err(|e| internal_error(format!("Failed to prepare types query: {}", e)))?;

        let types: Vec<String> = stmt
            .query_map(params![tenant_id], |row| row.get(0))
            .map_err(|e| internal_error(format!("Failed to query types: {}", e)))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(types)
    }

    async fn count_export_resources(
        &self,
        tenant: &TenantContext,
        request: &ExportRequest,
        resource_type: &str,
    ) -> StorageResult<u64> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        let mut query = "SELECT COUNT(*) FROM resources WHERE tenant_id = ?1 AND resource_type = ?2 AND is_deleted = 0".to_string();
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = vec![
            Box::new(tenant_id.to_string()),
            Box::new(resource_type.to_string()),
        ];

        // Apply _since filter if present
        if let Some(since) = request.since {
            query.push_str(" AND last_updated >= ?3");
            params_vec.push(Box::new(since.to_rfc3339()));
        }

        let params_slice: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();

        let count: i64 = conn
            .query_row(&query, params_slice.as_slice(), |row| row.get(0))
            .map_err(|e| internal_error(format!("Failed to count resources: {}", e)))?;

        Ok(count as u64)
    }

    async fn fetch_export_batch(
        &self,
        tenant: &TenantContext,
        request: &ExportRequest,
        resource_type: &str,
        cursor: Option<&str>,
        batch_size: u32,
    ) -> StorageResult<NdjsonBatch> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        let mut query = "SELECT id, data, last_updated FROM resources WHERE tenant_id = ?1 AND resource_type = ?2 AND is_deleted = 0".to_string();
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = vec![
            Box::new(tenant_id.to_string()),
            Box::new(resource_type.to_string()),
        ];

        // Apply _since filter if present
        if let Some(since) = request.since {
            query.push_str(" AND last_updated >= ?");
            params_vec.push(Box::new(since.to_rfc3339()));
        }

        // Apply cursor (keyset pagination)
        if let Some(cursor) = cursor {
            // Cursor format: "last_updated|id"
            let parts: Vec<&str> = cursor.splitn(2, '|').collect();
            if parts.len() == 2 {
                query.push_str(" AND (last_updated, id) > (?, ?)");
                params_vec.push(Box::new(parts[0].to_string()));
                params_vec.push(Box::new(parts[1].to_string()));
            }
        }

        query.push_str(" ORDER BY last_updated, id");
        query.push_str(&format!(" LIMIT {}", batch_size + 1)); // Fetch one extra to detect if there's more

        let params_slice: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();

        let mut stmt = conn
            .prepare(&query)
            .map_err(|e| internal_error(format!("Failed to prepare batch query: {}", e)))?;

        let rows: Vec<(String, Vec<u8>, String)> = stmt
            .query_map(params_slice.as_slice(), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|e| internal_error(format!("Failed to query batch: {}", e)))?
            .filter_map(|r| r.ok())
            .collect();

        let has_more = rows.len() > batch_size as usize;
        let rows = if has_more {
            &rows[..batch_size as usize]
        } else {
            &rows[..]
        };

        let mut lines = Vec::new();
        let mut last_cursor = None;

        for (id, data, last_updated) in rows {
            let resource: Value = serde_json::from_slice(data)
                .map_err(|e| internal_error(format!("Failed to parse resource: {}", e)))?;
            let line = serde_json::to_string(&resource)
                .map_err(|e| internal_error(format!("Failed to serialize resource: {}", e)))?;
            lines.push(line);
            last_cursor = Some(format!("{}|{}", last_updated, id));
        }

        Ok(NdjsonBatch {
            lines,
            next_cursor: if has_more { last_cursor } else { None },
            is_last: !has_more,
        })
    }
}

#[async_trait]
impl PatientExportProvider for SqliteBackend {
    async fn list_patient_ids(
        &self,
        tenant: &TenantContext,
        request: &ExportRequest,
        cursor: Option<&str>,
        batch_size: u32,
    ) -> StorageResult<(Vec<String>, Option<String>)> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        let mut query = "SELECT id FROM resources WHERE tenant_id = ?1 AND resource_type = 'Patient' AND is_deleted = 0".to_string();
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(tenant_id.to_string())];

        if let Some(since) = request.since {
            query.push_str(" AND last_updated >= ?");
            params_vec.push(Box::new(since.to_rfc3339()));
        }

        if let Some(cursor) = cursor {
            query.push_str(" AND id > ?");
            params_vec.push(Box::new(cursor.to_string()));
        }

        query.push_str(" ORDER BY id");
        query.push_str(&format!(" LIMIT {}", batch_size + 1));

        let params_slice: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();

        let mut stmt = conn
            .prepare(&query)
            .map_err(|e| internal_error(format!("Failed to prepare patient ids query: {}", e)))?;

        let ids: Vec<String> = stmt
            .query_map(params_slice.as_slice(), |row| row.get(0))
            .map_err(|e| internal_error(format!("Failed to query patient ids: {}", e)))?
            .filter_map(|r| r.ok())
            .collect();

        let has_more = ids.len() > batch_size as usize;
        let ids = if has_more {
            ids[..batch_size as usize].to_vec()
        } else {
            ids
        };

        let next_cursor = if has_more { ids.last().cloned() } else { None };

        Ok((ids, next_cursor))
    }

    async fn fetch_patient_compartment_batch(
        &self,
        tenant: &TenantContext,
        request: &ExportRequest,
        resource_type: &str,
        patient_ids: &[String],
        cursor: Option<&str>,
        batch_size: u32,
    ) -> StorageResult<NdjsonBatch> {
        if patient_ids.is_empty() {
            return Ok(NdjsonBatch::empty());
        }

        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        // For Patient resources, just filter by the IDs
        if resource_type == "Patient" {
            let placeholders: Vec<String> = (0..patient_ids.len())
                .map(|i| format!("?{}", i + 3))
                .collect();
            let mut query = format!(
                "SELECT id, data, last_updated FROM resources
                 WHERE tenant_id = ?1 AND resource_type = ?2 AND id IN ({}) AND is_deleted = 0",
                placeholders.join(",")
            );

            let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = vec![
                Box::new(tenant_id.to_string()),
                Box::new(resource_type.to_string()),
            ];
            for id in patient_ids {
                params_vec.push(Box::new(id.clone()));
            }

            if let Some(cursor) = cursor {
                let parts: Vec<&str> = cursor.splitn(2, '|').collect();
                if parts.len() == 2 {
                    query.push_str(" AND (last_updated, id) > (?, ?)");
                    params_vec.push(Box::new(parts[0].to_string()));
                    params_vec.push(Box::new(parts[1].to_string()));
                }
            }

            query.push_str(" ORDER BY last_updated, id");
            query.push_str(&format!(" LIMIT {}", batch_size + 1));

            let params_slice: Vec<&dyn rusqlite::ToSql> =
                params_vec.iter().map(|p| p.as_ref()).collect();

            let mut stmt = conn.prepare(&query).map_err(|e| {
                internal_error(format!("Failed to prepare compartment query: {}", e))
            })?;

            let rows: Vec<(String, Vec<u8>, String)> = stmt
                .query_map(params_slice.as_slice(), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(|e| internal_error(format!("Failed to query compartment: {}", e)))?
                .filter_map(|r| r.ok())
                .collect();

            let has_more = rows.len() > batch_size as usize;
            let rows = if has_more {
                &rows[..batch_size as usize]
            } else {
                &rows[..]
            };

            let mut lines = Vec::new();
            let mut last_cursor = None;

            for (id, data, last_updated) in rows {
                let resource: Value = serde_json::from_slice(data)
                    .map_err(|e| internal_error(format!("Failed to parse resource: {}", e)))?;
                let line = serde_json::to_string(&resource)
                    .map_err(|e| internal_error(format!("Failed to serialize resource: {}", e)))?;
                lines.push(line);
                last_cursor = Some(format!("{}|{}", last_updated, id));
            }

            return Ok(NdjsonBatch {
                lines,
                next_cursor: if has_more { last_cursor } else { None },
                is_last: !has_more,
            });
        }

        // For other resources, we need to use the search index to find resources
        // that reference these patients via subject/patient parameters
        let patient_refs: Vec<String> = patient_ids
            .iter()
            .map(|id| format!("Patient/{}", id))
            .collect();
        let placeholders: Vec<String> = (0..patient_refs.len())
            .map(|i| format!("?{}", i + 4))
            .collect();

        let mut query = format!(
            "SELECT DISTINCT r.id, r.data, r.last_updated
             FROM resources r
             INNER JOIN search_index si ON r.tenant_id = si.tenant_id
                AND r.resource_type = si.resource_type
                AND r.id = si.resource_id
             WHERE r.tenant_id = ?1
                AND r.resource_type = ?2
                AND r.is_deleted = 0
                AND si.param_name IN ('subject', 'patient')
                AND si.value_reference IN ({})",
            placeholders.join(",")
        );

        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = vec![
            Box::new(tenant_id.to_string()),
            Box::new(resource_type.to_string()),
        ];
        // Placeholder for since filter slot
        let since_value = request.since.map(|s| s.to_rfc3339());
        if since_value.is_some() {
            params_vec.push(Box::new(since_value.clone().unwrap()));
        }
        for patient_ref in &patient_refs {
            params_vec.push(Box::new(patient_ref.clone()));
        }

        if request.since.is_some() {
            query = query.replace(
                "r.is_deleted = 0",
                "r.is_deleted = 0 AND r.last_updated >= ?3",
            );
        }

        if let Some(cursor) = cursor {
            let parts: Vec<&str> = cursor.splitn(2, '|').collect();
            if parts.len() == 2 {
                query.push_str(" AND (r.last_updated, r.id) > (?, ?)");
                params_vec.push(Box::new(parts[0].to_string()));
                params_vec.push(Box::new(parts[1].to_string()));
            }
        }

        query.push_str(" ORDER BY r.last_updated, r.id");
        query.push_str(&format!(" LIMIT {}", batch_size + 1));

        let params_slice: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();

        let mut stmt = conn
            .prepare(&query)
            .map_err(|e| internal_error(format!("Failed to prepare compartment query: {}", e)))?;

        let rows: Vec<(String, Vec<u8>, String)> = stmt
            .query_map(params_slice.as_slice(), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|e| internal_error(format!("Failed to query compartment: {}", e)))?
            .filter_map(|r| r.ok())
            .collect();

        let has_more = rows.len() > batch_size as usize;
        let rows = if has_more {
            &rows[..batch_size as usize]
        } else {
            &rows[..]
        };

        let mut lines = Vec::new();
        let mut last_cursor = None;

        for (id, data, last_updated) in rows {
            let resource: Value = serde_json::from_slice(data)
                .map_err(|e| internal_error(format!("Failed to parse resource: {}", e)))?;
            let line = serde_json::to_string(&resource)
                .map_err(|e| internal_error(format!("Failed to serialize resource: {}", e)))?;
            lines.push(line);
            last_cursor = Some(format!("{}|{}", last_updated, id));
        }

        Ok(NdjsonBatch {
            lines,
            next_cursor: if has_more { last_cursor } else { None },
            is_last: !has_more,
        })
    }
}

#[async_trait]
impl GroupExportProvider for SqliteBackend {
    async fn get_group_members(
        &self,
        tenant: &TenantContext,
        group_id: &str,
    ) -> StorageResult<Vec<String>> {
        let conn = self.get_connection()?;
        let tenant_id = tenant.tenant_id().as_str();

        // Get the Group resource
        let data: Vec<u8> = conn
            .query_row(
                "SELECT data FROM resources WHERE tenant_id = ?1 AND resource_type = 'Group' AND id = ?2 AND is_deleted = 0",
                params![tenant_id, group_id],
                |row| row.get(0),
            )
            .map_err(|e| {
                if matches!(e, rusqlite::Error::QueryReturnedNoRows) {
                    StorageError::BulkExport(BulkExportError::GroupNotFound {
                        group_id: group_id.to_string(),
                    })
                } else {
                    internal_error(format!("Failed to get group: {}", e))
                }
            })?;

        let group: Value = serde_json::from_slice(&data)
            .map_err(|e| internal_error(format!("Failed to parse group: {}", e)))?;

        // Extract member references from Group.member[].entity.reference
        let mut members = Vec::new();
        if let Some(member_array) = group.get("member").and_then(|m| m.as_array()) {
            for member in member_array {
                if let Some(entity) = member.get("entity") {
                    if let Some(reference) = entity.get("reference").and_then(|r| r.as_str()) {
                        members.push(reference.to_string());
                    }
                }
            }
        }

        Ok(members)
    }

    async fn resolve_group_patient_ids(
        &self,
        tenant: &TenantContext,
        group_id: &str,
    ) -> StorageResult<Vec<String>> {
        let members = self.get_group_members(tenant, group_id).await?;

        // Filter to only Patient references and extract IDs
        let patient_ids: Vec<String> = members
            .into_iter()
            .filter_map(|reference| {
                if reference.starts_with("Patient/") {
                    Some(reference.strip_prefix("Patient/").unwrap().to_string())
                } else {
                    None
                }
            })
            .collect();

        Ok(patient_ids)
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
    async fn test_start_export() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        let request = ExportRequest::system().with_types(vec!["Patient".to_string()]);
        let job_id = backend.start_export(&tenant, request).await.unwrap();

        assert!(!job_id.as_str().is_empty());

        let progress = backend.get_export_status(&tenant, &job_id).await.unwrap();
        assert_eq!(progress.status, ExportStatus::Accepted);
    }

    #[tokio::test]
    async fn test_cancel_export() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        let request = ExportRequest::system();
        let job_id = backend.start_export(&tenant, request).await.unwrap();

        backend.cancel_export(&tenant, &job_id).await.unwrap();

        let progress = backend.get_export_status(&tenant, &job_id).await.unwrap();
        assert_eq!(progress.status, ExportStatus::Cancelled);
    }

    #[tokio::test]
    async fn test_list_exports() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create two exports
        let request1 = ExportRequest::system();
        let _job_id1 = backend.start_export(&tenant, request1).await.unwrap();

        let request2 = ExportRequest::patient();
        let _job_id2 = backend.start_export(&tenant, request2).await.unwrap();

        let exports = backend.list_exports(&tenant, false).await.unwrap();
        assert_eq!(exports.len(), 2);
    }

    #[tokio::test]
    async fn test_too_many_concurrent_exports() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create 5 exports (the limit)
        for _ in 0..5 {
            let request = ExportRequest::system();
            backend.start_export(&tenant, request).await.unwrap();
        }

        // Sixth should fail
        let request = ExportRequest::system();
        let result = backend.start_export(&tenant, request).await;
        assert!(matches!(
            result,
            Err(StorageError::BulkExport(
                BulkExportError::TooManyConcurrentExports { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn test_list_export_types() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create some resources
        backend
            .create(
                &tenant,
                "Patient",
                json!({"resourceType": "Patient", "name": [{"family": "Test"}]}),
            )
            .await
            .unwrap();

        backend
            .create(
                &tenant,
                "Observation",
                json!({"resourceType": "Observation", "status": "final"}),
            )
            .await
            .unwrap();

        let request = ExportRequest::system();
        let types = backend.list_export_types(&tenant, &request).await.unwrap();

        assert!(types.contains(&"Patient".to_string()));
        assert!(types.contains(&"Observation".to_string()));
    }

    #[tokio::test]
    async fn test_fetch_export_batch() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        // Create some resources
        for i in 0..5 {
            backend
                .create(
                    &tenant,
                    "Patient",
                    json!({"resourceType": "Patient", "name": [{"family": format!("Patient{}", i)}]}),
                )
                .await
                .unwrap();
        }

        let request = ExportRequest::system();
        let batch = backend
            .fetch_export_batch(&tenant, &request, "Patient", None, 3)
            .await
            .unwrap();

        assert_eq!(batch.lines.len(), 3);
        assert!(!batch.is_last);
        assert!(batch.next_cursor.is_some());

        // Fetch next batch
        let batch2 = backend
            .fetch_export_batch(
                &tenant,
                &request,
                "Patient",
                batch.next_cursor.as_deref(),
                3,
            )
            .await
            .unwrap();

        assert_eq!(batch2.lines.len(), 2);
        assert!(batch2.is_last);
    }

    #[tokio::test]
    async fn test_delete_export() {
        let backend = create_test_backend();
        let tenant = create_test_tenant();

        let request = ExportRequest::system();
        let job_id = backend.start_export(&tenant, request).await.unwrap();

        backend.delete_export(&tenant, &job_id).await.unwrap();

        // Should fail to get status now
        let result = backend.get_export_status(&tenant, &job_id).await;
        assert!(matches!(
            result,
            Err(StorageError::BulkExport(
                BulkExportError::JobNotFound { .. }
            ))
        ));
    }
}
