//! Bulk export implementation for PostgreSQL backend.

use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;

use crate::core::bulk_export::{
    BulkExportStorage, ExportDataProvider, ExportJobId, ExportLevel, ExportManifest,
    ExportOutputFile, ExportProgress, ExportRequest, ExportStatus, GroupExportProvider,
    NdjsonBatch, PatientExportProvider, TypeExportProgress,
};
use crate::error::{BackendError, BulkExportError, StorageError, StorageResult};
use crate::tenant::TenantContext;

use super::PostgresBackend;

fn internal_error(message: String) -> StorageError {
    StorageError::Backend(BackendError::Internal {
        backend_name: "postgres".to_string(),
        message,
        source: None,
    })
}

#[async_trait]
impl BulkExportStorage for PostgresBackend {
    async fn start_export(
        &self,
        tenant: &TenantContext,
        request: ExportRequest,
    ) -> StorageResult<ExportJobId> {
        let client = self.get_client().await?;
        let tenant_id = tenant.tenant_id().as_str();

        // Check for too many concurrent exports (limit to 5 active exports per tenant)
        let row = client
            .query_one(
                "SELECT COUNT(*) FROM bulk_export_jobs
                 WHERE tenant_id = $1 AND status IN ('accepted', 'in-progress')",
                &[&tenant_id],
            )
            .await
            .map_err(|e| internal_error(format!("Failed to count active exports: {}", e)))?;

        let active_count: i64 = row.get(0);
        if active_count >= 5 {
            return Err(StorageError::BulkExport(
                BulkExportError::TooManyConcurrentExports { max_concurrent: 5 },
            ));
        }

        let job_id = ExportJobId::new();
        let now = Utc::now();

        let level_str = match &request.level {
            ExportLevel::System => "system".to_string(),
            ExportLevel::Patient => "patient".to_string(),
            ExportLevel::Group { .. } => "group".to_string(),
        };

        let group_id = request.group_id().map(|s| s.to_string());

        let request_json = serde_json::to_string(&request)
            .map_err(|e| internal_error(format!("Failed to serialize request: {}", e)))?;

        client
            .execute(
                "INSERT INTO bulk_export_jobs
                 (id, tenant_id, status, level, group_id, request_json, transaction_time, created_at)
                 VALUES ($1, $2, 'accepted', $3, $4, $5, $6, $7)",
                &[
                    &job_id.as_str(),
                    &tenant_id,
                    &level_str.as_str(),
                    &group_id,
                    &request_json.as_str(),
                    &now,
                    &now,
                ],
            )
            .await
            .map_err(|e| internal_error(format!("Failed to create export job: {}", e)))?;

        Ok(job_id)
    }

    async fn get_export_status(
        &self,
        tenant: &TenantContext,
        job_id: &ExportJobId,
    ) -> StorageResult<ExportProgress> {
        let client = self.get_client().await?;
        let tenant_id = tenant.tenant_id().as_str();

        let rows = client
            .query(
                "SELECT status, level, group_id, transaction_time, started_at, completed_at, error_message, current_type
                 FROM bulk_export_jobs
                 WHERE id = $1 AND tenant_id = $2",
                &[&job_id.as_str(), &tenant_id],
            )
            .await
            .map_err(|e| internal_error(format!("Failed to get export status: {}", e)))?;

        if rows.is_empty() {
            return Err(StorageError::BulkExport(BulkExportError::JobNotFound {
                job_id: job_id.to_string(),
            }));
        }

        let row = &rows[0];
        let status_str: String = row.get(0);
        let level_str: String = row.get(1);
        let group_id: Option<String> = row.get(2);
        let transaction_time: chrono::DateTime<Utc> = row.get(3);
        let started_at: Option<chrono::DateTime<Utc>> = row.get(4);
        let completed_at: Option<chrono::DateTime<Utc>> = row.get(5);
        let error_message: Option<String> = row.get(6);
        let current_type: Option<String> = row.get(7);

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

        // Get per-type progress
        let progress_rows = client
            .query(
                "SELECT resource_type, total_count, exported_count, error_count, cursor_state
                 FROM bulk_export_progress
                 WHERE job_id = $1",
                &[&job_id.as_str()],
            )
            .await
            .map_err(|e| internal_error(format!("Failed to query progress: {}", e)))?;

        let type_progress: Vec<TypeExportProgress> = progress_rows
            .iter()
            .map(|r| TypeExportProgress {
                resource_type: r.get(0),
                total_count: r.get::<_, Option<i64>>(1).map(|v| v as u64),
                exported_count: r.get::<_, i64>(2) as u64,
                error_count: r.get::<_, i64>(3) as u64,
                cursor_state: r.get(4),
            })
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
        let client = self.get_client().await?;
        let tenant_id = tenant.tenant_id().as_str();

        let rows = client
            .query(
                "SELECT status FROM bulk_export_jobs WHERE id = $1 AND tenant_id = $2",
                &[&job_id.as_str(), &tenant_id],
            )
            .await
            .map_err(|e| internal_error(format!("Failed to get export status: {}", e)))?;

        if rows.is_empty() {
            return Err(StorageError::BulkExport(BulkExportError::JobNotFound {
                job_id: job_id.to_string(),
            }));
        }

        let current_status: String = rows[0].get(0);
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

        let now = Utc::now();
        client
            .execute(
                "UPDATE bulk_export_jobs SET status = 'cancelled', completed_at = $1 WHERE id = $2",
                &[&now, &job_id.as_str()],
            )
            .await
            .map_err(|e| internal_error(format!("Failed to cancel export: {}", e)))?;

        Ok(())
    }

    async fn delete_export(
        &self,
        tenant: &TenantContext,
        job_id: &ExportJobId,
    ) -> StorageResult<()> {
        let client = self.get_client().await?;
        let tenant_id = tenant.tenant_id().as_str();

        let result = client
            .execute(
                "DELETE FROM bulk_export_jobs WHERE id = $1 AND tenant_id = $2",
                &[&job_id.as_str(), &tenant_id],
            )
            .await
            .map_err(|e| internal_error(format!("Failed to delete export: {}", e)))?;

        if result == 0 {
            return Err(StorageError::BulkExport(BulkExportError::JobNotFound {
                job_id: job_id.to_string(),
            }));
        }

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

        let client = self.get_client().await?;

        let rows = client
            .query(
                "SELECT resource_type, file_path, resource_count, file_type
                 FROM bulk_export_files
                 WHERE job_id = $1
                 ORDER BY resource_type",
                &[&job_id.as_str()],
            )
            .await
            .map_err(|e| internal_error(format!("Failed to query files: {}", e)))?;

        let mut output_files = Vec::new();
        let mut error_files = Vec::new();

        for row in &rows {
            let resource_type: String = row.get(0);
            let file_path: String = row.get(1);
            let count: Option<i64> = row.get(2);
            let file_type: String = row.get(3);

            let file = ExportOutputFile {
                resource_type,
                url: file_path,
                count: count.map(|c| c as u64),
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
        let client = self.get_client().await?;
        let tenant_id = tenant.tenant_id().as_str();

        let query = if include_completed {
            "SELECT id FROM bulk_export_jobs WHERE tenant_id = $1 ORDER BY created_at DESC"
        } else {
            "SELECT id FROM bulk_export_jobs WHERE tenant_id = $1 AND status IN ('accepted', 'in-progress') ORDER BY created_at DESC"
        };

        let rows = client
            .query(query, &[&tenant_id])
            .await
            .map_err(|e| internal_error(format!("Failed to query exports: {}", e)))?;

        let mut results = Vec::new();
        for row in &rows {
            let id: String = row.get(0);
            let job_id = ExportJobId::from_string(id);
            if let Ok(progress) = self.get_export_status(tenant, &job_id).await {
                results.push(progress);
            }
        }

        Ok(results)
    }
}

#[async_trait]
impl ExportDataProvider for PostgresBackend {
    async fn list_export_types(
        &self,
        tenant: &TenantContext,
        request: &ExportRequest,
    ) -> StorageResult<Vec<String>> {
        let client = self.get_client().await?;
        let tenant_id = tenant.tenant_id().as_str();

        if !request.resource_types.is_empty() {
            let mut valid_types = Vec::new();
            for rt in &request.resource_types {
                let row = client
                    .query_one(
                        "SELECT EXISTS(SELECT 1 FROM resources WHERE tenant_id = $1 AND resource_type = $2 AND is_deleted = FALSE LIMIT 1)",
                        &[&tenant_id, &rt.as_str()],
                    )
                    .await
                    .map_err(|e| internal_error(format!("Failed to check type: {}", e)))?;

                let exists: bool = row.get(0);
                if exists {
                    valid_types.push(rt.clone());
                }
            }
            return Ok(valid_types);
        }

        let rows = client
            .query(
                "SELECT DISTINCT resource_type FROM resources
                 WHERE tenant_id = $1 AND is_deleted = FALSE
                 ORDER BY resource_type",
                &[&tenant_id],
            )
            .await
            .map_err(|e| internal_error(format!("Failed to query types: {}", e)))?;

        let types: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
        Ok(types)
    }

    async fn count_export_resources(
        &self,
        tenant: &TenantContext,
        request: &ExportRequest,
        resource_type: &str,
    ) -> StorageResult<u64> {
        let client = self.get_client().await?;
        let tenant_id = tenant.tenant_id().as_str();

        let (sql, params): (
            String,
            Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
        ) = if let Some(since) = request.since {
            (
                "SELECT COUNT(*) FROM resources WHERE tenant_id = $1 AND resource_type = $2 AND is_deleted = FALSE AND last_updated >= $3".to_string(),
                vec![
                    Box::new(tenant_id.to_string()),
                    Box::new(resource_type.to_string()),
                    Box::new(since),
                ],
            )
        } else {
            (
                "SELECT COUNT(*) FROM resources WHERE tenant_id = $1 AND resource_type = $2 AND is_deleted = FALSE".to_string(),
                vec![
                    Box::new(tenant_id.to_string()),
                    Box::new(resource_type.to_string()),
                ],
            )
        };

        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();

        let row = client
            .query_one(&sql, &param_refs)
            .await
            .map_err(|e| internal_error(format!("Failed to count resources: {}", e)))?;

        let count: i64 = row.get(0);
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
        let client = self.get_client().await?;
        let tenant_id = tenant.tenant_id().as_str();

        let mut sql = "SELECT id, data, last_updated FROM resources WHERE tenant_id = $1 AND resource_type = $2 AND is_deleted = FALSE".to_string();
        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = vec![
            Box::new(tenant_id.to_string()),
            Box::new(resource_type.to_string()),
        ];
        let mut param_idx = 3;

        if let Some(since) = request.since {
            sql.push_str(&format!(" AND last_updated >= ${}", param_idx));
            params.push(Box::new(since));
            param_idx += 1;
        }

        if let Some(cursor) = cursor {
            let parts: Vec<&str> = cursor.splitn(2, '|').collect();
            if parts.len() == 2 {
                sql.push_str(&format!(
                    " AND (last_updated, id) > (${}, ${})",
                    param_idx,
                    param_idx + 1
                ));
                params.push(Box::new(parts[0].to_string()));
                params.push(Box::new(parts[1].to_string()));
            }
        }

        sql.push_str(&format!(
            " ORDER BY last_updated, id LIMIT {}",
            batch_size + 1
        ));

        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();

        let rows = client
            .query(&sql, &param_refs)
            .await
            .map_err(|e| internal_error(format!("Failed to query batch: {}", e)))?;

        let has_more = rows.len() > batch_size as usize;
        let rows_to_process = if has_more {
            &rows[..batch_size as usize]
        } else {
            &rows[..]
        };

        let mut lines = Vec::new();
        let mut last_cursor = None;

        for row in rows_to_process {
            let id: String = row.get(0);
            let resource: Value = row.get(1);
            let last_updated: chrono::DateTime<Utc> = row.get(2);

            let line = serde_json::to_string(&resource)
                .map_err(|e| internal_error(format!("Failed to serialize resource: {}", e)))?;
            lines.push(line);
            last_cursor = Some(format!("{}|{}", last_updated.to_rfc3339(), id));
        }

        Ok(NdjsonBatch {
            lines,
            next_cursor: if has_more { last_cursor } else { None },
            is_last: !has_more,
        })
    }
}

#[async_trait]
impl PatientExportProvider for PostgresBackend {
    async fn list_patient_ids(
        &self,
        tenant: &TenantContext,
        request: &ExportRequest,
        cursor: Option<&str>,
        batch_size: u32,
    ) -> StorageResult<(Vec<String>, Option<String>)> {
        let client = self.get_client().await?;
        let tenant_id = tenant.tenant_id().as_str();

        let mut sql = "SELECT id FROM resources WHERE tenant_id = $1 AND resource_type = 'Patient' AND is_deleted = FALSE".to_string();
        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
            vec![Box::new(tenant_id.to_string())];
        let mut param_idx = 2;

        if let Some(since) = request.since {
            sql.push_str(&format!(" AND last_updated >= ${}", param_idx));
            params.push(Box::new(since));
            param_idx += 1;
        }

        if let Some(cursor) = cursor {
            sql.push_str(&format!(" AND id > ${}", param_idx));
            params.push(Box::new(cursor.to_string()));
        }

        sql.push_str(&format!(" ORDER BY id LIMIT {}", batch_size + 1));

        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();

        let rows = client
            .query(&sql, &param_refs)
            .await
            .map_err(|e| internal_error(format!("Failed to query patient ids: {}", e)))?;

        let mut ids: Vec<String> = rows.iter().map(|r| r.get(0)).collect();

        let has_more = ids.len() > batch_size as usize;
        if has_more {
            ids.truncate(batch_size as usize);
        }

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

        let client = self.get_client().await?;
        let tenant_id = tenant.tenant_id().as_str();

        if resource_type == "Patient" {
            // For Patient resources, just filter by the IDs using ANY($3::text[])
            let mut sql = "SELECT id, data, last_updated FROM resources
                 WHERE tenant_id = $1 AND resource_type = $2 AND id = ANY($3::text[]) AND is_deleted = FALSE".to_string();

            let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = vec![
                Box::new(tenant_id.to_string()),
                Box::new(resource_type.to_string()),
                Box::new(patient_ids.to_vec()),
            ];
            let param_idx = 4;

            if let Some(cursor) = cursor {
                let parts: Vec<&str> = cursor.splitn(2, '|').collect();
                if parts.len() == 2 {
                    sql.push_str(&format!(
                        " AND (last_updated, id) > (${}, ${})",
                        param_idx,
                        param_idx + 1
                    ));
                    params.push(Box::new(parts[0].to_string()));
                    params.push(Box::new(parts[1].to_string()));
                }
            }

            sql.push_str(&format!(
                " ORDER BY last_updated, id LIMIT {}",
                batch_size + 1
            ));

            let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
                .iter()
                .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();

            let rows = client
                .query(&sql, &param_refs)
                .await
                .map_err(|e| internal_error(format!("Failed to query compartment: {}", e)))?;

            let has_more = rows.len() > batch_size as usize;
            let rows_slice = if has_more {
                &rows[..batch_size as usize]
            } else {
                &rows[..]
            };

            let mut lines = Vec::new();
            let mut last_cursor = None;

            for row in rows_slice {
                let id: String = row.get(0);
                let resource: Value = row.get(1);
                let last_updated: chrono::DateTime<Utc> = row.get(2);

                let line = serde_json::to_string(&resource)
                    .map_err(|e| internal_error(format!("Failed to serialize: {}", e)))?;
                lines.push(line);
                last_cursor = Some(format!("{}|{}", last_updated.to_rfc3339(), id));
            }

            return Ok(NdjsonBatch {
                lines,
                next_cursor: if has_more { last_cursor } else { None },
                is_last: !has_more,
            });
        }

        // For other resources, use search index to find resources referencing these patients
        let patient_refs: Vec<String> = patient_ids
            .iter()
            .map(|id| format!("Patient/{}", id))
            .collect();

        let mut sql = "SELECT DISTINCT r.id, r.data, r.last_updated
             FROM resources r
             INNER JOIN search_index si ON r.tenant_id = si.tenant_id
                AND r.resource_type = si.resource_type
                AND r.id = si.resource_id
             WHERE r.tenant_id = $1
                AND r.resource_type = $2
                AND r.is_deleted = FALSE
                AND si.param_name IN ('subject', 'patient')
                AND si.value_reference = ANY($3::text[])"
            .to_string();

        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = vec![
            Box::new(tenant_id.to_string()),
            Box::new(resource_type.to_string()),
            Box::new(patient_refs),
        ];
        let mut param_idx = 4;

        if let Some(since) = request.since {
            sql.push_str(&format!(" AND r.last_updated >= ${}", param_idx));
            params.push(Box::new(since));
            param_idx += 1;
        }

        if let Some(cursor) = cursor {
            let parts: Vec<&str> = cursor.splitn(2, '|').collect();
            if parts.len() == 2 {
                sql.push_str(&format!(
                    " AND (r.last_updated, r.id) > (${}, ${})",
                    param_idx,
                    param_idx + 1
                ));
                params.push(Box::new(parts[0].to_string()));
                params.push(Box::new(parts[1].to_string()));
            }
        }

        sql.push_str(&format!(
            " ORDER BY r.last_updated, r.id LIMIT {}",
            batch_size + 1
        ));

        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();

        let rows = client
            .query(&sql, &param_refs)
            .await
            .map_err(|e| internal_error(format!("Failed to query compartment: {}", e)))?;

        let has_more = rows.len() > batch_size as usize;
        let rows_slice = if has_more {
            &rows[..batch_size as usize]
        } else {
            &rows[..]
        };

        let mut lines = Vec::new();
        let mut last_cursor = None;

        for row in rows_slice {
            let id: String = row.get(0);
            let resource: Value = row.get(1);
            let last_updated: chrono::DateTime<Utc> = row.get(2);

            let line = serde_json::to_string(&resource)
                .map_err(|e| internal_error(format!("Failed to serialize: {}", e)))?;
            lines.push(line);
            last_cursor = Some(format!("{}|{}", last_updated.to_rfc3339(), id));
        }

        Ok(NdjsonBatch {
            lines,
            next_cursor: if has_more { last_cursor } else { None },
            is_last: !has_more,
        })
    }
}

#[async_trait]
impl GroupExportProvider for PostgresBackend {
    async fn get_group_members(
        &self,
        tenant: &TenantContext,
        group_id: &str,
    ) -> StorageResult<Vec<String>> {
        let client = self.get_client().await?;
        let tenant_id = tenant.tenant_id().as_str();

        let rows = client
            .query(
                "SELECT data FROM resources WHERE tenant_id = $1 AND resource_type = 'Group' AND id = $2 AND is_deleted = FALSE",
                &[&tenant_id, &group_id],
            )
            .await
            .map_err(|e| internal_error(format!("Failed to fetch group: {}", e)))?;

        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let data: Value = rows[0].get(0);

        // Extract member references from the Group resource
        let mut member_refs = Vec::new();
        if let Some(members) = data.get("member").and_then(|m| m.as_array()) {
            for member in members {
                if let Some(reference) = member
                    .get("entity")
                    .and_then(|e| e.get("reference"))
                    .and_then(|r| r.as_str())
                {
                    member_refs.push(reference.to_string());
                }
            }
        }

        Ok(member_refs)
    }

    async fn resolve_group_patient_ids(
        &self,
        tenant: &TenantContext,
        group_id: &str,
    ) -> StorageResult<Vec<String>> {
        let members = self.get_group_members(tenant, group_id).await?;

        let mut patient_ids = Vec::new();
        for member_ref in &members {
            // Extract patient ID from "Patient/123" format
            if let Some(id) = member_ref.strip_prefix("Patient/") {
                patient_ids.push(id.to_string());
            }
        }

        Ok(patient_ids)
    }
}
