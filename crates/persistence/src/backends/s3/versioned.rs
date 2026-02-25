//! [`VersionedStorage`] implementation for S3.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use object_store::path::Path;
use object_store::ObjectStore;
use serde::Deserialize;
use serde_json::Value;

use crate::core::VersionedStorage;
use crate::error::{BackendError, StorageError, StorageResult};
use crate::tenant::TenantContext;
use crate::types::StoredResource;

use super::backend::internal_storage_error;
use super::S3Backend;

#[derive(Debug, Clone, Deserialize)]
struct VersionDocument {
    version_id: String,
    created_at: DateTime<Utc>,
    last_updated: DateTime<Utc>,
    deleted: bool,
    fhir_version: String,
    resource: Value,
}

#[async_trait]
impl VersionedStorage for S3Backend {
    async fn vread(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        version_id: &str,
    ) -> StorageResult<Option<StoredResource>> {
        let seq = match version_id.parse::<u64>() {
            Ok(seq) => seq,
            Err(_) => return Ok(None),
        };

        let scope = self.tenant_scope(tenant).await?;
        let version_key = self.version_key(&scope, resource_type, id, seq);

        let Some(version_doc) =
            read_json_opt::<VersionDocument>(&*scope.store, &version_key).await?
        else {
            return Ok(None);
        };

        let fhir_version =
            helios_fhir::FhirVersion::from_storage(&version_doc.fhir_version).unwrap_or_default();
        let deleted_at = version_doc.deleted.then_some(version_doc.last_updated);

        Ok(Some(StoredResource::from_storage(
            resource_type,
            id,
            version_doc.version_id,
            tenant.tenant_id().clone(),
            version_doc.resource,
            version_doc.created_at,
            version_doc.last_updated,
            deleted_at,
            fhir_version,
        )))
    }

    async fn update_with_match(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _id: &str,
        _expected_version: &str,
        _resource: Value,
    ) -> StorageResult<StoredResource> {
        Err(StorageError::Backend(BackendError::UnsupportedCapability {
            backend_name: "s3".to_string(),
            capability: "update_with_match".to_string(),
        }))
    }

    async fn delete_with_match(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _id: &str,
        _expected_version: &str,
    ) -> StorageResult<()> {
        Err(StorageError::Backend(BackendError::UnsupportedCapability {
            backend_name: "s3".to_string(),
            capability: "delete_with_match".to_string(),
        }))
    }

    async fn list_versions(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<Vec<String>> {
        let scope = self.tenant_scope(tenant).await?;
        let prefix = Path::from(format!(
            "{}/resources/{}/{}/versions/by_seq",
            scope.prefix, resource_type, id
        ));

        let listing = scope
            .store
            .list_with_delimiter(Some(&prefix))
            .await
            .map_err(|err| {
                internal_storage_error(format!(
                    "failed listing versions at '{}' in bucket '{}': {err}",
                    prefix.as_ref(),
                    scope.bucket
                ))
            })?;

        let mut versions_by_seq = Vec::with_capacity(listing.objects.len());
        for object in listing.objects {
            let key = object.location.as_ref();
            let Some(filename) = key.rsplit('/').next() else {
                continue;
            };
            let Some(seq_part) = filename.strip_suffix(".json") else {
                continue;
            };
            let Ok(seq) = seq_part.parse::<u64>() else {
                continue;
            };

            let Some(version_doc) =
                read_json_opt::<VersionDocument>(&*scope.store, &object.location).await?
            else {
                continue;
            };

            versions_by_seq.push((seq, version_doc.version_id));
        }

        versions_by_seq.sort_by_key(|(seq, _)| *seq);
        Ok(versions_by_seq
            .into_iter()
            .map(|(_, version_id)| version_id)
            .collect())
    }
}

async fn read_json_opt<T: serde::de::DeserializeOwned>(
    store: &dyn ObjectStore,
    key: &Path,
) -> StorageResult<Option<T>> {
    let get_result = match store.get(key).await {
        Ok(result) => result,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(err) => {
            return Err(internal_storage_error(format!(
                "failed reading key '{}' from object store: {err}",
                key.as_ref()
            )));
        }
    };

    let bytes = get_result.bytes().await.map_err(|err| {
        internal_storage_error(format!(
            "failed downloading bytes for key '{}': {err}",
            key.as_ref()
        ))
    })?;

    let parsed = serde_json::from_slice::<T>(&bytes).map_err(|err| {
        internal_storage_error(format!(
            "failed to deserialize JSON for key '{}': {err}",
            key.as_ref()
        ))
    })?;

    Ok(Some(parsed))
}
