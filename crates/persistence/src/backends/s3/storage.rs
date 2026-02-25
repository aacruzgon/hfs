//! [`ResourceStorage`] implementation for S3.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use helios_fhir::FhirVersion;
use object_store::ObjectStore;
use object_store::path::Path;
use object_store::{PutMode, PutOptions};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize as SerdeSerialize};
use serde_json::Value;
use tokio::time::sleep;
use uuid::Uuid;

use crate::core::ResourceStorage;
use crate::error::{BackendError, ConcurrencyError, ResourceError, StorageError, StorageResult};
use crate::tenant::TenantContext;
use crate::types::StoredResource;

use super::S3Backend;
use super::backend::{TenantScope, internal_storage_error};

#[derive(Debug, Clone, SerdeSerialize, Deserialize)]
struct StateDocument {
    schema_version: u32,
    current: CurrentState,
    #[serde(default)]
    search: Value,
}

#[derive(Debug, Clone, SerdeSerialize, Deserialize)]
struct CurrentState {
    seq: u64,
    version_id: String,
    created_at: DateTime<Utc>,
    last_updated: DateTime<Utc>,
    last_updated_ms: i64,
    deleted: bool,
    deleted_at: Option<DateTime<Utc>>,
    fhir_version: String,
}

#[derive(Debug, Clone, SerdeSerialize, Deserialize)]
struct VersionDocument {
    schema_version: u32,
    resource_type: String,
    id: String,
    tenant_id: String,
    seq: u64,
    version_id: String,
    created_at: DateTime<Utc>,
    last_updated: DateTime<Utc>,
    deleted: bool,
    fhir_version: String,
    resource: Value,
}

#[derive(Debug, Clone, SerdeSerialize, Deserialize)]
struct LockDocument {
    owner: String,
    expires_at: DateTime<Utc>,
    attempt: u64,
}

#[async_trait]
impl ResourceStorage for S3Backend {
    fn backend_name(&self) -> &'static str {
        "s3"
    }

    async fn create(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        resource: Value,
        fhir_version: FhirVersion,
    ) -> StorageResult<StoredResource> {
        let id = resource
            .get("id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let scope = self.tenant_scope(tenant).await?;
        let owner = self.acquire_lock(&scope, resource_type, &id).await?;

        let outcome = self
            .create_locked(&scope, tenant, resource_type, &id, resource, fhir_version)
            .await;

        self.release_lock(&scope, resource_type, &id, &owner).await;
        outcome
    }

    async fn create_or_update(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        resource: Value,
        fhir_version: FhirVersion,
    ) -> StorageResult<(StoredResource, bool)> {
        let existing = self.read(tenant, resource_type, id).await?;

        if let Some(current) = existing {
            let updated = self.update(tenant, &current, resource).await?;
            Ok((updated, false))
        } else {
            let mut resource = resource;
            if let Some(obj) = resource.as_object_mut() {
                obj.insert("id".to_string(), Value::String(id.to_string()));
            }
            let created = self
                .create(tenant, resource_type, resource, fhir_version)
                .await?;
            Ok((created, true))
        }
    }

    async fn read(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<Option<StoredResource>> {
        let scope = self.tenant_scope(tenant).await?;
        let state_key = self.state_key(&scope, resource_type, id);

        let Some(state) = read_json_opt::<StateDocument>(&scope.store, &state_key).await? else {
            return Ok(None);
        };

        if state.current.deleted {
            return Err(StorageError::Resource(ResourceError::Gone {
                resource_type: resource_type.to_string(),
                id: id.to_string(),
                deleted_at: state.current.deleted_at,
            }));
        }

        let version_key = self.version_key(&scope, resource_type, id, state.current.seq);
        let Some(version) = read_json_opt::<VersionDocument>(&scope.store, &version_key).await?
        else {
            return Err(internal_storage_error(format!(
                "missing current version object for {resource_type}/{id} at key {}",
                version_key.as_ref()
            )));
        };

        let fhir_version = FhirVersion::from_storage(&version.fhir_version).unwrap_or_default();

        Ok(Some(StoredResource::from_storage(
            resource_type,
            id,
            state.current.version_id,
            tenant.tenant_id().clone(),
            version.resource,
            version.created_at,
            version.last_updated,
            None,
            fhir_version,
        )))
    }

    async fn update(
        &self,
        tenant: &TenantContext,
        current: &StoredResource,
        resource: Value,
    ) -> StorageResult<StoredResource> {
        let resource_type = current.resource_type();
        let id = current.id();

        let scope = self.tenant_scope(tenant).await?;
        let owner = self.acquire_lock(&scope, resource_type, id).await?;

        let outcome = self.update_locked(&scope, tenant, current, resource).await;

        self.release_lock(&scope, resource_type, id, &owner).await;
        outcome
    }

    async fn delete(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<()> {
        let scope = self.tenant_scope(tenant).await?;
        let owner = self.acquire_lock(&scope, resource_type, id).await?;

        let outcome = self.delete_locked(&scope, tenant, resource_type, id).await;

        self.release_lock(&scope, resource_type, id, &owner).await;
        outcome
    }

    async fn count(
        &self,
        tenant: &TenantContext,
        resource_type: Option<&str>,
    ) -> StorageResult<u64> {
        let scope = self.tenant_scope(tenant).await?;

        let root_prefix = match resource_type {
            Some(rt) => format!("{}/resources/{rt}", scope.prefix),
            None => format!("{}/resources", scope.prefix),
        };

        let mut total = 0u64;
        let mut queue = VecDeque::from([Path::from(root_prefix)]);

        while let Some(prefix) = queue.pop_front() {
            let listing = scope
                .store
                .list_with_delimiter(Some(&prefix))
                .await
                .map_err(|err| {
                    internal_storage_error(format!(
                        "failed listing '{}' for count: {err}",
                        prefix.as_ref()
                    ))
                })?;

            for meta in listing.objects {
                if meta.location.as_ref().ends_with("/state.json") {
                    if let Some(state) =
                        read_json_opt::<StateDocument>(&scope.store, &meta.location).await?
                        && !state.current.deleted
                    {
                        total += 1;
                    }
                }
            }

            for common_prefix in listing.common_prefixes {
                let key = common_prefix.as_ref();
                if key.contains("/versions/") || key.contains("/locks/") {
                    continue;
                }
                queue.push_back(common_prefix);
            }
        }

        Ok(total)
    }
}

impl S3Backend {
    async fn create_locked(
        &self,
        scope: &TenantScope,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        resource: Value,
        fhir_version: FhirVersion,
    ) -> StorageResult<StoredResource> {
        let state_key = self.state_key(scope, resource_type, id);
        if read_json_opt::<StateDocument>(&scope.store, &state_key)
            .await?
            .is_some()
        {
            return Err(StorageError::Resource(ResourceError::AlreadyExists {
                resource_type: resource_type.to_string(),
                id: id.to_string(),
            }));
        }

        let resource = ensure_resource_identity(resource, resource_type, id);

        let now = Utc::now();
        let seq = 1;
        let version_id = seq.to_string();
        let fhir_version_str = fhir_version.as_mime_param().to_string();

        let version_doc = VersionDocument {
            schema_version: 1,
            resource_type: resource_type.to_string(),
            id: id.to_string(),
            tenant_id: tenant.tenant_id().as_str().to_string(),
            seq,
            version_id: version_id.clone(),
            created_at: now,
            last_updated: now,
            deleted: false,
            fhir_version: fhir_version_str.clone(),
            resource: resource.clone(),
        };

        let version_key = self.version_key(scope, resource_type, id, seq);
        put_json_create(&scope.store, &version_key, &version_doc).await?;

        let state = StateDocument {
            schema_version: 1,
            current: CurrentState {
                seq,
                version_id: version_id.clone(),
                created_at: now,
                last_updated: now,
                last_updated_ms: now.timestamp_millis(),
                deleted: false,
                deleted_at: None,
                fhir_version: fhir_version_str,
            },
            search: empty_search_doc(),
        };

        put_json_overwrite(&scope.store, &state_key, &state).await?;

        Ok(StoredResource::from_storage(
            resource_type,
            id,
            version_id,
            tenant.tenant_id().clone(),
            resource,
            now,
            now,
            None,
            fhir_version,
        ))
    }

    async fn update_locked(
        &self,
        scope: &TenantScope,
        tenant: &TenantContext,
        current: &StoredResource,
        resource: Value,
    ) -> StorageResult<StoredResource> {
        let resource_type = current.resource_type();
        let id = current.id();
        let state_key = self.state_key(scope, resource_type, id);

        let Some(state) = read_json_opt::<StateDocument>(&scope.store, &state_key).await? else {
            return Err(StorageError::Resource(ResourceError::NotFound {
                resource_type: resource_type.to_string(),
                id: id.to_string(),
            }));
        };

        if state.current.deleted {
            return Err(StorageError::Resource(ResourceError::NotFound {
                resource_type: resource_type.to_string(),
                id: id.to_string(),
            }));
        }

        if state.current.version_id != current.version_id() {
            return Err(StorageError::Concurrency(
                ConcurrencyError::VersionConflict {
                    resource_type: resource_type.to_string(),
                    id: id.to_string(),
                    expected_version: current.version_id().to_string(),
                    actual_version: state.current.version_id,
                },
            ));
        }

        let prior_version_key = self.version_key(scope, resource_type, id, state.current.seq);
        let Some(prior_version) =
            read_json_opt::<VersionDocument>(&scope.store, &prior_version_key).await?
        else {
            return Err(internal_storage_error(format!(
                "missing prior version object for {resource_type}/{id} at key {}",
                prior_version_key.as_ref()
            )));
        };

        let resource = ensure_resource_identity(resource, resource_type, id);

        let now = Utc::now();
        let seq = state.current.seq + 1;
        let version_id = seq.to_string();
        let fhir_version = current.fhir_version();
        let fhir_version_str = fhir_version.as_mime_param().to_string();

        let version_doc = VersionDocument {
            schema_version: 1,
            resource_type: resource_type.to_string(),
            id: id.to_string(),
            tenant_id: tenant.tenant_id().as_str().to_string(),
            seq,
            version_id: version_id.clone(),
            created_at: prior_version.created_at,
            last_updated: now,
            deleted: false,
            fhir_version: fhir_version_str.clone(),
            resource: resource.clone(),
        };

        let version_key = self.version_key(scope, resource_type, id, seq);
        put_json_create(&scope.store, &version_key, &version_doc).await?;

        let updated_state = StateDocument {
            schema_version: state.schema_version,
            current: CurrentState {
                seq,
                version_id: version_id.clone(),
                created_at: prior_version.created_at,
                last_updated: now,
                last_updated_ms: now.timestamp_millis(),
                deleted: false,
                deleted_at: None,
                fhir_version: fhir_version_str,
            },
            search: empty_search_doc(),
        };

        put_json_overwrite(&scope.store, &state_key, &updated_state).await?;

        Ok(StoredResource::from_storage(
            resource_type,
            id,
            version_id,
            tenant.tenant_id().clone(),
            resource,
            prior_version.created_at,
            now,
            None,
            fhir_version,
        ))
    }

    async fn delete_locked(
        &self,
        scope: &TenantScope,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<()> {
        let state_key = self.state_key(scope, resource_type, id);

        let Some(state) = read_json_opt::<StateDocument>(&scope.store, &state_key).await? else {
            return Err(StorageError::Resource(ResourceError::NotFound {
                resource_type: resource_type.to_string(),
                id: id.to_string(),
            }));
        };

        if state.current.deleted {
            return Err(StorageError::Resource(ResourceError::NotFound {
                resource_type: resource_type.to_string(),
                id: id.to_string(),
            }));
        }

        let prior_version_key = self.version_key(scope, resource_type, id, state.current.seq);
        let Some(prior_version) =
            read_json_opt::<VersionDocument>(&scope.store, &prior_version_key).await?
        else {
            return Err(internal_storage_error(format!(
                "missing prior version object for {resource_type}/{id} at key {}",
                prior_version_key.as_ref()
            )));
        };

        let now = Utc::now();
        let seq = state.current.seq + 1;
        let version_id = seq.to_string();

        let deleted_version = VersionDocument {
            schema_version: 1,
            resource_type: resource_type.to_string(),
            id: id.to_string(),
            tenant_id: tenant.tenant_id().as_str().to_string(),
            seq,
            version_id: version_id.clone(),
            created_at: prior_version.created_at,
            last_updated: now,
            deleted: true,
            fhir_version: prior_version.fhir_version.clone(),
            resource: prior_version.resource,
        };

        let version_key = self.version_key(scope, resource_type, id, seq);
        put_json_create(&scope.store, &version_key, &deleted_version).await?;

        let updated_state = StateDocument {
            schema_version: state.schema_version,
            current: CurrentState {
                seq,
                version_id,
                created_at: prior_version.created_at,
                last_updated: now,
                last_updated_ms: now.timestamp_millis(),
                deleted: true,
                deleted_at: Some(now),
                fhir_version: prior_version.fhir_version,
            },
            search: empty_search_doc(),
        };

        put_json_overwrite(&scope.store, &state_key, &updated_state).await?;

        Ok(())
    }

    async fn acquire_lock(
        &self,
        scope: &TenantScope,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<String> {
        let lock_key = self.lock_key(scope, resource_type, id);
        let owner = Uuid::new_v4().to_string();
        let deadline = Instant::now() + Duration::from_millis(self.config.lock_timeout_ms);
        let retry = Duration::from_millis(self.config.lock_retry_interval_ms);

        let mut attempt = 0u64;
        loop {
            attempt += 1;
            let now = Utc::now();
            let expires_at = now + chrono::Duration::milliseconds(self.config.lock_ttl_ms as i64);

            let lock_doc = LockDocument {
                owner: owner.clone(),
                expires_at,
                attempt,
            };

            let payload = serialize_to_bytes(&lock_doc)?;
            match scope
                .store
                .put_opts(
                    &lock_key,
                    payload.into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => return Ok(owner),
                Err(object_store::Error::AlreadyExists { .. }) => {
                    if let Some(existing) =
                        read_json_opt::<LockDocument>(&scope.store, &lock_key).await?
                        && existing.expires_at <= now
                    {
                        let _ = scope.store.delete(&lock_key).await;
                    }
                }
                Err(
                    object_store::Error::NotSupported { .. } | object_store::Error::NotImplemented,
                ) => {
                    return Err(StorageError::Backend(BackendError::UnsupportedCapability {
                        backend_name: "s3".to_string(),
                        capability: "create-only lock object".to_string(),
                    }));
                }
                Err(err) => {
                    return Err(internal_storage_error(format!(
                        "failed to acquire lock '{}' in bucket '{}': {err}",
                        lock_key.as_ref(),
                        scope.bucket
                    )));
                }
            }

            if Instant::now() >= deadline {
                return Err(StorageError::Concurrency(ConcurrencyError::LockTimeout {
                    resource_type: resource_type.to_string(),
                    id: id.to_string(),
                    timeout_ms: self.config.lock_timeout_ms,
                }));
            }

            sleep(retry).await;
        }
    }

    async fn release_lock(&self, scope: &TenantScope, resource_type: &str, id: &str, owner: &str) {
        let lock_key = self.lock_key(scope, resource_type, id);

        let current = read_json_opt::<LockDocument>(&scope.store, &lock_key).await;
        match current {
            Ok(Some(lock_doc)) if lock_doc.owner == owner => {
                let _ = scope.store.delete(&lock_key).await;
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(
                    "Failed reading lock document '{}' for release: {}",
                    lock_key.as_ref(),
                    err
                );
            }
        }
    }
}

fn ensure_resource_identity(resource: Value, resource_type: &str, id: &str) -> Value {
    let mut resource = resource;
    if let Some(obj) = resource.as_object_mut() {
        obj.insert(
            "resourceType".to_string(),
            Value::String(resource_type.to_string()),
        );
        obj.insert("id".to_string(), Value::String(id.to_string()));
    }
    resource
}

fn empty_search_doc() -> Value {
    serde_json::json!({ "params": {} })
}

fn serialize_to_bytes<T: Serialize>(value: &T) -> StorageResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|err| {
        StorageError::Backend(BackendError::SerializationError {
            message: format!("failed to serialize S3 document: {err}"),
        })
    })
}

async fn read_json_opt<T: DeserializeOwned>(
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

async fn put_json_overwrite<T: Serialize>(
    store: &dyn ObjectStore,
    key: &Path,
    value: &T,
) -> StorageResult<()> {
    let payload = serialize_to_bytes(value)?;
    store.put(key, payload.into()).await.map_err(|err| {
        internal_storage_error(format!(
            "failed writing key '{}' to object store: {err}",
            key.as_ref()
        ))
    })?;
    Ok(())
}

async fn put_json_create<T: Serialize>(
    store: &dyn ObjectStore,
    key: &Path,
    value: &T,
) -> StorageResult<()> {
    let payload = serialize_to_bytes(value)?;

    match store
        .put_opts(
            key,
            payload.into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => Err(internal_storage_error(format!(
            "create-only write collided for key '{}'",
            key.as_ref()
        ))),
        Err(err) => Err(internal_storage_error(format!(
            "failed create-only write for key '{}': {err}",
            key.as_ref()
        ))),
    }
}
