//! S3 backend core configuration and [`Backend`] implementation.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::core::{Backend, BackendCapability, BackendKind};
use crate::error::{BackendError, StorageError, StorageResult};
use crate::tenant::TenantContext;

const BACKEND_NAME: &str = "s3";

/// S3 backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3BackendConfig {
    /// S3 region.
    #[serde(default = "default_region")]
    pub region: String,

    /// Optional custom endpoint (for MinIO or S3-compatible services).
    #[serde(default)]
    pub endpoint: Option<String>,

    /// Optional static access key ID.
    #[serde(default)]
    pub access_key_id: Option<String>,

    /// Optional static secret access key.
    #[serde(default)]
    pub secret_access_key: Option<String>,

    /// Whether HTTP is allowed (useful for local MinIO).
    #[serde(default)]
    pub allow_http: bool,

    /// Whether virtual-hosted-style requests should be used.
    /// Set to `false` to force path-style addressing.
    #[serde(default)]
    pub virtual_hosted_style_request: bool,

    /// Tenant isolation mode for S3 object layout.
    #[serde(default)]
    pub tenancy_mode: S3TenancyMode,

    /// Lock timeout for per-resource write serialization.
    #[serde(default = "default_lock_timeout_ms")]
    pub lock_timeout_ms: u64,

    /// Backoff interval while waiting for a lock.
    #[serde(default = "default_lock_retry_interval_ms")]
    pub lock_retry_interval_ms: u64,

    /// Lock lease duration.
    #[serde(default = "default_lock_ttl_ms")]
    pub lock_ttl_ms: u64,
}

impl Default for S3BackendConfig {
    fn default() -> Self {
        Self {
            region: default_region(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            allow_http: false,
            virtual_hosted_style_request: true,
            tenancy_mode: S3TenancyMode::default(),
            lock_timeout_ms: default_lock_timeout_ms(),
            lock_retry_interval_ms: default_lock_retry_interval_ms(),
            lock_ttl_ms: default_lock_ttl_ms(),
        }
    }
}

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_lock_timeout_ms() -> u64 {
    5_000
}

fn default_lock_retry_interval_ms() -> u64 {
    50
}

fn default_lock_ttl_ms() -> u64 {
    15_000
}

/// S3 multitenancy mapping.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum S3TenancyMode {
    /// Shared bucket with tenant prefix isolation: `v1/tenants/{tenant_id}/...`
    SharedBucket {
        /// Shared bucket name.
        bucket: String,
    },
    /// Bucket-per-tenant with fixed prefix: `v1/...`
    BucketPerTenant {
        /// Prefix used to derive per-tenant bucket names.
        bucket_prefix: String,
        /// Optional suffix appended to per-tenant bucket names.
        #[serde(default)]
        bucket_suffix: Option<String>,
    },
}

impl Default for S3TenancyMode {
    fn default() -> Self {
        Self::SharedBucket {
            bucket: "helios-fhir".to_string(),
        }
    }
}

/// S3 backend implementation.
#[derive(Debug)]
pub struct S3Backend {
    pub(crate) config: S3BackendConfig,
    pub(crate) shared_store: Option<Arc<dyn ObjectStore>>,
    pub(crate) bucket_store_cache: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
}

/// Placeholder connection wrapper for [`Backend`] compatibility.
#[derive(Debug)]
pub struct S3Connection;

/// Resolved tenant scope for key layout and object store selection.
#[derive(Debug, Clone)]
pub(crate) struct TenantScope {
    pub(crate) bucket: String,
    pub(crate) prefix: String,
    pub(crate) store: Arc<dyn ObjectStore>,
}

impl S3Backend {
    /// Creates a new S3 backend from configuration.
    pub fn new(config: S3BackendConfig) -> StorageResult<Self> {
        let shared_store = match &config.tenancy_mode {
            S3TenancyMode::SharedBucket { bucket } => Some(build_store(&config, bucket)?),
            S3TenancyMode::BucketPerTenant { .. } => None,
        };

        Ok(Self {
            config,
            shared_store,
            bucket_store_cache: RwLock::new(HashMap::new()),
        })
    }

    /// Returns backend configuration.
    pub fn config(&self) -> &S3BackendConfig {
        &self.config
    }

    pub(crate) async fn tenant_scope(&self, tenant: &TenantContext) -> StorageResult<TenantScope> {
        match &self.config.tenancy_mode {
            S3TenancyMode::SharedBucket { bucket } => {
                let store = self
                    .shared_store
                    .clone()
                    .ok_or_else(|| internal_storage_error("shared store is not initialized"))?;
                Ok(TenantScope {
                    bucket: bucket.clone(),
                    prefix: format!("v1/tenants/{}", tenant.tenant_id().as_str()),
                    store,
                })
            }
            S3TenancyMode::BucketPerTenant {
                bucket_prefix,
                bucket_suffix,
            } => {
                let bucket =
                    self.bucket_for_tenant(tenant, bucket_prefix, bucket_suffix.as_deref());

                if let Some(store) = self.bucket_store_cache.read().await.get(&bucket).cloned() {
                    return Ok(TenantScope {
                        bucket,
                        prefix: "v1".to_string(),
                        store,
                    });
                }

                let store = build_store(&self.config, &bucket)?;
                let mut cache = self.bucket_store_cache.write().await;
                let cached = cache.entry(bucket.clone()).or_insert_with(|| store.clone());

                Ok(TenantScope {
                    bucket,
                    prefix: "v1".to_string(),
                    store: cached.clone(),
                })
            }
        }
    }

    pub(crate) fn bucket_for_tenant(
        &self,
        tenant: &TenantContext,
        bucket_prefix: &str,
        bucket_suffix: Option<&str>,
    ) -> String {
        let tenant_root = tenant.tenant_id().root().as_str().to_ascii_lowercase();
        let tenant_slug = sanitize_bucket_segment(&tenant_root);
        let mut base = format!("{}-{}", sanitize_bucket_segment(bucket_prefix), tenant_slug);

        if let Some(suffix) = bucket_suffix {
            let suffix = sanitize_bucket_segment(suffix);
            if !suffix.is_empty() {
                base = format!("{}-{}", base, suffix);
            }
        }

        let mut bucket = base
            .chars()
            .filter(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '-')
            .collect::<String>();

        bucket = bucket.trim_matches('-').to_string();
        if bucket.len() > 63 {
            bucket.truncate(63);
            bucket = bucket.trim_matches('-').to_string();
        }

        if bucket.is_empty() {
            "helios-s3-tenant".to_string()
        } else {
            bucket
        }
    }

    pub(crate) fn state_key(&self, scope: &TenantScope, resource_type: &str, id: &str) -> Path {
        Path::from(format!(
            "{}/resources/{}/{}/state.json",
            scope.prefix, resource_type, id
        ))
    }

    pub(crate) fn version_key(
        &self,
        scope: &TenantScope,
        resource_type: &str,
        id: &str,
        seq: u64,
    ) -> Path {
        Path::from(format!(
            "{}/resources/{}/{}/versions/by_seq/{:020}.json",
            scope.prefix, resource_type, id, seq
        ))
    }

    pub(crate) fn lock_key(&self, scope: &TenantScope, resource_type: &str, id: &str) -> Path {
        Path::from(format!(
            "{}/resources/{}/{}/locks/{}.lock",
            scope.prefix, resource_type, id, id
        ))
    }
}

fn build_store(config: &S3BackendConfig, bucket: &str) -> StorageResult<Arc<dyn ObjectStore>> {
    let mut builder = AmazonS3Builder::new()
        .with_region(&config.region)
        .with_bucket_name(bucket)
        .with_allow_http(config.allow_http)
        .with_virtual_hosted_style_request(config.virtual_hosted_style_request);

    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_endpoint(endpoint);
    }

    if let Some(access_key_id) = &config.access_key_id {
        builder = builder.with_access_key_id(access_key_id);
    }

    if let Some(secret_access_key) = &config.secret_access_key {
        builder = builder.with_secret_access_key(secret_access_key);
    }

    let store = builder.build().map_err(|e| {
        StorageError::Backend(BackendError::ConnectionFailed {
            backend_name: BACKEND_NAME.to_string(),
            message: format!("failed to build S3 store for bucket '{bucket}': {e}"),
        })
    })?;

    Ok(Arc::new(store))
}

fn sanitize_bucket_segment(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('-');
        }
    }

    let normalized = out
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");

    if normalized.is_empty() {
        "tenant".to_string()
    } else {
        normalized
    }
}

pub(crate) fn internal_storage_error(message: impl Into<String>) -> StorageError {
    StorageError::Backend(BackendError::Internal {
        backend_name: BACKEND_NAME.to_string(),
        message: message.into(),
        source: None,
    })
}

#[async_trait]
impl Backend for S3Backend {
    type Connection = S3Connection;

    fn kind(&self) -> BackendKind {
        BackendKind::S3
    }

    fn name(&self) -> &'static str {
        BACKEND_NAME
    }

    fn supports(&self, capability: BackendCapability) -> bool {
        matches!(
            capability,
            BackendCapability::Crud
                | BackendCapability::SharedSchema
                | BackendCapability::DatabasePerTenant
        )
    }

    fn capabilities(&self) -> Vec<BackendCapability> {
        vec![
            BackendCapability::Crud,
            BackendCapability::SharedSchema,
            BackendCapability::DatabasePerTenant,
        ]
    }

    async fn acquire(&self) -> Result<Self::Connection, BackendError> {
        Ok(S3Connection)
    }

    async fn release(&self, _conn: Self::Connection) {
        // No-op: object store clients are shared and connectionless.
    }

    async fn health_check(&self) -> Result<(), BackendError> {
        // Phase 1 is intentionally light: ensure shared-bucket wiring can issue
        // a request without requiring bucket-per-tenant provisioning.
        if let Some(store) = &self.shared_store {
            let probe_key = Path::from("v1/health/__probe__");
            match store.head(&probe_key).await {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(err) => Err(BackendError::Unavailable {
                    backend_name: BACKEND_NAME.to_string(),
                    message: format!("S3 health check failed: {err}"),
                }),
            }
        } else {
            Ok(())
        }
    }

    async fn initialize(&self) -> Result<(), BackendError> {
        Ok(())
    }

    async fn migrate(&self) -> Result<(), BackendError> {
        Ok(())
    }
}
