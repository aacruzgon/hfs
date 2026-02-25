//! Shared MinIO test harness for S3 integration tests.
//!
//! This module starts a single MinIO container per test process and provides
//! helpers for creating isolated buckets/prefixes and object_store clients.

use std::sync::Arc;
use std::time::{Duration, Instant};

use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::Client as AwsS3Client;
use aws_sdk_s3::config::Region;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::net::TcpStream;
use tokio::sync::OnceCell;
use tokio::time::sleep;
use uuid::Uuid;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const MINIO_IMAGE: &str = "minio/minio";
const MINIO_TAG: &str = "latest";
const MINIO_ROOT_USER: &str = "minioadmin";
const MINIO_ROOT_PASSWORD: &str = "minioadmin";
const MINIO_REGION: &str = "us-east-1";

/// Shared MinIO container state.
pub struct MinioHarness {
    endpoint: String,
    /// Kept alive for the full test process lifetime.
    _container: ContainerAsync<GenericImage>,
}

/// Basic client wrapper for smoke tests.
pub struct S3TestClient {
    pub bucket: String,
    pub root_prefix: String,
    pub store: Arc<dyn ObjectStore>,
}

impl S3TestClient {
    pub fn key(&self, relative: &str) -> object_store::path::Path {
        object_store::path::Path::from(format!(
            "{}/{}",
            self.root_prefix.trim_end_matches('/'),
            relative.trim_start_matches('/')
        ))
    }
}

static SHARED_MINIO: OnceCell<MinioHarness> = OnceCell::const_new();

impl MinioHarness {
    /// Returns the process-global MinIO harness, starting it on first use.
    pub async fn shared() -> &'static MinioHarness {
        SHARED_MINIO
            .get_or_init(|| async {
                Self::start()
                    .await
                    .expect("failed to initialize shared MinIO harness")
            })
            .await
    }

    async fn start() -> Result<Self, BoxError> {
        let run_id = std::env::var("GITHUB_RUN_ID").unwrap_or_default();

        let image = GenericImage::new(MINIO_IMAGE, MINIO_TAG)
            .with_exposed_port(9000.tcp())
            .with_exposed_port(9001.tcp())
            // Don't use listening_port/log waits (version differences + flakiness).
            // We do explicit readiness checks after the container starts.
            .with_wait_for(WaitFor::seconds(1))
            .with_env_var("MINIO_ROOT_USER", MINIO_ROOT_USER)
            .with_env_var("MINIO_ROOT_PASSWORD", MINIO_ROOT_PASSWORD)
            .with_cmd(vec!["server", "/data", "--console-address", ":9001"])
            .with_label("github.run_id", &run_id)
            .with_startup_timeout(Duration::from_secs(240));

        let container = image.start().await?;

        // Prefer host port mapping (works across Linux/macOS/CI).
        let port = container.get_host_port_ipv4(9000).await?;
        let endpoint = format!("http://127.0.0.1:{}", port);

        // Readiness 1: TCP reachable.
        wait_for_endpoint("127.0.0.1", port, Duration::from_secs(60)).await?;

        // Readiness 2: S3 API usable (HEAD + LIST).
        let bootstrap_bucket = format!("hfs-minio-ready-{}", short_suffix());
        create_bucket_if_missing_at(&endpoint, &bootstrap_bucket).await?;
        verify_bucket_access_at(&endpoint, &bootstrap_bucket).await?;

        Ok(Self {
            endpoint,
            _container: container,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Returns a client using shared-bucket + tenant-prefix isolation.
    pub async fn client_shared_bucket(&self, bucket: &str) -> Result<S3TestClient, BoxError> {
        create_bucket_if_missing_at(&self.endpoint, bucket).await?;
        let tenant_id = unique_tenant_id();
        let prefix = format!("v1/tenants/{}", tenant_id);
        let store = build_object_store_at(&self.endpoint, bucket)?;
        Ok(S3TestClient {
            bucket: bucket.to_string(),
            root_prefix: prefix,
            store,
        })
    }

    /// Returns a client using bucket-per-tenant isolation.
    pub async fn client_bucket_per_tenant(
        &self,
        bucket_prefix: &str,
    ) -> Result<S3TestClient, BoxError> {
        let tenant_id = unique_tenant_id();
        let bucket = bucket_name_for_tenant(bucket_prefix, &tenant_id);
        create_bucket_if_missing_at(&self.endpoint, &bucket).await?;
        let store = build_object_store_at(&self.endpoint, &bucket)?;
        Ok(S3TestClient {
            bucket,
            root_prefix: "v1".to_string(),
            store,
        })
    }
}

/// Returns `http://127.0.0.1:{mapped_port}` for the shared MinIO API endpoint.
pub async fn minio_endpoint() -> String {
    MinioHarness::shared().await.endpoint().to_string()
}

/// Returns the fixed MinIO root credentials used in tests.
pub fn minio_creds() -> (String, String) {
    (MINIO_ROOT_USER.to_string(), MINIO_ROOT_PASSWORD.to_string())
}

/// Creates the bucket if missing.
pub async fn create_bucket_if_missing(bucket: &str) -> Result<(), BoxError> {
    let endpoint = minio_endpoint().await;
    create_bucket_if_missing_at(&endpoint, bucket).await
}

/// Generates a unique tenant id for isolated tests.
pub fn unique_tenant_id() -> String {
    format!("tenant-{}", short_suffix())
}

/// Generates a unique key prefix for isolated tests.
pub fn unique_prefix() -> String {
    format!("test-{}", short_suffix())
}

/// Returns an object_store S3 client configured for MinIO path-style HTTP.
pub async fn minio_object_store(bucket: &str) -> Result<Arc<dyn ObjectStore>, BoxError> {
    let endpoint = minio_endpoint().await;
    create_bucket_if_missing_at(&endpoint, bucket).await?;
    build_object_store_at(&endpoint, bucket)
}

fn build_object_store_at(endpoint: &str, bucket: &str) -> Result<Arc<dyn ObjectStore>, BoxError> {
    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_region(MINIO_REGION)
        .with_access_key_id(MINIO_ROOT_USER)
        .with_secret_access_key(MINIO_ROOT_PASSWORD)
        .with_bucket_name(bucket)
        .with_virtual_hosted_style_request(false)
        .with_allow_http(true)
        .build()?;
    Ok(Arc::new(store))
}

async fn minio_aws_client(endpoint: &str) -> AwsS3Client {
    let creds = Credentials::new(
        MINIO_ROOT_USER,
        MINIO_ROOT_PASSWORD,
        None,
        None,
        "hfs-minio-tests",
    );

    let config = aws_sdk_s3::config::Builder::new()
        .endpoint_url(endpoint)
        .region(Region::new(MINIO_REGION))
        .credentials_provider(SharedCredentialsProvider::new(creds))
        .force_path_style(true)
        .timeout_config(
            aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                .operation_timeout(Duration::from_secs(10))
                .build(),
        )
        .build();

    AwsS3Client::from_conf(config)
}

async fn create_bucket_if_missing_at(endpoint: &str, bucket: &str) -> Result<(), BoxError> {
    let client = minio_aws_client(endpoint).await;

    if client.head_bucket().bucket(bucket).send().await.is_ok() {
        return Ok(());
    }

    let create_result = client.create_bucket().bucket(bucket).send().await;
    if let Err(err) = create_result {
        let msg = err.to_string();
        if !msg.contains("BucketAlreadyOwnedByYou") && !msg.contains("BucketAlreadyExists") {
            return Err(format!("failed creating bucket '{bucket}': {msg}").into());
        }
    }

    client.head_bucket().bucket(bucket).send().await?;
    Ok(())
}

async fn verify_bucket_access_at(endpoint: &str, bucket: &str) -> Result<(), BoxError> {
    let client = minio_aws_client(endpoint).await;
    client
        .list_objects_v2()
        .bucket(bucket)
        .max_keys(1)
        .send()
        .await?;
    Ok(())
}

async fn wait_for_endpoint(host: &str, port: u16, timeout: Duration) -> Result<(), BoxError> {
    let deadline = Instant::now() + timeout;
    loop {
        match TcpStream::connect((host, port)).await {
            Ok(_) => return Ok(()),
            Err(_) if Instant::now() < deadline => sleep(Duration::from_millis(200)).await,
            Err(err) => {
                return Err(format!(
                    "MinIO endpoint not reachable at {}:{} within {:?}: {}",
                    host, port, timeout, err
                )
                .into());
            }
        }
    }
}

fn bucket_name_for_tenant(bucket_prefix: &str, tenant_id: &str) -> String {
    let tenant = tenant_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();

    let suffix = short_suffix();
    let base = format!("{}-{}-{}", bucket_prefix, tenant, suffix);
    let normalized = base
        .chars()
        .filter(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '-')
        .collect::<String>();

    let mut result = normalized.trim_matches('-').to_string();
    if result.len() > 63 {
        result.truncate(63);
        result = result.trim_matches('-').to_string();
    }
    if result.is_empty() {
        format!("hfs-tenant-{}", short_suffix())
    } else {
        result
    }
}

fn short_suffix() -> String {
    Uuid::new_v4().simple().to_string()[..10].to_string()
}
