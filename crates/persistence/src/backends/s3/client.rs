use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ObjectMetadata {
    pub etag: Option<String>,
    pub last_modified: Option<DateTime<Utc>>,
    pub size: i64,
}

#[derive(Debug, Clone)]
pub struct ObjectData {
    pub bytes: Vec<u8>,
    pub metadata: ObjectMetadata,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ListObjectItem {
    pub key: String,
    pub etag: Option<String>,
    pub last_modified: Option<DateTime<Utc>>,
    pub size: i64,
}

#[derive(Debug, Clone)]
pub struct ListObjectsResult {
    pub items: Vec<ListObjectItem>,
    pub next_continuation_token: Option<String>,
}

#[derive(Debug, Clone)]
pub enum S3ClientError {
    NotFound,
    PreconditionFailed,
    Throttled(String),
    Unavailable(String),
    InvalidInput(String),
    Internal(String),
}

#[async_trait]
pub trait S3Api: Send + Sync {
    async fn head_bucket(&self, bucket: &str) -> Result<(), S3ClientError>;

    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, S3ClientError>;

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<ObjectData>, S3ClientError>;

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Result<ObjectMetadata, S3ClientError>;

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), S3ClientError>;

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        continuation: Option<&str>,
        max_keys: Option<i32>,
    ) -> Result<ListObjectsResult, S3ClientError>;
}

#[derive(Debug, Clone)]
pub struct AwsS3Client {
    client: Client,
}

impl AwsS3Client {
    pub fn from_sdk_config(config: &SdkConfig) -> Self {
        Self {
            client: Client::new(config),
        }
    }

    pub async fn load_sdk_config(region: Option<&str>) -> SdkConfig {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(region) = region {
            loader = loader.region(Region::new(region.to_string()));
        }
        loader.load().await
    }
}

#[async_trait]
impl S3Api for AwsS3Client {
    async fn head_bucket(&self, bucket: &str) -> Result<(), S3ClientError> {
        self.client
            .head_bucket()
            .bucket(bucket)
            .send()
            .await
            .map_err(map_sdk_error)?;
        Ok(())
    }

    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, S3ClientError> {
        match self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            Ok(out) => Ok(Some(ObjectMetadata {
                etag: out.e_tag().map(|s| s.to_string()),
                last_modified: None,
                size: out.content_length().unwrap_or_default(),
            })),
            Err(err) => {
                let mapped = map_sdk_error(err);
                if matches!(mapped, S3ClientError::NotFound) {
                    Ok(None)
                } else {
                    Err(mapped)
                }
            }
        }
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<ObjectData>, S3ClientError> {
        match self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            Ok(out) => {
                let etag = out.e_tag().map(|s| s.to_string());
                let bytes = out
                    .body
                    .collect()
                    .await
                    .map_err(|e| {
                        S3ClientError::Internal(format!("failed to collect object body: {e}"))
                    })?
                    .into_bytes()
                    .to_vec();
                Ok(Some(ObjectData {
                    metadata: ObjectMetadata {
                        etag,
                        last_modified: None,
                        size: bytes.len() as i64,
                    },
                    bytes,
                }))
            }
            Err(err) => {
                let mapped = map_sdk_error(err);
                if matches!(mapped, S3ClientError::NotFound) {
                    Ok(None)
                } else {
                    Err(mapped)
                }
            }
        }
    }

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Result<ObjectMetadata, S3ClientError> {
        let mut req = self
            .client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body));

        if let Some(content_type) = content_type {
            req = req.content_type(content_type);
        }
        if let Some(if_match) = if_match {
            req = req.if_match(if_match);
        }
        if let Some(if_none_match) = if_none_match {
            req = req.if_none_match(if_none_match);
        }

        let out = req.send().await.map_err(map_sdk_error)?;

        Ok(ObjectMetadata {
            etag: out.e_tag().map(|s| s.to_string()),
            last_modified: None,
            size: 0,
        })
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), S3ClientError> {
        self.client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(map_sdk_error)?;
        Ok(())
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        continuation: Option<&str>,
        max_keys: Option<i32>,
    ) -> Result<ListObjectsResult, S3ClientError> {
        let mut req = self.client.list_objects_v2().bucket(bucket).prefix(prefix);

        if let Some(token) = continuation {
            req = req.continuation_token(token);
        }
        if let Some(max_keys) = max_keys {
            req = req.max_keys(max_keys);
        }

        let out = req.send().await.map_err(map_sdk_error)?;
        let mut items = Vec::new();

        for item in out.contents() {
            if let Some(key) = item.key() {
                items.push(ListObjectItem {
                    key: key.to_string(),
                    etag: item.e_tag().map(|s| s.to_string()),
                    last_modified: None,
                    size: item.size().unwrap_or_default(),
                });
            }
        }

        Ok(ListObjectsResult {
            items,
            next_continuation_token: out.next_continuation_token().map(|s| s.to_string()),
        })
    }
}

fn map_sdk_error<E>(err: aws_sdk_s3::error::SdkError<E>) -> S3ClientError
where
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    let fallback = format!("{err:?}");

    match err {
        aws_sdk_s3::error::SdkError::ServiceError(service_err) => {
            let code = service_err.err().code().unwrap_or("Unknown");
            let message = service_err
                .err()
                .message()
                .map(str::to_string)
                .unwrap_or_else(|| fallback.clone());
            match code {
                "NoSuchKey" | "NotFound" | "NoSuchBucket" => S3ClientError::NotFound,
                "PreconditionFailed" => S3ClientError::PreconditionFailed,
                "SlowDown" | "Throttling" | "ThrottlingException" => {
                    S3ClientError::Throttled(message)
                }
                "InvalidBucketName" | "InvalidArgument" => S3ClientError::InvalidInput(message),
                _ => S3ClientError::Internal(message),
            }
        }
        aws_sdk_s3::error::SdkError::TimeoutError(_) => S3ClientError::Unavailable(fallback),
        aws_sdk_s3::error::SdkError::DispatchFailure(_) => S3ClientError::Unavailable(fallback),
        _ => S3ClientError::Internal(fallback),
    }
}
