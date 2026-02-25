#![cfg(feature = "s3")]

mod common;

use std::sync::Arc;

use chrono::{Duration, Utc};
use helios_fhir::FhirVersion;
use helios_persistence::backends::s3::{S3Backend, S3BackendConfig, S3TenancyMode};
use helios_persistence::core::{ResourceStorage, VersionedStorage};
use helios_persistence::error::{ResourceError, StorageError};
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use object_store::path::Path;
use object_store::ObjectStore;
use serde::Serialize;
use serde_json::{json, Value};

use common::minio::{
    create_bucket_if_missing, minio_creds, minio_endpoint, minio_object_store, unique_prefix,
    unique_tenant_id,
};

#[derive(Clone, Copy)]
enum TestTenancyMode {
    SharedBucket,
    BucketPerTenant,
}

#[derive(Debug)]
struct TestBackendContext {
    backend: S3Backend,
    tenant: TenantContext,
    store: Arc<dyn ObjectStore>,
    root_prefix: String,
}

#[tokio::test]
async fn s3_minio_vread_roundtrip() {
    for mode in [
        TestTenancyMode::SharedBucket,
        TestTenancyMode::BucketPerTenant,
    ] {
        let ctx = setup_backend(mode).await;

        // Seed equivalent of: create(v1) -> update(v2)
        seed_patient_versions(
            &ctx,
            "patient-vread",
            &[(1, "Initial", false), (2, "Updated", false)],
            2,
        )
        .await;

        let v1 = ctx
            .backend
            .vread(&ctx.tenant, "Patient", "patient-vread", "1")
            .await
            .expect("vread 1 failed")
            .expect("vread 1 should exist");
        let v2 = ctx
            .backend
            .vread(&ctx.tenant, "Patient", "patient-vread", "2")
            .await
            .expect("vread 2 failed")
            .expect("vread 2 should exist");

        assert_eq!(v1.content()["name"][0]["family"], json!("Initial"));
        assert_eq!(v2.content()["name"][0]["family"], json!("Updated"));
    }
}

#[tokio::test]
async fn s3_minio_vread_after_delete() {
    for mode in [
        TestTenancyMode::SharedBucket,
        TestTenancyMode::BucketPerTenant,
    ] {
        let ctx = setup_backend(mode).await;

        // Seed equivalent of: create(v1) -> update(v2) -> delete(v3 tombstone)
        seed_patient_versions(
            &ctx,
            "patient-delete",
            &[(1, "V1", false), (2, "V2", false), (3, "V2", true)],
            3,
        )
        .await;

        match ctx
            .backend
            .read(&ctx.tenant, "Patient", "patient-delete")
            .await
        {
            Err(StorageError::Resource(ResourceError::Gone { .. })) => {}
            other => panic!("expected Gone after delete, got: {other:?}"),
        }

        let v1 = ctx
            .backend
            .vread(&ctx.tenant, "Patient", "patient-delete", "1")
            .await
            .expect("vread 1 failed")
            .expect("vread 1 should still exist");
        assert_eq!(v1.content()["name"][0]["family"], json!("V1"));
    }
}

#[tokio::test]
async fn s3_minio_list_versions_ordering() {
    for mode in [
        TestTenancyMode::SharedBucket,
        TestTenancyMode::BucketPerTenant,
    ] {
        let ctx = setup_backend(mode).await;

        // Seed equivalent of: create(v1) -> update(v2) -> update(v3) -> update(v4)
        seed_patient_versions(
            &ctx,
            "patient-list",
            &[
                (1, "V1", false),
                (2, "V2", false),
                (3, "V3", false),
                (4, "V4", false),
            ],
            4,
        )
        .await;

        let versions = ctx
            .backend
            .list_versions(&ctx.tenant, "Patient", "patient-list")
            .await
            .expect("list_versions failed");

        assert_eq!(versions, vec!["1", "2", "3", "4"]);
    }
}

#[tokio::test]
async fn s3_minio_vread_missing_returns_none() {
    for mode in [
        TestTenancyMode::SharedBucket,
        TestTenancyMode::BucketPerTenant,
    ] {
        let ctx = setup_backend(mode).await;

        seed_patient_versions(&ctx, "patient-missing", &[(1, "V1", false)], 1).await;

        let missing = ctx
            .backend
            .vread(&ctx.tenant, "Patient", "patient-missing", "999")
            .await
            .expect("vread missing failed unexpectedly");

        assert!(missing.is_none());
    }
}

async fn setup_backend(mode: TestTenancyMode) -> TestBackendContext {
    let endpoint = minio_endpoint().await;
    let (access_key, secret_key) = minio_creds();

    let tenant_id = unique_tenant_id();
    let tenant = TenantContext::new(
        TenantId::new(tenant_id.clone()),
        TenantPermissions::full_access(),
    );

    let (tenancy_mode, root_prefix, store) = match mode {
        TestTenancyMode::SharedBucket => {
            let bucket = format!("hfs-s3-vread-shared-{}", unique_prefix());
            create_bucket_if_missing(&bucket)
                .await
                .expect("failed creating shared bucket");

            (
                S3TenancyMode::SharedBucket {
                    bucket: bucket.clone(),
                },
                format!("v1/tenants/{}", tenant_id),
                minio_object_store(&bucket)
                    .await
                    .expect("failed creating shared object store"),
            )
        }
        TestTenancyMode::BucketPerTenant => {
            let bucket_prefix = format!("hfs-s3-vread-{}", unique_prefix());
            let bucket_suffix = Some(unique_prefix());

            let expected_bucket =
                expected_bucket_name(&bucket_prefix, &tenant_id, bucket_suffix.as_deref());
            create_bucket_if_missing(&expected_bucket)
                .await
                .expect("failed creating tenant bucket");

            (
                S3TenancyMode::BucketPerTenant {
                    bucket_prefix,
                    bucket_suffix,
                },
                "v1".to_string(),
                minio_object_store(&expected_bucket)
                    .await
                    .expect("failed creating tenant object store"),
            )
        }
    };

    let config = S3BackendConfig {
        region: "us-east-1".to_string(),
        endpoint: Some(endpoint),
        access_key_id: Some(access_key),
        secret_access_key: Some(secret_key),
        allow_http: true,
        virtual_hosted_style_request: false,
        tenancy_mode,
        ..Default::default()
    };

    let backend = S3Backend::new(config).expect("failed to create s3 backend");

    TestBackendContext {
        backend,
        tenant,
        store,
        root_prefix,
    }
}

#[derive(Debug, Serialize)]
struct VersionDoc {
    schema_version: u32,
    resource_type: String,
    id: String,
    tenant_id: String,
    seq: u64,
    version_id: String,
    created_at: chrono::DateTime<Utc>,
    last_updated: chrono::DateTime<Utc>,
    deleted: bool,
    fhir_version: String,
    resource: Value,
}

#[derive(Debug, Serialize)]
struct StateDoc {
    schema_version: u32,
    current: StateCurrent,
    search: Value,
}

#[derive(Debug, Serialize)]
struct StateCurrent {
    seq: u64,
    version_id: String,
    created_at: chrono::DateTime<Utc>,
    last_updated: chrono::DateTime<Utc>,
    last_updated_ms: i64,
    deleted: bool,
    deleted_at: Option<chrono::DateTime<Utc>>,
    fhir_version: String,
}

async fn seed_patient_versions(
    ctx: &TestBackendContext,
    id: &str,
    versions: &[(u64, &str, bool)],
    current_seq: u64,
) {
    let created_at = Utc::now() - Duration::minutes(1);
    let fhir_version = FhirVersion::default().as_mime_param().to_string();

    for (seq, family, deleted) in versions {
        let last_updated = created_at + Duration::seconds(*seq as i64);
        let version_doc = VersionDoc {
            schema_version: 1,
            resource_type: "Patient".to_string(),
            id: id.to_string(),
            tenant_id: ctx.tenant.tenant_id().as_str().to_string(),
            seq: *seq,
            version_id: seq.to_string(),
            created_at,
            last_updated,
            deleted: *deleted,
            fhir_version: fhir_version.clone(),
            resource: patient_resource(id, family),
        };

        let key = Path::from(format!(
            "{}/resources/Patient/{}/versions/by_seq/{:020}.json",
            ctx.root_prefix, id, seq
        ));
        put_json(&*ctx.store, &key, &version_doc).await;
    }

    let (current_family, current_deleted) = versions
        .iter()
        .find(|(seq, _, _)| *seq == current_seq)
        .map(|(_, family, deleted)| (*family, *deleted))
        .expect("current seq must exist in versions");

    let current_last_updated = created_at + Duration::seconds(current_seq as i64);
    let state = StateDoc {
        schema_version: 1,
        current: StateCurrent {
            seq: current_seq,
            version_id: current_seq.to_string(),
            created_at,
            last_updated: current_last_updated,
            last_updated_ms: current_last_updated.timestamp_millis(),
            deleted: current_deleted,
            deleted_at: if current_deleted {
                Some(current_last_updated)
            } else {
                None
            },
            fhir_version,
        },
        search: json!({"params": {}}),
    };

    let state_key = Path::from(format!(
        "{}/resources/Patient/{}/state.json",
        ctx.root_prefix, id
    ));
    put_json(&*ctx.store, &state_key, &state).await;

    // Keep latest version payload aligned with state in case of read() fallback behavior changes.
    let latest_key = Path::from(format!(
        "{}/resources/Patient/{}/versions/by_seq/{:020}.json",
        ctx.root_prefix, id, current_seq
    ));
    let latest = VersionDoc {
        schema_version: 1,
        resource_type: "Patient".to_string(),
        id: id.to_string(),
        tenant_id: ctx.tenant.tenant_id().as_str().to_string(),
        seq: current_seq,
        version_id: current_seq.to_string(),
        created_at,
        last_updated: current_last_updated,
        deleted: current_deleted,
        fhir_version: FhirVersion::default().as_mime_param().to_string(),
        resource: patient_resource(id, current_family),
    };
    put_json(&*ctx.store, &latest_key, &latest).await;
}

async fn put_json<T: Serialize>(store: &dyn ObjectStore, key: &Path, value: &T) {
    let bytes = serde_json::to_vec(value).expect("serialize seed json");
    store
        .put(key, bytes.into())
        .await
        .unwrap_or_else(|e| panic!("seed put failed for key '{}': {e}", key.as_ref()));
}

fn patient_resource(id: &str, family: &str) -> Value {
    json!({
        "resourceType": "Patient",
        "id": id,
        "name": [{"family": family}]
    })
}

fn expected_bucket_name(
    bucket_prefix: &str,
    tenant_id: &str,
    bucket_suffix: Option<&str>,
) -> String {
    let tenant_slug = sanitize_bucket_segment(tenant_id);
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
