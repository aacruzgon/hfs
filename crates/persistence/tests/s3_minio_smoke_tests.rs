#![cfg(feature = "s3")]

//! Phase 0A MinIO smoke tests for object_store wiring only.

mod common;

use common::minio::{
    MinioHarness, create_bucket_if_missing, minio_object_store, unique_prefix, unique_tenant_id,
};
use object_store::ObjectStore;
use object_store::path::Path;

fn assert_not_found(err: object_store::Error) {
    match err {
        object_store::Error::NotFound { .. } => {}
        other => panic!("expected NotFound, got: {other}"),
    }
}

#[tokio::test]
async fn s3_minio_put_get_delete_roundtrip() {
    let bucket = format!("hfs-smoke-shared-{}", unique_prefix());
    create_bucket_if_missing(&bucket)
        .await
        .expect("bucket create failed");
    let store = minio_object_store(&bucket)
        .await
        .expect("store create failed");

    let tenant = unique_tenant_id();
    let key = Path::from(format!("v1/tenants/{tenant}/smoke/roundtrip.txt"));
    let payload = b"hello".to_vec();

    store
        .put(&key, payload.clone().into())
        .await
        .expect("put failed");

    let read = store
        .get(&key)
        .await
        .expect("get failed")
        .bytes()
        .await
        .expect("read bytes failed");
    assert_eq!(read.as_ref(), payload.as_slice());

    store.delete(&key).await.expect("delete failed");
    let err = store
        .get(&key)
        .await
        .expect_err("expected not found after delete");
    assert_not_found(err);
}

#[tokio::test]
async fn s3_minio_tenant_prefix_isolation() {
    let harness = MinioHarness::shared().await;
    let shared_bucket = format!("hfs-shared-{}", unique_prefix());
    let client_a = harness
        .client_shared_bucket(&shared_bucket)
        .await
        .expect("client A init failed");
    let client_b = harness
        .client_shared_bucket(&shared_bucket)
        .await
        .expect("client B init failed");

    let key_a = client_a.key("resources/patient-a.json");
    let wrong_scope_key = client_b.key("resources/patient-a.json");
    let payload = b"hello".to_vec();

    client_a
        .store
        .put(&key_a, payload.clone().into())
        .await
        .expect("put failed");

    let read = client_a
        .store
        .get(&key_a)
        .await
        .expect("get in tenant A failed")
        .bytes()
        .await
        .expect("read bytes failed");
    assert_eq!(read.as_ref(), payload.as_slice());

    let err = client_b
        .store
        .get(&wrong_scope_key)
        .await
        .expect_err("tenant B should not read tenant A object");
    assert_not_found(err);

    client_a.store.delete(&key_a).await.expect("delete failed");
    let err = client_a
        .store
        .get(&key_a)
        .await
        .expect_err("expected not found after delete");
    assert_not_found(err);
}

#[tokio::test]
async fn s3_minio_bucket_per_tenant_isolation() {
    let harness = MinioHarness::shared().await;
    let client_a = harness
        .client_bucket_per_tenant("hfs-tenant")
        .await
        .expect("client A init failed");
    let client_b = harness
        .client_bucket_per_tenant("hfs-tenant")
        .await
        .expect("client B init failed");

    let key = Path::from(format!("v1/{}/object.txt", unique_prefix()));
    let payload = b"hello".to_vec();

    client_a
        .store
        .put(&key, payload.clone().into())
        .await
        .expect("put failed");

    let read = client_a
        .store
        .get(&key)
        .await
        .expect("tenant A get failed")
        .bytes()
        .await
        .expect("read bytes failed");
    assert_eq!(read.as_ref(), payload.as_slice());

    let err = client_b
        .store
        .get(&key)
        .await
        .expect_err("tenant B bucket should not see tenant A object");
    assert_not_found(err);

    client_a.store.delete(&key).await.expect("delete failed");
    let err = client_a
        .store
        .get(&key)
        .await
        .expect_err("expected not found after delete");
    assert_not_found(err);
}
