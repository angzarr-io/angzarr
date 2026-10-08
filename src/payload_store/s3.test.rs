//! Tests for S3 payload store.
//!
//! S3 payload store uses content-addressable storage in S3 buckets:
//! - Object key: {prefix}/{hash[0:2]}/{hash}
//! - URI format: s3://{bucket}/{key}
//!
//! Why this matters: S3 is the most widely used object storage. The
//! hash-based key structure avoids hot partitions (S3 distributes objects
//! based on key prefix).
//!
//! Key behaviors verified:
//! - Object key uses hash-based directory sharding
//! - Prefix is correctly prepended when configured
//! - URI format follows s3:// scheme
//!
//! Note: Full S3 integration requires credentials and a real bucket.
//! Unit tests verify key/URI construction logic only.

use super::*;

/// Store over a client that never touches the network (no credentials
/// resolved, no requests sent by the tests below).
fn offline_store(prefix: Option<&str>) -> S3PayloadStore {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .build();
    S3PayloadStore::with_client(
        Client::from_conf(config),
        "payload-bucket",
        prefix.map(String::from),
    )
}

/// Object key is `[{prefix}/]{hash[0:2]}/{hash}` — the layout the reaper
/// recognizes, so payloads the store writes are the ones it reaps.
#[test]
fn test_object_key_layout_is_reapable() {
    let hash = compute_hash(b"test payload");
    let hex = hash_to_hex(&hash);

    let bare = offline_store(None).object_key(&hash);
    assert_eq!(bare, format!("{}/{}", &hex[0..2], hex));
    assert!(is_payload_object_key(&bare, None));

    let prefixed = offline_store(Some("payloads")).object_key(&hash);
    assert_eq!(prefixed, format!("payloads/{}/{}", &hex[0..2], hex));
    assert!(is_payload_object_key(&prefixed, Some("payloads")));
}

/// URIs name the bucket and key.
#[test]
fn test_uri_format() {
    let store = offline_store(None);
    assert_eq!(store.uri_for_object("ab/cd"), "s3://payload-bucket/ab/cd");
    assert_eq!(
        store.key_from_uri("s3://payload-bucket/ab/cd").unwrap(),
        "ab/cd"
    );
    assert!(store.key_from_uri("s3://other-bucket/ab/cd").is_err());
}

/// A reference whose key is not this store's object for its content hash
/// (another prefix, an arbitrary key) is refused before any request, so a
/// forged reference cannot read arbitrary bucket objects.
#[tokio::test]
async fn test_get_rejects_reference_outside_store_layout() {
    let store = offline_store(Some("payloads"));
    let hash = compute_hash(b"payload");
    let reference = PayloadReference {
        storage_type: PayloadStorageType::S3 as i32,
        uri: "s3://payload-bucket/secrets/credentials.json".into(),
        content_hash: hash,
        original_size: 7,
        stored_at: None,
    };

    let err = store.get(&reference).await.unwrap_err();
    assert!(matches!(err, PayloadStoreError::InvalidUri(_)), "{err}");
}
