//! Tests for payload store core functionality.
//!
//! The payload store implements the claim check pattern: large payloads
//! are stored externally and replaced with references. Content-addressable
//! storage (SHA-256 hashing) enables deduplication and integrity verification.
//!
//! Why this matters: Message buses have size limits (SQS: 256KB, Kafka: 1MB).
//! Without payload offloading, large events/commands would fail to transmit.
//! The hash-based storage also prevents storing duplicate payloads.
//!
//! Key behaviors verified:
//! - Hash computation is deterministic (same input → same hash)
//! - Hash hex roundtrip preserves data
//! - Factory respects enabled/disabled config
//! - Store + retrieve roundtrip works correctly

use super::*;
use tempfile::TempDir;

// ============================================================================
// Hash Function Tests
// ============================================================================

/// Same payload always produces the same hash.
///
/// This is foundational for content-addressable storage and deduplication.
#[test]
fn test_compute_hash_deterministic() {
    let payload = b"test payload data";
    let hash1 = compute_hash(payload);
    let hash2 = compute_hash(payload);
    assert_eq!(hash1, hash2);
    assert_eq!(hash1.len(), 32); // SHA-256 produces 32 bytes
}

/// Different payloads produce different hashes.
///
/// Collision resistance: distinct payloads get distinct storage keys.
#[test]
fn test_compute_hash_different_inputs() {
    let hash1 = compute_hash(b"payload 1");
    let hash2 = compute_hash(b"payload 2");
    assert_ne!(hash1, hash2);
}

/// Hash can be converted to hex and back without loss.
///
/// Hex encoding is used for filenames and URIs.
#[test]
fn test_hash_hex_roundtrip() {
    let original = compute_hash(b"test");
    let hex = hash_to_hex(&original);
    let recovered = hex_to_hash(&hex).unwrap();
    assert_eq!(original, recovered);
}

/// Invalid hex string returns error.
#[test]
fn test_hex_to_hash_invalid() {
    let result = hex_to_hash("not valid hex!");
    assert!(result.is_err());
}

// ============================================================================
// Factory Tests
// ============================================================================

/// Factory returns None when offloading is disabled.
///
/// Disabled config means no external storage needed.
#[tokio::test]
async fn test_init_payload_store_disabled() {
    let config = PayloadOffloadConfig {
        enabled: false,
        ..Default::default()
    };

    let result = init_payload_store(&config).await.unwrap();
    assert!(result.is_none());
}

/// Factory creates filesystem store when configured.
#[tokio::test]
async fn test_init_payload_store_filesystem() {
    let temp_dir = TempDir::new().unwrap();
    let config = PayloadOffloadConfig {
        enabled: true,
        store_type: PayloadStoreType::Filesystem,
        filesystem: FilesystemStoreConfig {
            base_path: temp_dir.path().to_path_buf(),
        },
        ..Default::default()
    };

    let result = init_payload_store(&config).await.unwrap();
    assert!(result.is_some());

    let store = result.unwrap();
    assert_eq!(
        store.storage_type(),
        crate::proto::PayloadStorageType::Filesystem
    );
}

/// Full store + retrieve roundtrip works correctly.
///
/// End-to-end test: payload → store → reference → retrieve → payload.
#[tokio::test]
async fn test_init_payload_store_can_store_and_retrieve() {
    let temp_dir = TempDir::new().unwrap();
    let config = PayloadOffloadConfig {
        enabled: true,
        store_type: PayloadStoreType::Filesystem,
        filesystem: FilesystemStoreConfig {
            base_path: temp_dir.path().to_path_buf(),
        },
        ..Default::default()
    };

    let store = init_payload_store(&config).await.unwrap().unwrap();

    let payload = b"test payload from factory";
    let reference = store.put(payload).await.unwrap();
    let retrieved = store.get(&reference).await.unwrap();

    assert_eq!(payload.as_slice(), retrieved.as_slice());
}

// ============================================================================
// init_payload_offload / PayloadOffload
// ============================================================================

fn fs_offload_config(dir: &TempDir) -> PayloadOffloadConfig {
    PayloadOffloadConfig {
        enabled: true,
        store_type: PayloadStoreType::Filesystem,
        threshold_bytes: 1024,
        filesystem: FilesystemStoreConfig {
            base_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    }
}

/// Disabled offload yields nothing to wrap with: buses pass through.
#[tokio::test]
async fn test_init_payload_offload_disabled() {
    let offload = init_payload_offload(&PayloadOffloadConfig::default())
        .await
        .unwrap();
    assert!(offload.is_none());
}

/// Enabled offload wraps a publisher so an oversized event is published
/// as an External reference that resolves back to the original bytes.
#[tokio::test]
async fn test_init_payload_offload_wraps_publisher() {
    use crate::bus::MockEventBus;
    use crate::proto::{event_page, EventBook, EventPage};

    let dir = TempDir::new().unwrap();
    let offload = init_payload_offload(&fs_offload_config(&dir))
        .await
        .unwrap()
        .expect("enabled");

    let mock = Arc::new(MockEventBus::new());
    let bus = with_offload(Arc::clone(&mock) as Arc<dyn EventBus>, Some(&offload));

    let book = EventBook {
        pages: vec![EventPage {
            payload: Some(event_page::Payload::Event(prost_types::Any {
                type_url: "t.Big".into(),
                value: vec![9u8; 8192],
            })),
            ..Default::default()
        }],
        ..Default::default()
    };
    bus.publish(Arc::new(book)).await.unwrap();

    let published = mock.take_published().await;
    match &published[0].pages[0].payload {
        Some(event_page::Payload::External(reference)) => {
            assert!(reference.uri.starts_with("file://"));
        }
        other => panic!("expected External, got {other:?}"),
    }
}

/// Without offload the bus is returned unchanged.
#[tokio::test]
async fn test_with_offload_none_is_identity() {
    use crate::bus::MockEventBus;
    let mock: Arc<dyn EventBus> = Arc::new(MockEventBus::new());
    let bus = with_offload(Arc::clone(&mock), None);
    assert!(Arc::ptr_eq(&mock, &bus));
}

/// Enabling offload starts the TTL reaper: payloads past retention are
/// removed without any caller driving cleanup.
#[tokio::test]
async fn test_init_payload_offload_spawns_reaper() {
    let dir = TempDir::new().unwrap();
    let store = FilesystemPayloadStore::new(dir.path()).await.unwrap();
    let reference = store.put(b"expired payload").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let config = PayloadOffloadConfig {
        retention_hours: 0,
        cleanup_interval_secs: 3600,
        ..fs_offload_config(&dir)
    };
    let _offload = init_payload_offload(&config)
        .await
        .unwrap()
        .expect("enabled");

    // The reaper's first tick fires immediately.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while store.get(&reference).await.is_ok() {
        assert!(
            std::time::Instant::now() < deadline,
            "reaper never removed the expired payload"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

// ============================================================================
// is_payload_object_key
// ============================================================================

const HASH: &str = "ab34567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

/// Keys the stores write are recognized, with and without a prefix.
#[test]
fn test_payload_object_key_matches_store_layout() {
    assert!(is_payload_object_key(&format!("ab/{HASH}"), None));
    assert!(is_payload_object_key(
        &format!("payloads/ab/{HASH}"),
        Some("payloads")
    ));
}

/// Anything else in the bucket — other prefixes, other names, a subdir
/// that does not match the hash — is never treated as a payload, so the
/// reaper cannot delete it.
#[test]
fn test_payload_object_key_rejects_foreign_objects() {
    assert!(!is_payload_object_key("backups/db.sql", None));
    assert!(!is_payload_object_key(&format!("cd/{HASH}"), None));
    assert!(!is_payload_object_key(&format!("ab/{HASH}.bin"), None));
    assert!(!is_payload_object_key(
        &format!("other/ab/{HASH}"),
        Some("payloads")
    ));
    assert!(!is_payload_object_key(
        &format!("payloadsab/{HASH}"),
        Some("payloads")
    ));
    assert!(!is_payload_object_key(&format!("ab/{}", &HASH[..63]), None));
    assert!(!is_payload_object_key(
        &format!("ab/{}", HASH.to_uppercase()),
        None
    ));
    assert!(!is_payload_object_key(&format!("a/{HASH}"), None));
}

/// Keys produced by `payload_object_key` are exactly the ones the reaper
/// treats as payloads.
#[test]
fn test_payload_object_key_round_trips_through_reaper_check() {
    let hash = compute_hash(b"payload");
    let hex = hash_to_hex(&hash);
    assert_eq!(
        payload_object_key(None, &hash),
        format!("{}/{}", &hex[..2], hex)
    );
    assert_eq!(
        payload_object_key(Some("p"), &hash),
        format!("p/{}/{}", &hex[..2], hex)
    );
    assert!(is_payload_object_key(
        &payload_object_key(None, &hash),
        None
    ));
    assert!(is_payload_object_key(
        &payload_object_key(Some("p"), &hash),
        Some("p")
    ));
}
