//! Tests for filesystem payload store.
//!
//! Filesystem store uses content-addressable files:
//! - Path: {base_path}/{hash[0:2]}/{hash}.bin
//! - URI format: file://{path}
//!
//! Why this matters: The filesystem store is the simplest backend for
//! local development and small deployments. It demonstrates the full
//! PayloadStore trait contract without cloud dependencies.
//!
//! Key behaviors verified:
//! - Put/get roundtrip stores and retrieves payloads
//! - Deduplication: same content = same file (content-addressable)
//! - Integrity verification: corrupted hash detected on get
//! - Age-based cleanup: delete_older_than removes old files
//! - Invalid URI handling: wrong scheme rejected

use super::*;
use tempfile::TempDir;

/// Helper to create a temporary store for testing.
async fn create_temp_store() -> (FilesystemPayloadStore, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let store = FilesystemPayloadStore::new(temp_dir.path()).await.unwrap();
    (store, temp_dir)
}

// ============================================================================
// Basic CRUD Tests
// ============================================================================

/// Put stores payload and get retrieves it.
#[tokio::test]
async fn test_put_and_get() {
    let (store, _temp) = create_temp_store().await;
    let payload = b"test payload data";

    let reference = store.put(payload).await.unwrap();

    assert_eq!(
        reference.storage_type,
        PayloadStorageType::Filesystem as i32
    );
    assert!(reference.uri.starts_with("file://"));
    assert_eq!(reference.original_size, payload.len() as u64);
    assert!(!reference.content_hash.is_empty());

    let retrieved = store.get(&reference).await.unwrap();
    assert_eq!(retrieved, payload);
}

// ============================================================================
// Deduplication Tests
// ============================================================================

/// Same content produces same reference (content-addressable).
#[tokio::test]
async fn test_deduplication() {
    let (store, _temp) = create_temp_store().await;
    let payload = b"duplicate payload";

    let ref1 = store.put(payload).await.unwrap();
    let ref2 = store.put(payload).await.unwrap();

    // Same hash means same URI
    assert_eq!(ref1.uri, ref2.uri);
    assert_eq!(ref1.content_hash, ref2.content_hash);
}

/// Different content produces different references.
#[tokio::test]
async fn test_different_payloads() {
    let (store, _temp) = create_temp_store().await;

    let ref1 = store.put(b"payload one").await.unwrap();
    let ref2 = store.put(b"payload two").await.unwrap();

    assert_ne!(ref1.uri, ref2.uri);
    assert_ne!(ref1.content_hash, ref2.content_hash);
}

// ============================================================================
// Error Handling Tests
// ============================================================================

/// Get returns NotFound for a payload this store never wrote.
#[tokio::test]
async fn test_get_not_found() {
    let (store, _temp) = create_temp_store().await;
    let hash = compute_hash(b"never stored");

    let reference = PayloadReference {
        storage_type: PayloadStorageType::Filesystem as i32,
        uri: store.uri_for_path(&store.path_for_hash(&hash)),
        content_hash: hash,
        original_size: 100,
        stored_at: None,
    };

    let result = store.get(&reference).await;
    assert!(matches!(result, Err(PayloadStoreError::NotFound(_))));
}

/// A file whose bytes no longer match the reference's hash (corrupted on
/// disk) fails the integrity check.
#[tokio::test]
async fn test_integrity_check() {
    let (store, _temp) = create_temp_store().await;
    let reference = store.put(b"original payload").await.unwrap();

    let path = store.path_from_uri(&reference.uri).unwrap();
    std::fs::write(&path, b"tampered payload").unwrap();

    let result = store.get(&reference).await;
    assert!(matches!(
        result,
        Err(PayloadStoreError::IntegrityFailed { .. })
    ));
}

/// A reference naming any file other than this store's file for its
/// content hash is refused without reading it — a forged reference cannot
/// read arbitrary files on the sidecar.
#[tokio::test]
async fn test_get_refuses_path_outside_store() {
    let (store, temp) = create_temp_store().await;
    let outside = temp.path().join("..").join("secret.txt");
    let reference = PayloadReference {
        storage_type: PayloadStorageType::Filesystem as i32,
        uri: format!("file://{}", outside.display()),
        content_hash: compute_hash(b"secret"),
        original_size: 6,
        stored_at: None,
    };

    let result = store.get(&reference).await;
    assert!(
        matches!(result, Err(PayloadStoreError::InvalidUri(_))),
        "{result:?}"
    );
}

/// Re-storing an existing payload refreshes its modification time, so the
/// TTL reaper does not delete a payload a new event has just referenced.
#[tokio::test]
async fn test_put_existing_refreshes_mtime() {
    let (store, _temp) = create_temp_store().await;
    let reference = store.put(b"shared payload").await.unwrap();
    let path = store.path_from_uri(&reference.uri).unwrap();

    let old = SystemTime::now() - Duration::from_secs(48 * 3600);
    std::fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_modified(old)
        .unwrap();

    store.put(b"shared payload").await.unwrap();

    let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    assert!(
        modified > SystemTime::now() - Duration::from_secs(60),
        "mtime not refreshed"
    );
    assert_eq!(
        store
            .delete_older_than(Duration::from_secs(3600))
            .await
            .unwrap(),
        0
    );
}

/// Concurrent writers of one payload each use their own temp file: all
/// succeed and no temp file is left behind.
#[tokio::test]
async fn test_concurrent_puts_of_same_payload() {
    let (store, temp) = create_temp_store().await;
    let store = std::sync::Arc::new(store);
    let payload = vec![5u8; 64 * 1024];

    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let store = std::sync::Arc::clone(&store);
            let payload = payload.clone();
            tokio::spawn(async move { store.put(&payload).await })
        })
        .collect();
    let mut refs = Vec::new();
    for t in tasks {
        refs.push(t.await.unwrap().unwrap());
    }

    assert_eq!(store.get(&refs[0]).await.unwrap(), payload);
    let leftovers: Vec<_> = walk(temp.path())
        .into_iter()
        .filter(|p| p.to_string_lossy().ends_with(".tmp"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(walk(&path));
        } else {
            out.push(path);
        }
    }
    out
}

// ============================================================================
// Cleanup Tests
// ============================================================================

/// delete_older_than removes files older than the threshold.
#[tokio::test]
async fn test_delete_older_than() {
    let (store, _temp) = create_temp_store().await;

    // Store a payload
    let reference = store.put(b"old payload").await.unwrap();
    let path = store.path_from_uri(&reference.uri).unwrap();

    // Verify it exists
    assert!(path.exists());

    // Delete nothing (age = 1 hour, file is new)
    let deleted = store
        .delete_older_than(Duration::from_secs(3600))
        .await
        .unwrap();
    assert_eq!(deleted, 0);
    assert!(path.exists());

    // Manually backdate the file
    let old_time =
        std::fs::FileTimes::new().set_modified(SystemTime::now() - Duration::from_secs(7200));
    std::fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_times(old_time)
        .unwrap();

    // Now delete (age = 1 hour, file is 2 hours old)
    let deleted = store
        .delete_older_than(Duration::from_secs(3600))
        .await
        .unwrap();
    assert_eq!(deleted, 1);
    assert!(!path.exists());
}

/// Wrong URI scheme (gs:// instead of file://) rejected.
#[tokio::test]
async fn test_invalid_uri() {
    let (store, _temp) = create_temp_store().await;

    let reference = PayloadReference {
        storage_type: PayloadStorageType::Filesystem as i32,
        uri: "gs://bucket/key".to_string(), // Wrong scheme
        content_hash: vec![0; 32],
        original_size: 100,
        stored_at: None,
    };

    let result = store.get(&reference).await;
    assert!(matches!(result, Err(PayloadStoreError::InvalidUri(_))));
}
