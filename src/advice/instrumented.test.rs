//! Tests for the metrics instrumentation wrapper.
//!
//! Why this matters: the wrapper sits in front of every production store;
//! it must be transparent (same results, same errors) or instrumentation
//! would change behaviour.

use super::*;
use crate::storage::contract::{main, page, root};
use crate::storage::{MemoryEventStore, MemorySnapshotStore, StorageError};

#[tokio::test]
async fn test_instrumented_delegates_to_inner() {
    let store = Instrumented::new(MemoryEventStore::new(), "memory");
    store
        .append(main(1), &[page(0, root(100), root(200), 10)])
        .await
        .unwrap();
    assert_eq!(store.read(main(1), 0, None).await.unwrap().len(), 1);
    assert_eq!(store.head(main(1)).await.unwrap().next_sequence, 1);
}

#[tokio::test]
async fn test_instrumented_preserves_errors() {
    let store = Instrumented::new(MemoryEventStore::new(), "memory");
    let err = store
        .append(main(1), &[page(3, root(100), root(200), 10)])
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::SequenceConflict { .. }));
}

#[tokio::test]
async fn test_instrumented_snapshot_store_delegates() {
    let store = Instrumented::new(MemorySnapshotStore::new(), "memory");
    store
        .put(main(1), &crate::storage::contract::snapshot(4))
        .await
        .unwrap();
    assert_eq!(store.get(main(1)).await.unwrap().unwrap().sequence, 4);
}

#[test]
fn test_instrumented_accessors() {
    let instrumented = Instrumented::new(MemoryEventStore::new(), "memory");
    let _ = instrumented.inner();
    let _recovered: MemoryEventStore = instrumented.into_inner();
}
