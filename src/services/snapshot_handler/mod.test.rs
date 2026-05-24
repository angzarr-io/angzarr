//! Tests for the snapshot persist helper.
//!
//! These tests pin the canonical persist contract:
//! - Persist only when client provides `snapshot.state`
//! - Use the last event's sequence; fall back to `fallback_sequence`
//!   for snapshot-only updates
//! - Honor `SnapshotRepository`'s `write_enabled` gate

use super::*;
use crate::proto::{
    event_page, page_header, Cover, EventPage, PageHeader, SnapshotRetention, Uuid as ProtoUuid,
};
use crate::repository::SnapshotRepository;
use crate::storage::mock::MockSnapshotStore;
use prost_types::Any;
use std::sync::Arc;
use uuid::Uuid;

fn make_event_page(sequence: u32) -> EventPage {
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(sequence)),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: "test.Event".to_string(),
            value: vec![],
        })),
        created_at: None,
        ..Default::default()
    }
}

fn make_event_book_with_snapshot(pages: Vec<EventPage>, has_snapshot: bool) -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: "test".to_string(),
            root: Some(ProtoUuid {
                value: Uuid::new_v4().as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        pages,
        snapshot: if has_snapshot {
            Some(Snapshot {
                sequence: 0, // Helper recomputes from pages / fallback
                state: Some(Any {
                    type_url: "test.State".to_string(),
                    value: vec![1, 2, 3],
                }),
                retention: SnapshotRetention::RetentionDefault as i32,
                created_at: None,
            })
        } else {
            None
        },
        ..Default::default()
    }
}

// -----------------------------------------------------------------
// compute_snapshot_sequence
// -----------------------------------------------------------------

#[test]
fn test_compute_snapshot_sequence_empty_pages_no_fallback() {
    let event_book = make_event_book_with_snapshot(vec![], false);
    assert_eq!(compute_snapshot_sequence(&event_book, None), 0);
}

#[test]
fn test_compute_snapshot_sequence_empty_pages_with_fallback() {
    // Snapshot-only update — no new events; fallback anchors the
    // snapshot at the most recent persisted event the caller knows about.
    let event_book = make_event_book_with_snapshot(vec![], false);
    assert_eq!(compute_snapshot_sequence(&event_book, Some(7)), 7);
}

#[test]
fn test_compute_snapshot_sequence_single_page_ignores_fallback() {
    // Pages-present path: the last page's seq wins; fallback is unused.
    let event_book = make_event_book_with_snapshot(vec![make_event_page(0)], false);
    assert_eq!(compute_snapshot_sequence(&event_book, Some(99)), 0);
}

#[test]
fn test_compute_snapshot_sequence_multiple_pages() {
    let event_book = make_event_book_with_snapshot(
        vec![make_event_page(0), make_event_page(1), make_event_page(2)],
        false,
    );
    assert_eq!(compute_snapshot_sequence(&event_book, None), 2);
}

// -----------------------------------------------------------------
// persist_snapshot_if_present
// -----------------------------------------------------------------

#[tokio::test]
async fn test_persist_with_write_disabled_is_noop() {
    let store = Arc::new(MockSnapshotStore::new());
    let repo = SnapshotRepository::with_flags(store.clone(), true, false);
    let event_book = make_event_book_with_snapshot(vec![make_event_page(0)], true);
    let root = Uuid::new_v4();

    persist_snapshot_if_present(&repo, &event_book, "test", "test", root, None)
        .await
        .unwrap();

    let stored = store.get_stored("test", "test", root).await;
    assert!(
        stored.is_none(),
        "write_enabled=false must drop the persist silently"
    );
}

#[tokio::test]
async fn test_persist_with_no_snapshot_state_is_noop() {
    let store = Arc::new(MockSnapshotStore::new());
    let repo = SnapshotRepository::new(store.clone());
    // Book carries no snapshot at all
    let event_book = make_event_book_with_snapshot(vec![make_event_page(0)], false);
    let root = Uuid::new_v4();

    persist_snapshot_if_present(&repo, &event_book, "test", "test", root, None)
        .await
        .unwrap();

    let stored = store.get_stored("test", "test", root).await;
    assert!(
        stored.is_none(),
        "absent snapshot.state means 'no persist requested by client'"
    );
}

#[tokio::test]
async fn test_persist_with_state_succeeds_and_uses_last_page_seq() {
    let store = Arc::new(MockSnapshotStore::new());
    let repo = SnapshotRepository::new(store.clone());
    let event_book = make_event_book_with_snapshot(
        vec![make_event_page(0), make_event_page(1), make_event_page(2)],
        true,
    );
    let root = Uuid::new_v4();

    persist_snapshot_if_present(&repo, &event_book, "test", "test", root, None)
        .await
        .unwrap();

    let stored = store
        .get_stored("test", "test", root)
        .await
        .expect("snapshot persisted");
    assert_eq!(
        stored.sequence, 2,
        "snapshot sequence must be the last event's seq, not the proto field"
    );
}

/// R2-SNAP-6: persisted snapshot carries a populated `created_at`
/// timestamp. Required for temporal-by-time queries (R2-SNAP-7) to
/// decide whether the snapshot's coverage predates the target.
#[tokio::test]
async fn test_persist_stamps_created_at() {
    let store = Arc::new(MockSnapshotStore::new());
    let repo = SnapshotRepository::new(store.clone());
    let event_book = make_event_book_with_snapshot(vec![make_event_page(0)], true);
    let root = Uuid::new_v4();

    let before = chrono::Utc::now().timestamp();
    persist_snapshot_if_present(&repo, &event_book, "test", "test", root, None)
        .await
        .unwrap();
    let after = chrono::Utc::now().timestamp();

    let stored = store
        .get_stored("test", "test", root)
        .await
        .expect("snapshot persisted");
    let ts = stored
        .created_at
        .as_ref()
        .expect("created_at must be populated by persist");
    // The stamped timestamp must be inside the persist window. Both
    // assertions kill mutations: missing the stamp (None passes
    // unwrap with panic-message) and using a fixed sentinel like
    // UNIX_EPOCH or constant-zero seconds.
    assert!(
        ts.seconds >= before && ts.seconds <= after,
        "created_at.seconds = {} must fall in [{}, {}]",
        ts.seconds,
        before,
        after
    );
}

#[tokio::test]
async fn test_persist_snapshot_only_uses_fallback_sequence() {
    // Snapshot-only update (no new events) — sequence should anchor at
    // the caller's fallback (typically prior_max_seq), NOT default to 0.
    let store = Arc::new(MockSnapshotStore::new());
    let repo = SnapshotRepository::new(store.clone());
    let event_book = make_event_book_with_snapshot(vec![], true);
    let root = Uuid::new_v4();

    persist_snapshot_if_present(&repo, &event_book, "test", "test", root, Some(42))
        .await
        .unwrap();

    let stored = store.get_stored("test", "test", root).await.unwrap();
    assert_eq!(
        stored.sequence, 42,
        "snapshot-only update must use fallback"
    );
}
