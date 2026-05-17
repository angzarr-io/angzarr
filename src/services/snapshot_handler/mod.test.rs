use super::*;
use crate::proto::{
    event_page, page_header, Cover, EventPage, PageHeader, SnapshotRetention, Uuid as ProtoUuid,
};
use crate::storage::mock::MockSnapshotStore;
use prost_types::Any;

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
        }),
        pages,
        snapshot: if has_snapshot {
            Some(Snapshot {
                sequence: 0, // Framework computes from pages
                state: Some(Any {
                    type_url: "test.State".to_string(),
                    value: vec![1, 2, 3],
                }),
                retention: SnapshotRetention::RetentionDefault as i32,
            })
        } else {
            None
        },
        ..Default::default()
    }
}

#[test]
fn test_compute_snapshot_sequence_empty_pages() {
    let event_book = make_event_book_with_snapshot(vec![], false);
    assert_eq!(compute_snapshot_sequence(&event_book), 0);
}

#[test]
fn test_compute_snapshot_sequence_single_page() {
    let event_book = make_event_book_with_snapshot(vec![make_event_page(0)], false);
    assert_eq!(compute_snapshot_sequence(&event_book), 0);
}

#[test]
fn test_compute_snapshot_sequence_multiple_pages() {
    let event_book = make_event_book_with_snapshot(
        vec![make_event_page(0), make_event_page(1), make_event_page(2)],
        false,
    );
    assert_eq!(compute_snapshot_sequence(&event_book), 2);
}

#[tokio::test]
async fn test_persist_snapshot_if_present_disabled() {
    let snapshot_store: Arc<dyn SnapshotStore> = Arc::new(MockSnapshotStore::new());
    let mock_store = Arc::new(MockSnapshotStore::new());
    let event_book = make_event_book_with_snapshot(vec![make_event_page(0)], true);
    let root = Uuid::new_v4();

    let result =
        persist_snapshot_if_present(&snapshot_store, &event_book, "test", "test", root, false)
            .await;

    assert!(result.is_ok());
    // No snapshot should be stored when disabled
    let stored = mock_store.get_stored("test", "test", root).await;
    assert!(stored.is_none());
}

#[tokio::test]
async fn test_persist_snapshot_if_present_no_state() {
    let mock_store = Arc::new(MockSnapshotStore::new());
    let snapshot_store: Arc<dyn SnapshotStore> = Arc::clone(&mock_store) as Arc<dyn SnapshotStore>;
    let event_book = make_event_book_with_snapshot(vec![make_event_page(0)], false);
    let root = Uuid::new_v4();

    let result =
        persist_snapshot_if_present(&snapshot_store, &event_book, "test", "test", root, true).await;

    assert!(result.is_ok());
    // No snapshot should be stored when no state
    let stored = mock_store.get_stored("test", "test", root).await;
    assert!(stored.is_none());
}

#[tokio::test]
async fn test_persist_snapshot_if_present_success() {
    let mock_store = Arc::new(MockSnapshotStore::new());
    let snapshot_store: Arc<dyn SnapshotStore> = Arc::clone(&mock_store) as Arc<dyn SnapshotStore>;
    let event_book = make_event_book_with_snapshot(vec![make_event_page(0)], true);
    let root = Uuid::new_v4();

    let result =
        persist_snapshot_if_present(&snapshot_store, &event_book, "test", "test", root, true).await;

    assert!(result.is_ok());
    let stored = mock_store.get_stored("test", "test", root).await;
    assert!(stored.is_some());
    // Snapshot sequence is the last event sequence (0), not incremented
    assert_eq!(stored.unwrap().sequence, 0);
}
