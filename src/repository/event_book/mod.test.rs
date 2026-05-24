//! Tests for EventBook repository.
//!
//! The repository combines event store and snapshot store operations:
//! - Optimized loading with snapshot + events-after-snapshot
//! - Temporal queries (by time, by sequence)
//! - Sparse sequence queries
//!
//! Key behaviors verified:
//! - New aggregates return empty EventBooks
//! - Snapshot loading starts from snapshot.sequence + 1
//! - Snapshot reading can be disabled for debugging/migration
//! - Temporal queries ignore snapshots (full replay required)
//! - Sparse queries filter to requested sequences

use super::*;
use crate::proto::{event_page, page_header, EventPage, PageHeader, Snapshot, SnapshotRetention};
use crate::proto_ext::EventPageExt;
use crate::repository::SnapshotRepository;
use crate::storage::mock::{MockEventStore, MockSnapshotStore};
use crate::storage::SnapshotStore;
use crate::test_utils::{make_event_book_with_root, make_event_page};

/// Build an EventBookRepository wrapping the snapshot store in a
/// default-policy SnapshotRepository (both flags enabled). Keeps the
/// existing test call sites short; explicit-policy tests use the
/// `make_repo_with_read_enabled` variant below.
fn make_repo(
    event_store: Arc<dyn crate::storage::EventStore>,
    snapshot_store: Arc<dyn SnapshotStore>,
) -> EventBookRepository {
    EventBookRepository::new(
        event_store,
        Arc::new(SnapshotRepository::new(snapshot_store)),
    )
}

/// Variant for tests that need to toggle snapshot reads.
fn make_repo_with_read_enabled(
    event_store: Arc<dyn crate::storage::EventStore>,
    snapshot_store: Arc<dyn SnapshotStore>,
    read_enabled: bool,
) -> EventBookRepository {
    EventBookRepository::new(
        event_store,
        Arc::new(SnapshotRepository::with_flags(
            snapshot_store,
            read_enabled,
            true,
        )),
    )
}

// ============================================================================
// Basic CRUD Tests
// ============================================================================

/// New aggregate returns empty EventBook.
#[tokio::test]
async fn test_get_returns_empty_book_for_new_aggregate() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let repo = make_repo(event_store, snapshot_store);

    let root = Uuid::new_v4();
    let book = repo.get("orders", "test", root).await.unwrap();

    assert!(book.pages.is_empty());
    assert!(book.snapshot.is_none());
    assert_eq!(book.cover.as_ref().unwrap().domain, "orders");
}

/// Put + get roundtrip preserves events.
#[tokio::test]
async fn test_put_and_get_roundtrip() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let repo = make_repo(event_store, snapshot_store);

    let root = Uuid::new_v4();
    let book =
        make_event_book_with_root("orders", root, vec![make_event_page(0), make_event_page(1)]);

    repo.put("test", &book, None, None).await.unwrap();

    let retrieved = repo.get("orders", "test", root).await.unwrap();
    assert_eq!(retrieved.pages.len(), 2);
}

// ============================================================================
// Snapshot Loading Tests
// ============================================================================

/// Loading with snapshot only returns events AFTER snapshot sequence.
///
/// Snapshot.sequence is the last event baked into the snapshot.
/// We start loading from snapshot.sequence + 1 to avoid double-apply.
#[tokio::test]
async fn test_get_with_snapshot_starts_from_snapshot_sequence() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let repo = make_repo(event_store.clone(), snapshot_store.clone());

    let root = Uuid::new_v4();

    // Add events 0-4
    event_store
        .add(
            "orders",
            "test",
            root,
            (0..5).map(make_event_page).collect(),
            "",
            None,
            None,
        )
        .await
        .unwrap();

    // Add snapshot at sequence 3
    snapshot_store
        .put(
            "orders",
            "test",
            root,
            Snapshot {
                sequence: 3,
                state: None,
                retention: SnapshotRetention::RetentionDefault as i32,
                created_at: None,
            },
        )
        .await
        .unwrap();

    let book = repo.get("orders", "test", root).await.unwrap();

    // Should only have events AFTER snapshot (snapshot contains seq 3, so load from 4)
    assert_eq!(book.pages.len(), 1); // Only event 4
    assert!(book.snapshot.is_some());
    assert_eq!(book.snapshot.as_ref().unwrap().sequence, 3);
}

// ============================================================================
// Range Query Tests
// ============================================================================

/// get_from_to returns events in the specified range.
#[tokio::test]
async fn test_get_from_to_returns_range() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let repo = make_repo(event_store.clone(), snapshot_store);

    let root = Uuid::new_v4();

    event_store
        .add(
            "orders",
            "test",
            root,
            (0..10).map(make_event_page).collect(),
            "",
            None,
            None,
        )
        .await
        .unwrap();

    let book = repo
        .get_from_to("orders", "test", root, 3, 7)
        .await
        .unwrap();

    assert_eq!(book.pages.len(), 4); // Events 3, 4, 5, 6
    assert!(book.snapshot.is_none()); // Range query doesn't include snapshot
}

// ============================================================================
// Error Handling Tests
// ============================================================================

/// Put with missing cover returns error.
#[tokio::test]
async fn test_put_missing_cover_returns_error() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let repo = make_repo(event_store, snapshot_store);

    let book = EventBook {
        cover: None,
        pages: vec![],
        snapshot: None,
        ..Default::default()
    };

    let result = repo.put("test", &book, None, None).await;

    assert!(result.is_err());
}

/// Put with missing root UUID returns error.
#[tokio::test]
async fn test_put_missing_root_returns_error() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let repo = make_repo(event_store, snapshot_store);

    let book = EventBook {
        cover: Some(Cover {
            domain: "orders".to_string(),
            root: None,
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        pages: vec![],
        snapshot: None,
        ..Default::default()
    };

    let result = repo.put("test", &book, None, None).await;

    assert!(result.is_err());
}

/// Put with invalid UUID bytes returns error.
#[tokio::test]
async fn test_put_invalid_uuid_returns_error() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let repo = make_repo(event_store, snapshot_store);

    let book = EventBook {
        cover: Some(Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid {
                value: vec![1, 2, 3], // Invalid: not 16 bytes
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        pages: vec![],
        snapshot: None,
        ..Default::default()
    };

    let result = repo.put("test", &book, None, None).await;

    assert!(result.is_err());
}

/// Get propagates errors from underlying event store.
#[tokio::test]
async fn test_get_propagates_store_error() {
    let event_store = Arc::new(MockEventStore::new());
    event_store.set_fail_on_get(true).await;
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let repo = make_repo(event_store, snapshot_store);

    let result = repo.get("orders", "test", Uuid::new_v4()).await;

    assert!(result.is_err());
}

/// Put propagates errors from underlying event store.
#[tokio::test]
async fn test_put_propagates_store_error() {
    let event_store = Arc::new(MockEventStore::new());
    event_store.set_fail_on_add(true).await;
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let repo = make_repo(event_store, snapshot_store);

    let root = Uuid::new_v4();
    let book = make_event_book_with_root("orders", root, vec![]);

    let result = repo.put("test", &book, None, None).await;

    assert!(result.is_err());
}

// ============================================================================
// Snapshot Enable/Disable Tests
// ============================================================================

/// With snapshot reading disabled, all events are loaded from beginning.
///
/// Useful for: debugging, migration, snapshot regeneration after bug fix.
#[tokio::test]
async fn test_with_config_snapshot_read_disabled_ignores_snapshot() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let repo = make_repo_with_read_enabled(event_store.clone(), snapshot_store.clone(), false);

    let root = Uuid::new_v4();

    // Add events 0-4
    event_store
        .add(
            "orders",
            "test",
            root,
            (0..5).map(make_event_page).collect(),
            "",
            None,
            None,
        )
        .await
        .unwrap();

    // Add snapshot at sequence 3
    snapshot_store
        .put(
            "orders",
            "test",
            root,
            Snapshot {
                sequence: 3,
                state: None,
                retention: SnapshotRetention::RetentionDefault as i32,
                created_at: None,
            },
        )
        .await
        .unwrap();

    let book = repo.get("orders", "test", root).await.unwrap();

    // With snapshot reading disabled, should load ALL events from beginning
    assert_eq!(book.pages.len(), 5);
    assert!(book.snapshot.is_none());
}

/// With snapshot reading enabled, events are loaded after snapshot.
#[tokio::test]
async fn test_with_config_snapshot_read_enabled_uses_snapshot() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let repo = make_repo_with_read_enabled(event_store.clone(), snapshot_store.clone(), true);

    let root = Uuid::new_v4();

    // Add events 0-4
    event_store
        .add(
            "orders",
            "test",
            root,
            (0..5).map(make_event_page).collect(),
            "",
            None,
            None,
        )
        .await
        .unwrap();

    // Add snapshot at sequence 3
    snapshot_store
        .put(
            "orders",
            "test",
            root,
            Snapshot {
                sequence: 3,
                state: None,
                retention: SnapshotRetention::RetentionDefault as i32,
                created_at: None,
            },
        )
        .await
        .unwrap();

    let book = repo.get("orders", "test", root).await.unwrap();

    // With snapshot reading enabled, should load from snapshot sequence + 1
    assert_eq!(book.pages.len(), 1); // Only event 4 (snapshot contains through seq 3)
    assert!(book.snapshot.is_some());
    assert_eq!(book.snapshot.as_ref().unwrap().sequence, 3);
}

/// with_config(true) behaves identically to new().
#[tokio::test]
async fn test_with_config_defaults_match_new_constructor() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());

    // with_config(true) should behave the same as new()
    let repo_new = make_repo(event_store.clone(), snapshot_store.clone());
    let repo_config =
        make_repo_with_read_enabled(event_store.clone(), snapshot_store.clone(), true);

    let root = Uuid::new_v4();

    event_store
        .add(
            "orders",
            "test",
            root,
            (0..3).map(make_event_page).collect(),
            "",
            None,
            None,
        )
        .await
        .unwrap();

    snapshot_store
        .put(
            "orders",
            "test",
            root,
            Snapshot {
                sequence: 2,
                state: None,
                retention: SnapshotRetention::RetentionDefault as i32,
                created_at: None,
            },
        )
        .await
        .unwrap();

    let book_new = repo_new.get("orders", "test", root).await.unwrap();
    let book_config = repo_config.get("orders", "test", root).await.unwrap();

    assert_eq!(book_new.pages.len(), book_config.pages.len());
    assert_eq!(book_new.snapshot.is_some(), book_config.snapshot.is_some());
}

// ============================================================================
// Temporal and Sparse Query Tests
// ============================================================================

mod mock_integration {
    //! Tests for temporal queries and sparse sequence queries.
    //!
    //! Temporal queries skip snapshots because snapshot state may not
    //! correspond to the requested point in time. Full replay ensures
    //! correctness for "what was state at time X?" queries.

    use super::*;
    use crate::storage::mock::{MockEventStore, MockSnapshotStore};
    use prost_types::{Any, Timestamp};

    fn test_event(sequence: u32, event_type: &str) -> EventPage {
        EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(sequence)),
            }),
            created_at: Some(Timestamp {
                seconds: 1704067200 + sequence as i64,
                nanos: 0,
            }),
            payload: Some(event_page::Payload::Event(Any {
                type_url: format!("type.googleapis.com/{}", event_type),
                value: vec![1, 2, 3, sequence as u8],
            })),
            ..Default::default()
        }
    }

    fn test_snapshot(sequence: u32) -> Snapshot {
        Snapshot {
            sequence,
            state: Some(Any {
                type_url: "type.googleapis.com/TestState".to_string(),
                value: vec![10, 20, 30],
            }),
            retention: SnapshotRetention::RetentionDefault as i32,
            created_at: None,
        }
    }

    fn setup_shared() -> (
        EventBookRepository,
        Arc<MockEventStore>,
        Arc<MockSnapshotStore>,
    ) {
        let event_store = Arc::new(MockEventStore::new());
        let snapshot_store = Arc::new(MockSnapshotStore::new());
        let repo = make_repo(event_store.clone(), snapshot_store.clone());
        (repo, event_store, snapshot_store)
    }

    // Note: Basic CRUD tests (get empty, put/get roundtrip, snapshot loading, range queries)
    // are covered in the top-level tests module. This module focuses on:
    // - Temporal queries (by time, by sequence)
    // - Sparse sequence queries (get_sequences)

    fn test_snapshot_with_created_at(sequence: u32, created_at_secs: i64) -> Snapshot {
        Snapshot {
            sequence,
            state: Some(Any {
                type_url: "type.googleapis.com/TestState".to_string(),
                value: vec![10, 20, 30],
            }),
            retention: SnapshotRetention::RetentionDefault as i32,
            created_at: Some(Timestamp {
                seconds: created_at_secs,
                nanos: 0,
            }),
        }
    }

    /// R2-SNAP-7: temporal-by-time USES the snapshot when its
    /// `created_at` is before (or equal to) the target timestamp.
    /// Pre-fix: the snapshot was always ignored. The snapshot
    /// represents rolled-up state as of its `created_at`; layering
    /// post-snapshot events that themselves predate the target gives
    /// the same state as a full replay.
    #[tokio::test]
    async fn test_get_temporal_by_time_uses_snapshot_when_created_at_is_before_target() {
        let (repo, event_store, snapshot_store) = setup_shared();

        let domain = "test_domain";
        let root = Uuid::new_v4();

        // Events at 1-second intervals from 1704067200 (epoch).
        // seq=0 → 1704067200, seq=4 → 1704067204
        use crate::storage::EventStore;
        event_store
            .add(
                domain,
                "test",
                root,
                vec![
                    test_event(0, "Event0"),
                    test_event(1, "Event1"),
                    test_event(2, "Event2"),
                    test_event(3, "Event3"),
                    test_event(4, "Event4"),
                ],
                "",
                None,
                None,
            )
            .await
            .unwrap();

        // Snapshot at seq=2, persisted at 1704067202 (== event 2's time).
        // Target T = 1704067203 (== event 3's time). Snapshot is behind T.
        use crate::storage::SnapshotStore;
        snapshot_store
            .put(
                domain,
                "test",
                root,
                test_snapshot_with_created_at(2, 1704067202),
            )
            .await
            .unwrap();

        let book = repo
            .get_temporal_by_time(domain, "test", root, "2024-01-01T00:00:03+00:00")
            .await
            .unwrap();

        assert!(
            book.snapshot.is_some(),
            "snapshot.created_at <= target → must use snapshot"
        );
        assert_eq!(book.snapshot.as_ref().unwrap().sequence, 2);
        // Layered events: seq > 2 AND created_at <= target.
        // Event 3 at 1704067203 (== T, ✓), event 4 at 1704067204 (> T, ✗).
        assert_eq!(
            book.pages.len(),
            1,
            "only event 3 should layer; got {:?}",
            book.pages
                .iter()
                .map(|p| p.sequence_num())
                .collect::<Vec<_>>()
        );
        assert_eq!(book.pages[0].sequence_num(), 3);
    }

    /// R2-SNAP-7: snapshot AHEAD of target timestamp is ignored.
    #[tokio::test]
    async fn test_get_temporal_by_time_skips_snapshot_when_created_at_is_after_target() {
        let (repo, event_store, snapshot_store) = setup_shared();

        let domain = "test_domain";
        let root = Uuid::new_v4();

        use crate::storage::EventStore;
        event_store
            .add(
                domain,
                "test",
                root,
                vec![
                    test_event(0, "Event0"),
                    test_event(1, "Event1"),
                    test_event(2, "Event2"),
                ],
                "",
                None,
                None,
            )
            .await
            .unwrap();

        // Snapshot persisted at 1704067210 — way after target 1704067201.
        use crate::storage::SnapshotStore;
        snapshot_store
            .put(
                domain,
                "test",
                root,
                test_snapshot_with_created_at(2, 1704067210),
            )
            .await
            .unwrap();

        let book = repo
            .get_temporal_by_time(domain, "test", root, "2024-01-01T00:00:01+00:00")
            .await
            .unwrap();

        assert!(
            book.snapshot.is_none(),
            "snapshot.created_at > target → must be ignored to avoid future state"
        );
        // Full replay returns events 0, 1 (both with ts <= target).
        assert_eq!(book.pages.len(), 2);
    }

    /// R2-SNAP-7 safe-degradation: snapshots persisted before R2-SNAP-6
    /// landed have `created_at = None`. Reader must treat them as
    /// "unusable for temporal-by-time" and fall through to full replay,
    /// rather than risk using a snapshot whose temporal position is
    /// unknown.
    #[tokio::test]
    async fn test_get_temporal_by_time_skips_snapshot_when_created_at_is_unset() {
        let (repo, event_store, snapshot_store) = setup_shared();

        let domain = "test_domain";
        let root = Uuid::new_v4();

        // Events at 1-second intervals starting at 2024-01-01T00:00:00Z
        use crate::storage::EventStore;
        event_store
            .add(
                domain,
                "test",
                root,
                vec![
                    test_event(0, "Event0"),
                    test_event(1, "Event1"),
                    test_event(2, "Event2"),
                    test_event(3, "Event3"),
                    test_event(4, "Event4"),
                ],
                "",
                None,
                None,
            )
            .await
            .unwrap();

        // Snapshot at sequence 3 — should be ignored
        use crate::storage::SnapshotStore;
        snapshot_store
            .put(domain, "test", root, test_snapshot(3))
            .await
            .unwrap();

        // Query as-of 2 seconds after epoch (should return events 0, 1, 2)
        let book = repo
            .get_temporal_by_time(domain, "test", root, "2024-01-01T00:00:02+00:00")
            .await
            .unwrap();

        assert!(book.snapshot.is_none()); // No snapshot in temporal query
        assert_eq!(book.pages.len(), 3); // Events 0, 1, 2
    }

    /// R2-SNAP-5: temporal by-sequence USES the snapshot when it's
    /// behind (or at) the target sequence. The snapshot represents
    /// rolled-up state through `snapshot.sequence`; layering events
    /// `snapshot.sequence + 1 .. target + 1` on top gives the same
    /// state a full replay would.
    #[tokio::test]
    async fn test_get_temporal_by_sequence_uses_snapshot_when_behind_target() {
        let (repo, event_store, snapshot_store) = setup_shared();

        let domain = "test_domain";
        let root = Uuid::new_v4();

        use crate::storage::EventStore;
        event_store
            .add(
                domain,
                "test",
                root,
                vec![
                    test_event(0, "Event0"),
                    test_event(1, "Event1"),
                    test_event(2, "Event2"),
                    test_event(3, "Event3"),
                    test_event(4, "Event4"),
                ],
                "",
                None,
                None,
            )
            .await
            .unwrap();

        // Snapshot at sequence 1 — behind target sequence 3.
        use crate::storage::SnapshotStore;
        snapshot_store
            .put(domain, "test", root, test_snapshot(1))
            .await
            .unwrap();

        let book = repo
            .get_temporal_by_sequence(domain, "test", root, 3)
            .await
            .unwrap();

        assert!(
            book.snapshot.is_some(),
            "snapshot behind target must be used; pre-fix returned None"
        );
        assert_eq!(book.snapshot.as_ref().unwrap().sequence, 1);
        // Events layered on top: 2, 3 (from snapshot.sequence + 1 through target inclusive)
        assert_eq!(
            book.pages.len(),
            2,
            "events from snapshot.sequence+1 (=2) through target (=3): {:?}",
            book.pages
                .iter()
                .map(|p| p.sequence_num())
                .collect::<Vec<_>>()
        );
        assert_eq!(book.pages[0].sequence_num(), 2);
        assert_eq!(book.pages[1].sequence_num(), 3);
    }

    /// R2-SNAP-5 boundary: snapshot exactly at target → use it,
    /// layer zero events.
    #[tokio::test]
    async fn test_get_temporal_by_sequence_uses_snapshot_when_exactly_at_target() {
        let (repo, event_store, snapshot_store) = setup_shared();

        let domain = "test_domain";
        let root = Uuid::new_v4();

        use crate::storage::EventStore;
        event_store
            .add(
                domain,
                "test",
                root,
                vec![
                    test_event(0, "Event0"),
                    test_event(1, "Event1"),
                    test_event(2, "Event2"),
                ],
                "",
                None,
                None,
            )
            .await
            .unwrap();

        use crate::storage::SnapshotStore;
        snapshot_store
            .put(domain, "test", root, test_snapshot(2))
            .await
            .unwrap();

        let book = repo
            .get_temporal_by_sequence(domain, "test", root, 2)
            .await
            .unwrap();

        assert!(book.snapshot.is_some());
        assert_eq!(book.snapshot.as_ref().unwrap().sequence, 2);
        assert!(
            book.pages.is_empty(),
            "snapshot==target → no events layered; got {:?}",
            book.pages
                .iter()
                .map(|p| p.sequence_num())
                .collect::<Vec<_>>()
        );
    }

    /// R2-SNAP-5: snapshot AHEAD of target is ignored — full replay.
    /// (Same scenario as the legacy test below; kept as the explicit
    /// regression guard for the "snapshot newer than target" case.)
    #[tokio::test]
    async fn test_get_temporal_by_sequence_skips_snapshot_when_ahead_of_target() {
        let (repo, event_store, snapshot_store) = setup_shared();

        let domain = "test_domain";
        let root = Uuid::new_v4();

        use crate::storage::EventStore;
        event_store
            .add(
                domain,
                "test",
                root,
                vec![
                    test_event(0, "Event0"),
                    test_event(1, "Event1"),
                    test_event(2, "Event2"),
                    test_event(3, "Event3"),
                    test_event(4, "Event4"),
                ],
                "",
                None,
                None,
            )
            .await
            .unwrap();

        // Snapshot at sequence 3 — should be ignored
        use crate::storage::SnapshotStore;
        snapshot_store
            .put(domain, "test", root, test_snapshot(3))
            .await
            .unwrap();

        // Query as-of sequence 2 — should return events 0, 1, 2
        let book = repo
            .get_temporal_by_sequence(domain, "test", root, 2)
            .await
            .unwrap();

        assert!(book.snapshot.is_none());
        assert_eq!(book.pages.len(), 3);
        assert_eq!(book.pages[0].sequence_num(), 0);
        assert_eq!(book.pages[2].sequence_num(), 2);
    }

    /// Temporal query with sequence 0 returns only the first event.
    #[tokio::test]
    async fn test_get_temporal_by_sequence_zero() {
        let (repo, event_store, _) = setup_shared();

        let domain = "test_domain";
        let root = Uuid::new_v4();

        use crate::storage::EventStore;
        event_store
            .add(
                domain,
                "test",
                root,
                vec![test_event(0, "Event0"), test_event(1, "Event1")],
                "",
                None,
                None,
            )
            .await
            .unwrap();

        // Query as-of sequence 0 — should return only event 0
        let book = repo
            .get_temporal_by_sequence(domain, "test", root, 0)
            .await
            .unwrap();

        assert_eq!(book.pages.len(), 1);
        assert_eq!(book.pages[0].sequence_num(), 0);
    }

    // ============================================================================
    // get_sequences Tests (Sparse Queries)
    // ============================================================================

    /// Sparse sequence query returns only requested events.
    #[tokio::test]
    async fn test_get_sequences_sparse() {
        let (repo, event_store, _) = setup_shared();

        let domain = "test_domain";
        let root = Uuid::new_v4();

        use crate::storage::EventStore;
        event_store
            .add(
                domain,
                "test",
                root,
                vec![
                    test_event(0, "Event0"),
                    test_event(1, "Event1"),
                    test_event(2, "Event2"),
                    test_event(3, "Event3"),
                    test_event(4, "Event4"),
                ],
                "",
                None,
                None,
            )
            .await
            .unwrap();

        // Request sparse sequences
        let book = repo
            .get_sequences(domain, "test", root, &[1, 3])
            .await
            .unwrap();

        assert_eq!(book.pages.len(), 2);
        assert_eq!(book.pages[0].sequence_num(), 1);
        assert_eq!(book.pages[1].sequence_num(), 3);
    }

    /// Contiguous sequences are optimized to a range query.
    #[tokio::test]
    async fn test_get_sequences_contiguous_uses_range() {
        let (repo, event_store, _) = setup_shared();

        let domain = "test_domain";
        let root = Uuid::new_v4();

        use crate::storage::EventStore;
        event_store
            .add(
                domain,
                "test",
                root,
                vec![
                    test_event(0, "Event0"),
                    test_event(1, "Event1"),
                    test_event(2, "Event2"),
                    test_event(3, "Event3"),
                    test_event(4, "Event4"),
                ],
                "",
                None,
                None,
            )
            .await
            .unwrap();

        // Request contiguous sequences: 1, 2, 3 — optimized to range query
        let book = repo
            .get_sequences(domain, "test", root, &[1, 2, 3])
            .await
            .unwrap();

        assert_eq!(book.pages.len(), 3);
        assert_eq!(book.pages[0].sequence_num(), 1);
        assert_eq!(book.pages[1].sequence_num(), 2);
        assert_eq!(book.pages[2].sequence_num(), 3);
    }

    /// Empty sequence list returns empty EventBook.
    #[tokio::test]
    async fn test_get_sequences_empty_returns_empty() {
        let (repo, event_store, _) = setup_shared();

        let domain = "test_domain";
        let root = Uuid::new_v4();

        use crate::storage::EventStore;
        event_store
            .add(
                domain,
                "test",
                root,
                vec![test_event(0, "Event0"), test_event(1, "Event1")],
                "",
                None,
                None,
            )
            .await
            .unwrap();

        // Request empty sequence list
        let book = repo.get_sequences(domain, "test", root, &[]).await.unwrap();

        assert!(book.pages.is_empty());
    }

    /// Single sequence request works correctly.
    #[tokio::test]
    async fn test_get_sequences_single() {
        let (repo, event_store, _) = setup_shared();

        let domain = "test_domain";
        let root = Uuid::new_v4();

        use crate::storage::EventStore;
        event_store
            .add(
                domain,
                "test",
                root,
                vec![
                    test_event(0, "Event0"),
                    test_event(1, "Event1"),
                    test_event(2, "Event2"),
                ],
                "",
                None,
                None,
            )
            .await
            .unwrap();

        // Request single sequence
        let book = repo
            .get_sequences(domain, "test", root, &[2])
            .await
            .unwrap();

        assert_eq!(book.pages.len(), 1);
        assert_eq!(book.pages[0].sequence_num(), 2);
    }

    /// Non-existent sequences are filtered out (no error).
    #[tokio::test]
    async fn test_get_sequences_nonexistent_filtered_out() {
        let (repo, event_store, _) = setup_shared();

        let domain = "test_domain";
        let root = Uuid::new_v4();

        use crate::storage::EventStore;
        event_store
            .add(
                domain,
                "test",
                root,
                vec![test_event(0, "Event0"), test_event(1, "Event1")],
                "",
                None,
                None,
            )
            .await
            .unwrap();

        // Request sequences including ones that don't exist
        let book = repo
            .get_sequences(domain, "test", root, &[0, 5, 10])
            .await
            .unwrap();

        // Only sequence 0 exists
        assert_eq!(book.pages.len(), 1);
        assert_eq!(book.pages[0].sequence_num(), 0);
    }

    /// Results are ordered by sequence, regardless of request order.
    #[tokio::test]
    async fn test_get_sequences_preserves_order() {
        let (repo, event_store, _) = setup_shared();

        let domain = "test_domain";
        let root = Uuid::new_v4();

        use crate::storage::EventStore;
        event_store
            .add(
                domain,
                "test",
                root,
                vec![
                    test_event(0, "Event0"),
                    test_event(1, "Event1"),
                    test_event(2, "Event2"),
                    test_event(3, "Event3"),
                ],
                "",
                None,
                None,
            )
            .await
            .unwrap();

        // Request in different order — output should be in sequence order
        let book = repo
            .get_sequences(domain, "test", root, &[3, 1, 2])
            .await
            .unwrap();

        assert_eq!(book.pages.len(), 3);
        // Order depends on storage implementation — MockEventStore returns in sequence order
        assert_eq!(book.pages[0].sequence_num(), 1);
        assert_eq!(book.pages[1].sequence_num(), 2);
        assert_eq!(book.pages[2].sequence_num(), 3);
    }
}
