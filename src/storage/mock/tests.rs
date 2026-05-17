//! Tests for mock storage implementations.
//!
//! Mock stores are in-memory implementations for testing without real databases.
//! They implement the same traits (EventStore, SnapshotStore) as production
//! backends, making them suitable for unit and integration tests.
//!
//! Key behaviors verified:
//! - Event persistence and retrieval by domain/root
//! - Correlation ID queries (cross-domain event aggregation)
//! - Timestamp-based queries (as-of queries for temporal consistency)
//! - Snapshot storage and retrieval

use uuid::Uuid;

use crate::proto::{EventPage, PageHeader, Snapshot};
use crate::storage::{EventStore, SnapshotStore};

use super::*;

// ============================================================================
// MockEventStore Basic Operations
// ============================================================================

/// Events can be added and retrieved by domain/root.
///
/// This is the fundamental EventStore contract: add events, get them back.
/// Verifies the mock correctly stores and retrieves events.
#[tokio::test]
async fn test_mock_event_store_add_and_get() {
    let store = MockEventStore::new();
    let root = Uuid::new_v4();

    let events = vec![EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
            type_url: "test.Event".to_string(),
            value: vec![],
        })),
        created_at: None,
        ..Default::default()
    }];

    store
        .add("orders", "test", root, events, "corr-123", None, None)
        .await
        .unwrap();

    let retrieved = store.get("orders", "test", root).await.unwrap();
    assert_eq!(retrieved.len(), 1);
}

// ============================================================================
// Correlation ID Queries
// ============================================================================

/// Events can be queried by correlation ID across domains.
///
/// Correlation ID links events across domains in a saga/PM flow.
/// get_by_correlation returns EventBooks grouped by (domain, root),
/// enabling process managers to see all related events regardless of domain.
#[tokio::test]
async fn test_mock_event_store_get_by_correlation() {
    let store = MockEventStore::new();
    let root1 = Uuid::new_v4();
    let root2 = Uuid::new_v4();

    let event1 = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
            type_url: "orders.Created".to_string(),
            value: vec![],
        })),
        created_at: None,
        ..Default::default()
    };

    let event2 = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
            type_url: "payment.Confirmed".to_string(),
            value: vec![],
        })),
        created_at: None,
        ..Default::default()
    };

    // Add events with same correlation_id across different domains
    store
        .add("orders", "test", root1, vec![event1], "tx-abc", None, None)
        .await
        .unwrap();
    store
        .add("payment", "test", root2, vec![event2], "tx-abc", None, None)
        .await
        .unwrap();

    // Query by correlation_id
    let books = store.get_by_correlation("tx-abc").await.unwrap();
    assert_eq!(books.len(), 2);

    // Query with different correlation_id returns empty
    let empty = store.get_by_correlation("tx-xyz").await.unwrap();
    assert!(empty.is_empty());
}

// ============================================================================
// Timestamp-Based Queries (As-Of)
// ============================================================================

/// Events can be filtered by created_at timestamp (as-of queries).
///
/// Temporal queries enable:
/// - Point-in-time aggregate reconstruction ("what was state at X?")
/// - Debugging by replaying to a specific moment
/// - Regulatory requirements for historical state audits
#[tokio::test]
async fn test_get_until_timestamp_filters_by_created_at() {
    let store = MockEventStore::new();
    let root = Uuid::new_v4();

    let events = vec![
        EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
            }),
            payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
                type_url: "test.Event0".to_string(),
                value: vec![],
            })),
            created_at: Some(prost_types::Timestamp {
                seconds: 1704067200, // 2024-01-01T00:00:00Z
                nanos: 0,
            }),
            ..Default::default()
        },
        EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(1)),
            }),
            payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
                type_url: "test.Event1".to_string(),
                value: vec![],
            })),
            created_at: Some(prost_types::Timestamp {
                seconds: 1704153600, // 2024-01-02T00:00:00Z
                nanos: 0,
            }),
            ..Default::default()
        },
        EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(2)),
            }),
            payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
                type_url: "test.Event2".to_string(),
                value: vec![],
            })),
            created_at: Some(prost_types::Timestamp {
                seconds: 1704240000, // 2024-01-03T00:00:00Z
                nanos: 0,
            }),
            ..Default::default()
        },
    ];
    store
        .add("orders", "test", root, events, "", None, None)
        .await
        .unwrap();

    // Query as-of Jan 2 — should return events 0 and 1
    let result = store
        .get_until_timestamp("orders", "test", root, "2024-01-02T00:00:00Z")
        .await
        .unwrap();
    assert_eq!(result.len(), 2);

    // Query as-of Jan 1 — should return event 0 only
    let result = store
        .get_until_timestamp("orders", "test", root, "2024-01-01T00:00:00Z")
        .await
        .unwrap();
    assert_eq!(result.len(), 1);

    // Query before any events — should return empty
    let result = store
        .get_until_timestamp("orders", "test", root, "2023-12-31T00:00:00Z")
        .await
        .unwrap();
    assert!(result.is_empty());

    // Query after all events — should return all
    let result = store
        .get_until_timestamp("orders", "test", root, "2024-01-04T00:00:00Z")
        .await
        .unwrap();
    assert_eq!(result.len(), 3);
}

/// Events without timestamps are excluded from timestamp queries.
///
/// If an event lacks created_at, it cannot be compared to the cutoff.
/// Excluding them is safer than guessing—ensures query semantics are
/// clear and predictable.
#[tokio::test]
async fn test_get_until_timestamp_excludes_events_without_timestamp() {
    let store = MockEventStore::new();
    let root = Uuid::new_v4();

    let events = vec![EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
            type_url: "test.Event".to_string(),
            value: vec![],
        })),
        created_at: None,
        ..Default::default()
    }];
    store
        .add("orders", "test", root, events, "", None, None)
        .await
        .unwrap();

    let result = store
        .get_until_timestamp("orders", "test", root, "2024-01-02T00:00:00Z")
        .await
        .unwrap();
    assert!(result.is_empty());
}

/// Invalid timestamp format returns error, not empty results.
///
/// User typos should fail loudly rather than returning misleading
/// empty result sets.
#[tokio::test]
async fn test_get_until_timestamp_invalid_format() {
    let store = MockEventStore::new();
    let root = Uuid::new_v4();

    let result = store
        .get_until_timestamp("orders", "test", root, "not-a-timestamp")
        .await;
    assert!(result.is_err());
}

// ============================================================================
// MockSnapshotStore Tests
// ============================================================================

/// Snapshots can be stored and retrieved.
///
/// Snapshots optimize aggregate loading by avoiding full event replay.
/// The mock store verifies the SnapshotStore trait contract is satisfied.
#[tokio::test]
async fn test_mock_snapshot_store() {
    let store = MockSnapshotStore::new();
    let root = Uuid::new_v4();

    let snapshot = Snapshot {
        sequence: 5,
        state: None,
        retention: crate::proto::SnapshotRetention::RetentionDefault as i32,
    };

    store
        .put("orders", "test", root, snapshot.clone())
        .await
        .unwrap();

    let retrieved = store.get("orders", "test", root).await.unwrap();
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().sequence, 5);
}

// ============================================================================
// Sequence Override Tests
// ============================================================================

/// set_next_sequence overrides the next sequence number returned.
///
/// This allows tests to control sequence assignment for deterministic testing.
#[tokio::test]
async fn test_set_next_sequence_overrides_returned_value() {
    let store = MockEventStore::new();
    let root = Uuid::new_v4();

    // Without override, empty store returns 0
    let seq = store
        .get_next_sequence("orders", "angzarr", root)
        .await
        .unwrap();
    assert_eq!(seq, 0);

    // Set override to 42
    store.set_next_sequence(42).await;
    let seq = store
        .get_next_sequence("orders", "angzarr", root)
        .await
        .unwrap();
    assert_eq!(seq, 42, "Override should return configured value");

    // Clear override
    store.clear_next_sequence_override().await;
    let seq = store
        .get_next_sequence("orders", "angzarr", root)
        .await
        .unwrap();
    assert_eq!(seq, 0, "After clearing, should return computed value");
}

// ============================================================================
// Idempotency Tests
// ============================================================================

/// Duplicate add with same external_id returns Duplicate outcome.
///
/// Idempotency prevents double-processing when commands are retried.
#[tokio::test]
async fn test_add_idempotency_returns_duplicate() {
    let store = MockEventStore::new();
    let root = Uuid::new_v4();

    let event = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(5)),
        }),
        payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
            type_url: "test.Event".to_string(),
            value: vec![],
        })),
        created_at: None,
        ..Default::default()
    };

    // First add succeeds
    let result = store
        .add(
            "orders",
            "test",
            root,
            vec![event.clone()],
            "",
            Some("ext-123"),
            None,
        )
        .await
        .unwrap();
    assert!(matches!(result, crate::storage::AddOutcome::Added { .. }));

    // Second add with same external_id returns Duplicate
    let result = store
        .add(
            "orders",
            "test",
            root,
            vec![event],
            "",
            Some("ext-123"),
            None,
        )
        .await
        .unwrap();
    match result {
        crate::storage::AddOutcome::Duplicate {
            first_sequence,
            last_sequence,
        } => {
            assert_eq!(first_sequence, 5);
            assert_eq!(last_sequence, 5);
        }
        _ => panic!("Expected Duplicate outcome"),
    }
}

/// Empty external_id does not trigger idempotency check.
#[tokio::test]
async fn test_add_empty_external_id_no_idempotency() {
    let store = MockEventStore::new();
    let root = Uuid::new_v4();

    let event = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
            type_url: "test.Event".to_string(),
            value: vec![],
        })),
        created_at: None,
        ..Default::default()
    };

    // Add twice with empty external_id - both should succeed as Added
    let result1 = store
        .add("orders", "test", root, vec![event.clone()], "", None, None)
        .await
        .unwrap();
    let result2 = store
        .add("orders", "test", root, vec![event], "", None, None)
        .await
        .unwrap();

    assert!(matches!(result1, crate::storage::AddOutcome::Added { .. }));
    assert!(matches!(result2, crate::storage::AddOutcome::Added { .. }));
}

// ============================================================================
// get_next_sequence Tests
// ============================================================================

/// get_next_sequence returns max sequence + 1 for existing events.
#[tokio::test]
async fn test_get_next_sequence_increments_from_max() {
    let store = MockEventStore::new();
    let root = Uuid::new_v4();

    // Add events with sequences 0, 1, 2
    let events: Vec<EventPage> = (0..3)
        .map(|i| EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(i)),
            }),
            payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
                type_url: "test.Event".to_string(),
                value: vec![],
            })),
            created_at: None,
            ..Default::default()
        })
        .collect();

    store
        .add("orders", "angzarr", root, events, "", None, None)
        .await
        .unwrap();

    let next = store
        .get_next_sequence("orders", "angzarr", root)
        .await
        .unwrap();
    assert_eq!(next, 3, "Next sequence should be max(2) + 1 = 3");
}

/// get_next_sequence for non-default edition falls back to main timeline.
#[tokio::test]
async fn test_get_next_sequence_edition_fallback() {
    let store = MockEventStore::new();
    let root = Uuid::new_v4();

    // Add events to main timeline (angzarr edition)
    let events: Vec<EventPage> = (0..5)
        .map(|i| EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(i)),
            }),
            payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
                type_url: "test.Event".to_string(),
                value: vec![],
            })),
            created_at: None,
            ..Default::default()
        })
        .collect();

    store
        .add("orders", "angzarr", root, events, "", None, None)
        .await
        .unwrap();

    // Query next sequence for a new edition (should fall back to main timeline)
    let next = store
        .get_next_sequence("orders", "branch-1", root)
        .await
        .unwrap();
    assert_eq!(next, 5, "New edition should use main timeline's max + 1");
}

/// get_next_sequence for edition with events uses edition's max.
#[tokio::test]
async fn test_get_next_sequence_edition_with_events() {
    let store = MockEventStore::new();
    let root = Uuid::new_v4();

    // Add events to main timeline
    let main_events: Vec<EventPage> = (0..5)
        .map(|i| EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(i)),
            }),
            payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
                type_url: "test.Event".to_string(),
                value: vec![],
            })),
            created_at: None,
            ..Default::default()
        })
        .collect();

    store
        .add("orders", "angzarr", root, main_events, "", None, None)
        .await
        .unwrap();

    // Add events to branch edition (sequences 5, 6, 7)
    let branch_events: Vec<EventPage> = (5..8)
        .map(|i| EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(i)),
            }),
            payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
                type_url: "test.Event".to_string(),
                value: vec![],
            })),
            created_at: None,
            ..Default::default()
        })
        .collect();

    store
        .add("orders", "branch-1", root, branch_events, "", None, None)
        .await
        .unwrap();

    // Query next sequence for branch - should use branch's max (7) + 1
    let next = store
        .get_next_sequence("orders", "branch-1", root)
        .await
        .unwrap();
    assert_eq!(next, 8, "Edition with events should use its own max + 1");
}

// ============================================================================
// delete_edition_events Tests
// ============================================================================

/// delete_edition_events removes all events for domain/edition and returns count.
#[tokio::test]
async fn test_delete_edition_events_removes_and_counts() {
    let store = MockEventStore::new();
    let root1 = Uuid::new_v4();
    let root2 = Uuid::new_v4();

    // Add 3 events to root1
    let events1: Vec<EventPage> = (0..3)
        .map(|i| EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(i)),
            }),
            payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
                type_url: "test.Event".to_string(),
                value: vec![],
            })),
            created_at: None,
            ..Default::default()
        })
        .collect();

    // Add 2 events to root2
    let events2: Vec<EventPage> = (0..2)
        .map(|i| EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(i)),
            }),
            payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
                type_url: "test.Event".to_string(),
                value: vec![],
            })),
            created_at: None,
            ..Default::default()
        })
        .collect();

    store
        .add("orders", "branch-1", root1, events1, "", None, None)
        .await
        .unwrap();
    store
        .add("orders", "branch-1", root2, events2, "", None, None)
        .await
        .unwrap();

    // Delete edition events
    let count = store
        .delete_edition_events("orders", "branch-1")
        .await
        .unwrap();
    assert_eq!(count, 5, "Should return total deleted count (3 + 2)");

    // Verify events are gone
    let remaining1 = store.get("orders", "branch-1", root1).await.unwrap();
    let remaining2 = store.get("orders", "branch-1", root2).await.unwrap();
    assert!(remaining1.is_empty());
    assert!(remaining2.is_empty());
}

/// delete_edition_events only affects specified domain/edition.
#[tokio::test]
async fn test_delete_edition_events_scoped_correctly() {
    let store = MockEventStore::new();
    let root = Uuid::new_v4();

    let event = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
            type_url: "test.Event".to_string(),
            value: vec![],
        })),
        created_at: None,
        ..Default::default()
    };

    // Add to different domain/edition combinations
    store
        .add(
            "orders",
            "branch-1",
            root,
            vec![event.clone()],
            "",
            None,
            None,
        )
        .await
        .unwrap();
    store
        .add(
            "orders",
            "angzarr",
            root,
            vec![event.clone()],
            "",
            None,
            None,
        )
        .await
        .unwrap();
    store
        .add("inventory", "branch-1", root, vec![event], "", None, None)
        .await
        .unwrap();

    // Delete only orders/branch-1
    let count = store
        .delete_edition_events("orders", "branch-1")
        .await
        .unwrap();
    assert_eq!(count, 1);

    // Verify others unaffected
    let orders_main = store.get("orders", "angzarr", root).await.unwrap();
    let inventory = store.get("inventory", "branch-1", root).await.unwrap();
    assert_eq!(orders_main.len(), 1);
    assert_eq!(inventory.len(), 1);
}

// ============================================================================
// find_by_source Tests
// ============================================================================

/// find_by_source returns events matching source info.
#[tokio::test]
async fn test_find_by_source_returns_matching_events() {
    let store = MockEventStore::new();
    let root = Uuid::new_v4();
    let source_root = Uuid::new_v4();

    let source_info = crate::storage::SourceInfo {
        domain: "orders".to_string(),
        edition: "angzarr".to_string(),
        root: source_root,
        seq: 5,
    };

    let event = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
            type_url: "test.Event".to_string(),
            value: vec![],
        })),
        created_at: None,
        ..Default::default()
    };

    store
        .add(
            "inventory",
            "angzarr",
            root,
            vec![event],
            "",
            None,
            Some(&source_info),
        )
        .await
        .unwrap();

    // Find by matching source
    let result = store
        .find_by_source("inventory", "angzarr", root, &source_info)
        .await
        .unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().len(), 1);
}

/// find_by_source returns None for non-matching source.
#[tokio::test]
async fn test_find_by_source_returns_none_for_mismatch() {
    let store = MockEventStore::new();
    let root = Uuid::new_v4();
    let source_root = Uuid::new_v4();

    let source_info = crate::storage::SourceInfo {
        domain: "orders".to_string(),
        edition: "angzarr".to_string(),
        root: source_root,
        seq: 5,
    };

    let event = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
            type_url: "test.Event".to_string(),
            value: vec![],
        })),
        created_at: None,
        ..Default::default()
    };

    store
        .add(
            "inventory",
            "angzarr",
            root,
            vec![event],
            "",
            None,
            Some(&source_info),
        )
        .await
        .unwrap();

    // Query with different source info
    let different_source = crate::storage::SourceInfo {
        domain: "orders".to_string(),
        edition: "angzarr".to_string(),
        root: source_root,
        seq: 99, // Different sequence
    };

    let result = store
        .find_by_source("inventory", "angzarr", root, &different_source)
        .await
        .unwrap();
    assert!(result.is_none());
}

/// find_by_source returns None for empty source info.
#[tokio::test]
async fn test_find_by_source_empty_source_returns_none() {
    let store = MockEventStore::new();
    let root = Uuid::new_v4();

    let empty_source = crate::storage::SourceInfo {
        domain: String::new(),
        edition: String::new(),
        root: Uuid::nil(),
        seq: 0,
    };

    let result = store
        .find_by_source("inventory", "angzarr", root, &empty_source)
        .await
        .unwrap();
    assert!(result.is_none());
}

/// find_by_source checks all source fields (domain, edition, root, seq).
#[tokio::test]
async fn test_find_by_source_checks_all_fields() {
    let store = MockEventStore::new();
    let root = Uuid::new_v4();
    let source_root = Uuid::new_v4();

    let source_info = crate::storage::SourceInfo {
        domain: "orders".to_string(),
        edition: "angzarr".to_string(),
        root: source_root,
        seq: 5,
    };

    let event = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
            type_url: "test.Event".to_string(),
            value: vec![],
        })),
        created_at: None,
        ..Default::default()
    };

    store
        .add(
            "inventory",
            "angzarr",
            root,
            vec![event],
            "",
            None,
            Some(&source_info),
        )
        .await
        .unwrap();

    // Wrong domain
    let wrong_domain = crate::storage::SourceInfo {
        domain: "WRONG".to_string(),
        edition: "angzarr".to_string(),
        root: source_root,
        seq: 5,
    };
    assert!(store
        .find_by_source("inventory", "angzarr", root, &wrong_domain)
        .await
        .unwrap()
        .is_none());

    // Wrong edition
    let wrong_edition = crate::storage::SourceInfo {
        domain: "orders".to_string(),
        edition: "WRONG".to_string(),
        root: source_root,
        seq: 5,
    };
    assert!(store
        .find_by_source("inventory", "angzarr", root, &wrong_edition)
        .await
        .unwrap()
        .is_none());

    // Wrong root
    let wrong_root = crate::storage::SourceInfo {
        domain: "orders".to_string(),
        edition: "angzarr".to_string(),
        root: Uuid::new_v4(),
        seq: 5,
    };
    assert!(store
        .find_by_source("inventory", "angzarr", root, &wrong_root)
        .await
        .unwrap()
        .is_none());

    // Wrong seq
    let wrong_seq = crate::storage::SourceInfo {
        domain: "orders".to_string(),
        edition: "angzarr".to_string(),
        root: source_root,
        seq: 999,
    };
    assert!(store
        .find_by_source("inventory", "angzarr", root, &wrong_seq)
        .await
        .unwrap()
        .is_none());

    // Correct - should find
    assert!(store
        .find_by_source("inventory", "angzarr", root, &source_info)
        .await
        .unwrap()
        .is_some());
}

// ============================================================================
// query_stale_cascades Tests
// ============================================================================

/// query_stale_cascades uses strict less-than for timestamp comparison.
///
/// Events created exactly at the threshold should NOT be considered stale.
#[tokio::test]
async fn test_query_stale_cascades_timestamp_boundary() {
    let store = MockEventStore::new();
    let root = Uuid::new_v4();

    // Create event exactly at threshold timestamp
    let threshold_ts = prost_types::Timestamp {
        seconds: 1704067200, // 2024-01-01T00:00:00Z
        nanos: 0,
    };

    let event = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
            type_url: "test.Event".to_string(),
            value: vec![],
        })),
        created_at: Some(threshold_ts),
        no_commit: true,
        cascade_id: Some("cascade-boundary".to_string()),
    };

    store
        .add("orders", "angzarr", root, vec![event], "", None, None)
        .await
        .unwrap();

    // Query with same timestamp as event - event should NOT be stale (< not <=)
    let stale = store
        .query_stale_cascades("2024-01-01T00:00:00Z")
        .await
        .unwrap();
    assert!(
        stale.is_empty(),
        "Event at threshold should not be stale (uses < not <=)"
    );

    // Query with later timestamp - event should be stale
    let stale = store
        .query_stale_cascades("2024-01-01T00:00:01Z")
        .await
        .unwrap();
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0], "cascade-boundary");
}
