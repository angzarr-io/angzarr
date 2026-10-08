//! Tests for the EventQueryService gRPC service.
//!
//! EventQueryService provides read access to aggregate event histories for:
//! - Debugging (inspect event stream)
//! - Analytics (query across aggregates)
//! - Process manager state reconstruction (query by correlation_id)
//! - Temporal queries (as-of-time, as-of-sequence)
//!
//! Key behaviors:
//! - Query by domain+root returns full event history
//! - Query by correlation_id returns events across aggregates in same workflow
//! - Range/sequence selection enables partial history retrieval
//! - Temporal queries support point-in-time views
//! - Missing/invalid parameters return InvalidArgument gRPC status
//!
//! A whole-history query returns every event with the aggregate's latest
//! snapshot alongside (C-0243); the snapshot never replaces the events it
//! summarizes.

use super::*;
use crate::proto::{event_page, page_header, EventPage, PageHeader, SequenceRange, TemporalQuery};
use crate::proto_ext::EventPageExt;
use crate::storage::mock::{MockEventStore, MockSnapshotStore};
use crate::storage::AddMeta;
use prost_types::{Any, Timestamp};
use tokio_stream::StreamExt;

// ============================================================================
// Test Setup
// ============================================================================

/// A snapshot repository that reads nothing.
fn no_snapshots() -> crate::repository::SnapshotRepository {
    crate::repository::SnapshotRepository::with_flags(
        Arc::new(MockSnapshotStore::new()),
        false,
        false,
    )
}

fn create_test_service_with_mocks(
    event_store: Arc<MockEventStore>,
    snapshot_store: Arc<MockSnapshotStore>,
) -> EventQueryService {
    EventQueryService::new(event_store, snapshot_store)
}

fn create_default_test_service() -> (
    EventQueryService,
    Arc<MockEventStore>,
    Arc<MockSnapshotStore>,
) {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());

    let service = create_test_service_with_mocks(event_store.clone(), snapshot_store.clone());

    (service, event_store, snapshot_store)
}

// ============================================================================
// get_event_book Tests - Unary Query
// ============================================================================

/// Empty aggregate returns empty pages, not error.
///
/// Aggregates may not exist yet (pre-creation query) or may have had all
/// events compacted. Both cases should return successfully with no events.
#[tokio::test]
async fn test_get_event_book_empty_aggregate() {
    let (service, _, _) = create_default_test_service();
    let root = uuid::Uuid::new_v4();

    let query = Query {
        cover: Some(crate::proto::Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        selection: None,
    };

    let response = service.get_event_book(Request::new(query)).await;

    assert!(response.is_ok());
    let book = response.unwrap().into_inner();
    assert!(book.pages.is_empty());
}

/// Event data returned when aggregate has events.
#[tokio::test]
async fn test_get_event_book_with_data() {
    let (service, event_store, _) = create_default_test_service();
    let root = uuid::Uuid::new_v4();

    let events = vec![EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: "test.Event".to_string(),
            value: vec![],
        })),
        created_at: None,
    }];
    event_store
        .add(
            "orders",
            "",
            root,
            events,
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();

    let query = Query {
        cover: Some(crate::proto::Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        selection: None,
    };

    let response = service.get_event_book(Request::new(query)).await;

    assert!(response.is_ok());
    let book = response.unwrap().into_inner();
    assert_eq!(book.pages.len(), 1);
}

// ============================================================================
// Input Validation Tests
// ============================================================================

/// Missing root returns InvalidArgument — can't locate aggregate.
#[tokio::test]
async fn test_get_event_book_missing_root() {
    let (service, _, _) = create_default_test_service();

    let query = Query {
        cover: Some(crate::proto::Cover {
            domain: "orders".to_string(),
            root: None,
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        selection: None,
    };

    let response = service.get_event_book(Request::new(query)).await;

    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

/// Invalid UUID returns InvalidArgument — malformed identifier.
#[tokio::test]
async fn test_get_event_book_invalid_uuid() {
    let (service, _, _) = create_default_test_service();

    let query = Query {
        cover: Some(crate::proto::Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid {
                value: vec![1, 2, 3], // Invalid UUID
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        selection: None,
    };

    let response = service.get_event_book(Request::new(query)).await;

    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

// ============================================================================
// Range Selection Tests
// ============================================================================

/// Range selection returns events within inclusive bounds.
///
/// Enables efficient partial history retrieval for large aggregates.
#[tokio::test]
async fn test_get_event_book_with_range() {
    let (service, event_store, _) = create_default_test_service();
    let root = uuid::Uuid::new_v4();

    // Add multiple events
    for i in 0..5 {
        let events = vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(i)),
            }),
            payload: Some(event_page::Payload::Event(Any {
                type_url: format!("test.Event{}", i),
                value: vec![],
            })),
            created_at: None,
        }];
        event_store
            .add(
                "orders",
                "",
                root,
                events,
                &AddMeta {
                    correlation_id: "",
                    external_id: None,
                    source_info: None,
                    ext: None,
                },
            )
            .await
            .unwrap();
    }

    // Query for range [2, 4] - inclusive bounds, should return events 2, 3, 4
    let query = Query {
        cover: Some(crate::proto::Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        selection: Some(Selection::Range(SequenceRange {
            lower: 2,
            upper: Some(4),
        })),
    };

    let response = service.get_event_book(Request::new(query)).await;

    assert!(response.is_ok());
    let book = response.unwrap().into_inner();
    assert_eq!(book.pages.len(), 3); // Events 2, 3, 4 (inclusive upper bound)
}

// ============================================================================
// get_events Tests - Streaming Query
// ============================================================================

/// Streaming API returns single empty book for empty aggregate.
#[tokio::test]
async fn test_get_events_empty_aggregate() {
    let (service, _, _) = create_default_test_service();
    let root = uuid::Uuid::new_v4();

    let query = Query {
        cover: Some(crate::proto::Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        selection: None,
    };

    let response = service.get_events(Request::new(query)).await;

    assert!(response.is_ok());
    let mut stream = response.unwrap().into_inner();
    let first = stream.next().await;
    assert!(first.is_some());
    let book = first.unwrap().unwrap();
    assert!(book.pages.is_empty());
}

/// Streaming API returns event books.
#[tokio::test]
async fn test_get_events_with_data() {
    let (service, event_store, _) = create_default_test_service();
    let root = uuid::Uuid::new_v4();

    // First add some events via the store directly
    let events = vec![EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: "test.Event".to_string(),
            value: vec![],
        })),
        created_at: None,
    }];
    event_store
        .add(
            "orders",
            "",
            root,
            events,
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();

    let query = Query {
        cover: Some(crate::proto::Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        selection: None,
    };

    let response = service.get_events(Request::new(query)).await;

    assert!(response.is_ok());
    let mut stream = response.unwrap().into_inner();
    let first = stream.next().await;
    assert!(first.is_some());
    let book = first.unwrap().unwrap();
    assert_eq!(book.pages.len(), 1);
}

/// Streaming API validates inputs same as unary.
#[tokio::test]
async fn test_get_events_missing_root() {
    let (service, _, _) = create_default_test_service();

    let query = Query {
        cover: Some(crate::proto::Cover {
            domain: "orders".to_string(),
            root: None,
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        selection: None,
    };

    let response = service.get_events(Request::new(query)).await;

    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_get_events_invalid_uuid() {
    let (service, _, _) = create_default_test_service();

    let query = Query {
        cover: Some(crate::proto::Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid {
                value: vec![1, 2, 3], // Invalid: must be 16 bytes
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        selection: None,
    };

    let response = service.get_events(Request::new(query)).await;

    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

// ============================================================================
// get_aggregate_roots Tests - Discovery
// ============================================================================

/// Empty system returns no aggregate roots.
#[tokio::test]
async fn test_get_aggregate_roots_empty() {
    let (service, _, _) = create_default_test_service();

    let response = service.get_aggregate_roots(Request::new(())).await;

    assert!(response.is_ok());
    let mut stream = response.unwrap().into_inner();
    let first = stream.next().await;
    assert!(first.is_none()); // No aggregates yet
}

/// Returns all aggregate roots for debugging/analytics.
#[tokio::test]
async fn test_get_aggregate_roots_with_data() {
    let (service, event_store, _) = create_default_test_service();
    let root1 = uuid::Uuid::new_v4();
    let root2 = uuid::Uuid::new_v4();

    // Add some events - must have at least one event to create an aggregate root
    let event = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(0)),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: "test.Event".to_string(),
            value: vec![],
        })),
        created_at: None,
    };
    event_store
        .add(
            "orders",
            "",
            root1,
            vec![event.clone()],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();
    event_store
        .add(
            "orders",
            "",
            root2,
            vec![event],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();

    let response = service.get_aggregate_roots(Request::new(())).await;

    assert!(response.is_ok());
    let stream = response.unwrap().into_inner();
    let roots: Vec<_> = stream.collect().await;
    assert_eq!(roots.len(), 2);
}

/// Returns roots across multiple domains.
#[tokio::test]
async fn test_get_aggregate_roots_multiple_domains() {
    let (service, event_store, _) = create_default_test_service();

    // Must add at least one event to create an aggregate root
    let event = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(0)),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: "test.Event".to_string(),
            value: vec![],
        })),
        created_at: None,
    };
    event_store
        .add(
            "orders",
            "",
            uuid::Uuid::new_v4(),
            vec![event.clone()],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();
    event_store
        .add(
            "inventory",
            "",
            uuid::Uuid::new_v4(),
            vec![event],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();

    let response = service.get_aggregate_roots(Request::new(())).await;

    assert!(response.is_ok());
    let stream = response.unwrap().into_inner();
    let roots: Vec<_> = stream.collect().await;
    assert_eq!(roots.len(), 2);
}

// ============================================================================
// Correlation ID Query Tests
// ============================================================================

/// Query by correlation_id returns events across aggregates in workflow.
///
/// Process managers use correlation_id to track cross-domain workflows.
/// This enables debugging and state reconstruction for PM flows.
#[tokio::test]
async fn test_get_event_book_by_correlation_id() {
    let (service, event_store, _) = create_default_test_service();
    let root = uuid::Uuid::new_v4();
    let correlation_id = "corr-123";

    // Add events with correlation ID
    let events = vec![EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: "test.Event".to_string(),
            value: vec![],
        })),
        created_at: None,
    }];
    event_store
        .add(
            "orders",
            "",
            root,
            events,
            &AddMeta {
                correlation_id,
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();

    // Query by correlation ID (no root needed)
    let query = Query {
        cover: Some(crate::proto::Cover {
            domain: String::new(),
            root: None,
            correlation_id: correlation_id.to_string(),
            edition: None,
            ext: None,
        }),
        selection: None,
    };

    let response = service.get_event_book(Request::new(query)).await;

    assert!(response.is_ok());
    let book = response.unwrap().into_inner();
    assert_eq!(book.pages.len(), 1);
}

/// Non-existent correlation_id returns empty (not error).
#[tokio::test]
async fn test_get_event_book_by_correlation_id_not_found() {
    let (service, _, _) = create_default_test_service();

    let query = Query {
        cover: Some(crate::proto::Cover {
            domain: String::new(),
            root: None,
            correlation_id: "nonexistent".to_string(),
            edition: None,
            ext: None,
        }),
        selection: None,
    };

    let response = service.get_event_book(Request::new(query)).await;

    assert!(response.is_ok());
    let book = response.unwrap().into_inner();
    assert!(book.pages.is_empty());
}

/// Multiple aggregates with same correlation_id all returned.
///
/// Workflows span domains — order, inventory, fulfillment may all share
/// the same correlation_id. Query returns events from all participating aggregates.
#[tokio::test]
async fn test_get_events_by_correlation_id_multiple_aggregates() {
    let (service, event_store, _) = create_default_test_service();
    let correlation_id = "corr-multi";

    // Add events to multiple aggregates with same correlation ID
    for (domain, root) in [
        ("orders", uuid::Uuid::new_v4()),
        ("inventory", uuid::Uuid::new_v4()),
    ] {
        let events = vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
            }),
            payload: Some(event_page::Payload::Event(Any {
                type_url: format!("{}.Event", domain),
                value: vec![],
            })),
            created_at: None,
        }];
        event_store
            .add(
                domain,
                "",
                root,
                events,
                &AddMeta {
                    correlation_id,
                    external_id: None,
                    source_info: None,
                    ext: None,
                },
            )
            .await
            .unwrap();
    }

    // Query by correlation ID - should return both
    let query = Query {
        cover: Some(crate::proto::Cover {
            domain: String::new(),
            root: None,
            correlation_id: correlation_id.to_string(),
            edition: None,
            ext: None,
        }),
        selection: None,
    };

    let response = service.get_events(Request::new(query)).await;

    assert!(response.is_ok());
    let stream = response.unwrap().into_inner();
    let books: Vec<_> = stream.collect().await;
    assert_eq!(books.len(), 2);
}

// ============================================================================
// Temporal Query Tests
// ============================================================================

/// as_of_time returns events up to specified timestamp.
///
/// Enables point-in-time debugging: "what did this aggregate look like
/// at 2pm yesterday?" Essential for incident investigation.
#[tokio::test]
async fn test_get_event_book_temporal_by_time() {
    let (service, event_store, _) = create_default_test_service();
    let root = uuid::Uuid::new_v4();

    let events = vec![
        EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
            }),
            payload: Some(event_page::Payload::Event(Any {
                type_url: "test.Event0".to_string(),
                value: vec![],
            })),
            created_at: Some(Timestamp {
                seconds: 1704067200, // 2024-01-01T00:00:00Z
                nanos: 0,
            }),
        },
        EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(1)),
            }),
            payload: Some(event_page::Payload::Event(Any {
                type_url: "test.Event1".to_string(),
                value: vec![],
            })),
            created_at: Some(Timestamp {
                seconds: 1704153600, // 2024-01-02T00:00:00Z
                nanos: 0,
            }),
        },
        EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(2)),
            }),
            payload: Some(event_page::Payload::Event(Any {
                type_url: "test.Event2".to_string(),
                value: vec![],
            })),
            created_at: Some(Timestamp {
                seconds: 1704240000, // 2024-01-03T00:00:00Z
                nanos: 0,
            }),
        },
    ];
    event_store
        .add(
            "orders",
            "",
            root,
            events,
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();

    // Query as-of Jan 2
    let query = Query {
        cover: Some(crate::proto::Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        selection: Some(Selection::Temporal(TemporalQuery {
            point_in_time: Some(PointInTime::AsOfTime(Timestamp {
                seconds: 1704153600, // 2024-01-02T00:00:00Z
                nanos: 0,
            })),
        })),
    };

    let response = service.get_event_book(Request::new(query)).await;

    assert!(response.is_ok());
    let book = response.unwrap().into_inner();
    assert_eq!(book.pages.len(), 2); // Events 0 and 1
    assert!(book.snapshot.is_none());
}

/// as_of_sequence returns events up to specified sequence.
///
/// More precise than time-based queries — sequence is monotonic and
/// unambiguous. Used when you know the exact event version to inspect.
#[tokio::test]
async fn test_get_event_book_temporal_by_sequence() {
    let (service, event_store, _) = create_default_test_service();
    let root = uuid::Uuid::new_v4();

    for i in 0..5 {
        let events = vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(i)),
            }),
            payload: Some(event_page::Payload::Event(Any {
                type_url: format!("test.Event{}", i),
                value: vec![],
            })),
            created_at: None,
        }];
        event_store
            .add(
                "orders",
                "",
                root,
                events,
                &AddMeta {
                    correlation_id: "",
                    external_id: None,
                    source_info: None,
                    ext: None,
                },
            )
            .await
            .unwrap();
    }

    // Query as-of sequence 2 — should return events 0, 1, 2
    let query = Query {
        cover: Some(crate::proto::Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        selection: Some(Selection::Temporal(TemporalQuery {
            point_in_time: Some(PointInTime::AsOfSequence(2)),
        })),
    };

    let response = service.get_event_book(Request::new(query)).await;

    assert!(response.is_ok());
    let book = response.unwrap().into_inner();
    assert_eq!(book.pages.len(), 3);
    assert!(book.snapshot.is_none());
}

/// Empty temporal query (no point_in_time) returns InvalidArgument.
#[tokio::test]
async fn test_get_event_book_temporal_empty_point_in_time() {
    let (service, _, _) = create_default_test_service();
    let root = uuid::Uuid::new_v4();

    let query = Query {
        cover: Some(crate::proto::Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        selection: Some(Selection::Temporal(TemporalQuery {
            point_in_time: None,
        })),
    };

    let response = service.get_event_book(Request::new(query)).await;

    assert!(response.is_err());
    assert_eq!(response.unwrap_err().code(), tonic::Code::InvalidArgument);
}

// ============================================================================
// Snapshot Handling Tests
// ============================================================================

/// A whole-history query returns every event, with the aggregate's latest
/// snapshot alongside (C-0243): the snapshot never replaces the events it
/// summarizes.
#[tokio::test]
async fn test_get_event_book_returns_all_events_despite_snapshot() {
    let (service, event_store, snapshot_store) = create_default_test_service();
    let root = uuid::Uuid::new_v4();

    // Add an event at sequence 0
    let events = vec![EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: "test.CustomerCreated".to_string(),
            value: vec![],
        })),
        created_at: None,
    }];
    event_store
        .add(
            "customer",
            "",
            root,
            events,
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();

    // Store a snapshot at sequence 0 (as the aggregate coordinator would)
    let snapshot = crate::proto::Snapshot {
        sequence: 0,
        state: Some(Any {
            type_url: "test.CustomerState".to_string(),
            value: vec![1, 2, 3],
        }),
        retention: crate::proto::SnapshotRetention::RetentionDefault as i32,
        created_at: None,
    };
    snapshot_store
        .put("customer", "", root, snapshot)
        .await
        .unwrap();

    // Query should return the event despite snapshot existing at same sequence
    let query = Query {
        cover: Some(crate::proto::Cover {
            domain: "customer".to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        selection: None,
    };

    let response = service.get_event_book(Request::new(query)).await;

    assert!(response.is_ok());
    let book = response.unwrap().into_inner();
    assert_eq!(
        book.pages.len(),
        1,
        "EventQuery must return all events regardless of snapshots"
    );
    assert_eq!(
        book.snapshot.as_ref().map(|s| s.sequence),
        Some(0),
        "the latest snapshot is surfaced alongside the history"
    );
}

// ============================================================================
// Selection::Sequences Tests
// ============================================================================

/// Verify that Selection::Sequences returns only the requested event sequences.
///
/// Projectors and sagas sometimes need specific events rather than a range or
/// full history. The Sequences selection type enables fetching a sparse set of
/// events by their exact sequence numbers.
#[tokio::test]
async fn test_get_event_book_with_sequences() {
    let (service, event_store, _) = create_default_test_service();
    let root = uuid::Uuid::new_v4();

    for i in 0..5 {
        let events = vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(i)),
            }),
            payload: Some(event_page::Payload::Event(Any {
                type_url: format!("test.Event{}", i),
                value: vec![],
            })),
            created_at: None,
        }];
        event_store
            .add(
                "orders",
                "",
                root,
                events,
                &AddMeta {
                    correlation_id: "",
                    external_id: None,
                    source_info: None,
                    ext: None,
                },
            )
            .await
            .unwrap();
    }

    let query = Query {
        cover: Some(crate::proto::Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        selection: Some(Selection::Sequences(crate::proto::SequenceSet {
            values: vec![1, 3],
        })),
    };

    let response = service.get_event_book(Request::new(query)).await;

    assert!(response.is_ok());
    let book = response.unwrap().into_inner();
    assert_eq!(
        book.pages.len(),
        2,
        "Should return exactly sequences 1 and 3"
    );
}

// ============================================================================
// Missing Cover Validation Tests
// ============================================================================

/// Verify that get_event_book rejects queries without a cover when no
/// correlation_id is provided.
///
/// The cover contains domain and root_id which identify the aggregate.
/// Without either a cover or correlation_id, we cannot locate events.
#[tokio::test]
async fn test_get_event_book_missing_cover() {
    let (service, _, _) = create_default_test_service();

    let query = Query {
        cover: None,
        selection: None,
    };

    let response = service.get_event_book(Request::new(query)).await;

    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

/// Verify that get_events (streaming) also rejects queries without a cover.
///
/// Same validation as get_event_book — both endpoints need either a cover
/// with domain/root or a correlation_id to locate events.
#[tokio::test]
async fn test_get_events_missing_cover() {
    let (service, _, _) = create_default_test_service();

    let query = Query {
        cover: None,
        selection: None,
    };

    let response = service.get_events(Request::new(query)).await;

    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

// ============================================================================
// dispatch_selection / range-bound parity tests (H-35, H-36)
// ============================================================================
//
// `get_event_book` (unary) and `synchronize` (bidi-stream) used to dispatch
// `query.selection` in two divergent copies of the same `match` block. The two
// copies disagreed on:
//   * H-35: the "TemporalQuery missing point_in_time" error message — one path
//     returned the literal path `"crate::services::errmsg::TEMPORAL_QUERY_..."`
//     instead of the constant's value, so clients saw a Rust module path.
//   * H-36: the `SequenceRange.upper` bound — `get_event_book` correctly
//     treated it as inclusive (matching `test_get_event_book_with_range`'s
//     "[2, 4] returns events 2, 3, 4" contract), while `synchronize` treated
//     it as exclusive, so the same proto Query yielded different event sets
//     through the two methods.
//
// These tests pin the unified `dispatch_selection` helper. By construction the
// two service entry points now share this helper, so parity follows from a
// single source of truth.

/// H-35: the missing-point-in-time error returns the constant's VALUE
/// (`"TemporalQuery must specify ..."`), not the constant's Rust path. The
/// path leaked because the literal string `"crate::services::errmsg::..."`
/// was passed instead of the `const`.
#[tokio::test]
async fn test_dispatch_selection_temporal_missing_point_returns_descriptive_message() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let repo = crate::repository::EventBookRepository::new(
        event_store,
        std::sync::Arc::new(crate::repository::SnapshotRepository::with_flags(
            snapshot_store,
            false,
            false,
        )),
    );

    let result = super::dispatch_selection(
        &repo,
        &no_snapshots(),
        "orders",
        "",
        uuid::Uuid::new_v4(),
        Some(Selection::Temporal(TemporalQuery {
            point_in_time: None,
        })),
    )
    .await;

    let status = result.expect_err("missing point_in_time must be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    let msg = status.message();
    assert!(
        msg.contains("TemporalQuery must specify"),
        "error message must describe the contract, got: {msg:?}"
    );
    assert!(
        !msg.contains("crate::services::errmsg"),
        "error message must not leak the Rust module path of the constant, got: {msg:?}"
    );
    // Pin equality to the canonical constant so the message can't drift.
    assert_eq!(msg, crate::services::errmsg::TEMPORAL_QUERY_MISSING_POINT);
}

/// H-36: `SequenceRange.upper` is INCLUSIVE per the proto contract (see
/// `test_get_event_book_with_range`). The helper must convert
/// inclusive→exclusive internally so storage's `[from, to)` half-open range
/// returns the documented events. Range [2, 4] → 3 events (2, 3, 4).
#[tokio::test]
async fn test_dispatch_selection_range_upper_is_inclusive() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let root = uuid::Uuid::new_v4();

    for i in 0..5u32 {
        let events = vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(i)),
            }),
            payload: Some(event_page::Payload::Event(Any {
                type_url: format!("test.Event{i}"),
                value: vec![],
            })),
            created_at: None,
        }];
        event_store
            .add(
                "orders",
                "",
                root,
                events,
                &AddMeta {
                    correlation_id: "",
                    external_id: None,
                    source_info: None,
                    ext: None,
                },
            )
            .await
            .unwrap();
    }

    let repo = crate::repository::EventBookRepository::new(
        event_store,
        std::sync::Arc::new(crate::repository::SnapshotRepository::with_flags(
            snapshot_store,
            false,
            false,
        )),
    );

    let book = super::dispatch_selection(
        &repo,
        &no_snapshots(),
        "orders",
        "",
        root,
        Some(Selection::Range(SequenceRange {
            lower: 2,
            upper: Some(4),
        })),
    )
    .await
    .expect("range query must succeed");

    assert_eq!(
        book.pages.len(),
        3,
        "Range [2, 4] is inclusive per the proto contract — must return 3 events"
    );
}

/// H-36: `synchronize` and `get_event_book` must produce the same event set
/// for the same proto Query. This is the parity test for the helper.
///
/// We assert via `get_event_book` (the public, easily-callable entry point)
/// and via the shared helper that `synchronize` now calls. If either diverges
/// from inclusive-upper semantics, this test fails.
#[tokio::test]
async fn test_dispatch_selection_matches_get_event_book_on_same_range() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let root = uuid::Uuid::new_v4();

    for i in 0..6u32 {
        let events = vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(i)),
            }),
            payload: Some(event_page::Payload::Event(Any {
                type_url: format!("test.Event{i}"),
                value: vec![],
            })),
            created_at: None,
        }];
        event_store
            .add(
                "orders",
                "",
                root,
                events,
                &AddMeta {
                    correlation_id: "",
                    external_id: None,
                    source_info: None,
                    ext: None,
                },
            )
            .await
            .unwrap();
    }

    let service = create_test_service_with_mocks(event_store.clone(), snapshot_store.clone());
    let repo = crate::repository::EventBookRepository::new(
        event_store,
        std::sync::Arc::new(crate::repository::SnapshotRepository::with_flags(
            snapshot_store,
            false,
            false,
        )),
    );

    let range = SequenceRange {
        lower: 1,
        upper: Some(5),
    };

    // get_event_book path
    let unary_query = Query {
        cover: Some(crate::proto::Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        selection: Some(Selection::Range(range)),
    };
    let unary_book = service
        .get_event_book(Request::new(unary_query))
        .await
        .expect("get_event_book must succeed")
        .into_inner();

    // dispatch_selection path (used by synchronize)
    let stream_book = super::dispatch_selection(
        &repo,
        &no_snapshots(),
        "orders",
        "",
        root,
        Some(Selection::Range(range)),
    )
    .await
    .expect("dispatch_selection must succeed");

    assert_eq!(
        unary_book.pages.len(),
        stream_book.pages.len(),
        "get_event_book and synchronize must return the same event count for the same Query"
    );
    assert_eq!(
        unary_book.pages.len(),
        5,
        "Range [1, 5] inclusive must return 5 events (1, 2, 3, 4, 5)"
    );
}

/// H-36: `upper: None` means "to latest". Helper must not truncate.
#[tokio::test]
async fn test_dispatch_selection_range_upper_none_returns_to_latest() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let root = uuid::Uuid::new_v4();

    for i in 0..4u32 {
        let events = vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(i)),
            }),
            payload: Some(event_page::Payload::Event(Any {
                type_url: format!("test.Event{i}"),
                value: vec![],
            })),
            created_at: None,
        }];
        event_store
            .add(
                "orders",
                "",
                root,
                events,
                &AddMeta {
                    correlation_id: "",
                    external_id: None,
                    source_info: None,
                    ext: None,
                },
            )
            .await
            .unwrap();
    }

    let repo = crate::repository::EventBookRepository::new(
        event_store,
        std::sync::Arc::new(crate::repository::SnapshotRepository::with_flags(
            snapshot_store,
            false,
            false,
        )),
    );

    let book = super::dispatch_selection(
        &repo,
        &no_snapshots(),
        "orders",
        "",
        root,
        Some(Selection::Range(SequenceRange {
            lower: 0,
            upper: None,
        })),
    )
    .await
    .expect("range query must succeed");

    assert_eq!(book.pages.len(), 4, "upper: None means 'to latest'");
}

// ============================================================================
// Every query RPC honours the same selection and validation
// ============================================================================

async fn seed_three_events(event_store: &Arc<MockEventStore>, root: uuid::Uuid) {
    let pages = (0..3)
        .map(|seq| EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(seq)),
            }),
            payload: Some(event_page::Payload::Event(Any {
                type_url: "test.Event".to_string(),
                value: vec![],
            })),
            ..Default::default()
        })
        .collect();
    event_store
        .add("orders", "", root, pages, &AddMeta::default())
        .await
        .unwrap();
}

fn root_query(root: uuid::Uuid, edition: Option<&str>, selection: Option<Selection>) -> Query {
    Query {
        cover: Some(crate::proto::Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: edition.map(|name| crate::proto::Edition {
                name: name.to_string(),
                divergences: vec![],
            }),
            ext: None,
        }),
        selection,
    }
}

/// GetEvents streams the selected range, exactly like GetEventBook — not the
/// whole aggregate.
#[tokio::test]
async fn test_get_events_honours_range_selection() {
    let (service, event_store, _) = create_default_test_service();
    let root = uuid::Uuid::new_v4();
    seed_three_events(&event_store, root).await;
    let range = Some(Selection::Range(SequenceRange {
        lower: 1,
        upper: Some(1),
    }));

    let unary = service
        .get_event_book(Request::new(root_query(root, None, range.clone())))
        .await
        .unwrap()
        .into_inner();
    let mut stream = service
        .get_events(Request::new(root_query(root, None, range)))
        .await
        .unwrap()
        .into_inner();
    let streamed = stream.next().await.unwrap().unwrap();
    assert_eq!(streamed.pages.len(), 1);
    assert_eq!(streamed.pages[0].sequence_num(), 1);
    assert_eq!(streamed.pages, unary.pages);
}

/// GetEvents rejects an invalid edition name like GetEventBook does.
#[tokio::test]
async fn test_get_events_validates_edition() {
    let (service, _, _) = create_default_test_service();
    let bad = "x".repeat(1000);
    let err = service
        .get_events(Request::new(root_query(
            uuid::Uuid::new_v4(),
            Some(&bad),
            None,
        )))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

/// Synchronize applies the unary RPC's domain and edition validation to each
/// query (an invalid one is answered with INVALID_ARGUMENT, not served).
#[tokio::test]
async fn test_synchronize_validates_domain_and_edition() {
    let root = uuid::Uuid::new_v4();
    let mut bad_domain = root_query(root, None, None);
    bad_domain.cover.as_mut().unwrap().domain = "bad domain!".to_string();
    let bad_edition = root_query(root, Some(&"x".repeat(1000)), None);

    for (invalid, valid_answer) in [
        (Some(bad_domain), None),
        (Some(bad_edition), None),
        (None, Some(3)),
    ] {
        let (service, event_store, _) = create_default_test_service();
        seed_three_events(&event_store, root).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    crate::proto::event_query_service_server::EventQueryServiceServer::new(service),
                )
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let mut client = crate::proto::event_query_service_client::EventQueryServiceClient::new(
            tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{port}"))
                .unwrap()
                .connect_lazy(),
        );
        let query = invalid.unwrap_or_else(|| root_query(root, None, None));
        let mut out = client
            .synchronize(tokio_stream::iter(vec![query]))
            .await
            .unwrap()
            .into_inner();
        match valid_answer {
            None => assert_eq!(
                out.message().await.unwrap_err().code(),
                tonic::Code::InvalidArgument
            ),
            Some(pages) => assert_eq!(out.message().await.unwrap().unwrap().pages.len(), pages),
        }
    }
}

// ============================================================================
// Snapshots alongside the history (C-0243)
// ============================================================================

async fn ten_events_with_snapshot_at_five(
    event_store: &MockEventStore,
    snapshot_store: &MockSnapshotStore,
    root: uuid::Uuid,
) {
    let events = (0..10)
        .map(|seq| EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(seq)),
            }),
            payload: Some(event_page::Payload::Event(Any {
                type_url: "test.OrderEvent".to_string(),
                value: vec![],
            })),
            created_at: None,
        })
        .collect();
    event_store
        .add(
            "orders",
            "",
            root,
            events,
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();
    for sequence in [2, 5] {
        snapshot_store
            .put(
                "orders",
                "",
                root,
                crate::proto::Snapshot {
                    sequence,
                    state: Some(Any {
                        type_url: "test.OrderState".to_string(),
                        value: vec![sequence as u8],
                    }),
                    retention: crate::proto::SnapshotRetention::RetentionDefault as i32,
                    created_at: None,
                },
            )
            .await
            .unwrap();
    }
}

fn whole_history_query(root: uuid::Uuid) -> Query {
    Query {
        cover: Some(crate::proto::Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        selection: None,
    }
}

/// C-0243: the unary and streamed history queries carry the latest
/// snapshot (sequence 5) and still every event.
#[tokio::test]
async fn test_history_query_carries_the_latest_snapshot_and_every_event() {
    let (service, event_store, snapshot_store) = create_default_test_service();
    let root = uuid::Uuid::new_v4();
    ten_events_with_snapshot_at_five(&event_store, &snapshot_store, root).await;

    let unary = service
        .get_event_book(Request::new(whole_history_query(root)))
        .await
        .unwrap()
        .into_inner();
    let mut stream = service
        .get_events(Request::new(whole_history_query(root)))
        .await
        .unwrap()
        .into_inner();
    let streamed = stream.next().await.unwrap().unwrap();

    for book in [unary, streamed] {
        assert_eq!(book.snapshot.as_ref().map(|s| s.sequence), Some(5));
        assert_eq!(book.pages.len(), 10);
        assert_eq!(book.next_sequence, 10);
    }
}

/// A selection (range, sequences, point in time) answers events only; the
/// snapshot belongs to the whole-history read.
#[tokio::test]
async fn test_range_query_does_not_substitute_a_snapshot() {
    let (service, event_store, snapshot_store) = create_default_test_service();
    let root = uuid::Uuid::new_v4();
    ten_events_with_snapshot_at_five(&event_store, &snapshot_store, root).await;
    let mut query = whole_history_query(root);
    query.selection = Some(Selection::Range(SequenceRange {
        lower: 0,
        upper: None,
    }));
    let book = service
        .get_event_book(Request::new(query))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(book.pages.len(), 10);
    assert!(book.snapshot.is_none());
}

/// With snapshot reads disabled the history query carries no snapshot.
#[tokio::test]
async fn test_history_query_without_snapshot_reads_carries_none() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let root = uuid::Uuid::new_v4();
    ten_events_with_snapshot_at_five(&event_store, &snapshot_store, root).await;
    let service = EventQueryService::with_options(event_store, snapshot_store, false);
    let book = service
        .get_event_book(Request::new(whole_history_query(root)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(book.pages.len(), 10);
    assert!(book.snapshot.is_none());
}
