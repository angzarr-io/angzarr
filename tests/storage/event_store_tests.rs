//! EventStore interface tests.
//!
//! These tests verify the contract of the EventStore trait.
//! Each storage implementation should run these tests.
//!
//! `#![allow(dead_code)]` because each backend's integration-test binary
//! only invokes the subset of contract tests its implementation actually
//! supports (e.g. ImmuDB skips the delete group).

// Each backend binary compiles only the subset it runs; the inventory
// test at the bottom of this file (T12) guards against silent unwiring.
#![allow(dead_code)]

use prost_types::Any;
use uuid::Uuid;

use angzarr::proto::{event_page, page_header::SequenceType, EventPage, PageHeader};
use angzarr::proto_ext::EventPageExt;
use angzarr::storage::{AddMeta, AddOutcome, EventStore, StorageError};

/// Create a test event with given sequence and type.
pub fn make_event(seq: u32, event_type: &str) -> EventPage {
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(seq)),
        }),
        created_at: None,
        payload: Some(event_page::Payload::Event(Any {
            type_url: format!("type.example/{}", event_type),
            value: vec![1, 2, 3, seq as u8],
        })),
    }
}

/// Create multiple sequential events.
pub fn make_events(start: u32, count: u32) -> Vec<EventPage> {
    (start..start + count)
        .map(|i| make_event(i, &format!("Event{}", i)))
        .collect()
}

// =============================================================================
// EventStore::add tests
// =============================================================================

pub async fn test_add_single_event<S: EventStore>(store: &S) {
    let domain = "test_add_single";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root,
            vec![make_event(0, "Created")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let events = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed");
    assert_eq!(events.len(), 1, "should have 1 event");
}

pub async fn test_add_multiple_events<S: EventStore>(store: &S) {
    let domain = "test_add_multiple";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root,
            make_events(0, 5),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let events = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed");
    assert_eq!(events.len(), 5, "should have 5 events");
}

pub async fn test_add_empty_events<S: EventStore>(store: &S) {
    let domain = "test_add_empty";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root,
            vec![],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("empty add should succeed");

    let events = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed");
    assert!(events.is_empty(), "should have no events");
}

pub async fn test_add_sequential_batches<S: EventStore>(store: &S) {
    let domain = "test_add_sequential";
    let root = Uuid::new_v4();

    // First batch: events 0, 1
    store
        .add(
            domain,
            "test",
            root,
            make_events(0, 2),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("first batch should succeed");

    // Second batch: events 2, 3, 4
    store
        .add(
            domain,
            "test",
            root,
            make_events(2, 3),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("second batch should succeed");

    let events = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed");
    assert_eq!(events.len(), 5, "should have 5 events total");

    // Verify sequence continuity
    for (i, event) in events.iter().enumerate() {
        assert_eq!(
            event.sequence_num(),
            i as u32,
            "sequence {} should match index",
            i
        );
    }
}

pub async fn test_add_sequence_conflict<S: EventStore>(store: &S) {
    let domain = "test_seq_conflict";
    let root = Uuid::new_v4();

    // Add events 0, 1, 2
    store
        .add(
            domain,
            "test",
            root,
            make_events(0, 3),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("first add should succeed");

    // Try to add with sequence lower than current (rewinding) - should fail
    // This tests the common contract: can't rewrite history
    let result = store
        .add(
            domain,
            "test",
            root,
            vec![make_event(1, "Rewind")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await;
    assert!(result.is_err(), "sequence lower than current should fail");
}

pub async fn test_add_duplicate_sequence<S: EventStore>(store: &S) {
    let domain = "test_dup_seq";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root,
            make_events(0, 3),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("first add should succeed");

    // Try to add at sequence 0 again
    let result = store
        .add(
            domain,
            "test",
            root,
            vec![make_event(0, "Dup")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await;
    assert!(result.is_err(), "duplicate sequence should fail");
}

/// H-24: contract that the SQL backends already enforce via PRIMARY KEY +
/// sequence-conflict guards. The mock historically accepted any sequence,
/// so a unit test built on mock could silently demonstrate "passing"
/// behavior that the production stores would reject. After H-24's fix,
/// mock must reject the same shapes SQL rejects.
///
/// Three rejection shapes, each on a fresh root:
///   1. Duplicate (re-adding an existing sequence)
///   2. Rewind (sequence below `next_sequence`)
///   3. In-batch duplicate (same sequence appearing twice in one `add()`
///      call) — SQL backends fail this on the second INSERT.
///
/// Gap inserts (e.g. seq=5 with no prior events) are NOT asserted here:
/// the framework supports a few callers that legitimately seed an
/// aggregate at a non-zero sequence (snapshot rehydrate paths), and the
/// SQL backends do not reject those at the storage layer. The bug being
/// closed is duplicate/overlap, which IS uniformly rejected by SQL.
pub async fn test_add_rejects_duplicate_sequences<S: EventStore>(store: &S) {
    // (1) Duplicate: re-adding the same sequence after a successful add.
    {
        let domain = "test_h24_duplicate";
        let root = Uuid::new_v4();
        store
            .add(
                domain,
                "test",
                root,
                make_events(0, 3),
                &AddMeta {
                    correlation_id: "",
                    external_id: None,
                    source_info: None,
                    ext: None,
                },
            )
            .await
            .expect("first add should succeed");

        let result = store
            .add(
                domain,
                "test",
                root,
                vec![make_event(2, "DuplicateOfSeq2")],
                &AddMeta {
                    correlation_id: "",
                    external_id: None,
                    source_info: None,
                    ext: None,
                },
            )
            .await;
        assert!(
            result.is_err(),
            "H-24: re-adding an existing sequence MUST fail; got {:?}",
            result
        );
    }

    // (2) Rewind: a single-event add at a sequence strictly below
    // next_sequence (the canonical "another writer beat me" shape).
    {
        let domain = "test_h24_rewind";
        let root = Uuid::new_v4();
        store
            .add(
                domain,
                "test",
                root,
                make_events(0, 5),
                &AddMeta {
                    correlation_id: "",
                    external_id: None,
                    source_info: None,
                    ext: None,
                },
            )
            .await
            .expect("first add should succeed");

        let result = store
            .add(
                domain,
                "test",
                root,
                vec![make_event(1, "Rewind")],
                &AddMeta {
                    correlation_id: "",
                    external_id: None,
                    source_info: None,
                    ext: None,
                },
            )
            .await;
        assert!(
            result.is_err(),
            "H-24: rewinding to a lower sequence MUST fail; got {:?}",
            result
        );
    }

    // (3) In-batch duplicate: the same sequence twice in one add() call.
    // SQL backends fail on the second INSERT inside the transaction.
    {
        let domain = "test_h24_in_batch_duplicate";
        let root = Uuid::new_v4();
        let result = store
            .add(
                domain,
                "test",
                root,
                vec![make_event(0, "First"), make_event(0, "Duplicate")],
                &AddMeta {
                    correlation_id: "",
                    external_id: None,
                    source_info: None,
                    ext: None,
                },
            )
            .await;
        assert!(
            result.is_err(),
            "H-24: in-batch duplicate sequence MUST fail; got {:?}",
            result
        );
    }
}

// =============================================================================
// Cover.ext round-trip
// =============================================================================

/// The parent-aggregate routing slot (`Cover.ext`, a packed parent `Cover`) must
/// survive a storage round-trip: an aggregate stored with a non-empty `ext`
/// reconstructs with the identical `ext` on read. Dropping it to `None` loses
/// the routing metadata the framework stamps onto every emitted book.
///
/// Backend-agnostic contract — runs against every EventStore implementation.
pub async fn test_cover_ext_round_trips<S: EventStore>(store: &S) {
    let domain = "test_ext_roundtrip";
    let root = Uuid::new_v4();
    let ext = Any {
        type_url: "type.example/ParentCover".to_string(),
        value: vec![0xCA, 0xFE, 0xBA, 0xBE],
    };

    store
        .add(
            domain,
            "test",
            root,
            make_events(0, 2),
            &AddMeta {
                correlation_id: "corr-ext-roundtrip",
                external_id: None,
                source_info: None,
                ext: Some(&ext),
            },
        )
        .await
        .expect("add with ext should succeed");

    let books = store
        .get_by_correlation("corr-ext-roundtrip")
        .await
        .expect("get_by_correlation should succeed");

    let book = books
        .iter()
        .find(|b| b.cover.as_ref().map(|c| c.domain.as_str()) == Some(domain))
        .expect("a book for the aggregate should be returned");
    let cover = book.cover.as_ref().expect("book has a cover");
    assert_eq!(
        cover.ext.as_ref(),
        Some(&ext),
        "Cover.ext must round-trip through storage, not be dropped to None"
    );
}

// =============================================================================
// EventStore::get tests
// =============================================================================

pub async fn test_get_all_events<S: EventStore>(store: &S) {
    let domain = "test_get_all";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root,
            make_events(0, 10),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let events = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed");
    assert_eq!(events.len(), 10, "should return all events");

    // Verify order
    for (i, event) in events.iter().enumerate() {
        assert_eq!(event.sequence_num(), i as u32, "events should be ordered");
    }
}

pub async fn test_get_empty_aggregate<S: EventStore>(store: &S) {
    let domain = "test_get_empty";
    let root = Uuid::new_v4();

    let events = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed");
    assert!(events.is_empty(), "non-existent aggregate should be empty");
}

pub async fn test_get_preserves_event_data<S: EventStore>(store: &S) {
    let domain = "test_preserve_data";
    let root = Uuid::new_v4();

    let original = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(0)),
        }),
        created_at: None,
        payload: Some(event_page::Payload::Event(Any {
            type_url: "type.example/TestEvent".to_string(),
            value: vec![10, 20, 30, 40, 50, 100, 200],
        })),
    };

    store
        .add(
            domain,
            "test",
            root,
            vec![original],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let events = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed");
    assert_eq!(events.len(), 1);

    let event = &events[0];
    if let Some(event_page::Payload::Event(payload)) = &event.payload {
        assert_eq!(payload.type_url, "type.example/TestEvent");
        assert_eq!(payload.value, vec![10, 20, 30, 40, 50, 100, 200]);
    } else {
        panic!("event should have Event payload");
    }
}

// =============================================================================
// EventStore::get_from tests
// =============================================================================

pub async fn test_get_from_zero<S: EventStore>(store: &S) {
    let domain = "test_get_from_zero";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root,
            make_events(0, 5),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let events = store
        .get_from(domain, "test", root, 0)
        .await
        .expect("get_from should succeed");
    assert_eq!(events.len(), 5, "get_from(0) should return all events");
}

pub async fn test_get_from_middle<S: EventStore>(store: &S) {
    let domain = "test_get_from_mid";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root,
            make_events(0, 10),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let events = store
        .get_from(domain, "test", root, 5)
        .await
        .expect("get_from should succeed");
    assert_eq!(events.len(), 5, "should return events 5-9");
    assert_eq!(
        events[0].sequence_num(),
        5,
        "first event should be sequence 5"
    );
}

pub async fn test_get_from_end<S: EventStore>(store: &S) {
    let domain = "test_get_from_end";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root,
            make_events(0, 5),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let events = store
        .get_from(domain, "test", root, 100)
        .await
        .expect("get_from should succeed");
    assert!(events.is_empty(), "get_from beyond end should be empty");
}

pub async fn test_get_from_last<S: EventStore>(store: &S) {
    let domain = "test_get_from_last";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root,
            make_events(0, 5),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let events = store
        .get_from(domain, "test", root, 4)
        .await
        .expect("get_from should succeed");
    assert_eq!(events.len(), 1, "should return last event only");
}

// =============================================================================
// EventStore::get_from_to tests
// =============================================================================

pub async fn test_get_from_to_full_range<S: EventStore>(store: &S) {
    let domain = "test_range_full";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root,
            make_events(0, 5),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let events = store
        .get_from_to(domain, "test", root, 0, 5)
        .await
        .expect("get_from_to should succeed");
    assert_eq!(events.len(), 5, "should return all events");
}

pub async fn test_get_from_to_partial<S: EventStore>(store: &S) {
    let domain = "test_range_partial";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root,
            make_events(0, 10),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    // Get events 3, 4, 5, 6 (exclusive end at 7)
    let events = store
        .get_from_to(domain, "test", root, 3, 7)
        .await
        .expect("get_from_to should succeed");
    assert_eq!(events.len(), 4, "should return 4 events");
    assert_eq!(events[0].sequence_num(), 3, "first should be 3");
    assert_eq!(events[3].sequence_num(), 6, "last should be 6");
}

pub async fn test_get_from_to_single<S: EventStore>(store: &S) {
    let domain = "test_range_single";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root,
            make_events(0, 5),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let events = store
        .get_from_to(domain, "test", root, 2, 3)
        .await
        .expect("get_from_to should succeed");
    assert_eq!(events.len(), 1, "should return single event");
    assert_eq!(events[0].sequence_num(), 2);
}

pub async fn test_get_from_to_empty<S: EventStore>(store: &S) {
    let domain = "test_range_empty";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root,
            make_events(0, 5),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let events = store
        .get_from_to(domain, "test", root, 100, 200)
        .await
        .expect("get_from_to should succeed");
    assert!(events.is_empty(), "out of range should be empty");
}

/// H-25: `get_from_to(from, to=0)` describes the empty half-open range
/// `[from, 0)` and must return zero events without panicking.
///
/// Pre-fix, `dynamo` computed `to - 1` as a bare `u32` subtraction which
/// underflowed and panicked for `to == 0`. Other backends had latent bugs
/// of the same shape — this test pins the contract across all of them.
pub async fn test_get_from_to_zero_to<S: EventStore>(store: &S) {
    let domain = "test_range_zero_to";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root,
            make_events(0, 3),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    // `[0, 0)` is the empty range — must not panic and must return zero events.
    let events = store
        .get_from_to(domain, "test", root, 0, 0)
        .await
        .expect("get_from_to(0, 0) must not panic");
    assert!(
        events.is_empty(),
        "[0, 0) is the empty half-open range, expected 0 events, got {}",
        events.len()
    );
}

// =============================================================================
// EventStore::list_roots tests
// =============================================================================

pub async fn test_list_roots_single<S: EventStore>(store: &S) {
    let domain = "test_roots_single";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root,
            vec![make_event(0, "E")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let roots = store
        .list_roots(domain, "test")
        .await
        .expect("list_roots should succeed");
    assert_eq!(roots.len(), 1);
    assert!(roots.contains(&root));
}

pub async fn test_list_roots_multiple<S: EventStore>(store: &S) {
    let domain = "test_roots_multi";
    let root1 = Uuid::new_v4();
    let root2 = Uuid::new_v4();
    let root3 = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root1,
            vec![make_event(0, "E1")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();
    store
        .add(
            domain,
            "test",
            root2,
            vec![make_event(0, "E2")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();
    store
        .add(
            domain,
            "test",
            root3,
            vec![make_event(0, "E3")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();

    let roots = store
        .list_roots(domain, "test")
        .await
        .expect("list_roots should succeed");
    assert_eq!(roots.len(), 3);
    assert!(roots.contains(&root1));
    assert!(roots.contains(&root2));
    assert!(roots.contains(&root3));
}

pub async fn test_list_roots_empty_domain<S: EventStore>(store: &S) {
    let roots = store
        .list_roots("nonexistent_domain_xyz", "test")
        .await
        .expect("list_roots should succeed");
    assert!(roots.is_empty());
}

pub async fn test_list_roots_domain_isolation<S: EventStore>(store: &S) {
    let domain1 = "test_roots_d1";
    let domain2 = "test_roots_d2";
    let root1 = Uuid::new_v4();
    let root2 = Uuid::new_v4();

    store
        .add(
            domain1,
            "test",
            root1,
            vec![make_event(0, "E1")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();
    store
        .add(
            domain2,
            "test",
            root2,
            vec![make_event(0, "E2")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();

    let roots1 = store.list_roots(domain1, "test").await.unwrap();
    let roots2 = store.list_roots(domain2, "test").await.unwrap();

    assert_eq!(roots1.len(), 1);
    assert_eq!(roots2.len(), 1);
    assert!(roots1.contains(&root1));
    assert!(!roots1.contains(&root2));
    assert!(roots2.contains(&root2));
    assert!(!roots2.contains(&root1));
}

// =============================================================================
// EventStore::list_domains tests
// =============================================================================

pub async fn test_list_domains_contains<S: EventStore>(store: &S) {
    let domain = format!("test_domain_{}", Uuid::new_v4());
    let root = Uuid::new_v4();

    store
        .add(
            &domain,
            "test",
            root,
            vec![make_event(0, "E")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let domains = store
        .list_domains()
        .await
        .expect("list_domains should succeed");
    assert!(domains.contains(&domain), "should contain new domain");
}

pub async fn test_list_domains_multiple<S: EventStore>(store: &S) {
    let domains_to_add: Vec<String> = (0..3)
        .map(|i| format!("test_multi_domain_{}_{}", i, Uuid::new_v4()))
        .collect();

    for domain in &domains_to_add {
        store
            .add(
                domain,
                "test",
                Uuid::new_v4(),
                vec![make_event(0, "E")],
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

    let domains = store
        .list_domains()
        .await
        .expect("list_domains should succeed");
    for domain in &domains_to_add {
        assert!(domains.contains(domain), "should contain {}", domain);
    }
}

// =============================================================================
// EventStore::get_next_sequence tests
// =============================================================================

pub async fn test_get_next_sequence_empty<S: EventStore>(store: &S) {
    let next = store
        .get_next_sequence("nonexistent_seq", "test", Uuid::new_v4())
        .await
        .expect("get_next_sequence should succeed");
    assert_eq!(next, 0, "empty aggregate should have next seq 0");
}

pub async fn test_get_next_sequence_after_events<S: EventStore>(store: &S) {
    let domain = "test_next_seq";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root,
            make_events(0, 7),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let next = store
        .get_next_sequence(domain, "test", root)
        .await
        .expect("get_next_sequence should succeed");
    assert_eq!(next, 7, "next should be 7 after adding 0-6");
}

pub async fn test_get_next_sequence_increments<S: EventStore>(store: &S) {
    let domain = "test_seq_inc";
    let root = Uuid::new_v4();

    assert_eq!(
        store.get_next_sequence(domain, "test", root).await.unwrap(),
        0
    );

    store
        .add(
            domain,
            "test",
            root,
            vec![make_event(0, "E0")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store.get_next_sequence(domain, "test", root).await.unwrap(),
        1
    );

    store
        .add(
            domain,
            "test",
            root,
            make_events(1, 3),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store.get_next_sequence(domain, "test", root).await.unwrap(),
        4
    );
}

// =============================================================================
// Integration tests
// =============================================================================

pub async fn test_aggregate_isolation<S: EventStore>(store: &S) {
    let domain = "test_isolation";
    let root1 = Uuid::new_v4();
    let root2 = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root1,
            make_events(0, 3),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();
    store
        .add(
            domain,
            "test",
            root2,
            make_events(0, 5),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();

    let events1 = store.get(domain, "test", root1).await.unwrap();
    let events2 = store.get(domain, "test", root2).await.unwrap();

    assert_eq!(events1.len(), 3);
    assert_eq!(events2.len(), 5);

    assert_eq!(
        store
            .get_next_sequence(domain, "test", root1)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        store
            .get_next_sequence(domain, "test", root2)
            .await
            .unwrap(),
        5
    );
}

pub async fn test_large_batch<S: EventStore>(store: &S) {
    let domain = "test_large";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root,
            make_events(0, 100),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("large batch should succeed");

    let events = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed");
    assert_eq!(events.len(), 100);

    for (i, event) in events.iter().enumerate() {
        assert_eq!(event.sequence_num(), i as u32);
    }
}

// =============================================================================
// Correlation ID tests
// =============================================================================

pub async fn test_correlation_id_query<S: EventStore>(store: &S) {
    let domain1 = "test_corr_d1";
    let domain2 = "test_corr_d2";
    let root1 = Uuid::new_v4();
    let root2 = Uuid::new_v4();
    let correlation_id = format!("corr-{}", Uuid::new_v4());

    // Add events with same correlation_id across different domains
    store
        .add(
            domain1,
            "test",
            root1,
            vec![make_event(0, "E1")],
            &AddMeta {
                correlation_id: &correlation_id,
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    store
        .add(
            domain2,
            "test",
            root2,
            vec![make_event(0, "E2")],
            &AddMeta {
                correlation_id: &correlation_id,
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    // Query by correlation_id should return both
    let books = store
        .get_by_correlation(&correlation_id)
        .await
        .expect("get_by_correlation should succeed");

    assert_eq!(books.len(), 2, "should find events in both domains");

    // Verify domains are correct
    let domains: Vec<_> = books
        .iter()
        .filter_map(|b| b.cover.as_ref().map(|c| c.domain.as_str()))
        .collect();
    assert!(domains.contains(&domain1));
    assert!(domains.contains(&domain2));
}

pub async fn test_correlation_id_empty_query<S: EventStore>(store: &S) {
    // Query with unknown correlation_id should return empty
    let books = store
        .get_by_correlation("nonexistent-correlation-id-xyz")
        .await
        .expect("get_by_correlation should succeed");

    assert!(books.is_empty(), "unknown correlation should return empty");
}

/// C-16 regression: `get_by_correlation` must tolerate main-timeline rows on
/// SQL backends that normalize the edition column to NULL (per Postgres
/// migration 0007). Before the fix, `row.get::<String, _>("edition")` panicked
/// (sqlx decode error: "unexpected null") and the whole query failed for any
/// caller whose correlation set crossed the main timeline.
pub async fn test_correlation_id_query_main_timeline_null_edition<S: EventStore>(store: &S) {
    let domain = "test_corr_main_null";
    let root = Uuid::new_v4();
    let correlation_id = format!("corr-main-null-{}", Uuid::new_v4());

    // Write an event on the main timeline (`""` at the API layer). Postgres
    // stores this as `edition = NULL`; SQLite stores it as the literal empty
    // string. Either way, the call must not panic and must return the row.
    store
        .add(
            domain,
            "",
            root,
            vec![make_event(0, "MainEvent")],
            &AddMeta {
                correlation_id: &correlation_id,
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add to main timeline should succeed");

    let books = store
        .get_by_correlation(&correlation_id)
        .await
        .expect("get_by_correlation must not panic on main-timeline rows");

    assert_eq!(books.len(), 1, "should find the main-timeline event");
    let cover = books[0].cover.as_ref().expect("book must have a cover");
    assert_eq!(cover.domain, domain);
    assert_eq!(
        cover.correlation_id, correlation_id,
        "correlation_id must round-trip"
    );
    let edition_name = cover
        .edition
        .as_ref()
        .map(|e| e.name.as_str())
        .unwrap_or("");
    // Main-timeline rows surface as empty-string edition at the API. NULL ↔ ""
    // is the normalization contract; the wire form must not leak.
    assert_eq!(
        edition_name, "",
        "main-timeline edition should round-trip as empty string at the API"
    );
}

pub async fn test_correlation_id_preserved<S: EventStore>(store: &S) {
    let domain = "test_corr_preserved";
    let root = Uuid::new_v4();
    let correlation_id = format!("preserved-{}", Uuid::new_v4());

    store
        .add(
            domain,
            "test",
            root,
            vec![make_event(0, "E")],
            &AddMeta {
                correlation_id: &correlation_id,
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let books = store
        .get_by_correlation(&correlation_id)
        .await
        .expect("get_by_correlation should succeed");

    assert_eq!(books.len(), 1);
    let book = &books[0];
    assert_eq!(
        book.cover.as_ref().unwrap().correlation_id,
        correlation_id,
        "correlation_id should be preserved"
    );
}

// =============================================================================
// Edition isolation tests
// =============================================================================

pub async fn test_edition_isolation<S: EventStore>(store: &S) {
    let domain = "test_edition_iso";
    let root = Uuid::new_v4();

    // Add events to main edition
    store
        .add(
            domain,
            "angzarr",
            root,
            vec![make_event(0, "Main")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add to main should succeed");

    // Add events to a named edition
    store
        .add(
            domain,
            "v2",
            root,
            vec![make_event(0, "V2")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add to v2 should succeed");

    // Get from main edition
    let main_events = store
        .get(domain, "angzarr", root)
        .await
        .expect("get should succeed");
    assert_eq!(main_events.len(), 1, "main edition should have 1 event");

    // Get from v2 edition
    let v2_events = store
        .get(domain, "v2", root)
        .await
        .expect("get should succeed");
    assert_eq!(v2_events.len(), 1, "v2 edition should have 1 event");

    // Events should be different
    if let (
        Some(event_page::Payload::Event(main_payload)),
        Some(event_page::Payload::Event(v2_payload)),
    ) = (&main_events[0].payload, &v2_events[0].payload)
    {
        assert_ne!(
            main_payload.type_url, v2_payload.type_url,
            "editions should have different events"
        );
    }
}

pub async fn test_edition_sequences_independent<S: EventStore>(store: &S) {
    let domain = "test_edition_seq";
    let root = Uuid::new_v4();

    // Add 3 events to main edition
    store
        .add(
            domain,
            "angzarr",
            root,
            make_events(0, 3),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    // Add 5 events to v2 edition (sequence starts at 0)
    store
        .add(
            domain,
            "v2",
            root,
            make_events(0, 5),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    // Check sequences are independent
    let main_next = store
        .get_next_sequence(domain, "angzarr", root)
        .await
        .expect("get_next_sequence should succeed");
    assert_eq!(main_next, 3, "main edition should have next seq 3");

    let v2_next = store
        .get_next_sequence(domain, "v2", root)
        .await
        .expect("get_next_sequence should succeed");
    assert_eq!(v2_next, 5, "v2 edition should have next seq 5");
}

pub async fn test_edition_divergence_read<S: EventStore>(store: &S) {
    let domain = "test_diverge_read";
    let root = Uuid::new_v4();

    // Add events to main edition (0, 1, 2)
    store
        .add(
            domain,
            "angzarr",
            root,
            make_events(0, 3),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add to main should succeed");

    // Add diverged events to branch starting at seq 1 (diverges from main)
    // This should return main[0] + branch[1, 2] when read
    store
        .add(
            domain,
            "branch-div",
            root,
            make_events(1, 2),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add to branch should succeed");

    // Read from branch - should get main[0] + branch[1, 2]
    let branch_events = store
        .get(domain, "branch-div", root)
        .await
        .expect("get branch should succeed");

    // The branch should have 3 events: main[0], branch[1], branch[2]
    assert_eq!(
        branch_events.len(),
        3,
        "diverged branch should have 3 events"
    );
    assert_eq!(branch_events[0].sequence_num(), 0, "first should be seq 0");
    assert_eq!(branch_events[1].sequence_num(), 1, "second should be seq 1");
    assert_eq!(branch_events[2].sequence_num(), 2, "third should be seq 2");
}

pub async fn test_edition_divergence_from_middle<S: EventStore>(store: &S) {
    let domain = "test_diverge_mid";
    let root = Uuid::new_v4();

    // Add events to main edition (0, 1, 2, 3, 4)
    store
        .add(
            domain,
            "angzarr",
            root,
            make_events(0, 5),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add to main should succeed");

    // Branch diverges at seq 3
    store
        .add(
            domain,
            "mid-branch",
            root,
            make_events(3, 2),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add to mid-branch should succeed");

    // Read from branch - should get main[0..3] + branch[3, 4]
    let branch_events = store
        .get(domain, "mid-branch", root)
        .await
        .expect("get branch should succeed");

    assert_eq!(branch_events.len(), 5, "branch should have 5 events");

    // First 3 from main (0, 1, 2), then 2 from branch (3, 4)
    for (i, e) in branch_events.iter().enumerate() {
        assert_eq!(e.sequence_num(), i as u32, "seq {} should match", i);
    }
}

pub async fn test_edition_divergence_get_from<S: EventStore>(store: &S) {
    let domain = "test_diverge_from";
    let root = Uuid::new_v4();

    // Main has 0-4
    store
        .add(
            domain,
            "angzarr",
            root,
            make_events(0, 5),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add to main should succeed");

    // Branch diverges at 2
    store
        .add(
            domain,
            "from-branch",
            root,
            make_events(2, 3),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add to branch should succeed");

    // get_from(3) on branch should return branch[3, 4]
    let from_events = store
        .get_from(domain, "from-branch", root, 3)
        .await
        .expect("get_from should succeed");

    assert_eq!(from_events.len(), 2, "should have events 3, 4");
    assert_eq!(from_events[0].sequence_num(), 3);
    assert_eq!(from_events[1].sequence_num(), 4);
}

/// Finding #10: `get_from_to` on a diverged edition must include the
/// pre-divergence main-timeline prefix, exactly as `get`/`get_from` do.
///
/// Pre-fix, the SQL backends built `get_from_to` as a single query filtered
/// only on `edition_predicate(edition)` — it never merged in the main
/// timeline. So a ranged read on a diverged edition returned ONLY the
/// edition-tagged rows in range, dropping every main-timeline event before
/// the divergence point. This is the range-read half of #10 (the temporal
/// read is `test_edition_get_until_timestamp_includes_main_prefix`).
pub async fn test_edition_get_from_to_includes_main_prefix<S: EventStore>(store: &S) {
    let domain = "test_range_prefix";
    let root = Uuid::new_v4();

    // Main timeline: 0, 1, 2, 3, 4.
    store
        .add(
            domain,
            "angzarr",
            root,
            make_events(0, 5),
            &AddMeta::default(),
        )
        .await
        .expect("add to main should succeed");

    // Branch diverges at seq 3 (branch owns 3, 4; inherits main 0,1,2).
    store
        .add(
            domain,
            "range-branch",
            root,
            make_events(3, 2),
            &AddMeta::default(),
        )
        .await
        .expect("add to branch should succeed");

    // get_from_to(1, 4) over the branch spans the divergence: it must
    // return main[1], main[2] (pre-divergence prefix) + branch[3].
    let ranged = store
        .get_from_to(domain, "range-branch", root, 1, 4)
        .await
        .expect("get_from_to should succeed");

    let seqs: Vec<u32> = ranged.iter().map(|e| e.sequence_num()).collect();
    assert_eq!(
        seqs,
        vec![1, 2, 3],
        "get_from_to on a diverged edition must include the pre-divergence \
         main-timeline prefix (seqs 1,2) alongside the branch event (seq 3); \
         got {:?} — dropping the prefix is finding #10",
        seqs
    );
}

/// Finding #10: `get_until_timestamp` on a diverged edition must include the
/// pre-divergence main-timeline prefix.
///
/// This is the path `EventBookRepository::get_temporal_by_sequence` takes
/// on its full-replay branch: a "state as of T" read that, pre-fix, filtered
/// only on `edition_predicate(edition)` and so reconstructed corrupt state
/// from post-divergence rows only. Whole-second timestamps keep the test
/// valid on ImmuDB (whose TIMESTAMP column floors sub-second precision).
pub async fn test_edition_get_until_timestamp_includes_main_prefix<S: EventStore>(store: &S) {
    use prost_types::Timestamp;

    let domain = "test_temporal_prefix";
    let root = Uuid::new_v4();

    // Main timeline events 0,1,2 at t0<t1<t2 (whole seconds).
    let base = 1_700_000_000i64;
    let main: Vec<EventPage> = (0..3)
        .map(|i| {
            let mut e = make_event(i, &format!("Main{i}"));
            e.created_at = Some(Timestamp {
                seconds: base + i as i64,
                nanos: 0,
            });
            e
        })
        .collect();
    store
        .add(domain, "angzarr", root, main, &AddMeta::default())
        .await
        .expect("add to main should succeed");

    // Branch diverges at seq 3; branch events 3,4 at strictly later instants.
    let branch: Vec<EventPage> = (3..5)
        .map(|i| {
            let mut e = make_event(i, &format!("Branch{i}"));
            e.created_at = Some(Timestamp {
                seconds: base + i as i64,
                nanos: 0,
            });
            e
        })
        .collect();
    store
        .add(domain, "temporal-branch", root, branch, &AddMeta::default())
        .await
        .expect("add to branch should succeed");

    // "State as of t3": inclusive of main[0,1,2] (t0,t1,t2 <= t3) and
    // branch[3] (t3 <= t3), excluding branch[4] (t4 > t3).
    let until = Timestamp {
        seconds: base + 3,
        nanos: 0,
    };
    let as_of = store
        .get_until_timestamp(domain, "temporal-branch", root, &until)
        .await
        .expect("get_until_timestamp should succeed");

    let seqs: Vec<u32> = as_of.iter().map(|e| e.sequence_num()).collect();
    assert_eq!(
        seqs,
        vec![0, 1, 2, 3],
        "get_until_timestamp on a diverged edition must include the \
         pre-divergence main-timeline prefix (seqs 0,1,2); got {:?} — \
         dropping it corrupts temporal reconstruction (finding #10)",
        seqs
    );
}

/// Finding #12 (LOCKED: inherit main timeline): a named edition with NO
/// events of its own and no explicit divergence must read as its base — the
/// entire main timeline — until it explicitly diverges.
///
/// SQLite, ImmuDB, and the mock already behave this way. Postgres did NOT:
/// its composite stored procedure computed the divergence point as
/// `COALESCE(explicit, MIN(edition.seq), 0)`, so with no edition rows the
/// point was the literal `0`, the main-timeline filter `sequence < 0` was
/// never true, and the read returned ZERO rows. This contract test pins the
/// uniform behavior across all SQL backends (RED on pre-fix Postgres).
pub async fn test_eventless_edition_inherits_main_timeline<S: EventStore>(store: &S) {
    let domain = "test_eventless_inherit";
    let root = Uuid::new_v4();

    // Main timeline: 0, 1, 2. The named edition below writes nothing.
    store
        .add(
            domain,
            "angzarr",
            root,
            make_events(0, 3),
            &AddMeta::default(),
        )
        .await
        .expect("add to main should succeed");

    // Read an eventless named edition — must inherit the whole main timeline.
    let inherited = store
        .get(domain, "ghost-edition", root)
        .await
        .expect("get on eventless edition should succeed");

    let seqs: Vec<u32> = inherited.iter().map(|e| e.sequence_num()).collect();
    assert_eq!(
        seqs,
        vec![0, 1, 2],
        "an eventless named edition must inherit the full main timeline \
         (LOCKED #12 contract); got {:?}. Zero rows here is the pre-fix \
         Postgres 'divergence=0 → sequence<0' bug",
        seqs
    );

    // The same must hold for get_from from the middle of the inherited range.
    let from_mid = store
        .get_from(domain, "ghost-edition", root, 1)
        .await
        .expect("get_from on eventless edition should succeed");
    let from_seqs: Vec<u32> = from_mid.iter().map(|e| e.sequence_num()).collect();
    assert_eq!(
        from_seqs,
        vec![1, 2],
        "get_from on an eventless edition must inherit the main-timeline \
         tail from the requested sequence; got {:?}",
        from_seqs
    );
}

/// Test explicit divergence for NEW edition branches (no prior edition events).
///
/// This tests the case where we create a brand new branch with an explicit
/// divergence point. Without existing edition events, implicit divergence
/// fails - the stored procedure needs explicit divergence to know where to
/// branch from the main timeline.
///
/// Scenario:
/// - Main timeline has events 0-4
/// - We want to branch at seq 3 (new branch starts from main[0..3])
/// - Branch has NO existing events yet
/// - Reading from branch should return main[0, 1, 2] (up to divergence)
pub async fn test_edition_explicit_divergence_new_branch<S: EventStore>(store: &S) {
    let domain = "test_explicit_div";
    let root = Uuid::new_v4();

    // Add events to main timeline (0, 1, 2, 3, 4)
    store
        .add(
            domain,
            "angzarr",
            root,
            make_events(0, 5),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add to main should succeed");

    // Read from a NEW branch with explicit divergence at seq 3.
    // This branch has NO existing events.
    // With explicit divergence at 3, we should get main[0, 1, 2].
    let branch_events = store
        .get_with_divergence(domain, "new-explicit-branch", root, Some(3))
        .await
        .expect("get from new branch should succeed");

    // Should get main events up to (but not including) divergence point
    assert_eq!(
        branch_events.len(),
        3,
        "new branch with explicit divergence at 3 should have main[0, 1, 2]"
    );
    assert_eq!(branch_events[0].sequence_num(), 0);
    assert_eq!(branch_events[1].sequence_num(), 1);
    assert_eq!(branch_events[2].sequence_num(), 2);
}

pub async fn test_edition_filtered_roots<S: EventStore>(store: &S) {
    let domain = "test_edition_roots";
    let root_main = Uuid::new_v4();
    let root_v2 = Uuid::new_v4();

    store
        .add(
            domain,
            "angzarr",
            root_main,
            vec![make_event(0, "Main")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();
    store
        .add(
            domain,
            "v2",
            root_v2,
            vec![make_event(0, "V2")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();

    let main_roots = store
        .list_roots(domain, "angzarr")
        .await
        .expect("list_roots should succeed");
    let v2_roots = store
        .list_roots(domain, "v2")
        .await
        .expect("list_roots should succeed");

    assert!(main_roots.contains(&root_main));
    assert!(!main_roots.contains(&root_v2));
    assert!(v2_roots.contains(&root_v2));
    assert!(!v2_roots.contains(&root_main));
}

// =============================================================================
// Main-timeline sentinel polarity tests (C-15)
// =============================================================================
//
// The "main timeline" has TWO documented sentinel forms at the API boundary:
// the empty string `""` and the literal `"angzarr"` (per `is_main_timeline`).
// These MUST be interchangeable on both writes and reads. Otherwise the same
// caller writing `edition=""` then reading `edition="angzarr"` (or vice versa)
// silently misses the row.
//
// On SQL backends with a nullable edition column (Postgres + SQLite both have
// the migration that normalizes `""` and `"angzarr"` to SQL NULL), the storage
// layer must:
//   1. Normalize BOTH `""` and `"angzarr"` to `NULL` on writes.
//   2. Match `NULL` on reads when the caller passes either sentinel.
//
// Pre-fix bug: Postgres `edition_to_db` only normalized `""` → NULL, leaving
// `"angzarr"` to land as a literal `"angzarr"` row that `is_main_timeline()`
// reads then went looking for as NULL — so the round-trip silently lost data.
// SQLite stored every form raw, so legacy rows that the migration 0006
// already converted to NULL were invisible to any query.

/// C-15: round-trip via the empty-string sentinel must be visible via BOTH
/// main-timeline forms.
pub async fn test_main_timeline_sentinel_write_empty_read_both<S: EventStore>(store: &S) {
    let domain = "test_main_sentinel_empty";
    let root = Uuid::new_v4();

    // Write using `""`.
    store
        .add(
            domain,
            "",
            root,
            vec![make_event(0, "MainEmpty")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add with empty-string main-timeline sentinel should succeed");

    let via_empty = store
        .get(domain, "", root)
        .await
        .expect("get with empty sentinel should succeed");
    assert_eq!(
        via_empty.len(),
        1,
        "read via empty-string sentinel must find the row written with empty-string sentinel"
    );

    let via_angzarr = store
        .get(domain, "angzarr", root)
        .await
        .expect("get with angzarr sentinel should succeed");
    assert_eq!(
        via_angzarr.len(),
        1,
        "read via 'angzarr' sentinel must find the row written with empty-string sentinel"
    );
}

/// C-15: round-trip via the `"angzarr"` sentinel must be visible via BOTH
/// main-timeline forms.
pub async fn test_main_timeline_sentinel_write_angzarr_read_both<S: EventStore>(store: &S) {
    let domain = "test_main_sentinel_angzarr";
    let root = Uuid::new_v4();

    // Write using `"angzarr"`.
    store
        .add(
            domain,
            "angzarr",
            root,
            vec![make_event(0, "MainAngzarr")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add with 'angzarr' main-timeline sentinel should succeed");

    let via_angzarr = store
        .get(domain, "angzarr", root)
        .await
        .expect("get with angzarr sentinel should succeed");
    assert_eq!(
        via_angzarr.len(),
        1,
        "read via 'angzarr' sentinel must find the row written with 'angzarr' sentinel"
    );

    let via_empty = store
        .get(domain, "", root)
        .await
        .expect("get with empty sentinel should succeed");
    assert_eq!(
        via_empty.len(),
        1,
        "read via empty-string sentinel must find the row written with 'angzarr' sentinel"
    );
}

/// C-15: `find_by_external_id` must also honor the polarity equivalence.
/// An aggregate written with idempotency key on `""` must be discoverable
/// when the lookup uses `"angzarr"`.
pub async fn test_main_timeline_external_id_sentinel_polarity<S: EventStore>(store: &S) {
    let domain = "test_main_sentinel_extid";
    let root = Uuid::new_v4();
    let external_id = format!("ext-c15-{}", Uuid::new_v4());

    store
        .add(
            domain,
            "",
            root,
            vec![make_event(0, "ExtClaim")],
            &AddMeta {
                correlation_id: "",
                external_id: Some(&external_id),
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add with external_id on empty sentinel should succeed");

    // Look up via "angzarr" — the saga idempotency contract must not depend
    // on which form of the sentinel the original caller used.
    let result = store
        .find_by_external_id(domain, "angzarr", root, &external_id)
        .await
        .expect("find_by_external_id should succeed");
    assert!(
        result.is_some(),
        "find_by_external_id via 'angzarr' sentinel must find the row written with empty-string sentinel"
    );
    assert_eq!(result.unwrap().len(), 1);
}

/// C-15: `delete_edition_events` must reject BOTH main-timeline sentinels.
///
/// Previously the Postgres stored proc only raised on NULL/empty `p_edition`;
/// a caller passing `"angzarr"` would bypass that guard and the proc would
/// silently delete zero rows (because main-timeline rows are stored as NULL).
/// That's a confusing no-op, but more importantly the documented contract is
/// "main timeline ('angzarr' or empty edition) protection" — `"angzarr"`
/// must raise the same as `""`/NULL.
pub async fn test_delete_edition_events_rejects_main_timeline_sentinels<S: EventStore>(store: &S) {
    let domain = "test_del_main_sentinel";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "",
            root,
            vec![make_event(0, "Main")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("seed main-timeline event");

    // Delete with `"angzarr"` MUST raise.
    let result = store.delete_edition_events(domain, "angzarr").await;
    assert!(
        result.is_err(),
        "delete_edition_events('angzarr') must raise to protect the main timeline; got Ok({:?})",
        result.ok(),
    );

    // The seeded event must still be there.
    let after = store
        .get(domain, "", root)
        .await
        .expect("main-timeline read after rejected delete");
    assert_eq!(
        after.len(),
        1,
        "main-timeline event must survive a rejected delete_edition_events('angzarr')"
    );

    // Delete with `""` MUST also raise (unchanged behavior).
    let result_empty = store.delete_edition_events(domain, "").await;
    assert!(
        result_empty.is_err(),
        "delete_edition_events('') must raise to protect the main timeline; got Ok({:?})",
        result_empty.ok(),
    );
}

// =============================================================================
// Idempotency tests (external_id)
// =============================================================================

pub async fn test_add_with_external_id_returns_duplicate<S: EventStore>(store: &S) {
    let domain = "test_idem_dup";
    let root = Uuid::new_v4();

    // First add with external_id
    let result1 = store
        .add(
            domain,
            "test",
            root,
            make_events(0, 3),
            &AddMeta {
                correlation_id: "",
                external_id: Some("ext-123"),
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("first add should succeed");

    // Verify first add was Added
    match result1 {
        angzarr::storage::AddOutcome::Added {
            first_sequence,
            last_sequence,
        } => {
            assert_eq!(first_sequence, 0);
            assert_eq!(last_sequence, 2);
        }
        _ => panic!("expected Added outcome"),
    }

    // Second add with same external_id should return Duplicate
    let result2 = store
        .add(
            domain,
            "test",
            root,
            make_events(3, 2),
            &AddMeta { correlation_id: // Different events
            "", external_id: Some("ext-123"), source_info: // Same external_id
            None, ext: None },
        )
        .await
        .expect("duplicate add should succeed");

    match result2 {
        angzarr::storage::AddOutcome::Duplicate {
            first_sequence,
            last_sequence,
        } => {
            assert_eq!(first_sequence, 0, "should return original first seq");
            assert_eq!(last_sequence, 2, "should return original last seq");
        }
        _ => panic!("expected Duplicate outcome"),
    }

    // Verify no new events were added
    let events = store.get(domain, "test", root).await.unwrap();
    assert_eq!(events.len(), 3, "should only have original 3 events");
}

pub async fn test_add_different_external_ids_allowed<S: EventStore>(store: &S) {
    let domain = "test_idem_diff";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "test",
            root,
            make_events(0, 2),
            &AddMeta {
                correlation_id: "",
                external_id: Some("ext-aaa"),
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("first add should succeed");

    store
        .add(
            domain,
            "test",
            root,
            make_events(2, 2),
            &AddMeta {
                correlation_id: "",
                external_id: Some("ext-bbb"),
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("second add with different external_id should succeed");

    let events = store.get(domain, "test", root).await.unwrap();
    assert_eq!(events.len(), 4, "should have all 4 events");
}

// =============================================================================
// Timestamp tests
// =============================================================================

pub async fn test_get_until_timestamp_filters<S: EventStore>(store: &S) {
    use prost_types::Timestamp;

    let domain = "test_ts_filter";
    let root = Uuid::new_v4();

    // Create events at different timestamps
    let ts_old = Timestamp {
        seconds: 1700000000, // 2023-11-14
        nanos: 0,
    };
    let ts_new = Timestamp {
        seconds: 1710000000, // 2024-03-09
        nanos: 0,
    };

    let event_old = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(0)),
        }),
        created_at: Some(ts_old),
        payload: Some(event_page::Payload::Event(Any {
            type_url: "type.example/Old".to_string(),
            value: vec![1],
        })),
    };

    let event_new = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(1)),
        }),
        created_at: Some(ts_new),
        payload: Some(event_page::Payload::Event(Any {
            type_url: "type.example/New".to_string(),
            value: vec![2],
        })),
    };

    store
        .add(
            domain,
            "test",
            root,
            vec![event_old, event_new],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    // Query with timestamp between old and new
    let until = Timestamp {
        seconds: 1704067200, // 2024-01-01T00:00:00Z — after old, before new
        nanos: 0,
    };
    let filtered = store
        .get_until_timestamp(domain, "test", root, &until)
        .await
        .expect("get_until_timestamp should succeed");

    assert_eq!(filtered.len(), 1, "should only return old event");
    assert_eq!(filtered[0].sequence_num(), 0);
}

pub async fn test_get_until_timestamp_returns_all_when_recent<S: EventStore>(store: &S) {
    use prost_types::Timestamp;

    let domain = "test_ts_all";
    let root = Uuid::new_v4();

    let ts = Timestamp {
        seconds: 1700000000,
        nanos: 0,
    };

    let event = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(0)),
        }),
        created_at: Some(ts),
        payload: Some(event_page::Payload::Event(Any {
            type_url: "type.example/E".to_string(),
            value: vec![1],
        })),
    };

    store
        .add(
            domain,
            "test",
            root,
            vec![event],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    // Query with timestamp far in the future
    let until = Timestamp {
        seconds: 1893456000, // 2030-01-01T00:00:00Z
        nanos: 0,
    };
    let all = store
        .get_until_timestamp(domain, "test", root, &until)
        .await
        .expect("get_until_timestamp should succeed");

    assert_eq!(all.len(), 1, "should return all events");
}

/// C10 (finding #26) regression: the pre-fix `get_until_timestamp(until:
/// &str)` compared a caller-formatted string lexically against the SQL
/// backends' TEXT `created_at` column. A caller-chosen `Z` suffix instead
/// of the producer's uniform `+00:00` corrupted the comparison — the
/// stored fractional-second marker `.` (0x2E) sorts BELOW `Z` (0x5A) but
/// ABOVE `+` (0x2B), so a `Z`-suffixed boundary leaked a strictly-later
/// event into a "state as of T" read (confirmed red against the pre-fix
/// SQLite backend: 2 events returned via `Z`, 1 via `+00:00`, for the
/// identical instant).
///
/// The typed `until: &prost_types::Timestamp` signature removes the
/// possibility structurally — there is no second string spelling of the
/// same instant for a caller to pick, and every backend derives its own
/// comparable form from the SAME typed value through exactly one function
/// (`storage::helpers::timestamp_to_rfc3339` for TEXT-column SQL
/// backends). This test pins the resulting nanosecond-precision boundary
/// behavior: an event exactly AT `until` is included (inclusive `<=`); an
/// event one nanosecond-granularity tick AFTER `until` is excluded.
///
/// Every backend runs it: ImmuDB's whole-second `TIMESTAMP` column only
/// narrows the candidates; the exact cut uses each page's own timestamp.
pub async fn test_get_until_timestamp_nanosecond_boundary_precision<S: EventStore>(store: &S) {
    use prost_types::Timestamp;

    let domain = "test_ts_boundary";
    let root = Uuid::new_v4();

    let boundary = Timestamp {
        seconds: 1704153600, // 2024-01-02T00:00:00Z
        nanos: 0,
    };
    let one_micro_after = Timestamp {
        seconds: 1704153600,
        nanos: 1_000, // +1 microsecond — strictly after the boundary
    };

    let event_at_boundary = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(0)),
        }),
        created_at: Some(boundary),
        payload: Some(event_page::Payload::Event(Any {
            type_url: "type.example/AtBoundary".to_string(),
            value: vec![1],
        })),
    };
    let event_after_boundary = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(1)),
        }),
        created_at: Some(one_micro_after),
        payload: Some(event_page::Payload::Event(Any {
            type_url: "type.example/AfterBoundary".to_string(),
            value: vec![2],
        })),
    };

    store
        .add(
            domain,
            "test",
            root,
            vec![event_at_boundary, event_after_boundary],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let filtered = store
        .get_until_timestamp(domain, "test", root, &boundary)
        .await
        .expect("get_until_timestamp should succeed");

    assert_eq!(
        filtered.len(),
        1,
        "boundary query must include the at-boundary event and exclude the \
         strictly-later (+1us) event — a regression here means the typed \
         boundary reopened the C10 lexical-comparison footgun"
    );
    assert_eq!(filtered[0].sequence_num(), 0);
}

/// A page added without `created_at` reads back with the time it was
/// stored: readers (and temporal cuts) see when every event happened.
pub async fn test_add_stamps_created_at_on_unstamped_pages<S: EventStore>(store: &S) {
    let domain = "test_created_at_stamp";
    let root = Uuid::new_v4();
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    store
        .add(
            domain,
            "test",
            root,
            vec![make_event(0, "Created")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let events = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed");
    assert_eq!(events.len(), 1);
    let stamped = events[0]
        .created_at
        .as_ref()
        .expect("the stored page carries its persist time");
    assert!(
        stamped.seconds >= before && stamped.seconds <= after,
        "created_at {} not within [{before}, {after}]",
        stamped.seconds
    );
}

pub async fn test_timestamp_preservation<S: EventStore>(store: &S) {
    use prost_types::Timestamp;

    let domain = "test_timestamp";
    let root = Uuid::new_v4();

    let timestamp = Timestamp {
        seconds: 1704067200, // 2024-01-01 00:00:00 UTC
        nanos: 123456789,
    };

    let event = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(0)),
        }),
        created_at: Some(timestamp),
        payload: Some(event_page::Payload::Event(Any {
            type_url: "type.example/TimestampTest".to_string(),
            value: vec![1, 2, 3],
        })),
    };

    store
        .add(
            domain,
            "test",
            root,
            vec![event],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let events = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed");
    assert_eq!(events.len(), 1);

    let retrieved = &events[0];
    assert!(
        retrieved.created_at.is_some(),
        "timestamp should be preserved"
    );
    let retrieved_ts = retrieved.created_at.as_ref().unwrap();
    assert_eq!(
        retrieved_ts.seconds, timestamp.seconds,
        "seconds should match"
    );
    assert_eq!(retrieved_ts.nanos, timestamp.nanos, "nanos should match");
}

// =============================================================================
// Large scale tests
// =============================================================================

pub async fn test_large_aggregate_10k<S: EventStore>(store: &S) {
    let domain = "test_large_10k";
    let root = Uuid::new_v4();

    // Add 10,000 events in batches of 1000
    for batch in 0..10 {
        let start = batch * 1000;
        store
            .add(
                domain,
                "test",
                root,
                make_events(start, 1000),
                &AddMeta {
                    correlation_id: "",
                    external_id: None,
                    source_info: None,
                    ext: None,
                },
            )
            .await
            .expect("batch add should succeed");
    }

    let events = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed");
    assert_eq!(events.len(), 10000, "should have 10,000 events");

    // Verify sequence continuity
    for (i, event) in events.iter().enumerate() {
        assert_eq!(
            event.sequence_num(),
            i as u32,
            "sequence {} should match",
            i
        );
    }

    // Verify partial range retrieval works
    let partial = store
        .get_from_to(domain, "test", root, 5000, 5010)
        .await
        .expect("get_from_to should succeed");
    assert_eq!(partial.len(), 10, "partial range should return 10 events");
    assert_eq!(partial[0].sequence_num(), 5000);
    assert_eq!(partial[9].sequence_num(), 5009);
}

// =============================================================================
// delete_edition_events tests
// =============================================================================

pub async fn test_delete_edition_events_removes_all<S: EventStore>(store: &S) {
    let domain = "test_del_edition";
    let root1 = Uuid::new_v4();
    let root2 = Uuid::new_v4();

    // Add events to two aggregates in the same edition
    store
        .add(
            domain,
            "branch-1",
            root1,
            make_events(0, 3),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");
    store
        .add(
            domain,
            "branch-1",
            root2,
            make_events(0, 2),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    // Delete edition events
    let count = store
        .delete_edition_events(domain, "branch-1")
        .await
        .expect("delete should succeed");
    assert_eq!(count, 5, "should delete 3 + 2 = 5 events");

    // Verify events are gone
    let events1 = store.get(domain, "branch-1", root1).await.unwrap();
    let events2 = store.get(domain, "branch-1", root2).await.unwrap();
    assert!(events1.is_empty(), "events should be deleted");
    assert!(events2.is_empty(), "events should be deleted");
}

pub async fn test_delete_edition_events_scoped<S: EventStore>(store: &S) {
    let domain = "test_del_scoped";
    let root = Uuid::new_v4();

    // Add to main edition
    store
        .add(
            domain,
            "angzarr",
            root,
            vec![make_event(0, "Main")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    // Add to branch edition
    store
        .add(
            domain,
            "branch-1",
            root,
            vec![make_event(0, "Branch")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    // Delete only branch edition
    let count = store
        .delete_edition_events(domain, "branch-1")
        .await
        .expect("delete should succeed");
    assert_eq!(count, 1, "should delete 1 event");

    // Main edition should be unaffected
    let main_events = store.get(domain, "angzarr", root).await.unwrap();
    assert_eq!(main_events.len(), 1, "main edition should still have event");
}

// =============================================================================
// find_by_source tests
// =============================================================================

pub async fn test_find_by_source_returns_match<S: EventStore>(store: &S) {
    let domain = "test_find_src";
    let root = Uuid::new_v4();
    let source_root = Uuid::new_v4();

    let source_info = angzarr::storage::SourceInfo {
        domain: "orders".to_string(),
        edition: "angzarr".to_string(),
        root: source_root,
        seq: 5,
        component: "saga-orders-test".to_string(),
        command_index: 0,
        kind: angzarr::storage::ProvenanceKind::Command,
    };

    store
        .add(
            domain,
            "angzarr",
            root,
            vec![make_event(0, "Derived")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: Some(&source_info),
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let result = store
        .find_by_source(domain, "angzarr", root, &source_info)
        .await
        .expect("find_by_source should succeed");

    assert!(result.is_some(), "should find matching event");
    assert_eq!(result.unwrap().len(), 1);
}

pub async fn test_find_by_source_no_match<S: EventStore>(store: &S) {
    let domain = "test_find_no_match";
    let root = Uuid::new_v4();
    let source_root = Uuid::new_v4();

    let source_info = angzarr::storage::SourceInfo {
        domain: "orders".to_string(),
        edition: "angzarr".to_string(),
        root: source_root,
        seq: 5,
        component: "saga-orders-test".to_string(),
        command_index: 0,
        kind: angzarr::storage::ProvenanceKind::Command,
    };

    store
        .add(
            domain,
            "angzarr",
            root,
            vec![make_event(0, "NoSource")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let result = store
        .find_by_source(domain, "angzarr", root, &source_info)
        .await
        .expect("find_by_source should succeed");

    assert!(result.is_none(), "should not find non-matching event");
}

// =============================================================================
// find_by_external_id round-trip tests (C-18)
//
// Trait contract (`src/storage/event_store.rs:250-267`): `find_by_external_id`
// returns Some(events) for matches recorded by `add(..., Some(external_id), _)`,
// None otherwise. Backends that silently drop `_external_id` at add() time
// pass `test_find_by_external_id_no_match` (None is correctly returned for a
// claim that was never recorded) but fail `_round_trip` (the claim that
// SHOULD have been recorded was thrown away).
// =============================================================================

pub async fn test_find_by_external_id_round_trip<S: EventStore>(store: &S) {
    let domain = "test_find_ext_rt";
    let root = Uuid::new_v4();
    let external_id = "ext-roundtrip-abc";

    store
        .add(
            domain,
            "angzarr",
            root,
            vec![make_event(0, "Claimed")],
            &AddMeta {
                correlation_id: "",
                external_id: Some(external_id),
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add with external_id should succeed");

    let result = store
        .find_by_external_id(domain, "angzarr", root, external_id)
        .await
        .expect("find_by_external_id should succeed");

    assert!(
        result.is_some(),
        "find_by_external_id should return the persisted claim — the backend \
         silently dropped external_id at add() time if this fails (see C-18)"
    );
    let events = result.unwrap();
    assert_eq!(
        events.len(),
        1,
        "should retrieve the event that carried the external_id"
    );
}

pub async fn test_find_by_external_id_no_match<S: EventStore>(store: &S) {
    let domain = "test_find_ext_nomatch";
    let root = Uuid::new_v4();

    // Persist an event WITHOUT external_id
    store
        .add(
            domain,
            "angzarr",
            root,
            vec![make_event(0, "NoClaim")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let result = store
        .find_by_external_id(domain, "angzarr", root, "ext-never-stored")
        .await
        .expect("find_by_external_id should succeed");

    assert!(
        result.is_none(),
        "find_by_external_id should return None when the claim was never recorded"
    );
}

pub async fn test_find_by_external_id_empty_returns_none<S: EventStore>(store: &S) {
    let domain = "test_find_ext_empty";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "angzarr",
            root,
            vec![make_event(0, "NoClaim")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("add should succeed");

    let result = store
        .find_by_external_id(domain, "angzarr", root, "")
        .await
        .expect("find_by_external_id should succeed");

    assert!(
        result.is_none(),
        "find_by_external_id with empty external_id must return None per trait contract"
    );
}

// =============================================================================
// find_by_source round-trip test (C-18)
//
// Distinct from `test_find_by_source_returns_match` above: this variant pins
// the **multi-field** round trip — every field of `SourceInfo` (edition,
// domain, root, seq, component, command_index) must survive `add()` and be matched on by
// `find_by_source`. Backends that drop `_source_info` at add() time pass
// `test_find_by_source_no_match` (None is correctly returned for a
// non-existent claim) but fail this test (the claim that SHOULD have been
// stored was thrown away — and additionally, every SourceInfo field is
// exercised distinctly).
// =============================================================================

pub async fn test_find_by_source_round_trip<S: EventStore>(store: &S) {
    let domain = "test_find_src_rt";
    let root = Uuid::new_v4();
    let source_root = Uuid::new_v4();

    let source_info = angzarr::storage::SourceInfo {
        domain: "orders".to_string(),
        edition: "angzarr".to_string(),
        root: source_root,
        seq: 42,
        component: "saga-orders-test".to_string(),
        command_index: 1,
        kind: angzarr::storage::ProvenanceKind::Command,
    };

    store
        .add(
            domain,
            "angzarr",
            root,
            vec![make_event(0, "Translated")],
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: Some(&source_info),
                ext: None,
            },
        )
        .await
        .expect("add with source_info should succeed");

    let result = store
        .find_by_source(domain, "angzarr", root, &source_info)
        .await
        .expect("find_by_source should succeed");

    assert!(
        result.is_some(),
        "find_by_source should return the persisted source claim — the backend \
         silently dropped source_info at add() time if this fails (see C-18)"
    );
    assert_eq!(result.unwrap().len(), 1);

    // Mismatch on seq must NOT match — confirms ALL fields are stored, not just one.
    let mismatched_seq = angzarr::storage::SourceInfo {
        seq: source_info.seq + 1,
        ..source_info.clone()
    };
    let result_wrong = store
        .find_by_source(domain, "angzarr", root, &mismatched_seq)
        .await
        .expect("find_by_source should succeed");
    assert!(
        result_wrong.is_none(),
        "different source seq must not match — backend isn't storing seq correctly"
    );

    // O1: mismatch on component must NOT match — a different component
    // reacting to the same source event is a distinct idempotency claim.
    let mismatched_component = angzarr::storage::SourceInfo {
        component: "saga-orders-other".to_string(),
        ..source_info.clone()
    };
    let result_wrong = store
        .find_by_source(domain, "angzarr", root, &mismatched_component)
        .await
        .expect("find_by_source should succeed");
    assert!(
        result_wrong.is_none(),
        "different source component must not match — backend isn't storing component correctly"
    );

    // O1: mismatch on command_index must NOT match — a different command of
    // the same invocation is a distinct idempotency claim.
    let mismatched_index = angzarr::storage::SourceInfo {
        command_index: source_info.command_index + 1,
        ..source_info.clone()
    };
    let result_wrong = store
        .find_by_source(domain, "angzarr", root, &mismatched_index)
        .await
        .expect("find_by_source should succeed");
    assert!(
        result_wrong.is_none(),
        "different command_index must not match — backend isn't storing command_index correctly"
    );
}

/// Main-timeline source round trip: a claim whose source cover spells the
/// main timeline as `""` (how covers actually arrive — `Cover.edition` is
/// `None` on the main timeline) must persist and be found again. Pins the
/// write-side polarity: SQLite's insert used to gate the source columns on
/// `!source_edition.is_empty()`, silently dropping every main-timeline
/// claim while the `"angzarr"`-spelled tests above kept passing (C-15 class).
pub async fn test_find_by_source_round_trip_main_timeline_source<S: EventStore>(store: &S) {
    let domain = "test_find_src_main_tl";
    let root = Uuid::new_v4();
    let source_root = Uuid::new_v4();

    let source_info = angzarr::storage::SourceInfo {
        domain: "orders".to_string(),
        edition: String::new(),
        root: source_root,
        seq: 3,
        component: "saga-orders-test".to_string(),
        command_index: 0,
        kind: angzarr::storage::ProvenanceKind::Command,
    };

    store
        .add(
            domain,
            "angzarr",
            root,
            vec![make_event(0, "MainTimelineSourced")],
            &AddMeta {
                source_info: Some(&source_info),
                ..Default::default()
            },
        )
        .await
        .expect("add with main-timeline source_info should succeed");

    let result = store
        .find_by_source(domain, "angzarr", root, &source_info)
        .await
        .expect("find_by_source should succeed");
    assert!(
        result.is_some(),
        "main-timeline (\"\") source claim was dropped at add() time — \
         write-side edition polarity bug"
    );
}

/// The deferred-idempotency key includes what the provenance tuple is
/// attached to: a command, a rejection-notification or a
/// compensate-notification. A Compensate notification carries the provenance
/// tuple of the command it undoes and must never be dropped as a duplicate
/// of that command (compensation_delivery.feature C-0473).
pub async fn test_find_by_source_distinguishes_provenance_kind<S: EventStore>(store: &S) {
    use angzarr::storage::ProvenanceKind;
    let domain = "test_find_src_kind";
    let root = Uuid::new_v4();
    let command_claim = angzarr::storage::SourceInfo::new(
        "angzarr",
        "orders",
        Uuid::new_v4(),
        3,
        "OrderFulfillment",
        0,
    );
    let compensate_claim = command_claim
        .clone()
        .with_kind(ProvenanceKind::CompensateNotification);
    let rejection_claim = command_claim
        .clone()
        .with_kind(ProvenanceKind::RejectionNotification);

    store
        .add(
            domain,
            "angzarr",
            root,
            vec![make_event(0, "StockReserved")],
            &AddMeta {
                source_info: Some(&command_claim),
                ..Default::default()
            },
        )
        .await
        .expect("command's events should persist");

    for claim in [&compensate_claim, &rejection_claim] {
        let premature = store
            .find_by_source(domain, "angzarr", root, claim)
            .await
            .expect("find_by_source should succeed");
        assert!(
            premature.is_none(),
            "a {:?} claim matched the command's claim with the same tuple",
            claim.kind
        );
    }

    store
        .add(
            domain,
            "angzarr",
            root,
            vec![make_event(1, "StockReleased")],
            &AddMeta {
                source_info: Some(&compensate_claim),
                ..Default::default()
            },
        )
        .await
        .expect("compensation's events should persist");

    let command_pages = store
        .find_by_source(domain, "angzarr", root, &command_claim)
        .await
        .expect("find_by_source should succeed")
        .expect("command claim should be found");
    let compensate_pages = store
        .find_by_source(domain, "angzarr", root, &compensate_claim)
        .await
        .expect("find_by_source should succeed")
        .expect("compensate claim should be found");
    assert_eq!(command_pages.len(), 1);
    assert_eq!(command_pages[0].sequence_num(), 0);
    assert_eq!(compensate_pages.len(), 1);
    assert_eq!(compensate_pages[0].sequence_num(), 1);
    assert!(store
        .find_by_source(domain, "angzarr", root, &rejection_claim)
        .await
        .expect("find_by_source should succeed")
        .is_none());
}

/// O1 collision regression at the storage contract level: two commands of ONE
/// invocation — identical (source edition/domain/root/seq), differing only in
/// command_index — persist as DISTINCT idempotency claims and each lookup
/// returns only its own events. Pre-fix the key excluded command_index, so the
/// second command's lookup matched the first's events and the pipeline
/// swallowed the second command as an already-processed duplicate.
pub async fn test_find_by_source_distinguishes_invocation_commands<S: EventStore>(store: &S) {
    let domain = "test_find_src_o1";
    let root = Uuid::new_v4();
    let source_root = Uuid::new_v4();

    let claim_first = angzarr::storage::SourceInfo {
        domain: "orders".to_string(),
        edition: "angzarr".to_string(),
        root: source_root,
        seq: 7,
        component: "pm-fulfillment".to_string(),
        command_index: 0,
        kind: angzarr::storage::ProvenanceKind::Command,
    };
    let claim_second = angzarr::storage::SourceInfo {
        command_index: 1,
        ..claim_first.clone()
    };

    store
        .add(
            domain,
            "angzarr",
            root,
            vec![make_event(0, "FirstCommandEvent")],
            &AddMeta {
                source_info: Some(&claim_first),
                ..Default::default()
            },
        )
        .await
        .expect("first command's events should persist");

    // Before the second command executes, its idempotency lookup must MISS:
    // this is exactly the lookup that pre-fix returned the first command's
    // events and silently dropped the second command.
    let premature = store
        .find_by_source(domain, "angzarr", root, &claim_second)
        .await
        .expect("find_by_source should succeed");
    assert!(
        premature.is_none(),
        "second command of the invocation matched the FIRST command's claim — \
         command_index is not part of the idempotency key (O1 collision)"
    );

    store
        .add(
            domain,
            "angzarr",
            root,
            vec![make_event(1, "SecondCommandEvent")],
            &AddMeta {
                source_info: Some(&claim_second),
                ..Default::default()
            },
        )
        .await
        .expect("second command's events should persist");

    // Each claim now resolves to exactly its own single event.
    let first = store
        .find_by_source(domain, "angzarr", root, &claim_first)
        .await
        .expect("find_by_source should succeed")
        .expect("first claim should be found");
    let second = store
        .find_by_source(domain, "angzarr", root, &claim_second)
        .await
        .expect("find_by_source should succeed")
        .expect("second claim should be found");
    assert_eq!(first.len(), 1, "first claim must return only its own event");
    assert_eq!(
        second.len(),
        1,
        "second claim must return only its own event"
    );
}

/// An aggregate history larger than one backend result page (DynamoDB pages
/// at 1 MB) must be read completely by every read path, and the external-id
/// probe must see claims written before the page boundary.
pub async fn test_history_larger_than_one_result_page<S: EventStore>(store: &S) {
    let domain = "test_large_history";
    let root = Uuid::new_v4();
    // 1200 x 1 KiB = 1.2 MB: past one DynamoDB page, with rows small enough
    // for ImmuDB's single-statement inserts. Batches of 150 exceed one
    // DynamoDB transaction (100 items).
    const EVENTS: u32 = 1200;
    const BATCH: u32 = 150;
    const PAYLOAD: usize = 1024;

    let event = |seq: u32| {
        let mut page = make_event(seq, "Bulky");
        if let Some(event_page::Payload::Event(ref mut any)) = page.payload {
            any.value = vec![(seq % 251) as u8; PAYLOAD];
        }
        page
    };

    for batch_start in (0..EVENTS).step_by(BATCH as usize) {
        let external_id = format!("bulk-{batch_start}");
        store
            .add(
                domain,
                "angzarr",
                root,
                (batch_start..batch_start + BATCH).map(event).collect(),
                &AddMeta {
                    external_id: Some(&external_id),
                    ..Default::default()
                },
            )
            .await
            .expect("bulky add should succeed");
    }

    let all = store
        .get(domain, "angzarr", root)
        .await
        .expect("get should succeed");
    let seqs: Vec<u32> = all.iter().map(|e| e.sequence_num()).collect();
    assert_eq!(
        seqs,
        (0..EVENTS).collect::<Vec<_>>(),
        "get must return every event"
    );
    for page in &all {
        match &page.payload {
            Some(event_page::Payload::Event(any)) => assert_eq!(
                any.value,
                vec![(page.sequence_num() % 251) as u8; PAYLOAD],
                "payload must round-trip intact"
            ),
            other => panic!("unexpected payload {other:?}"),
        }
    }

    let tail = store
        .get_from(domain, "angzarr", root, 6)
        .await
        .expect("get_from should succeed");
    assert_eq!(tail.len(), 1194, "get_from must return the whole tail");

    let range = store
        .get_from_to(domain, "angzarr", root, 1, 1199)
        .await
        .expect("get_from_to should succeed");
    assert_eq!(range.len(), 1198, "get_from_to must return the whole range");

    let duplicate = store
        .add(
            domain,
            "angzarr",
            root,
            vec![event(EVENTS)],
            &AddMeta {
                external_id: Some("bulk-0"),
                ..Default::default()
            },
        )
        .await
        .expect("duplicate add should succeed");
    assert_eq!(
        duplicate,
        AddOutcome::Duplicate {
            first_sequence: 0,
            last_sequence: BATCH - 1
        },
        "the external-id probe must find the first batch's claim"
    );

    assert_eq!(
        store
            .get_next_sequence(domain, "angzarr", root)
            .await
            .expect("get_next_sequence should succeed"),
        EVENTS
    );
    assert!(
        store
            .list_roots(domain, "angzarr")
            .await
            .expect("list_roots should succeed")
            .contains(&root),
        "list_roots must see the aggregate"
    );
}

/// Every batch must continue the stream exactly: a first sequence past the
/// next free one, or a gap inside the batch, is a sequence conflict.
pub async fn test_add_rejects_sequence_gaps<S: EventStore>(store: &S) {
    let domain = "test_seq_gaps";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "angzarr",
            root,
            make_events(0, 2),
            &AddMeta::default(),
        )
        .await
        .expect("initial add should succeed");

    let skipped = store
        .add(
            domain,
            "angzarr",
            root,
            vec![make_event(3, "Skipped")],
            &AddMeta::default(),
        )
        .await;
    assert!(
        matches!(
            skipped,
            Err(StorageError::SequenceConflict {
                expected: 2,
                actual: 3
            })
        ),
        "a first sequence past the next free one must conflict; got {skipped:?}"
    );

    let holed = store
        .add(
            domain,
            "angzarr",
            root,
            vec![make_event(2, "A"), make_event(4, "B")],
            &AddMeta::default(),
        )
        .await;
    assert!(
        matches!(
            holed,
            Err(StorageError::SequenceConflict {
                expected: 3,
                actual: 4
            })
        ),
        "a gap inside the batch must conflict; got {holed:?}"
    );

    let events = store.get(domain, "angzarr", root).await.unwrap();
    assert_eq!(
        events.iter().map(|e| e.sequence_num()).collect::<Vec<_>>(),
        vec![0, 1],
        "rejected batches must write nothing"
    );
}

/// A new edition may branch below the main timeline's head: its first event
/// sits at the divergence point, and reads compose the main prefix with it.
pub async fn test_edition_first_write_branches_below_main_head<S: EventStore>(store: &S) {
    let domain = "test_branch_below_head";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "angzarr",
            root,
            make_events(0, 5),
            &AddMeta::default(),
        )
        .await
        .expect("main add should succeed");

    store
        .add(
            domain,
            "branch",
            root,
            vec![make_event(2, "Branch2"), make_event(3, "Branch3")],
            &AddMeta::default(),
        )
        .await
        .expect("a branch may start at a divergence point below the main head");

    let events = store.get(domain, "branch", root).await.unwrap();
    let shape: Vec<(u32, String)> = events
        .iter()
        .map(|e| {
            let type_url = match &e.payload {
                Some(event_page::Payload::Event(any)) => any.type_url.clone(),
                _ => String::new(),
            };
            (e.sequence_num(), type_url)
        })
        .collect();
    assert_eq!(
        shape,
        vec![
            (0, "type.example/Event0".to_string()),
            (1, "type.example/Event1".to_string()),
            (2, "type.example/Branch2".to_string()),
            (3, "type.example/Branch3".to_string()),
        ],
        "branch reads main[0,1] then its own events"
    );
    assert_eq!(
        store
            .get_next_sequence(domain, "branch", root)
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        store
            .get_next_sequence(domain, "angzarr", root)
            .await
            .unwrap(),
        5,
        "the branch must not move the main timeline"
    );
}

/// A new edition cannot start past the main timeline's next sequence (that
/// would leave a gap between the inherited prefix and the branch).
pub async fn test_edition_first_write_past_main_head_rejected<S: EventStore>(store: &S) {
    let domain = "test_branch_past_head";
    let root = Uuid::new_v4();

    store
        .add(
            domain,
            "angzarr",
            root,
            make_events(0, 3),
            &AddMeta::default(),
        )
        .await
        .expect("main add should succeed");

    let result = store
        .add(
            domain,
            "branch",
            root,
            vec![make_event(5, "TooFar")],
            &AddMeta::default(),
        )
        .await;
    assert!(
        matches!(
            result,
            Err(StorageError::SequenceConflict {
                expected: 3,
                actual: 5
            })
        ),
        "a branch must start at or below main's next sequence; got {result:?}"
    );
}

// =============================================================================
// Concurrent writes — sequence integrity under race
// =============================================================================
//
// Bug C-19: DynamoDB/Bigtable/ImmuDB EventStore backends compute the next
// sequence via a read-then-write pattern with no conditional write or
// transactional fence. Concurrent writers on the same `(domain, edition, root)`
// can both observe `get_next_sequence() == N`, then both write seq=N — the
// second write OVERWRITES the first (Dynamo/Bigtable) or duplicates the row
// (ImmuDB without PRIMARY KEY enforcement on its writes), corrupting the
// event stream.
//
// SQLite and Postgres pass this test because they use `BEGIN IMMEDIATE` /
// proper transactions plus a PRIMARY KEY on `(domain, edition, root, sequence)`:
// the race is serialized at the storage layer and a colliding insert fails
// with a UNIQUE-violation that surfaces as `StorageError`.
//
// The test contract is: when N concurrent writers each pick the next free
// sequence and call `add()`, the store MUST contain exactly N events at the
// expected `0..N` sequences (each writer retries on its own conflict). A
// store that silently overwrites or duplicates will fail one of:
//   - `events.len() == N` (overwrite collapses to <N events)
//   - the sequence set equals `{0, 1, ..., N-1}` (duplicate sequences or gaps)
//   - per-sequence event-type uniqueness (overwrite changes content under
//     a sequence "owned" by an earlier writer).

/// Concurrent writes must yield exactly N distinct sequences with no
/// overwrites or duplicates.
///
/// Each writer:
///   1. Reads `get_next_sequence` to pick the next available slot.
///   2. Constructs an event at that slot.
///   3. Calls `add()`. A lost race must be `StorageError::SequenceConflict`
///      (retryable), on which the writer retries; any other error fails.
///
/// After all writers complete, the aggregate must have exactly N events at
/// sequences `0..N`, and every sequence must contain the writer's unique
/// payload (no silent overwrites).
pub async fn test_add_concurrent_writes_unique_sequences<S>(store: std::sync::Arc<S>)
where
    S: EventStore + 'static,
{
    let domain = "test_concurrent_seq";
    let root = Uuid::new_v4();
    // N is small enough to keep the test fast, large enough to provoke
    // races on backends without transactional fencing.
    const N: u32 = 8;
    // Bounded retry budget per writer: prevents the test from hanging if a
    // backend is broken in a way that makes progress impossible. Real
    // contention should resolve in O(N) retries per writer.
    const MAX_RETRIES: u32 = 256;

    let mut handles = Vec::with_capacity(N as usize);
    for writer_id in 0..N {
        let store = std::sync::Arc::clone(&store);
        let domain = domain.to_string();
        handles.push(tokio::spawn(async move {
            for _attempt in 0..MAX_RETRIES {
                // Read the next free sequence.
                let next = store
                    .get_next_sequence(&domain, "test", root)
                    .await
                    .expect("get_next_sequence must not fail under contention");
                // Tag the event payload with this writer's id so an overwrite
                // is detectable post-hoc by inspecting the stored payload.
                let event_type = format!("Writer{}@Seq{}", writer_id, next);
                let event = make_event(next, &event_type);
                match store
                    .add(
                        &domain,
                        "test",
                        root,
                        vec![event],
                        &AddMeta {
                            correlation_id: "",
                            external_id: None,
                            source_info: None,
                            ext: None,
                        },
                    )
                    .await
                {
                    Ok(_) => return Some((writer_id, next, event_type)),
                    // A lost race must surface as a retryable sequence
                    // conflict; any other error (e.g. a raw unique-key
                    // violation) fails the contract.
                    Err(StorageError::SequenceConflict { .. }) => continue,
                    Err(other) => panic!(
                        "writer {writer_id}: a lost race must be SequenceConflict, got {other:?}"
                    ),
                }
            }
            None
        }));
    }

    let mut successes: Vec<(u32, u32, String)> = Vec::new();
    for h in handles {
        match h.await {
            Ok(Some(record)) => successes.push(record),
            Ok(None) => {}
            Err(join_error) => std::panic::resume_unwind(join_error.into_panic()),
        }
    }

    assert_eq!(
        successes.len(),
        N as usize,
        "all {} writers should have eventually allocated a sequence (got {})",
        N,
        successes.len(),
    );

    // Read back the persisted stream.
    let events = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed");

    // Property 1: row count must match successful writers.
    // An overwrite-on-race backend will have fewer rows than N.
    assert_eq!(
        events.len(),
        N as usize,
        "store contains {} events but {} writers succeeded — overwrite or duplicate on concurrent add()",
        events.len(),
        N,
    );

    // Property 2: sequences must be exactly {0, 1, ..., N-1} with no
    // duplicates and no gaps. A backend that allows two writes at the same
    // sequence will either show duplicates or, if it deduped on PRIMARY KEY,
    // gaps where the loser was discarded.
    let mut seqs: Vec<u32> = events.iter().map(|e| e.sequence_num()).collect();
    seqs.sort_unstable();
    let unique_seqs: std::collections::HashSet<_> = seqs.iter().copied().collect();
    assert_eq!(
        unique_seqs.len(),
        seqs.len(),
        "duplicate sequences in store: {:?}",
        seqs,
    );
    assert_eq!(
        seqs,
        (0..N).collect::<Vec<_>>(),
        "sequences should be {:?} but got {:?}",
        (0..N).collect::<Vec<_>>(),
        seqs,
    );

    // Property 3: each sequence must contain the payload from the writer
    // that claimed it. If a later writer overwrote an earlier write at the
    // same sequence (the silent-overwrite bug on Dynamo/Bigtable), the
    // stored type_url will not match the writer's `Writer{i}@Seq{n}` tag
    // for the writer recorded in `successes`.
    let stored_types_by_seq: std::collections::HashMap<u32, String> = events
        .iter()
        .filter_map(|e| {
            let seq = e.sequence_num();
            e.payload.as_ref().and_then(|p| match p {
                angzarr::proto::event_page::Payload::Event(any) => {
                    // type_url format from make_event: "type.example/{event_type}"
                    let suffix = any.type_url.rsplit('/').next().unwrap_or("");
                    Some((seq, suffix.to_string()))
                }
                _ => None,
            })
        })
        .collect();

    for (writer_id, seq, expected_type) in &successes {
        let stored = stored_types_by_seq.get(seq).unwrap_or_else(|| {
            panic!(
                "writer {} reported success at seq {} but no event found at that sequence",
                writer_id, seq
            )
        });
        assert_eq!(
            stored, expected_type,
            "writer {} reported success at seq {} with payload {:?}, \
             but store has {:?} at that sequence — silent overwrite on concurrent add()",
            writer_id, seq, expected_type, stored,
        );
    }
}

// =============================================================================
// Test generator macros (T11)
// =============================================================================
//
// One `#[tokio::test]` is generated PER contract fn, so a failure in one
// contract surfaces individually instead of fail-fasting the rest of the
// suite inside a single mega-test. `$fixture` is an async fn returning a
// fresh store handle; each generated test calls it once. Backends with
// expensive setup share a container behind the fixture (see
// storage_postgres.rs / storage_immudb.rs); SQLite builds a fresh
// in-memory store per test.

/// Emit one `#[tokio::test]` per listed contract fn.
#[doc(hidden)]
#[macro_export]
macro_rules! __gen_event_store_tests {
    ($fixture:path, $($name:ident),+ $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                let store = $fixture().await;
                $crate::storage::event_store_tests::$name(&store).await;
            }
        )+
    };
}

/// Generate the CORE EventStore contract tests — everything except the
/// `delete_edition_events` group, which is split into
/// `generate_event_store_delete_tests!` because ImmuDB is append-only and
/// asserts NotImplemented in its own suite instead. Fully-featured backends
/// should invoke `generate_event_store_tests!`, which composes both groups.
#[macro_export]
macro_rules! generate_event_store_core_tests {
    ($fixture:path) => {
        $crate::__gen_event_store_tests!(
            $fixture,
            // add tests
            test_add_single_event,
            test_add_multiple_events,
            test_add_empty_events,
            test_add_sequential_batches,
            test_add_sequence_conflict,
            test_add_duplicate_sequence,
            test_add_rejects_duplicate_sequences,
            test_add_rejects_sequence_gaps,
            // get tests
            test_get_all_events,
            test_get_empty_aggregate,
            test_get_preserves_event_data,
            // get_from tests
            test_get_from_zero,
            test_get_from_middle,
            test_get_from_end,
            test_get_from_last,
            // get_from_to tests
            test_get_from_to_full_range,
            test_get_from_to_partial,
            test_get_from_to_single,
            test_get_from_to_empty,
            test_get_from_to_zero_to,
            // list_roots tests
            test_list_roots_single,
            test_list_roots_multiple,
            test_list_roots_empty_domain,
            test_list_roots_domain_isolation,
            // list_domains tests
            test_list_domains_contains,
            test_list_domains_multiple,
            // get_next_sequence tests
            test_get_next_sequence_empty,
            test_get_next_sequence_after_events,
            test_get_next_sequence_increments,
            // integration tests
            test_aggregate_isolation,
            test_large_batch,
            // correlation_id tests
            test_correlation_id_query,
            test_correlation_id_empty_query,
            test_correlation_id_query_main_timeline_null_edition,
            test_correlation_id_preserved,
            test_cover_ext_round_trips,
            // edition tests
            test_edition_isolation,
            test_edition_sequences_independent,
            test_edition_divergence_read,
            test_edition_divergence_from_middle,
            test_edition_divergence_get_from,
            test_edition_get_from_to_includes_main_prefix,
            test_edition_get_until_timestamp_includes_main_prefix,
            test_eventless_edition_inherits_main_timeline,
            test_edition_filtered_roots,
            test_edition_explicit_divergence_new_branch,
            test_edition_first_write_branches_below_main_head,
            test_edition_first_write_past_main_head_rejected,
            // main-timeline sentinel polarity tests (C-15)
            test_main_timeline_sentinel_write_empty_read_both,
            test_main_timeline_sentinel_write_angzarr_read_both,
            test_main_timeline_external_id_sentinel_polarity,
            // idempotency tests
            test_add_with_external_id_returns_duplicate,
            test_add_different_external_ids_allowed,
            // timestamp tests
            test_get_until_timestamp_filters,
            test_get_until_timestamp_returns_all_when_recent,
            test_get_until_timestamp_nanosecond_boundary_precision,
            test_timestamp_preservation,
            test_add_stamps_created_at_on_unstamped_pages,
            // large scale tests
            test_large_aggregate_10k,
            test_history_larger_than_one_result_page,
            // find_by_source / find_by_external_id tests (C-18 round trips)
            test_find_by_source_returns_match,
            test_find_by_source_no_match,
            test_find_by_source_round_trip,
            test_find_by_source_round_trip_main_timeline_source,
            test_find_by_source_distinguishes_invocation_commands,
            test_find_by_source_distinguishes_provenance_kind,
            test_find_by_external_id_round_trip,
            test_find_by_external_id_no_match,
            test_find_by_external_id_empty_returns_none,
        );
    };
}

/// `delete_edition_events` group — only for backends with hard-delete
/// support. ImmuDB is append-only: its suite asserts NotImplemented via
/// `test_immudb_delete_not_supported` instead of generating these.
#[macro_export]
macro_rules! generate_event_store_delete_tests {
    ($fixture:path) => {
        $crate::__gen_event_store_tests!(
            $fixture,
            test_delete_edition_events_rejects_main_timeline_sentinels,
            test_delete_edition_events_removes_all,
            test_delete_edition_events_scoped,
        );
    };
}

/// Generate ALL EventStore contract tests (core + delete) — the
/// full contract for fully-featured backends (SQLite, Postgres).
#[macro_export]
macro_rules! generate_event_store_tests {
    ($fixture:path) => {
        $crate::generate_event_store_core_tests!($fixture);
        $crate::generate_event_store_delete_tests!($fixture);
    };
}

/// Run concurrent-write contract tests against a store implementation.
///
/// Separate from `run_event_store_tests!` because the concurrent test
/// requires an owned `Arc<S>` to satisfy `tokio::spawn`'s `'static` bound,
/// whereas the rest of the suite takes a borrowed `&S`. Invoke after the
/// main suite, with a freshly-built store to avoid coupling to prior state.
///
/// Bug context: C-19 in `plans/deep-review-remediation.md`.
#[macro_export]
macro_rules! run_event_store_concurrent_tests {
    ($arc_store:expr) => {
        use $crate::storage::event_store_tests::*;

        test_add_concurrent_writes_unique_sequences($arc_store).await;
        println!("  test_add_concurrent_writes_unique_sequences: PASSED");
    };
}

/// T12: every contract fn in this module must be wired somewhere — see
/// `crate::storage::assert_contract_inventory`.
#[test]
fn event_store_contract_inventory_is_fully_wired() {
    crate::storage::assert_contract_inventory(
        include_str!("event_store_tests.rs"),
        "// Test generator macros (T11)",
        &[
            include_str!("../storage_sqlite.rs"),
            include_str!("../storage_postgres.rs"),
            include_str!("../storage_immudb.rs"),
            include_str!("../storage_redis.rs"),
            include_str!("../storage_mock.rs"),
            include_str!("../storage_dynamo.rs"),
            include_str!("../storage_bigtable.rs"),
        ],
        &[],
    );
}
