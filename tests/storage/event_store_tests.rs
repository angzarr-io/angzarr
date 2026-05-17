//! EventStore interface tests.
//!
//! These tests verify the contract of the EventStore trait.
//! Each storage implementation should run these tests.

use prost_types::Any;
use uuid::Uuid;

use angzarr::proto::{event_page, page_header::SequenceType, EventPage, PageHeader};
use angzarr::proto_ext::EventPageExt;
use angzarr::storage::EventStore;

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
        ..Default::default()
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
            "",
            None,
            None,
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
        .add(domain, "test", root, make_events(0, 5), "", None, None)
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
        .add(domain, "test", root, vec![], "", None, None)
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
        .add(domain, "test", root, make_events(0, 2), "", None, None)
        .await
        .expect("first batch should succeed");

    // Second batch: events 2, 3, 4
    store
        .add(domain, "test", root, make_events(2, 3), "", None, None)
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
        .add(domain, "test", root, make_events(0, 3), "", None, None)
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
            "",
            None,
            None,
        )
        .await;
    assert!(result.is_err(), "sequence lower than current should fail");
}

pub async fn test_add_duplicate_sequence<S: EventStore>(store: &S) {
    let domain = "test_dup_seq";
    let root = Uuid::new_v4();

    store
        .add(domain, "test", root, make_events(0, 3), "", None, None)
        .await
        .expect("first add should succeed");

    // Try to add at sequence 0 again
    let result = store
        .add(
            domain,
            "test",
            root,
            vec![make_event(0, "Dup")],
            "",
            None,
            None,
        )
        .await;
    assert!(result.is_err(), "duplicate sequence should fail");
}

// =============================================================================
// EventStore::get tests
// =============================================================================

pub async fn test_get_all_events<S: EventStore>(store: &S) {
    let domain = "test_get_all";
    let root = Uuid::new_v4();

    store
        .add(domain, "test", root, make_events(0, 10), "", None, None)
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
        ..Default::default()
    };

    store
        .add(domain, "test", root, vec![original], "", None, None)
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
        .add(domain, "test", root, make_events(0, 5), "", None, None)
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
        .add(domain, "test", root, make_events(0, 10), "", None, None)
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
        .add(domain, "test", root, make_events(0, 5), "", None, None)
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
        .add(domain, "test", root, make_events(0, 5), "", None, None)
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
        .add(domain, "test", root, make_events(0, 5), "", None, None)
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
        .add(domain, "test", root, make_events(0, 10), "", None, None)
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
        .add(domain, "test", root, make_events(0, 5), "", None, None)
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
        .add(domain, "test", root, make_events(0, 5), "", None, None)
        .await
        .expect("add should succeed");

    let events = store
        .get_from_to(domain, "test", root, 100, 200)
        .await
        .expect("get_from_to should succeed");
    assert!(events.is_empty(), "out of range should be empty");
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
            "",
            None,
            None,
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
            "",
            None,
            None,
        )
        .await
        .unwrap();
    store
        .add(
            domain,
            "test",
            root2,
            vec![make_event(0, "E2")],
            "",
            None,
            None,
        )
        .await
        .unwrap();
    store
        .add(
            domain,
            "test",
            root3,
            vec![make_event(0, "E3")],
            "",
            None,
            None,
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
            "",
            None,
            None,
        )
        .await
        .unwrap();
    store
        .add(
            domain2,
            "test",
            root2,
            vec![make_event(0, "E2")],
            "",
            None,
            None,
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
            "",
            None,
            None,
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
                "",
                None,
                None,
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
        .add(domain, "test", root, make_events(0, 7), "", None, None)
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
            "",
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        store.get_next_sequence(domain, "test", root).await.unwrap(),
        1
    );

    store
        .add(domain, "test", root, make_events(1, 3), "", None, None)
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
        .add(domain, "test", root1, make_events(0, 3), "", None, None)
        .await
        .unwrap();
    store
        .add(domain, "test", root2, make_events(0, 5), "", None, None)
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
        .add(domain, "test", root, make_events(0, 100), "", None, None)
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
            &correlation_id,
            None,
            None,
        )
        .await
        .expect("add should succeed");

    store
        .add(
            domain2,
            "test",
            root2,
            vec![make_event(0, "E2")],
            &correlation_id,
            None,
            None,
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
            &correlation_id,
            None,
            None,
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
            "",
            None,
            None,
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
            "",
            None,
            None,
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
        .add(domain, "angzarr", root, make_events(0, 3), "", None, None)
        .await
        .expect("add should succeed");

    // Add 5 events to v2 edition (sequence starts at 0)
    store
        .add(domain, "v2", root, make_events(0, 5), "", None, None)
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
        .add(domain, "angzarr", root, make_events(0, 3), "", None, None)
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
            "",
            None,
            None,
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
        .add(domain, "angzarr", root, make_events(0, 5), "", None, None)
        .await
        .expect("add to main should succeed");

    // Branch diverges at seq 3
    store
        .add(
            domain,
            "mid-branch",
            root,
            make_events(3, 2),
            "",
            None,
            None,
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
        .add(domain, "angzarr", root, make_events(0, 5), "", None, None)
        .await
        .expect("add to main should succeed");

    // Branch diverges at 2
    store
        .add(
            domain,
            "from-branch",
            root,
            make_events(2, 3),
            "",
            None,
            None,
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
        .add(domain, "angzarr", root, make_events(0, 5), "", None, None)
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
            "",
            None,
            None,
        )
        .await
        .unwrap();
    store
        .add(
            domain,
            "v2",
            root_v2,
            vec![make_event(0, "V2")],
            "",
            None,
            None,
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
            "",
            Some("ext-123"),
            None,
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
            make_events(3, 2), // Different events
            "",
            Some("ext-123"), // Same external_id
            None,
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
            "",
            Some("ext-aaa"),
            None,
        )
        .await
        .expect("first add should succeed");

    store
        .add(
            domain,
            "test",
            root,
            make_events(2, 2),
            "",
            Some("ext-bbb"),
            None,
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
        ..Default::default()
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
        ..Default::default()
    };

    store
        .add(
            domain,
            "test",
            root,
            vec![event_old, event_new],
            "",
            None,
            None,
        )
        .await
        .expect("add should succeed");

    // Query with timestamp between old and new
    let until = "2024-01-01T00:00:00Z"; // After old, before new
    let filtered = store
        .get_until_timestamp(domain, "test", root, until)
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
        ..Default::default()
    };

    store
        .add(domain, "test", root, vec![event], "", None, None)
        .await
        .expect("add should succeed");

    // Query with timestamp far in the future
    let until = "2030-01-01T00:00:00Z";
    let all = store
        .get_until_timestamp(domain, "test", root, until)
        .await
        .expect("get_until_timestamp should succeed");

    assert_eq!(all.len(), 1, "should return all events");
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
        ..Default::default()
    };

    store
        .add(domain, "test", root, vec![event], "", None, None)
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
                "",
                None,
                None,
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
        .add(domain, "branch-1", root1, make_events(0, 3), "", None, None)
        .await
        .expect("add should succeed");
    store
        .add(domain, "branch-1", root2, make_events(0, 2), "", None, None)
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
            "",
            None,
            None,
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
            "",
            None,
            None,
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
    };

    store
        .add(
            domain,
            "angzarr",
            root,
            vec![make_event(0, "Derived")],
            "",
            None,
            Some(&source_info),
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
    };

    store
        .add(
            domain,
            "angzarr",
            root,
            vec![make_event(0, "NoSource")],
            "",
            None,
            None,
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
// query_stale_cascades tests
// =============================================================================

/// Create a test event with cascade tracking fields.
pub fn make_cascade_event(
    seq: u32,
    no_commit: bool,
    cascade_id: Option<&str>,
    timestamp_secs: i64,
) -> EventPage {
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(seq)),
        }),
        created_at: Some(prost_types::Timestamp {
            seconds: timestamp_secs,
            nanos: 0,
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: format!("type.example/CascadeEvent{}", seq),
            value: vec![seq as u8],
        })),
        no_commit,
        cascade_id: cascade_id.map(String::from),
    }
}

pub async fn test_query_stale_cascades_finds_old_uncommitted<S: EventStore>(store: &S) {
    let domain = "test_stale_cascade";
    let root = Uuid::new_v4();

    // Add uncommitted event from 2 hours ago
    let old_time = chrono::Utc::now() - chrono::Duration::hours(2);
    let event = make_cascade_event(0, true, Some("cascade-stale-1"), old_time.timestamp());

    store
        .add(domain, "angzarr", root, vec![event], "", None, None)
        .await
        .expect("add should succeed");

    // Query with 1-hour threshold
    let threshold = (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
    let stale = store
        .query_stale_cascades(&threshold)
        .await
        .expect("query should succeed");

    assert!(
        stale.contains(&"cascade-stale-1".to_string()),
        "should find stale cascade"
    );
}

pub async fn test_query_stale_cascades_ignores_resolved<S: EventStore>(store: &S) {
    let domain = "test_resolved_cascade";
    let root = Uuid::new_v4();

    // Add uncommitted event from 2 hours ago
    let old_time = chrono::Utc::now() - chrono::Duration::hours(2);
    let uncommitted = make_cascade_event(0, true, Some("cascade-resolved-1"), old_time.timestamp());

    store
        .add(domain, "angzarr", root, vec![uncommitted], "", None, None)
        .await
        .expect("add should succeed");

    // Add committed event with same cascade_id (resolves the cascade)
    let committed = make_cascade_event(
        1,
        false,
        Some("cascade-resolved-1"),
        chrono::Utc::now().timestamp(),
    );
    store
        .add(domain, "angzarr", root, vec![committed], "", None, None)
        .await
        .expect("add should succeed");

    // Query with 1-hour threshold
    let threshold = (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
    let stale = store
        .query_stale_cascades(&threshold)
        .await
        .expect("query should succeed");

    assert!(
        !stale.contains(&"cascade-resolved-1".to_string()),
        "resolved cascade should not be stale"
    );
}

pub async fn test_query_stale_cascades_ignores_fresh<S: EventStore>(store: &S) {
    let domain = "test_fresh_cascade";
    let root = Uuid::new_v4();

    // Add uncommitted event from just now
    let event = make_cascade_event(
        0,
        true,
        Some("cascade-fresh-1"),
        chrono::Utc::now().timestamp(),
    );

    store
        .add(domain, "angzarr", root, vec![event], "", None, None)
        .await
        .expect("add should succeed");

    // Query with 1-hour threshold
    let threshold = (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
    let stale = store
        .query_stale_cascades(&threshold)
        .await
        .expect("query should succeed");

    assert!(
        !stale.contains(&"cascade-fresh-1".to_string()),
        "fresh cascade should not be stale"
    );
}

// =============================================================================
// query_cascade_participants tests
// =============================================================================

pub async fn test_query_cascade_participants_finds_uncommitted<S: EventStore>(store: &S) {
    let domain = "test_cascade_parts";
    let root = Uuid::new_v4();

    // Add uncommitted events with cascade_id
    let event1 = make_cascade_event(
        0,
        true,
        Some("cascade-parts-1"),
        chrono::Utc::now().timestamp(),
    );
    let event2 = make_cascade_event(
        1,
        true,
        Some("cascade-parts-1"),
        chrono::Utc::now().timestamp(),
    );

    store
        .add(
            domain,
            "angzarr",
            root,
            vec![event1, event2],
            "",
            None,
            None,
        )
        .await
        .expect("add should succeed");

    let participants = store
        .query_cascade_participants("cascade-parts-1")
        .await
        .expect("query should succeed");

    assert_eq!(participants.len(), 1, "should find one participant");
    assert_eq!(participants[0].domain, domain);
    assert_eq!(participants[0].root, root);
    assert_eq!(
        participants[0].sequences.len(),
        2,
        "should have 2 sequences"
    );
}

pub async fn test_query_cascade_participants_ignores_committed<S: EventStore>(store: &S) {
    let domain = "test_cascade_committed";
    let root = Uuid::new_v4();

    // Add committed event (should not be returned as participant)
    let event = make_cascade_event(
        0,
        false,
        Some("cascade-committed-1"),
        chrono::Utc::now().timestamp(),
    );

    store
        .add(domain, "angzarr", root, vec![event], "", None, None)
        .await
        .expect("add should succeed");

    let participants = store
        .query_cascade_participants("cascade-committed-1")
        .await
        .expect("query should succeed");

    assert!(
        participants.is_empty(),
        "committed events should not be participants"
    );
}

pub async fn test_query_cascade_participants_multiple_aggregates<S: EventStore>(store: &S) {
    let domain = "test_cascade_multi";
    let root1 = Uuid::new_v4();
    let root2 = Uuid::new_v4();

    // Add uncommitted events to two aggregates with same cascade_id
    let event1 = make_cascade_event(
        0,
        true,
        Some("cascade-multi-1"),
        chrono::Utc::now().timestamp(),
    );
    let event2 = make_cascade_event(
        0,
        true,
        Some("cascade-multi-1"),
        chrono::Utc::now().timestamp(),
    );

    store
        .add(domain, "angzarr", root1, vec![event1], "", None, None)
        .await
        .expect("add should succeed");
    store
        .add(domain, "angzarr", root2, vec![event2], "", None, None)
        .await
        .expect("add should succeed");

    let participants = store
        .query_cascade_participants("cascade-multi-1")
        .await
        .expect("query should succeed");

    assert_eq!(participants.len(), 2, "should find two participants");
}

// =============================================================================
// Test runner macro
// =============================================================================

/// Run all EventStore interface tests against a store implementation.
#[macro_export]
macro_rules! run_event_store_tests {
    ($store:expr) => {
        use $crate::storage::event_store_tests::*;

        // add tests
        test_add_single_event($store).await;
        println!("  test_add_single_event: PASSED");

        test_add_multiple_events($store).await;
        println!("  test_add_multiple_events: PASSED");

        test_add_empty_events($store).await;
        println!("  test_add_empty_events: PASSED");

        test_add_sequential_batches($store).await;
        println!("  test_add_sequential_batches: PASSED");

        test_add_sequence_conflict($store).await;
        println!("  test_add_sequence_conflict: PASSED");

        test_add_duplicate_sequence($store).await;
        println!("  test_add_duplicate_sequence: PASSED");

        // get tests
        test_get_all_events($store).await;
        println!("  test_get_all_events: PASSED");

        test_get_empty_aggregate($store).await;
        println!("  test_get_empty_aggregate: PASSED");

        test_get_preserves_event_data($store).await;
        println!("  test_get_preserves_event_data: PASSED");

        // get_from tests
        test_get_from_zero($store).await;
        println!("  test_get_from_zero: PASSED");

        test_get_from_middle($store).await;
        println!("  test_get_from_middle: PASSED");

        test_get_from_end($store).await;
        println!("  test_get_from_end: PASSED");

        test_get_from_last($store).await;
        println!("  test_get_from_last: PASSED");

        // get_from_to tests
        test_get_from_to_full_range($store).await;
        println!("  test_get_from_to_full_range: PASSED");

        test_get_from_to_partial($store).await;
        println!("  test_get_from_to_partial: PASSED");

        test_get_from_to_single($store).await;
        println!("  test_get_from_to_single: PASSED");

        test_get_from_to_empty($store).await;
        println!("  test_get_from_to_empty: PASSED");

        // list_roots tests
        test_list_roots_single($store).await;
        println!("  test_list_roots_single: PASSED");

        test_list_roots_multiple($store).await;
        println!("  test_list_roots_multiple: PASSED");

        test_list_roots_empty_domain($store).await;
        println!("  test_list_roots_empty_domain: PASSED");

        test_list_roots_domain_isolation($store).await;
        println!("  test_list_roots_domain_isolation: PASSED");

        // list_domains tests
        test_list_domains_contains($store).await;
        println!("  test_list_domains_contains: PASSED");

        test_list_domains_multiple($store).await;
        println!("  test_list_domains_multiple: PASSED");

        // get_next_sequence tests
        test_get_next_sequence_empty($store).await;
        println!("  test_get_next_sequence_empty: PASSED");

        test_get_next_sequence_after_events($store).await;
        println!("  test_get_next_sequence_after_events: PASSED");

        test_get_next_sequence_increments($store).await;
        println!("  test_get_next_sequence_increments: PASSED");

        // integration tests
        test_aggregate_isolation($store).await;
        println!("  test_aggregate_isolation: PASSED");

        test_large_batch($store).await;
        println!("  test_large_batch: PASSED");

        // correlation_id tests
        test_correlation_id_query($store).await;
        println!("  test_correlation_id_query: PASSED");

        test_correlation_id_empty_query($store).await;
        println!("  test_correlation_id_empty_query: PASSED");

        test_correlation_id_preserved($store).await;
        println!("  test_correlation_id_preserved: PASSED");

        // edition tests
        test_edition_isolation($store).await;
        println!("  test_edition_isolation: PASSED");

        test_edition_sequences_independent($store).await;
        println!("  test_edition_sequences_independent: PASSED");

        test_edition_divergence_read($store).await;
        println!("  test_edition_divergence_read: PASSED");

        test_edition_divergence_from_middle($store).await;
        println!("  test_edition_divergence_from_middle: PASSED");

        test_edition_divergence_get_from($store).await;
        println!("  test_edition_divergence_get_from: PASSED");

        test_edition_filtered_roots($store).await;
        println!("  test_edition_filtered_roots: PASSED");

        test_edition_explicit_divergence_new_branch($store).await;
        println!("  test_edition_explicit_divergence_new_branch: PASSED");

        // idempotency tests
        test_add_with_external_id_returns_duplicate($store).await;
        println!("  test_add_with_external_id_returns_duplicate: PASSED");

        test_add_different_external_ids_allowed($store).await;
        println!("  test_add_different_external_ids_allowed: PASSED");

        // timestamp tests
        test_get_until_timestamp_filters($store).await;
        println!("  test_get_until_timestamp_filters: PASSED");

        test_get_until_timestamp_returns_all_when_recent($store).await;
        println!("  test_get_until_timestamp_returns_all_when_recent: PASSED");

        test_timestamp_preservation($store).await;
        println!("  test_timestamp_preservation: PASSED");

        // large scale tests
        test_large_aggregate_10k($store).await;
        println!("  test_large_aggregate_10k: PASSED");

        // delete_edition_events tests
        test_delete_edition_events_removes_all($store).await;
        println!("  test_delete_edition_events_removes_all: PASSED");

        test_delete_edition_events_scoped($store).await;
        println!("  test_delete_edition_events_scoped: PASSED");

        // find_by_source tests
        test_find_by_source_returns_match($store).await;
        println!("  test_find_by_source_returns_match: PASSED");

        test_find_by_source_no_match($store).await;
        println!("  test_find_by_source_no_match: PASSED");

        // cascade tests
        test_query_stale_cascades_finds_old_uncommitted($store).await;
        println!("  test_query_stale_cascades_finds_old_uncommitted: PASSED");

        test_query_stale_cascades_ignores_resolved($store).await;
        println!("  test_query_stale_cascades_ignores_resolved: PASSED");

        test_query_stale_cascades_ignores_fresh($store).await;
        println!("  test_query_stale_cascades_ignores_fresh: PASSED");

        test_query_cascade_participants_finds_uncommitted($store).await;
        println!("  test_query_cascade_participants_finds_uncommitted: PASSED");

        test_query_cascade_participants_ignores_committed($store).await;
        println!("  test_query_cascade_participants_ignores_committed: PASSED");

        test_query_cascade_participants_multiple_aggregates($store).await;
        println!("  test_query_cascade_participants_multiple_aggregates: PASSED");
    };
}
