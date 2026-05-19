//! NATS JetStream EventBus contract tests using testcontainers.
//!
//! Run with: cargo test --test bus_nats --features nats -- --nocapture
//!
//! These tests verify that the NATS JetStream bus implementation correctly
//! fulfills the EventBus trait contract. Uses testcontainers-rs to spin up NATS.

mod bus;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use angzarr::bus::nats::NatsEventBus;
use angzarr::bus::{BusError, EventBus, EventHandler};
use angzarr::proto::{
    event_page, page_header::SequenceType, Cover, Edition, EventBook, EventPage, PageHeader,
};
use angzarr::storage::nats::NatsEventStore;
use angzarr::storage::EventStore;
use angzarr::test_utils::CapturingHandler;
use futures::future::BoxFuture;
use prost_types::Any;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio::sync::mpsc;
use uuid::Uuid;

/// Start NATS container with JetStream enabled.
async fn start_nats() -> (
    testcontainers::ContainerAsync<GenericImage>,
    async_nats::Client,
) {
    let image = GenericImage::new("nats", "2.10")
        .with_exposed_port(4222.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "Listening for client connections",
        ))
        .with_cmd(vec!["-js"]); // Enable JetStream

    let container = image
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("Failed to start NATS container");

    let host_port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("Failed to get mapped port");

    let host = container
        .get_host()
        .await
        .expect("Failed to get container host");

    let url = format!("nats://{}:{}", host, host_port);
    println!("NATS available at: {}", url);

    let client = async_nats::connect(&url)
        .await
        .expect("Failed to connect to NATS");

    (container, client)
}

fn test_prefix() -> String {
    format!(
        "test_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "")[..8].to_string()
    )
}

#[tokio::test]
async fn test_nats_event_bus() {
    println!("=== NATS EventBus Tests ===");

    let (_container, client) = start_nats().await;
    let prefix = test_prefix();

    let bus = NatsEventBus::new(client.clone(), Some(&prefix))
        .await
        .expect("Failed to create NATS EventBus");

    run_event_bus_tests!(&bus, &prefix);

    // H-11: per-root ordering contract test. Re-create the bus inside an
    // Arc so the helper can clone it across concurrent producer tasks
    // (`NatsEventBus` does not implement `Clone`).
    let bus_arc: Arc<dyn EventBus> = Arc::new(
        NatsEventBus::new(client.clone(), Some(&prefix))
            .await
            .expect("Failed to create NATS EventBus for ordering test"),
    );
    run_per_root_ordering_test!(bus_arc, &prefix);

    println!("=== All NATS EventBus tests PASSED ===");
}

// =============================================================================
// NATS-specific tests
// =============================================================================

/// Create a test EventBook with specific sequence and root.
fn make_event_book_with_seq(domain: &str, root: Uuid, first_seq: u32, count: u32) -> EventBook {
    let pages: Vec<EventPage> = (0..count)
        .map(|i| EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::Sequence(first_seq + i)),
            }),
            created_at: None,
            payload: Some(event_page::Payload::Event(Any {
                type_url: format!("type.example/{}Event", domain),
                value: vec![1, 2, 3, (first_seq + i) as u8],
            })),
            ..Default::default()
        })
        .collect();

    let next_seq = first_seq + count;

    EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(angzarr::proto::Uuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: "interop-test".to_string(),
            edition: Some(Edition {
                name: "angzarr".to_string(),
                divergences: vec![],
            }),
        }),
        snapshot: None,
        pages,
        next_sequence: next_seq,
    }
}

/// Create test EventPages for EventStore.add().
fn make_event_pages(first_seq: u32, count: u32) -> Vec<EventPage> {
    (0..count)
        .map(|i| EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::Sequence(first_seq + i)),
            }),
            created_at: None,
            payload: Some(event_page::Payload::Event(Any {
                type_url: "type.example/TestEvent".to_string(),
                value: vec![10, 20, 30, (first_seq + i) as u8],
            })),
            ..Default::default()
        })
        .collect()
}

/// Test consumer group load balancing.
///
/// Two subscribers with the same name should share messages.
#[tokio::test]
async fn test_consumer_group_load_balancing() {
    println!("=== test_consumer_group_load_balancing ===");
    let (_container, client) = start_nats().await;
    let prefix = test_prefix();

    let bus = NatsEventBus::new(client.clone(), Some(&prefix))
        .await
        .expect("Failed to create NATS EventBus");

    let (tx1, mut rx1) = mpsc::channel(10);
    let (tx2, mut rx2) = mpsc::channel(10);

    // Two subscribers with SAME name = consumer group
    let sub1 = bus
        .create_subscriber("shared-consumer", Some("order"))
        .await
        .expect("Failed to create subscriber 1");

    let sub2 = bus
        .create_subscriber("shared-consumer", Some("order"))
        .await
        .expect("Failed to create subscriber 2");

    sub1.subscribe(Box::new(CapturingHandler::new(tx1)))
        .await
        .expect("Failed to subscribe 1");
    sub2.subscribe(Box::new(CapturingHandler::new(tx2)))
        .await
        .expect("Failed to subscribe 2");

    sub1.start_consuming().await.expect("Failed to start 1");
    sub2.start_consuming().await.expect("Failed to start 2");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Publish 10 messages
    for _ in 0..10 {
        bus.publish(Arc::new(bus::event_bus_tests::make_event_book("order")))
            .await
            .expect("Failed to publish");
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Count received by each subscriber
    let mut count1 = 0;
    let mut count2 = 0;

    while rx1.try_recv().is_ok() {
        count1 += 1;
    }
    while rx2.try_recv().is_ok() {
        count2 += 1;
    }

    let total = count1 + count2;
    println!(
        "  Sub1 received: {}, Sub2 received: {}, Total: {}",
        count1, count2, total
    );

    assert_eq!(total, 10, "Should receive all 10 messages across consumers");
    // In a consumer group, messages are distributed. Both should get some.
    assert!(
        count1 > 0 && count2 > 0,
        "Both consumers should receive messages"
    );

    println!("  PASSED");
}

/// Test EventStore/EventBus interoperability.
///
/// Events written via EventStore should be received by EventBus subscribers,
/// and events published via EventBus should be readable via EventStore.
#[tokio::test]
async fn test_eventstore_eventbus_interoperability() {
    println!("=== test_eventstore_eventbus_interoperability ===");
    let (_container, client) = start_nats().await;
    let prefix = test_prefix();

    // Create both EventStore and EventBus pointing to same NATS
    let event_store = NatsEventStore::new(client.clone(), Some(&prefix))
        .await
        .expect("Failed to create NatsEventStore");

    let event_bus = NatsEventBus::new(client.clone(), Some(&prefix))
        .await
        .expect("Failed to create NatsEventBus");

    let root = Uuid::new_v4();
    let domain = "interop";
    let edition = "angzarr";

    // Set up subscriber to capture events
    let (tx, mut rx) = mpsc::channel(10);
    let subscriber = event_bus
        .create_subscriber("interop-test", Some(domain))
        .await
        .expect("Failed to create subscriber");
    subscriber
        .subscribe(Box::new(CapturingHandler::new(tx)))
        .await
        .expect("Failed to subscribe");
    subscriber
        .start_consuming()
        .await
        .expect("Failed to start consuming");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // === Part 1: Write via EventStore, read via EventBus subscriber ===
    println!("  Part 1: EventStore.add() -> EventBus subscriber");

    let events = make_event_pages(0, 2);
    event_store
        .add(domain, edition, root, events, "interop-test", None, None)
        .await
        .expect("Failed to add events via EventStore");

    // Should receive via subscriber
    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("Timeout waiting for event from EventStore")
        .expect("Channel closed");

    assert_eq!(received.cover.as_ref().unwrap().domain, domain);
    assert_eq!(received.pages.len(), 2);
    println!(
        "    EventBus received {} pages from EventStore write",
        received.pages.len()
    );

    // === Part 2: Verify EventStore.get() can read what it wrote ===
    println!("  Part 2: EventStore.get() reads EventStore.add() data");

    let stored_events = event_store
        .get(domain, edition, root)
        .await
        .expect("Failed to get events");

    assert_eq!(stored_events.len(), 2);
    println!(
        "    EventStore.get() returned {} events",
        stored_events.len()
    );

    // === Part 3: Write via EventBus, read via EventStore ===
    println!("  Part 3: EventBus.publish() -> EventStore.get()");

    let book = make_event_book_with_seq(domain, root, 2, 2); // seq 2, 3
    event_bus
        .publish(Arc::new(book))
        .await
        .expect("Failed to publish via EventBus");

    // Small delay for NATS to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Read all events via EventStore
    let all_events = event_store
        .get(domain, edition, root)
        .await
        .expect("Failed to get all events");

    assert_eq!(
        all_events.len(),
        4,
        "Should have 4 total events (2 from store + 2 from bus)"
    );
    println!(
        "    EventStore.get() returned {} total events",
        all_events.len()
    );

    // Verify sequence ordering
    for (i, event) in all_events.iter().enumerate() {
        let seq = match event.header.as_ref().and_then(|h| h.sequence_type.as_ref()) {
            Some(SequenceType::Sequence(n)) => *n,
            _ => panic!("Expected sequence in event header"),
        };
        assert_eq!(seq, i as u32, "Events should be in sequence order");
    }
    println!("    Events are in correct sequence order (0, 1, 2, 3)");

    // === Part 4: Verify get_next_sequence works with mixed writes ===
    println!("  Part 4: get_next_sequence() reflects all writes");

    let next_seq = event_store
        .get_next_sequence(domain, edition, root)
        .await
        .expect("Failed to get next sequence");

    assert_eq!(next_seq, 4, "Next sequence should be 4");
    println!("    get_next_sequence() returned {}", next_seq);

    println!("  PASSED");
}

/// Handler that fails its first N invocations and succeeds afterwards.
///
/// Used to drive the C-10 redelivery test against JetStream: after the
/// consumer leaves the message unacked, JetStream's `ack-pending`
/// timeout (default 30s) or an explicit `msg.nak()` re-delivers the
/// message and the handler's call count crosses N+1.
struct FlakyHandler {
    fail_until: usize,
    calls: Arc<AtomicUsize>,
}

impl EventHandler for FlakyHandler {
    fn handle(&self, _book: Arc<EventBook>) -> BoxFuture<'static, Result<(), BusError>> {
        let fail_until = self.fail_until;
        let calls = self.calls.clone();
        Box::pin(async move {
            let attempt = calls.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt <= fail_until {
                Err(BusError::ProjectorFailed {
                    name: "nats-c10-flaky".to_string(),
                    message: format!("synthetic failure (attempt {})", attempt),
                })
            } else {
                Ok(())
            }
        })
    }
}

/// Regression test for finding C-10 (NATS transport): when a handler
/// returns `Err`, the NATS JetStream consumer must NOT `ack` the message;
/// JetStream must re-deliver it.
///
/// Baseline (pre-C-10) calls `msg.ack().await` unconditionally after
/// dispatch, so JetStream marks the message delivered and never retries.
/// A failing handler observes one invocation and the event is silently
/// lost.
///
/// After the fix, the consumer issues `msg.ack_with(NakWithDelay)` (or
/// equivalently leaves the message unacked so it redelivers after the
/// ack-pending timeout) on failure. We use a low-latency redelivery path
/// — `msg.ack_with(AckKind::Nak(None))` requests immediate redelivery —
/// so the test terminates quickly.
#[tokio::test]
async fn test_handler_err_triggers_nats_redelivery() {
    println!("=== NATS handler-failure redelivery test (C-10) ===");

    let (_container, client) = start_nats().await;
    let prefix = test_prefix();
    let domain = "c10domain"; // NATS stream subjects don't tolerate '-' in component

    let publisher = NatsEventBus::new(client.clone(), Some(&prefix))
        .await
        .expect("Failed to create NATS EventBus publisher");

    let subscriber = publisher
        .create_subscriber("c10-flaky-sub", Some(domain))
        .await
        .expect("Failed to create NATS subscriber");

    let calls = Arc::new(AtomicUsize::new(0));
    subscriber
        .subscribe(Box::new(FlakyHandler {
            fail_until: 1,
            calls: calls.clone(),
        }))
        .await
        .expect("Failed to subscribe");

    subscriber
        .start_consuming()
        .await
        .expect("Failed to start consuming");

    // Let the consumer attach to the stream.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let book = Arc::new(make_event_book_with_seq(domain, uuid::Uuid::new_v4(), 0, 1));
    publisher
        .publish(book)
        .await
        .expect("Failed to publish event");

    // JetStream's nak-based redelivery should land attempt #2 within a
    // couple of seconds. Poll up to 10s to absorb scheduler/network
    // jitter; the baseline behavior is "stuck at 1" indefinitely.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if calls.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let observed = calls.load(Ordering::SeqCst);
    assert!(
        observed >= 2,
        "expected handler to observe >= 2 invocations after failing the \
         first (JetStream must redeliver unacked messages), but saw {}. \
         Baseline (pre-C-10) acks unconditionally and the message is \
         silently lost — observed = 1.",
        observed
    );

    println!(
        "=== NATS handler-failure redelivery: PASSED (observed {} invocations) ===",
        observed
    );
}
