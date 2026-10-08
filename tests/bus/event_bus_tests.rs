//! EventBus interface tests.
//!
//! These tests verify the contract of the EventBus trait.
//! Each bus implementation should run these tests.
//!
//! Requires the `test-utils` feature to be enabled.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use angzarr::bus::EventBus;
use angzarr::dlq::{
    init_dlq_publisher, AngzarrDeadLetter, DeadLetterPayload, DlqConfig,
    EventProcessingFailedDetails, RejectionDetails, SequenceMismatchDetails,
};
use angzarr::proto::{
    event_page, page_header::SequenceType, CommandBook, Cover, EventBook, EventPage, MergeStrategy,
    PageHeader, Uuid,
};
#[cfg(feature = "test-utils")]
use angzarr::test_utils::CapturingHandler;
use prost_types::Any;
use tokio::sync::mpsc;

/// Create a test EventBook for a given domain.
pub fn make_event_book(domain: &str) -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(Uuid {
                value: uuid::Uuid::new_v4().as_bytes().to_vec(),
            }),
            correlation_id: format!("test-{}", uuid::Uuid::new_v4()),
            edition: None,
            ..Default::default()
        }),
        pages: vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::Sequence(0)),
            }),
            created_at: None,
            payload: Some(event_page::Payload::Event(Any {
                type_url: "type.googleapis.com/test.TestEvent".to_string(),
                value: vec![1, 2, 3],
            })),
            ..Default::default()
        }],
        snapshot: None,
        ..Default::default()
    }
}

/// Create a test CommandBook for DLQ tests.
#[allow(dead_code)] // not every bus binary runs the DLQ suite
pub fn make_command_book(domain: &str) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(Uuid {
                value: uuid::Uuid::new_v4().as_bytes().to_vec(),
            }),
            correlation_id: format!("txn-{}", uuid::Uuid::new_v4()),
            edition: None,
            ..Default::default()
        }),
        pages: vec![],
        ..Default::default()
    }
}

// =============================================================================
// DLQ publisher contract tests
// =============================================================================
// Restored after the macro-consolidation refactor (effffc28) deleted them
// while bus_kafka.rs / bus_sns_sqs.rs kept calling them, leaving those test
// binaries uncompilable (review finding T1).

/// Contract: publishing a saga/projector-failure dead letter through a real
/// broker-backed DLQ target succeeds end-to-end — factory init, topic/queue
/// provisioning, serialization, and broker acknowledgment.
///
/// TODO(plan T7): consume the DLQ destination and assert the payload
/// round-trips byte-exact, mirroring bus_amqp's H-06 coverage.
#[allow(dead_code)]
pub async fn test_dlq_publish(dlq_config: &DlqConfig) {
    let dlq_publisher = init_dlq_publisher(dlq_config)
        .await
        .expect("Failed to create DLQ publisher");

    let book = make_event_book("orders");
    let dead_letter = AngzarrDeadLetter {
        cover: book.cover.clone(),
        payload: DeadLetterPayload::Events(book),
        rejection_reason: "Handler threw an exception".to_string(),
        rejection_details: Some(RejectionDetails::EventProcessingFailed(
            EventProcessingFailedDetails {
                error: "Connection refused".to_string(),
                retry_count: 3,
                is_transient: false,
                stack_trace: vec![],
            },
        )),
        source_component: "saga-order-fulfillment".to_string(),
        source_component_type: "saga".to_string(),
        occurred_at: None,
        metadata: std::collections::HashMap::new(),
    };

    dlq_publisher
        .publish(dead_letter)
        .await
        .expect("DLQ publish should succeed");
}

/// Contract: a MERGE_MANUAL sequence-mismatch dead letter (command payload)
/// publishes successfully with its structured rejection details intact.
#[allow(dead_code)]
pub async fn test_dlq_sequence_mismatch(dlq_config: &DlqConfig) {
    let dlq_publisher = init_dlq_publisher(dlq_config)
        .await
        .expect("Failed to create DLQ publisher");

    let command_book = make_command_book("inventory");
    let dead_letter = AngzarrDeadLetter {
        cover: command_book.cover.clone(),
        payload: DeadLetterPayload::Command(command_book),
        rejection_reason: "Sequence mismatch".to_string(),
        rejection_details: Some(RejectionDetails::SequenceMismatch(
            SequenceMismatchDetails {
                expected_sequence: 0,
                actual_sequence: 5,
                merge_strategy: MergeStrategy::MergeManual,
            },
        )),
        source_component: "aggregate-inventory".to_string(),
        source_component_type: "aggregate".to_string(),
        occurred_at: None,
        metadata: std::collections::HashMap::new(),
    };

    dlq_publisher
        .publish(dead_letter)
        .await
        .expect("DLQ publish should succeed");
}

// =============================================================================
// Handler-failure redelivery contract (C-10 class)
// =============================================================================

/// Handler that fails its first N invocations and succeeds afterwards.
///
/// Drives the redelivery contract test: after the broker re-delivers a
/// failed message, the next invocation must succeed and the call count
/// must reach N+1 (proving redelivery actually happened).
#[allow(dead_code)]
pub struct FlakyHandler {
    pub fail_until: usize,
    pub calls: Arc<AtomicUsize>,
}

impl angzarr::bus::EventHandler for FlakyHandler {
    fn handle(
        &self,
        _book: Arc<EventBook>,
    ) -> futures::future::BoxFuture<'static, Result<(), angzarr::bus::BusError>> {
        let fail_until = self.fail_until;
        let calls = self.calls.clone();
        Box::pin(async move {
            let attempt = calls.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt <= fail_until {
                Err(angzarr::bus::BusError::ProjectorFailed {
                    name: "c10-flaky".to_string(),
                    message: format!("synthetic failure (attempt {})", attempt),
                })
            } else {
                Ok(())
            }
        })
    }
}

/// Contract (C-10 class): when a handler returns `Err`, the bus must NOT
/// treat the message as consumed — the broker must re-deliver until the
/// handler succeeds (or the broker's own retry/DLQ policy intervenes).
/// A consumer that acks/commits on handler failure silently loses the
/// event, exactly once, with no operator signal.
///
/// `redelivery_deadline` is per-broker: AMQP requeues nacked messages in
/// milliseconds; SQS waits out the visibility timeout; Kafka redelivery
/// latency depends on the consumer's seek/retry strategy.
///
/// T7 (review remediation): previously this contract was pinned on AMQP
/// only — the other brokers could commit-past-failure and stay green.
#[allow(dead_code)]
pub async fn test_handler_err_triggers_redelivery<B: EventBus>(
    publisher: &B,
    domain: &str,
    subscriber_name: &str,
    redelivery_deadline: Duration,
) {
    let subscriber = publisher
        .create_subscriber(subscriber_name, Some(domain))
        .await
        .expect("Failed to create subscriber");

    let calls = Arc::new(AtomicUsize::new(0));
    subscriber
        .subscribe(Box::new(FlakyHandler {
            fail_until: 1,
            calls: calls.clone(),
        }))
        .await
        .expect("Failed to subscribe FlakyHandler");

    subscriber
        .start_consuming()
        .await
        .expect("Failed to start consuming");

    publisher
        .publish(Arc::new(make_event_book(domain)))
        .await
        .expect("Failed to publish event");

    // Poll up to the broker-appropriate deadline for the second delivery.
    let deadline = std::time::Instant::now() + redelivery_deadline;
    while std::time::Instant::now() < deadline {
        if calls.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let observed = calls.load(Ordering::SeqCst);
    assert!(
        observed >= 2,
        "expected handler to observe >= 2 invocations after failing the \
         first delivery (got {observed}): the bus acked/committed a FAILED \
         message — that event is silently lost (C-10 class)",
    );
}

// =============================================================================
// EventBus publish/subscribe tests
// =============================================================================

/// Test basic publish and subscribe roundtrip.
pub async fn test_publish_subscribe_roundtrip<B: EventBus>(
    publisher: &B,
    domain: &str,
    subscriber_name: &str,
) {
    let subscriber = publisher
        .create_subscriber(subscriber_name, Some(domain))
        .await
        .expect("Failed to create subscriber");

    let count = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::channel(10);

    subscriber
        .subscribe(Box::new(CapturingHandler::with_count(tx, count.clone())))
        .await
        .expect("Failed to subscribe");

    subscriber
        .start_consuming()
        .await
        .expect("Failed to start consuming");

    // Publish event
    let book = make_event_book(domain);
    publisher
        .publish(Arc::new(book.clone()))
        .await
        .expect("Failed to publish");

    // Wait for message
    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("Timed out waiting for message")
        .expect("Channel closed");

    assert_eq!(
        received.cover.as_ref().unwrap().domain,
        domain,
        "Received event should have correct domain"
    );
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "Handler should be called once"
    );
}

/// Test that publish works without any subscribers.
pub async fn test_publish_only<B: EventBus>(publisher: &B, domain: &str) {
    let book = make_event_book(domain);

    // Should succeed without error
    publisher
        .publish(Arc::new(book))
        .await
        .expect("Publish should succeed without subscribers");
}

/// Test receiving multiple messages.
pub async fn test_multiple_messages<B: EventBus>(
    publisher: &B,
    domain: &str,
    subscriber_name: &str,
    message_count: usize,
) {
    let subscriber = publisher
        .create_subscriber(subscriber_name, Some(domain))
        .await
        .expect("Failed to create subscriber");

    let count = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::channel(100);

    subscriber
        .subscribe(Box::new(CapturingHandler::with_count(tx, count.clone())))
        .await
        .expect("Failed to subscribe");

    subscriber
        .start_consuming()
        .await
        .expect("Failed to start consuming");

    // Publish multiple messages
    for _ in 0..message_count {
        let book = make_event_book(domain);
        publisher
            .publish(Arc::new(book))
            .await
            .expect("Failed to publish");
    }

    // Wait for all messages
    for i in 0..message_count {
        tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect(&format!("Timed out waiting for message {}", i))
            .expect("Channel closed");
    }

    assert_eq!(
        count.load(Ordering::SeqCst),
        message_count,
        "Handler should be called {} times",
        message_count
    );
}

/// Test domain filtering - subscriber should only receive events for its domain.
pub async fn test_domain_filtering<B: EventBus>(
    publisher: &B,
    target_domain: &str,
    other_domain: &str,
    subscriber_name: &str,
) {
    let subscriber = publisher
        .create_subscriber(subscriber_name, Some(target_domain))
        .await
        .expect("Failed to create subscriber");

    let (tx, mut rx) = mpsc::channel(10);

    subscriber
        .subscribe(Box::new(CapturingHandler::new(tx)))
        .await
        .expect("Failed to subscribe");

    subscriber
        .start_consuming()
        .await
        .expect("Failed to start consuming");

    // Publish to target domain - should be received
    publisher
        .publish(Arc::new(make_event_book(target_domain)))
        .await
        .expect("Failed to publish to target domain");

    // Publish to other domain - should NOT be received
    publisher
        .publish(Arc::new(make_event_book(other_domain)))
        .await
        .expect("Failed to publish to other domain");

    // Should receive only the target domain event
    let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("Timed out waiting for target domain event")
        .expect("Channel closed");

    assert_eq!(
        received.cover.as_ref().unwrap().domain,
        target_domain,
        "Should receive event from target domain"
    );

    // Should NOT receive another event (other domain was filtered)
    let timeout_result: Result<Option<EventBook>, _> =
        tokio::time::timeout(Duration::from_millis(500), rx.recv()).await;
    assert!(
        timeout_result.is_err(),
        "Should not receive event from other domain"
    );
}

// =============================================================================
// Multi-domain and multi-handler tests
// =============================================================================

/// Test subscribing to all domains (no filter).
pub async fn test_multi_domain_subscription<B: EventBus>(
    publisher: &B,
    domain1: &str,
    domain2: &str,
    subscriber_name: &str,
) {
    // Create subscriber with no domain filter - receives all domains
    let subscriber = publisher
        .create_subscriber(subscriber_name, None)
        .await
        .expect("Failed to create subscriber");

    let (tx, mut rx) = mpsc::channel(10);

    subscriber
        .subscribe(Box::new(CapturingHandler::new(tx)))
        .await
        .expect("Failed to subscribe");

    subscriber
        .start_consuming()
        .await
        .expect("Failed to start consuming");

    // Publish to domain1
    publisher
        .publish(Arc::new(make_event_book(domain1)))
        .await
        .expect("Failed to publish to domain1");

    // Publish to domain2
    publisher
        .publish(Arc::new(make_event_book(domain2)))
        .await
        .expect("Failed to publish to domain2");

    // Contract: the unfiltered subscriber observes BOTH domains' events.
    // Two backend behaviors make "the first two messages are ours" wrong:
    //   - topic DISCOVERY: an all-domains Kafka subscriber sees topics
    //     created after attach only on metadata refresh (bounded at 5s by
    //     consumer config) — hence the generous deadline;
    //   - HISTORY REPLAY: log-retaining backends (Kafka + earliest reset)
    //     deliver events persisted by EARLIER tests on the same broker
    //     before ours — ignore foreign domains, don't assert on ordering.
    // AMQP/Pub/Sub bind wildcards up front and deliver only the two new
    // events in ms; the deadline is an upper bound, not a wait.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let (mut seen1, mut seen2) = (false, false);
    while !(seen1 && seen2) {
        let remaining = deadline
            .checked_duration_since(std::time::Instant::now())
            .unwrap_or_else(|| {
                panic!(
                    "timed out waiting for events from both domains \
                     (saw {domain1}: {seen1}, {domain2}: {seen2})"
                )
            });
        let book = tokio::time::timeout(remaining, rx.recv())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "timed out waiting for events from both domains \
                     (saw {domain1}: {seen1}, {domain2}: {seen2})"
                )
            })
            .expect("Channel closed");
        match book.cover.as_ref().unwrap().domain.as_str() {
            d if d == domain1 => seen1 = true,
            d if d == domain2 => seen2 = true,
            _ => {} // replayed history from another test's domain
        }
    }
}

/// Test multiple independent handlers on same event.
pub async fn test_multiple_handlers_independent<B: EventBus>(
    publisher: &B,
    domain: &str,
    subscriber1_name: &str,
    subscriber2_name: &str,
) {
    // Create two separate subscribers
    let subscriber1 = publisher
        .create_subscriber(subscriber1_name, Some(domain))
        .await
        .expect("Failed to create subscriber1");

    let subscriber2 = publisher
        .create_subscriber(subscriber2_name, Some(domain))
        .await
        .expect("Failed to create subscriber2");

    let count1 = Arc::new(AtomicUsize::new(0));
    let count2 = Arc::new(AtomicUsize::new(0));

    let (tx1, _rx1) = mpsc::channel(10);
    let (tx2, _rx2) = mpsc::channel(10);

    subscriber1
        .subscribe(Box::new(CapturingHandler::with_count(tx1, count1.clone())))
        .await
        .expect("Failed to subscribe");

    subscriber2
        .subscribe(Box::new(CapturingHandler::with_count(tx2, count2.clone())))
        .await
        .expect("Failed to subscribe");

    subscriber1
        .start_consuming()
        .await
        .expect("Failed to start");
    subscriber2
        .start_consuming()
        .await
        .expect("Failed to start");

    // Publish single event
    publisher
        .publish(Arc::new(make_event_book(domain)))
        .await
        .expect("Failed to publish");

    // T10: poll to a deadline instead of a flat 500ms sleep — consumer
    // attach is broker-dependent (Kafka group join + partition assignment
    // takes seconds; AMQP attaches in ms). The deadline is an upper bound.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while std::time::Instant::now() < deadline {
        if count1.load(Ordering::SeqCst) >= 1 && count2.load(Ordering::SeqCst) >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Both handlers must observe the event independently...
    assert_eq!(
        count1.load(Ordering::SeqCst),
        1,
        "subscriber1 should receive 1 event"
    );
    assert_eq!(
        count2.load(Ordering::SeqCst),
        1,
        "subscriber2 should receive 1 event"
    );

    // ...and exactly once: a short settle window catches duplicates that
    // would arrive right behind the first delivery.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        count1.load(Ordering::SeqCst),
        1,
        "subscriber1 must not receive duplicates"
    );
    assert_eq!(
        count2.load(Ordering::SeqCst),
        1,
        "subscriber2 must not receive duplicates"
    );
}

// =============================================================================
// Metadata and payload preservation tests
// =============================================================================

/// Test that correlation_id is preserved through transport.
pub async fn test_routing_metadata_preserved<B: EventBus>(
    publisher: &B,
    domain: &str,
    subscriber_name: &str,
) {
    let subscriber = publisher
        .create_subscriber(subscriber_name, Some(domain))
        .await
        .expect("Failed to create subscriber");

    let (tx, mut rx) = mpsc::channel(10);

    subscriber
        .subscribe(Box::new(CapturingHandler::new(tx)))
        .await
        .expect("Failed to subscribe");

    subscriber.start_consuming().await.expect("Failed to start");

    // Create event with specific correlation_id
    let correlation_id = format!("corr-{}", uuid::Uuid::new_v4());
    let mut book = make_event_book(domain);
    book.cover.as_mut().unwrap().correlation_id = correlation_id.clone();

    publisher
        .publish(Arc::new(book))
        .await
        .expect("Failed to publish");

    let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("Timed out")
        .expect("Channel closed");

    assert_eq!(
        received.cover.as_ref().unwrap().correlation_id,
        correlation_id,
        "correlation_id should be preserved"
    );
}

/// Test that binary payload is preserved exactly.
pub async fn test_payload_bytes_exact<B: EventBus>(
    publisher: &B,
    domain: &str,
    subscriber_name: &str,
) {
    let subscriber = publisher
        .create_subscriber(subscriber_name, Some(domain))
        .await
        .expect("Failed to create subscriber");

    let (tx, mut rx) = mpsc::channel(10);

    subscriber
        .subscribe(Box::new(CapturingHandler::new(tx)))
        .await
        .expect("Failed to subscribe");

    subscriber.start_consuming().await.expect("Failed to start");

    // Create event with specific binary payload
    let payload_bytes: Vec<u8> = (0..256).map(|i| i as u8).collect();
    let book = EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(Uuid {
                value: uuid::Uuid::new_v4().as_bytes().to_vec(),
            }),
            correlation_id: "test".to_string(),
            edition: None,
            ..Default::default()
        }),
        pages: vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::Sequence(0)),
            }),
            created_at: None,
            payload: Some(event_page::Payload::Event(Any {
                type_url: "type.test/BinaryPayload".to_string(),
                value: payload_bytes.clone(),
            })),
            ..Default::default()
        }],
        snapshot: None,
        ..Default::default()
    };

    publisher
        .publish(Arc::new(book))
        .await
        .expect("Failed to publish");

    let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("Timed out")
        .expect("Channel closed");

    if let Some(event_page::Payload::Event(event)) = &received.pages[0].payload {
        assert_eq!(
            event.value, payload_bytes,
            "payload bytes should match exactly"
        );
    } else {
        panic!("Expected Event payload");
    }
}

// =============================================================================
// Concurrency tests
// =============================================================================

/// Build an `EventBook` with a SPECIFIC root and an in-payload `(root, seq)`
/// marker so a downstream consumer can reconstruct the publish order.
///
/// `make_event_book()` allocates a fresh random root on every call, which
/// is what most of the EventBus contract tests want — but the per-root
/// ordering check in `test_per_root_ordering_under_concurrent_publish`
/// needs N publishers to keep emitting under the same root for the
/// duration of the test, with the publish-order sequence embedded so the
/// receiver can detect reordering.
///
/// The payload `value` is laid out as `[root_bytes (16) || seq (u32 BE)]`.
/// We do NOT encode the sequence into `PageHeader.sequence_type` because
/// some buses (notably SNS/SQS FIFO) include the page's `max_seq` in their
/// MessageDeduplicationId — reusing the same seq across publishers would
/// produce identical dedup_ids and collide inside AWS's 5-minute dedup
/// window. Carrying the order marker in the payload sidesteps that.
fn make_event_book_with_root_and_seq(domain: &str, root: uuid::Uuid, seq: u32) -> EventBook {
    let mut payload_bytes = Vec::with_capacity(20);
    payload_bytes.extend_from_slice(root.as_bytes());
    payload_bytes.extend_from_slice(&seq.to_be_bytes());

    EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(Uuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: format!("test-{}-{}", root, seq),
            edition: None,
            ..Default::default()
        }),
        pages: vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                // Unique per (root, seq) so the bus's own dedup logic (if
                // any) does not drop the second event from a producer that
                // restarted its counter.
                sequence_type: Some(SequenceType::Sequence(seq)),
            }),
            created_at: None,
            payload: Some(event_page::Payload::Event(Any {
                type_url: "type.googleapis.com/test.OrderedEvent".to_string(),
                value: payload_bytes,
            })),
            ..Default::default()
        }],
        snapshot: None,
        ..Default::default()
    }
}

/// Test that the framework's "events for the same aggregate root arrive in
/// publish order" guarantee holds under concurrent publishing from
/// multiple producers writing to multiple roots.
///
/// Background (H-11 in `plans/deep-review-remediation.md`): the original
/// concurrent-publish test (`test_concurrent_publish_no_loss`) only
/// counted received messages — it never verified the framework's
/// strongest delivery claim. Pub/Sub (with `enable_message_ordering=false`)
/// and Kafka (with no message key) both silently violate per-root
/// ordering under load while passing a pure-count contract test. C-11
/// turned on Pub/Sub ordering and H-10 made Kafka reject root-less
/// EventBooks, so this contract test should now pass on every backend
/// that documents per-root ordering as a guarantee.
///
/// The test:
/// 1. Picks `num_roots` distinct aggregate roots.
/// 2. Spawns `num_roots` concurrent producers — one per root — each
///    publishing `events_per_root` events with payload `(root, seq)` in
///    monotonic order 0..events_per_root.
/// 3. The consumer groups received events by root and asserts each
///    group's sequences appear in publish order (no reordering inside a
///    root). Across roots, no ordering is asserted — that is not a
///    framework contract.
pub async fn test_per_root_ordering_under_concurrent_publish(
    publisher: Arc<dyn EventBus>,
    domain: &str,
    subscriber_name: &str,
    num_roots: usize,
    events_per_root: u32,
) {
    let subscriber = publisher
        .create_subscriber(subscriber_name, Some(domain))
        .await
        .expect("Failed to create subscriber");

    let total_events = num_roots * events_per_root as usize;
    let (tx, mut rx) = mpsc::channel(total_events * 2);

    subscriber
        .subscribe(Box::new(CapturingHandler::new(tx)))
        .await
        .expect("Failed to subscribe");

    subscriber.start_consuming().await.expect("Failed to start");

    // Allocate roots up-front so producers don't race the test on UUID
    // generation.
    let roots: Vec<uuid::Uuid> = (0..num_roots).map(|_| uuid::Uuid::new_v4()).collect();

    // One producer task per root; events 0..events_per_root in order.
    // Sharing an `Arc<dyn EventBus>` across producers exercises the
    // documented concurrent-publish contract without requiring `B: Clone`
    // (which most production bus types do not implement).
    let mut handles = Vec::with_capacity(num_roots);
    for root in roots.iter().copied() {
        let pub_arc = publisher.clone();
        let domain_owned = domain.to_string();
        handles.push(tokio::spawn(async move {
            for seq in 0..events_per_root {
                let book = make_event_book_with_root_and_seq(&domain_owned, root, seq);
                pub_arc
                    .publish(Arc::new(book))
                    .await
                    .unwrap_or_else(|e| panic!("publish root={} seq={} failed: {}", root, seq, e));
            }
        }));
    }
    for h in handles {
        h.await.expect("producer task panicked");
    }

    // Receive everything we expect with a generous timeout — the per-root
    // ordering claim is about ordering, not latency; we still need a
    // bound so a regression that silently drops a frame doesn't hang.
    let mut received_per_root: std::collections::HashMap<uuid::Uuid, Vec<u32>> =
        std::collections::HashMap::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut received_total = 0usize;
    while received_total < total_events {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(book)) => {
                // Extract (root, seq) from the payload bytes laid down by
                // `make_event_book_with_root_and_seq`. Skip events whose
                // payload doesn't match our marker shape — some backends
                // may interleave messages from prior tests on the same
                // domain queue.
                let Some(payload) = book.pages.first().and_then(|p| match &p.payload {
                    Some(event_page::Payload::Event(any)) => Some(&any.value),
                    _ => None,
                }) else {
                    continue;
                };
                if payload.len() != 20 {
                    continue;
                }
                let mut root_bytes = [0u8; 16];
                root_bytes.copy_from_slice(&payload[..16]);
                let root = uuid::Uuid::from_bytes(root_bytes);
                if !roots.contains(&root) {
                    // Not one of our roots — ignore.
                    continue;
                }
                let mut seq_bytes = [0u8; 4];
                seq_bytes.copy_from_slice(&payload[16..20]);
                let seq = u32::from_be_bytes(seq_bytes);
                received_per_root.entry(root).or_default().push(seq);
                received_total += 1;
            }
            _ => break,
        }
    }

    // First: no losses on any root. This guards against the case where
    // per-root ordering is "verified" only on roots that happened to
    // round-trip everything.
    for root in &roots {
        let got = received_per_root.get(root).cloned().unwrap_or_default();
        assert_eq!(
            got.len(),
            events_per_root as usize,
            "root {} received {} events, expected {}",
            root,
            got.len(),
            events_per_root,
        );
    }

    // Second: per-root, the received sequences must equal the publish
    // sequence. Equal — not just monotone — because we know exactly what
    // each producer emitted. Any reordering inside a root surfaces here.
    let expected: Vec<u32> = (0..events_per_root).collect();
    for root in &roots {
        let got = received_per_root.get(root).expect("checked above");
        assert_eq!(
            got, &expected,
            "root {} events arrived in order {:?}, expected {:?} \
             (per-root ordering is a documented framework contract; \
              this is the H-11 regression)",
            root, got, expected,
        );
    }
}

// =============================================================================
// Ordering after a handler failure (X-067)
// =============================================================================

/// Handler that records the sequence of every delivery and fails the first
/// delivery of sequence 0 after a delay.
#[allow(dead_code)]
struct FailFirstOfRoot {
    attempts: Arc<std::sync::Mutex<Vec<(u32, bool)>>>,
}

impl angzarr::bus::EventHandler for FailFirstOfRoot {
    fn handle(
        &self,
        book: Arc<EventBook>,
    ) -> futures::future::BoxFuture<'static, Result<(), angzarr::bus::BusError>> {
        let seq = match book.pages.first().and_then(|p| p.header.as_ref()) {
            Some(PageHeader {
                sequence_type: Some(SequenceType::Sequence(s)),
                ..
            }) => *s,
            _ => u32::MAX,
        };
        let mut attempts = self.attempts.lock().unwrap();
        let first_try = !attempts.iter().any(|(s, _)| *s == seq);
        let ok = !(seq == 0 && first_try);
        attempts.push((seq, ok));
        Box::pin(async move {
            if ok {
                Ok(())
            } else {
                // Fail slowly: by now the root's later events have reached
                // the consumer, so a transport that hands them out ahead
                // of the redelivery is caught reordering.
                tokio::time::sleep(Duration::from_millis(500)).await;
                Err(angzarr::bus::BusError::Subscribe(
                    "first attempt fails".into(),
                ))
            }
        })
    }
}

/// After a handler failure, the failed event of a root is handled again
/// before any later event of the same root: per-root order survives
/// redelivery (AMQP prefetch 1 + requeue to head, SQS/Pub/Sub skipping the
/// rest of the failed group's batch, Kafka seek-back).
#[allow(dead_code)]
pub async fn test_root_order_preserved_after_handler_failure(
    subscriber: Arc<dyn EventBus>,
    publisher: &dyn EventBus,
    domain: &str,
    redelivery_deadline: Duration,
) {
    let attempts = Arc::new(std::sync::Mutex::new(Vec::new()));
    subscriber
        .subscribe(Box::new(FailFirstOfRoot {
            attempts: attempts.clone(),
        }))
        .await
        .expect("subscribe");
    subscriber.start_consuming().await.expect("start consuming");

    let root = uuid::Uuid::new_v4();
    for seq in 0..3 {
        publisher
            .publish(Arc::new(make_event_book_with_root_and_seq(
                domain, root, seq,
            )))
            .await
            .expect("publish");
    }

    let deadline = std::time::Instant::now() + redelivery_deadline;
    loop {
        let succeeded = attempts
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, ok)| *ok)
            .count();
        if succeeded >= 3 || std::time::Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let attempts = attempts.lock().unwrap().clone();
    let successes: Vec<u32> = attempts
        .iter()
        .filter(|(_, ok)| *ok)
        .map(|(s, _)| *s)
        .collect();
    assert_eq!(
        successes,
        vec![0, 1, 2],
        "events of one root must be handled in publish order after a failure \
         (attempts: {attempts:?})"
    );
}

// =============================================================================
// Domain-scoped subscriptions on per-domain-topic transports (X-010)
// =============================================================================

/// A transport with one topic per domain has nothing to attach an
/// all-domains subscription to; it must refuse at start-up rather than
/// silently subscribe to a topic nothing publishes to.
#[allow(dead_code)]
pub async fn test_all_domains_subscription_refused<B: EventBus>(
    publisher: &B,
    subscriber_name: &str,
) {
    let subscriber = publisher
        .create_subscriber(subscriber_name, None)
        .await
        .expect("create subscriber");
    let err = subscriber
        .start_consuming()
        .await
        .expect_err("all-domains subscription must be refused");
    assert!(
        matches!(err, angzarr::bus::BusError::AllDomainsUnsupported(_)),
        "unexpected error: {err}"
    );
}

/// A subscriber naming several domains receives events from each of them.
#[allow(dead_code)]
pub async fn test_explicit_multi_domain_subscription(
    subscriber: Arc<dyn EventBus>,
    publisher: &dyn EventBus,
    domain1: &str,
    domain2: &str,
) {
    let (tx, mut rx) = mpsc::channel(10);
    subscriber
        .subscribe(Box::new(CapturingHandler::new(tx)))
        .await
        .expect("subscribe");
    subscriber.start_consuming().await.expect("start consuming");

    for domain in [domain1, domain2] {
        publisher
            .publish(Arc::new(make_event_book(domain)))
            .await
            .expect("publish");
    }

    let (mut seen1, mut seen2) = (false, false);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !(seen1 && seen2) {
        let remaining = deadline
            .checked_duration_since(std::time::Instant::now())
            .unwrap_or_else(|| panic!("timed out (saw {domain1}: {seen1}, {domain2}: {seen2})"));
        let book = tokio::time::timeout(remaining, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("timed out (saw {domain1}: {seen1}, {domain2}: {seen2})"))
            .expect("channel closed");
        match book.cover.as_ref().unwrap().domain.as_str() {
            d if d == domain1 => seen1 = true,
            d if d == domain2 => seen2 = true,
            other => panic!("received event from unsubscribed domain {other}"),
        }
    }
}

// =============================================================================
// Test runner macro
// =============================================================================

/// Run all EventBus interface tests against a bus implementation.
///
/// # Arguments
/// * `$publisher` - The bus instance configured as publisher
/// * `$prefix` - Unique prefix for subscriber names (for test isolation)
/// * `$dlq_config` - Optional DlqConfig for DLQ tests
#[macro_export]
macro_rules! run_event_bus_tests {
    ($publisher:expr, $prefix:expr) => {
        use $crate::bus::event_bus_tests::*;

        // Publish/subscribe roundtrip
        test_publish_subscribe_roundtrip(
            $publisher,
            &format!("{}-roundtrip", $prefix),
            &format!("{}-sub-roundtrip", $prefix),
        )
        .await;
        println!("  test_publish_subscribe_roundtrip: PASSED");

        // Publish only
        test_publish_only($publisher, &format!("{}-publish-only", $prefix)).await;
        println!("  test_publish_only: PASSED");

        // Multiple messages
        test_multiple_messages(
            $publisher,
            &format!("{}-multi", $prefix),
            &format!("{}-sub-multi", $prefix),
            5,
        )
        .await;
        println!("  test_multiple_messages: PASSED");

        // Domain filtering
        test_domain_filtering(
            $publisher,
            &format!("{}-target", $prefix),
            &format!("{}-other", $prefix),
            &format!("{}-sub-filter", $prefix),
        )
        .await;
        println!("  test_domain_filtering: PASSED");

        // Multiple handlers independent
        test_multiple_handlers_independent(
            $publisher,
            &format!("{}-mh", $prefix),
            &format!("{}-sub-mh1", $prefix),
            &format!("{}-sub-mh2", $prefix),
        )
        .await;
        println!("  test_multiple_handlers_independent: PASSED");

        // Metadata preservation
        test_routing_metadata_preserved(
            $publisher,
            &format!("{}-meta", $prefix),
            &format!("{}-sub-meta", $prefix),
        )
        .await;
        println!("  test_routing_metadata_preserved: PASSED");

        // Payload bytes exact
        test_payload_bytes_exact(
            $publisher,
            &format!("{}-bytes", $prefix),
            &format!("{}-sub-bytes", $prefix),
        )
        .await;
        println!("  test_payload_bytes_exact: PASSED");
    };
}

/// Run the all-domains subscription test against a transport that supports
/// subscribing to every domain (AMQP `#`, Kafka topic regex).
#[macro_export]
macro_rules! run_all_domains_subscription_test {
    ($publisher:expr, $prefix:expr) => {
        $crate::bus::event_bus_tests::test_multi_domain_subscription(
            $publisher,
            &format!("{}-md1", $prefix),
            &format!("{}-md2", $prefix),
            &format!("{}-sub-multi-domain", $prefix),
        )
        .await;
        println!("  test_multi_domain_subscription: PASSED");
    };
}

/// Run the per-root ordering contract test against a bus implementation
/// that documents per-aggregate-root ordering as a guarantee (SNS/SQS
/// FIFO, NATS JetStream, Kafka with keying, Pub/Sub with
/// `enable_message_ordering=true`).
///
/// Separated from `run_event_bus_tests!` because some bus implementations
/// (e.g., the in-process channel bus) do not claim per-root ordering
/// across concurrent publishers and would fail a contract test that
/// applies to ordered transports only.
#[macro_export]
macro_rules! run_per_root_ordering_test {
    ($publisher_arc:expr, $prefix:expr) => {
        use $crate::bus::event_bus_tests::test_per_root_ordering_under_concurrent_publish;

        // 4 roots × 10 events = 40 events total. Enough roots that
        // accidental ordering on a single partition is improbable;
        // enough events per root that a single reordering will surface.
        // We expect an `Arc<dyn EventBus>` (or other clonable Arc-like)
        // so concurrent producers can share the publisher without
        // requiring `B: Clone`.
        test_per_root_ordering_under_concurrent_publish(
            $publisher_arc,
            &format!("{}-ordered", $prefix),
            &format!("{}-sub-ordered", $prefix),
            4,
            10,
        )
        .await;
        println!("  test_per_root_ordering_under_concurrent_publish: PASSED");
    };
}

/// T12: every contract fn in this module must be wired somewhere — see
/// `crate::bus::assert_contract_inventory`. The per-fn
/// `#[allow(dead_code)]` markers above exist because each broker binary
/// runs only the subset its transport supports; this inventory test
/// guards against a fn losing its LAST caller silently (the T1 rot class
/// that left phantom DLQ helpers uncompiled for months).
#[test]
fn event_bus_contract_inventory_is_fully_wired() {
    crate::bus::assert_contract_inventory(
        include_str!("event_bus_tests.rs"),
        "// Test runner macro",
        &[
            include_str!("../bus_amqp.rs"),
            include_str!("../bus_kafka.rs"),
            include_str!("../bus_pubsub.rs"),
            include_str!("../bus_sns_sqs.rs"),
        ],
        &[],
    );
}
