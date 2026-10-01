//! Tests for the consumer-side handler decorators.
//!
//! Every bus transport redelivers an event whose handler fails. These
//! decorators decide (a) whether a sidecar sees an event at all and
//! (b) when a repeatedly failing event stops being redelivered and is
//! dead-lettered instead of blocking its key forever.

use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;

use super::*;
use crate::dlq::{ChannelDeadLetterPublisher, DlqError, RejectionDetails};
use crate::proto::{event_page, Cover, EventPage, Uuid as ProtoUuid};

// ============================================================================
// Test doubles
// ============================================================================

/// Handler that fails its first `fail_times` calls, then succeeds.
struct FlakyHandler {
    calls: Arc<AtomicU32>,
    fail_times: u32,
}

impl EventHandler for FlakyHandler {
    fn handle(&self, _book: Arc<EventBook>) -> BoxFuture<'static, Result<(), BusError>> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let fail = n <= self.fail_times;
        Box::pin(async move {
            if fail {
                Err(BusError::SagaFailed {
                    name: "s".into(),
                    message: "downstream unavailable".into(),
                })
            } else {
                Ok(())
            }
        })
    }
}

fn flaky(fail_times: u32) -> (Box<dyn EventHandler>, Arc<AtomicU32>) {
    let calls = Arc::new(AtomicU32::new(0));
    (
        Box::new(FlakyHandler {
            calls: Arc::clone(&calls),
            fail_times,
        }),
        calls,
    )
}

/// DLQ publisher that is configured but always fails.
struct FailingDlq;

#[async_trait]
impl DeadLetterPublisher for FailingDlq {
    async fn publish(&self, _dead_letter: AngzarrDeadLetter) -> Result<(), DlqError> {
        Err(DlqError::Connection("dlq down".into()))
    }
}

fn book(domain: &str, type_url: &str, root: u8) -> Arc<EventBook> {
    Arc::new(EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(ProtoUuid {
                value: vec![root; 16],
            }),
            ..Default::default()
        }),
        pages: vec![EventPage {
            payload: Some(event_page::Payload::Event(prost_types::Any {
                type_url: type_url.to_string(),
                value: vec![],
            })),
            ..Default::default()
        }],
        ..Default::default()
    })
}

fn policy(max_attempts: u32) -> DeliveryConfig {
    DeliveryConfig {
        max_attempts,
        initial_backoff_ms: 100,
        max_backoff_ms: 1_000,
    }
}

// ============================================================================
// TargetFilterHandler
// ============================================================================

/// An event from a subscribed domain with a subscribed type reaches the
/// client.
#[tokio::test]
async fn target_filter_passes_matching_event() {
    let (inner, calls) = flaky(0);
    let handler = TargetFilterHandler::new(inner, vec![Target::new("order", vec!["OrderCreated"])]);

    handler
        .handle(book("order", "type.googleapis.com/x.OrderCreated", 1))
        .await
        .expect("matching event handled");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// Events from other domains or of unsubscribed types are acknowledged
/// without a client call — the review's "invokes its client for every
/// event on the bus".
#[tokio::test]
async fn target_filter_skips_unsubscribed_domain_and_type() {
    let (inner, calls) = flaky(u32::MAX);
    let handler = TargetFilterHandler::new(inner, vec![Target::new("order", vec!["OrderCreated"])]);

    handler
        .handle(book("inventory", "type.googleapis.com/x.OrderCreated", 1))
        .await
        .expect("other domain is acknowledged");
    handler
        .handle(book("order", "type.googleapis.com/x.OrderShipped", 1))
        .await
        .expect("unsubscribed type is acknowledged");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

/// No targets means "everything" (projector default).
#[tokio::test]
async fn target_filter_without_targets_passes_everything() {
    let (inner, calls) = flaky(0);
    let handler = TargetFilterHandler::new(inner, Vec::new());

    handler
        .handle(book("anything", "type.googleapis.com/x.Any", 1))
        .await
        .expect("handled");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

// ============================================================================
// DeadLetteringHandler
// ============================================================================

/// Below the budget a failure propagates (so the transport redelivers),
/// after waiting the backoff for that attempt.
#[tokio::test(start_paused = true)]
async fn failure_below_budget_propagates_after_backoff() {
    let (inner, _calls) = flaky(u32::MAX);
    let (dlq, mut rx) = ChannelDeadLetterPublisher::new();
    let handler = DeadLetteringHandler::new(inner, policy(3), Arc::new(dlq), "fulfil", "saga");

    let start = tokio::time::Instant::now();
    let result = handler
        .handle(book("order", "type.googleapis.com/x.OrderCreated", 1))
        .await;

    assert!(result.is_err(), "first failure must be redelivered");
    assert_eq!(start.elapsed().as_millis(), 100, "first backoff");
    assert!(rx.try_recv().is_err(), "nothing dead-lettered yet");
}

/// At the budget the event is dead-lettered with its payload, attempt
/// count and component, and acknowledged so its key is unblocked.
#[tokio::test(start_paused = true)]
async fn failure_at_budget_dead_letters_and_acknowledges() {
    let (inner, calls) = flaky(u32::MAX);
    let (dlq, mut rx) = ChannelDeadLetterPublisher::new();
    let handler = DeadLetteringHandler::new(inner, policy(3), Arc::new(dlq), "fulfil", "saga");
    let event = book("order", "type.googleapis.com/x.OrderCreated", 1);

    assert!(handler.handle(Arc::clone(&event)).await.is_err());
    assert!(handler.handle(Arc::clone(&event)).await.is_err());
    handler
        .handle(Arc::clone(&event))
        .await
        .expect("third failure is dead-lettered and acknowledged");

    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let dead = rx.try_recv().expect("dead letter published");
    assert_eq!(dead.source_component, "fulfil");
    assert_eq!(dead.source_component_type, "saga");
    assert_eq!(
        dead.cover.as_ref().map(|c| c.domain.as_str()),
        Some("order")
    );
    match dead.rejection_details {
        Some(RejectionDetails::EventProcessingFailed(d)) => {
            assert_eq!(d.retry_count, 3);
            assert!(d.error.contains("downstream unavailable"), "{}", d.error);
        }
        other => panic!("expected EventProcessingFailed, got {other:?}"),
    }
}

/// Failure counts are per event: another event's failures do not spend
/// this event's budget.
#[tokio::test(start_paused = true)]
async fn budgets_are_tracked_per_event() {
    let (inner, _calls) = flaky(u32::MAX);
    let (dlq, mut rx) = ChannelDeadLetterPublisher::new();
    let handler = DeadLetteringHandler::new(inner, policy(2), Arc::new(dlq), "c", "saga");

    assert!(handler.handle(book("order", "t/x.A", 1)).await.is_err());
    assert!(handler.handle(book("order", "t/x.A", 2)).await.is_err());
    assert!(rx.try_recv().is_err(), "each event has failed only once");
}

/// A success clears the event's count: a later failure starts a fresh
/// budget instead of being dead-lettered early.
#[tokio::test(start_paused = true)]
async fn success_resets_the_failure_count() {
    let (inner, calls) = flaky(1);
    let (dlq, mut rx) = ChannelDeadLetterPublisher::new();
    let handler = DeadLetteringHandler::new(inner, policy(2), Arc::new(dlq), "c", "saga");
    let event = book("order", "t/x.A", 1);

    assert!(handler.handle(Arc::clone(&event)).await.is_err());
    handler.handle(Arc::clone(&event)).await.expect("recovers");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(rx.try_recv().is_err());
}

/// Without a configured DLQ an exhausted event is never dropped: it keeps
/// failing (so the transport keeps it) at the maximum backoff.
#[tokio::test(start_paused = true)]
async fn exhausted_without_dlq_keeps_event_on_bus() {
    let (inner, _calls) = flaky(u32::MAX);
    let handler = DeadLetteringHandler::new(
        inner,
        policy(1),
        Arc::new(crate::dlq::NoopDeadLetterPublisher),
        "c",
        "saga",
    );

    let start = tokio::time::Instant::now();
    let result = handler.handle(book("order", "t/x.A", 1)).await;
    assert!(result.is_err(), "unconfigured DLQ must not drop the event");
    assert_eq!(start.elapsed().as_millis(), 100);
}

/// A failing DLQ publish must not acknowledge the event.
#[tokio::test(start_paused = true)]
async fn dlq_publish_failure_keeps_event_on_bus() {
    let (inner, _calls) = flaky(u32::MAX);
    let handler = DeadLetteringHandler::new(inner, policy(1), Arc::new(FailingDlq), "c", "saga");

    let result = handler.handle(book("order", "t/x.A", 1)).await;
    assert!(result.is_err(), "event must stay on the bus when DLQ fails");
}

/// `max_attempts = 0` never dead-letters.
#[tokio::test(start_paused = true)]
async fn unlimited_budget_never_dead_letters() {
    let (inner, _calls) = flaky(u32::MAX);
    let (dlq, mut rx) = ChannelDeadLetterPublisher::new();
    let handler = DeadLetteringHandler::new(inner, policy(0), Arc::new(dlq), "c", "saga");
    let event = book("order", "t/x.A", 1);

    for _ in 0..5 {
        assert!(handler.handle(Arc::clone(&event)).await.is_err());
    }
    assert!(rx.try_recv().is_err());
}

/// The tracking table is bounded; overflowing it only resets counts.
#[test]
fn failure_table_is_bounded() {
    let failures = Mutex::new(HashMap::new());
    for key in 0..MAX_TRACKED_EVENTS as u64 {
        DeadLetteringHandler::record_failure(&failures, key);
    }
    assert_eq!(failures.lock().unwrap().len(), MAX_TRACKED_EVENTS);

    assert_eq!(DeadLetteringHandler::record_failure(&failures, u64::MAX), 1);
    assert_eq!(failures.lock().unwrap().len(), 1);

    // An already-tracked key keeps counting without clearing.
    assert_eq!(DeadLetteringHandler::record_failure(&failures, u64::MAX), 2);
}
