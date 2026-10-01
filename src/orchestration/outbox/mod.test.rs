//! Coordinator outbox (compensation_delivery.feature): an obligation is
//! recorded before the trigger is acknowledged, delivered at least once with
//! backoff, dead-lettered when the budget is spent or the target cannot
//! handle it, and survives a restart.

use super::*;

use std::collections::VecDeque;
use std::sync::Mutex as StdMutex;

use crate::dlq::{DlqError, RejectionDetails};
use crate::proto::{
    command_page, page_header::SequenceType, AngzarrDeferredSequence, CommandPage, Cover,
    PageHeader, Uuid as ProtoUuid,
};
use crate::storage::mock::MockEventStore;

// -- fixtures ---------------------------------------------------------------

fn cover(domain: &str, root: u8) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(ProtoUuid {
            value: vec![root; 16],
        }),
        correlation_id: "corr-1".to_string(),
        edition: None,
        ext: None,
    }
}

/// A deferred ReserveStock to inventory sku-1 from saga OrderFulfillment,
/// triggered by order/order-1 at sequence 0.
fn reserve_stock(command_index: u32) -> CommandBook {
    CommandBook {
        cover: Some(cover("inventory", 2)),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::AngzarrDeferred(AngzarrDeferredSequence {
                    source: Some(cover("order", 1)),
                    source_seq: 0,
                    source_component: "OrderFulfillment".to_string(),
                    command_index,
                })),
            }),
            payload: Some(command_page::Payload::Command(prost_types::Any {
                type_url: "/inventory.ReserveStock".to_string(),
                value: vec![],
            })),
            merge_strategy: 0,
        }],
    }
}

fn rejection(command_index: u32) -> OutboxEntry {
    let envelope = crate::orchestration::compensation::rejection_envelope(
        &reserve_stock(command_index),
        "out of stock",
    )
    .unwrap();
    OutboxEntry::notification(envelope).unwrap()
}

fn compensate(command_index: u32) -> OutboxEntry {
    let envelope = crate::orchestration::compensation::compensate_envelope(
        &reserve_stock(command_index),
        None,
        "card declined",
    );
    OutboxEntry::notification(envelope).unwrap()
}

/// Answers each attempt from a script (then Delivered), recording what it
/// was asked to deliver.
#[derive(Default)]
struct ScriptedDeliverer {
    script: StdMutex<VecDeque<DeliveryResult>>,
    delivered: StdMutex<Vec<OutboxEntry>>,
}

impl ScriptedDeliverer {
    fn failing(results: Vec<DeliveryResult>) -> Arc<Self> {
        Arc::new(Self {
            script: StdMutex::new(results.into()),
            ..Default::default()
        })
    }
    fn always(result: DeliveryResult, times: usize) -> Arc<Self> {
        Self::failing(vec![result; times])
    }
    fn attempts(&self) -> usize {
        self.delivered.lock().unwrap().len()
    }
}

#[async_trait]
impl OutboxDeliverer for ScriptedDeliverer {
    async fn deliver(&self, entry: &OutboxEntry) -> DeliveryResult {
        self.delivered.lock().unwrap().push(entry.clone());
        self.script
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(DeliveryResult::Delivered)
    }
}

#[derive(Default)]
struct CapturingDlq(StdMutex<Vec<AngzarrDeadLetter>>);

#[async_trait]
impl DeadLetterPublisher for CapturingDlq {
    async fn publish(&self, dead_letter: AngzarrDeadLetter) -> Result<(), DlqError> {
        self.0.lock().unwrap().push(dead_letter);
        Ok(())
    }
}

impl CapturingDlq {
    fn taken(&self) -> Vec<AngzarrDeadLetter> {
        self.0.lock().unwrap().clone()
    }
}

/// A log whose writes fail.
struct BrokenLog;

#[async_trait]
impl OutboxLog for BrokenLog {
    async fn append_record(&self, _entry: &OutboxEntry) -> Result<(), OutboxError> {
        Err(OutboxError::Log("disk full".to_string()))
    }
    async fn append_attempt(&self, _key: &str, _error: &str) -> Result<(), OutboxError> {
        Err(OutboxError::Log("disk full".to_string()))
    }
    async fn append_close(&self, _key: &str) -> Result<(), OutboxError> {
        Err(OutboxError::Log("disk full".to_string()))
    }
    async fn open_entries(&self) -> Result<Vec<OutboxEntry>, OutboxError> {
        Ok(Vec::new())
    }
}

fn policy(max_attempts: u32) -> RetryPolicy {
    RetryPolicy {
        max_attempts,
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(10),
        jitter: false,
    }
}

fn outbox(deliverer: Arc<ScriptedDeliverer>, max_attempts: u32) -> (Outbox, Arc<CapturingDlq>) {
    let dlq = Arc::new(CapturingDlq::default());
    let outbox = Outbox::new(
        "OrderFulfillment",
        "saga",
        Arc::new(MemoryOutboxLog),
        deliverer,
        policy(max_attempts),
    )
    .with_dead_letters(dlq.clone());
    (outbox, dlq)
}

// -- recording and delivery ---------------------------------------------------

/// C-0464: a recorded notification is delivered once and the record closes.
#[tokio::test]
async fn submit_delivers_once_and_closes() {
    let deliverer = Arc::new(ScriptedDeliverer::default());
    let (outbox, dlq) = outbox(deliverer.clone(), 10);

    let entry = compensate(0);
    let stats = outbox.submit(entry.clone()).await.unwrap();

    assert_eq!(stats.delivered, 1);
    assert_eq!(deliverer.attempts(), 1);
    assert_eq!(deliverer.delivered.lock().unwrap()[0], entry);
    assert!(outbox.open_keys().await.is_empty(), "record closed");
    assert!(dlq.taken().is_empty());
}

/// Recording the same obligation twice keeps one entry.
#[tokio::test]
async fn record_is_idempotent_on_the_key() {
    let deliverer = Arc::new(ScriptedDeliverer::default());
    let (outbox, _) = outbox(deliverer.clone(), 10);

    outbox.record(rejection(0)).await.unwrap();
    outbox.record(rejection(0)).await.unwrap();
    assert_eq!(outbox.open_keys().await.len(), 1);

    let stats = outbox.drain_once().await.unwrap();
    assert_eq!(stats.delivered, 1);
    assert_eq!(deliverer.attempts(), 1);
}

/// C-0468: notifications that differ only in command_index are distinct
/// obligations; a notification and the command it concerns are distinct too.
#[tokio::test]
async fn keys_separate_command_index_and_kind() {
    let deliverer = Arc::new(ScriptedDeliverer::default());
    let (outbox, _) = outbox(deliverer.clone(), 10);

    outbox.record(rejection(0)).await.unwrap();
    outbox.record(rejection(1)).await.unwrap();
    outbox.record(compensate(0)).await.unwrap();
    outbox
        .record(OutboxEntry::command(reserve_stock(0)))
        .await
        .unwrap();

    assert_eq!(outbox.open_keys().await.len(), 4);
    assert_eq!(outbox.drain_once().await.unwrap().delivered, 4);
}

/// C-0463: an obligation that cannot be recorded is an error (the trigger
/// stays unacknowledged) and nothing is delivered.
#[tokio::test]
async fn unrecordable_obligation_fails_without_delivery() {
    let deliverer = Arc::new(ScriptedDeliverer::default());
    let outbox = Outbox::new(
        "OrderFulfillment",
        "saga",
        Arc::new(BrokenLog),
        deliverer.clone(),
        policy(10),
    );

    let err = outbox.submit(rejection(0)).await.unwrap_err();

    assert!(matches!(err, OutboxError::Log(_)));
    assert_eq!(deliverer.attempts(), 0);
    assert!(outbox.open_keys().await.is_empty());
}

// -- retry and backoff ---------------------------------------------------------

/// C-0466: a failed delivery is retried with backoff until it succeeds; each
/// retry waits longer than the one before; nothing is dead-lettered.
#[tokio::test]
async fn failed_delivery_retries_with_growing_backoff() {
    let deliverer = ScriptedDeliverer::always(DeliveryResult::Retryable("UNAVAILABLE".into()), 2);
    let (outbox, dlq) = outbox(deliverer.clone(), 10);
    let entry = rejection(0);
    let key = entry.key.clone();

    outbox.record(entry).await.unwrap();
    let t0 = Instant::now();
    assert_eq!(outbox.drain_due(t0).await.unwrap().retried, 1);
    let due1 = outbox.next_due(&key).await.unwrap();
    assert_eq!(due1 - t0, Duration::from_millis(100));

    // Not due yet: nothing is attempted.
    assert_eq!(outbox.drain_due(t0).await.unwrap(), DrainStats::default());
    assert_eq!(deliverer.attempts(), 1);

    assert_eq!(outbox.drain_due(due1).await.unwrap().retried, 1);
    let due2 = outbox.next_due(&key).await.unwrap();
    assert!(due2 - due1 > due1 - t0, "second retry waits longer");
    let pending = outbox.open_entry(&key).await.unwrap();
    assert_eq!(pending.attempts, 2);
    assert_eq!(pending.last_error, "UNAVAILABLE");

    assert_eq!(outbox.drain_due(due2).await.unwrap().delivered, 1);
    assert_eq!(deliverer.attempts(), 3, "delivery attempted 3 times");
    assert!(outbox.open_keys().await.is_empty());
    assert!(dlq.taken().is_empty(), "no dead letter is published");
}

/// C-0469: a notification whose delivery keeps failing is dead-lettered
/// after the budget, with the envelope and the attempt count and last error.
#[tokio::test]
async fn exhausted_notification_is_dead_lettered() {
    let deliverer = ScriptedDeliverer::always(
        DeliveryResult::Retryable("inventory service down".into()),
        9,
    );
    let (outbox, dlq) = outbox(deliverer.clone(), 3);
    let entry = compensate(0);
    let envelope = entry.book.clone();

    outbox.record(entry).await.unwrap();
    let mut stats = DrainStats::default();
    for hour in 1..=5 {
        let later = Instant::now() + Duration::from_secs(3600 * hour);
        stats.add(outbox.drain_due(later).await.unwrap());
    }

    assert_eq!(deliverer.attempts(), 3, "delivery attempted 3 times");
    assert_eq!(stats.retried, 2);
    assert_eq!(stats.dead_lettered, 1);
    assert!(outbox.open_keys().await.is_empty(), "record closed");
    let dead = dlq.taken();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].topic(), "angzarr.dlq.inventory");
    assert_eq!(dead[0].source_component, "OrderFulfillment");
    assert_eq!(dead[0].source_component_type, "saga");
    match &dead[0].payload {
        crate::dlq::DeadLetterPayload::Command(book) => assert_eq!(book, &envelope),
        other => panic!("expected the envelope, got {other:?}"),
    }
    match &dead[0].rejection_details {
        Some(RejectionDetails::CompensationDeliveryFailed(d)) => {
            assert_eq!(d.attempts, 3);
            assert_eq!(d.last_error, "inventory service down");
        }
        other => panic!("expected compensation_delivery_failed, got {other:?}"),
    }
}

/// C-0479: a target that answers UNIMPLEMENTED (no undo handler) is not
/// retried: the envelope is dead-lettered on that attempt.
#[tokio::test]
async fn unimplemented_notification_is_dead_lettered_at_once() {
    let deliverer = ScriptedDeliverer::failing(vec![DeliveryResult::Rejected {
        code: tonic::Code::Unimplemented,
        message: "no undo handler for inventory.CountStock".into(),
    }]);
    let (outbox, dlq) = outbox(deliverer.clone(), 10);

    let stats = outbox.submit(compensate(0)).await.unwrap();

    assert_eq!(stats.dead_lettered, 1);
    assert_eq!(deliverer.attempts(), 1, "delivery attempted once");
    assert!(outbox.open_keys().await.is_empty());
    match &dlq.taken()[0].rejection_details {
        Some(RejectionDetails::CompensationDeliveryFailed(d)) => assert_eq!(d.attempts, 1),
        other => panic!("expected compensation_delivery_failed, got {other:?}"),
    }
}

/// Any other rejection of a notification is retried.
#[tokio::test]
async fn other_notification_rejections_are_retried() {
    let deliverer = ScriptedDeliverer::failing(vec![DeliveryResult::Rejected {
        code: tonic::Code::InvalidArgument,
        message: "bad".into(),
    }]);
    let (outbox, dlq) = outbox(deliverer, 10);

    let stats = outbox.submit(rejection(0)).await.unwrap();

    assert_eq!(stats.retried, 1);
    assert_eq!(outbox.open_keys().await.len(), 1);
    assert!(dlq.taken().is_empty());
}

/// A command rejected on redelivery is final: dead-lettered, and its
/// rejection still reaches its source as a recorded RejectionNotification.
#[tokio::test]
async fn rejected_command_dead_letters_and_raises_its_rejection() {
    let deliverer = ScriptedDeliverer::failing(vec![
        DeliveryResult::Rejected {
            code: tonic::Code::FailedPrecondition,
            message: "out of stock".into(),
        },
        DeliveryResult::Retryable("order down".into()),
    ]);
    let (outbox, dlq) = outbox(deliverer.clone(), 10);

    let stats = outbox
        .submit(OutboxEntry::command(reserve_stock(0)))
        .await
        .unwrap();

    assert_eq!(stats.dead_lettered, 1);
    let dead = dlq.taken();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].topic(), "angzarr.dlq.inventory");
    match &dead[0].rejection_details {
        Some(RejectionDetails::EventProcessingFailed(d)) => assert!(!d.is_transient),
        other => panic!("expected event_processing_failed, got {other:?}"),
    }
    let open = outbox.open_keys().await;
    assert_eq!(open, vec![rejection(0).key], "the rejection is recorded");

    outbox
        .drain_due(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    let attempted = deliverer.delivered.lock().unwrap().clone();
    assert_eq!(attempted.len(), 2);
    assert_eq!(attempted[1].kind, ProvenanceKind::RejectionNotification);
    assert_eq!(attempted[1].book.domain(), "order", "routed to the source");
}

/// A command that keeps failing transiently is dead-lettered as transient
/// and raises no rejection.
#[tokio::test]
async fn exhausted_command_dead_letters_as_transient() {
    let deliverer = ScriptedDeliverer::always(DeliveryResult::Retryable("broker down".into()), 9);
    let (outbox, dlq) = outbox(deliverer, 2);

    outbox
        .submit(OutboxEntry::command(reserve_stock(0)))
        .await
        .unwrap();
    outbox
        .drain_due(Instant::now() + Duration::from_secs(3600))
        .await
        .unwrap();

    assert!(outbox.open_keys().await.is_empty());
    let dead = dlq.taken();
    assert_eq!(dead.len(), 1);
    match &dead[0].rejection_details {
        Some(RejectionDetails::EventProcessingFailed(d)) => {
            assert!(d.is_transient);
            assert_eq!(d.retry_count, 2);
        }
        other => panic!("expected event_processing_failed, got {other:?}"),
    }
}

/// An entry being attempted is not attempted again concurrently.
#[tokio::test]
async fn in_flight_entry_is_not_claimed_twice() {
    let deliverer = Arc::new(ScriptedDeliverer::default());
    let (outbox, _) = outbox(deliverer, 10);
    let entry = rejection(0);
    let key = entry.key.clone();
    outbox.record(entry).await.unwrap();

    assert!(outbox.claim(&key, None).await.is_some());
    assert!(outbox.claim(&key, None).await.is_none());
    assert!(outbox.claim("absent", None).await.is_none());
}

// -- durability ------------------------------------------------------------------

/// C-0465: a recorded obligation is delivered after a coordinator restart,
/// once, and its record is closed for good.
#[tokio::test]
async fn recorded_obligation_survives_a_restart() {
    let store: Arc<dyn crate::storage::EventStore> = Arc::new(MockEventStore::new());
    let before = Outbox::new(
        "OrderFulfillment",
        "saga",
        Arc::new(EventStoreOutboxLog::new(store.clone(), "OrderFulfillment")),
        Arc::new(ScriptedDeliverer::default()),
        policy(10),
    );
    let entry = compensate(0);
    before.record(entry.clone()).await.unwrap();
    drop(before);

    let deliverer = Arc::new(ScriptedDeliverer::default());
    let after = Outbox::new(
        "OrderFulfillment",
        "saga",
        Arc::new(EventStoreOutboxLog::new(store.clone(), "OrderFulfillment")),
        deliverer.clone(),
        policy(10),
    );
    assert_eq!(after.recover().await.unwrap(), 1);
    assert_eq!(after.drain_once().await.unwrap().delivered, 1);
    assert_eq!(deliverer.attempts(), 1);
    assert_eq!(deliverer.delivered.lock().unwrap()[0], entry);

    let again = Outbox::new(
        "OrderFulfillment",
        "saga",
        Arc::new(EventStoreOutboxLog::new(store, "OrderFulfillment")),
        Arc::new(ScriptedDeliverer::default()),
        policy(10),
    );
    assert_eq!(
        again.recover().await.unwrap(),
        0,
        "closed records stay closed"
    );
}

// -- policy and keys ----------------------------------------------------------

#[test]
fn retry_policy_defaults() {
    let policy = RetryPolicy::default();
    assert_eq!(policy.max_attempts, 10);
    assert_eq!(policy.initial_backoff, Duration::from_millis(200));
    assert_eq!(policy.max_backoff, Duration::from_secs(30));
    assert!(policy.jitter);
}

/// The backoff doubles per attempt and stops at the cap.
#[test]
fn retry_delay_doubles_up_to_the_cap() {
    let policy = RetryPolicy {
        max_attempts: 50,
        initial_backoff: Duration::from_millis(200),
        max_backoff: Duration::from_secs(1),
        jitter: false,
    };
    assert_eq!(policy.delay(1), Duration::from_millis(200));
    assert_eq!(policy.delay(2), Duration::from_millis(400));
    assert_eq!(policy.delay(3), Duration::from_millis(800));
    assert_eq!(policy.delay(4), Duration::from_secs(1));
    assert_eq!(policy.delay(40), Duration::from_secs(1));
    assert_eq!(policy.delay(0), Duration::from_millis(200));
}

/// With jitter each delay lies in the upper half of its unjittered value,
/// so a later retry never waits less than an earlier one below the cap.
#[test]
fn retry_delay_jitter_stays_in_the_upper_half() {
    let policy = RetryPolicy {
        jitter: true,
        ..RetryPolicy::default()
    };
    for attempt in 1..=5 {
        let base = RetryPolicy {
            jitter: false,
            ..policy
        }
        .delay(attempt);
        for _ in 0..50 {
            let delay = policy.delay(attempt);
            assert!(
                delay >= base / 2 && delay <= base,
                "{delay:?} outside [{:?}, {base:?}]",
                base / 2
            );
        }
    }
}

#[test]
fn entry_kinds_and_keys() {
    let command = OutboxEntry::command(reserve_stock(0));
    assert_eq!(command.kind, ProvenanceKind::Command);
    assert!(!command.is_notification());
    assert!(command.key.starts_with("command#"));
    assert!(command.key.contains("OrderFulfillment"));

    let notification = compensate(0);
    assert_eq!(notification.kind, ProvenanceKind::CompensateNotification);
    assert!(notification.is_notification());
    assert!(notification.key.starts_with("compensate-notification#"));

    assert_eq!(
        OutboxEntry::from_recorded(notification.book.clone()),
        notification
    );
    assert_eq!(OutboxEntry::from_recorded(reserve_stock(0)), command);
    assert!(matches!(
        OutboxEntry::notification(reserve_stock(0)),
        Err(OutboxError::InvalidEntry(_))
    ));
}

/// A command without deferred provenance is keyed by its target.
#[test]
fn entry_key_without_provenance_uses_the_target() {
    let mut book = reserve_stock(0);
    book.pages[0].header = Some(PageHeader {
        sync_mode: None,
        sequence_type: Some(SequenceType::Sequence(5)),
    });
    let key = entry_key(ProvenanceKind::Command, &book);
    assert_eq!(
        key,
        format!("command#inventory:{}:corr-1:5", hex::encode([2u8; 16]))
    );
}

/// A started outbox reloads what the log holds open and drains it in the
/// background with the configured schedule.
#[tokio::test]
async fn started_outbox_recovers_and_drains() {
    let store: Arc<dyn crate::storage::EventStore> = Arc::new(MockEventStore::new());
    let log = Arc::new(EventStoreOutboxLog::new(store.clone(), "OrderFulfillment"));
    log.append_record(&rejection(0)).await.unwrap();

    let deliverer = Arc::new(ScriptedDeliverer::default());
    let config = OutboxConfig {
        drain_interval_ms: 10,
        max_attempts: 4,
        ..OutboxConfig::default()
    };
    let outbox = Outbox::start(
        "OrderFulfillment",
        "saga",
        log,
        deliverer.clone(),
        &config,
        Arc::new(CapturingDlq::default()),
    )
    .await
    .unwrap();

    assert_eq!(outbox.policy().max_attempts, 4);
    for _ in 0..200 {
        if deliverer.attempts() > 0 && outbox.open_keys().await.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        deliverer.attempts(),
        1,
        "the recovered obligation is delivered once"
    );
    assert!(outbox.open_keys().await.is_empty());
}
