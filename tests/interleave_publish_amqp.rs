//! B1 end-to-end verification: persisted events ALWAYS reach the bus.
//!
//! The 2026-04-07 full-sail bug: a HandleEvent fact interleaved with a
//! HandleCommand on the same aggregate caused the command's events to be
//! persisted but NEVER published — the transient post-persist publish
//! failure triggered a whole-pipeline retry, the re-execution classified
//! as NoOp, and `publish_unless_noop` (H-16) skipped the publish while
//! returning success. Fix (B1, `c2fa2beb`): `publish_unless_noop` retries
//! the publish IN PLACE with the exact persisted book; exhaustion goes to
//! the DLQ capture hook instead of silently succeeding.
//!
//! This suite is the e2e pin for that class, chosen over reviving the
//! original full-sail repro (separate app, mid-migration): the REAL
//! aggregate pipeline (`AggregateService::handle_command` /
//! `handle_event`) writes to a REAL SQLite event store and publishes
//! through a REAL RabbitMQ broker:
//!
//! - `injected_publish_failure_still_reaches_subscriber` pins the fix
//!   deterministically: the first publish attempt fails (injected
//!   transient), and the subscriber must still receive the event via the
//!   in-place retry. Pre-fix, the pipeline rerun classified NoOp and the
//!   event never arrived.
//! - `interleaved_fact_and_command_publish_parity` replays the original
//!   race shape: facts and commands interleaving on the same root, with
//!   the invariant that EVERY event in the store is observed on the bus.
//!
//! Run with:
//! cargo test --test interleave_publish_amqp --features "amqp test-utils"

#![cfg(all(feature = "amqp", feature = "test-utils"))]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use prost_types::Any;
use sqlx::sqlite::SqlitePoolOptions;
use tokio::sync::mpsc;
use tonic::Request;
use uuid::Uuid;

use angzarr::bus::amqp::{AmqpConfig, AmqpEventBus};
use angzarr::bus::{BusError, EventBus, PublishResult};
use angzarr::discovery::StaticServiceDiscovery;
use angzarr::orchestration::aggregate::{ClientLogic, FactContext};
use angzarr::proto::command_handler_coordinator_service_server::CommandHandlerCoordinatorService;
use angzarr::proto::{
    business_response, command_page, event_page, page_header, BusinessResponse, CascadeErrorMode,
    CommandBook, CommandPage, CommandRequest, ContextualCommand, Cover, EventBook, EventPage,
    EventRequest, ExternalDeferredSequence, MergeStrategy, PageHeader, SyncMode, Uuid as ProtoUuid,
};
use angzarr::repository::SnapshotRepository;
use angzarr::services::AggregateService;
use angzarr::storage::{EventStore, SqliteEventStore, SqliteSnapshotStore};
use angzarr::test_utils::CapturingHandler;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

// ============================================================================
// Broker harness (mirrors tests/bus_amqp.rs)
// ============================================================================

async fn start_rabbitmq() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    let image = GenericImage::new("rabbitmq", "3-management")
        .with_exposed_port(5672.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Server startup complete"));

    let container = image
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("Failed to start rabbitmq container");

    tokio::time::sleep(Duration::from_secs(2)).await;

    let host_port = container
        .get_host_port_ipv4(5672)
        .await
        .expect("Failed to get mapped port");

    let host = match std::env::var("TESTCONTAINERS_HOST") {
        Ok(h) => h,
        Err(_) => container
            .get_host()
            .await
            .expect("Failed to get container host")
            .to_string(),
    };

    (
        container,
        format!("amqp://guest:guest@{}:{}", host, host_port),
    )
}

// ============================================================================
// Fault injection: fail the first N publishes, pass through afterwards
// ============================================================================

struct FailFirstPublishes {
    inner: Arc<dyn EventBus>,
    remaining_failures: AtomicUsize,
    attempts: AtomicUsize,
}

impl FailFirstPublishes {
    fn new(inner: Arc<dyn EventBus>, failures: usize) -> Self {
        Self {
            inner,
            remaining_failures: AtomicUsize::new(failures),
            attempts: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl EventBus for FailFirstPublishes {
    async fn publish(&self, book: Arc<EventBook>) -> angzarr::bus::Result<PublishResult> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        let should_fail = self
            .remaining_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if should_fail {
            return Err(BusError::Publish(
                "injected transient broker failure".to_string(),
            ));
        }
        self.inner.publish(book).await
    }

    async fn subscribe(
        &self,
        handler: Box<dyn angzarr::bus::EventHandler>,
    ) -> angzarr::bus::Result<()> {
        self.inner.subscribe(handler).await
    }

    async fn start_consuming(&self) -> angzarr::bus::Result<()> {
        self.inner.start_consuming().await
    }

    async fn create_subscriber(
        &self,
        name: &str,
        domain_filter: Option<&str>,
    ) -> angzarr::bus::Result<Arc<dyn EventBus>> {
        self.inner.create_subscriber(name, domain_filter).await
    }

    fn max_message_size(&self) -> Option<usize> {
        self.inner.max_message_size()
    }
}

// ============================================================================
// Business-logic double: emits one event continuing the prior history
// ============================================================================

/// Unlike a queued double, this computes the event sequence from the
/// prior events the pipeline hands it — deterministic under interleaving,
/// exactly like a real aggregate handler.
struct ContinuingClientLogic;

fn next_sequence(events: Option<&EventBook>) -> u32 {
    events
        .map(|book| {
            book.pages
                .iter()
                .filter_map(
                    |p| match p.header.as_ref().and_then(|h| h.sequence_type.as_ref()) {
                        Some(page_header::SequenceType::Sequence(s)) => Some(*s + 1),
                        _ => None,
                    },
                )
                .max()
                .unwrap_or(0)
        })
        .unwrap_or(0)
}

#[async_trait]
impl ClientLogic for ContinuingClientLogic {
    async fn invoke(&self, cmd: ContextualCommand) -> Result<BusinessResponse, tonic::Status> {
        let seq = next_sequence(cmd.events.as_ref());
        let cover = cmd.command.as_ref().and_then(|c| c.cover.clone());
        Ok(BusinessResponse {
            result: Some(business_response::Result::Events(EventBook {
                cover,
                pages: vec![event_page(seq, "test.CommandEvent")],
                snapshot: None,
                ..Default::default()
            })),
        })
    }

    async fn invoke_fact(&self, ctx: FactContext) -> Result<EventBook, tonic::Status> {
        Ok(ctx.facts)
    }
}

// ============================================================================
// Builders
// ============================================================================

fn proto_uuid(u: Uuid) -> ProtoUuid {
    ProtoUuid {
        value: u.as_bytes().to_vec(),
    }
}

fn cover(domain: &str, root: Uuid) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(proto_uuid(root)),
        correlation_id: format!("b1-{root}"),
        edition: None,
        ext: None,
    }
}

fn event_page(seq: u32, type_url: &str) -> EventPage {
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(seq)),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: format!("type.googleapis.com/{type_url}"),
            value: vec![],
        })),
        created_at: None,
        ..Default::default()
    }
}

fn command_book(domain: &str, root: Uuid, sequence: u32) -> CommandBook {
    CommandBook {
        cover: Some(cover(domain, root)),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(sequence)),
            }),
            payload: Some(command_page::Payload::Command(Any {
                type_url: "type.googleapis.com/test.Command".to_string(),
                value: vec![],
            })),
            merge_strategy: MergeStrategy::MergeCommutative as i32,
        }],
    }
}

/// External facts carry ExternalDeferredSequence markers (the framework
/// stamps the actual sequence on persist) and no Angzarr source provenance
/// — exactly the full-sail fact shape from the original bug report.
fn fact_book(domain: &str, root: Uuid, external_id: &str) -> EventBook {
    EventBook {
        cover: Some(cover(domain, root)),
        pages: vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::ExternalDeferred(
                    ExternalDeferredSequence {
                        external_id: external_id.to_string(),
                        description: "b1 interleave e2e".to_string(),
                    },
                )),
            }),
            payload: Some(event_page::Payload::Event(Any {
                type_url: "type.googleapis.com/test.FactEvent".to_string(),
                value: vec![],
            })),
            created_at: None,
            ..Default::default()
        }],
        snapshot: None,
        ..Default::default()
    }
}

fn command_request(book: CommandBook) -> Request<CommandRequest> {
    Request::new(CommandRequest {
        command: Some(book),
        sync_mode: SyncMode::Async as i32,
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
    })
}

fn fact_request(book: EventBook) -> Request<EventRequest> {
    Request::new(EventRequest {
        events: Some(book),
        sync_mode: SyncMode::Async as i32,
        // false = route through handle_fact (the safe proto3 default;
        // same behavior the old routing bool opted into explicitly).
        skip_handler: false,
    })
}

async fn create_sqlite_event_store() -> Arc<SqliteEventStore> {
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect("sqlite::memory:")
        .await
        .expect("connect SQLite pool");
    sqlx::migrate!("./migrations/sqlite")
        .run(&pool)
        .await
        .expect("run sqlite migrations");
    Arc::new(SqliteEventStore::new(pool))
}

async fn create_snapshot_repo() -> Arc<SnapshotRepository> {
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect("sqlite::memory:")
        .await
        .expect("connect SQLite snapshot pool");
    sqlx::migrate!("./migrations/sqlite")
        .run(&pool)
        .await
        .expect("run sqlite migrations");
    Arc::new(SnapshotRepository::new(Arc::new(SqliteSnapshotStore::new(
        pool,
    ))))
}

fn page_seq(page: &EventPage) -> u32 {
    match page.header.as_ref().and_then(|h| h.sequence_type.as_ref()) {
        Some(page_header::SequenceType::Sequence(s)) => *s,
        other => panic!("expected Sequence variant, got {other:?}"),
    }
}

/// Attach a capturing AMQP subscriber to `domain`; relies on the T10
/// readiness contract (consumer established when start_consuming returns).
async fn attach_subscriber(
    publisher: &AmqpEventBus,
    name: &str,
    domain: &str,
) -> mpsc::Receiver<EventBook> {
    let subscriber = publisher
        .create_subscriber(name, Some(domain))
        .await
        .expect("create_subscriber");
    let (tx, rx) = mpsc::channel(256);
    subscriber
        .subscribe(Box::new(CapturingHandler::new(tx)))
        .await
        .expect("subscribe");
    subscriber.start_consuming().await.expect("start_consuming");
    rx
}

// ============================================================================
// Tests
// ============================================================================

/// Deterministic pin of the B1 fix: the FIRST publish attempt for a
/// persisted book fails transiently. Pre-`c2fa2beb`, the pipeline retried
/// itself, reclassified the attempt as NoOp, skipped the publish, and
/// returned success — the event sat in the store forever while no
/// subscriber ever saw it. Post-fix the publish retries IN PLACE and the
/// subscriber receives the event.
#[tokio::test]
async fn injected_publish_failure_still_reaches_subscriber() {
    let (_container, url) = start_rabbitmq().await;
    let domain = format!("b1f_{}", &Uuid::new_v4().simple().to_string()[..8]);

    let amqp = Arc::new(
        AmqpEventBus::new(AmqpConfig::publisher(&url))
            .await
            .expect("create AMQP publisher"),
    );
    let mut rx = attach_subscriber(&amqp, &format!("{domain}-sub"), &domain).await;

    let flaky = Arc::new(FailFirstPublishes::new(amqp.clone(), 1));
    let store = create_sqlite_event_store().await;
    let service = AggregateService::with_business_logic(
        store.clone(),
        create_snapshot_repo().await,
        Arc::new(ContinuingClientLogic),
        flaky.clone(),
        Arc::new(StaticServiceDiscovery::new()),
    );

    let root = Uuid::new_v4();
    let response = service
        .handle_command(command_request(command_book(&domain, root, 0)))
        .await;
    assert!(
        response.is_ok(),
        "pipeline should succeed via in-place publish retry: {:?}",
        response.err()
    );

    // The persisted event must arrive despite the injected first failure.
    let received = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect(
            "persisted event never reached the AMQP subscriber — the \
             persisted-but-never-published bug (B1) is back",
        )
        .expect("channel closed");
    assert_eq!(received.cover.as_ref().unwrap().domain, domain);
    assert_eq!(received.pages.len(), 1);
    assert_eq!(page_seq(&received.pages[0]), 0);

    assert!(
        flaky.attempts.load(Ordering::SeqCst) >= 2,
        "expected the publish to be retried in place after the injected failure"
    );

    let persisted = store.get(&domain, "", root).await.expect("store get");
    assert_eq!(persisted.len(), 1, "exactly one persisted event");
}

/// The original full-sail race shape: HandleEvent facts interleaving with
/// HandleCommands on the same aggregate root. Invariant: every event that
/// lands in the store is observed on the bus — persisted-vs-published
/// parity. The stale-sequence command (MERGE_COMMUTATIVE) racing the fact
/// is exactly the condition that manufactured the silent skip pre-fix.
#[tokio::test]
async fn interleaved_fact_and_command_publish_parity() {
    const ROUNDS: usize = 10;

    let (_container, url) = start_rabbitmq().await;
    let domain = format!("b1i_{}", &Uuid::new_v4().simple().to_string()[..8]);

    let amqp = Arc::new(
        AmqpEventBus::new(AmqpConfig::publisher(&url))
            .await
            .expect("create AMQP publisher"),
    );
    let mut rx = attach_subscriber(&amqp, &format!("{domain}-sub"), &domain).await;

    let store = create_sqlite_event_store().await;
    let service = Arc::new(AggregateService::with_business_logic(
        store.clone(),
        create_snapshot_repo().await,
        Arc::new(ContinuingClientLogic),
        amqp.clone(),
        Arc::new(StaticServiceDiscovery::new()),
    ));

    let mut roots = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let root = Uuid::new_v4();
        roots.push(root);

        // Both target the same fresh root: the fact claims sequence 0 and
        // the command claims sequence 0 — whichever loses the race forces
        // the other path through conflict handling, the bug's trigger.
        let svc_a = service.clone();
        let domain_a = domain.clone();
        let fact = tokio::spawn(async move {
            // Facts are contractually unrejectable, but the fact path has
            // no in-pipeline conflict retry: a concurrent same-root write
            // surfaces FailedPrecondition to the caller (observed while
            // building this test — filed as a follow-up). External fact
            // sources redeliver, so mirror that here; the external_id
            // keeps the retry idempotent.
            let mut last = None;
            for _ in 0..3 {
                match svc_a
                    .handle_event(fact_request(fact_book(
                        &domain_a,
                        root,
                        &format!("b1-fact-{root}"),
                    )))
                    .await
                {
                    Ok(resp) => return Ok(resp),
                    Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                        last = Some(status);
                    }
                    Err(status) => return Err(status),
                }
            }
            Err(last.unwrap())
        });
        let svc_b = service.clone();
        let domain_b = domain.clone();
        let store_b = store.clone();
        let command = tokio::spawn(async move {
            // Real clients reissue at the corrected sequence on a stale
            // write (see aggregate_client.feature). The bug under test was
            // never the rejection — it was a command that SUCCEEDED while
            // its events silently skipped the bus.
            let mut seq = 0u32;
            for attempt in 0..3 {
                match svc_b
                    .handle_command(command_request(command_book(&domain_b, root, seq)))
                    .await
                {
                    Ok(resp) => return Ok(resp),
                    Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                        seq = store_b
                            .get_next_sequence(&domain_b, "", root)
                            .await
                            .expect("get_next_sequence");
                        if attempt == 2 {
                            return Err(status);
                        }
                    }
                    Err(status) => return Err(status),
                }
            }
            unreachable!("retry loop returns")
        });

        let (fact_res, cmd_res) = tokio::join!(fact, command);
        fact_res
            .expect("fact task panicked")
            .expect("fact injection failed");
        cmd_res
            .expect("command task panicked")
            .expect("command failed after stale-sequence retries");
    }

    // Ground truth: everything the store persisted, across all roots.
    let mut persisted_total = 0usize;
    for root in &roots {
        persisted_total += store.get(&domain, "", *root).await.expect("get").len();
    }
    assert!(
        persisted_total >= ROUNDS * 2,
        "expected at least 2 events per round persisted, got {persisted_total}"
    );

    // Parity: the subscriber must observe every persisted event.
    let mut received = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while received < persisted_total {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(book)) => received += book.pages.len(),
            _ => break,
        }
    }
    assert_eq!(
        received, persisted_total,
        "persisted-vs-published parity violated: {persisted_total} events in \
         the store but only {received} observed on the bus — the \
         persisted-but-never-published class (B1) has regressed"
    );
}
