//! Integration test: aggregate pipeline -> real SqliteEventStore
//!
//! Run with:
//! ```bash
//! cargo test --test aggregate_pipeline_event_store --features "test-utils" -- --nocapture
//! ```
//!
//! Closes the highest-leverage Category B drift gap from the R2-15
//! audit. The existing `storage_sqlite.rs` integration tests exercise
//! `EventStore.add` directly via a macro suite -- they prove the store
//! impl conforms to its trait contract. The unit tests for the
//! aggregate service (`services/aggregate.test.rs`) drive the pipeline
//! through `MockEventStore` -- they prove the orchestration calls the
//! trait correctly. Nothing bridged the two: a pipeline bug that
//! happens to be papered over by `MockEventStore` (which doesn't
//! enforce every SQL-level invariant the real store does) would not
//! surface in either layer.
//!
//! This test wires `AggregateService` to a real in-memory
//! `SqliteEventStore` and sends commands through `handle_command` --
//! the production gRPC entry point. After each command, it inspects
//! the persisted rows directly via `EventStore.get` to verify the
//! pipeline-specific shape:
//!
//! - Sequence stamping (first event at 0, second at 1).
//! - Sequence-conflict rejection (stale sequence -> FailedPrecondition).
//! - Edition propagation (command on a branch persists under the
//!   branch's edition, not the default).

#![cfg(feature = "test-utils")]

use std::collections::VecDeque;
use std::sync::Arc;

use prost_types::Any;
use sqlx::sqlite::SqlitePoolOptions;
use tokio::sync::Mutex;
use tonic::{Code, Request};
use uuid::Uuid;

use angzarr::bus::MockEventBus;
use angzarr::cascade::CascadeReaper;
use angzarr::discovery::StaticServiceDiscovery;
use angzarr::orchestration::aggregate::{
    transform_for_two_phase, ClientLogic, FactContext, TwoPhaseContext,
};
use angzarr::proto::command_handler_coordinator_service_server::CommandHandlerCoordinatorService;
use angzarr::proto::{
    business_response, command_page, event_page, page_header, AngzarrDeferredSequence,
    BusinessResponse, CascadeErrorMode, CommandBook, CommandPage, CommandRequest, Confirmation,
    ContextualCommand, Cover, Edition, EventBook, EventPage, MergeStrategy, NoOp, PageHeader,
    Revocation, Snapshot, SnapshotRetention, SyncMode, Uuid as ProtoUuid,
};
use angzarr::proto_ext::{type_url, EventPageExt};
use angzarr::repository::{EventBookRepository, SnapshotRepository};
use angzarr::services::{AggregateService, EventQueryService};
use angzarr::storage::{AddMeta, EventStore, SqliteEventStore, SqliteSnapshotStore};
use async_trait::async_trait;
use prost::Message;

// ============================================================================
// Test fixtures
// ============================================================================

/// Minimal `ClientLogic` test double: returns pre-enqueued event books
/// in FIFO order. Each command invocation pops the next queued
/// response. Fact invocations get their own queue.
struct QueuedClientLogic {
    responses: Mutex<VecDeque<EventBook>>,
    fact_responses: Mutex<VecDeque<EventBook>>,
    invocations: Mutex<Vec<ContextualCommand>>,
}

impl QueuedClientLogic {
    fn new() -> Self {
        Self {
            responses: Mutex::new(VecDeque::new()),
            fact_responses: Mutex::new(VecDeque::new()),
            invocations: Mutex::new(Vec::new()),
        }
    }

    async fn enqueue(&self, events: EventBook) {
        self.responses.lock().await.push_back(events);
    }
}

#[async_trait]
impl ClientLogic for QueuedClientLogic {
    async fn invoke(&self, cmd: ContextualCommand) -> Result<BusinessResponse, tonic::Status> {
        self.invocations.lock().await.push(cmd);
        let events = self.responses.lock().await.pop_front().unwrap_or_default();
        Ok(BusinessResponse {
            result: Some(business_response::Result::Events(events)),
        })
    }

    async fn invoke_fact(&self, ctx: FactContext) -> Result<EventBook, tonic::Status> {
        Ok(self
            .fact_responses
            .lock()
            .await
            .pop_front()
            .unwrap_or(ctx.facts))
    }

    /// Constant empty state: the COMMUTATIVE post-exec field-overlap check
    /// replays prior vs prior+received and diffs the two states. A constant
    /// state diffs to "no fields touched", so commutative-merge commands
    /// pass the gate — without this the trait-default `replay`
    /// (Unimplemented) degrades the gate to STRICT and rejects any deferred
    /// command against an aggregate with prior history.
    async fn replay(&self, _events: &EventBook) -> Result<Any, tonic::Status> {
        Ok(Any {
            type_url: "test.State".to_string(),
            value: vec![],
        })
    }
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

async fn create_sqlite_snapshot_repo() -> Arc<SnapshotRepository> {
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

/// Full variant that also returns the event bus and snapshot repo handles,
/// needed by the C01 cascade acceptance tests below to assert on bus
/// publishes and snapshot state directly. `create_service_with_sqlite`
/// (below) is the pre-existing narrower helper every other test in this
/// file uses; it now just discards the two extra handles.
async fn create_service_with_sqlite_full() -> (
    AggregateService,
    Arc<QueuedClientLogic>,
    Arc<SqliteEventStore>,
    Arc<MockEventBus>,
    Arc<SnapshotRepository>,
) {
    let event_store = create_sqlite_event_store().await;
    let snapshot_repo = create_sqlite_snapshot_repo().await;
    let business = Arc::new(QueuedClientLogic::new());
    let event_bus = Arc::new(MockEventBus::new());
    let discovery = Arc::new(StaticServiceDiscovery::new());

    let service = AggregateService::with_business_logic(
        event_store.clone(),
        snapshot_repo.clone(),
        business.clone(),
        event_bus.clone(),
        discovery,
    );
    (service, business, event_store, event_bus, snapshot_repo)
}

async fn create_service_with_sqlite() -> (
    AggregateService,
    Arc<QueuedClientLogic>,
    Arc<SqliteEventStore>,
) {
    let (service, business, event_store, _bus, _snapshot_repo) =
        create_service_with_sqlite_full().await;
    (service, business, event_store)
}

fn proto_uuid(u: Uuid) -> ProtoUuid {
    ProtoUuid {
        value: u.as_bytes().to_vec(),
    }
}

fn cover(domain: &str, root: Uuid, edition: Option<&str>) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(proto_uuid(root)),
        correlation_id: String::new(),
        edition: edition.map(|name| Edition {
            name: name.to_string(),
            divergences: vec![],
        }),
        ext: None,
    }
}

fn command_book(domain: &str, root: Uuid, sequence: u32, edition: Option<&str>) -> CommandBook {
    CommandBook {
        cover: Some(cover(domain, root, edition)),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(sequence)),
            }),
            payload: Some(command_page::Payload::Command(Any {
                type_url: "test.Command".to_string(),
                value: vec![],
            })),
            merge_strategy: MergeStrategy::MergeCommutative as i32,
        }],
    }
}

fn event_page(seq: u32) -> EventPage {
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(seq)),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: "test.Event".to_string(),
            value: vec![],
        })),
        created_at: None,
        ..Default::default()
    }
}

fn event_book(domain: &str, root: Uuid, edition: Option<&str>, pages: Vec<EventPage>) -> EventBook {
    EventBook {
        cover: Some(cover(domain, root, edition)),
        pages,
        snapshot: None,
        ..Default::default()
    }
}

fn send(command_book: CommandBook) -> Request<CommandRequest> {
    Request::new(CommandRequest {
        command: Some(command_book),
        sync_mode: SyncMode::Async as i32,
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
        cascade_id: None,
    })
}

/// Same as `send`, but with an explicit `cascade_id` — enables 2PC: the
/// aggregate stamps every new page `no_commit=true` for this command.
fn send_with_cascade(command_book: CommandBook, cascade_id: &str) -> Request<CommandRequest> {
    Request::new(CommandRequest {
        command: Some(command_book),
        sync_mode: SyncMode::Async as i32,
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
        cascade_id: Some(cascade_id.to_string()),
    })
}

fn event_sequence_num(page: &EventPage) -> u32 {
    match page.header.as_ref().and_then(|h| h.sequence_type.as_ref()) {
        Some(page_header::SequenceType::Sequence(s)) => *s,
        other => panic!("expected Sequence variant, got {other:?}"),
    }
}

// ============================================================================
// Tests
// ============================================================================

/// A single command flows through the pipeline and persists exactly
/// one event at sequence 0 in the SQLite event store.
///
/// What this catches that lib tests don't: the pipeline's persist
/// call goes through `EventStore.add`'s real SQL path -- column
/// types, transaction handling, sequence column constraints. A bug
/// in the pipeline that produces a malformed insert payload would
/// surface here but not in MockEventStore-based unit tests.
#[tokio::test]
async fn pipeline_persists_single_event_at_sequence_zero() {
    let (service, business, store) = create_service_with_sqlite().await;
    let root = Uuid::new_v4();

    business
        .enqueue(event_book("orders", root, None, vec![event_page(0)]))
        .await;

    let response = service
        .handle_command(send(command_book("orders", root, 0, None)))
        .await;
    assert!(response.is_ok(), "pipeline returned: {:?}", response.err());

    let persisted = store
        .get("orders", "", root)
        .await
        .expect("event_store.get");
    assert_eq!(persisted.len(), 1, "expected exactly 1 persisted event");
    assert_eq!(event_sequence_num(&persisted[0]), 0);
}

/// Two commands against the same root produce two events with
/// monotonically increasing sequences (0, 1). The pipeline must
/// re-read the prior state between commands and stamp the next
/// sequence correctly -- a regression in `next_sequence` calculation
/// would surface here.
#[tokio::test]
async fn pipeline_increments_sequence_across_two_commands() {
    let (service, business, store) = create_service_with_sqlite().await;
    let root = Uuid::new_v4();

    business
        .enqueue(event_book("orders", root, None, vec![event_page(0)]))
        .await;
    business
        .enqueue(event_book("orders", root, None, vec![event_page(1)]))
        .await;

    let r1 = service
        .handle_command(send(command_book("orders", root, 0, None)))
        .await;
    assert!(r1.is_ok(), "first command failed: {:?}", r1.err());
    let r2 = service
        .handle_command(send(command_book("orders", root, 1, None)))
        .await;
    assert!(r2.is_ok(), "second command failed: {:?}", r2.err());

    let persisted = store
        .get("orders", "", root)
        .await
        .expect("event_store.get");
    assert_eq!(persisted.len(), 2, "expected 2 persisted events");
    assert_eq!(event_sequence_num(&persisted[0]), 0);
    assert_eq!(event_sequence_num(&persisted[1]), 1);
}

/// A stale command (sequence 0 when the aggregate is already at 1)
/// must be rejected with `FailedPrecondition` -- the framework's
/// sequence-conflict signal -- and must NOT add a second event at
/// sequence 0. Mirrors the production single-sequence-check contract
/// against a real store rather than a mock.
#[tokio::test]
async fn pipeline_rejects_stale_sequence_against_sqlite_store() {
    let (service, business, store) = create_service_with_sqlite().await;
    let root = Uuid::new_v4();

    // Persist event at sequence 0 via a successful first command.
    business
        .enqueue(event_book("orders", root, None, vec![event_page(0)]))
        .await;
    let first = service
        .handle_command(send(command_book("orders", root, 0, None)))
        .await;
    assert!(first.is_ok());

    // Stale command at sequence 0 -- the store already has an event
    // at 0, so this must fail.
    business
        .enqueue(event_book("orders", root, None, vec![event_page(0)]))
        .await;
    let stale = service
        .handle_command(send(command_book("orders", root, 0, None)))
        .await;
    assert!(stale.is_err(), "stale-sequence command must be rejected");
    let status = stale.unwrap_err();
    assert_eq!(
        status.code(),
        Code::FailedPrecondition,
        "stale-sequence rejection must be FailedPrecondition, got {:?}: {}",
        status.code(),
        status.message()
    );

    // No additional events should have been written.
    let persisted = store
        .get("orders", "", root)
        .await
        .expect("event_store.get");
    assert_eq!(
        persisted.len(),
        1,
        "stale-sequence rejection must not add a second event; got {} events",
        persisted.len()
    );
}

/// A command with `cover.edition = "branch-a"` persists its event
/// under edition "branch-a", not the default edition. This pins the
/// pipeline's edition-extraction-and-propagation logic against the
/// real store's edition column.
#[tokio::test]
async fn pipeline_propagates_edition_to_event_store_writes() {
    let (service, business, store) = create_service_with_sqlite().await;
    let root = Uuid::new_v4();

    business
        .enqueue(event_book(
            "orders",
            root,
            Some("branch-a"),
            vec![event_page(0)],
        ))
        .await;

    let response = service
        .handle_command(send(command_book("orders", root, 0, Some("branch-a"))))
        .await;
    assert!(response.is_ok(), "pipeline returned: {:?}", response.err());

    // Event should be visible under the branch edition, NOT the default.
    let branch = store.get("orders", "branch-a", root).await.expect("get");
    assert_eq!(
        branch.len(),
        1,
        "expected 1 event under edition 'branch-a', got {}",
        branch.len()
    );
    let default = store.get("orders", "", root).await.expect("get default");
    assert_eq!(
        default.len(),
        0,
        "default-edition read must NOT see branch events; got {}",
        default.len()
    );
}

// ============================================================================
// O1: deferred-idempotency key must distinguish commands of one invocation
// ============================================================================

/// Build a saga-produced command: `AngzarrDeferred` header carrying full
/// source provenance (source cover + seq + producing component + the
/// command's position in the invocation's emitted list).
fn deferred_command_book(
    domain: &str,
    root: Uuid,
    source_domain: &str,
    source_root: Uuid,
    source_seq: u32,
    source_component: &str,
    command_index: u32,
) -> CommandBook {
    CommandBook {
        cover: Some(cover(domain, root, None)),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::AngzarrDeferred(
                    AngzarrDeferredSequence {
                        source: Some(cover(source_domain, source_root, None)),
                        source_seq,
                        source_component: source_component.to_string(),
                        command_index,
                    },
                )),
            }),
            payload: Some(command_page::Payload::Command(Any {
                type_url: "test.Command".to_string(),
                value: vec![],
            })),
            merge_strategy: MergeStrategy::MergeCommutative as i32,
        }],
    }
}

/// O1 collision regression: ONE saga/PM invocation emits TWO commands at the
/// same destination root. Both must execute.
///
/// Pre-fix, the deferred-idempotency key was only (source cover, source_seq)
/// — identical for every command of the invocation. The second command's
/// idempotency lookup matched the FIRST command's persisted events, so the
/// pipeline returned them as a cached duplicate and the second command was
/// silently lost in normal operation. command_index now disambiguates.
#[tokio::test]
async fn pipeline_executes_all_commands_of_one_invocation() {
    let (service, business, store) = create_service_with_sqlite().await;
    let root = Uuid::new_v4();
    let source_root = Uuid::new_v4();

    business
        .enqueue(event_book("inventory", root, None, vec![event_page(0)]))
        .await;
    business
        .enqueue(event_book("inventory", root, None, vec![event_page(1)]))
        .await;

    let first = service
        .handle_command(send(deferred_command_book(
            "inventory",
            root,
            "orders",
            source_root,
            7,
            "saga-orders-inventory",
            0,
        )))
        .await;
    assert!(first.is_ok(), "first command failed: {:?}", first.err());

    let second = service
        .handle_command(send(deferred_command_book(
            "inventory",
            root,
            "orders",
            source_root,
            7,
            "saga-orders-inventory",
            1,
        )))
        .await;
    assert!(second.is_ok(), "second command failed: {:?}", second.err());

    let invocations = business.invocations.lock().await.len();
    assert_eq!(
        invocations, 2,
        "second command of the invocation was swallowed by the deferred-idempotency \
         check instead of reaching the handler (O1 collision)"
    );

    let persisted = store.get("inventory", "", root).await.expect("get");
    assert_eq!(
        persisted.len(),
        2,
        "both commands of the invocation must persist their events; got {}",
        persisted.len()
    );
}

/// O1 companion: two DISTINCT components react to the same source event and
/// each sends a command to the same destination root. Both must execute —
/// the producing component's name is part of the idempotency key.
#[tokio::test]
async fn pipeline_executes_commands_from_distinct_components_on_same_trigger() {
    let (service, business, store) = create_service_with_sqlite().await;
    let root = Uuid::new_v4();
    let source_root = Uuid::new_v4();

    business
        .enqueue(event_book("inventory", root, None, vec![event_page(0)]))
        .await;
    business
        .enqueue(event_book("inventory", root, None, vec![event_page(1)]))
        .await;

    let from_saga = service
        .handle_command(send(deferred_command_book(
            "inventory",
            root,
            "orders",
            source_root,
            7,
            "saga-orders-inventory",
            0,
        )))
        .await;
    assert!(
        from_saga.is_ok(),
        "saga command failed: {:?}",
        from_saga.err()
    );

    let from_pm = service
        .handle_command(send(deferred_command_book(
            "inventory",
            root,
            "orders",
            source_root,
            7,
            "pm-fulfillment",
            0,
        )))
        .await;
    assert!(from_pm.is_ok(), "PM command failed: {:?}", from_pm.err());

    let invocations = business.invocations.lock().await.len();
    assert_eq!(
        invocations, 2,
        "a second component's command was swallowed by the first component's \
         idempotency claim (O1 collision)"
    );

    let persisted = store.get("inventory", "", root).await.expect("get");
    assert_eq!(persisted.len(), 2);
}

/// The point of the deferred-idempotency check, re-pinned with the widened
/// key: an exact redelivery (same source, seq, component, AND index) must
/// still be deduplicated — cached events returned, handler NOT re-invoked.
#[tokio::test]
async fn pipeline_dedupes_exact_redelivery_of_deferred_command() {
    let (service, business, store) = create_service_with_sqlite().await;
    let root = Uuid::new_v4();
    let source_root = Uuid::new_v4();

    business
        .enqueue(event_book("inventory", root, None, vec![event_page(0)]))
        .await;

    let delivery = deferred_command_book(
        "inventory",
        root,
        "orders",
        source_root,
        7,
        "saga-orders-inventory",
        0,
    );

    let first = service.handle_command(send(delivery.clone())).await;
    assert!(first.is_ok(), "first delivery failed: {:?}", first.err());

    let redelivery = service.handle_command(send(delivery)).await;
    assert!(
        redelivery.is_ok(),
        "redelivery must succeed with cached events: {:?}",
        redelivery.err()
    );

    let invocations = business.invocations.lock().await.len();
    assert_eq!(
        invocations, 1,
        "exact redelivery must be served from the idempotency cache, not re-invoke \
         the handler"
    );

    let persisted = store.get("inventory", "", root).await.expect("get");
    assert_eq!(persisted.len(), 1, "redelivery must not double-write");
}

// ============================================================================
// C01 — 2PC cascade visibility loop: the four remediation-plan acceptance
// tests. The 2PC model suppresses provisional (`no_commit=true`) cascade
// pages from the bus on the promise that a confirmation-point republish,
// full-stream gap-fill resolution, and correlation-query resolution make
// them eventually visible again. These four tests are the "done" bar for
// that design being complete rather than half-built.
// ============================================================================

/// (a) cascade commit -> suppressed events observed on the bus.
///
/// Command 1 runs under `cascade_id` (2PC): its event persists with
/// `no_commit=true` and must be suppressed from the bus (O2) at that
/// moment. Command 2 (no `cascade_id`) emits a Confirmation naming that
/// sequence. `post_persist`'s new confirmation-point republish (C01 #1)
/// must then push the ORIGINAL suppressed event onto the bus — completing
/// the design's producer/consumer halves.
#[tokio::test]
async fn cascade_confirmation_republishes_suppressed_event_to_bus() {
    let (service, business, store, bus, _snapshot_repo) = create_service_with_sqlite_full().await;
    let root = Uuid::new_v4();

    business
        .enqueue(event_book("orders", root, None, vec![event_page(0)]))
        .await;
    let first = service
        .handle_command(send_with_cascade(
            command_book("orders", root, 0, None),
            "cascade-accept-1",
        ))
        .await;
    assert!(first.is_ok(), "cascade command failed: {:?}", first.err());

    assert_eq!(
        bus.published_count().await,
        0,
        "the provisional event must be suppressed from the bus at its own \
         persist time (O2) — it belongs to an in-flight cascade"
    );
    let persisted = store.get("orders", "", root).await.expect("get");
    assert_eq!(persisted.len(), 1);
    assert!(
        persisted[0].no_commit,
        "cascade event must be stamped provisional in storage"
    );

    // Command 2: confirming call (no cascade_id). Business logic emits a
    // Confirmation for sequence 0's cascade.
    let confirmation = Confirmation {
        target: None,
        sequences: vec![0],
        cascade_id: "cascade-accept-1".to_string(),
    };
    let marker_page = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(1)),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: type_url::CONFIRMATION.to_string(),
            value: confirmation.encode_to_vec(),
        })),
        created_at: None,
        ..Default::default()
    };
    business
        .enqueue(event_book("orders", root, None, vec![marker_page]))
        .await;
    let second = service
        .handle_command(send(command_book("orders", root, 1, None)))
        .await;
    assert!(
        second.is_ok(),
        "confirming command failed: {:?}",
        second.err()
    );

    let published = bus.take_published().await;
    assert_eq!(
        published.len(),
        2,
        "expected the republished confirmed event (first, per the documented \
         ordering) plus the confirming command's own ordinary committed publish"
    );
    assert_eq!(
        published[0].pages.len(),
        1,
        "republished book carries exactly the confirmed sequence"
    );
    assert_eq!(published[0].pages[0].sequence_num(), 0);
    assert_eq!(
        published[0].pages[0].type_url(),
        Some("test.Event"),
        "republished page must carry the ORIGINAL suppressed event's payload, \
         not a placeholder"
    );
}

/// (b) cascade revoke -> snapshot never reflects the revoked state, and a
/// full replay (raw pages resolved with the SAME transform, independent of
/// any snapshot) agrees with what a snapshot-aware read would produce.
///
/// Command 1 runs under `cascade_id` and ALSO returns a snapshot in its
/// response (as if it captured post-event state) — finding #2's exact
/// trap: pre-fix, `persist_snapshot_if_present` ran unconditionally and
/// would have baked the not-yet-confirmed event into a durable snapshot.
/// The reaper then times the cascade out (Revocation). The snapshot must
/// never have existed to diverge from the resolved (revoked) event view.
#[tokio::test]
async fn cascade_revoke_snapshot_never_reflects_revoked_state() {
    let (service, business, store, _bus, snapshot_repo) = create_service_with_sqlite_full().await;
    let root = Uuid::new_v4();

    let mut provisional_book = event_book("orders", root, None, vec![event_page(0)]);
    provisional_book.snapshot = Some(Snapshot {
        sequence: 0, // recomputed by the persist helper
        state: Some(Any {
            type_url: "test.State".to_string(),
            value: vec![9, 9, 9],
        }),
        retention: SnapshotRetention::RetentionDefault as i32,
        created_at: None,
    });
    business.enqueue(provisional_book).await;

    let first = service
        .handle_command(send_with_cascade(
            command_book("orders", root, 0, None),
            "cascade-revoke-1",
        ))
        .await;
    assert!(first.is_ok(), "cascade command failed: {:?}", first.err());

    // C01 #2: snapshot must be DEFERRED while the cascade is in flight —
    // never baked in.
    let snapshot = snapshot_repo.get("orders", "", root).await.unwrap();
    assert!(
        snapshot.is_none(),
        "snapshot must not be persisted while the cascade is in flight"
    );

    // Simulate the reaper timing this cascade out.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let reaper = CascadeReaper::new(store.clone(), std::time::Duration::from_secs(0))
        .with_snapshot_repo(snapshot_repo.clone());
    let revoked = reaper.run_once().await.expect("reaper run_once");
    assert_eq!(revoked, 1, "reaper must revoke the stale provisional event");

    // Post-revoke: still no snapshot. If a regression reintroduced eager
    // snapshot persistence, the reaper's delete-on-revoke backstop would
    // need to have cleaned it up here — this assertion catches either
    // failure mode.
    let snapshot_after = snapshot_repo.get("orders", "", root).await.unwrap();
    assert!(
        snapshot_after.is_none(),
        "snapshot must never reflect revoked cascade state"
    );

    // "Full replay agrees": resolve the RAW stream via the same canonical
    // 2PC transform the aggregate's own read path uses, independent of any
    // snapshot. Event 0 must resolve as revoked (NoOp) — this is the state
    // a snapshot-enabled read would ALSO produce, since no snapshot exists
    // to diverge from.
    let raw_pages = store.get("orders", "", root).await.expect("get raw");
    let raw_book = EventBook {
        pages: raw_pages,
        ..Default::default()
    };
    let full_replay = transform_for_two_phase(&raw_book, &TwoPhaseContext::standard()).events;

    assert_eq!(
        full_replay.pages.len(),
        2,
        "provisional event + Revocation marker"
    );
    let noop: NoOp = full_replay.pages[0]
        .decode_typed()
        .expect("event 0 must resolve as a NoOp placeholder (revoked)");
    assert_eq!(noop.reason, "revoked");
    assert_eq!(noop.original_sequence, 0);
}

/// (c) gap-fill over a range whose Confirmation is OUTSIDE the range
/// delivers the confirmed events (C01 #6).
///
/// A gap-filling consumer backfills the exact hole `[1, 3)` left by O2's
/// suppression. The Confirmation marker that resolves it lives at
/// sequence 3 — outside that window. Pre-fix, `get_from_to`'s marker
/// visibility was bounded by the requested window and would withhold the
/// confirmed pages forever unless some unrelated later read happened to
/// include sequence 3 too.
#[tokio::test]
async fn gap_fill_range_resolves_confirmation_outside_window() {
    let store = create_sqlite_event_store().await;
    let snapshot_repo = create_sqlite_snapshot_repo().await;
    let root = Uuid::new_v4();

    store
        .add("orders", "", root, vec![event_page(0)], &AddMeta::default())
        .await
        .unwrap();

    let mut p1 = event_page(1);
    p1.no_commit = true;
    p1.cascade_id = Some("cascade-gf".to_string());
    let mut p2 = event_page(2);
    p2.no_commit = true;
    p2.cascade_id = Some("cascade-gf".to_string());
    store
        .add("orders", "", root, vec![p1, p2], &AddMeta::default())
        .await
        .unwrap();

    let confirmation = Confirmation {
        target: None,
        sequences: vec![1, 2],
        cascade_id: "cascade-gf".to_string(),
    };
    let marker = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(3)),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: type_url::CONFIRMATION.to_string(),
            value: confirmation.encode_to_vec(),
        })),
        created_at: None,
        ..Default::default()
    };
    store
        .add("orders", "", root, vec![marker], &AddMeta::default())
        .await
        .unwrap();

    let repo = EventBookRepository::new(store, snapshot_repo);

    // Gap-fill request for exactly the hole [1, 3) — the marker at seq 3
    // sits OUTSIDE this window.
    let book = repo
        .get_from_to("orders", "", root, 1, 3)
        .await
        .expect("get_from_to");

    assert_eq!(book.pages.len(), 2, "range [1,3) must stay contiguous");
    assert_eq!(
        book.pages[0].type_url(),
        Some("test.Event"),
        "confirmed page 1 must deliver its real payload, not a placeholder"
    );
    assert_eq!(
        book.pages[1].type_url(),
        Some("test.Event"),
        "confirmed page 2 must deliver its real payload, not a placeholder"
    );
}

/// (d) correlation-id query never returns raw `no_commit` pages (C01 #7).
///
/// `EventStore::get_by_correlation` filters at the storage layer by the
/// `correlation_id` column — the reaper's Revocation marker carries an
/// EMPTY correlation_id (it's framework plumbing, not tied to any client
/// workflow) and so is invisible to that filter. `EventQueryService` must
/// still resolve the provisional page against its root's FULL stream
/// before returning it, or a saga/PM reconstructing workflow state via
/// correlation_id would see the cancelled business event as if live.
#[tokio::test]
async fn correlation_query_never_returns_raw_no_commit_pages() {
    let store = create_sqlite_event_store().await;
    let snapshot_pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect("sqlite::memory:")
        .await
        .expect("connect SQLite snapshot pool");
    sqlx::migrate!("./migrations/sqlite")
        .run(&snapshot_pool)
        .await
        .expect("run sqlite migrations");
    let snapshot_store: Arc<dyn angzarr::storage::SnapshotStore> =
        Arc::new(SqliteSnapshotStore::new(snapshot_pool));

    let query_service = EventQueryService::new(store.clone(), snapshot_store);
    let root = Uuid::new_v4();
    let correlation_id = "corr-2pc-query";

    let mut provisional = event_page(0);
    provisional.no_commit = true;
    provisional.cascade_id = Some("cascade-query".to_string());
    store
        .add(
            "orders",
            "",
            root,
            vec![provisional],
            &AddMeta {
                correlation_id,
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();

    // Reaper-style Revocation: no correlation_id.
    let revocation = Revocation {
        target: None,
        sequences: vec![0],
        cascade_id: "cascade-query".to_string(),
        reason: "test".to_string(),
    };
    let marker = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(1)),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: type_url::REVOCATION.to_string(),
            value: revocation.encode_to_vec(),
        })),
        created_at: None,
        ..Default::default()
    };
    store
        .add("orders", "", root, vec![marker], &AddMeta::default())
        .await
        .unwrap();

    let query = angzarr::proto::Query {
        cover: Some(Cover {
            domain: String::new(),
            root: None,
            correlation_id: correlation_id.to_string(),
            edition: None,
            ext: None,
        }),
        selection: None,
    };

    use angzarr::proto::event_query_service_server::EventQueryService as EventQueryTrait;
    let response = query_service
        .get_event_book(Request::new(query))
        .await
        .expect("correlation query must succeed");
    let book = response.into_inner();

    assert_eq!(
        book.pages.len(),
        1,
        "only sequence 0 was ever stamped with this correlation_id"
    );
    assert_ne!(
        book.pages[0].type_url(),
        Some("test.Event"),
        "correlation query must NOT return the raw revoked business event"
    );
    let noop: NoOp = book.pages[0]
        .decode_typed()
        .expect("withheld page must be a NoOp placeholder");
    assert_eq!(noop.reason, "revoked");
}
