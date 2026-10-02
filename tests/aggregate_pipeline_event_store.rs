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
use angzarr::discovery::StaticServiceDiscovery;
use angzarr::orchestration::aggregate::{ClientLogic, FactContext};
use angzarr::proto::command_handler_coordinator_service_server::CommandHandlerCoordinatorService;
use angzarr::proto::{
    business_response, command_page, event_page, page_header, AngzarrDeferredSequence,
    BusinessResponse, CascadeErrorMode, CommandBook, CommandPage, CommandRequest,
    ContextualCommand, Cover, Edition, EventBook, EventPage, MergeStrategy, PageHeader, SyncMode,
    Uuid as ProtoUuid,
};
use angzarr::repository::SnapshotRepository;
use angzarr::services::AggregateService;
use angzarr::storage::{EventStore, SqliteEventStore, SqliteSnapshotStore};
use async_trait::async_trait;

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

/// A stale MERGE_STRICT command (sequence 0 when the aggregate is already
/// at 1) is rejected with `FailedPrecondition` -- the retryable
/// sequence-conflict signal -- and adds no event (merge_strategy.feature,
/// STRICT). Against a real store rather than a mock.
#[tokio::test]
async fn pipeline_rejects_stale_strict_command_against_sqlite_store() {
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

    // Stale STRICT command at sequence 0 -- the store already has an event
    // at 0, so this must fail.
    business
        .enqueue(event_book("orders", root, None, vec![event_page(1)]))
        .await;
    let mut stale_command = command_book("orders", root, 0, None);
    stale_command.pages[0].merge_strategy = MergeStrategy::MergeStrict as i32;
    let stale = service.handle_command(send(stale_command)).await;
    assert!(stale.is_err(), "stale STRICT command must be rejected");
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

/// A stale MERGE_COMMUTATIVE command whose fields do not overlap the
/// events it did not see is merged: its event lands at the head
/// (merge_strategy.feature C-0150). The test double's Replay returns a
/// constant state, so the window and the command touch no common field.
#[tokio::test]
async fn pipeline_merges_stale_disjoint_commutative_command_against_sqlite_store() {
    let (service, business, store) = create_service_with_sqlite().await;
    let root = Uuid::new_v4();

    business
        .enqueue(event_book("orders", root, None, vec![event_page(0)]))
        .await;
    service
        .handle_command(send(command_book("orders", root, 0, None)))
        .await
        .expect("first command persists");

    business
        .enqueue(event_book("orders", root, None, vec![event_page(1)]))
        .await;
    service
        .handle_command(send(command_book("orders", root, 0, None)))
        .await
        .expect("a disjoint stale COMMUTATIVE command is merged");

    let persisted = store.get("orders", "", root).await.expect("get");
    assert_eq!(persisted.len(), 2, "the merged event lands at the head");
    assert_eq!(event_sequence_num(&persisted[1]), 1);
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
