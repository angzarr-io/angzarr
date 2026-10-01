//! Integration test: aggregate pipeline -> real `SqliteSnapshotStore`.
//!
//! Run with:
//! ```bash
//! cargo test --test aggregate_pipeline_snapshot --features "test-utils" -- --nocapture
//! ```
//!
//! Sister test to `aggregate_pipeline_event_store.rs`. The pipeline's
//! snapshot-write step at `aggregate/grpc/mod.rs:543` calls into
//! `SnapshotRepository::put` only when the business response carries
//! a snapshot AND the repo's write policy allows it. `MockSnapshotStore`
//! in unit tests stores whatever it's handed without enforcing
//! SQL-level invariants (unique constraints, NULL handling).
//! `storage_sqlite.rs` covers `SnapshotStore.put` directly via the
//! trait surface. Neither sees the pipeline's specific write logic
//! against the real SQLite snapshot column shape.
//!
//! What this test pins:
//!
//! - Pipeline persists a snapshot when the business logic returns
//!   one in its `EventBook`.
//! - Snapshot write is gated on the business response actually
//!   carrying a snapshot (no snapshot in response -> no write).
//! - Edition propagates from the command cover to the snapshot
//!   storage key (snapshot persisted under "branch-x", not the
//!   default edition).
//! - On a subsequent command for the same root, the pipeline reads
//!   the prior snapshot from the store and threads it into the
//!   `ContextualCommand` passed to `ClientLogic.invoke` -- so the
//!   business logic sees the snapshot it persisted earlier.

#![cfg(feature = "test-utils")]

use std::collections::VecDeque;
use std::sync::Arc;

use prost_types::Any;
use sqlx::sqlite::SqlitePoolOptions;
use tokio::sync::Mutex;
use tonic::Request;
use uuid::Uuid;

use angzarr::bus::MockEventBus;
use angzarr::discovery::StaticServiceDiscovery;
use angzarr::orchestration::aggregate::{ClientLogic, FactContext};
use angzarr::proto::command_handler_coordinator_service_server::CommandHandlerCoordinatorService;
use angzarr::proto::{
    business_response, command_page, event_page, page_header, BusinessResponse, CascadeErrorMode,
    CommandBook, CommandPage, CommandRequest, ContextualCommand, Cover, Edition, EventBook,
    EventPage, MergeStrategy, PageHeader, Snapshot, SnapshotRetention, SyncMode, Uuid as ProtoUuid,
};
use angzarr::repository::SnapshotRepository;
use angzarr::services::AggregateService;
use angzarr::storage::{SnapshotStore, SqliteEventStore, SqliteSnapshotStore};
use async_trait::async_trait;

// ============================================================================
// Fixtures
// ============================================================================

/// Mirrors `aggregate_pipeline_event_store::QueuedClientLogic` but
/// extends `enqueue` to support per-response snapshots so each test
/// can stage a different snapshot policy.
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

    async fn last_invocation(&self) -> Option<ContextualCommand> {
        self.invocations.lock().await.last().cloned()
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
}

async fn create_sqlite_pool() -> sqlx::SqlitePool {
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect("sqlite::memory:")
        .await
        .expect("connect SQLite pool");
    sqlx::migrate!("./migrations/sqlite")
        .run(&pool)
        .await
        .expect("run sqlite migrations");
    pool
}

struct TestRig {
    service: AggregateService,
    business: Arc<QueuedClientLogic>,
    snapshot_store: Arc<SqliteSnapshotStore>,
}

async fn create_rig() -> TestRig {
    let event_store_pool = create_sqlite_pool().await;
    let snapshot_pool = create_sqlite_pool().await;

    let event_store = Arc::new(SqliteEventStore::new(event_store_pool));
    let snapshot_store = Arc::new(SqliteSnapshotStore::new(snapshot_pool));
    let snapshot_repo = Arc::new(SnapshotRepository::new(snapshot_store.clone()));
    let business = Arc::new(QueuedClientLogic::new());
    let event_bus = Arc::new(MockEventBus::new());
    let discovery = Arc::new(StaticServiceDiscovery::new());

    let service = AggregateService::with_business_logic(
        event_store,
        snapshot_repo,
        business.clone(),
        event_bus,
        discovery,
    );

    TestRig {
        service,
        business,
        snapshot_store,
    }
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

fn snapshot_at(seq: u32, state_bytes: Vec<u8>) -> Snapshot {
    Snapshot {
        sequence: seq,
        state: Some(Any {
            type_url: "test.State".to_string(),
            value: state_bytes,
        }),
        retention: SnapshotRetention::RetentionDefault as i32,
        created_at: None,
    }
}

fn event_book_with_snapshot(
    domain: &str,
    root: Uuid,
    edition: Option<&str>,
    pages: Vec<EventPage>,
    snapshot: Option<Snapshot>,
) -> EventBook {
    EventBook {
        cover: Some(cover(domain, root, edition)),
        pages,
        snapshot,
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

// ============================================================================
// Tests
// ============================================================================

/// When the business response carries a snapshot, the pipeline
/// persists it to the SQLite snapshot store under the same
/// (domain, edition, root) key the events landed under. Pins the
/// snapshot-write code path at `aggregate/grpc/mod.rs:543` against
/// a real backend instead of `MockSnapshotStore`.
#[tokio::test]
async fn pipeline_persists_snapshot_from_business_response() {
    let rig = create_rig().await;
    let root = Uuid::new_v4();

    rig.business
        .enqueue(event_book_with_snapshot(
            "orders",
            root,
            None,
            vec![event_page(0)],
            Some(snapshot_at(0, vec![1, 2, 3, 4])),
        ))
        .await;

    let r = rig
        .service
        .handle_command(send(command_book("orders", root, 0, None)))
        .await;
    assert!(r.is_ok(), "pipeline returned: {:?}", r.err());

    let snapshot = rig
        .snapshot_store
        .get("orders", "", root)
        .await
        .expect("snapshot_store.get");
    let snapshot = snapshot.expect("snapshot must be persisted");
    assert_eq!(snapshot.sequence, 0);
    assert_eq!(
        snapshot
            .state
            .as_ref()
            .map(|any| any.value.clone())
            .unwrap_or_default(),
        vec![1, 2, 3, 4],
        "snapshot state must round-trip the bytes the business logic returned"
    );
}

/// When the business response carries NO snapshot, the pipeline
/// does NOT call `SnapshotStore.put`. Pins the
/// "snapshot_changed" gate at `aggregate/grpc/mod.rs:543` against
/// a real store -- a mock that records every put would hide a
/// regression where the gate was deleted, but a real store with
/// `get` returning None is unambiguous.
#[tokio::test]
async fn pipeline_does_not_write_snapshot_when_business_returns_none() {
    let rig = create_rig().await;
    let root = Uuid::new_v4();

    rig.business
        .enqueue(event_book_with_snapshot(
            "orders",
            root,
            None,
            vec![event_page(0)],
            None, // <-- no snapshot
        ))
        .await;

    let r = rig
        .service
        .handle_command(send(command_book("orders", root, 0, None)))
        .await;
    assert!(r.is_ok(), "pipeline returned: {:?}", r.err());

    let snapshot = rig
        .snapshot_store
        .get("orders", "", root)
        .await
        .expect("snapshot_store.get");
    assert!(
        snapshot.is_none(),
        "no snapshot should be persisted when business returns none, got {snapshot:?}"
    );
}

/// A command with `cover.edition = "branch-a"` persists the
/// snapshot under that edition. Default-edition `get` sees no
/// snapshot. Mirrors the event-store edition test from the sister
/// suite, against the snapshot table.
#[tokio::test]
async fn pipeline_propagates_edition_to_snapshot_store() {
    let rig = create_rig().await;
    let root = Uuid::new_v4();

    rig.business
        .enqueue(event_book_with_snapshot(
            "orders",
            root,
            Some("branch-a"),
            vec![event_page(0)],
            Some(snapshot_at(0, vec![9; 8])),
        ))
        .await;

    let r = rig
        .service
        .handle_command(send(command_book("orders", root, 0, Some("branch-a"))))
        .await;
    assert!(r.is_ok(), "pipeline returned: {:?}", r.err());

    let branch = rig
        .snapshot_store
        .get("orders", "branch-a", root)
        .await
        .expect("snapshot get");
    assert!(
        branch.is_some(),
        "snapshot must be persisted under edition 'branch-a'"
    );
    assert_eq!(branch.as_ref().unwrap().sequence, 0);

    let default = rig
        .snapshot_store
        .get("orders", "", root)
        .await
        .expect("snapshot get default");
    assert!(
        default.is_none(),
        "default-edition read must NOT see branch snapshot, got {default:?}"
    );
}

/// On a second command for the same root, the pipeline reads the
/// prior snapshot from the SQLite snapshot store and threads it
/// into the `ContextualCommand.prior_events` passed to the
/// business logic. Pins the snapshot-loaded code path at
/// `EventBookRepository::get_with_snapshot` against a real store.
#[tokio::test]
async fn pipeline_loads_prior_snapshot_on_subsequent_command() {
    let rig = create_rig().await;
    let root = Uuid::new_v4();

    // Command 1: produces an event + a snapshot at seq 0.
    rig.business
        .enqueue(event_book_with_snapshot(
            "orders",
            root,
            None,
            vec![event_page(0)],
            Some(snapshot_at(0, vec![42])),
        ))
        .await;
    let r1 = rig
        .service
        .handle_command(send(command_book("orders", root, 0, None)))
        .await;
    assert!(r1.is_ok(), "first command: {:?}", r1.err());

    // Command 2: business returns one more event (no new snapshot).
    rig.business
        .enqueue(event_book_with_snapshot(
            "orders",
            root,
            None,
            vec![event_page(1)],
            None,
        ))
        .await;
    let r2 = rig
        .service
        .handle_command(send(command_book("orders", root, 1, None)))
        .await;
    assert!(r2.is_ok(), "second command: {:?}", r2.err());

    // Inspect what command 2's invoke saw.
    let invocation = rig
        .business
        .last_invocation()
        .await
        .expect("business must have been invoked twice");
    let prior = invocation
        .events
        .as_ref()
        .expect("events (prior aggregate state) must be Some on the second command");
    let snap = prior
        .snapshot
        .as_ref()
        .expect("pipeline must have loaded the prior snapshot from SQLite");
    assert_eq!(snap.sequence, 0, "loaded snapshot seq mismatch");
    assert_eq!(
        snap.state
            .as_ref()
            .map(|a| a.value.clone())
            .unwrap_or_default(),
        vec![42],
        "loaded snapshot bytes mismatch -- did not round-trip via SQLite"
    );
}

/// snapshot_retention.feature C-0451/C-0452/C-0454 end to end: the
/// retention a handler sets is persisted unchanged, a later snapshot prunes
/// an older RETENTION_DEFAULT one, and a RETENTION_PERSIST one survives.
#[tokio::test]
async fn pipeline_persists_handler_retention_and_prunes_default() {
    let rig = create_rig().await;
    let root = Uuid::new_v4();
    let with_retention = |seq: u32, retention: SnapshotRetention| Snapshot {
        retention: retention as i32,
        ..snapshot_at(seq, vec![seq as u8 + 1])
    };

    for (seq, retention) in [
        (0, SnapshotRetention::RetentionPersist),
        (1, SnapshotRetention::RetentionDefault),
        (2, SnapshotRetention::RetentionDefault),
    ] {
        rig.business
            .enqueue(event_book_with_snapshot(
                "orders",
                root,
                None,
                vec![event_page(seq)],
                Some(with_retention(seq, retention)),
            ))
            .await;
        rig.service
            .handle_command(send(command_book("orders", root, seq, None)))
            .await
            .expect("command persists");
    }

    let newest = rig
        .snapshot_store
        .get("orders", "", root)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(newest.sequence, 2);
    assert_eq!(newest.retention, SnapshotRetention::RetentionDefault as i32);

    let at_one = rig
        .snapshot_store
        .get_at_seq("orders", "", root, 1)
        .await
        .unwrap()
        .expect("the persistent snapshot remains");
    assert_eq!(at_one.sequence, 0, "the DEFAULT snapshot at 1 was pruned");
    assert_eq!(
        at_one.retention,
        SnapshotRetention::RetentionPersist as i32,
        "the handler's retention is persisted unchanged"
    );
}
