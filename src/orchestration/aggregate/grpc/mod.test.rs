//! Tests for the gRPC aggregate context.
//!
//! The `should_skip_post_persist` predicate that used to live here moved to
//! `super::super::sync_policy` (C-06), so its tests are now in
//! `sync_policy.test.rs` — they are the single source of truth that drives
//! both the local and gRPC `post_persist` short-circuit.
//!
//! Tests below pin gRPC-context-specific load semantics (R2-SNAP-4 etc.).

use super::*;
use crate::bus::MockEventBus;
use crate::discovery::StaticServiceDiscovery;
use crate::orchestration::channels::ChannelCache;
use crate::proto::Snapshot;
use crate::repository::SnapshotRepository;
use crate::storage::mock::{MockEventStore, MockSnapshotStore};
use crate::storage::{AddMeta, SnapshotStore};
use crate::test_utils::{make_event_page, make_uncommitted_event_page};

fn build_ctx_with_stores(
    event_store: Arc<MockEventStore>,
    snapshot_store: Arc<MockSnapshotStore>,
) -> GrpcAggregateContext {
    let snapshot_repo = Arc::new(SnapshotRepository::new(snapshot_store));
    GrpcAggregateContext::new(
        event_store,
        snapshot_repo,
        Arc::new(StaticServiceDiscovery::new()),
        Arc::new(MockEventBus::new()),
    )
}

// ============================================================================
// R2-SNAP-4: load_prior_events_with_divergence honors snapshot when present
// ============================================================================

/// Standard (no-divergence) load uses the snapshot when one exists for
/// the aggregate. Regression guard for the "snapshot exists, load it;
/// layer events from snapshot.sequence + 1" contract.
#[tokio::test]
async fn test_load_standard_path_uses_snapshot_when_present() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let root = Uuid::new_v4();
    let edition = "";

    event_store
        .add(
            "orders",
            edition,
            root,
            (0..5).map(make_event_page).collect(),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();
    snapshot_store
        .put(
            "orders",
            edition,
            root,
            Snapshot {
                sequence: 2,
                state: None,
                retention: crate::proto::SnapshotRetention::RetentionDefault as i32,
                created_at: None,
            },
        )
        .await
        .unwrap();

    let ctx = build_ctx_with_stores(event_store, snapshot_store);
    let book = ctx
        .load_prior_events_with_divergence(
            "orders",
            edition,
            root,
            &super::TemporalQuery::Current,
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        book.pages.len(),
        2,
        "snapshot at seq=2 → only events 3,4 should be loaded"
    );
    assert!(
        book.snapshot.is_some(),
        "loaded EventBook must carry the snapshot"
    );
    assert_eq!(book.snapshot.unwrap().sequence, 2);
}

/// R2-SNAP-4 contract: explicit_divergence load uses the snapshot
/// when one exists for the branch's edition. Pre-fix: the
/// explicit_divergence branch unconditionally skipped the snapshot
/// and replayed from the divergence point.
#[tokio::test]
async fn test_load_explicit_divergence_uses_snapshot_when_present() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let root = Uuid::new_v4();
    let edition = "branch-v2";

    // Branch has events 0..5 and a snapshot at sequence 2.
    event_store
        .add(
            "orders",
            edition,
            root,
            (0..5).map(make_event_page).collect(),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();
    snapshot_store
        .put(
            "orders",
            edition,
            root,
            Snapshot {
                sequence: 2,
                state: None,
                retention: crate::proto::SnapshotRetention::RetentionDefault as i32,
                created_at: None,
            },
        )
        .await
        .unwrap();

    let ctx = build_ctx_with_stores(event_store, snapshot_store);
    let book = ctx
        .load_prior_events_with_divergence(
            "orders",
            edition,
            root,
            &super::TemporalQuery::Current,
            Some(0), // explicit divergence at 0 (branch starts at sequence 0)
        )
        .await
        .unwrap();

    assert!(
        book.snapshot.is_some(),
        "explicit_divergence must NOT skip the snapshot when one exists"
    );
    assert_eq!(book.snapshot.as_ref().unwrap().sequence, 2);
    assert_eq!(
        book.pages.len(),
        2,
        "snapshot at seq=2 → only events 3,4 loaded; pre-fix would have returned all 5"
    );
}

/// R2-SNAP-4 regression guard: when no snapshot exists for the
/// branch, explicit_divergence falls back to the get_with_divergence
/// path (the legacy behavior). Required so the fix doesn't change
/// behavior for the common "new branch, no snapshot yet" case the
/// path was originally designed for.
#[tokio::test]
async fn test_load_explicit_divergence_falls_back_when_no_snapshot() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let root = Uuid::new_v4();
    let edition = "branch-no-snap";

    event_store
        .add(
            "orders",
            edition,
            root,
            (0..3).map(make_event_page).collect(),
            &AddMeta {
                correlation_id: "",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .unwrap();

    let ctx = build_ctx_with_stores(event_store, snapshot_store);
    let book = ctx
        .load_prior_events_with_divergence(
            "orders",
            edition,
            root,
            &super::TemporalQuery::Current,
            Some(0),
        )
        .await
        .unwrap();

    assert!(
        book.snapshot.is_none(),
        "no snapshot exists; loaded book must reflect that"
    );
    // The exact page count depends on the mock's get_with_divergence
    // implementation; assert non-empty to confirm the path ran.
    assert!(
        !book.pages.is_empty(),
        "fallback path must still produce events"
    );
}

// ============================================================================
// O2: post_persist must NOT publish provisional (no_commit) pages to the bus
// ============================================================================
//
// Cascade pages persisted with no_commit=true are PROVISIONAL — a later
// Revocation may undo them (see cascade::reaper / two_phase). Publishing them
// to the bus lets downstream async consumers observe a commit that may never
// become real (a "phantom commit"). post_persist must publish only committed
// pages; provisional pages are published later, at the confirmation point.

/// Build a context wired to a MockEventBus we retain for assertions, plus an
/// empty StaticServiceDiscovery so the sync projector/saga/PM fan-out is a
/// no-op. sync_mode is left None: `should_skip_post_persist(None)` is false
/// (only ISOLATED skips) so the bus publish runs, and
/// `should_call_sync_projectors(None)` is false so no discovery is needed.
fn build_ctx_with_bus() -> (GrpcAggregateContext, Arc<MockEventBus>) {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store: Arc<MockSnapshotStore> = Arc::new(MockSnapshotStore::default());
    let snapshot_repo = Arc::new(SnapshotRepository::new(snapshot_store));
    let bus = Arc::new(MockEventBus::new());
    let ctx = GrpcAggregateContext::new(
        event_store,
        snapshot_repo,
        Arc::new(StaticServiceDiscovery::new()),
        bus.clone(),
    );
    (ctx, bus)
}

fn book_with_cover(pages: Vec<crate::proto::EventPage>) -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: "orders".to_string(),
            root: None,
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        pages,
        snapshot: None,
        next_sequence: 0,
    }
}

/// A fully provisional book (every page no_commit=true) must publish NOTHING —
/// the whole book is an in-flight cascade that may still be revoked.
#[tokio::test]
async fn post_persist_suppresses_fully_provisional_book() {
    use crate::orchestration::aggregate::traits::AggregateContext;
    let (ctx, bus) = build_ctx_with_bus();

    let provisional = book_with_cover(vec![
        make_uncommitted_event_page(0, "cascade-x"),
        make_uncommitted_event_page(1, "cascade-x"),
    ]);
    ctx.publish(&provisional).await.unwrap();

    assert_eq!(
        bus.published_count().await,
        0,
        "provisional (no_commit) pages must not be published — publishing them \
         would be a phantom commit visible downstream before the cascade commits"
    );
}

/// A fully committed book publishes normally (regression guard: the O2 filter
/// must not suppress ordinary, non-cascade commits).
#[tokio::test]
async fn post_persist_publishes_committed_book() {
    use crate::orchestration::aggregate::traits::AggregateContext;
    let (ctx, bus) = build_ctx_with_bus();

    let committed = book_with_cover(vec![make_event_page(0), make_event_page(1)]);
    ctx.publish(&committed).await.unwrap();

    let published = bus.take_published().await;
    assert_eq!(published.len(), 1, "committed events must be published");
    assert_eq!(
        published[0].pages.len(),
        2,
        "all committed pages must reach the bus"
    );
}

/// A mixed book publishes ONLY the committed pages — the filter is per-page,
/// not all-or-nothing. Kills mutants that publish the whole book (or nothing)
/// when any/all pages are provisional.
#[tokio::test]
async fn post_persist_publishes_only_committed_pages_from_mixed_book() {
    use crate::orchestration::aggregate::traits::AggregateContext;
    let (ctx, bus) = build_ctx_with_bus();

    let mixed = book_with_cover(vec![
        make_event_page(0),                          // committed
        make_uncommitted_event_page(1, "cascade-y"), // provisional
    ]);
    ctx.publish(&mixed).await.unwrap();

    let published = bus.take_published().await;
    assert_eq!(published.len(), 1, "the committed page must be published");
    assert_eq!(
        published[0].pages.len(),
        1,
        "only the committed page is published; the provisional page is withheld"
    );
    assert!(
        !published[0].pages[0].no_commit,
        "the published page must be the committed one"
    );
}

// ============================================================================
// O2 carve-out: per-leg visibility of provisional pages in the sync fan-out
// ============================================================================
//
// The three sync fan-out legs deliberately see DIFFERENT views of a book that
// carries provisional (no_commit) pages:
//
//   - PROJECTOR leg: committed-only. Projectors write externally visible read
//     models; a provisional page they consume may later be revoked, and there
//     is NO framework path mapping a Revocation to a read-model undo. Same
//     phantom-commit hazard as the bus publish, same filter.
//   - SAGA / PM legs: FULL book, provisional pages included. In CASCADE mode
//     these calls ARE the cascade's forward propagation — sagas/PMs react to
//     the provisional events to emit the next aggregate's commands. Filtering
//     here would halt every multi-aggregate cascade at its first hop.
//
// The tests below capture what each leg actually receives via in-process
// tonic servers (same pattern as services/projector_coord.test.rs).

use crate::proto::process_manager_coordinator_service_server::{
    ProcessManagerCoordinatorService as PmCoordServiceTrait, ProcessManagerCoordinatorServiceServer,
};
use crate::proto::projector_coordinator_service_server::{
    ProjectorCoordinatorService as ProjectorCoordServiceTrait, ProjectorCoordinatorServiceServer,
};
use crate::proto::saga_coordinator_service_server::{
    SagaCoordinatorService as SagaCoordServiceTrait, SagaCoordinatorServiceServer,
};
use crate::proto::{
    ProcessManagerHandleResponse, SagaResponse, SpeculatePmRequest, SpeculateProjectorRequest,
    SpeculateSagaRequest,
};

/// Captures the EventRequest a projector coordinator receives from the sync
/// projector leg.
#[derive(Clone, Default)]
struct CapturingProjectorServer {
    requests: Arc<Mutex<Vec<EventRequest>>>,
}

#[tonic::async_trait]
impl ProjectorCoordServiceTrait for CapturingProjectorServer {
    async fn handle_sync(
        &self,
        request: tonic::Request<EventRequest>,
    ) -> Result<tonic::Response<Projection>, Status> {
        self.requests.lock().await.push(request.into_inner());
        Ok(tonic::Response::new(Projection::default()))
    }

    async fn handle(
        &self,
        _request: tonic::Request<EventBook>,
    ) -> Result<tonic::Response<()>, Status> {
        Err(Status::unimplemented("not exercised by these tests"))
    }

    async fn handle_speculative(
        &self,
        _request: tonic::Request<SpeculateProjectorRequest>,
    ) -> Result<tonic::Response<Projection>, Status> {
        Err(Status::unimplemented("not exercised by these tests"))
    }
}

/// Captures the SagaHandleRequest a saga coordinator receives from the
/// CASCADE saga leg.
#[derive(Clone, Default)]
struct CapturingSagaServer {
    requests: Arc<Mutex<Vec<SagaHandleRequest>>>,
}

#[tonic::async_trait]
impl SagaCoordServiceTrait for CapturingSagaServer {
    async fn execute(
        &self,
        request: tonic::Request<SagaHandleRequest>,
    ) -> Result<tonic::Response<SagaResponse>, Status> {
        self.requests.lock().await.push(request.into_inner());
        Ok(tonic::Response::new(SagaResponse::default()))
    }

    async fn execute_speculative(
        &self,
        _request: tonic::Request<SpeculateSagaRequest>,
    ) -> Result<tonic::Response<SagaResponse>, Status> {
        Err(Status::unimplemented("not exercised by these tests"))
    }
}

/// Captures the ProcessManagerCoordinatorRequest a PM coordinator receives
/// from the CASCADE PM leg.
#[derive(Clone, Default)]
struct CapturingPmServer {
    requests: Arc<Mutex<Vec<ProcessManagerCoordinatorRequest>>>,
}

#[tonic::async_trait]
impl PmCoordServiceTrait for CapturingPmServer {
    async fn handle(
        &self,
        request: tonic::Request<ProcessManagerCoordinatorRequest>,
    ) -> Result<tonic::Response<ProcessManagerHandleResponse>, Status> {
        self.requests.lock().await.push(request.into_inner());
        Ok(tonic::Response::new(ProcessManagerHandleResponse::default()))
    }

    async fn handle_speculative(
        &self,
        _request: tonic::Request<SpeculatePmRequest>,
    ) -> Result<tonic::Response<ProcessManagerHandleResponse>, Status> {
        Err(Status::unimplemented("not exercised by these tests"))
    }
}

/// Bind an ephemeral local port and return (listener, port). The caller adds
/// services and serves on the listener (pattern from projector_coord.test.rs).
async fn bind_ephemeral() -> (tokio::net::TcpListener, u16) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

/// O2 carve-out (projector half): a mixed committed/provisional book must
/// reach the sync PROJECTOR leg with ONLY the committed pages. Kills the
/// mutant/regression where `post_persist` hands projectors the raw `events`
/// book — which would let a read model materialize a provisional page that a
/// later Revocation undoes, with no framework path to un-project it.
#[tokio::test]
async fn post_persist_projector_leg_receives_only_committed_pages() {
    use crate::orchestration::aggregate::traits::AggregateContext;

    // In-process projector coordinator capturing what it is sent.
    let projector = CapturingProjectorServer::default();
    let captured = projector.requests.clone();
    let (listener, port) = bind_ephemeral().await;
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ProjectorCoordinatorServiceServer::new(projector))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let discovery = Arc::new(StaticServiceDiscovery::new());
    discovery
        .register_projector("prj-capture", "orders", "127.0.0.1", port)
        .await;

    let event_store = Arc::new(MockEventStore::new());
    let snapshot_repo = Arc::new(SnapshotRepository::new(Arc::new(MockSnapshotStore::new())));
    let ctx = GrpcAggregateContext::new(
        event_store,
        snapshot_repo,
        discovery,
        Arc::new(MockEventBus::new()),
    )
    // SIMPLE runs the projector leg but not the saga/PM legs — isolates the
    // projector-side assertion.
    .with_sync_mode(crate::proto::SyncMode::Simple);

    let mixed = book_with_cover(vec![
        make_event_page(0),                          // committed
        make_uncommitted_event_page(1, "cascade-p"), // provisional
    ]);
    ctx.sync_fanout(&mixed).await.unwrap();

    let requests = captured.lock().await;
    assert_eq!(requests.len(), 1, "projector must be called exactly once");
    let sent = requests[0]
        .events
        .as_ref()
        .expect("projector EventRequest must carry events");
    assert_eq!(
        sent.pages.len(),
        1,
        "projector must receive ONLY the committed page; sending the \
         provisional page would materialize a phantom commit in a read model"
    );
    assert!(
        !sent.pages[0].no_commit,
        "the page the projector receives must be the committed one"
    );
}

/// O2 carve-out PIN (saga/PM half): the saga and PM legs must receive the
/// FULL book INCLUDING provisional (no_commit) pages.
///
/// WHY THIS PIN EXISTS: this is a deliberate carve-out from the O2
/// phantom-commit filter, not an oversight. In CASCADE mode the sync
/// saga/PM calls are the cascade's forward propagation — the saga/PM reads
/// the provisional events of the current hop to emit the commands that drive
/// the NEXT hop, all before anything is confirmed. Filtering no_commit pages
/// out of these legs (e.g. by a future "consistency" refactor that reuses the
/// projector/bus filter here) would silently halt every multi-aggregate
/// cascade at its first hop: the saga would see an empty/committed-only book
/// and never produce the follow-on commands. The review explicitly flagged
/// that nothing pinned this behavior; this test is that pin.
#[tokio::test]
async fn post_persist_saga_and_pm_legs_receive_full_book_including_provisional() {
    use crate::orchestration::aggregate::traits::AggregateContext;

    // One in-process server hosting BOTH coordinator services.
    let saga = CapturingSagaServer::default();
    let pm = CapturingPmServer::default();
    let saga_captured = saga.requests.clone();
    let pm_captured = pm.requests.clone();
    let (listener, port) = bind_ephemeral().await;
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(SagaCoordinatorServiceServer::new(saga))
            .add_service(ProcessManagerCoordinatorServiceServer::new(pm))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let discovery = Arc::new(StaticServiceDiscovery::new());
    discovery
        .register_saga("saga-orders-capture", "orders", "127.0.0.1", port)
        .await;
    discovery
        .register_pm("pm-capture", &["orders"], "127.0.0.1", port)
        .await;
    // No projectors registered: the projector leg no-ops (empty client list),
    // keeping this test focused on the saga/PM carve-out.

    let event_store = Arc::new(MockEventStore::new());
    let snapshot_repo = Arc::new(SnapshotRepository::new(Arc::new(MockSnapshotStore::new())));
    let ctx = GrpcAggregateContext::new(
        event_store,
        snapshot_repo,
        discovery,
        Arc::new(MockEventBus::new()),
    )
    .with_sync_mode(crate::proto::SyncMode::Cascade);

    // Cover needs a correlation_id: the PM leg skips books without one.
    let mixed = EventBook {
        cover: Some(Cover {
            domain: "orders".to_string(),
            root: None,
            correlation_id: "corr-cascade".to_string(),
            edition: None,
            ext: None,
        }),
        pages: vec![
            make_event_page(0),                          // committed
            make_uncommitted_event_page(1, "cascade-q"), // provisional
        ],
        snapshot: None,
        next_sequence: 0,
    };
    ctx.sync_fanout(&mixed).await.unwrap();

    // Saga leg: full book, provisional page intact.
    let saga_requests = saga_captured.lock().await;
    assert_eq!(saga_requests.len(), 1, "saga must be called exactly once");
    let saga_book = saga_requests[0]
        .source
        .as_ref()
        .expect("SagaHandleRequest must carry source events");
    assert_eq!(
        saga_book.pages.len(),
        2,
        "saga must receive the FULL book (committed + provisional); filtering \
         would break cascade forward propagation"
    );
    assert!(
        saga_book.pages.iter().any(|p| p.no_commit),
        "the provisional page must reach the saga with no_commit intact"
    );

    // PM leg: full book, provisional page intact.
    let pm_requests = pm_captured.lock().await;
    assert_eq!(pm_requests.len(), 1, "PM must be called exactly once");
    let pm_book = pm_requests[0]
        .trigger
        .as_ref()
        .expect("ProcessManagerCoordinatorRequest must carry trigger events");
    assert_eq!(
        pm_book.pages.len(),
        2,
        "PM must receive the FULL book (committed + provisional); filtering \
         would break cascade forward propagation"
    );
    assert!(
        pm_book.pages.iter().any(|p| p.no_commit),
        "the provisional page must reach the PM with no_commit intact"
    );
}

// ============================================================================
// publish_aggregate_sequence_mismatch_dlq (R2-15 step 4 seam, refactored
// 2026-05-27 to be testable without a full GrpcAggregateContext)
// ============================================================================

use crate::dlq::{
    AngzarrDeadLetter as DlAngzarrDeadLetter, DeadLetterPayload, DeadLetterPublisher, DlqError,
    RejectionDetails,
};
use crate::proto::Cover;
use async_trait::async_trait;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::Mutex;

/// Captures dead letters so the test can inspect the constructed
/// shape that flows from the MergeManual sequence-mismatch path.
#[derive(Default)]
struct CapturingDlqPublisher {
    captured: Mutex<Vec<DlAngzarrDeadLetter>>,
    publish_calls: AtomicU32,
}

#[async_trait]
impl DeadLetterPublisher for CapturingDlqPublisher {
    async fn publish(&self, dead_letter: DlAngzarrDeadLetter) -> Result<(), DlqError> {
        self.publish_calls.fetch_add(1, Ordering::SeqCst);
        self.captured.lock().await.push(dead_letter);
        Ok(())
    }
}

fn cmd_for(domain: &str, correlation_id: &str) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: None,
            correlation_id: correlation_id.to_string(),
            edition: None,
            ext: None,
        }),
        pages: vec![],
    }
}

/// `publish_aggregate_sequence_mismatch_dlq` constructs an
/// `AngzarrDeadLetter` with the expected shape and forwards it to
/// the publisher. The MergeManual contract: sequence mismatch
/// details + Command payload + source_component_type "aggregate".
#[tokio::test]
async fn publish_aggregate_sequence_mismatch_dlq_builds_correct_shape() {
    let capture = Arc::new(CapturingDlqPublisher::default());
    let publisher: Arc<dyn DeadLetterPublisher> = capture.clone();

    let command = cmd_for("orders", "corr-1");
    publish_aggregate_sequence_mismatch_dlq(
        &publisher,
        &command,
        3, // expected
        7, // actual
        "orders",
        "aggregate-orders",
    )
    .await;

    assert_eq!(
        capture.publish_calls.load(Ordering::SeqCst),
        1,
        "publisher.publish must be called exactly once"
    );
    let entries = capture.captured.lock().await.clone();
    assert_eq!(entries.len(), 1);

    let dl = &entries[0];
    assert_eq!(dl.source_component, "aggregate-orders");
    assert_eq!(dl.source_component_type, "aggregate");
    match &dl.payload {
        DeadLetterPayload::Command(_) => {}
        other => panic!("expected Command payload, got {other:?}"),
    }
    match &dl.rejection_details {
        Some(RejectionDetails::SequenceMismatch(details)) => {
            assert_eq!(details.expected_sequence, 3);
            assert_eq!(details.actual_sequence, 7);
        }
        other => panic!("expected SequenceMismatch details, got {other:?}"),
    }
}

// ============================================================================
// O5: snapshot persistence is best-effort — its failure must never fail an
// already-persisted command
// ============================================================================
//
// persist_events commits the new event pages FIRST, then writes the snapshot.
// Events are the source of truth; the snapshot is derived, rebuildable state.
// Pre-fix, a snapshot-store blip after the events committed `?`-propagated as
// `Status::internal` — retryable per retry.rs — so the retry wrapper re-entered
// the pipeline with events already stored and never published: spurious
// retry-exhaust (STRICT), spurious DLQ (MANUAL), or a genuine double-apply
// (AGGREGATE_HANDLES re-runs the handler against state containing its own
// events).

/// SnapshotStore double whose `put` always fails; reads/deletes delegate to an
/// inner MockSnapshotStore. Models a snapshot-store blip at exactly the wrong
/// moment: after the events committed.
struct FailingPutSnapshotStore {
    inner: MockSnapshotStore,
}

#[async_trait]
impl SnapshotStore for FailingPutSnapshotStore {
    async fn get(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
    ) -> crate::storage::Result<Option<Snapshot>> {
        self.inner.get(domain, edition, root).await
    }

    async fn get_at_seq(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        seq: u32,
    ) -> crate::storage::Result<Option<Snapshot>> {
        self.inner.get_at_seq(domain, edition, root, seq).await
    }

    async fn put(
        &self,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
        _snapshot: Snapshot,
    ) -> crate::storage::Result<()> {
        Err(StorageError::Backend(
            "injected snapshot put failure".to_string(),
        ))
    }

    async fn delete(&self, domain: &str, edition: &str, root: Uuid) -> crate::storage::Result<()> {
        self.inner.delete(domain, edition, root).await
    }
}

/// EventBook carrying `pages` plus a client-supplied snapshot state (the
/// "snapshot me here" signal that makes persist_events attempt the write).
fn book_with_snapshot_state(pages: Vec<crate::proto::EventPage>) -> EventBook {
    EventBook {
        cover: None, // persist_events builds the cover from its parameters
        pages,
        snapshot: Some(Snapshot {
            sequence: 0, // recomputed by the persist helper
            state: Some(prost_types::Any {
                type_url: "test.State".to_string(),
                value: vec![1, 2, 3],
            }),
            retention: crate::proto::SnapshotRetention::RetentionDefault as i32,
            created_at: None,
        }),
        ..Default::default()
    }
}

/// O5 core contract: once the events committed, the command HAS succeeded.
/// A snapshot-store failure after that point must be swallowed (logged) —
/// persist_events returns Ok(Persisted) and the events remain durably stored.
/// Pre-fix this returned a retryable Internal error, re-running the command
/// on top of its own already-persisted events.
#[tokio::test]
async fn persist_events_snapshot_put_failure_does_not_fail_command() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(FailingPutSnapshotStore {
        inner: MockSnapshotStore::new(),
    });
    let snapshot_repo = Arc::new(SnapshotRepository::new(snapshot_store));
    let ctx = GrpcAggregateContext::new(
        event_store.clone(),
        snapshot_repo,
        Arc::new(StaticServiceDiscovery::new()),
        Arc::new(MockEventBus::new()),
    );
    let root = Uuid::new_v4();

    let prior = EventBook::default();
    let received = book_with_snapshot_state(vec![make_event_page(0)]);

    let outcome = ctx
        .persist_events(&prior, &received, "orders", "", root, "corr-o5", None, None)
        .await
        .expect("snapshot put failure must NOT fail the command — events already committed");

    match outcome {
        PersistOutcome::Persisted(book) => {
            assert_eq!(book.pages.len(), 1, "the new event page is the outcome");
        }
        other => panic!("expected Persisted, got {other:?}"),
    }
    let stored = event_store.get("orders", "", root).await.unwrap();
    assert_eq!(
        stored.len(),
        1,
        "events must be durably persisted despite the snapshot blip"
    );
}

/// Best-effort applies to snapshot-only updates too (no new events, changed
/// snapshot state). Snapshots never carry new facts — all state is derivable
/// from already-persisted events — so their write failure is never a command
/// error; the snapshot is simply rewritten on the next state change.
#[tokio::test]
async fn persist_events_snapshot_only_update_put_failure_returns_ok() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(FailingPutSnapshotStore {
        inner: MockSnapshotStore::new(),
    });
    let snapshot_repo = Arc::new(SnapshotRepository::new(snapshot_store));
    let ctx = GrpcAggregateContext::new(
        event_store,
        snapshot_repo,
        Arc::new(StaticServiceDiscovery::new()),
        Arc::new(MockEventBus::new()),
    );
    let root = Uuid::new_v4();

    // Prior already holds events 0..2; received repeats them (no new pages)
    // and adds only a snapshot state.
    let prior = EventBook {
        pages: vec![make_event_page(0), make_event_page(1)],
        ..Default::default()
    };
    let received = book_with_snapshot_state(vec![make_event_page(0), make_event_page(1)]);

    let outcome = ctx
        .persist_events(
            &prior, &received, "orders", "", root, "corr-o5b", None, None,
        )
        .await
        .expect("snapshot-only put failure must not surface as a command error");
    match outcome {
        PersistOutcome::Persisted(book) => {
            assert!(
                book.pages.is_empty(),
                "no new pages in a snapshot-only update"
            );
        }
        other => panic!("expected Persisted, got {other:?}"),
    }
}

/// Regression guard: the swallow is for SNAPSHOT failures only. An
/// events-persist failure happens BEFORE anything is durable — the command
/// has NOT succeeded — so it must still propagate (and stay retryable).
#[tokio::test]
async fn persist_events_event_store_failure_still_propagates() {
    let event_store = Arc::new(MockEventStore::new());
    event_store.set_fail_on_add(true).await;
    let snapshot_store: Arc<MockSnapshotStore> = Arc::new(MockSnapshotStore::new());
    let ctx = build_ctx_with_stores(event_store, snapshot_store);
    let root = Uuid::new_v4();

    let prior = EventBook::default();
    let received = book_with_snapshot_state(vec![make_event_page(0)]);

    let err = ctx
        .persist_events(
            &prior, &received, "orders", "", root, "corr-o5c", None, None,
        )
        .await
        .expect_err("events-persist failure must fail the command");
    assert_eq!(err.code(), tonic::Code::Internal);
    assert!(
        err.message().contains("Failed to persist events"),
        "must be the events-persist error, not a snapshot error: {}",
        err.message()
    );
}

/// `send_to_dlq` on the full `GrpcAggregateContext` delegates to the
/// free fn. Same shape as the test above, but routed through the
/// context method to pin the wrapper.
#[tokio::test]
async fn send_to_dlq_on_context_delegates_to_free_fn() {
    let publisher = Arc::new(CapturingDlqPublisher::default());
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store: Arc<MockSnapshotStore> = Arc::new(MockSnapshotStore::default());
    let snapshot_repo = Arc::new(SnapshotRepository::new(snapshot_store));
    let ctx = GrpcAggregateContext::new(
        event_store,
        snapshot_repo,
        Arc::new(StaticServiceDiscovery::new()),
        Arc::new(MockEventBus::new()),
    )
    .with_dlq_publisher(publisher.clone())
    .with_component_name("aggregate-orders");

    use crate::orchestration::aggregate::traits::AggregateContext;
    ctx.send_to_dlq(&cmd_for("orders", "corr-2"), 1, 4, "orders")
        .await;

    let calls = publisher.publish_calls.load(Ordering::SeqCst);
    assert_eq!(calls, 1, "context method must forward to publisher");
    let entries = publisher.captured.lock().await.clone();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].source_component, "aggregate-orders");
    assert_eq!(entries[0].source_component_type, "aggregate");
}

// ============================================================================
// C01 #2: snapshot persistence must defer while a cascade is in flight
// ============================================================================

/// While `cascade_id` is set, `persist_events` must NOT persist a snapshot
/// even when the received book carries one — it would bake in state a
/// later Revocation could undo (there is only one snapshot slot per
/// aggregate; a revoke has no way to "un-snapshot" it).
#[tokio::test]
async fn persist_events_defers_snapshot_when_cascade_in_flight() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let snapshot_repo = Arc::new(SnapshotRepository::new(snapshot_store.clone()));
    let ctx = GrpcAggregateContext::new(
        event_store.clone(),
        snapshot_repo,
        Arc::new(StaticServiceDiscovery::new()),
        Arc::new(MockEventBus::new()),
    )
    .with_cascade_id("cascade-defer");
    let root = Uuid::new_v4();

    let prior = EventBook::default();
    let received = book_with_snapshot_state(vec![make_event_page(0)]);

    let outcome = ctx
        .persist_events(
            &prior,
            &received,
            "orders",
            "",
            root,
            "corr-defer",
            None,
            None,
        )
        .await
        .expect("persist must succeed even though the snapshot is deferred");

    match outcome {
        PersistOutcome::Persisted(book) => {
            assert_eq!(book.pages.len(), 1);
            assert!(
                book.pages[0].no_commit,
                "cascade pages must be stamped provisional"
            );
        }
        other => panic!("expected Persisted, got {other:?}"),
    }

    let snapshot = snapshot_store.get("orders", "", root).await.unwrap();
    assert!(
        snapshot.is_none(),
        "C01 #2: snapshot must not be persisted while a cascade is in flight"
    );
}

/// Regression guard / companion: OUTSIDE a cascade (no `cascade_id` set),
/// snapshot persistence proceeds exactly as before — the deferral is
/// specific to cascades in flight, not a general regression.
#[tokio::test]
async fn persist_events_persists_snapshot_when_no_cascade_in_flight() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let snapshot_repo = Arc::new(SnapshotRepository::new(snapshot_store.clone()));
    let ctx = GrpcAggregateContext::new(
        event_store.clone(),
        snapshot_repo,
        Arc::new(StaticServiceDiscovery::new()),
        Arc::new(MockEventBus::new()),
    );
    let root = Uuid::new_v4();

    let prior = EventBook::default();
    let received = book_with_snapshot_state(vec![make_event_page(0)]);

    ctx.persist_events(
        &prior,
        &received,
        "orders",
        "",
        root,
        "corr-nocascade",
        None,
        None,
    )
    .await
    .expect("persist must succeed");

    let snapshot = snapshot_store.get("orders", "", root).await.unwrap();
    assert!(
        snapshot.is_some(),
        "outside a cascade, snapshot persistence must proceed as before"
    );
}

// ============================================================================
// C01 #9: bus-published books must never carry a snapshot
// ============================================================================

/// `committed_only_book` strips the snapshot from the bus-published view —
/// a snapshot on a bus book makes `GapFiller::fill_if_needed` treat it as
/// "already complete" and skip gap repair entirely, unrelated to whether a
/// gap actually exists.
#[test]
fn committed_only_book_strips_snapshot() {
    let events = EventBook {
        cover: Some(Cover {
            domain: "orders".to_string(),
            root: None,
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        pages: vec![make_event_page(0)],
        snapshot: Some(Snapshot {
            sequence: 0,
            state: Some(prost_types::Any {
                type_url: "test.State".to_string(),
                value: vec![1, 2, 3],
            }),
            retention: crate::proto::SnapshotRetention::RetentionDefault as i32,
            created_at: None,
        }),
        next_sequence: 1,
    };

    let result = committed_only_book(&events).expect("committed pages exist");
    assert!(
        result.snapshot.is_none(),
        "C01 #9: bus-published book must never carry a snapshot"
    );
}

// ============================================================================
// C01 #1: confirmation-point republish
// ============================================================================
//
// The cascade design suppresses provisional (`no_commit=true`) pages from
// the bus (O2) on the promise that a Confirmation marker's arrival
// republishes them. These tests exercise that promise directly against
// `post_persist`.

fn build_ctx_with_bus_and_store() -> (GrpcAggregateContext, Arc<MockEventBus>, Arc<MockEventStore>)
{
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store: Arc<MockSnapshotStore> = Arc::new(MockSnapshotStore::default());
    let snapshot_repo = Arc::new(SnapshotRepository::new(snapshot_store));
    let bus = Arc::new(MockEventBus::new());
    let ctx = GrpcAggregateContext::new(
        event_store.clone(),
        snapshot_repo,
        Arc::new(StaticServiceDiscovery::new()),
        bus.clone(),
    );
    (ctx, bus, event_store)
}

/// Build a committed Confirmation marker `EventPage` at `seq`.
fn confirmation_marker_page(seq: u32, cascade_id: &str, confirmed: Vec<u32>) -> EventPage {
    use crate::proto::Confirmation;
    use prost::Message;
    let conf = Confirmation {
        target: None,
        sequences: confirmed,
        cascade_id: cascade_id.to_string(),
    };
    EventPage {
        header: Some(crate::proto::PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(seq)),
        }),
        payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
            type_url: crate::proto_ext::type_url::CONFIRMATION.to_string(),
            value: conf.encode_to_vec(),
        })),
        ..Default::default()
    }
}

/// Build a committed Revocation marker `EventPage` at `seq`.
fn revocation_marker_page(seq: u32, cascade_id: &str, revoked: Vec<u32>) -> EventPage {
    use crate::proto::Revocation;
    use prost::Message;
    let rev = Revocation {
        target: None,
        sequences: revoked,
        cascade_id: cascade_id.to_string(),
        reason: "test".to_string(),
    };
    EventPage {
        header: Some(crate::proto::PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(seq)),
        }),
        payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
            type_url: crate::proto_ext::type_url::REVOCATION.to_string(),
            value: rev.encode_to_vec(),
        })),
        ..Default::default()
    }
}

fn book_with_root(root: Uuid, pages: Vec<EventPage>) -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: "orders".to_string(),
            root: Some(crate::proto::Uuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        pages,
        snapshot: None,
        next_sequence: 0,
    }
}

/// The acceptance case: a provisional event was suppressed from the bus at
/// its own persist time (simulated directly here via `event_store.add`,
/// mirroring what `persist_events` does under a cascade_id); a LATER call
/// persists a Confirmation marker for it. `post_persist` for that later
/// call must republish the previously-suppressed event — this is the
/// consumer-facing half of the 2PC design that C01 completes.
#[tokio::test]
async fn post_persist_republishes_confirmed_events_on_confirmation_marker() {
    use crate::orchestration::aggregate::traits::AggregateContext;
    let (ctx, bus, event_store) = build_ctx_with_bus_and_store();
    let root = Uuid::new_v4();

    // Seed storage: the provisional event, suppressed from the bus when it
    // was originally persisted (simulated by direct storage write, exactly
    // as `persist_events` would have produced under `cascade_id`).
    event_store
        .add(
            "orders",
            "",
            root,
            vec![make_uncommitted_event_page(0, "cascade-c1")],
            &AddMeta::default(),
        )
        .await
        .unwrap();

    let marker = confirmation_marker_page(1, "cascade-c1", vec![0]);
    // The marker is already durable by the time post_persist runs (persist
    // happens before post_persist in the real pipeline) — seed it too.
    event_store
        .add(
            "orders",
            "",
            root,
            vec![marker.clone()],
            &AddMeta::default(),
        )
        .await
        .unwrap();

    let events = book_with_root(root, vec![marker]);
    ctx.publish(&events)
        .await
        .expect("post_persist must succeed");

    let published = bus.take_published().await;
    assert_eq!(
        published.len(),
        2,
        "expected the republished confirmed-events book (first, per the \
         documented ordering) plus the marker's own ordinary committed publish"
    );
    assert_eq!(
        published[0].pages.len(),
        1,
        "republished book carries exactly the confirmed sequence"
    );
    assert_eq!(published[0].pages[0].sequence_num(), 0);
    assert_eq!(
        published[0].pages[0].type_url(),
        Some("test.Event0"),
        "the republished page must carry the ORIGINAL suppressed payload"
    );
    assert_eq!(
        published[1].pages[0].type_url(),
        Some(crate::proto_ext::type_url::CONFIRMATION),
        "second publish is the marker's own ordinary committed publish"
    );
}

/// No double-publish: a sequence the Confirmation names that was ALREADY
/// committed at its own persist time (no_commit=false in storage) must not
/// republish — it already reached the bus once.
#[tokio::test]
async fn post_persist_confirmation_does_not_republish_already_committed_sequence() {
    use crate::orchestration::aggregate::traits::AggregateContext;
    let (ctx, bus, event_store) = build_ctx_with_bus_and_store();
    let root = Uuid::new_v4();

    // Sequence 0 was already committed (no_commit=false) — e.g. a stale or
    // redundant Confirmation naming a sequence that was never provisional.
    event_store
        .add(
            "orders",
            "",
            root,
            vec![make_event_page(0)],
            &AddMeta::default(),
        )
        .await
        .unwrap();
    let marker = confirmation_marker_page(1, "cascade-c2", vec![0]);
    event_store
        .add(
            "orders",
            "",
            root,
            vec![marker.clone()],
            &AddMeta::default(),
        )
        .await
        .unwrap();

    let events = book_with_root(root, vec![marker]);
    ctx.publish(&events).await.unwrap();

    let published = bus.take_published().await;
    assert_eq!(
        published.len(),
        1,
        "only the marker's own ordinary committed publish — sequence 0 was \
         already committed and must not republish"
    );
}

/// C01 #21: confirm-after-revoke guard. If a Revocation for the SAME
/// cascade_id already exists, `transform_for_two_phase` documents "revoked
/// wins" — the confirmed sequences resolve to nothing. Pre-fix this would
/// silently no-op; the fix surfaces it (error log + DLQ) instead.
#[tokio::test]
async fn post_persist_confirm_after_revoke_does_not_republish_and_dlqs() {
    use crate::orchestration::aggregate::traits::AggregateContext;
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store: Arc<MockSnapshotStore> = Arc::new(MockSnapshotStore::default());
    let snapshot_repo = Arc::new(SnapshotRepository::new(snapshot_store));
    let bus = Arc::new(MockEventBus::new());
    let dlq = Arc::new(CapturingDlqPublisher::default());
    let ctx = GrpcAggregateContext::new(
        event_store.clone(),
        snapshot_repo,
        Arc::new(StaticServiceDiscovery::new()),
        bus.clone(),
    )
    .with_dlq_publisher(dlq.clone());
    let root = Uuid::new_v4();

    event_store
        .add(
            "orders",
            "",
            root,
            vec![make_uncommitted_event_page(0, "cascade-r1")],
            &AddMeta::default(),
        )
        .await
        .unwrap();
    // A Revocation for this cascade already landed (e.g. the reaper timed
    // it out) BEFORE the confirming call's persist reached storage.
    event_store
        .add(
            "orders",
            "",
            root,
            vec![revocation_marker_page(1, "cascade-r1", vec![0])],
            &AddMeta::default(),
        )
        .await
        .unwrap();

    // The confirming call still landed racily (TOCTOU) and its Confirmation
    // marker is now ALSO durably persisted.
    let marker = confirmation_marker_page(2, "cascade-r1", vec![0]);
    event_store
        .add(
            "orders",
            "",
            root,
            vec![marker.clone()],
            &AddMeta::default(),
        )
        .await
        .unwrap();

    let events = book_with_root(root, vec![marker]);
    ctx.publish(&events)
        .await
        .expect("post_persist must not error — the conflict is handled, not propagated");

    let published = bus.take_published().await;
    assert_eq!(
        published.len(),
        1,
        "only the marker's own ordinary committed publish — the confirmed \
         sequence must NOT republish when a conflicting Revocation exists"
    );

    let dlq_calls = dlq.publish_calls.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        dlq_calls, 1,
        "confirm-after-revoke conflict must be surfaced to the DLQ/operator, \
         not silently swallowed"
    );
}

// ============================================================================
// Sync fan-out: CascadeErrorMode, deadlines, channel reuse
// ============================================================================

/// A saga coordinator that answers every Execute with `fail` (or OK) and
/// records the requests (with their metadata) it received.
#[derive(Clone, Default)]
struct ScriptedSagaServer {
    fail: Option<tonic::Code>,
    requests: Arc<Mutex<Vec<(SagaHandleRequest, tonic::metadata::MetadataMap)>>>,
}

#[tonic::async_trait]
impl SagaCoordServiceTrait for ScriptedSagaServer {
    async fn execute(
        &self,
        request: tonic::Request<SagaHandleRequest>,
    ) -> Result<tonic::Response<SagaResponse>, Status> {
        let metadata = request.metadata().clone();
        self.requests
            .lock()
            .await
            .push((request.into_inner(), metadata));
        match self.fail {
            Some(code) => Err(Status::new(code, "saga delivery rejected")),
            None => Ok(tonic::Response::new(SagaResponse::default())),
        }
    }

    async fn execute_speculative(
        &self,
        _request: tonic::Request<SpeculateSagaRequest>,
    ) -> Result<tonic::Response<SagaResponse>, Status> {
        Err(Status::unimplemented("not exercised"))
    }
}

/// Serve `server` on an ephemeral port and register it as a saga for
/// `orders` under `name`.
async fn spawn_saga(discovery: &StaticServiceDiscovery, name: &str, server: ScriptedSagaServer) {
    let (listener, port) = bind_ephemeral().await;
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(SagaCoordinatorServiceServer::new(server))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    discovery
        .register_saga(name, "orders", "127.0.0.1", port)
        .await;
}

struct CascadeRig {
    ctx: GrpcAggregateContext,
    dlq: Arc<CapturingDlqPublisher>,
    first: ScriptedSagaServer,
    second: ScriptedSagaServer,
}

/// Two sagas subscribed to `orders`: `saga-a` answers `first_fails`,
/// `saga-b` answers `second_fails`. Discovery order is unspecified.
async fn cascade_rig_with(
    mode: CascadeErrorMode,
    first_fails: Option<tonic::Code>,
    second_fails: Option<tonic::Code>,
) -> CascadeRig {
    let discovery = Arc::new(StaticServiceDiscovery::new());
    let first = ScriptedSagaServer {
        fail: first_fails,
        ..Default::default()
    };
    let second = ScriptedSagaServer {
        fail: second_fails,
        ..Default::default()
    };
    spawn_saga(&discovery, "saga-a", first.clone()).await;
    spawn_saga(&discovery, "saga-b", second.clone()).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let dlq = Arc::new(CapturingDlqPublisher::default());
    let ctx = GrpcAggregateContext::new(
        Arc::new(MockEventStore::new()),
        Arc::new(SnapshotRepository::new(Arc::new(MockSnapshotStore::new()))),
        discovery,
        Arc::new(MockEventBus::new()),
    )
    .with_sync_mode(crate::proto::SyncMode::Cascade)
    .with_cascade_error_mode(mode)
    .with_dlq_publisher(dlq.clone());
    CascadeRig {
        ctx,
        dlq,
        first,
        second,
    }
}

/// `saga-a` answers `first_fails`; `saga-b` succeeds.
async fn cascade_rig(mode: CascadeErrorMode, first_fails: Option<tonic::Code>) -> CascadeRig {
    cascade_rig_with(mode, first_fails, None).await
}

fn cascade_book() -> EventBook {
    book_with_cover(vec![make_event_page(0)])
}

async fn calls(server: &ScriptedSagaServer) -> usize {
    server.requests.lock().await.len()
}

/// FAIL_FAST (the default): the first failing saga fails the command and no
/// saga after it is called.
#[tokio::test]
async fn sync_fanout_fail_fast_stops_at_first_failure() {
    let rig = cascade_rig_with(
        CascadeErrorMode::CascadeErrorFailFast,
        Some(tonic::Code::FailedPrecondition),
        Some(tonic::Code::FailedPrecondition),
    )
    .await;
    let err = rig.ctx.sync_fanout(&cascade_book()).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("saga delivery rejected"));
    assert_eq!(calls(&rig.first).await + calls(&rig.second).await, 1);
    assert_eq!(rig.dlq.publish_calls.load(Ordering::SeqCst), 0);
}

/// COMPENSATE also stops at the first failure and fails the command (the
/// saga coordinator compensates the rejected command at its source).
#[tokio::test]
async fn sync_fanout_compensate_stops_at_first_failure() {
    let rig = cascade_rig_with(
        CascadeErrorMode::CascadeErrorCompensate,
        Some(tonic::Code::FailedPrecondition),
        Some(tonic::Code::FailedPrecondition),
    )
    .await;
    rig.ctx.sync_fanout(&cascade_book()).await.unwrap_err();
    assert_eq!(calls(&rig.first).await + calls(&rig.second).await, 1);
    assert_eq!(rig.dlq.publish_calls.load(Ordering::SeqCst), 0);
}

/// CONTINUE runs every saga and succeeds with the ones that succeeded —
/// nothing is dead-lettered.
#[tokio::test]
async fn sync_fanout_continue_runs_all_and_succeeds() {
    let rig = cascade_rig_with(
        CascadeErrorMode::CascadeErrorContinue,
        Some(tonic::Code::FailedPrecondition),
        Some(tonic::Code::FailedPrecondition),
    )
    .await;
    rig.ctx.sync_fanout(&cascade_book()).await.unwrap();
    assert_eq!(calls(&rig.first).await, 1);
    assert_eq!(calls(&rig.second).await, 1);
    assert_eq!(rig.dlq.publish_calls.load(Ordering::SeqCst), 0);
}

/// DEAD_LETTER runs every saga, dead-letters each failure, and lets the
/// command succeed.
#[tokio::test]
async fn sync_fanout_dead_letter_runs_all_and_captures_failures() {
    let rig = cascade_rig(
        CascadeErrorMode::CascadeErrorDeadLetter,
        Some(tonic::Code::FailedPrecondition),
    )
    .await;
    rig.ctx.sync_fanout(&cascade_book()).await.unwrap();
    assert_eq!(calls(&rig.first).await, 1);
    assert_eq!(calls(&rig.second).await, 1);
    let captured = rig.dlq.captured.lock().await.clone();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].source_component, "saga-a");
    assert_eq!(captured[0].source_component_type, "cascade");
    match &captured[0].rejection_details {
        Some(RejectionDetails::EventProcessingFailed(details)) => {
            assert!(!details.is_transient, "a rejection is not transient");
            assert!(details.error.contains("saga-a"));
        }
        other => panic!("expected EventProcessingFailed, got {other:?}"),
    }
}

/// A transient saga failure is dead-lettered as transient.
#[tokio::test]
async fn sync_fanout_dead_letter_marks_transient_failures() {
    let rig = cascade_rig(
        CascadeErrorMode::CascadeErrorDeadLetter,
        Some(tonic::Code::Unavailable),
    )
    .await;
    rig.ctx.sync_fanout(&cascade_book()).await.unwrap();
    let captured = rig.dlq.captured.lock().await.clone();
    match &captured[0].rejection_details {
        Some(RejectionDetails::EventProcessingFailed(details)) => assert!(details.is_transient),
        other => panic!("expected EventProcessingFailed, got {other:?}"),
    }
}

/// Every mode succeeds when every saga succeeds; each saga receives the
/// caller's cascade error mode and a deadline.
#[tokio::test]
async fn sync_fanout_forwards_mode_and_deadline() {
    for mode in [
        CascadeErrorMode::CascadeErrorFailFast,
        CascadeErrorMode::CascadeErrorContinue,
        CascadeErrorMode::CascadeErrorCompensate,
        CascadeErrorMode::CascadeErrorDeadLetter,
    ] {
        let rig = cascade_rig(mode, None).await;
        let ctx = rig
            .ctx
            .with_downstream_timeout(std::time::Duration::from_secs(7));
        ctx.sync_fanout(&cascade_book()).await.unwrap();
        let requests = rig.first.requests.lock().await;
        assert_eq!(requests.len(), 1);
        let (request, metadata) = &requests[0];
        assert_eq!(request.cascade_error_mode, mode as i32);
        assert_eq!(
            request.sync_mode,
            crate::proto::SyncMode::Cascade as i32,
            "the cascade continues downstream"
        );
        let timeout = metadata
            .get("grpc-timeout")
            .expect("fan-out calls carry a deadline")
            .to_str()
            .unwrap()
            .to_string();
        assert!(timeout.starts_with('7'), "deadline was {timeout}");
    }
}

/// Repeated fan-outs reuse one channel per saga endpoint.
#[tokio::test]
async fn sync_fanout_reuses_channels_across_commands() {
    let rig = cascade_rig(CascadeErrorMode::CascadeErrorFailFast, None).await;
    let channels = Arc::new(ChannelCache::new());
    let ctx = rig.ctx.with_channel_cache(channels.clone());
    ctx.sync_fanout(&cascade_book()).await.unwrap();
    ctx.sync_fanout(&cascade_book()).await.unwrap();
    assert_eq!(channels.len(), 2, "one channel per saga endpoint");
    assert_eq!(calls(&rig.first).await, 2);
}

/// Without a sync mode there is no synchronous fan-out at all.
#[tokio::test]
async fn sync_fanout_without_sync_mode_calls_nothing() {
    let rig = cascade_rig(CascadeErrorMode::CascadeErrorFailFast, None).await;
    let ctx = GrpcAggregateContext::new(
        Arc::new(MockEventStore::new()),
        Arc::new(SnapshotRepository::new(Arc::new(MockSnapshotStore::new()))),
        Arc::new(StaticServiceDiscovery::new()),
        Arc::new(MockEventBus::new()),
    );
    assert!(ctx.sync_fanout(&cascade_book()).await.unwrap().is_empty());
    drop(rig);
}

/// SIMPLE runs projectors only — sagas are never called.
#[tokio::test]
async fn sync_fanout_simple_does_not_call_sagas() {
    let rig = cascade_rig(CascadeErrorMode::CascadeErrorFailFast, None).await;
    let ctx = rig.ctx.with_sync_mode(crate::proto::SyncMode::Simple);
    ctx.sync_fanout(&cascade_book()).await.unwrap();
    assert_eq!(calls(&rig.first).await, 0);
    assert_eq!(calls(&rig.second).await, 0);
}

/// A projector endpoint that does not serve ProjectorCoordinatorService
/// (UNIMPLEMENTED) is skipped instead of failing every SIMPLE command.
#[tokio::test]
async fn sync_projectors_skip_unimplemented_endpoints() {
    #[derive(Clone)]
    struct Unserved(tonic::Code);
    #[tonic::async_trait]
    impl ProjectorCoordServiceTrait for Unserved {
        async fn handle_sync(
            &self,
            _request: tonic::Request<EventRequest>,
        ) -> Result<tonic::Response<Projection>, Status> {
            Err(Status::new(self.0, "nope"))
        }
        async fn handle(
            &self,
            _request: tonic::Request<EventBook>,
        ) -> Result<tonic::Response<()>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn handle_speculative(
            &self,
            _request: tonic::Request<SpeculateProjectorRequest>,
        ) -> Result<tonic::Response<Projection>, Status> {
            Err(Status::unimplemented("unused"))
        }
    }

    for (code, ok) in [
        (tonic::Code::Unimplemented, true),
        (tonic::Code::NotFound, true),
        (tonic::Code::Internal, false),
    ] {
        let (listener, port) = bind_ephemeral().await;
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(ProjectorCoordinatorServiceServer::new(Unserved(code)))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let discovery = Arc::new(StaticServiceDiscovery::new());
        discovery
            .register_projector("prj", "orders", "127.0.0.1", port)
            .await;
        let ctx = GrpcAggregateContext::new(
            Arc::new(MockEventStore::new()),
            Arc::new(SnapshotRepository::new(Arc::new(MockSnapshotStore::new()))),
            discovery,
            Arc::new(MockEventBus::new()),
        )
        .with_sync_mode(crate::proto::SyncMode::Simple);
        let result = ctx.sync_fanout(&cascade_book()).await;
        assert_eq!(result.is_ok(), ok, "{code:?}: {result:?}");
        if let Ok(projections) = result {
            assert!(projections.is_empty());
        }
    }
}

// ============================================================================
// persist_target_cover
// ============================================================================

fn response_with_cover(domain: &str, root: Option<Uuid>) -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: root.map(|r| ProtoUuid {
                value: r.as_bytes().to_vec(),
            }),
            correlation_id: "from-business".to_string(),
            edition: Some(Edition {
                name: "v2".to_string(),
                divergences: vec![],
            }),
            ext: None,
        }),
        ..Default::default()
    }
}

/// Events always land on the command's aggregate under the validated
/// correlation id; the response's edition is kept.
#[test]
fn persist_target_cover_uses_command_target() {
    let root = Uuid::new_v4();
    let cover = persist_target_cover(
        &response_with_cover("orders", Some(root)),
        "orders",
        root,
        "corr",
    )
    .unwrap();
    assert_eq!(cover.domain, "orders");
    assert_eq!(cover.root.unwrap().value, root.as_bytes().to_vec());
    assert_eq!(cover.correlation_id, "corr");
    assert_eq!(cover.edition.unwrap().name, "v2");
}

/// An unset domain/root in the response is filled from the command.
#[test]
fn persist_target_cover_fills_missing_identity() {
    let root = Uuid::new_v4();
    for received in [EventBook::default(), response_with_cover("", None)] {
        let cover = persist_target_cover(&received, "orders", root, "corr").unwrap();
        assert_eq!(cover.domain, "orders");
        assert_eq!(cover.root.unwrap().value, root.as_bytes().to_vec());
    }
}

/// A response naming another domain or root is refused (non-retryable).
#[test]
fn persist_target_cover_refuses_foreign_aggregate() {
    let root = Uuid::new_v4();
    for received in [
        response_with_cover("payments", Some(root)),
        response_with_cover("orders", Some(Uuid::new_v4())),
        response_with_cover("", Some(Uuid::new_v4())),
    ] {
        let err = persist_target_cover(&received, "orders", root, "corr").unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(!crate::utils::retry::is_retryable_status(&err));
    }
}

/// persist_events writes the new pages to the command's aggregate even when
/// the business response's cover points elsewhere — never to the foreign one.
#[tokio::test]
async fn persist_events_refuses_business_cover_for_another_aggregate() {
    let store = Arc::new(MockEventStore::new());
    let ctx = build_ctx_with_stores(store.clone(), Arc::new(MockSnapshotStore::new()));
    let root = Uuid::new_v4();
    let foreign = Uuid::new_v4();
    let mut received = response_with_cover("orders", Some(foreign));
    received.pages = vec![make_event_page(0)];
    let err = ctx
        .persist_events(
            &EventBook::default(),
            &received,
            "orders",
            "",
            root,
            "corr",
            None,
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(store.get("orders", "", foreign).await.unwrap().is_empty());
    assert!(store.get("orders", "", root).await.unwrap().is_empty());
}
