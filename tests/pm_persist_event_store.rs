//! Integration test: PM persist -> real SqliteEventStore.
//!
//! Run with:
//! ```bash
//! cargo test --test pm_persist_event_store --features "test-utils" -- --nocapture
//! ```
//!
//! Sister test to `aggregate_pipeline_event_store.rs`. The
//! `GrpcPMContext::persist_pm_events` path at
//! `process_manager/grpc/mod.rs:115` writes PM-state events directly
//! to the event store, bypassing the aggregate command pipeline.
//! `PmWithEvents` in `process_manager/tests.rs` stubs the outcome,
//! so unit tests never see this code run against a real store. The
//! storage tests in `storage_sqlite.rs` exercise `EventStore.add`
//! at the trait surface but don't know anything about PM's
//! edition / correlation-id / pm_root extraction logic.
//!
//! This test wires `persist_pm_event_book` (the free fn extracted
//! from `GrpcPMContext::persist_pm_events` for testability) to a
//! real in-memory SQLite event store + a tracking event bus, and
//! verifies:
//!
//! - O7/D-11: the storage key is derived from the in-flight
//!   `correlation_id` via `CorrelationRootExt::correlation_root`
//!   (UUID pass-through; friendly id → UUIDv5) — NOT from the
//!   handler book's `cover.root`. This keeps the persist-side root
//!   identical to the command-stamping side in
//!   `execute_pm_commands`, so rejection notifications route back
//!   to the persisted PM aggregate.
//! - PM events are persisted under the PM's `pm_domain` argument
//!   (not whatever the trigger's domain was).
//! - Edition propagates from the book's cover to the store column.
//! - After persist, the bus sees a book carrying exactly the pages
//!   the handler emitted (R2-02-LIVE: the publish step no longer
//!   re-reads the store, so historical PM events are NOT re-fired).
//! - Two consecutive persist calls produce events at sequences 0, 1
//!   and each publish carries only its own slice (not the full
//!   accumulated history).
//! - A sequence conflict (re-using a sequence the store already has)
//!   returns `CommandOutcome::Rejected { code: Internal, .. }`,
//!   which `orchestrate_pm` classifies as immediate-Rejected per
//!   R2-15 (does NOT count toward retry budget).

#![cfg(feature = "test-utils")]

use std::sync::Arc;

use prost_types::Any;
use sqlx::sqlite::SqlitePoolOptions;
use tokio::sync::Mutex;
use uuid::Uuid;

use angzarr::bus::{self, EventBus, EventHandler, PublishResult};
use angzarr::orchestration::command::CommandOutcome;
use angzarr::orchestration::process_manager::grpc::persist_pm_event_book;
use angzarr::orchestration::shared::CorrelationRootExt;
use angzarr::proto::{
    event_page, page_header, Cover, Edition, EventBook, EventPage, PageHeader, Uuid as ProtoUuid,
};
use angzarr::storage::{AddMeta, EventStore, SqliteEventStore};
use async_trait::async_trait;

// ============================================================================
// Fixtures
// ============================================================================

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

/// Event bus that records every publish so the test can assert which
/// books reached the bus alongside the storage write.
struct RecordingEventBus {
    published: Mutex<Vec<EventBook>>,
}

impl RecordingEventBus {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            published: Mutex::new(Vec::new()),
        })
    }

    async fn calls(&self) -> Vec<EventBook> {
        self.published.lock().await.clone()
    }
}

#[async_trait]
impl EventBus for RecordingEventBus {
    async fn publish(&self, book: Arc<EventBook>) -> bus::error::Result<PublishResult> {
        self.published.lock().await.push((*book).clone());
        Ok(PublishResult::default())
    }

    async fn subscribe(&self, _handler: Box<dyn EventHandler>) -> bus::error::Result<()> {
        unimplemented!("Not needed for these tests")
    }

    async fn create_subscriber(
        &self,
        _name: &str,
        _domain_filter: Option<&str>,
    ) -> bus::error::Result<Arc<dyn EventBus>> {
        unimplemented!("Not needed for these tests")
    }
}

fn proto_uuid(u: Uuid) -> ProtoUuid {
    ProtoUuid {
        value: u.as_bytes().to_vec(),
    }
}

fn pm_event_book(
    pm_domain: &str,
    pm_root: Uuid,
    correlation_id: &str,
    edition: Option<&str>,
    sequences: &[u32],
) -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: pm_domain.to_string(),
            root: Some(proto_uuid(pm_root)),
            correlation_id: correlation_id.to_string(),
            edition: edition.map(|name| Edition {
                name: name.to_string(),
                divergences: vec![],
            }),
            ext: None,
        }),
        pages: sequences
            .iter()
            .map(|&seq| EventPage {
                header: Some(PageHeader {
                    sync_mode: None,
                    sequence_type: Some(page_header::SequenceType::Sequence(seq)),
                }),
                payload: Some(event_page::Payload::Event(Any {
                    type_url: "test.PmEvent".to_string(),
                    value: vec![],
                })),
                created_at: None,
                ..Default::default()
            })
            .collect(),
        snapshot: None,
        ..Default::default()
    }
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

/// A PM event book persists to the SQLite store under the PM's
/// `pm_domain` + the CORRELATION-DERIVED root (O7/D-11) + default
/// edition. The handler's cover.root (a random v4 here) is NOT the
/// storage key — the correlation id is the authoritative PM root, so
/// the persist-side identity always matches the command-stamping
/// side. The bus sees the emitted book published.
#[tokio::test]
async fn pm_persist_writes_event_book_to_store_and_bus() {
    let event_store = create_sqlite_event_store().await;
    let bus_recorder = RecordingEventBus::new();
    let event_bus: Arc<dyn EventBus> = bus_recorder.clone();
    let cover_root = Uuid::new_v4();

    let book = pm_event_book("fulfillment-pm", cover_root, "corr-1", None, &[0]);
    let outcome = persist_pm_event_book(
        &(event_store.clone() as Arc<dyn EventStore>),
        &event_bus,
        "fulfillment-pm",
        &book,
        "corr-1",
        None,
        None,
    )
    .await;
    assert!(
        matches!(outcome, CommandOutcome::Success(_)),
        "persist outcome must be Success, got {outcome:?}"
    );

    // Stored under (pm_domain, default_edition, correlation_root("corr-1")).
    let derived_root = "corr-1".correlation_root();
    let persisted = event_store
        .get("fulfillment-pm", "", derived_root)
        .await
        .expect("event_store.get");
    assert_eq!(
        persisted.len(),
        1,
        "expected 1 PM event under the correlation-derived root (D-11)"
    );
    assert_eq!(event_sequence_num(&persisted[0]), 0);

    // The handler's cover.root is NOT a storage identity — pre-D-11 it
    // was, and pinning that here would resurrect the split-identity bug.
    let under_cover_root = event_store
        .get("fulfillment-pm", "", cover_root)
        .await
        .expect("event_store.get");
    assert_eq!(
        under_cover_root.len(),
        0,
        "nothing may be stored under the handler's cover.root (O7/D-11: \
         the correlation id is the authoritative PM root)"
    );

    // The bus saw exactly one publish carrying the page the
    // handler just emitted.
    let bus_calls = bus_recorder.calls().await;
    assert_eq!(bus_calls.len(), 1, "expected exactly one bus publish");
    assert_eq!(
        bus_calls[0].pages.len(),
        1,
        "published book must carry the newly-emitted event"
    );
}

/// Two consecutive persist calls on the same PM root produce events
/// at sequences 0, 1. Pins the PM-persist publish flow against a
/// real store, mirroring the aggregate `pipeline_increments_sequence`
/// test.
#[tokio::test]
async fn pm_persist_increments_sequence_across_two_calls() {
    let event_store = create_sqlite_event_store().await;
    let bus_recorder = RecordingEventBus::new();
    let event_bus: Arc<dyn EventBus> = bus_recorder.clone();
    let cover_root = Uuid::new_v4();

    let book0 = pm_event_book("pm-domain", cover_root, "corr-1", None, &[0]);
    let outcome0 = persist_pm_event_book(
        &(event_store.clone() as Arc<dyn EventStore>),
        &event_bus,
        "pm-domain",
        &book0,
        "corr-1",
        None,
        None,
    )
    .await;
    assert!(matches!(outcome0, CommandOutcome::Success(_)));

    let book1 = pm_event_book("pm-domain", cover_root, "corr-1", None, &[1]);
    let outcome1 = persist_pm_event_book(
        &(event_store.clone() as Arc<dyn EventStore>),
        &event_bus,
        "pm-domain",
        &book1,
        "corr-1",
        None,
        None,
    )
    .await;
    assert!(matches!(outcome1, CommandOutcome::Success(_)));

    // O7/D-11: both persists share correlation "corr-1", so both land
    // under the same correlation-derived root.
    let persisted = event_store
        .get("pm-domain", "", "corr-1".correlation_root())
        .await
        .expect("event_store.get");
    assert_eq!(persisted.len(), 2);
    assert_eq!(event_sequence_num(&persisted[0]), 0);
    assert_eq!(event_sequence_num(&persisted[1]), 1);

    // Two bus publishes -- one per persist -- and each carries
    // exactly the slice that call emitted (R2-02-LIVE: the second
    // publish must not re-fire the first event).
    let bus_calls = bus_recorder.calls().await;
    assert_eq!(bus_calls.len(), 2);
    assert_eq!(
        bus_calls[0].pages.len(),
        1,
        "first publish carries the 1 event that call persisted"
    );
    assert_eq!(event_sequence_num(&bus_calls[0].pages[0]), 0);
    assert_eq!(
        bus_calls[1].pages.len(),
        1,
        "second publish carries the 1 newly-emitted event, NOT the full 2-event history"
    );
    assert_eq!(event_sequence_num(&bus_calls[1].pages[0]), 1);
}

/// R2-02-LIVE regression: when prior PM events already exist in the
/// store, a fresh persist publishes ONLY the newly-emitted pages,
/// not the entire event-store history. Pre-fix `persist_pm_event_book`
/// called `event_store.get(pm_domain, edition, pm_root)` after the
/// add and re-published every historical event, fanning out
/// O(history) on every PM update.
#[tokio::test]
async fn pm_persist_publishes_only_new_events_not_history() {
    let event_store = create_sqlite_event_store().await;
    let bus_recorder = RecordingEventBus::new();
    let event_bus: Arc<dyn EventBus> = bus_recorder.clone();
    let cover_root = Uuid::new_v4();
    // O7/D-11: the production persist keys storage by the
    // correlation-derived root, so the seeded history must live under
    // that same root for the persist to append to it.
    let pm_root = "flow-corr".correlation_root();

    // Seed 3 prior PM events at sequences 0, 1, 2 by calling
    // event_store.add directly so the bus recorder stays empty --
    // we only want to observe what the post-load persist publishes.
    let seed_pages = pm_event_book("pm-domain", cover_root, "flow-corr", None, &[0, 1, 2]).pages;
    event_store
        .add(
            "pm-domain",
            "",
            pm_root,
            seed_pages,
            &AddMeta {
                correlation_id: "flow-corr",
                external_id: None,
                source_info: None,
                ext: None,
            },
        )
        .await
        .expect("seed prior PM events");
    assert_eq!(
        bus_recorder.calls().await.len(),
        0,
        "seed must not touch the bus"
    );

    // Now persist 2 NEW events at sequences 3, 4 through the
    // production path.
    let new_book = pm_event_book("pm-domain", cover_root, "flow-corr", None, &[3, 4]);
    let outcome = persist_pm_event_book(
        &(event_store.clone() as Arc<dyn EventStore>),
        &event_bus,
        "pm-domain",
        &new_book,
        "flow-corr",
        None,
        None,
    )
    .await;
    assert!(matches!(outcome, CommandOutcome::Success(_)));

    // Storage holds all 5 events (3 prior + 2 new).
    let persisted = event_store
        .get("pm-domain", "", pm_root)
        .await
        .expect("event_store.get");
    assert_eq!(persisted.len(), 5, "store should hold all 5 events");

    // Bus saw exactly ONE publish (one persist call) carrying
    // exactly the 2 newly-emitted pages -- NOT all 5 from history.
    let bus_calls = bus_recorder.calls().await;
    assert_eq!(
        bus_calls.len(),
        1,
        "one persist call must produce one bus publish"
    );
    assert_eq!(
        bus_calls[0].pages.len(),
        2,
        "published book must carry only the 2 newly-persisted events, not all 5 from history"
    );
    let published_seqs: Vec<u32> = bus_calls[0].pages.iter().map(event_sequence_num).collect();
    assert_eq!(
        published_seqs,
        vec![3, 4],
        "published pages must be the new sequences, not the seeded 0..2"
    );
}

/// R2-02-LIVE: the published cover's `correlation_id` always reflects
/// the in-flight `correlation_id` parameter, even if the PM service
/// returned a `process_events.cover` with a stale or empty value.
/// Downstream subscribers (other PMs, sagas, projectors) rely on this
/// to track the active cross-domain flow.
#[tokio::test]
async fn pm_persist_publishes_book_with_stamped_correlation_id() {
    let event_store = create_sqlite_event_store().await;
    let bus_recorder = RecordingEventBus::new();
    let event_bus: Arc<dyn EventBus> = bus_recorder.clone();
    let cover_root = Uuid::new_v4();

    // Simulate the PM service returning a cover with NO correlation_id
    // set (or whatever happened to be on the handler-built book).
    // The coordinator passes the in-flight correlation_id separately
    // and the publish step must stamp it onto the outgoing cover.
    let book_with_blank_corr = pm_event_book("pm-domain", cover_root, "", None, &[0]);
    let outcome = persist_pm_event_book(
        &(event_store.clone() as Arc<dyn EventStore>),
        &event_bus,
        "pm-domain",
        &book_with_blank_corr,
        "in-flight-corr",
        None,
        None,
    )
    .await;
    assert!(matches!(outcome, CommandOutcome::Success(_)));

    let bus_calls = bus_recorder.calls().await;
    assert_eq!(bus_calls.len(), 1);
    let published_corr = bus_calls[0]
        .cover
        .as_ref()
        .map(|c| c.correlation_id.as_str())
        .unwrap_or("");
    assert_eq!(
        published_corr, "in-flight-corr",
        "published cover must carry the in-flight correlation_id, not the (blank) one the PM service returned"
    );
}

/// PM events with `cover.edition = "branch-x"` persist under that
/// edition, not the default. The default-edition view sees zero
/// events. Mirrors the aggregate edition-propagation test against
/// PM's persist path (which extracts edition from the cover via
/// `process_events.edition()`).
#[tokio::test]
async fn pm_persist_propagates_edition_to_store() {
    let event_store = create_sqlite_event_store().await;
    let event_bus: Arc<dyn EventBus> = RecordingEventBus::new();
    let cover_root = Uuid::new_v4();
    // O7/D-11: storage is keyed by the correlation-derived root.
    let pm_root = "corr-1".correlation_root();

    let book = pm_event_book("pm-domain", cover_root, "corr-1", Some("branch-x"), &[0]);
    let outcome = persist_pm_event_book(
        &(event_store.clone() as Arc<dyn EventStore>),
        &event_bus,
        "pm-domain",
        &book,
        "corr-1",
        None,
        None,
    )
    .await;
    assert!(matches!(outcome, CommandOutcome::Success(_)));

    let branch = event_store
        .get("pm-domain", "branch-x", pm_root)
        .await
        .expect("get");
    assert_eq!(
        branch.len(),
        1,
        "expected 1 event under 'branch-x', got {}",
        branch.len()
    );
    let default = event_store
        .get("pm-domain", "", pm_root)
        .await
        .expect("get");
    assert_eq!(
        default.len(),
        0,
        "default-edition read must not see branch events; got {}",
        default.len()
    );
}

/// Persisting at a sequence the store already has returns
/// `CommandOutcome::Retryable` (O3, `43dc9f1a`): a PM sequence conflict
/// means a concurrent workflow update won the race — `orchestrate_pm`'s
/// refetch-and-retry loop reloads fresh PM state and re-runs the handler.
/// Pre-O3 this surfaced as `Rejected { Internal }`, routing recoverable
/// contention to the DLQ instead of retrying.
#[tokio::test]
async fn pm_persist_sequence_conflict_returns_retryable() {
    let event_store = create_sqlite_event_store().await;
    let event_bus: Arc<dyn EventBus> = RecordingEventBus::new();
    let cover_root = Uuid::new_v4();

    // First persist at sequence 0 -- succeeds.
    let book = pm_event_book("pm-domain", cover_root, "corr-1", None, &[0]);
    let first = persist_pm_event_book(
        &(event_store.clone() as Arc<dyn EventStore>),
        &event_bus,
        "pm-domain",
        &book,
        "corr-1",
        None,
        None,
    )
    .await;
    assert!(matches!(first, CommandOutcome::Success(_)));

    // Second persist at sequence 0 -- conflict.
    let conflict = persist_pm_event_book(
        &(event_store.clone() as Arc<dyn EventStore>),
        &event_bus,
        "pm-domain",
        &book,
        "corr-1",
        None,
        None,
    )
    .await;
    match conflict {
        CommandOutcome::Retryable { reason, .. } => {
            assert!(
                reason.contains("sequence conflict"),
                "Retryable reason should name the sequence conflict, got: {reason}"
            );
        }
        other => panic!(
            "expected Retryable (O3: conflict drives orchestrate_pm's refetch-and-retry), \
             got {other:?}"
        ),
    }

    // No second event written (under the correlation-derived root, D-11).
    let persisted = event_store
        .get("pm-domain", "", "corr-1".correlation_root())
        .await
        .expect("event_store.get");
    assert_eq!(
        persisted.len(),
        1,
        "sequence conflict must not double-persist, got {} events",
        persisted.len()
    );
}
