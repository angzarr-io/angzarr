//! Unit tests for `persist_pm_event_book`, the PM persist + publish
//! seam extracted from `GrpcPMContext::persist_pm_events` so the
//! ephemeral mutants container (which runs `cargo test --lib` only)
//! can see the contract.
//!
//! The integration tests in `tests/pm_persist_event_store.rs` cover
//! the same surface end-to-end against a real SQLite store; the unit
//! tests here exist so mutation testing on the publish-step struct
//! fields (cover, pages, correlation_id stamping) actually gets a
//! chance to kill mutants — without `--lib`, `cargo mutants` cannot
//! observe an integration-only assertion.

use std::sync::Arc;

use prost_types::Any;
use uuid::Uuid;

use crate::bus::{EventBus, MockEventBus};
use crate::orchestration::command::CommandOutcome;
use crate::proto::{
    event_page, page_header, Cover, EventBook, EventPage, PageHeader, Uuid as ProtoUuid,
};
use crate::storage::{mock::MockEventStore, EventStore};

use super::persist_pm_event_book;

fn proto_uuid(u: Uuid) -> ProtoUuid {
    ProtoUuid {
        value: u.as_bytes().to_vec(),
    }
}

fn pm_book(pm_domain: &str, pm_root: Uuid, correlation_id: &str, sequences: &[u32]) -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: pm_domain.to_string(),
            root: Some(proto_uuid(pm_root)),
            correlation_id: correlation_id.to_string(),
            edition: None,
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

/// R2-02-LIVE: the publish step ships the pages from
/// `process_events` directly. Pre-fix, the code re-read the store
/// after `event_store.add` and republished the full history.
#[tokio::test]
async fn persist_publishes_pages_from_process_events() {
    let store: Arc<dyn EventStore> = Arc::new(MockEventStore::new());
    let bus = Arc::new(MockEventBus::new());
    let bus_dyn: Arc<dyn EventBus> = bus.clone();
    let pm_root = Uuid::new_v4();

    let book = pm_book("pm", pm_root, "corr", &[0, 1, 2]);
    let outcome = persist_pm_event_book(&store, &bus_dyn, "pm", &book, "corr").await;
    assert!(matches!(outcome, CommandOutcome::Success(_)));

    let published = bus.take_published().await;
    assert_eq!(published.len(), 1, "expected one publish per persist call");
    assert_eq!(
        published[0].pages.len(),
        3,
        "publish must carry exactly the 3 handler-emitted pages"
    );
}

/// O3 regression: a sequence conflict from `event_store.add` must map to
/// `CommandOutcome::Retryable` — not `Rejected` — so `orchestrate_pm`'s
/// documented refetch-and-retry loop actually fires. Pre-fix, ALL add
/// errors mapped to `Rejected { Internal }`, making that loop dead code
/// and DLQ'ing healthy concurrent workflow updates.
#[tokio::test]
async fn persist_sequence_conflict_maps_to_retryable() {
    let store: Arc<dyn EventStore> = Arc::new(MockEventStore::new());
    let bus = Arc::new(MockEventBus::new());
    let bus_dyn: Arc<dyn EventBus> = bus.clone();
    let pm_root = Uuid::new_v4();

    // First persist claims sequence 0.
    let first = pm_book("pm", pm_root, "corr", &[0]);
    assert!(matches!(
        persist_pm_event_book(&store, &bus_dyn, "pm", &first, "corr").await,
        CommandOutcome::Success(_)
    ));

    // A "concurrent PM instance" persists at the same sequence.
    let conflicting = pm_book("pm", pm_root, "corr", &[0]);
    let outcome = persist_pm_event_book(&store, &bus_dyn, "pm", &conflicting, "corr").await;
    match outcome {
        CommandOutcome::Retryable { reason, .. } => {
            assert!(
                reason.to_lowercase().contains("sequence"),
                "reason should name the conflict, got: {reason}"
            );
        }
        other => panic!("a sequence conflict must be Retryable (refetch-and-retry), got {other:?}"),
    }
}

/// R2-02-LIVE: the publish step preserves the cover from
/// `process_events`. Mutating away the `cover` field would send a
/// book with no cover to the bus, breaking routing-key resolution.
#[tokio::test]
async fn persist_publishes_book_with_cover_present() {
    let store: Arc<dyn EventStore> = Arc::new(MockEventStore::new());
    let bus = Arc::new(MockEventBus::new());
    let bus_dyn: Arc<dyn EventBus> = bus.clone();
    let pm_root = Uuid::new_v4();

    let book = pm_book("pm-domain", pm_root, "corr", &[0]);
    persist_pm_event_book(&store, &bus_dyn, "pm-domain", &book, "corr").await;

    let published = bus.take_published().await;
    let cover = published[0]
        .cover
        .as_ref()
        .expect("published book must carry a cover");
    assert_eq!(
        cover.domain, "pm-domain",
        "cover.domain must come from process_events (not Default::default)"
    );
}

/// R2-02-LIVE: the publish step ALWAYS stamps the in-flight
/// `correlation_id` onto the published cover, even when the PM
/// service returned a book with a blank or stale correlation. This
/// is the coordinator's guarantee that downstream subscribers see
/// the same correlation across the cross-domain flow.
#[tokio::test]
async fn persist_stamps_in_flight_correlation_id_on_cover() {
    let store: Arc<dyn EventStore> = Arc::new(MockEventStore::new());
    let bus = Arc::new(MockEventBus::new());
    let bus_dyn: Arc<dyn EventBus> = bus.clone();
    let pm_root = Uuid::new_v4();

    // PM service returned a cover with NO correlation_id.
    let book = pm_book("pm", pm_root, "", &[0]);
    persist_pm_event_book(&store, &bus_dyn, "pm", &book, "in-flight").await;

    let published = bus.take_published().await;
    assert_eq!(
        published[0].cover.as_ref().unwrap().correlation_id,
        "in-flight",
        "publish cover must carry the in-flight correlation_id"
    );
}

/// O7/D-11: `persist_pm_event_book` keys storage by the CORRELATION-derived
/// root (`CorrelationRootExt::correlation_root`), NOT by the handler book's
/// `cover.root` — even when that cover.root is a perfectly valid, different
/// UUID. The correlation id is the authoritative PM root by design; the
/// command-stamping side (`execute_pm_commands`) derives the compensation
/// root the same way, so a rejection notification always reaches the PM
/// state persisted here. Pre-fix this function used cover.root (NIL
/// fallback), which could disagree with the stamped root.
#[tokio::test]
async fn persist_stores_under_correlation_derived_root_not_cover_root() {
    use crate::orchestration::shared::CorrelationRootExt;

    let store: Arc<dyn EventStore> = Arc::new(MockEventStore::new());
    let bus = Arc::new(MockEventBus::new());
    let bus_dyn: Arc<dyn EventBus> = bus.clone();
    // A valid v4 UUID on the cover — deliberately different from any
    // correlation-derived value.
    let cover_root = Uuid::new_v4();

    let book = pm_book("pm", cover_root, "friendly-flow-7", &[0]);
    let outcome = persist_pm_event_book(&store, &bus_dyn, "pm", &book, "friendly-flow-7").await;
    assert!(matches!(outcome, CommandOutcome::Success(_)));

    let derived = "friendly-flow-7".correlation_root();
    assert_ne!(
        derived, cover_root,
        "test precondition: the derived root must differ from the cover root"
    );

    let under_derived = store.get("pm", "", derived).await.expect("get");
    assert_eq!(
        under_derived.len(),
        1,
        "the PM event must be stored under the correlation-derived root (D-11)"
    );
    let under_cover = store.get("pm", "", cover_root).await.expect("get");
    assert_eq!(
        under_cover.len(),
        0,
        "nothing may be stored under the handler's cover.root — using it \
         would split the persist-side identity from the stamped rejection \
         route (the O7 bug)"
    );
}

/// F6/O7: the PUBLISHED cover carries the same correlation-derived root that
/// storage was keyed by. Pre-fix the publish kept the handler's cover.root,
/// so bus consumers keying by root saw a different identity than storage.
#[tokio::test]
async fn persist_publishes_cover_with_correlation_derived_root() {
    use crate::orchestration::shared::CorrelationRootExt;

    let store: Arc<dyn EventStore> = Arc::new(MockEventStore::new());
    let bus = Arc::new(MockEventBus::new());
    let bus_dyn: Arc<dyn EventBus> = bus.clone();
    let cover_root = Uuid::new_v4(); // differs from the derived root

    let book = pm_book("pm", cover_root, "friendly-flow-7", &[0]);
    persist_pm_event_book(&store, &bus_dyn, "pm", &book, "friendly-flow-7").await;

    let published = bus.take_published().await;
    assert_eq!(published.len(), 1);
    let root = published[0]
        .cover
        .as_ref()
        .and_then(|c| c.root.as_ref())
        .expect("published cover must carry a root");
    assert_eq!(
        root.value,
        "friendly-flow-7".correlation_root().as_bytes().to_vec(),
        "published cover.root must be the correlation-derived pm_root so bus \
         consumers and storage agree on the PM's identity (F6)"
    );
}

/// The `AddMeta` handed to `event_store.add` must carry (a) the in-flight
/// `correlation_id` and (b) the book cover's `ext` (the packed
/// parent-aggregate routing cover).
///
/// WHY each matters:
/// - correlation_id is how PM state is FOUND: `fetch_by_correlation` /
///   `get_by_correlation` match on the stored value. Dropping it persists
///   the rows under an empty correlation, and on the next trigger the PM
///   fetch finds nothing — it restarts a mid-flight workflow from empty.
/// - ext must survive the storage round-trip (BookParts.ext) or the
///   parent-aggregate routing cover is lost the first time state is
///   reloaded from the store instead of the bus.
///
/// Observed through the mock store's `get_by_correlation`, which only
/// returns rows whose stored correlation matches and reassembles `ext`
/// onto the book cover — so both drops are visible from the outside.
#[tokio::test]
async fn persist_add_meta_carries_correlation_id_and_ext() {
    let mock_store = Arc::new(MockEventStore::new());
    let store: Arc<dyn EventStore> = mock_store.clone();
    let bus = Arc::new(MockEventBus::new());
    let bus_dyn: Arc<dyn EventBus> = bus.clone();

    let parent_ext = Any {
        type_url: "test.ParentCover".to_string(),
        value: vec![7, 7, 7],
    };
    let mut book = pm_book("pm", Uuid::new_v4(), "corr-meta", &[0]);
    book.cover.as_mut().unwrap().ext = Some(parent_ext.clone());

    let outcome = persist_pm_event_book(&store, &bus_dyn, "pm", &book, "corr-meta").await;
    assert!(matches!(outcome, CommandOutcome::Success(_)));

    let books = mock_store
        .get_by_correlation("corr-meta")
        .await
        .expect("correlation lookup succeeds");
    assert_eq!(
        books.len(),
        1,
        "the persisted PM state must be findable by its workflow correlation_id — \
         an empty stored correlation makes the workflow invisible to the PM fetch"
    );
    assert_eq!(
        books[0].cover.as_ref().and_then(|c| c.ext.as_ref()),
        Some(&parent_ext),
        "the parent-aggregate routing cover (ext) must survive the storage round-trip"
    );
}

/// Sanity contract: `event_store.add` failure surfaces as
/// `Rejected{Internal}` and the bus sees no publish.
#[tokio::test]
async fn persist_returns_rejected_internal_when_store_add_fails() {
    let mock_store = Arc::new(MockEventStore::new());
    mock_store.set_fail_on_add(true).await;
    let store: Arc<dyn EventStore> = mock_store.clone();
    let bus = Arc::new(MockEventBus::new());
    let bus_dyn: Arc<dyn EventBus> = bus.clone();
    let pm_root = Uuid::new_v4();

    let book = pm_book("pm", pm_root, "corr", &[0]);
    let outcome = persist_pm_event_book(&store, &bus_dyn, "pm", &book, "corr").await;
    match outcome {
        CommandOutcome::Rejected { code, .. } => assert_eq!(code, tonic::Code::Internal),
        other => panic!("expected Rejected, got {other:?}"),
    }
    assert_eq!(
        bus.take_published().await.len(),
        0,
        "no publish must occur when storage rejects the add"
    );
}
