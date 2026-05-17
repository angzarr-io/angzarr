//! Tests for LocalAggregateContext and LocalAggregateContextFactory.
//!
//! The local aggregate context uses in-process SQLite storage with optional
//! service discovery for sync projectors. Key behaviors tested:
//! - Factory domain and client_logic accessors
//! - Context builder pattern (with_* methods)
//! - Helper functions: extract_sequence, build_event_book

use super::*;
use crate::bus::mock::MockEventBus;
use crate::discovery::StaticServiceDiscovery;
use crate::proto::{ContextualCommand, PageHeader};
use crate::storage::mock::{MockEventStore, MockSnapshotStore};
use crate::storage::DomainStorage;

// ========================================================================
// Mock ClientLogic for testing
// ========================================================================

struct MockClientLogic;

impl MockClientLogic {
    fn new(_id: usize) -> Self {
        Self
    }
}

#[async_trait]
impl ClientLogic for MockClientLogic {
    async fn invoke(
        &self,
        _cmd: ContextualCommand,
    ) -> Result<crate::proto::BusinessResponse, Status> {
        use crate::proto::business_response::Result as BrResult;
        Ok(crate::proto::BusinessResponse {
            result: Some(BrResult::Events(EventBook::default())),
        })
    }

    async fn invoke_fact(
        &self,
        ctx: crate::orchestration::aggregate::FactContext,
    ) -> Result<EventBook, Status> {
        Ok(ctx.facts)
    }
}

fn create_test_storage() -> DomainStorage {
    DomainStorage {
        event_store: Arc::new(MockEventStore::new()),
        snapshot_store: Arc::new(MockSnapshotStore::new()),
    }
}

fn create_test_factory(domain: &str, client_id: usize) -> LocalAggregateContextFactory {
    LocalAggregateContextFactory::new(
        domain.to_string(),
        create_test_storage(),
        Arc::new(StaticServiceDiscovery::new()),
        Arc::new(MockEventBus::new()),
        Arc::new(MockClientLogic::new(client_id)),
    )
}

// ========================================================================
// LocalAggregateContextFactory Tests
// ========================================================================

#[test]
fn test_factory_domain_returns_configured_domain() {
    let factory = create_test_factory("orders", 1);
    assert_eq!(factory.domain(), "orders");
}

#[test]
fn test_factory_domain_returns_different_domains() {
    let factory1 = create_test_factory("orders", 1);
    let factory2 = create_test_factory("inventory", 2);
    assert_eq!(factory1.domain(), "orders");
    assert_eq!(factory2.domain(), "inventory");
    assert_ne!(factory1.domain(), factory2.domain());
}

#[test]
fn test_factory_client_logic_returns_arc() {
    let factory = create_test_factory("orders", 42);
    let logic = factory.client_logic();
    // Verify we can clone the Arc (it's a shared reference)
    let _logic2 = logic.clone();
}

#[test]
fn test_factory_create_returns_context() {
    let factory = create_test_factory("orders", 1);
    let context = factory.create();
    // Verify context is created - we can't directly inspect it but we can ensure it's valid
    let _context2 = context;
}

#[test]
fn test_factory_with_dlq_publisher_returns_self() {
    let factory = create_test_factory("orders", 1);
    let updated = factory.with_dlq_publisher(Arc::new(NoopDeadLetterPublisher));
    // Verify domain is still correct
    assert_eq!(updated.domain(), "orders");
}

// ========================================================================
// LocalAggregateContext Builder Tests
// ========================================================================

#[test]
fn test_context_new_sets_defaults() {
    let storage = create_test_storage();
    let discovery = Arc::new(StaticServiceDiscovery::new());
    let bus = Arc::new(MockEventBus::new());

    let ctx = LocalAggregateContext::new(storage, discovery, bus);

    // Verify snapshot write is enabled by default
    assert!(ctx.snapshot_write_enabled);
}

#[test]
fn test_context_without_discovery() {
    let storage = create_test_storage();
    let bus = Arc::new(MockEventBus::new());

    let ctx = LocalAggregateContext::without_discovery(storage, bus);

    // Verify discovery is None
    assert!(ctx.discovery.is_none());
}

#[test]
fn test_context_with_snapshot_write_disabled() {
    let storage = create_test_storage();
    let discovery = Arc::new(StaticServiceDiscovery::new());
    let bus = Arc::new(MockEventBus::new());

    let ctx = LocalAggregateContext::new(storage, discovery, bus).with_snapshot_write_disabled();

    assert!(!ctx.snapshot_write_enabled);
}

#[test]
fn test_context_with_component_name() {
    let storage = create_test_storage();
    let discovery = Arc::new(StaticServiceDiscovery::new());
    let bus = Arc::new(MockEventBus::new());

    let ctx =
        LocalAggregateContext::new(storage, discovery, bus).with_component_name("my-aggregate");

    assert_eq!(ctx.component_name, "my-aggregate");
}

#[test]
fn test_context_with_sync_mode() {
    let storage = create_test_storage();
    let discovery = Arc::new(StaticServiceDiscovery::new());
    let bus = Arc::new(MockEventBus::new());

    let ctx = LocalAggregateContext::new(storage, discovery, bus)
        .with_sync_mode(crate::proto::SyncMode::Cascade);

    assert_eq!(ctx.sync_mode, Some(crate::proto::SyncMode::Cascade));
}

// ========================================================================
// Helper function tests
// ========================================================================

#[test]
fn test_extract_sequence_from_some() {
    let page = crate::proto::EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(5)),
        }),
        payload: None,
        created_at: None,
        ..Default::default()
    };
    assert_eq!(extract_sequence(Some(&page)), 5);
}

#[test]
fn test_extract_sequence_from_none() {
    assert_eq!(extract_sequence(None), 0);
}

#[test]
fn test_build_event_book_sets_cover() {
    let root = Uuid::new_v4();
    let book = build_event_book("orders", "angzarr", root, vec![], None);

    let cover = book.cover.as_ref().unwrap();
    assert_eq!(cover.domain, "orders");
    assert_eq!(cover.edition.as_ref().unwrap().name, "angzarr");
}

#[test]
fn test_build_event_book_with_pages() {
    let root = Uuid::new_v4();
    let pages = vec![
        crate::proto::EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
            }),
            payload: None,
            created_at: None,
            ..Default::default()
        },
        crate::proto::EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(1)),
            }),
            payload: None,
            created_at: None,
            ..Default::default()
        },
    ];
    let book = build_event_book("orders", "angzarr", root, pages, None);

    assert_eq!(book.pages.len(), 2);
}

// ========================================================================
// check_deferred_idempotency Tests
//
// AMQP redelivery of a saga's trigger event causes the saga to redispatch
// the same logical command. The pipeline calls
// `ctx.check_deferred_idempotency` first; on a redelivery it must return
// the cached events from the prior successful dispatch so the destination
// aggregate's business handler is never invoked twice. The default trait
// impl returns Ok(None) (no idempotency); the LocalAggregateContext
// override consults the storage layer's `find_by_source` lookup.
// ========================================================================

fn deferred(source_domain: &str, source_root: Uuid, source_seq: u32) -> AngzarrDeferredSequence {
    AngzarrDeferredSequence {
        source: Some(Cover {
            domain: source_domain.to_string(),
            root: Some(ProtoUuid {
                value: source_root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
        }),
        source_seq,
    }
}

#[tokio::test]
async fn test_check_deferred_idempotency_returns_none_when_no_prior_dispatch() {
    let ctx = LocalAggregateContext::without_discovery(
        create_test_storage(),
        Arc::new(MockEventBus::new()),
    );
    let target_root = Uuid::new_v4();
    let source_root = Uuid::new_v4();
    let result = ctx
        .check_deferred_idempotency("hand", "", target_root, &deferred("table", source_root, 5))
        .await;
    assert!(matches!(result, Ok(None)));
}

#[tokio::test]
async fn test_check_deferred_idempotency_returns_cached_events_on_redelivery() {
    // Setup: persist an event at the target aggregate that carries source
    // provenance from a saga trigger. A subsequent check_deferred_idempotency
    // call with the same provenance must return that cached event.
    let storage = create_test_storage();
    let event_store = storage.event_store.clone();
    let ctx = LocalAggregateContext::without_discovery(storage, Arc::new(MockEventBus::new()));

    let target_root = Uuid::new_v4();
    let source_root = Uuid::new_v4();
    let source_info = crate::storage::SourceInfo::new("", "table", source_root, 5);

    let event = crate::proto::EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        ..Default::default()
    };
    event_store
        .add(
            "hand",
            "",
            target_root,
            vec![event],
            "corr-1",
            None,
            Some(&source_info),
        )
        .await
        .expect("seed event");

    let cached = ctx
        .check_deferred_idempotency("hand", "", target_root, &deferred("table", source_root, 5))
        .await
        .expect("idempotency lookup");

    let book = cached.expect("redelivery should hit the cached prior dispatch");
    assert_eq!(
        book.pages.len(),
        1,
        "exactly the prior event should be returned"
    );
    let cover = book.cover.as_ref().expect("event book carries cover");
    assert_eq!(cover.domain, "hand");
}

#[tokio::test]
async fn test_persist_events_propagates_source_info_for_deferred_commands() {
    // When the pipeline persists events produced by a saga-deferred command,
    // the destination aggregate's events must be tagged with the source
    // provenance. Without this, a subsequent redelivery's
    // `check_deferred_idempotency` lookup finds nothing and the
    // handler is invoked redundantly.
    let storage = create_test_storage();
    let event_store = storage.event_store.clone();
    let ctx = LocalAggregateContext::without_discovery(storage, Arc::new(MockEventBus::new()));

    let target_root = Uuid::new_v4();
    let source_root = Uuid::new_v4();
    let source_info = crate::storage::SourceInfo::new("", "table", source_root, 5);

    let prior = build_event_book("hand", "", target_root, vec![], None);
    let received_pages = vec![EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        ..Default::default()
    }];
    let received = build_event_book("hand", "", target_root, received_pages, None);

    let outcome = ctx
        .persist_events(
            &prior,
            &received,
            "hand",
            "",
            target_root,
            "corr-1",
            None,
            Some(&source_info),
        )
        .await
        .expect("persist should succeed");
    assert!(matches!(outcome, PersistOutcome::Persisted(_)));

    // Round-trip: the freshly persisted event must be discoverable by
    // its source provenance, otherwise the idempotency check on
    // redelivery wouldn't find it.
    let cached = event_store
        .find_by_source("hand", "", target_root, &source_info)
        .await
        .expect("find_by_source");
    let pages = cached.expect("source_info should propagate from persist into the store");
    assert_eq!(pages.len(), 1);
}

// ========================================================================
// check_external_idempotency Tests
//
// External webhook delivery is at-least-once: a Stripe retry of the same
// payment_intent (external_id) should not re-invoke the fact handler.
// Storage-level external_id dedup at persist already prevents
// double-write, but pre-handler dedup is symmetric with the saga path
// and avoids redundant business invocation.
// ========================================================================

#[tokio::test]
async fn test_check_external_idempotency_returns_none_when_no_prior_fact() {
    let ctx = LocalAggregateContext::without_discovery(
        create_test_storage(),
        Arc::new(MockEventBus::new()),
    );
    let target_root = Uuid::new_v4();
    let result = ctx
        .check_external_idempotency("player", "", target_root, "stripe-pi-1")
        .await;
    assert!(matches!(result, Ok(None)));
}

#[tokio::test]
async fn test_check_external_idempotency_returns_cached_events_on_redelivery() {
    let storage = create_test_storage();
    let event_store = storage.event_store.clone();
    let ctx = LocalAggregateContext::without_discovery(storage, Arc::new(MockEventBus::new()));

    let target_root = Uuid::new_v4();
    let event = crate::proto::EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        ..Default::default()
    };
    event_store
        .add(
            "player",
            "",
            target_root,
            vec![event],
            "corr-1",
            Some("stripe-pi-1"),
            None,
        )
        .await
        .expect("seed event");

    let cached = ctx
        .check_external_idempotency("player", "", target_root, "stripe-pi-1")
        .await
        .expect("idempotency lookup");

    let book = cached.expect("redelivery should hit the cached prior fact");
    assert_eq!(book.pages.len(), 1);
}

#[tokio::test]
async fn test_check_external_idempotency_distinguishes_external_id() {
    let storage = create_test_storage();
    let event_store = storage.event_store.clone();
    let ctx = LocalAggregateContext::without_discovery(storage, Arc::new(MockEventBus::new()));

    let target_root = Uuid::new_v4();
    let event = crate::proto::EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        ..Default::default()
    };
    event_store
        .add(
            "player",
            "",
            target_root,
            vec![event],
            "corr-1",
            Some("stripe-pi-1"),
            None,
        )
        .await
        .expect("seed event");

    let result = ctx
        .check_external_idempotency("player", "", target_root, "stripe-pi-2")
        .await
        .expect("idempotency lookup");
    assert!(
        result.is_none(),
        "different external_id must not collide with prior fact"
    );
}

#[tokio::test]
async fn test_check_external_idempotency_returns_none_for_empty_external_id() {
    // Empty external_id means "non-idempotent fact" — the storage layer
    // never records empty strings for dedup, so the lookup must short-
    // circuit to None and let the handler run normally.
    let ctx = LocalAggregateContext::without_discovery(
        create_test_storage(),
        Arc::new(MockEventBus::new()),
    );
    let target_root = Uuid::new_v4();
    let result = ctx
        .check_external_idempotency("player", "", target_root, "")
        .await;
    assert!(matches!(result, Ok(None)));
}

#[tokio::test]
async fn test_check_deferred_idempotency_distinguishes_source_seq() {
    // Same source.root but a different source_seq is a *different* logical
    // saga dispatch — return None so the pipeline invokes the handler.
    let storage = create_test_storage();
    let event_store = storage.event_store.clone();
    let ctx = LocalAggregateContext::without_discovery(storage, Arc::new(MockEventBus::new()));

    let target_root = Uuid::new_v4();
    let source_root = Uuid::new_v4();
    let event = crate::proto::EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        ..Default::default()
    };
    event_store
        .add(
            "hand",
            "",
            target_root,
            vec![event],
            "corr-1",
            None,
            Some(&crate::storage::SourceInfo::new(
                "",
                "table",
                source_root,
                5,
            )),
        )
        .await
        .expect("seed event");

    let result = ctx
        .check_deferred_idempotency("hand", "", target_root, &deferred("table", source_root, 6))
        .await
        .expect("idempotency lookup");
    assert!(
        result.is_none(),
        "different source_seq must not collide with prior dispatch"
    );
}
