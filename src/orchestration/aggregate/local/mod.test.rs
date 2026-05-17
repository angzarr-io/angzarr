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

// ========================================================================
// C-04: Idempotency-republish must preserve correlation_id
//
// When a saga-deferred command redelivers (AMQP at-least-once) and the
// pipeline finds the prior dispatch via `check_deferred_idempotency`, it
// re-publishes the cached events to recover from a *prior* bus failure
// (the first attempt persisted but the bus publish itself failed). Process
// managers filter events by correlation_id and never fire if the bus
// message arrives with correlation_id="". The cached EventBook returned
// by `build_event_book` hardcodes `correlation_id: String::new()` —
// republish therefore strips the correlation_id, and PMs miss the
// redelivery. The fix must stamp the in-flight command's correlation_id
// onto the rebuilt EventBook's cover before `post_persist`.
// ========================================================================

use crate::orchestration::aggregate::{execute_command_pipeline, PipelineMode};
use crate::proto::{command_page, page_header, CommandBook, CommandPage, EventPage, MergeStrategy};

fn deferred_command_book(
    target_domain: &str,
    target_root: Uuid,
    correlation_id: &str,
    source_domain: &str,
    source_root: Uuid,
    source_seq: u32,
) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: target_domain.to_string(),
            root: Some(ProtoUuid {
                value: target_root.as_bytes().to_vec(),
            }),
            correlation_id: correlation_id.to_string(),
            edition: None,
        }),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::AngzarrDeferred(deferred(
                    source_domain,
                    source_root,
                    source_seq,
                ))),
            }),
            payload: Some(command_page::Payload::Command(prost_types::Any {
                type_url: "test.Command".to_string(),
                value: vec![],
            })),
            merge_strategy: MergeStrategy::MergeAggregateHandles as i32,
        }],
    }
}

/// C-04 reproducer: a deferred command redelivery must re-publish the
/// cached events with the original correlation_id intact, not an empty
/// string. Currently `build_event_book` hardcodes `correlation_id:
/// String::new()`, defeating the "republish recovers from prior bus
/// failure" semantic because PMs filter by correlation_id and skip
/// events without one.
#[tokio::test]
async fn test_idempotent_republish_preserves_correlation_id() {
    let storage = create_test_storage();
    let event_store = storage.event_store.clone();
    let event_bus = Arc::new(MockEventBus::new());
    let ctx = LocalAggregateContext::without_discovery(storage, event_bus.clone());

    let target_root = Uuid::new_v4();
    let source_root = Uuid::new_v4();
    let correlation_id = "corr-X-cross-domain";
    let source_info = crate::storage::SourceInfo::new("", "table", source_root, 5);

    // Seed: the prior dispatch persisted an event tagged with the source
    // provenance. The first dispatch's correlation_id was "corr-X" — but
    // the bug we're reproducing surfaces regardless of what was stored,
    // because the rebuilt EventBook ignores stored cover entirely and
    // hardcodes "".
    let prior_event = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(0)),
        }),
        ..Default::default()
    };
    event_store
        .add(
            "hand",
            "",
            target_root,
            vec![prior_event],
            correlation_id,
            None,
            Some(&source_info),
        )
        .await
        .expect("seed prior dispatch");

    // Redelivery: same source provenance, same correlation_id.
    let command =
        deferred_command_book("hand", target_root, correlation_id, "table", source_root, 5);

    let business = MockClientLogic::new(0);
    let response = execute_command_pipeline(&ctx, &business, command, PipelineMode::Execute)
        .await
        .expect("redelivery should succeed via idempotency hit");

    // Assert: the republished events on the bus carry the original
    // correlation_id, not an empty string. Without the C-04 fix, this
    // assertion fails — `build_event_book` returns a cover with
    // `correlation_id: ""`, and `post_persist` publishes that book
    // verbatim to the bus.
    let published = event_bus.take_published().await;
    assert_eq!(
        published.len(),
        1,
        "idempotent redelivery must republish exactly one EventBook to recover from a prior bus failure"
    );
    let published_cover = published[0]
        .cover
        .as_ref()
        .expect("republished book carries a cover");
    assert_eq!(
        published_cover.correlation_id, correlation_id,
        "C-04: republished EventBook must preserve the in-flight command's correlation_id so PMs fire on redelivery; got empty string means the bug is present"
    );

    // The CommandResponse to the caller must also carry the
    // correlation_id so downstream framework code that inspects the
    // returned events (e.g., for tracing) sees it correctly.
    let response_cover = response
        .events
        .as_ref()
        .and_then(|e| e.cover.as_ref())
        .expect("response carries cover");
    assert_eq!(
        response_cover.correlation_id, correlation_id,
        "CommandResponse.events cover must also carry the correlation_id"
    );
}

// ========================================================================
// C-03: Cascade-conflict gate must observe command-produced events
//
// `check_cascade_conflict` at pipeline.rs:278 is invoked with an empty
// `command_events` *before* the command runs. merge.rs:281 then computes
// `command_fields = diff_state_fields(state_all, replay(prior + empty))
//                = diff_state_fields(state_all, state_all)
//                = {}`,
// so the gate's overlap check is always empty and (unless
// `locked_fields == {"*"}`) it returns `NoConflict`. Uncommitted-cascade
// field collisions slip through silently.
//
// The fix moves the gate to *after* `business.invoke` so `command_events`
// reflects the actual events the command produced.
// ========================================================================

use crate::proto::business_response::Result as BrResult;
use crate::proto::{event_page, BusinessResponse, SyncMode};

/// Test ClientLogic that:
/// - On `replay`, sums "balance" deltas from all events (encoded as the
///   page payload value byte 0) and returns a `test.StatefulState`
///   JSON-like blob.
/// - On `invoke`, returns one event whose payload's first byte is the
///   command's first page payload's first byte (treated as a "delta" to
///   balance), wrapped in an EventBook with the correct cover.
///
/// State diff uses `merge_test_support::diff_test_state_fields` (matched
/// by the `test.StatefulState` type_url branch in `diff_state_fields`).
struct BalanceClientLogic;

#[async_trait]
impl ClientLogic for BalanceClientLogic {
    async fn invoke(&self, cmd: ContextualCommand) -> Result<BusinessResponse, Status> {
        // Compute next sequence
        let prior = cmd.events.as_ref().expect("prior events present");
        let next_seq = prior
            .pages
            .iter()
            .map(|p| {
                use crate::proto_ext::EventPageExt;
                p.sequence_num()
            })
            .max()
            .map(|m| m + 1)
            .unwrap_or(0);

        // Extract the delta from the command's first page payload
        let delta = cmd
            .command
            .as_ref()
            .and_then(|c| c.pages.first())
            .and_then(|p| p.payload.as_ref())
            .and_then(|payload| match payload {
                command_page::Payload::Command(any) => {
                    Some(any.value.first().copied().unwrap_or(0))
                }
                _ => None,
            })
            .unwrap_or(0);

        let event = EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(next_seq)),
            }),
            payload: Some(event_page::Payload::Event(prost_types::Any {
                type_url: "test.BalanceDelta".to_string(),
                value: vec![delta],
            })),
            created_at: None,
            ..Default::default()
        };

        let book = EventBook {
            cover: prior.cover.clone(),
            pages: vec![event],
            snapshot: None,
            ..Default::default()
        };

        Ok(BusinessResponse {
            result: Some(BrResult::Events(book)),
        })
    }

    async fn invoke_fact(
        &self,
        ctx: crate::orchestration::aggregate::FactContext,
    ) -> Result<EventBook, Status> {
        Ok(ctx.facts)
    }

    async fn replay(&self, events: &EventBook) -> Result<prost_types::Any, Status> {
        let balance: u32 = events
            .pages
            .iter()
            .filter_map(|p| match p.payload.as_ref() {
                Some(event_page::Payload::Event(any)) if any.type_url == "test.BalanceDelta" => {
                    Some(any.value.first().copied().unwrap_or(0) as u32)
                }
                _ => None,
            })
            .sum();
        let json = format!("{{\"balance\":{}}}", balance);
        Ok(prost_types::Any {
            type_url: "test.StatefulState".to_string(),
            value: json.into_bytes(),
        })
    }
}

fn balance_command_book(
    target_domain: &str,
    target_root: Uuid,
    expected_seq: u32,
    delta: u8,
) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: target_domain.to_string(),
            root: Some(ProtoUuid {
                value: target_root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
        }),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(expected_seq)),
            }),
            payload: Some(command_page::Payload::Command(prost_types::Any {
                type_url: "test.BalanceCmd".to_string(),
                value: vec![delta],
            })),
            merge_strategy: MergeStrategy::MergeAggregateHandles as i32,
        }],
    }
}

/// C-03 reproducer: an uncommitted cascade event has locked the `balance`
/// field. A new command in a *different* cascade context that also
/// touches `balance` must be rejected by the cascade-conflict gate.
/// Currently the gate is a no-op (it computes overlap against an empty
/// command_events book), so the command succeeds and produces a second
/// event also touching balance — uncommitted-cascade field collisions
/// slip through silently.
#[tokio::test]
async fn test_cascade_conflict_gate_rejects_uncommitted_field_collision() {
    let storage = create_test_storage();
    let event_store = storage.event_store.clone();
    let target_root = Uuid::new_v4();

    // Seed: cascade-A is mid-flight and has written an uncommitted event
    // that changes the `balance` field (delta=10).
    let uncommitted = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(0)),
        }),
        payload: Some(event_page::Payload::Event(prost_types::Any {
            type_url: "test.BalanceDelta".to_string(),
            value: vec![10],
        })),
        created_at: None,
        no_commit: true,
        cascade_id: Some("cascade-A".to_string()),
    };
    event_store
        .add(
            "wallet",
            "",
            target_root,
            vec![uncommitted],
            "corr-A",
            None,
            None,
        )
        .await
        .expect("seed cascade-A uncommitted event");

    // Run a command in cascade-B context. The command also touches
    // balance (delta=5). The cascade-conflict gate must reject it
    // because cascade-A has locked the balance field.
    let ctx = LocalAggregateContext::without_discovery(storage, Arc::new(MockEventBus::new()))
        .with_sync_mode(SyncMode::Cascade)
        .with_cascade_id("cascade-B");

    let command = balance_command_book("wallet", target_root, 1, 5);
    let business = BalanceClientLogic;
    let result = execute_command_pipeline(&ctx, &business, command, PipelineMode::Execute).await;

    // Assert: pipeline must reject with a "Cascade conflict" error
    // because cascade-A has the balance field locked.
    let err = result.expect_err(
        "C-03: command in cascade-B that touches balance must be rejected; cascade-A has it locked. \
         Bug present: pipeline returned Ok, meaning the no-op gate let the colliding command through."
    );
    assert_eq!(
        err.code(),
        tonic::Code::Aborted,
        "cascade-conflict should be reported as Aborted; got {:?}: {}",
        err.code(),
        err.message()
    );
    assert!(
        err.message().contains("Cascade conflict"),
        "error should reference cascade conflict; got: {}",
        err.message()
    );
}

/// Regression guard: when no uncommitted cascade events exist, the gate
/// must NOT reject the command. This is the negative case — ensures the
/// gate's overlap check actually considers events, rather than always
/// rejecting whenever the cascade context is set.
#[tokio::test]
async fn test_cascade_conflict_gate_allows_when_no_uncommitted() {
    let storage = create_test_storage();
    let event_store = storage.event_store.clone();
    let target_root = Uuid::new_v4();

    // Seed: a *committed* event that changes balance (delta=10). No
    // uncommitted events exist, so the gate must not block.
    let committed = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(0)),
        }),
        payload: Some(event_page::Payload::Event(prost_types::Any {
            type_url: "test.BalanceDelta".to_string(),
            value: vec![10],
        })),
        created_at: None,
        no_commit: false,
        cascade_id: None,
    };
    event_store
        .add(
            "wallet",
            "",
            target_root,
            vec![committed],
            "corr-committed",
            None,
            None,
        )
        .await
        .expect("seed committed event");

    let ctx = LocalAggregateContext::without_discovery(storage, Arc::new(MockEventBus::new()))
        .with_sync_mode(SyncMode::Cascade)
        .with_cascade_id("cascade-B");

    let command = balance_command_book("wallet", target_root, 1, 5);
    let business = BalanceClientLogic;
    let result = execute_command_pipeline(&ctx, &business, command, PipelineMode::Execute).await;

    assert!(
        result.is_ok(),
        "no uncommitted events means no cascade conflict — command must succeed; got {:?}",
        result.err()
    );
}

/// Regression guard: an uncommitted cascade event that locks a
/// *different* field than the new command must NOT cause rejection.
/// This isolates the gate's field-overlap logic from the "any
/// uncommitted event blocks everything" failure mode.
///
/// NOTE on field-level disjointness: the framework's
/// `diff_state_fields` only achieves field-level granularity through
/// the `test.StatefulState` JSON path (via `merge_test_support`). When
/// the state representation is opaque to the diff (different `type_url`
/// or no test-state recognition), the diff returns "*" (wildcard) and
/// the gate must conservatively reject. So this test uses
/// BalanceClientLogic, whose replay emits `test.StatefulState`, but
/// distinguishes fields by emitting a state JSON that varies per
/// command — see the "limit" variant below.
///
/// We construct disjointness by having two test-state aggregates that
/// only ever set one field each: cascade-A locks `field_a` (a balance
/// computed from `test.FieldADelta` events), the command produces
/// `field_b` (from `test.FieldBDelta` events). With distinct field
/// names in the JSON, the diff sees them as disjoint.
#[tokio::test]
async fn test_cascade_conflict_gate_allows_disjoint_field_changes() {
    struct DisjointBalanceClientLogic;

    #[async_trait]
    impl ClientLogic for DisjointBalanceClientLogic {
        async fn invoke(&self, cmd: ContextualCommand) -> Result<BusinessResponse, Status> {
            let prior = cmd.events.as_ref().expect("prior events present");
            let next_seq = prior
                .pages
                .iter()
                .map(|p| {
                    use crate::proto_ext::EventPageExt;
                    p.sequence_num()
                })
                .max()
                .map(|m| m + 1)
                .unwrap_or(0);

            let delta = cmd
                .command
                .as_ref()
                .and_then(|c| c.pages.first())
                .and_then(|p| p.payload.as_ref())
                .and_then(|payload| match payload {
                    command_page::Payload::Command(any) => {
                        Some(any.value.first().copied().unwrap_or(0))
                    }
                    _ => None,
                })
                .unwrap_or(0);

            let event = EventPage {
                header: Some(PageHeader {
                    sync_mode: None,
                    sequence_type: Some(page_header::SequenceType::Sequence(next_seq)),
                }),
                payload: Some(event_page::Payload::Event(prost_types::Any {
                    type_url: "test.FieldBDelta".to_string(),
                    value: vec![delta],
                })),
                created_at: None,
                ..Default::default()
            };

            let book = EventBook {
                cover: prior.cover.clone(),
                pages: vec![event],
                snapshot: None,
                ..Default::default()
            };

            Ok(BusinessResponse {
                result: Some(BrResult::Events(book)),
            })
        }

        async fn invoke_fact(
            &self,
            ctx: crate::orchestration::aggregate::FactContext,
        ) -> Result<EventBook, Status> {
            Ok(ctx.facts)
        }

        async fn replay(&self, events: &EventBook) -> Result<prost_types::Any, Status> {
            // Sum field_a deltas separately from field_b deltas.
            let mut field_a: u32 = 0;
            let mut field_b: u32 = 0;
            for p in &events.pages {
                if let Some(event_page::Payload::Event(any)) = p.payload.as_ref() {
                    match any.type_url.as_str() {
                        "test.FieldADelta" => {
                            field_a += any.value.first().copied().unwrap_or(0) as u32;
                        }
                        "test.FieldBDelta" => {
                            field_b += any.value.first().copied().unwrap_or(0) as u32;
                        }
                        _ => {}
                    }
                }
            }
            let json = format!("{{\"field_a\":{},\"field_b\":{}}}", field_a, field_b);
            Ok(prost_types::Any {
                type_url: "test.StatefulState".to_string(),
                value: json.into_bytes(),
            })
        }
    }

    let storage = create_test_storage();
    let event_store = storage.event_store.clone();
    let target_root = Uuid::new_v4();

    // Seed: cascade-A locks field_a.
    let uncommitted = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(0)),
        }),
        payload: Some(event_page::Payload::Event(prost_types::Any {
            type_url: "test.FieldADelta".to_string(),
            value: vec![10],
        })),
        created_at: None,
        no_commit: true,
        cascade_id: Some("cascade-A".to_string()),
    };
    event_store
        .add(
            "wallet",
            "",
            target_root,
            vec![uncommitted],
            "corr-A",
            None,
            None,
        )
        .await
        .expect("seed cascade-A uncommitted event");

    // Command produces field_b only — must not collide with cascade-A.
    let ctx = LocalAggregateContext::without_discovery(storage, Arc::new(MockEventBus::new()))
        .with_sync_mode(SyncMode::Cascade)
        .with_cascade_id("cascade-B");

    let command = balance_command_book("wallet", target_root, 1, 5);
    let business = DisjointBalanceClientLogic;
    let result = execute_command_pipeline(&ctx, &business, command, PipelineMode::Execute).await;

    assert!(
        result.is_ok(),
        "disjoint field changes must not be blocked by cascade gate; got {:?}",
        result.err()
    );
}

// ========================================================================
// C-05: Local post_persist must honor SyncMode::Isolated
//
// The gRPC sibling (`aggregate/grpc/mod.rs:575`) short-circuits the entire
// `post_persist` callback when `sync_mode == Isolated` — neither the bus
// publish nor the sync-projector dispatch fires. The local context's
// `post_persist` predates the Isolated mode: it only special-cases `Async`
// (skip projectors) and otherwise always publishes plus calls projectors.
// That means a `LocalAggregateContext` used for recovery / migration /
// replay writes — exactly the workload Isolated was added to support —
// leaks events to the bus and triggers sync projectors as if they were
// fresh business events.
//
// These tests pin the contract: Isolated → no publish; Simple → publish.
// The fix delegates the policy decision to the shared `sync_policy` module
// (C-06 declares it), keeping both call sites from drifting.
// ========================================================================

/// Builds a minimal EventBook the post_persist callback will receive after
/// a successful persist. The pages list is non-empty so the MockEventBus
/// can distinguish "we published something" from "no-op event book."
fn isolated_test_event_book(domain: &str, root: Uuid) -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: "corr-isolated".to_string(),
            edition: None,
        }),
        pages: vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(0)),
            }),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// C-05 reproducer: `SyncMode::Isolated` must skip the bus publish.
///
/// Today `post_persist` in `local/mod.rs:486` only checks for `Async`; any
/// other mode (including `Isolated`) goes down the publish path. With the
/// fix, the shared `should_skip_post_persist`/`sync_policy` predicate
/// causes the entire callback to short-circuit with `Ok(vec![])`.
#[tokio::test]
async fn test_post_persist_isolated_skips_bus_publish() {
    let storage = create_test_storage();
    let event_bus = Arc::new(MockEventBus::new());
    let ctx = LocalAggregateContext::without_discovery(storage, event_bus.clone())
        .with_sync_mode(SyncMode::Isolated);

    let book = isolated_test_event_book("hand", Uuid::new_v4());

    let projections = ctx
        .post_persist(&book)
        .await
        .expect("Isolated post_persist must succeed");

    assert!(
        projections.is_empty(),
        "Isolated mode must return no projections (sync projectors skipped); got {} projections",
        projections.len()
    );

    let published = event_bus.take_published().await;
    assert!(
        published.is_empty(),
        "C-05: Isolated mode must NOT publish to bus during post_persist (recovery / migration / replay writes must not leak to the bus); got {} published EventBook(s)",
        published.len()
    );
}

/// Regression guard: `SyncMode::Simple` must still publish, so the fix
/// can't silently break the common path. The MockEventBus captures one
/// EventBook per successful publish.
#[tokio::test]
async fn test_post_persist_simple_still_publishes() {
    let storage = create_test_storage();
    let event_bus = Arc::new(MockEventBus::new());
    let ctx = LocalAggregateContext::without_discovery(storage, event_bus.clone())
        .with_sync_mode(SyncMode::Simple);

    let book = isolated_test_event_book("hand", Uuid::new_v4());

    ctx.post_persist(&book)
        .await
        .expect("Simple post_persist must succeed");

    let published = event_bus.take_published().await;
    assert_eq!(
        published.len(),
        1,
        "Simple mode must publish exactly one EventBook to the bus (regression guard against an overly aggressive C-05 fix that skips the common path)"
    );
    let published_cover = published[0]
        .cover
        .as_ref()
        .expect("published book carries cover");
    assert_eq!(published_cover.correlation_id, "corr-isolated");
}

/// Regression guard: `SyncMode::Async` must still publish (fire-and-forget
/// still requires the bus delivery; what it skips is the sync-projector
/// wait). Pre-fix behavior preserved: Async published, Async skipped
/// projectors. Post-fix it's the same.
#[tokio::test]
async fn test_post_persist_async_still_publishes() {
    let storage = create_test_storage();
    let event_bus = Arc::new(MockEventBus::new());
    let ctx = LocalAggregateContext::without_discovery(storage, event_bus.clone())
        .with_sync_mode(SyncMode::Async);

    let book = isolated_test_event_book("hand", Uuid::new_v4());

    let projections = ctx
        .post_persist(&book)
        .await
        .expect("Async post_persist must succeed");

    assert!(
        projections.is_empty(),
        "Async mode must skip sync projectors; got {} projections",
        projections.len()
    );
    let published = event_bus.take_published().await;
    assert_eq!(
        published.len(),
        1,
        "Async mode publishes but does not wait for projectors; bus must still receive the EventBook"
    );
}
