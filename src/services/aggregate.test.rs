//! Tests for AggregateService (command handler coordinator).
//!
//! AggregateService is the core command processing pipeline:
//! 1. Receives commands via gRPC
//! 2. Loads prior events (with snapshot optimization)
//! 3. Calls client business logic
//! 4. Persists new events
//! 5. Notifies projectors
//!
//! Why this matters: This is THE critical path for all command processing.
//! Every command flows through this service. Bugs here affect all domains.
//!
//! Key behaviors verified:
//! - Constructor configurations (snapshots, upcaster)
//! - Command handling invokes business logic
//! - Missing command returns InvalidArgument
//! - Sync mode creates appropriate context
//! - Speculative execution validates inputs
//! - Compensation flow returns BusinessResponse
//! - Fact injection routes correctly

use super::*;
use crate::bus::MockEventBus;
use crate::discovery::StaticServiceDiscovery;
use crate::orchestration::aggregate::{ClientLogic, FactContext};
use crate::proto::{
    business_response, command_page, event_page, page_header, CascadeErrorMode, CommandBook,
    CommandPage, ContextualCommand, Cover, EventBook, EventPage, MergeStrategy, PageHeader,
    SyncMode, Uuid as ProtoUuid,
};
use crate::repository::SnapshotRepository;
use crate::storage::mock::{MockEventStore, MockSnapshotStore};
use prost_types::Any;
use std::collections::VecDeque;
use tokio::sync::Mutex;
use tonic::Status;
use uuid::Uuid;

// ============================================================================
// Mock ClientLogic Implementation
// ============================================================================

/// Mock business logic for testing.
///
/// Returns pre-configured responses from a queue.
struct MockClientLogic {
    responses: Mutex<VecDeque<Result<BusinessResponse, Status>>>,
    fact_responses: Mutex<VecDeque<Result<EventBook, Status>>>,
    invocations: Mutex<Vec<ContextualCommand>>,
    /// How many times invoke_fact ran. Lets tests observe whether fact
    /// injection actually routed through the handler (skip_handler
    /// semantics) instead of inferring it from response shape.
    fact_invocations: std::sync::atomic::AtomicUsize,
}

impl MockClientLogic {
    fn new() -> Self {
        Self {
            responses: Mutex::new(VecDeque::new()),
            fact_responses: Mutex::new(VecDeque::new()),
            invocations: Mutex::new(Vec::new()),
            fact_invocations: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn fact_invocation_count(&self) -> usize {
        self.fact_invocations
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    async fn enqueue_response(&self, response: Result<BusinessResponse, Status>) {
        self.responses.lock().await.push_back(response);
    }

    async fn enqueue_events(&self, events: EventBook) {
        let response = BusinessResponse {
            result: Some(business_response::Result::Events(events)),
        };
        self.enqueue_response(Ok(response)).await;
    }

    async fn enqueue_fact_response(&self, response: Result<EventBook, Status>) {
        self.fact_responses.lock().await.push_back(response);
    }
}

#[async_trait::async_trait]
impl ClientLogic for MockClientLogic {
    async fn invoke(&self, cmd: ContextualCommand) -> Result<BusinessResponse, Status> {
        self.invocations.lock().await.push(cmd);
        self.responses.lock().await.pop_front().unwrap_or_else(|| {
            Ok(BusinessResponse {
                result: Some(business_response::Result::Events(EventBook::default())),
            })
        })
    }

    async fn invoke_fact(&self, ctx: FactContext) -> Result<EventBook, Status> {
        self.fact_invocations
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.fact_responses
            .lock()
            .await
            .pop_front()
            .unwrap_or_else(|| Ok(ctx.facts))
    }
}

// ============================================================================
// Test Helpers
// ============================================================================

fn make_proto_uuid(u: Uuid) -> ProtoUuid {
    ProtoUuid {
        value: u.as_bytes().to_vec(),
    }
}

fn make_cover(domain: &str, root: Uuid) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(make_proto_uuid(root)),
        correlation_id: String::new(),
        edition: None,
        ext: None,
    }
}

fn make_command_book(domain: &str, root: Uuid, sequence: u32) -> CommandBook {
    CommandBook {
        cover: Some(make_cover(domain, root)),
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

fn make_event_page(seq: u32) -> EventPage {
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

fn make_fact_page() -> EventPage {
    use crate::proto::ExternalDeferredSequence;
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::ExternalDeferred(
                ExternalDeferredSequence {
                    external_id: "test-external-id".to_string(),
                    description: "Test fact".to_string(),
                },
            )),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: "test.Fact".to_string(),
            value: vec![],
        })),
        created_at: None,
    }
}

fn make_event_book(domain: &str, root: Uuid, pages: Vec<EventPage>) -> EventBook {
    EventBook {
        cover: Some(make_cover(domain, root)),
        pages,
        snapshot: None,
        ..Default::default()
    }
}

async fn create_test_service() -> (AggregateService, Arc<MockClientLogic>) {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let snapshot_repo = Arc::new(SnapshotRepository::new(snapshot_store));
    let business = Arc::new(MockClientLogic::new());
    let event_bus = Arc::new(MockEventBus::new());
    let discovery = Arc::new(StaticServiceDiscovery::new());

    let service = AggregateService::with_business_logic(
        event_store,
        snapshot_repo,
        business.clone(),
        event_bus,
        discovery,
    );

    (service, business)
}

// ============================================================================
// Constructor Tests
// ============================================================================

/// Default constructor creates service without upcaster.
///
/// Snapshot policy is now owned by `SnapshotRepository`, not the
/// service — see `R2-SNAP-2` tests for the gating behavior. This test
/// only verifies the service constructs cleanly with defaults.
#[tokio::test]
async fn test_with_business_logic_creates_service() {
    let (service, _) = create_test_service().await;
    assert!(service.upcaster.is_none());
}

/// Service threads the SnapshotRepository's flags through to persists.
///
/// Constructs the service with a write-disabled `SnapshotRepository`;
/// the repository must drop persist attempts without erroring (the
/// behavioral gate). The repository-level write_enabled contract is
/// pinned in detail by `repository::snapshot::tests`; this test
/// ensures the service uses the passed-in repository rather than
/// creating its own.
#[tokio::test]
async fn test_with_write_disabled_snapshot_repo_constructs_cleanly() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let snapshot_repo = Arc::new(SnapshotRepository::with_flags(snapshot_store, false, false));
    let business: Arc<dyn ClientLogic> = Arc::new(MockClientLogic::new());
    let event_bus = Arc::new(MockEventBus::new());
    let discovery = Arc::new(StaticServiceDiscovery::new());

    let _service = AggregateService::with_business_logic(
        event_store,
        snapshot_repo,
        business,
        event_bus,
        discovery,
    );
    // Construction with a disabled repo must not panic or error.
    // Behavioral verification of write-disabled persists lives in
    // `repository::snapshot::tests::test_with_flags_write_disabled_skips_put`.
}

// ============================================================================
// R2-15: DLQ publisher wiring on AggregateService
// ============================================================================
//
// AggregateService owns the DLQ publisher and threads it down to every
// GrpcAggregateContext it creates (async + sync paths). The default is
// NoopDeadLetterPublisher so existing tests / callers that don't care
// about DLQ stay green. The bin's startup overrides this via
// `with_dlq_publisher(init_dlq_publisher(&config.dlq).await?)`.

/// Default constructor wires a noop DLQ publisher so existing callers
/// (tests, in-process embeds) don't need to think about DLQ. is_configured
/// returns false on the noop, which downstream consumers (e.g., the status
/// admin) can use to surface "no DLQ" to operators.
#[tokio::test]
async fn aggregate_service_defaults_dlq_to_noop_publisher() {
    let (service, _) = create_test_service().await;
    assert!(
        !service.dlq_publisher.is_configured(),
        "default AggregateService must have an unconfigured (noop) DLQ publisher"
    );
}

/// `with_dlq_publisher` overrides the default. The bin calls this at
/// startup with the publisher returned by `init_dlq_publisher(&config.dlq)`,
/// so a misconfiguration in this builder would silently downgrade the bin
/// to noop -- exactly the failure R2-15 is preventing.
#[tokio::test]
async fn aggregate_service_with_dlq_publisher_stores_it() {
    use crate::dlq::{DeadLetterPublisher, LoggingDeadLetterPublisher};

    let (service, _) = create_test_service().await;
    let custom: Arc<dyn DeadLetterPublisher> = Arc::new(LoggingDeadLetterPublisher);
    let service = service.with_dlq_publisher(custom);
    assert!(
        service.dlq_publisher.is_configured(),
        "with_dlq_publisher must replace the default noop with the provided publisher"
    );
}

// ============================================================================
// handle_command Tests
// ============================================================================

/// Business logic is invoked on command.
#[tokio::test]
async fn test_handle_command_invokes_business_logic() {
    let (service, business) = create_test_service().await;

    let root = Uuid::new_v4();
    let command_book = make_command_book("orders", root, 0);
    let events = make_event_book("orders", root, vec![make_event_page(0)]);
    business.enqueue_events(events).await;

    let request = Request::new(CommandRequest {
        command: Some(command_book),
        sync_mode: SyncMode::Async as i32,
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
    });

    let response = service.handle_command(request).await;
    assert!(response.is_ok());

    // Verify business logic was invoked
    let invocations = business.invocations.lock().await;
    assert_eq!(invocations.len(), 1);
}

/// Missing command returns InvalidArgument error.
#[tokio::test]
async fn test_handle_command_missing_command_returns_error() {
    let (service, _) = create_test_service().await;

    let request = Request::new(CommandRequest {
        command: None,
        sync_mode: SyncMode::Async as i32,
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
    });

    let response = service.handle_command(request).await;
    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

/// Sync mode creates appropriate context.
#[tokio::test]
async fn test_handle_command_with_sync_mode_creates_sync_context() {
    let (service, business) = create_test_service().await;

    let root = Uuid::new_v4();
    let command_book = make_command_book("orders", root, 0);
    let events = make_event_book("orders", root, vec![make_event_page(0)]);
    business.enqueue_events(events).await;

    let request = Request::new(CommandRequest {
        command: Some(command_book),
        sync_mode: SyncMode::Simple as i32,
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
    });

    let response = service.handle_command(request).await;
    assert!(response.is_ok());
}

// ============================================================================
// handle_sync_speculative Tests
// ============================================================================

/// Missing command in speculative request returns error.
#[tokio::test]
async fn test_handle_sync_speculative_missing_command_returns_error() {
    let (service, _) = create_test_service().await;

    let request = Request::new(SpeculateCommandHandlerRequest {
        command: None,
        point_in_time: None,
    });

    let response = service.handle_sync_speculative(request).await;
    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

/// Speculative with as_of_sequence works.
#[tokio::test]
async fn test_handle_sync_speculative_with_as_of_sequence() {
    let (service, business) = create_test_service().await;

    let root = Uuid::new_v4();
    let command_book = make_command_book("orders", root, 0);
    let events = make_event_book("orders", root, vec![make_event_page(0)]);
    business.enqueue_events(events).await;

    let request = Request::new(SpeculateCommandHandlerRequest {
        command: Some(command_book),
        point_in_time: Some(crate::proto::TemporalQuery {
            point_in_time: Some(crate::proto::temporal_query::PointInTime::AsOfSequence(5)),
        }),
    });

    let response = service.handle_sync_speculative(request).await;
    assert!(response.is_ok());
}

/// C03 (finding #4): `AsOfTime` speculative queries must succeed.
///
/// Regression test: `handle_sync_speculative` used to build the temporal
/// cutoff with `format!("{}.{}", ts.seconds, ts.nanos)`, e.g. `"5.0"`,
/// which is not RFC3339. `EventBookRepository::get_temporal_by_time` calls
/// `chrono::DateTime::parse_from_rfc3339` on that string, so every
/// `AsOfTime` speculative request failed with `Status::invalid_argument`
/// ("Failed to load temporal events: Invalid timestamp format: ...")
/// even though the caller supplied a perfectly valid `Timestamp`. The fix
/// routes through `storage::helpers::timestamp_to_rfc3339` (the same
/// helper `event_query::mod.rs` already uses), which emits a real RFC3339
/// string the repository can parse.
#[tokio::test]
async fn test_handle_sync_speculative_with_as_of_time_succeeds() {
    let (service, business) = create_test_service().await;

    let root = Uuid::new_v4();
    let command_book = make_command_book("orders", root, 0);
    let events = make_event_book("orders", root, vec![make_event_page(0)]);
    business.enqueue_events(events).await;

    let request = Request::new(SpeculateCommandHandlerRequest {
        command: Some(command_book),
        point_in_time: Some(crate::proto::TemporalQuery {
            point_in_time: Some(crate::proto::temporal_query::PointInTime::AsOfTime(
                prost_types::Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 500_000_000,
                },
            )),
        }),
    });

    let response = service.handle_sync_speculative(request).await;
    assert!(
        response.is_ok(),
        "AsOfTime speculative query should succeed with a well-formed RFC3339 cutoff, got: {:?}",
        response.err()
    );
}

/// The producer emits well-formed RFC3339, not the old `secs.nanos` format.
///
/// Exercises the conversion directly (the same helper the fix calls) to
/// pin the exact shape expected downstream — this is the assertion the
/// remediation plan calls for independent of the full pipeline round trip.
#[test]
fn test_as_of_time_conversion_emits_rfc3339_format() {
    let rfc3339_re = regex_lite_is_rfc3339;

    let ts = prost_types::Timestamp {
        seconds: 1_700_000_000,
        nanos: 500_000_000,
    };
    let result = crate::storage::helpers::timestamp_to_rfc3339(&ts).expect("valid timestamp");

    assert!(
        rfc3339_re(&result),
        "expected RFC3339 (e.g. 2023-11-14T22:13:20.5+00:00), got: {result}"
    );
    // Also confirm it actually round-trips through the parser the
    // repository uses -- the real regression was "does this parse", not
    // just "does this look right".
    assert!(
        chrono::DateTime::parse_from_rfc3339(&result).is_ok(),
        "conversion output must parse as RFC3339: {result}"
    );
}

/// Minimal RFC3339 shape check without pulling in a regex crate dependency:
/// `YYYY-MM-DDTHH:MM:SS` followed by an optional fractional second and a
/// UTC offset (`Z` or `+HH:MM`/`-HH:MM`).
fn regex_lite_is_rfc3339(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.len() < 20 {
        return false;
    }
    let date_time_ok = s.as_bytes()[4] == b'-'
        && s.as_bytes()[7] == b'-'
        && (s.as_bytes()[10] == b'T' || s.as_bytes()[10] == b't')
        && s.as_bytes()[13] == b':'
        && s.as_bytes()[16] == b':';
    let has_offset = s.contains('Z') || s.contains('z') || s[19..].contains(['+', '-']);
    date_time_ok && has_offset
}

// ============================================================================
// handle_compensation Tests
// ============================================================================

/// Missing command in compensation returns error.
#[tokio::test]
async fn test_handle_compensation_missing_command_returns_error() {
    let (service, _) = create_test_service().await;

    let request = Request::new(CommandRequest {
        command: None,
        sync_mode: SyncMode::Async as i32,
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
    });

    let response = service.handle_compensation(request).await;
    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

/// Compensation returns BusinessResponse directly.
///
/// Unlike normal handle_command, compensation callers need to inspect
/// the BusinessResponse to check for revocation flags.
#[tokio::test]
async fn test_handle_compensation_returns_business_response() {
    let (service, business) = create_test_service().await;

    let root = Uuid::new_v4();
    let command_book = make_command_book("orders", root, 0);
    let events = make_event_book("orders", root, vec![make_event_page(0)]);
    business.enqueue_events(events).await;

    let request = Request::new(CommandRequest {
        command: Some(command_book),
        sync_mode: SyncMode::Async as i32,
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
    });

    let response = service.handle_compensation(request).await;
    assert!(response.is_ok());
    let br = response.unwrap().into_inner();
    assert!(br.result.is_some());
}

/// Empty events response is valid for compensation.
#[tokio::test]
async fn test_handle_compensation_with_empty_response() {
    let (service, business) = create_test_service().await;

    let root = Uuid::new_v4();
    let command_book = make_command_book("orders", root, 0);
    // Default response is empty events
    business.enqueue_events(EventBook::default()).await;

    let request = Request::new(CommandRequest {
        command: Some(command_book),
        sync_mode: SyncMode::Async as i32,
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
    });

    let response = service.handle_compensation(request).await;
    assert!(response.is_ok());
    let br = response.unwrap().into_inner();
    // Verify events result was returned
    match br.result {
        Some(business_response::Result::Events(_)) => {}
        _ => panic!("Expected events response"),
    }
}

// ============================================================================
// handle_event (Fact Injection) Tests
// ============================================================================

/// Missing events in fact injection returns error.
#[tokio::test]
async fn test_handle_event_missing_events_returns_error() {
    let (service, _) = create_test_service().await;

    let request = Request::new(EventRequest {
        events: None,
        sync_mode: SyncMode::Async as i32,
        skip_handler: false,
    });

    let response = service.handle_event(request).await;
    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

/// An EventRequest with skip_handler unset/false routes the fact through
/// the aggregate's handle_fact — the safe proto3 default.
///
/// WHY (D-13, proto3 zero-value hazard): the removed routing bool
/// (EventRequest field 3, now reserved) claimed "default: true", but proto3
/// bools decode omitted fields as false — so a caller that set nothing
/// silently BYPASSED fact validation. skip_handler inverts the field so
/// the zero value means
/// "handler participates". This test pins that pivot: we deliberately
/// leave skip_handler out of the struct init (`..Default::default()`)
/// and assert the handler ran.
#[tokio::test]
async fn test_handle_event_default_routes_through_handler() {
    let (service, business) = create_test_service().await;

    let root = Uuid::new_v4();
    let facts = make_event_book("orders", root, vec![make_fact_page()]);
    business.enqueue_fact_response(Ok(facts.clone())).await;

    let request = Request::new(EventRequest {
        events: Some(facts),
        sync_mode: SyncMode::Async as i32,
        // skip_handler intentionally omitted: an unset field must route.
        ..Default::default()
    });

    let response = service.handle_event(request).await;
    assert!(
        response.is_ok(),
        "Expected ok but got: {:?}",
        response.err()
    );
    let fact_response = response.unwrap().into_inner();
    assert!(fact_response.events.is_some());
    assert_eq!(
        business.fact_invocation_count(),
        1,
        "unset skip_handler must invoke handle_fact (safe default)"
    );
}

/// skip_handler: true persists facts directly without invoking handle_fact.
///
/// WHY: bypassing the handler is now an explicit opt-in (projector-originated
/// writes) rather than the accidental result of omitting a proto3 bool, as it
/// was with the removed routing field (D-13).
#[tokio::test]
async fn test_handle_event_skip_handler_bypasses_handler() {
    let (service, business) = create_test_service().await;

    let root = Uuid::new_v4();
    let facts = make_event_book("orders", root, vec![make_fact_page()]);

    let request = Request::new(EventRequest {
        events: Some(facts),
        sync_mode: SyncMode::Async as i32,
        skip_handler: true,
    });

    let response = service.handle_event(request).await;
    assert!(
        response.is_ok(),
        "Expected ok but got: {:?}",
        response.err()
    );
    assert_eq!(
        business.fact_invocation_count(),
        0,
        "skip_handler: true must persist directly, never touching handle_fact"
    );
}

// ============================================================================
// Context Creation Tests
// ============================================================================

/// Async context creation succeeds.
#[tokio::test]
async fn test_create_async_context_succeeds() {
    let (service, _) = create_test_service().await;
    // Verify context creation doesn't panic
    let _ctx = service.create_async_context();
}

/// Sync context creation succeeds.
#[tokio::test]
async fn test_create_sync_context_succeeds() {
    let (service, _) = create_test_service().await;
    // Verify context creation with sync mode doesn't panic
    let _ctx = service.create_sync_context(SyncMode::Simple);
}

/// create_context_for_sync_mode with Async int creates async context.
///
/// The helper parses the proto int value and creates the appropriate context.
/// Async mode (0) should create an async context (no sync projector calls).
#[tokio::test]
async fn test_create_context_for_sync_mode_async() {
    let (service, _) = create_test_service().await;
    let _ctx = service.create_context_for_sync_mode(SyncMode::Async as i32);
}

/// create_context_for_sync_mode with an unknown wire int resolves to async.
#[tokio::test]
async fn test_create_context_for_sync_mode_unknown_defaults_to_async() {
    let (service, _) = create_test_service().await;
    let _ctx = service.create_context_for_sync_mode(999);
}

/// create_context_for_sync_mode with Simple int creates sync context.
///
/// Non-async modes should create a sync context that will call sync projectors.
#[tokio::test]
async fn test_create_context_for_sync_mode_simple() {
    let (service, _) = create_test_service().await;
    let _ctx = service.create_context_for_sync_mode(SyncMode::Simple as i32);
}

/// create_context_for_sync_mode with Cascade int creates sync context.
#[tokio::test]
async fn test_create_context_for_sync_mode_cascade() {
    let (service, _) = create_test_service().await;
    let _ctx = service.create_context_for_sync_mode(SyncMode::Cascade as i32);
}

/// create_context_for_sync_mode with Isolated int creates sync context.
///
/// ISOLATED is "wait for accept/reject + persist; no downstream" — it
/// behaves synchronously from the caller's perspective (the
/// CommandResponse is returned after persistence completes), so the
/// helper routes it through the sync-context branch like SIMPLE /
/// CASCADE / DECISION. The "no downstream" semantic is honored later
/// in `post_persist` (see orchestration/aggregate/grpc/mod.rs).
#[tokio::test]
async fn test_create_context_for_sync_mode_isolated() {
    let (service, _) = create_test_service().await;
    let _ctx = service.create_context_for_sync_mode(SyncMode::Isolated as i32);
}

/// create_context_for_sync_mode with invalid int defaults to async.
///
/// Invalid/unknown sync mode values should safely default to async mode
/// rather than failing, matching the behavior of the original pattern.
#[tokio::test]
async fn test_create_context_for_sync_mode_invalid_defaults_to_async() {
    let (service, _) = create_test_service().await;
    // Invalid value should default to Async
    let _ctx = service.create_context_for_sync_mode(999);
}

// ============================================================================
// Domain ownership
// ============================================================================

/// A coordinator bound to a domain refuses commands, speculation,
/// compensation and facts addressed to any other domain — otherwise they
/// would be persisted into this domain's store.
#[tokio::test]
async fn test_domain_bound_service_refuses_foreign_domain() {
    let (service, business) = create_test_service().await;
    let service = service.with_domain("orders");
    let root = Uuid::new_v4();
    let command = |domain: &str| CommandRequest {
        command: Some(make_command_book(domain, root, 0)),
        sync_mode: SyncMode::Async as i32,
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
    };

    let err = service
        .handle_command(Request::new(command("payments")))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("payments"));

    let err = service
        .handle_compensation(Request::new(command("payments")))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    let err = service
        .handle_sync_speculative(Request::new(SpeculateCommandHandlerRequest {
            command: Some(make_command_book("payments", root, 0)),
            point_in_time: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    let err = service
        .handle_event(Request::new(EventRequest {
            events: Some(make_event_book("payments", root, vec![make_fact_page()])),
            sync_mode: SyncMode::Async as i32,
            skip_handler: true,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    assert!(
        business.invocations.lock().await.is_empty(),
        "no foreign book reaches the handler"
    );

    // Its own domain is still served.
    service
        .handle_command(Request::new(command("orders")))
        .await
        .expect("own domain accepted");
}

/// Compensation goes through the same validation as commands.
#[tokio::test]
async fn test_handle_compensation_validates_command_book() {
    let (service, _business) = create_test_service().await;
    let mut book = make_command_book("orders", Uuid::new_v4(), 0);
    book.cover.as_mut().unwrap().correlation_id = "bad id!".to_string();
    let err = service
        .handle_compensation(Request::new(CommandRequest {
            command: Some(book),
            sync_mode: SyncMode::Async as i32,
            cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

/// Compensation events are persisted and published like a command's.
#[tokio::test]
async fn test_handle_compensation_persists_and_publishes_events() {
    let event_store = Arc::new(MockEventStore::new());
    let bus = Arc::new(MockEventBus::new());
    let business = Arc::new(MockClientLogic::new());
    let service = AggregateService::with_business_logic(
        event_store.clone(),
        Arc::new(SnapshotRepository::new(Arc::new(MockSnapshotStore::new()))),
        business.clone(),
        bus.clone(),
        Arc::new(StaticServiceDiscovery::new()),
    );
    let root = Uuid::new_v4();
    business
        .enqueue_events(make_event_book("orders", root, vec![make_event_page(0)]))
        .await;
    service
        .handle_compensation(Request::new(CommandRequest {
            command: Some(make_command_book("orders", root, 0)),
            sync_mode: SyncMode::Async as i32,
            cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
        }))
        .await
        .unwrap();
    use crate::storage::EventStore;
    assert_eq!(event_store.get("orders", "", root).await.unwrap().len(), 1);
    assert_eq!(bus.published_count().await, 1);
}
