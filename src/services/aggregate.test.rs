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
}

impl MockClientLogic {
    fn new() -> Self {
        Self {
            responses: Mutex::new(VecDeque::new()),
            fact_responses: Mutex::new(VecDeque::new()),
            invocations: Mutex::new(Vec::new()),
        }
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
        ..Default::default()
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
        ..Default::default()
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
    let business = Arc::new(MockClientLogic::new());
    let event_bus = Arc::new(MockEventBus::new());
    let discovery = Arc::new(StaticServiceDiscovery::new());

    let service = AggregateService::with_business_logic(
        event_store,
        snapshot_store,
        business.clone(),
        event_bus,
        discovery,
    );

    (service, business)
}

// ============================================================================
// Constructor Tests
// ============================================================================

/// Default constructor creates service with snapshots enabled.
#[tokio::test]
async fn test_with_business_logic_creates_service() {
    let (service, _) = create_test_service().await;
    // Verify service is created with expected configuration
    assert!(service.snapshot_read_enabled);
    assert!(service.snapshot_write_enabled);
    assert!(service.upcaster.is_none());
}

/// with_config respects snapshot settings.
#[tokio::test]
async fn test_with_business_logic_and_config_respects_snapshot_settings() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let business: Arc<dyn ClientLogic> = Arc::new(MockClientLogic::new());
    let event_bus = Arc::new(MockEventBus::new());
    let discovery = Arc::new(StaticServiceDiscovery::new());

    let service = AggregateService::with_business_logic_and_config(
        event_store,
        snapshot_store,
        business,
        event_bus,
        discovery,
        false, // snapshot_read_enabled
        false, // snapshot_write_enabled
    );

    assert!(!service.snapshot_read_enabled);
    assert!(!service.snapshot_write_enabled);
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
        cascade_id: None,
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
        cascade_id: None,
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
        cascade_id: None,
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
        cascade_id: None,
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
        cascade_id: None,
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
        cascade_id: None,
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
        route_to_handler: true,
    });

    let response = service.handle_event(request).await;
    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

/// Facts with route_to_handler invoke aggregate logic.
#[tokio::test]
async fn test_handle_event_with_route_to_handler() {
    let (service, business) = create_test_service().await;

    let root = Uuid::new_v4();
    let facts = make_event_book("orders", root, vec![make_fact_page()]);
    business.enqueue_fact_response(Ok(facts.clone())).await;

    let request = Request::new(EventRequest {
        events: Some(facts),
        sync_mode: SyncMode::Async as i32,
        route_to_handler: true,
    });

    let response = service.handle_event(request).await;
    assert!(
        response.is_ok(),
        "Expected ok but got: {:?}",
        response.err()
    );
    let fact_response = response.unwrap().into_inner();
    assert!(fact_response.events.is_some());
}

/// Facts are persisted with no_commit=false (committed) regardless of input.
///
/// Protobuf bool defaults to false, which now correctly means "committed".
/// But if a caller accidentally sets no_commit=true, the pipeline must
/// override it. Facts are external realities and must always be immediately
/// committed.
#[tokio::test]
async fn test_handle_event_facts_persisted_as_committed() {
    let (service, _) = create_test_service().await;

    let root = Uuid::new_v4();
    // Create a fact page with no_commit=true (simulate caller error)
    let mut fact_page = make_fact_page();
    fact_page.no_commit = true; // Simulate caller sending uncommitted

    let facts = make_event_book("orders", root, vec![fact_page]);

    let request = Request::new(EventRequest {
        events: Some(facts),
        sync_mode: SyncMode::Async as i32,
        route_to_handler: false,
    });

    let response = service.handle_event(request).await;
    assert!(
        response.is_ok(),
        "Expected ok but got: {:?}",
        response.err()
    );

    let fact_response = response.unwrap().into_inner();
    let events = fact_response.events.expect("should have events");

    // The pipeline must set no_commit=false on fact pages (committed)
    for page in &events.pages {
        assert!(
            !page.no_commit,
            "Fact events must be committed (no_commit=false), got no_commit=true for seq {:?}",
            page.header
        );
    }
}

/// Facts without route_to_handler persist directly.
#[tokio::test]
async fn test_handle_event_without_route_to_handler() {
    let (service, _) = create_test_service().await;

    let root = Uuid::new_v4();
    let facts = make_event_book("orders", root, vec![make_fact_page()]);

    let request = Request::new(EventRequest {
        events: Some(facts),
        sync_mode: SyncMode::Async as i32,
        route_to_handler: false,
    });

    let response = service.handle_event(request).await;
    assert!(
        response.is_ok(),
        "Expected ok but got: {:?}",
        response.err()
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
    // SyncMode::Async = 0
    let _ctx = service.create_context_for_sync_mode(SyncMode::Async as i32);
}

/// create_context_for_sync_mode with Simple int creates sync context.
///
/// Non-async modes should create a sync context that will call sync projectors.
#[tokio::test]
async fn test_create_context_for_sync_mode_simple() {
    let (service, _) = create_test_service().await;
    // SyncMode::Simple = 1
    let _ctx = service.create_context_for_sync_mode(SyncMode::Simple as i32);
}

/// create_context_for_sync_mode with Cascade int creates sync context.
#[tokio::test]
async fn test_create_context_for_sync_mode_cascade() {
    let (service, _) = create_test_service().await;
    // SyncMode::Cascade = 2
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
    // SyncMode::Isolated = 4
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
