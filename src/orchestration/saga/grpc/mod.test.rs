//! Tests for GrpcSagaContext and GrpcSagaContextFactory.
//!
//! The saga context handles the prepare/execute lifecycle for sagas
//! in distributed mode. Key behaviors:
//! - prepare_destinations: Gets covers for destination aggregates
//! - handle: Executes saga logic and returns commands
//! - source_cover: Provides access to source event's cover
//! - on_command_rejected: Initiates compensation flow
//!
//! In-process tonic servers stand in for the saga's client logic and the
//! source aggregate's coordinator.

use super::*;
use crate::proto::{Cover, Edition, Uuid as ProtoUuid};

fn make_source_event_book(domain: &str) -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(ProtoUuid {
                value: vec![1, 2, 3, 4],
            }),
            correlation_id: "corr-123".to_string(),
            edition: Some(Edition {
                name: "v1".to_string(),
                divergences: vec![],
            }),
            ext: None,
        }),
        pages: vec![],
        snapshot: None,
        ..Default::default()
    }
}

// ============================================================================
// GrpcSagaContext Tests (non-gRPC aspects)
// ============================================================================

// ============================================================================
// GrpcSagaContext against an in-process SagaService / coordinator
// ============================================================================

use crate::proto::command_handler_coordinator_service_server::{
    CommandHandlerCoordinatorService, CommandHandlerCoordinatorServiceServer,
};
use crate::proto::saga_service_server::{SagaService, SagaServiceServer};
use crate::proto::{
    BusinessResponse, CommandResponse, EventRequest, FactInjectionResponse,
    SpeculateCommandHandlerRequest,
};

/// Saga client logic: records the request and emits one command and one
/// fact carrying a different edition than the source.
#[derive(Clone, Default)]
struct RecordingSaga(Arc<Mutex<Vec<SagaHandleRequest>>>);

#[tonic::async_trait]
impl SagaService for RecordingSaga {
    async fn handle(
        &self,
        request: tonic::Request<SagaHandleRequest>,
    ) -> Result<tonic::Response<SagaResponse>, tonic::Status> {
        self.0.lock().await.push(request.into_inner());
        let wrong_edition = || Cover {
            domain: "inventory".into(),
            edition: Some(Edition {
                name: "handler-chose-this".into(),
                divergences: vec![],
            }),
            ..Default::default()
        };
        Ok(tonic::Response::new(SagaResponse {
            commands: vec![CommandBook {
                cover: Some(wrong_edition()),
                pages: vec![],
            }],
            events: vec![EventBook {
                cover: Some(wrong_edition()),
                ..Default::default()
            }],
        }))
    }
}

/// Source aggregate coordinator recording HandleCompensation calls.
#[derive(Clone, Default)]
struct RecordingCompensation(Arc<Mutex<Vec<CommandRequest>>>);

#[tonic::async_trait]
impl CommandHandlerCoordinatorService for RecordingCompensation {
    async fn handle_command(
        &self,
        _: tonic::Request<CommandRequest>,
    ) -> Result<tonic::Response<CommandResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn handle_sync_speculative(
        &self,
        _: tonic::Request<SpeculateCommandHandlerRequest>,
    ) -> Result<tonic::Response<CommandResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn handle_compensation(
        &self,
        request: tonic::Request<CommandRequest>,
    ) -> Result<tonic::Response<BusinessResponse>, tonic::Status> {
        self.0.lock().await.push(request.into_inner());
        Ok(tonic::Response::new(BusinessResponse::default()))
    }
    async fn handle_event(
        &self,
        _: tonic::Request<EventRequest>,
    ) -> Result<tonic::Response<FactInjectionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
}

async fn serve(router: tonic::transport::server::Router) -> tonic::transport::Channel {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        router
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect_lazy()
}

async fn context_with(
    saga: RecordingSaga,
    compensation: Option<RecordingCompensation>,
    source: EventBook,
) -> GrpcSagaContext {
    let saga_channel =
        serve(tonic::transport::Server::builder().add_service(SagaServiceServer::new(saga))).await;
    let compensation_client = match compensation {
        Some(c) => Some(Arc::new(Mutex::new(
            CommandHandlerCoordinatorServiceClient::new(
                serve(
                    tonic::transport::Server::builder()
                        .add_service(CommandHandlerCoordinatorServiceServer::new(c)),
                )
                .await,
            ),
        ))),
        None => None,
    };
    GrpcSagaContext::new(
        Arc::new(Mutex::new(SagaServiceClient::new(saga_channel))),
        Arc::new(crate::bus::MockEventBus::new()),
        SagaCompensationConfig::default(),
        compensation_client,
        source,
        Arc::new(crate::dlq::NoopDeadLetterPublisher),
        "saga-orders-inventory".into(),
    )
}

/// handle() sends the source with the inherited sync mode, and every
/// emitted command and fact inherits the source's edition whatever the
/// handler set.
#[tokio::test]
async fn test_handle_forwards_sync_mode_and_propagates_source_edition() {
    let saga = RecordingSaga::default();
    let ctx = context_with(saga.clone(), None, make_source_event_book("orders")).await;

    let response = ctx.handle(SyncMode::Cascade).await.unwrap();

    let requests = saga.0.lock().await;
    assert_eq!(requests[0].sync_mode, SyncMode::Cascade as i32);
    assert_eq!(
        requests[0]
            .source
            .as_ref()
            .unwrap()
            .cover
            .as_ref()
            .unwrap()
            .domain,
        "orders"
    );
    let edition = |cover: &Option<Cover>| cover.as_ref().unwrap().edition.clone().unwrap().name;
    assert_eq!(edition(&response.commands[0].cover), "v1");
    assert_eq!(edition(&response.events[0].cover), "v1");
}

/// source_cover / source_max_sequence describe the triggering book.
#[tokio::test]
async fn test_source_accessors_describe_the_source_book() {
    let mut source = make_source_event_book("orders");
    source.pages = [3u32, 7, 5]
        .into_iter()
        .map(|seq| crate::proto::EventPage {
            header: Some(crate::proto::PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(seq)),
            }),
            ..Default::default()
        })
        .collect();
    let ctx = context_with(RecordingSaga::default(), None, source).await;
    assert_eq!(ctx.source_cover().unwrap().domain, "orders");
    assert_eq!(ctx.source_max_sequence(), 7);
    assert_eq!(ctx.component_name(), "saga-orders-inventory");
    assert!(ctx.dlq_publisher().is_some());
}

/// A rejected saga command is sent back to its source aggregate's
/// coordinator as a compensation notification.
#[tokio::test]
async fn test_rejected_command_is_routed_to_source_compensation() {
    use crate::proto::{
        page_header::SequenceType, AngzarrDeferredSequence, CommandPage, PageHeader,
    };
    let compensation = RecordingCompensation::default();
    let ctx = context_with(
        RecordingSaga::default(),
        Some(compensation.clone()),
        make_source_event_book("orders"),
    )
    .await;
    let source = make_source_event_book("orders").cover.unwrap();
    let rejected = CommandBook {
        cover: Some(Cover {
            domain: "inventory".into(),
            correlation_id: "corr-123".into(),
            ..Default::default()
        }),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::AngzarrDeferred(AngzarrDeferredSequence {
                    source: Some(source),
                    source_seq: 2,
                    source_component: "saga-orders-inventory".into(),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        }],
    };

    ctx.on_command_rejected(&rejected, "out of stock").await;

    let calls = compensation.0.lock().await;
    assert_eq!(calls.len(), 1, "one compensation notification");
    let notification = calls[0].command.as_ref().unwrap();
    assert_eq!(notification.cover.as_ref().unwrap().domain, "orders");
}

// ============================================================================
// CompensationContext Tests (via saga_compensation module)
// ============================================================================

/// Non-saga commands don't create compensation context.
///
/// Direct API commands (without angzarr_deferred provenance) are rejected directly
/// to the caller, not through the compensation flow.
#[test]
fn test_compensation_context_requires_angzarr_deferred() {
    use crate::proto::CommandBook;

    let command = CommandBook {
        cover: Some(Cover {
            domain: "orders".to_string(),
            root: None,
            correlation_id: "corr-123".to_string(),
            edition: None,
            ext: None,
        }),
        pages: vec![],
    };

    let context =
        CompensationContext::from_rejected_command(&command, "test rejection".to_string());

    assert!(
        context.is_none(),
        "Non-saga command should not create context"
    );
}

/// Saga commands create compensation context with source info.
#[test]
fn test_compensation_context_captures_saga_source() {
    use crate::proto::{
        command_page, page_header, AngzarrDeferredSequence, CommandBook, CommandPage,
        MergeStrategy, PageHeader,
    };

    let command = CommandBook {
        cover: Some(Cover {
            domain: "customer".to_string(),
            root: Some(ProtoUuid {
                value: vec![5, 6, 7, 8],
            }),
            correlation_id: "corr-456".to_string(),
            edition: None,
            ext: None,
        }),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::AngzarrDeferred(
                    AngzarrDeferredSequence {
                        source: Some(Cover {
                            domain: "orders".to_string(),
                            root: Some(ProtoUuid {
                                value: vec![1, 2, 3, 4],
                            }),
                            correlation_id: "corr-456".to_string(),
                            edition: None,
                            ext: None,
                        }),
                        source_seq: 5,
                        ..Default::default()
                    },
                )),
            }),
            payload: Some(command_page::Payload::Command(prost_types::Any {
                type_url: "test.SomeCommand".to_string(),
                value: vec![],
            })),
            merge_strategy: MergeStrategy::MergeCommutative as i32,
        }],
    };

    let context =
        CompensationContext::from_rejected_command(&command, "customer not found".to_string());

    assert!(context.is_some());
    let ctx = context.unwrap();
    assert_eq!(ctx.source.source_seq, 5);
    assert_eq!(ctx.source.source.as_ref().unwrap().domain, "orders");
    assert_eq!(ctx.rejection_reason, "customer not found");
    assert_eq!(ctx.correlation_id, "corr-456");
}

// ============================================================================
// H-17: SagaHandleRequest must carry the inherited sync_mode
// ============================================================================

#[test]
fn test_build_saga_handle_request_propagates_inherited_sync_mode_decision() {
    let source = make_source_event_book("orders");
    let request = super::build_saga_handle_request(&source, crate::proto::SyncMode::Decision);
    assert_eq!(
        request.sync_mode,
        crate::proto::SyncMode::Decision as i32,
        "H-17: SagaHandleRequest.sync_mode must reflect orchestrate_saga\'s sync_mode (Decision), not legacy hardcoded Simple"
    );
    assert!(request.source.is_some());
}

#[test]
fn test_build_saga_handle_request_propagates_inherited_sync_mode_cascade() {
    let source = make_source_event_book("orders");
    let request = super::build_saga_handle_request(&source, crate::proto::SyncMode::Cascade);
    assert_eq!(request.sync_mode, crate::proto::SyncMode::Cascade as i32);
}

#[test]
fn test_build_saga_handle_request_propagates_inherited_sync_mode_simple() {
    let source = make_source_event_book("orders");
    let request = super::build_saga_handle_request(&source, crate::proto::SyncMode::Simple);
    assert_eq!(request.sync_mode, crate::proto::SyncMode::Simple as i32);
}
