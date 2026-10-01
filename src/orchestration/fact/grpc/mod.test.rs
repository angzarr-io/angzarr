use std::collections::HashMap;

use super::*;

/// GrpcFactExecutor returns AggregateNotFound for unknown domain.
#[tokio::test]
async fn test_inject_unknown_domain_returns_not_found() {
    let executor = GrpcFactExecutor::new(HashMap::new());
    let fact = EventBook {
        cover: Some(crate::proto::Cover {
            domain: "unknown".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let result = executor
        .inject(
            fact,
            crate::orchestration::FactDelivery::handled(crate::proto::SyncMode::Async),
        )
        .await;
    assert!(result.is_err());
    match result.unwrap_err() {
        FactInjectionError::AggregateNotFound { domain } => {
            assert_eq!(domain, "unknown");
        }
        other => panic!("Expected AggregateNotFound, got {:?}", other),
    }
}

/// The fact reaches the target coordinator with the producing flow's sync
/// mode and handler routing — a CASCADE stays synchronous through an
/// injected fact, and framework markers can bypass the fact handler.
#[tokio::test]
async fn test_inject_forwards_sync_mode_and_skip_handler() {
    use crate::proto::command_handler_coordinator_service_server::{
        CommandHandlerCoordinatorService, CommandHandlerCoordinatorServiceServer,
    };
    use crate::proto::{
        BusinessResponse, CommandRequest, CommandResponse, FactInjectionResponse,
        SpeculateCommandHandlerRequest, SyncMode,
    };
    use std::sync::Arc;
    use tonic::{Request, Response, Status};

    #[derive(Clone, Default)]
    struct Capture(Arc<tokio::sync::Mutex<Vec<EventRequest>>>);

    #[tonic::async_trait]
    impl CommandHandlerCoordinatorService for Capture {
        async fn handle_command(
            &self,
            _: Request<CommandRequest>,
        ) -> Result<Response<CommandResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn handle_sync_speculative(
            &self,
            _: Request<SpeculateCommandHandlerRequest>,
        ) -> Result<Response<CommandResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn handle_compensation(
            &self,
            _: Request<CommandRequest>,
        ) -> Result<Response<BusinessResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn handle_event(
            &self,
            request: Request<EventRequest>,
        ) -> Result<Response<FactInjectionResponse>, Status> {
            self.0.lock().await.push(request.into_inner());
            Ok(Response::new(FactInjectionResponse::default()))
        }
    }

    let capture = Capture::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = capture.clone();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(CommandHandlerCoordinatorServiceServer::new(server))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let channel = tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect_lazy();
    let executor = GrpcFactExecutor::new(HashMap::from([(
        "inventory".to_string(),
        crate::proto::command_handler_coordinator_service_client::CommandHandlerCoordinatorServiceClient::new(channel),
    )]));
    let fact = EventBook {
        cover: Some(crate::proto::Cover {
            domain: "inventory".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    };

    executor
        .inject(
            fact.clone(),
            crate::orchestration::FactDelivery::handled(SyncMode::Cascade),
        )
        .await
        .unwrap();
    executor
        .inject(
            fact,
            crate::orchestration::FactDelivery {
                sync_mode: SyncMode::Async,
                skip_handler: true,
            },
        )
        .await
        .unwrap();

    let seen = capture.0.lock().await;
    assert_eq!(seen[0].sync_mode, SyncMode::Cascade as i32);
    assert!(!seen[0].skip_handler);
    assert_eq!(seen[1].sync_mode, SyncMode::Async as i32);
    assert!(seen[1].skip_handler);
}
