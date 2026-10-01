//! Speculative projection must reach the projector's side-effect-free RPC;
//! returning an empty Projection (or calling `Handle`, which persists) breaks
//! the "what would this projection be" contract.

use std::sync::Arc;

use tokio::sync::Mutex;

use super::*;
use crate::proto::projector_service_server::{ProjectorService, ProjectorServiceServer};

#[derive(Clone, Default)]
struct RecordingProjector {
    calls: Arc<Mutex<Vec<&'static str>>>,
}

#[tonic::async_trait]
impl ProjectorService for RecordingProjector {
    async fn handle(
        &self,
        _request: tonic::Request<EventBook>,
    ) -> Result<tonic::Response<Projection>, Status> {
        self.calls.lock().await.push("handle");
        Ok(tonic::Response::new(Projection {
            projector: "persisted".into(),
            ..Default::default()
        }))
    }

    async fn handle_speculative(
        &self,
        _request: tonic::Request<EventBook>,
    ) -> Result<tonic::Response<Projection>, Status> {
        self.calls.lock().await.push("speculative");
        Ok(tonic::Response::new(Projection {
            projector: "speculative".into(),
            ..Default::default()
        }))
    }
}

async fn handler_for(projector: RecordingProjector) -> GrpcProjectorHandler {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ProjectorServiceServer::new(projector))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let channel = tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect_lazy();
    GrpcProjectorHandler::new(ProjectorServiceClient::new(channel))
}

#[tokio::test]
async fn test_speculate_calls_handle_speculative_only() {
    let projector = RecordingProjector::default();
    let handler = handler_for(projector.clone()).await;
    let projection = handler
        .handle(&EventBook::default(), ProjectionMode::Speculate)
        .await
        .unwrap();
    assert_eq!(projection.projector, "speculative");
    assert_eq!(*projector.calls.lock().await, vec!["speculative"]);
}

#[tokio::test]
async fn test_execute_calls_handle() {
    let projector = RecordingProjector::default();
    let handler = handler_for(projector.clone()).await;
    let projection = handler
        .handle(&EventBook::default(), ProjectionMode::Execute)
        .await
        .unwrap();
    assert_eq!(projection.projector, "persisted");
    assert_eq!(*projector.calls.lock().await, vec!["handle"]);
}
