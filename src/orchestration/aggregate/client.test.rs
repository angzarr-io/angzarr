//! Concurrent commands must reach the business-logic service concurrently.
//! Holding a lock across the RPC serialized every command of a sidecar and
//! could deadlock a CASCADE that re-enters the same sidecar.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Barrier;

use super::*;
use crate::proto::command_handler_service_server::{
    CommandHandlerService, CommandHandlerServiceServer,
};
use crate::proto::{BusinessResponse as Response, ReplayResponse};

/// Answers `Handle` only once two calls are in flight at the same time.
struct NeedsTwoConcurrentCalls(Arc<Barrier>);

#[tonic::async_trait]
impl CommandHandlerService for NeedsTwoConcurrentCalls {
    async fn handle(
        &self,
        _request: tonic::Request<ContextualCommand>,
    ) -> Result<tonic::Response<Response>, Status> {
        self.0.wait().await;
        Ok(tonic::Response::new(Response::default()))
    }

    async fn handle_fact(
        &self,
        _request: tonic::Request<FactRequest>,
    ) -> Result<tonic::Response<EventBook>, Status> {
        Err(Status::unimplemented("unused"))
    }

    async fn replay(
        &self,
        _request: tonic::Request<ReplayRequest>,
    ) -> Result<tonic::Response<ReplayResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
}

#[tokio::test]
async fn test_concurrent_invocations_are_in_flight_together() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let service = NeedsTwoConcurrentCalls(Arc::new(Barrier::new(2)));
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(CommandHandlerServiceServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let channel = tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect_lazy();
    let logic = Arc::new(GrpcBusinessLogic::new(CommandHandlerServiceClient::new(
        channel,
    )));

    let both = futures::future::join(
        logic.invoke(ContextualCommand::default()),
        logic.invoke(ContextualCommand::default()),
    );
    let (first, second) = tokio::time::timeout(Duration::from_secs(5), both)
        .await
        .expect("calls were serialized: the second never reached the service");
    first.unwrap();
    second.unwrap();
}
