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

/// Echoes each request back in its response, so a test can see exactly
/// what the coordinator put on the wire and what it made of the answer.
struct Echo;

#[tonic::async_trait]
impl CommandHandlerService for Echo {
    async fn handle(
        &self,
        request: tonic::Request<ContextualCommand>,
    ) -> Result<tonic::Response<Response>, Status> {
        let events = request.into_inner().events.unwrap_or_default();
        Ok(tonic::Response::new(Response {
            result: Some(crate::proto::business_response::Result::Events(events)),
        }))
    }

    async fn handle_fact(
        &self,
        request: tonic::Request<FactRequest>,
    ) -> Result<tonic::Response<EventBook>, Status> {
        let request = request.into_inner();
        let mut book = request.facts.unwrap_or_default();
        book.pages
            .extend(request.prior_events.unwrap_or_default().pages);
        Ok(tonic::Response::new(book))
    }

    async fn replay(
        &self,
        request: tonic::Request<ReplayRequest>,
    ) -> Result<tonic::Response<ReplayResponse>, Status> {
        let request = request.into_inner();
        Ok(tonic::Response::new(ReplayResponse {
            state: Some(prost_types::Any {
                type_url: format!(
                    "pages={} snapshot={}",
                    request.events.len(),
                    request.base_snapshot.map_or(0, |s| s.sequence)
                ),
                value: vec![],
            }),
        }))
    }
}

async fn echo_logic() -> GrpcBusinessLogic {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(CommandHandlerServiceServer::new(Echo))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let channel = tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect_lazy();
    GrpcBusinessLogic::new(CommandHandlerServiceClient::new(channel))
}

fn book_with_pages(domain: &str, pages: usize) -> EventBook {
    EventBook {
        cover: Some(crate::proto::Cover {
            domain: domain.to_string(),
            ..Default::default()
        }),
        pages: vec![crate::proto::EventPage::default(); pages],
        ..Default::default()
    }
}

/// Command, fact and replay calls carry the coordinator's inputs to the
/// business-logic service and hand back what it answered.
#[tokio::test]
async fn test_calls_carry_inputs_and_return_the_service_answer() {
    let logic = echo_logic().await;

    let response = logic
        .invoke(ContextualCommand {
            events: Some(book_with_pages("orders", 2)),
            ..Default::default()
        })
        .await
        .unwrap();
    match response.result {
        Some(crate::proto::business_response::Result::Events(book)) => {
            assert_eq!(book.pages.len(), 2);
        }
        other => panic!("expected the echoed events, got {other:?}"),
    }

    let recorded = logic
        .invoke_fact(FactContext {
            facts: book_with_pages("orders", 1),
            prior_events: Some(book_with_pages("orders", 3)),
        })
        .await
        .unwrap();
    assert_eq!(recorded.pages.len(), 4);
    assert_eq!(recorded.cover.unwrap().domain, "orders");

    let mut history = book_with_pages("orders", 5);
    history.snapshot = Some(crate::proto::Snapshot {
        sequence: 7,
        ..Default::default()
    });
    let state = logic.replay(&history).await.unwrap();
    assert_eq!(state.type_url, "pages=5 snapshot=7");
}
