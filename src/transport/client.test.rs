//! Tests for client-side gRPC message limits.
//!
//! Servers accept and send messages up to the framework limit (10 MiB by
//! default). A generated client keeps tonic's 4 MiB decode limit unless the
//! framework limit is applied, so a 4-10 MiB response would fail on the
//! caller after the server successfully produced it.

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use super::*;
use crate::proto::upcaster_service_client::UpcasterServiceClient;
use crate::proto::upcaster_service_server::{UpcasterService, UpcasterServiceServer};
use crate::proto::{EventPage, UpcastRequest, UpcastResponse};

/// Server limit for the test server; above every client limit under test.
const SERVER_LIMIT: usize = 16 * 1024 * 1024;

/// Response payload between tonic's 4 MiB default and the 10 MiB framework
/// limit.
const RESPONSE_BYTES: usize = 5 * 1024 * 1024;

/// Answers every upcast with one event page carrying `RESPONSE_BYTES`.
struct LargeResponseUpcaster;

#[tonic::async_trait]
impl UpcasterService for LargeResponseUpcaster {
    async fn upcast(
        &self,
        _request: Request<UpcastRequest>,
    ) -> Result<Response<UpcastResponse>, Status> {
        let page = EventPage {
            payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
                type_url: "type.googleapis.com/test.Large".into(),
                value: vec![7u8; RESPONSE_BYTES],
            })),
            ..Default::default()
        };
        Ok(Response::new(UpcastResponse { events: vec![page] }))
    }
}

async fn start_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                UpcasterServiceServer::new(LargeResponseUpcaster)
                    .max_decoding_message_size(SERVER_LIMIT)
                    .max_encoding_message_size(SERVER_LIMIT),
            )
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("serve");
    });
    format!("{}", addr)
}

fn request() -> Request<UpcastRequest> {
    Request::new(UpcastRequest {
        domain: "order".into(),
        events: vec![],
    })
}

/// A client with the framework limits receives a 5 MiB response.
#[tokio::test]
async fn client_with_message_limits_receives_large_response() {
    let addr = start_server().await;
    let channel = connect_to_address(&addr).await.expect("connect");
    let mut client = UpcasterServiceClient::new(channel).with_message_limits();

    let response = client.upcast(request()).await.expect("large response");
    let page = &response.into_inner().events[0];
    match &page.payload {
        Some(crate::proto::event_page::Payload::Event(any)) => {
            assert_eq!(any.value.len(), RESPONSE_BYTES)
        }
        other => panic!("unexpected payload {other:?}"),
    }
}

/// Control: the same response overflows an unconfigured client, which is
/// what the framework limit exists to prevent.
#[tokio::test]
async fn client_without_message_limits_rejects_large_response() {
    let addr = start_server().await;
    let channel = connect_to_address(&addr).await.expect("connect");
    let mut client = UpcasterServiceClient::new(channel);

    let status = client
        .upcast(request())
        .await
        .expect_err("4 MiB default must reject a 5 MiB response");
    assert_eq!(status.code(), tonic::Code::OutOfRange, "{status}");
}
