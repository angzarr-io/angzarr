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

// ============================================================================
// Calls that never reached their target are sent again
// ============================================================================
//
// A coordinator restarting behind its Service address refuses connections
// for a moment; the request was never delivered, so sending it again is safe
// and keeps a CASCADE caller from failing on the restart. A request that was
// delivered is never sent twice.

/// An address nothing listens on (bound, then released).
async fn refused_address() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    listener.local_addr().expect("addr")
}

fn lazy_client(addr: std::net::SocketAddr) -> UpcasterServiceClient<tonic::transport::Channel> {
    let channel = tcp_endpoint(format!("http://{addr}"))
        .expect("endpoint")
        .connect_lazy();
    UpcasterServiceClient::new(channel).with_message_limits()
}

/// A refused connection is recognised as never having reached the target.
#[tokio::test]
async fn a_refused_connection_is_unconnected() {
    let mut client = lazy_client(refused_address().await);
    let status = client.upcast(request()).await.expect_err("nothing listens");
    assert!(is_unconnected(&status), "{status:?}");
}

/// The call succeeds once the target starts listening.
#[tokio::test]
async fn an_unconnected_call_is_sent_again_until_the_target_listens() {
    let addr = refused_address().await;
    let client = lazy_client(addr);
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let listener = TcpListener::bind(addr).await.expect("rebind");
        tonic::transport::Server::builder()
            .add_service(
                UpcasterServiceServer::new(LargeResponseUpcaster)
                    .max_encoding_message_size(SERVER_LIMIT),
            )
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("serve");
    });
    let attempts = std::sync::atomic::AtomicU32::new(0);
    let response = retry_unconnected(|| {
        attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut client = client.clone();
        async move { client.upcast(request()).await }
    })
    .await
    .expect("delivered once the target listens");
    assert_eq!(response.into_inner().events.len(), 1);
    assert!(attempts.load(std::sync::atomic::Ordering::SeqCst) > 1);
}

#[tokio::test]
async fn unconnected_calls_give_up_after_a_bounded_number_of_attempts() {
    let attempts = std::sync::atomic::AtomicU32::new(0);
    let outcome: Result<(), Status> = retry_unconnected(|| {
        attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        async {
            Err(Status::unavailable(
                "error trying to connect: tcp connect error",
            ))
        }
    })
    .await;
    assert_eq!(outcome.unwrap_err().code(), tonic::Code::Unavailable);
    assert_eq!(
        attempts.load(std::sync::atomic::Ordering::SeqCst),
        UNCONNECTED_ATTEMPTS
    );
}

#[tokio::test]
async fn a_delivered_call_that_fails_is_not_sent_again() {
    for status in [
        Status::unavailable("upstream overloaded"),
        Status::failed_precondition("tcp connect error"),
        Status::internal("boom"),
    ] {
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let outcome: Result<(), Status> = retry_unconnected(|| {
            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let status = status.clone();
            async move { Err(status) }
        })
        .await;
        assert!(outcome.is_err());
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}

#[test]
fn unconnected_markers() {
    for message in [
        "tcp connect error: Connection refused",
        "error trying to connect: x",
        "dns error: failed to lookup",
    ] {
        assert!(is_unconnected(&Status::unavailable(message)), "{message}");
    }
    assert!(!is_unconnected(&Status::unavailable("connection reset")));
}

/// An outbound endpoint takes a valid URI and refuses an invalid one.
#[test]
fn tcp_endpoint_validates_the_uri() {
    assert!(tcp_endpoint("http://127.0.0.1:1310").is_ok());
    assert!(tcp_endpoint("not a uri").is_err());
}
