//! Integration tests for upcaster gRPC communication.
//!
//! The upcaster calls a client-provided UpcasterService to transform old
//! event versions. These tests verify:
//! - V1 → V2 type_url transformation
//! - Sequence numbers preserved through transformation
//! - Error propagation from upcaster service
//! - Connection failure handling
//!
//! Why this matters: Event upcasting happens during aggregate replay, which
//! is on the critical path for command processing. Failures here block all
//! business operations until resolved. These tests ensure the gRPC integration
//! handles both success and error cases correctly.
//!
//! A mock upcaster service simulates the client implementation.

use super::*;
use crate::proto::event_page;
use crate::proto::upcaster_service_server::{UpcasterService, UpcasterServiceServer};
use crate::proto::{page_header, PageHeader, UpcastRequest, UpcastResponse};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use tonic::transport::Server;
use tonic::{Request, Response};

// ============================================================================
// Test Doubles
// ============================================================================

/// Mock upcaster that transforms event type_urls from V1 to V2.
struct MockUpcasterService {
    call_count: AtomicU32,
    should_fail: bool,
}

impl MockUpcasterService {
    fn new() -> Self {
        Self {
            call_count: AtomicU32::new(0),
            should_fail: false,
        }
    }

    fn failing() -> Self {
        Self {
            call_count: AtomicU32::new(0),
            should_fail: true,
        }
    }
}

#[tonic::async_trait]
impl UpcasterService for MockUpcasterService {
    async fn upcast(
        &self,
        request: Request<UpcastRequest>,
    ) -> Result<Response<UpcastResponse>, tonic::Status> {
        self.call_count.fetch_add(1, Ordering::SeqCst);

        if self.should_fail {
            return Err(tonic::Status::internal("Simulated upcaster failure"));
        }

        let req = request.into_inner();

        // Transform events: rename V1 type_urls to V2
        let transformed: Vec<EventPage> = req
            .events
            .into_iter()
            .map(|mut page| {
                if let Some(event_page::Payload::Event(ref mut event)) = page.payload {
                    // Simulate V1 -> V2 transformation
                    if event.type_url.ends_with("V1") {
                        event.type_url = event.type_url.replace("V1", "V2");
                    }
                    // Simulate field migration: add marker byte
                    if !event.value.is_empty() {
                        event.value.push(0xFF); // Migration marker
                    }
                }
                page
            })
            .collect();

        Ok(Response::new(UpcastResponse {
            events: transformed,
        }))
    }
}

/// Start a mock upcaster server and return its address.
async fn start_mock_server(service: MockUpcasterService) -> SocketAddr {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(UpcasterServiceServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Give server time to start
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    local_addr
}

fn make_test_event(seq: u32, type_url: &str, value: Vec<u8>) -> EventPage {
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(seq)),
        }),
        created_at: None,
        payload: Some(event_page::Payload::Event(prost_types::Any {
            type_url: type_url.to_string(),
            value,
        })),
        ..Default::default()
    }
}

// ============================================================================
// Transformation Tests
// ============================================================================

/// V1 type_urls transformed to V2 by upcaster service.
///
/// Primary use case: schema evolution. Old events with CustomerCreatedV1
/// are transformed to CustomerCreatedV2 with migrated field values.
#[tokio::test]
async fn test_upcaster_transforms_events() {
    let addr = start_mock_server(MockUpcasterService::new()).await;

    let upcaster = Upcaster::from_address(&addr.to_string())
        .await
        .expect("Failed to connect");
    assert!(upcaster.is_enabled());

    let events = vec![
        make_test_event(0, "example.OrderCreatedV1", vec![1, 2, 3]),
        make_test_event(1, "example.OrderUpdatedV1", vec![4, 5, 6]),
    ];

    let result = upcaster.upcast("order", events).await.unwrap();

    assert_eq!(result.len(), 2);

    // Verify V1 -> V2 transformation
    let event0 = match &result[0].payload {
        Some(event_page::Payload::Event(e)) => e,
        _ => panic!("Expected event payload"),
    };
    assert_eq!(event0.type_url, "example.OrderCreatedV2");
    assert_eq!(event0.value, vec![1, 2, 3, 0xFF]); // Migration marker added

    let event1 = match &result[1].payload {
        Some(event_page::Payload::Event(e)) => e,
        _ => panic!("Expected event payload"),
    };
    assert_eq!(event1.type_url, "example.OrderUpdatedV2");
    assert_eq!(event1.value, vec![4, 5, 6, 0xFF]);
}

/// Non-V1 events pass through with value modification only.
///
/// The mock adds a migration marker even for non-V1 events to simulate
/// field migration. In production, no-op transformation is valid.
#[tokio::test]
async fn test_upcaster_preserves_non_v1_events() {
    let addr = start_mock_server(MockUpcasterService::new()).await;

    let upcaster = Upcaster::from_address(&addr.to_string()).await.unwrap();

    let events = vec![
        make_test_event(0, "example.OrderCreated", vec![1, 2]), // No V1 suffix
    ];

    let result = upcaster.upcast("order", events).await.unwrap();

    assert_eq!(result.len(), 1);
    let event = match &result[0].payload {
        Some(event_page::Payload::Event(e)) => e,
        _ => panic!("Expected event payload"),
    };
    assert_eq!(event.type_url, "example.OrderCreated"); // Unchanged
    assert_eq!(event.value, vec![1, 2, 0xFF]); // But value still gets marker
}

/// Sequence numbers preserved through transformation.
///
/// Critical invariant: upcasting transforms content, not identity.
/// Sequence numbers identify event positions in the stream.
#[tokio::test]
async fn test_upcaster_preserves_sequence_numbers() {
    let addr = start_mock_server(MockUpcasterService::new()).await;

    let upcaster = Upcaster::from_address(&addr.to_string()).await.unwrap();

    let events = vec![
        make_test_event(5, "example.EventV1", vec![]),
        make_test_event(6, "example.EventV1", vec![]),
        make_test_event(7, "example.EventV1", vec![]),
    ];

    let result = upcaster.upcast("test", events).await.unwrap();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].sequence_num(), 5);
    assert_eq!(result[1].sequence_num(), 6);
    assert_eq!(result[2].sequence_num(), 7);
}

/// Empty events short-circuit without calling server.
#[tokio::test]
async fn test_upcaster_handles_empty_events() {
    let addr = start_mock_server(MockUpcasterService::new()).await;

    let upcaster = Upcaster::from_address(&addr.to_string()).await.unwrap();

    // Empty events should short-circuit without calling server
    let result = upcaster.upcast("test", vec![]).await.unwrap();
    assert!(result.is_empty());
}

// ============================================================================
// Error Handling Tests
// ============================================================================

/// Upcaster service errors propagate as gRPC status.
///
/// Transformation failures are fatal — aggregate can't be reconstructed
/// without proper upcasting. Error must propagate to caller.
#[tokio::test]
async fn test_upcaster_error_propagation() {
    let addr = start_mock_server(MockUpcasterService::failing()).await;

    let upcaster = Upcaster::from_address(&addr.to_string()).await.unwrap();

    let events = vec![make_test_event(0, "example.Event", vec![1])];

    let result = upcaster.upcast("test", events).await;

    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::Internal);
    assert!(status.message().contains("Simulated upcaster failure"));
}

/// Connection failure returns error during client creation.
#[tokio::test]
async fn test_upcaster_connection_failure() {
    let result = Upcaster::from_address("127.0.0.1:1").await;
    assert!(result.is_err());
}

// ============================================================================
// Channel Sharing Tests
// ============================================================================

/// Upcaster can share channel with client logic.
///
/// Both AggregateService and UpcasterService run on same client binary.
/// Sharing channel reduces connection overhead.
#[tokio::test]
async fn test_upcaster_from_channel() {
    let addr = start_mock_server(MockUpcasterService::new()).await;

    // Connect via channel (simulates sharing channel with client logic)
    let channel = Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();

    let upcaster = Upcaster::from_channel(channel);
    assert!(upcaster.is_enabled());

    let events = vec![make_test_event(0, "example.EventV1", vec![1])];
    let result = upcaster.upcast("test", events).await.unwrap();

    assert_eq!(result.len(), 1);
    let event = match &result[0].payload {
        Some(event_page::Payload::Event(e)) => e,
        _ => panic!("Expected event payload"),
    };
    assert_eq!(event.type_url, "example.EventV2");
}
