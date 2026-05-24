//! Tests for projector coordinator service.
//!
//! The projector coordinator distributes events to registered projectors.
//! It ensures projectors receive complete EventBooks by fetching missing
//! history from the EventQuery service when needed.
//!
//! Why this matters: Projectors build read models from events. If they
//! receive incomplete EventBooks (missing history), the read model will
//! be inconsistent. The coordinator's repair mechanism ensures eventual
//! consistency by fetching missing events before forwarding.
//!
//! Key behaviors verified:
//! - handle_sync returns empty projection when no projectors registered
//! - handle succeeds (fire-and-forget) even with no projectors
//! - Invalid projector addresses are rejected during registration

use super::*;
use crate::proto::event_query_service_server::EventQueryServiceServer;
use crate::proto::{Cover, Uuid as ProtoUuid};
use crate::services::EventQueryService;
use crate::storage::mock::{MockEventStore, MockSnapshotStore};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tonic::transport::Server;

fn make_event_book() -> EventBook {
    use crate::proto::Edition;
    EventBook {
        cover: Some(Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid { value: vec![1; 16] }),
            correlation_id: String::new(),
            edition: Some(Edition {
                name: String::new(),
                divergences: vec![],
            }),
            ext: None,
        }),
        pages: vec![],
        snapshot: None,
        ..Default::default()
    }
}

async fn start_event_query_server() -> SocketAddr {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let service = EventQueryService::new(event_store, snapshot_store);

    tokio::spawn(async move {
        Server::builder()
            .add_service(EventQueryServiceServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    addr
}

// ============================================================================
// Handle Sync Tests
// ============================================================================

/// No projectors returns empty projection.
///
/// When no projectors are registered, handle_sync returns an empty projection
/// rather than failing. This allows the system to operate during deployment.
#[tokio::test]
async fn test_handle_sync_with_no_projectors_returns_empty_projection() {
    let addr = start_event_query_server().await;
    let coordinator = ProjectorCoord::connect(&addr.to_string()).await.unwrap();

    let event_book = make_event_book();
    let sync_request = EventRequest {
        events: Some(event_book),
        sync_mode: crate::proto::SyncMode::Simple.into(),
        route_to_handler: false,
    };

    let response = coordinator.handle_sync(Request::new(sync_request)).await;

    assert!(response.is_ok());
    let projection = response.unwrap().into_inner();
    assert!(projection.projector.is_empty());
    assert_eq!(projection.sequence, 0);
}

// ============================================================================
// Handle Async Tests
// ============================================================================

/// Fire-and-forget succeeds with no projectors.
///
/// Async handle is for event distribution without waiting for results.
/// Success with no projectors allows gradual projector deployment.
#[tokio::test]
async fn test_handle_with_no_projectors_succeeds() {
    let addr = start_event_query_server().await;
    let coordinator = ProjectorCoord::connect(&addr.to_string()).await.unwrap();

    let event_book = make_event_book();

    let response = coordinator.handle(Request::new(event_book)).await;

    assert!(response.is_ok());
}

// ============================================================================
// Registration Tests
// ============================================================================

/// Invalid address rejected during registration.
///
/// Early validation prevents silent failures during event distribution.
#[tokio::test]
async fn test_add_projector_invalid_address() {
    let addr = start_event_query_server().await;
    let coordinator = ProjectorCoord::connect(&addr.to_string()).await.unwrap();

    let config = ServiceEndpoint {
        name: "test".to_string(),
        address: "".to_string(), // Invalid
    };

    let result = coordinator.add_projector(config).await;

    assert!(result.is_err());
}
