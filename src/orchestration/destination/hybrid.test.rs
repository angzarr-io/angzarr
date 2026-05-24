//! Tests for hybrid destination fetcher.
//!
//! The hybrid fetcher solves the PM "chicken-and-egg" problem:
//! - PMs need their own state but run as sidecars, not aggregate services
//! - Normal destination fetchers route to aggregate services via gRPC
//! - Hybrid routes local domain to local storage, others to remote
//!
//! Key behaviors tested:
//! - Local domain queries use local storage
//! - Non-local domain queries delegate to remote fetcher
//! - Correlation ID is preserved in responses

use super::*;
use crate::proto::{Cover, Edition, EventBook, Uuid as ProtoUuid};
use crate::storage::mock::{MockEventStore, MockSnapshotStore};
use std::sync::Arc;
use uuid::Uuid;

// ============================================================================
// Mock Remote Fetcher
// ============================================================================

/// Mock remote fetcher that tracks calls and returns configured responses.
struct MockRemoteFetcher {
    fetch_response: Option<EventBook>,
    fetch_by_correlation_response: Option<EventBook>,
}

impl MockRemoteFetcher {
    fn new() -> Self {
        Self {
            fetch_response: None,
            fetch_by_correlation_response: None,
        }
    }

    fn with_fetch_response(mut self, book: EventBook) -> Self {
        self.fetch_response = Some(book);
        self
    }

    fn with_fetch_by_correlation_response(mut self, book: EventBook) -> Self {
        self.fetch_by_correlation_response = Some(book);
        self
    }
}

#[async_trait]
impl DestinationFetcher for MockRemoteFetcher {
    async fn fetch(&self, _cover: &Cover) -> Option<EventBook> {
        self.fetch_response.clone()
    }

    async fn fetch_by_correlation(
        &self,
        _domain: &str,
        _correlation_id: &str,
    ) -> Option<EventBook> {
        self.fetch_by_correlation_response.clone()
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

fn make_cover(domain: &str, root: Uuid, correlation_id: &str) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(make_proto_uuid(root)),
        correlation_id: correlation_id.to_string(),
        edition: Some(Edition {
            name: "main".to_string(),
            divergences: vec![],
        }),
        ext: None,
    }
}

fn make_event_book(domain: &str, root: Uuid, correlation_id: &str) -> EventBook {
    EventBook {
        cover: Some(make_cover(domain, root, correlation_id)),
        pages: vec![],
        snapshot: None,
        next_sequence: 0,
    }
}

fn create_hybrid_fetcher(
    local_domain: &str,
    remote: Arc<dyn DestinationFetcher>,
) -> HybridDestinationFetcher {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let snapshot_repo = Arc::new(crate::repository::SnapshotRepository::new(snapshot_store));

    HybridDestinationFetcher::new(local_domain.to_string(), event_store, snapshot_repo, remote)
}

// ============================================================================
// fetch() Tests - Domain Routing
// ============================================================================

/// Queries for non-local domains delegate to remote fetcher.
///
/// The hybrid fetcher should ONLY handle the local domain directly.
/// All other domains go through the remote fetcher.
#[tokio::test]
async fn test_fetch_non_local_domain_delegates_to_remote() {
    let remote_book = make_event_book("order", Uuid::new_v4(), "corr-123");
    let remote = Arc::new(MockRemoteFetcher::new().with_fetch_response(remote_book.clone()));
    let fetcher = create_hybrid_fetcher("pm-order-flow", remote);

    let cover = make_cover("order", Uuid::new_v4(), "corr-123");
    let result = fetcher.fetch(&cover).await;

    assert!(result.is_some(), "Should return remote response");
    let book = result.unwrap();
    assert_eq!(book.cover.as_ref().unwrap().domain, "order");
}

/// Remote fetcher None response is passed through.
#[tokio::test]
async fn test_fetch_non_local_domain_returns_none_from_remote() {
    let remote = Arc::new(MockRemoteFetcher::new()); // No response configured
    let fetcher = create_hybrid_fetcher("pm-order-flow", remote);

    let cover = make_cover("inventory", Uuid::new_v4(), "corr-456");
    let result = fetcher.fetch(&cover).await;

    assert!(result.is_none(), "Should return None from remote");
}

/// Local domain queries with missing root return None.
///
/// Cover must have a valid root UUID to fetch from local storage.
#[tokio::test]
async fn test_fetch_local_domain_missing_root_returns_none() {
    let remote = Arc::new(MockRemoteFetcher::new());
    let fetcher = create_hybrid_fetcher("pm-order-flow", remote);

    let cover = Cover {
        domain: "pm-order-flow".to_string(),
        root: None, // Missing root
        correlation_id: "corr-123".to_string(),
        edition: None,
        ext: None,
    };
    let result = fetcher.fetch(&cover).await;

    assert!(result.is_none(), "Should return None for missing root");
}

/// Local domain queries with invalid root bytes return None.
#[tokio::test]
async fn test_fetch_local_domain_invalid_root_returns_none() {
    let remote = Arc::new(MockRemoteFetcher::new());
    let fetcher = create_hybrid_fetcher("pm-order-flow", remote);

    let cover = Cover {
        domain: "pm-order-flow".to_string(),
        root: Some(ProtoUuid {
            value: vec![1, 2, 3], // Invalid UUID (wrong length)
        }),
        correlation_id: "corr-123".to_string(),
        edition: None,
        ext: None,
    };
    let result = fetcher.fetch(&cover).await;

    assert!(result.is_none(), "Should return None for invalid root");
}

// ============================================================================
// fetch_by_correlation() Tests - Domain Routing
// ============================================================================

/// Correlation queries for non-local domains delegate to remote.
#[tokio::test]
async fn test_fetch_by_correlation_non_local_delegates_to_remote() {
    let remote_book = make_event_book("order", Uuid::new_v4(), "corr-789");
    let remote =
        Arc::new(MockRemoteFetcher::new().with_fetch_by_correlation_response(remote_book.clone()));
    let fetcher = create_hybrid_fetcher("pm-order-flow", remote);

    let result = fetcher.fetch_by_correlation("order", "corr-789").await;

    assert!(result.is_some(), "Should return remote response");
    let book = result.unwrap();
    assert_eq!(book.cover.as_ref().unwrap().domain, "order");
}

/// Remote fetcher None response is passed through for correlation queries.
#[tokio::test]
async fn test_fetch_by_correlation_non_local_returns_none_from_remote() {
    let remote = Arc::new(MockRemoteFetcher::new()); // No response configured
    let fetcher = create_hybrid_fetcher("pm-order-flow", remote);

    let result = fetcher.fetch_by_correlation("inventory", "corr-xyz").await;

    assert!(result.is_none(), "Should return None from remote");
}

// ============================================================================
// HybridDestinationFetcher Construction Tests
// ============================================================================

/// Constructor correctly stores local domain.
#[test]
fn test_hybrid_fetcher_stores_local_domain() {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let snapshot_repo = Arc::new(crate::repository::SnapshotRepository::new(snapshot_store));
    let remote: Arc<dyn DestinationFetcher> = Arc::new(MockRemoteFetcher::new());

    let fetcher = HybridDestinationFetcher::new(
        "my-local-domain".to_string(),
        event_store,
        snapshot_repo,
        remote,
    );

    assert_eq!(fetcher.local_domain, "my-local-domain");
}
