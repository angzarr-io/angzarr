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
//! - O9 error contract: fetch FAILURES surface as Err, never as Ok(None) —
//!   Ok(None) is reserved for "the store answered and holds no state"

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
    error: Option<Status>,
}

impl MockRemoteFetcher {
    fn new() -> Self {
        Self {
            fetch_response: None,
            fetch_by_correlation_response: None,
            error: None,
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

    fn with_error(mut self, status: Status) -> Self {
        self.error = Some(status);
        self
    }
}

#[async_trait]
impl DestinationFetcher for MockRemoteFetcher {
    async fn fetch(&self, _cover: &Cover) -> Result<Option<EventBook>, Status> {
        if let Some(e) = &self.error {
            return Err(e.clone());
        }
        Ok(self.fetch_response.clone())
    }

    async fn fetch_by_correlation(
        &self,
        _domain: &str,
        _correlation_id: &str,
    ) -> Result<Option<EventBook>, Status> {
        if let Some(e) = &self.error {
            return Err(e.clone());
        }
        Ok(self.fetch_by_correlation_response.clone())
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

/// Same as `create_hybrid_fetcher` but hands back the event store so a test
/// can inject storage failures (O9).
fn create_hybrid_fetcher_with_store(
    local_domain: &str,
    remote: Arc<dyn DestinationFetcher>,
) -> (HybridDestinationFetcher, Arc<MockEventStore>) {
    let event_store = Arc::new(MockEventStore::new());
    let snapshot_store = Arc::new(MockSnapshotStore::new());
    let snapshot_repo = Arc::new(crate::repository::SnapshotRepository::new(snapshot_store));

    let fetcher = HybridDestinationFetcher::new(
        local_domain.to_string(),
        event_store.clone(),
        snapshot_repo,
        remote,
    );
    (fetcher, event_store)
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

    let book = result
        .expect("remote fetch should succeed")
        .expect("Should return remote response");
    assert_eq!(book.cover.as_ref().unwrap().domain, "order");
}

/// Remote fetcher Ok(None) ("no state") response is passed through.
#[tokio::test]
async fn test_fetch_non_local_domain_returns_none_from_remote() {
    let remote = Arc::new(MockRemoteFetcher::new()); // No response configured
    let fetcher = create_hybrid_fetcher("pm-order-flow", remote);

    let cover = make_cover("inventory", Uuid::new_v4(), "corr-456");
    let result = fetcher.fetch(&cover).await;

    assert!(
        result.expect("remote fetch should succeed").is_none(),
        "Should return Ok(None) from remote"
    );
}

/// O9: remote fetch ERRORS propagate through the hybrid — a transport blip
/// on another domain's query must not be presented as "no state".
#[tokio::test]
async fn test_fetch_non_local_domain_propagates_remote_error() {
    let remote =
        Arc::new(MockRemoteFetcher::new().with_error(Status::unavailable("connection refused")));
    let fetcher = create_hybrid_fetcher("pm-order-flow", remote);

    let cover = make_cover("order", Uuid::new_v4(), "corr-123");
    let result = fetcher.fetch(&cover).await;

    let status = result.expect_err("remote error must propagate, not become Ok(None)");
    assert_eq!(status.code(), tonic::Code::Unavailable);
}

/// Local domain queries with missing root are INVALID_ARGUMENT.
///
/// Cover must have a valid root UUID to fetch from local storage. A
/// malformed request is an error, not evidence of absent state (O9).
#[tokio::test]
async fn test_fetch_local_domain_missing_root_is_invalid_argument() {
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

    let status = result.expect_err("missing root must be an error, not 'no state'");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

/// Local domain queries with invalid root bytes are INVALID_ARGUMENT.
#[tokio::test]
async fn test_fetch_local_domain_invalid_root_is_invalid_argument() {
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

    let status = result.expect_err("invalid root must be an error, not 'no state'");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

/// O9 (the defect's local flavor): a local STORAGE failure surfaces as Err.
/// Pre-fix it was mapped to None and the PM restarted the workflow from
/// empty even though its state was sitting in the store.
#[tokio::test]
async fn test_fetch_local_domain_storage_error_is_err_not_none() {
    let remote = Arc::new(MockRemoteFetcher::new());
    let (fetcher, event_store) = create_hybrid_fetcher_with_store("pm-order-flow", remote);
    event_store.set_fail_on_get(true).await;

    let cover = make_cover("pm-order-flow", Uuid::new_v4(), "corr-123");
    let result = fetcher.fetch(&cover).await;

    let status = result.expect_err(
        "a storage failure must surface as Err — Ok(None) would be read as \
         'no state' and restart the workflow (O9)",
    );
    assert_eq!(status.code(), tonic::Code::Internal);
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

    let book = result
        .expect("remote fetch should succeed")
        .expect("Should return remote response");
    assert_eq!(book.cover.as_ref().unwrap().domain, "order");
}

/// Remote fetcher Ok(None) response is passed through for correlation queries.
#[tokio::test]
async fn test_fetch_by_correlation_non_local_returns_none_from_remote() {
    let remote = Arc::new(MockRemoteFetcher::new()); // No response configured
    let fetcher = create_hybrid_fetcher("pm-order-flow", remote);

    let result = fetcher.fetch_by_correlation("inventory", "corr-xyz").await;

    assert!(
        result.expect("remote fetch should succeed").is_none(),
        "Should return Ok(None) from remote"
    );
}

/// O9: remote correlation-fetch ERRORS propagate through the hybrid.
#[tokio::test]
async fn test_fetch_by_correlation_non_local_propagates_remote_error() {
    let remote =
        Arc::new(MockRemoteFetcher::new().with_error(Status::unavailable("connection refused")));
    let fetcher = create_hybrid_fetcher("pm-order-flow", remote);

    let result = fetcher.fetch_by_correlation("order", "corr-789").await;

    let status = result.expect_err("remote error must propagate, not become Ok(None)");
    assert_eq!(status.code(), tonic::Code::Unavailable);
}

/// O9 regression guard: a local correlation lookup that finds NOTHING is the
/// one genuine "no state" case — Ok(None), a brand-new workflow. Error
/// propagation must not turn absence into failure.
#[tokio::test]
async fn test_fetch_by_correlation_local_no_state_is_ok_none() {
    let remote = Arc::new(MockRemoteFetcher::new());
    let fetcher = create_hybrid_fetcher("pm-order-flow", remote);

    let result = fetcher
        .fetch_by_correlation("pm-order-flow", "corr-new-workflow")
        .await;

    assert!(
        result
            .expect("an empty store is not an error — the lookup succeeded")
            .is_none(),
        "no matching local state means Ok(None): a genuinely new workflow"
    );
}

/// A correlation_id spans domains: `get_by_correlation` returns books from
/// EVERY domain participating in the workflow (order, inventory, the PM
/// itself, ...). The domain filter in the `find` is what stops the PM from
/// adopting another domain's aggregate as its own state — if the filter
/// inverted (`!=`), the PM would re-fetch by the WRONG root and come back
/// with an empty book for a live workflow.
///
/// Seeds one wrong-domain book and one local-domain book under the same
/// correlation (one each, so the pick is deterministic regardless of store
/// iteration order) and asserts the LOCAL book — identified by its root and
/// pages — is the one returned.
#[tokio::test]
async fn test_fetch_by_correlation_local_selects_own_domain_among_mixed_correlation() {
    use crate::storage::AddMeta;
    use crate::test_utils::make_event_page;

    let remote = Arc::new(MockRemoteFetcher::new());
    let (fetcher, event_store) = create_hybrid_fetcher_with_store("pm-order-flow", remote);

    let wrong_domain_root = Uuid::new_v4();
    let local_root = Uuid::new_v4();
    let meta = AddMeta {
        correlation_id: "corr-mix",
        ..Default::default()
    };
    // Wrong-domain book sharing the correlation (seeded first).
    event_store
        .add(
            "order",
            "",
            wrong_domain_root,
            vec![make_event_page(0)],
            &meta,
        )
        .await
        .expect("seed wrong-domain book");
    // The PM's own state under the same correlation.
    event_store
        .add(
            "pm-order-flow",
            "",
            local_root,
            vec![make_event_page(0), make_event_page(1)],
            &meta,
        )
        .await
        .expect("seed local-domain book");

    let book = fetcher
        .fetch_by_correlation("pm-order-flow", "corr-mix")
        .await
        .expect("lookup succeeds")
        .expect("local state exists for this correlation");

    let cover = book.cover.as_ref().expect("fetched book carries a cover");
    assert_eq!(
        cover.root.as_ref().map(|r| r.value.clone()),
        Some(local_root.as_bytes().to_vec()),
        "must re-fetch by the LOCAL domain book's root, not the other domain's"
    );
    assert_eq!(
        book.pages.len(),
        2,
        "must return the local book's pages — the wrong root would find nothing \
         in the local store and silently present a live workflow as empty"
    );
    assert_eq!(
        cover.correlation_id, "corr-mix",
        "the in-flight correlation must be preserved on the result"
    );
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
