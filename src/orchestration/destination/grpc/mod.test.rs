//! Tests for GrpcDestinationFetcher.
//!
//! The fetcher retrieves aggregate state from remote EventQueryService.
//! Used by sagas/PMs to get destination state before sending commands.
//!
//! Key behaviors:
//! - Cover validation: root is required for fetch
//! - Domain routing: fetches are routed by domain name
//! - Error handling (O9): failures are `Err(Status)`, never `Ok(None)` —
//!   `Ok(None)` would be read by callers as "no state / new workflow"

use super::*;
use crate::proto::Uuid as ProtoUuid;
use std::collections::HashMap;

fn make_cover(domain: &str, with_root: bool) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: if with_root {
            Some(ProtoUuid {
                value: vec![1, 2, 3, 4],
            })
        } else {
            None
        },
        correlation_id: "corr-123".to_string(),
        edition: None,
        ext: None,
    }
}

// ============================================================================
// Input Validation Tests
// ============================================================================

/// Cover without root returns INVALID_ARGUMENT.
///
/// The root UUID is required to identify which aggregate to fetch.
/// Missing root is a malformed request, not a missing aggregate — it must
/// surface as Err, not Ok(None) (O9).
#[tokio::test]
async fn test_fetch_missing_root_returns_invalid_argument() {
    let fetcher = GrpcDestinationFetcher::new(HashMap::new());
    let cover = make_cover("orders", false);

    let result = fetcher.fetch(&cover).await;

    let status = result.expect_err("missing root must be an error, not 'no state'");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(status
        .message()
        .contains(crate::orchestration::errmsg::COVER_MISSING_ROOT));
}

/// Valid cover but no client returns NOT_FOUND.
///
/// Domain routing fails when no EventQueryService is registered.
/// This is a configuration error, not a missing aggregate — pre-O9 it was
/// swallowed to None and callers restarted workflows from empty.
#[tokio::test]
async fn test_fetch_no_client_returns_not_found() {
    let fetcher = GrpcDestinationFetcher::new(HashMap::new());
    let cover = make_cover("orders", true);

    let result = fetcher.fetch(&cover).await;

    let status = result.expect_err("missing client must be an error, not 'no state'");
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert!(status.message().contains(errmsg::NO_EVENT_QUERY_FOR_DOMAIN));
    assert!(status.message().contains("orders"));
}

/// fetch_by_correlation with no registered client returns NOT_FOUND.
///
/// O9 regression guard: this method used to early-return None on a missing
/// client, which saga Phase-1 read as "destination doesn't exist yet →
/// sequence 0" and PMs read as "brand-new workflow". A client we never
/// connected to is a failure to fetch, not evidence of absence.
#[tokio::test]
async fn test_fetch_by_correlation_no_client_returns_not_found() {
    let fetcher = GrpcDestinationFetcher::new(HashMap::new());

    let result = fetcher.fetch_by_correlation("orders", "corr-123").await;

    let status = result.expect_err("missing client must be an error, not 'no state'");
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert!(status.message().contains(errmsg::NO_EVENT_QUERY_FOR_DOMAIN));
    assert!(status.message().contains("orders"));
}
