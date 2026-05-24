//! Tests for GrpcDestinationFetcher.
//!
//! The fetcher retrieves aggregate state from remote EventQueryService.
//! Used by sagas/PMs to get destination state before sending commands.
//!
//! Key behaviors:
//! - Cover validation: root is required for fetch_result
//! - Domain routing: fetches are routed by domain name
//! - Missing domain: returns error/None
//! - Error handling: fetch() returns None on any error

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
/// Missing root is a malformed request, not a missing aggregate.
#[tokio::test]
async fn test_fetch_result_missing_root_returns_invalid_argument() {
    let fetcher = GrpcDestinationFetcher::new(HashMap::new());
    let cover = make_cover("orders", false);

    let result = fetcher.fetch_result(&cover).await;

    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(status
        .message()
        .contains(crate::orchestration::errmsg::COVER_MISSING_ROOT));
}

/// Valid cover but no client returns NOT_FOUND.
///
/// Domain routing fails when no EventQueryService is registered.
/// This is a configuration error, not a missing aggregate.
#[tokio::test]
async fn test_fetch_result_no_client_returns_not_found() {
    let fetcher = GrpcDestinationFetcher::new(HashMap::new());
    let cover = make_cover("orders", true);

    let result = fetcher.fetch_result(&cover).await;

    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert!(status.message().contains(errmsg::NO_EVENT_QUERY_FOR_DOMAIN));
    assert!(status.message().contains("orders"));
}

// ============================================================================
// DestinationFetcher Trait Implementation Tests
// ============================================================================

/// fetch() returns None when fetch_result fails.
///
/// The trait method converts errors to None for simpler caller logic.
/// Errors are logged internally.
#[tokio::test]
async fn test_fetch_returns_none_on_error() {
    let fetcher = GrpcDestinationFetcher::new(HashMap::new());
    let cover = make_cover("orders", true);

    let result = fetcher.fetch(&cover).await;

    assert!(result.is_none());
}

/// fetch_by_correlation returns None when no client is registered.
///
/// Unlike fetch_result, this uses early return (?) instead of error.
#[tokio::test]
async fn test_fetch_by_correlation_no_client_returns_none() {
    let fetcher = GrpcDestinationFetcher::new(HashMap::new());

    let result = fetcher.fetch_by_correlation("orders", "corr-123").await;

    assert!(result.is_none());
}
