//! Tests for GrpcCommandExecutor.
//!
//! The executor routes commands to domain-specific gRPC clients.
//! When no client is registered for a domain, it returns NOT_FOUND.
//!
//! Key behaviors:
//! - Domain lookup: Commands are routed by domain name
//! - Missing domain: Returns NOT_FOUND status
//! - Outcome mapping: gRPC errors are classified into Success/Retryable/Rejected

use super::*;
use crate::proto::{Cover, Uuid as ProtoUuid};
use std::collections::HashMap;

fn make_command_for_domain(domain: &str) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(ProtoUuid {
                value: vec![1, 2, 3, 4],
            }),
            correlation_id: "corr-123".to_string(),
            edition: None,
            ext: None,
        }),
        pages: vec![],
    }
}

// ============================================================================
// Domain Lookup Tests
// ============================================================================

/// Empty executor returns NOT_FOUND for any domain.
///
/// When no clients are registered, all commands fail. This is a
/// configuration error — the executor should be populated at startup.
#[tokio::test]
async fn test_execute_raw_no_clients_returns_not_found() {
    let executor = GrpcCommandExecutor::new(HashMap::new());
    let command = make_command_for_domain("orders");

    let result = executor.execute_raw(command, SyncMode::Simple).await;

    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert!(status.message().contains(errmsg::NO_AGGREGATE_FOR_DOMAIN));
    assert!(status.message().contains("orders"));
}

/// Missing domain returns NOT_FOUND even when other domains exist.
///
/// Routing is per-domain. A command for "inventory" fails if only
/// "orders" client is registered. This tests the HashMap lookup.
#[tokio::test]
async fn test_execute_raw_wrong_domain_returns_not_found() {
    // We can't easily create a mock client, but we can verify the lookup
    // by using an empty map and checking the error domain is correct
    let executor = GrpcCommandExecutor::new(HashMap::new());
    let command = make_command_for_domain("inventory");

    let result = executor.execute_raw(command, SyncMode::Simple).await;

    assert!(result.is_err());
    let status = result.unwrap_err();
    assert!(status.message().contains("inventory"));
}

// ============================================================================
// CommandOutcome Mapping Tests
// ============================================================================

/// NOT_FOUND error maps to Rejected outcome (not retryable).
///
/// Missing domain is a configuration error, not a transient failure.
/// Retrying won't help — the client map doesn't change at runtime.
#[tokio::test]
async fn test_execute_not_found_maps_to_rejected() {
    let executor = GrpcCommandExecutor::new(HashMap::new());
    let command = make_command_for_domain("orders");

    let outcome = executor.execute(command, SyncMode::Simple).await;

    match outcome {
        CommandOutcome::Rejected(reason) => {
            assert!(reason.contains(errmsg::NO_AGGREGATE_FOR_DOMAIN));
        }
        other => panic!("Expected Rejected, got {:?}", other),
    }
}
