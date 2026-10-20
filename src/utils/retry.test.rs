//! Tests for the retryable set and retry semantics.
//!
//! Why this matters: the contract has ONE retryable set
//! (command_handler.proto, "Status codes"): UNAVAILABLE,
//! DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED and ABORTED. Retrying a terminal
//! code (INTERNAL is a handler bug, FAILED_PRECONDITION a business
//! rejection) re-hits the same bug or rule; failing to retry a retryable
//! one dead-letters a message that would have succeeded.

use super::test_support::ALL_CODES;
use super::*;

/// The one table every retry loop and the DLQ classifier share (ST-1).
#[test]
fn test_is_retryable_matches_the_contract_table() {
    for &(code, retryable) in ALL_CODES {
        assert_eq!(is_retryable_code(code), retryable, "{code:?}");
        assert_eq!(
            is_retryable_status(&Status::new(code, "x")),
            retryable,
            "{code:?}"
        );
    }
}

/// A sequence-conflict message no longer makes FAILED_PRECONDITION
/// retryable: a stale sequence is ABORTED, and message prefixes are not
/// parsed (ST-1).
#[test]
fn test_failed_precondition_is_terminal_whatever_its_message() {
    assert!(!is_retryable_status(&Status::failed_precondition(
        "Sequence mismatch: command expects 3"
    )));
}

// ============================================================================
// Backoff Builder Tests
// ============================================================================

/// saga_backoff() configures appropriate limits for command retries.
///
/// - Fast initial retry (10ms) for quick sequence conflict resolution
/// - Cap at 2s to avoid long blocking
/// - 10 attempts for typical contention scenarios
#[test]
fn test_saga_backoff_configuration() {
    use backon::BackoffBuilder;

    let builder = saga_backoff();
    let backoff = builder.build();

    // Should produce exactly 10 delays (max_times = 10)
    let delays: Vec<_> = backoff.collect();
    assert_eq!(
        delays.len(),
        10,
        "Should have exactly 10 delays (max_times=10), got {}",
        delays.len()
    );

    // First delay should be small (around 10ms, jitter can double it)
    let first = delays[0];
    assert!(
        first >= Duration::from_millis(5) && first < Duration::from_millis(100),
        "First delay should be around 10ms, got {:?}",
        first
    );
}

/// connection_backoff() configures appropriate limits for startup retries.
///
/// - Moderate initial retry (100ms) for network operations
/// - Cap at 5s for K8s service discovery
/// - 30 attempts for ~2-3 minutes total retry time
#[test]
fn test_connection_backoff_configuration() {
    use backon::BackoffBuilder;

    let builder = connection_backoff();
    let backoff = builder.build();

    // Should produce exactly 30 delays (max_times = 30)
    let delays: Vec<_> = backoff.collect();
    assert_eq!(
        delays.len(),
        30,
        "Should have exactly 30 delays (max_times=30), got {}",
        delays.len()
    );

    // First delay should be around 100ms (jitter can vary it)
    let first = delays[0];
    assert!(
        first >= Duration::from_millis(50) && first < Duration::from_millis(500),
        "First delay should be around 100ms, got {:?}",
        first
    );
}

// ============================================================================
// RetryOutcome Tests
// ============================================================================

/// RetryOutcome::Success carries the success value.
#[test]
fn test_retry_outcome_success() {
    let outcome: RetryOutcome<i32, String> = RetryOutcome::Success(42);
    match outcome {
        RetryOutcome::Success(val) => assert_eq!(val, 42),
        _ => panic!("Expected Success"),
    }
}

/// RetryOutcome::Retryable carries the failure value.
#[test]
fn test_retry_outcome_retryable() {
    let outcome: RetryOutcome<i32, String> = RetryOutcome::Retryable("temp error".to_string());
    match outcome {
        RetryOutcome::Retryable(err) => assert_eq!(err, "temp error"),
        _ => panic!("Expected Retryable"),
    }
}

/// RetryOutcome::Fatal carries the failure value.
#[test]
fn test_retry_outcome_fatal() {
    let outcome: RetryOutcome<i32, String> = RetryOutcome::Fatal("permanent error".to_string());
    match outcome {
        RetryOutcome::Fatal(err) => assert_eq!(err, "permanent error"),
        _ => panic!("Expected Fatal"),
    }
}
