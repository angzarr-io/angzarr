//! Tests for gRPC error classification and retry semantics.
//!
//! The retry system distinguishes between:
//! - **Retryable errors**: 5xx-class transient codes plus sequence-conflict
//!   `FailedPrecondition`. The handler can backoff and retry; on retry
//!   exhaustion the caller publishes to the DLQ.
//! - **Non-retryable errors**: 4xx-class permanent codes plus
//!   non-sequence-conflict `FailedPrecondition` (business guard
//!   rejections). The DLQ layer takes the entry immediately.
//!
//! Why this matters: classification governs both the retry loop AND
//! whether a failure becomes a dead letter immediately vs. after retry
//! exhaustion. The classifier stays aligned with
//! `crate::dlq::trigger::CodeDlqExt::classify_for_dlq` so the four
//! handler types (aggregate, saga, PM, projector) share one operator
//! contract per R2-15 decision #2.

use super::*;

/// Exhaustive: every 5xx-class transient code is retryable.
///
/// R2-15 decision #2 retry list — these are the codes where backoff
/// has a meaningful chance of succeeding on the next attempt. Each
/// arm is asserted independently so a regression on one code is
/// localized in the failure.
#[test]
fn test_is_retryable_5xx_class_codes_are_all_retryable() {
    assert!(
        is_retryable_status(&Status::unavailable("broker down")),
        "Unavailable should be retryable (transient broker issue)"
    );
    assert!(
        is_retryable_status(&Status::deadline_exceeded("timed out")),
        "DeadlineExceeded should be retryable (transient slow path)"
    );
    assert!(
        is_retryable_status(&Status::resource_exhausted("rate limited")),
        "ResourceExhausted should be retryable (backpressure)"
    );
    assert!(
        is_retryable_status(&Status::internal("Internal error")),
        "Internal should be retryable (server-side fault may be transient)"
    );
    assert!(
        is_retryable_status(&Status::unknown("unknown")),
        "Unknown should be retryable (no information either way → retry)"
    );
    assert!(
        is_retryable_status(&Status::data_loss("data loss")),
        "DataLoss should be retryable (storage transient hiccup)"
    );
    assert!(
        is_retryable_status(&Status::cancelled("client cancel")),
        "Cancelled should be retryable (upstream / shutdown recovers next tick)"
    );
}

/// FailedPrecondition is retryable ONLY for sequence conflicts.
///
/// FailedPrecondition is overloaded in this codebase:
/// - sequence-conflict messages from `single_sequence_check` /
///   `storage::error::SEQUENCE_CONFLICT` are transient races and should
///   retry with fresh state (event-sourcing idempotency makes this safe);
/// - business-guard rejections from aggregate handlers (e.g.
///   "Hand already dealt") are permanent — refresh just re-hits the
///   guard. These must NOT retry.
#[test]
fn test_is_retryable_failed_precondition_only_for_sequence_messages() {
    // Sequence mismatch (STRICT/COMMUTATIVE merge) is retryable.
    assert!(is_retryable_status(&Status::failed_precondition(
        "Sequence mismatch: command expects 0, aggregate at 5"
    )));

    // Storage-level sequence conflict is retryable.
    assert!(is_retryable_status(&Status::failed_precondition(
        "Sequence conflict: expected 0, got 3"
    )));

    // Business-guard FailedPrecondition is NOT retryable.
    assert!(!is_retryable_status(&Status::failed_precondition(
        "Hand already dealt"
    )));
    assert!(!is_retryable_status(&Status::failed_precondition(
        "Player does not exist"
    )));
}

/// Exhaustive: every 4xx-class permanent code is non-retryable.
///
/// These all map to `DlqTrigger::Immediate` — they require human
/// intervention or operator/code fixes, not automated retry. Asserted
/// per-code so a regression on any single arm is localized.
#[test]
fn test_is_retryable_4xx_class_codes_are_all_non_retryable() {
    assert!(
        !is_retryable_status(&Status::invalid_argument("bad arg")),
        "InvalidArgument should NOT be retryable (caller bug)"
    );
    assert!(
        !is_retryable_status(&Status::not_found("missing")),
        "NotFound should NOT be retryable (resource doesn't exist)"
    );
    assert!(
        !is_retryable_status(&Status::aborted("DLQ for manual review")),
        "Aborted should NOT be retryable (MERGE_MANUAL routing)"
    );
    assert!(
        !is_retryable_status(&Status::unimplemented("missing handler")),
        "Unimplemented should NOT be retryable (code fix required)"
    );
    assert!(
        !is_retryable_status(&Status::permission_denied("denied")),
        "PermissionDenied should NOT be retryable (authz)"
    );
    assert!(
        !is_retryable_status(&Status::unauthenticated("no creds")),
        "Unauthenticated should NOT be retryable (auth)"
    );
    assert!(
        !is_retryable_status(&Status::out_of_range("out of range")),
        "OutOfRange should NOT be retryable (caller bug)"
    );
    assert!(
        !is_retryable_status(&Status::already_exists("dup")),
        "AlreadyExists should NOT be retryable (idempotency conflict)"
    );
}

/// `is_retryable_status` MUST stay aligned with
/// `classify_for_dlq`'s transient bucket, with one documented exception:
/// `FailedPrecondition`. The DLQ classifier sends ALL
/// `FailedPrecondition` to `Immediate`; the retry classifier sends
/// sequence-conflict messages to retryable. This alignment test pins
/// the invariant — if a future change adds a new `tonic::Code` variant
/// or moves an existing one between the buckets, exactly one of the
/// two classifiers must change to match the other, and this test will
/// catch the drift.
#[test]
fn test_is_retryable_status_aligned_with_classify_for_dlq() {
    use crate::dlq::trigger::{CodeDlqExt, DlqTrigger};

    // Codes whose retry-classification can be inferred from
    // classify_for_dlq alone (i.e. all codes EXCEPT FailedPrecondition,
    // which depends on message content and is tested separately above).
    let codes_no_message_dependence: &[Code] = &[
        Code::Ok,
        Code::Cancelled,
        Code::Unknown,
        Code::InvalidArgument,
        Code::DeadlineExceeded,
        Code::NotFound,
        Code::AlreadyExists,
        Code::PermissionDenied,
        Code::ResourceExhausted,
        Code::Aborted,
        Code::OutOfRange,
        Code::Unimplemented,
        Code::Internal,
        Code::Unavailable,
        Code::DataLoss,
        Code::Unauthenticated,
    ];

    for code in codes_no_message_dependence {
        let status = Status::new(*code, "test");
        let expected_retry = matches!(code.classify_for_dlq(), DlqTrigger::RetryThenDlq(_))
            // Defensive carve-out: classify_for_dlq sends Ok to RetryThenDlq
            // (so a phantom-success can't slip through as a dead letter),
            // but Ok is not retryable from is_retryable_status' perspective
            // — a successful response shouldn't reach this helper, and if
            // it did we want false.
            && *code != Code::Ok;
        assert_eq!(
            is_retryable_status(&status),
            expected_retry,
            "drift for {:?}: classify_for_dlq says {:?}, is_retryable_status says {}",
            code,
            code.classify_for_dlq(),
            is_retryable_status(&status),
        );
    }
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
