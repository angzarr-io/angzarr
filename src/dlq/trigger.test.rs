//! Tests for [`CodeDlqExt::classify_for_dlq`] -- the single source of truth
//! for "should this handler failure publish to the DLQ now, or fall back to
//! the retry loop and DLQ only on exhaustion?"
//!
//! Why this matters: saga / PM / projector / aggregate all call into this
//! one helper. A misclassification in either direction is a foot-gun:
//!
//! - Treating a permanent error as transient burns the retry budget on a
//!   guaranteed-failing operation and adds latency to the eventual DLQ
//!   entry.
//! - Treating a transient error as permanent DLQ-s a message that would
//!   have succeeded on retry, surfacing operator noise.
//!
//! The bucketing mirrors gRPC's canonical retryability guidance (HTTP
//! 4xx-class vs 5xx-class) so the contract is "use the standard gRPC
//! retryability bucket, plus three explicit choices for the codes the
//! standard leaves ambiguous." Coverage is exhaustive over all 17
//! `tonic::Code` variants -- a future tonic upgrade that adds a variant
//! must update both the impl `match` and the tests here. That mutual
//! dependency is intentional.

use super::*;
use tonic::Code;

// ---------------------------------------------------------------------------
// Permanent (4xx-class) -- DlqTrigger::Immediate
// ---------------------------------------------------------------------------
//
// All 9 cases below skip the retry loop and publish to the DLQ on the first
// failure. Eight come directly from the round-2 plan; AlreadyExists is the
// CQRS-ES-specific addition (an aggregate returning AlreadyExists usually
// means the command was already processed, so retrying re-hits the same
// idempotency conflict and just wastes budget).

#[test]
fn invalid_argument_is_immediate() {
    // Client sent malformed args; retry won't sanitize them.
    assert_eq!(
        Code::InvalidArgument.classify_for_dlq(),
        DlqTrigger::Immediate(Immediate(Code::InvalidArgument)),
    );
}

#[test]
fn not_found_is_immediate() {
    // Target entity missing; retry won't conjure it.
    assert_eq!(
        Code::NotFound.classify_for_dlq(),
        DlqTrigger::Immediate(Immediate(Code::NotFound)),
    );
}

#[test]
fn failed_precondition_is_immediate() {
    // System not in the required state; retry without external state change
    // will fail identically.
    assert_eq!(
        Code::FailedPrecondition.classify_for_dlq(),
        DlqTrigger::Immediate(Immediate(Code::FailedPrecondition)),
    );
}

#[test]
fn aborted_is_immediate() {
    // Concurrency conflict / sequence mismatch -- the source of truth has
    // moved; the operator needs to look at the dead letter to decide
    // whether to discard, replay, or fix.
    assert_eq!(
        Code::Aborted.classify_for_dlq(),
        DlqTrigger::Immediate(Immediate(Code::Aborted)),
    );
}

#[test]
fn unimplemented_is_immediate() {
    // Server doesn't know how to handle this method; retry against the
    // same server is a guaranteed re-rejection.
    assert_eq!(
        Code::Unimplemented.classify_for_dlq(),
        DlqTrigger::Immediate(Immediate(Code::Unimplemented)),
    );
}

#[test]
fn permission_denied_is_immediate() {
    // AuthZ failure; the caller's permissions won't change between retries
    // within the same flow.
    assert_eq!(
        Code::PermissionDenied.classify_for_dlq(),
        DlqTrigger::Immediate(Immediate(Code::PermissionDenied)),
    );
}

#[test]
fn unauthenticated_is_immediate() {
    // Bad credentials; same reasoning as PermissionDenied.
    assert_eq!(
        Code::Unauthenticated.classify_for_dlq(),
        DlqTrigger::Immediate(Immediate(Code::Unauthenticated)),
    );
}

#[test]
fn out_of_range_is_immediate() {
    // Argument out of valid range; retry with the same argument fails
    // identically.
    assert_eq!(
        Code::OutOfRange.classify_for_dlq(),
        DlqTrigger::Immediate(Immediate(Code::OutOfRange)),
    );
}

#[test]
fn already_exists_is_immediate() {
    // CQRS-ES idempotency-conflict signal: the aggregate has already
    // recorded a command with this identity. Retrying re-hits the same
    // conflict; the operator's dead-letter review is where the duplicate
    // gets sorted (real conflict vs duplicate delivery artifact).
    assert_eq!(
        Code::AlreadyExists.classify_for_dlq(),
        DlqTrigger::Immediate(Immediate(Code::AlreadyExists)),
    );
}

// ---------------------------------------------------------------------------
// Transient (5xx-class) -- DlqTrigger::RetryThenDlq
// ---------------------------------------------------------------------------
//
// All 8 cases below run the existing retry loop first. Six are the
// canonical "server is unhappy, retry might help" gRPC codes; Ok is the
// defensive arm; Cancelled is shutdown/upstream cancellation.

#[test]
fn unavailable_is_retry_then_dlq() {
    // Service is down; back off and retry per the gRPC retryability
    // standard.
    assert_eq!(
        Code::Unavailable.classify_for_dlq(),
        DlqTrigger::RetryThenDlq(RetryThenDlq(Code::Unavailable)),
    );
}

#[test]
fn deadline_exceeded_is_retry_then_dlq() {
    // Timer expired; the next attempt may complete inside its deadline.
    assert_eq!(
        Code::DeadlineExceeded.classify_for_dlq(),
        DlqTrigger::RetryThenDlq(RetryThenDlq(Code::DeadlineExceeded)),
    );
}

#[test]
fn resource_exhausted_is_retry_then_dlq() {
    // Quota / OOM / rate limit; retry-with-backoff is the canonical
    // recovery.
    assert_eq!(
        Code::ResourceExhausted.classify_for_dlq(),
        DlqTrigger::RetryThenDlq(RetryThenDlq(Code::ResourceExhausted)),
    );
}

#[test]
fn internal_is_retry_then_dlq() {
    // Server-side invariant broken; the next attempt may hit a healthy
    // replica or a transient bug that has since cleared.
    assert_eq!(
        Code::Internal.classify_for_dlq(),
        DlqTrigger::RetryThenDlq(RetryThenDlq(Code::Internal)),
    );
}

#[test]
fn unknown_is_retry_then_dlq() {
    // Error not mapped to a specific code; treat as transient until we
    // know it isn't. Surfacing as Immediate would risk silently swallowing
    // a recoverable failure.
    assert_eq!(
        Code::Unknown.classify_for_dlq(),
        DlqTrigger::RetryThenDlq(RetryThenDlq(Code::Unknown)),
    );
}

#[test]
fn data_loss_is_retry_then_dlq() {
    // gRPC's DataLoss signals unrecoverable corruption -- yet the standard
    // retryability table still buckets it transient (the retry might land
    // on an uncorrupted replica). We follow the standard rather than
    // diverge.
    assert_eq!(
        Code::DataLoss.classify_for_dlq(),
        DlqTrigger::RetryThenDlq(RetryThenDlq(Code::DataLoss)),
    );
}

#[test]
fn ok_is_retry_then_dlq_defensive() {
    // Ok should never reach this helper -- the caller is expected to
    // branch on success first. The defensive arm returns RetryThenDlq so
    // that if a future refactor mistake routes Ok through here, the retry
    // loop sees a successful result on the first attempt and exits without
    // publishing a phantom dead letter. Returning Immediate here would
    // surface a phantom DLQ entry for a successful handler call.
    assert_eq!(
        Code::Ok.classify_for_dlq(),
        DlqTrigger::RetryThenDlq(RetryThenDlq(Code::Ok)),
    );
}

#[test]
fn cancelled_is_retry_then_dlq() {
    // Caller cancellation, typically shutdown or upstream. The next event
    // loop tick after the framework re-establishes its caller chain will
    // succeed; permanent classification would DLQ messages that the
    // operator never wanted to inspect.
    assert_eq!(
        Code::Cancelled.classify_for_dlq(),
        DlqTrigger::RetryThenDlq(RetryThenDlq(Code::Cancelled)),
    );
}
