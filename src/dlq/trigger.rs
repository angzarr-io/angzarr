//! Classification of handler `tonic::Status` values into DLQ trigger buckets.
//!
//! Single source of truth for "should this failure DLQ now, or run the retry
//! loop first?" Used by saga / PM / projector / aggregate handler runtimes
//! (R2-15 in the round-2 review plan).
//!
//! Bucketing follows gRPC's canonical retryability guidance:
//!
//! - 4xx-class (permanent) `Code` variants -> [`DlqTrigger::Immediate`].
//!   Retrying a permanent error wastes the retry budget on a
//!   guaranteed-failing operation and adds latency to the eventual DLQ
//!   entry, so we skip retries and publish now.
//! - 5xx-class (transient) `Code` variants -> [`DlqTrigger::RetryThenDlq`].
//!   The framework's existing retry loop handles the recovery; on
//!   exhaustion the caller publishes to the DLQ.
//!
//! Three `Code` variants are outside the standard retryability table and
//! decided here explicitly: `Ok` -> RetryThenDlq (defensive -- Ok should
//! not reach this helper, but if it does the retry loop's success check
//! catches it without publishing a phantom dead letter); `Cancelled` ->
//! RetryThenDlq (shutdown / upstream cancellation is recovered by the
//! next event loop tick); `AlreadyExists` -> Immediate (in CQRS-ES this
//! usually means an idempotency conflict, where retrying re-hits the
//! same conflict).

use tonic::Code;

/// A permanent failure classification. Carries the originating `Code` so
/// downstream consumers can include it in the dead-letter payload without
/// re-deriving it. Type-level evidence that a status was classified
/// permanent: a `fn(Immediate)` can only be called with a permanent
/// classification, removing a class of "did we already retry this?"
/// foot-guns at the call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Immediate(pub Code);

/// A transient failure classification. Same role as [`Immediate`] but
/// flags the status as one the retry loop should attempt before any
/// DLQ publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryThenDlq(pub Code);

/// Outcome of classifying a handler `Code` for DLQ disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DlqTrigger {
    /// Permanent error -- publish to the DLQ now, skip retries.
    Immediate(Immediate),
    /// Transient error -- run the existing retry loop; publish to the
    /// DLQ only on exhaustion. Also the defensive arm for `Code::Ok`.
    RetryThenDlq(RetryThenDlq),
}

/// Extension trait adding DLQ classification onto `tonic::Code`. Keeps
/// call sites reading as `status.code().classify_for_dlq()` -- the
/// `tonic::Code` enum is external so a free-standing inherent method
/// would be orphan-rule disallowed. Matches the codebase's
/// proto-extension-trait convention.
pub trait CodeDlqExt {
    /// Classify this gRPC `Code` into its DLQ disposition.
    fn classify_for_dlq(self) -> DlqTrigger;
}

impl CodeDlqExt for Code {
    fn classify_for_dlq(self) -> DlqTrigger {
        match self {
            Code::InvalidArgument
            | Code::NotFound
            | Code::FailedPrecondition
            | Code::Aborted
            | Code::Unimplemented
            | Code::PermissionDenied
            | Code::Unauthenticated
            | Code::OutOfRange
            | Code::AlreadyExists => DlqTrigger::Immediate(Immediate(self)),
            Code::Unavailable
            | Code::DeadlineExceeded
            | Code::ResourceExhausted
            | Code::Internal
            | Code::Unknown
            | Code::DataLoss
            | Code::Cancelled
            | Code::Ok => DlqTrigger::RetryThenDlq(RetryThenDlq(self)),
        }
    }
}

#[cfg(test)]
#[path = "trigger.test.rs"]
mod tests;
