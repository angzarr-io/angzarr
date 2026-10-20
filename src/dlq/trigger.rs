//! Classification of handler `tonic::Status` values into DLQ trigger buckets.
//!
//! Single source of truth for "should this failure DLQ now, or run the retry
//! loop first?" Used by saga / PM / projector / aggregate handler runtimes
//! (R2-15 in the round-2 review plan).
//!
//! Bucketing follows the one retryable set of the contract
//! (command_handler.proto, "Status codes"; backlog ST-1), shared with every
//! framework retry loop through [`crate::utils::retry::is_retryable_code`]:
//!
//! - retryable codes (UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED,
//!   ABORTED) -> [`DlqTrigger::RetryThenDlq`]: retry, then dead-letter on
//!   exhaustion;
//! - every other code (terminal) -> [`DlqTrigger::Immediate`]: dead-letter now.
//!
//! `Ok` is the one exception: it should never reach this helper, and if it
//! does, the retry loop's success check handles it without publishing a
//! phantom dead letter, so it is bucketed RetryThenDlq.

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
        if self == Code::Ok || crate::utils::retry::is_retryable_code(self) {
            DlqTrigger::RetryThenDlq(RetryThenDlq(self))
        } else {
            DlqTrigger::Immediate(Immediate(self))
        }
    }
}

#[cfg(test)]
#[path = "trigger.test.rs"]
mod tests;
