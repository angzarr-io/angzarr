//! Retry utilities: backoff builders and retryable error classification.
//!
//! DOC: This file is referenced in docs/docs/operations/error-recovery.mdx
//!      Update documentation when making changes to retry patterns.
//!
//! Uses `backon` for exponential backoff with jitter. Provides standard
//! backoff configurations for saga/PM command retries and gRPC connection retries.

use std::time::Duration;

use async_trait::async_trait;
use backon::{BackoffBuilder, ExponentialBuilder};
use tonic::{Code, Status};
use tracing::{error, warn};

// ============================================================================
// Logging Helpers
// ============================================================================

/// Log a retryable failure with backoff delay.
///
/// Used by both `run_with_retry` and manual retry loops (PM orchestration).
pub fn log_retry_attempt(
    operation: &str,
    attempt: u32,
    error: &impl std::fmt::Display,
    delay: std::time::Duration,
) {
    warn!(
        operation = %operation,
        attempt = attempt,
        error = %error,
        delay = ?delay,
        "Operation failed, retrying after backoff"
    );
}

/// Log when retry limit is exhausted.
pub fn log_retry_exhausted(operation: &str, attempts: u32, error: &impl std::fmt::Display) {
    error!(
        operation = %operation,
        attempts = attempts,
        error = %error,
        "Operation failed and retry limit exhausted"
    );
}

/// Log a fatal (non-retryable) error.
pub fn log_fatal_error(operation: &str, attempt: u32, error: &impl std::fmt::Display) {
    error!(
        operation = %operation,
        attempt = attempt,
        error = %error,
        "Operation failed with fatal error"
    );
}

// ============================================================================
// Backoff Builders
// ============================================================================

/// Standard backoff for saga/PM command retries (sequence conflicts).
///
/// - Min delay: 10ms
/// - Max delay: 2s
/// - Max attempts: 10
/// - Jitter enabled
///
/// # Why These Values?
///
/// - **10ms min**: Sequence conflicts typically resolve quickly — another writer
///   just beat us. A brief delay is enough to let their write complete.
/// - **2s max**: If conflicts persist beyond 2s, something unusual is happening
///   (high contention, slow storage). Cap the delay to avoid blocking too long.
/// - **10 attempts**: Enough to ride out typical contention spikes. Beyond 10,
///   the conflict is likely structural (two components fighting over the same
///   aggregate), not transient.
/// - **Jitter**: Prevents thundering herd when multiple retries happen simultaneously.
pub fn saga_backoff() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(10))
        .with_max_delay(Duration::from_secs(2))
        .with_max_times(10)
        .with_jitter()
}

/// Backoff for gRPC connection retries at startup.
///
/// - Min delay: 100ms
/// - Max delay: 5s
/// - Max attempts: 30
/// - Jitter enabled
///
/// # Why These Values?
///
/// - **100ms min**: Network connections need more time than sequence retries.
///   Services might still be starting, DNS might be propagating.
/// - **5s max**: Connection issues can be transient (pod rescheduling, DNS
///   propagation). 5s is long enough for most K8s operations without making
///   startup feel stuck.
/// - **30 attempts**: At exponential backoff from 100ms to 5s, this gives
///   roughly 2-3 minutes of total retry time — enough for pod startup and
///   K8s service discovery to stabilize.
pub fn connection_backoff() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(5))
        .with_max_times(30)
        .with_jitter()
}

/// Determines if a gRPC error is retryable.
///
/// # Retryable Codes
///
/// - **`FailedPrecondition`**: Sequence mismatch with STRICT or COMMUTATIVE merge
///   strategy. The client's command had a stale sequence number. Fix: fetch fresh
///   state, rebuild command with correct sequence, retry.
///
/// # Non-Retryable Codes
///
/// - **`Aborted`**: Used for MERGE_MANUAL when sequence mismatch occurs. The
///   framework routes these to DLQ for human review — automated retry won't help.
/// - **Other codes**: Business rejections, validation errors, network errors, etc.
///   These require human intervention or code fixes.
///
/// # Why This Distinction?
///
/// Sequence conflicts are often transient — two concurrent writers raced, one won.
/// The loser should fetch fresh state and retry with the updated sequence. This is
/// safe because event sourcing guarantees idempotency via sequence numbers.
///
/// MERGE_MANUAL conflicts are different: the aggregate owner explicitly chose to
/// require human review rather than auto-retry. Respecting that decision means
/// routing to DLQ, not retrying.
pub fn is_retryable_status(status: &Status) -> bool {
    if !matches!(status.code(), Code::FailedPrecondition) {
        return false;
    }
    // FailedPrecondition is overloaded: aggregate guards (e.g.
    // "Hand already dealt", "Player does not exist") also surface as
    // FailedPrecondition. Only sequence-related rejections are actually
    // retryable — refetching state and retrying a guard rejection just
    // hits the same guard. Match the message prefix the framework emits
    // (`Sequence mismatch:` from single_sequence_check,
    // `Sequence conflict:` from storage::error::SEQUENCE_CONFLICT).
    let msg = status.message();
    msg.starts_with("Sequence mismatch:") || msg.starts_with("Sequence conflict:")
}

/// The outcome of a single attempt of a retryable operation.
pub enum RetryOutcome<S, F> {
    /// The operation succeeded.
    Success(S),
    /// The operation failed with a retryable error.
    Retryable(F),
    /// The operation failed with a fatal error.
    Fatal(F),
}

/// An operation that can be retried with backoff.
#[async_trait]
pub trait RetryableOperation: Send + Sync {
    /// The output of a successful operation.
    type Success;
    /// The error type for a failed operation.
    type Failure: std::fmt::Display + Send + Sync;

    /// The name of the operation, for logging.
    fn name(&self) -> &str;

    /// Attempt to perform the operation.
    async fn try_execute(&mut self) -> RetryOutcome<Self::Success, Self::Failure>;

    /// Prepare for the next attempt after a retryable failure.
    ///
    /// This method can be used to refresh state before the next try.
    /// If it returns an error, the retry loop is aborted.
    async fn prepare_for_retry(&mut self) -> Result<(), Self::Failure> {
        Ok(())
    }
}

/// Run a `RetryableOperation` with exponential backoff.
///
/// The operation is retried until it succeeds, fails with a fatal error,
/// or the backoff policy gives up.
pub async fn run_with_retry<Op>(
    mut operation: Op,
    backoff: ExponentialBuilder,
) -> Result<Op::Success, Op::Failure>
where
    Op: RetryableOperation,
{
    let mut attempt = 0;
    let mut delays = backoff.build();
    loop {
        attempt += 1;
        match operation.try_execute().await {
            RetryOutcome::Success(success) => return Ok(success),
            RetryOutcome::Retryable(failure) => {
                if let Some(delay) = delays.next() {
                    log_retry_attempt(operation.name(), attempt, &failure, delay);
                    if let Err(fatal_failure) = operation.prepare_for_retry().await {
                        error!(
                            operation = %operation.name(),
                            "Failed to prepare for retry: {}",
                            fatal_failure
                        );
                        return Err(fatal_failure);
                    }
                    tokio::time::sleep(delay).await;
                } else {
                    log_retry_exhausted(operation.name(), attempt, &failure);
                    return Err(failure);
                }
            }
            RetryOutcome::Fatal(failure) => {
                log_fatal_error(operation.name(), attempt, &failure);
                return Err(failure);
            }
        }
    }
}

#[cfg(test)]
#[path = "retry.test.rs"]
mod tests;
