//! Retry utilities: backoff builders and retryable error classification.
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
/// Aligned with R2-15 decision #2 so the same classifier governs retry
/// behavior across aggregate, saga, PM, and projector handler paths.
/// Stays consistent with `crate::dlq::trigger::CodeDlqExt::classify_for_dlq`:
/// codes that classifier sends to `DlqTrigger::RetryThenDlq` are the
/// retryable ones here, with the sequence-conflict exception below.
///
/// # Retryable Codes
///
/// - **`Unavailable`**, **`DeadlineExceeded`**, **`ResourceExhausted`**:
///   Downstream temporarily overloaded or unreachable. A retry after
///   backoff often succeeds.
/// - **`Internal`**, **`Unknown`**, **`DataLoss`**: Server-side faults
///   that may be transient (leader re-election, transient storage hiccup,
///   process restart). Retry, then DLQ on exhaustion.
/// - **`Cancelled`**: Upstream / shutdown cancellation that typically
///   recovers on the next event-loop tick.
/// - **Sequence-conflict `FailedPrecondition`**: messages starting
///   `"Sequence mismatch:"` (from `single_sequence_check`) or
///   `"Sequence conflict:"` (from `storage::error::SEQUENCE_CONFLICT`).
///   Two writers raced; the loser refetches fresh state and retries.
///   Event sourcing's per-sequence idempotency makes this safe.
///
/// # Non-Retryable Codes
///
/// All other 4xx-class codes (`InvalidArgument`, `NotFound`, `Aborted`,
/// `Unimplemented`, `PermissionDenied`, `Unauthenticated`, `OutOfRange`,
/// `AlreadyExists`) plus non-sequence-conflict `FailedPrecondition`
/// (business guards like "Hand already dealt", "Player does not exist" —
/// refreshing state just re-hits the guard). `Aborted` specifically
/// covers MERGE_MANUAL routing, where the aggregate owner asked for
/// human review rather than automated retry. These all map to
/// `DlqTrigger::Immediate` at the DLQ layer.
///
/// # Why This Set?
///
/// Before R2-15, only sequence-conflict `FailedPrecondition` was
/// retryable — the narrow definition made sense for the aggregate
/// pipeline (sequence races are the dominant transient case there) but
/// silently dropped legitimate transient errors (broker briefly down,
/// downstream rebalancing) into the no-retry / DLQ-immediate bucket on
/// the saga/PM/projector paths. R2-15 broadens the helper to the gRPC
/// canonical transient set so all four handler types share one
/// operator contract: 4xx → DLQ now, 5xx → retry then DLQ.
///
/// # Consequence on Aggregate Retry Behavior
///
/// The aggregate command pipeline (`orchestration/aggregate/pipeline.rs`)
/// will now retry on the full 5xx-class set, not just sequence
/// conflicts. Aggregates will hold commands longer in the face of
/// downstream blips rather than rejecting fast; operators who relied
/// on fast-reject behavior for non-sequence transient codes will see
/// delayed rejections. Accepted per R2-15.
pub fn is_retryable_status(status: &Status) -> bool {
    use Code::*;
    match status.code() {
        Unavailable | DeadlineExceeded | ResourceExhausted | Internal | Unknown | DataLoss
        | Cancelled => true,
        FailedPrecondition => {
            // FailedPrecondition is overloaded: business guards (e.g.
            // "Hand already dealt") also surface as FailedPrecondition.
            // Only sequence-conflict messages are retryable — refetching
            // state and retrying a guard rejection just re-hits the
            // guard. Match the message prefixes the framework emits.
            let msg = status.message();
            msg.starts_with("Sequence mismatch:") || msg.starts_with("Sequence conflict:")
        }
        Ok | InvalidArgument | NotFound | Aborted | Unimplemented | PermissionDenied
        | Unauthenticated | OutOfRange | AlreadyExists => false,
    }
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
