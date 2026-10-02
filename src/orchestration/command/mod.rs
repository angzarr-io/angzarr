//! Command execution abstraction.
//!
//! `CommandExecutor` sends commands to aggregates and classifies the outcome
//! via `grpc/`'s remote `AggregateCoordinatorServiceClient`.

pub mod grpc;

use async_trait::async_trait;
use tonic::Code;

use crate::proto::{CascadeErrorMode, CommandBook, CommandResponse, EventBook, SyncMode};

/// Outcome of executing a single command.
#[derive(Debug)]
pub enum CommandOutcome {
    /// Command executed successfully.
    Success(CommandResponse),
    /// Retryable error (transient, per `crate::utils::retry::is_retryable_status`).
    /// Contains error description and optionally the current aggregate state
    /// for optimized retry without refetching. Set for sequence-conflict
    /// `FailedPrecondition` and the 5xx-class transient codes from R2-15
    /// decision #2 (`Unavailable`, `DeadlineExceeded`, `ResourceExhausted`,
    /// `Internal`, `Unknown`, `DataLoss`, `Cancelled`).
    Retryable {
        reason: String,
        current_state: Option<EventBook>,
    },
    /// Non-retryable rejection. Carries the originating gRPC `Code` so
    /// downstream consumers (DLQ, compensation) can classify without
    /// re-parsing the message (see `crate::dlq::trigger::CodeDlqExt` for
    /// the canonical permanent/transient split), the status message, and
    /// the machine rejection code (`google.rpc.ErrorInfo.reason` in the
    /// status details; empty when there is none).
    Rejected {
        code: Code,
        message: String,
        error_code: String,
    },
}

impl CommandOutcome {
    /// Classify a failed command's status: retryable codes become
    /// `Retryable` (carrying the aggregate state from the details when
    /// present), everything else `Rejected` with the status's code, message
    /// and ErrorInfo.reason.
    pub fn from_status(status: tonic::Status) -> Self {
        use crate::proto_ext::StatusExt;
        if crate::utils::retry::is_retryable_status(&status) {
            return CommandOutcome::Retryable {
                reason: status.message().to_string(),
                current_state: crate::utils::single_sequence_check::extract_event_book_from_status(
                    &status,
                ),
            };
        }
        CommandOutcome::Rejected {
            code: status.code(),
            message: status.message().to_string(),
            error_code: status.error_info_reason(),
        }
    }
}

/// Executes commands against aggregates.
#[async_trait]
pub trait CommandExecutor: Send + Sync {
    /// Execute a command and classify the result.
    ///
    /// `sync_mode` is forwarded to the destination aggregate:
    /// - `Async`: the destination publishes and returns; downstream runs off the bus
    /// - `Simple`: the destination also waits for sync projectors
    /// - `Cascade`: the destination also runs sagas/PMs synchronously
    /// - `Decision`: accept/reject only, downstream async
    /// - `Isolated`: persist only, no downstream
    async fn execute(&self, command: CommandBook, sync_mode: SyncMode) -> CommandOutcome;
}

/// What happens when a saga- or PM-emitted command cannot be delivered.
///
/// A rejected command's RejectionNotification always reaches its source,
/// whatever the policy. Bus-driven sagas and PMs (`None` error mode) have no
/// caller to report to: a rejected command is dead-lettered, and a command
/// that exhausts its retries is dead-lettered. A synchronous caller (CASCADE)
/// chooses with its `CascadeErrorMode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeliveryPolicy {
    /// Dead-letter, then carry on; the orchestration succeeds.
    Background,
    /// Stop at the first failure and fail the orchestration.
    FailFast,
    /// Stop at the first failure, record a Compensate notification for every
    /// command already executed, and fail.
    Compensate,
    /// Deliver every command; the orchestration succeeds with the commands
    /// that were delivered.
    Continue,
    /// Dead-letter failures and carry on; the orchestration succeeds.
    DeadLetter,
}

impl DeliveryPolicy {
    pub(crate) fn from_mode(mode: Option<CascadeErrorMode>) -> Self {
        use crate::proto_ext::CascadeErrorModeExt;
        let Some(mode) = mode else {
            return DeliveryPolicy::Background;
        };
        match CascadeErrorMode::or_default_fail_fast(mode as i32) {
            CascadeErrorMode::CascadeErrorCompensate => DeliveryPolicy::Compensate,
            CascadeErrorMode::CascadeErrorContinue => DeliveryPolicy::Continue,
            CascadeErrorMode::CascadeErrorDeadLetter => DeliveryPolicy::DeadLetter,
            CascadeErrorMode::CascadeErrorFailFast | CascadeErrorMode::CascadeErrorUnspecified => {
                DeliveryPolicy::FailFast
            }
        }
    }

    /// Whether a failure ends delivery of the remaining commands.
    pub(crate) fn stops_on_failure(self) -> bool {
        matches!(self, DeliveryPolicy::FailFast | DeliveryPolicy::Compensate)
    }

    /// Whether failed commands are dead-lettered.
    pub(crate) fn dead_letters(self) -> bool {
        matches!(
            self,
            DeliveryPolicy::Background | DeliveryPolicy::DeadLetter
        )
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
