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
    /// re-parsing the message. See `crate::dlq::trigger::CodeDlqExt` for
    /// the canonical permanent/transient split.
    Rejected { code: Code, message: String },
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
/// Bus-driven sagas and PMs (`None` error mode) have no caller to report to: a
/// rejection is compensated at the source and dead-lettered, and a command
/// that exhausts its retries is dead-lettered. A synchronous caller (CASCADE)
/// chooses with its `CascadeErrorMode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeliveryPolicy {
    /// Compensate + DLQ, then carry on; the orchestration succeeds.
    Background,
    /// Stop at the first failure and fail the orchestration.
    FailFast,
    /// Compensate the failed command at its source, then stop and fail.
    Compensate,
    /// Deliver every command; the orchestration succeeds with the commands
    /// that were delivered.
    Continue,
    /// Dead-letter failures and carry on; the orchestration succeeds.
    DeadLetter,
}

impl DeliveryPolicy {
    pub(crate) fn from_mode(mode: Option<CascadeErrorMode>) -> Self {
        match mode {
            None => DeliveryPolicy::Background,
            Some(CascadeErrorMode::CascadeErrorFailFast) => DeliveryPolicy::FailFast,
            Some(CascadeErrorMode::CascadeErrorCompensate) => DeliveryPolicy::Compensate,
            Some(CascadeErrorMode::CascadeErrorContinue) => DeliveryPolicy::Continue,
            Some(CascadeErrorMode::CascadeErrorDeadLetter) => DeliveryPolicy::DeadLetter,
        }
    }

    /// Whether a failure ends delivery of the remaining commands.
    pub(crate) fn stops_on_failure(self) -> bool {
        matches!(self, DeliveryPolicy::FailFast | DeliveryPolicy::Compensate)
    }

    /// Whether a failed command is routed back to its source for compensation.
    pub(crate) fn compensates(self) -> bool {
        matches!(
            self,
            DeliveryPolicy::Background | DeliveryPolicy::Compensate
        )
    }

    /// Whether failed commands are dead-lettered.
    pub(crate) fn dead_letters(self) -> bool {
        matches!(
            self,
            DeliveryPolicy::Background | DeliveryPolicy::DeadLetter
        )
    }

    /// Whether failures are reported to the caller as an error.
    pub(crate) fn reports_failures(self) -> bool {
        matches!(self, DeliveryPolicy::FailFast | DeliveryPolicy::Compensate)
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
