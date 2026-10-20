//! How the pipeline fails, and the status each failure answers.

use tonic::Status;

/// A pipeline failure. Each maps to one gRPC status
/// ([`PipelineError::to_status`]); whether it is also dead-lettered depends
/// on the delivery (see [`super::Delivery`]).
#[derive(Debug, Clone)]
pub enum PipelineError {
    /// Refused before anything ran: malformed (INVALID_ARGUMENT), a field
    /// reserved for framework components (PERMISSION_DENIED), or a
    /// never-created edition (NOT_FOUND).
    Refused(Status),
    /// A stale expected sequence under STRICT: ABORTED (a failed
    /// test-and-set; re-read and re-issue).
    Stale { expected: u64, actual: u64 },
    /// Business code refused the command; its status is returned unchanged.
    Rejected(Status),
    /// The business response broke the contract (a handler bug): INTERNAL.
    /// Nothing was persisted.
    HandlerBug(String),
    /// No business binary has registered with this coordinator yet:
    /// UNAVAILABLE (retryable).
    NotRegistered,
    /// Storage, bus or transport failure, as a status.
    Infrastructure(Status),
}

impl PipelineError {
    /// The status a caller is answered with.
    pub fn to_status(&self) -> Status {
        match self {
            PipelineError::Refused(s) | PipelineError::Rejected(s) => s.clone(),
            PipelineError::Infrastructure(s) => s.clone(),
            PipelineError::Stale { expected, actual } => Status::aborted(format!(
                "stale expected_sequence: command expects {expected}, stream is at {actual}"
            )),
            PipelineError::HandlerBug(reason) => Status::internal(format!("handler bug: {reason}")),
            PipelineError::NotRegistered => {
                Status::unavailable("no business binary has registered with this coordinator yet")
            }
        }
    }
}

impl From<crate::storage::StorageError> for PipelineError {
    fn from(e: crate::storage::StorageError) -> Self {
        // Storage faults are transient from the caller's view: UNAVAILABLE
        // is retryable, INTERNAL is not (command_handler.proto).
        PipelineError::Infrastructure(Status::unavailable(e.to_string()))
    }
}

#[cfg(test)]
#[path = "error.test.rs"]
mod tests;
