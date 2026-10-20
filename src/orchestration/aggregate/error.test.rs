//! Tests for the pipeline's status mapping.
//!
//! Why this matters: callers and retry loops act on the code alone
//! (command_handler.proto, "Status codes"); a stale sequence must be
//! ABORTED (retryable), a handler bug INTERNAL (terminal), storage faults
//! UNAVAILABLE (retryable), and a business rejection unchanged.

use super::*;
use tonic::Code;

#[test]
fn test_status_codes() {
    assert_eq!(
        PipelineError::Stale {
            expected: 1,
            actual: 3
        }
        .to_status()
        .code(),
        Code::Aborted
    );
    assert_eq!(
        PipelineError::HandlerBug("skew".into()).to_status().code(),
        Code::Internal
    );
    assert_eq!(
        PipelineError::NotRegistered.to_status().code(),
        Code::Unavailable
    );
    let rejection = Status::failed_precondition("order is closed");
    let mapped = PipelineError::Rejected(rejection).to_status();
    assert_eq!(mapped.code(), Code::FailedPrecondition);
    assert_eq!(mapped.message(), "order is closed");
}

#[test]
fn test_storage_errors_are_unavailable() {
    let e: PipelineError = crate::storage::StorageError::NotImplemented("x".into()).into();
    assert_eq!(e.to_status().code(), Code::Unavailable);
}
