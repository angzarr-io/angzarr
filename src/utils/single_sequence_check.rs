//! Single-value sequence comparison helpers for `AggregateService`.
//!
//! DOC: This file is referenced in docs/docs/operations/error-recovery.mdx
//!      Update documentation when making changes to sequence validation.
//!
//! **Scope.** This module compares a single expected `u32` against a single
//! actual `u32` and produces the standard `FailedPrecondition` status when
//! they disagree. The name was previously `sequence_validator`, which
//! implied a richer EventBook-level scan (gaps, duplicates) that this code
//! does NOT perform — see H-38 in the deep-review remediation plan.
//!
//! If you need EventBook page-sequence gap or duplicate detection, use
//! [`crate::services::gap_fill`] instead — that module already scans for
//! gaps between adjacent pages and is the canonical entry point for
//! reconciliation flows.

use prost::Message;
use tonic::Status;
use uuid::Uuid;

use crate::proto::EventBook;
use crate::storage::StorageError;

/// Result of a sequence validation check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceValidationResult {
    /// Sequence matches expected value.
    Valid,
    /// Sequence mismatch detected.
    Mismatch { expected: u32, actual: u32 },
}

/// Validates that the command sequence matches the aggregate's current sequence.
///
/// # Arguments
/// * `expected_sequence` - The sequence number from the command
/// * `actual_sequence` - The current aggregate sequence from the event store
///
/// # Returns
/// `SequenceValidationResult::Valid` if sequences match,
/// otherwise `SequenceValidationResult::Mismatch` with details.
pub fn validate_sequence(expected_sequence: u32, actual_sequence: u32) -> SequenceValidationResult {
    if expected_sequence == actual_sequence {
        SequenceValidationResult::Valid
    } else {
        SequenceValidationResult::Mismatch {
            expected: expected_sequence,
            actual: actual_sequence,
        }
    }
}

/// Creates a Status error for a sequence mismatch.
///
/// Uses `FailedPrecondition` because sequence mismatches are client errors —
/// the client sent a command with stale sequence information. The client
/// must fetch fresh state before retrying. This is NOT automatically retryable.
///
/// `Aborted` is reserved for storage-level conflicts (concurrent write races)
/// which ARE retryable since the client had correct information at validation time.
pub fn sequence_mismatch_error(expected: u32, actual: u32) -> Status {
    Status::failed_precondition(format!(
        "Sequence mismatch: command expects {}, aggregate at {}",
        expected, actual
    ))
}

/// Creates a Status error for sequence mismatch with EventBook attached as details.
///
/// The EventBook is serialized and attached to the status details,
/// allowing the caller to extract current state for a manual retry.
///
/// Uses `FailedPrecondition` — this is a client error, not automatically retryable.
pub fn sequence_mismatch_error_with_state(
    expected: u32,
    actual: u32,
    current_state: &EventBook,
) -> Status {
    let message = format!(
        "Sequence mismatch: command expects {}, aggregate at {}",
        expected, actual
    );

    // Serialize EventBook to binary for status details
    let details = current_state.encode_to_vec();

    Status::with_details(tonic::Code::FailedPrecondition, message, details.into())
}

/// Extract EventBook from status details if present.
///
/// Returns None if details are empty or cannot be decoded.
pub fn extract_event_book_from_status(status: &Status) -> Option<EventBook> {
    let details = status.details();
    if details.is_empty() {
        return None;
    }

    EventBook::decode(details).ok()
}

/// Outcome of handling a storage error during event persistence.
#[derive(Debug)]
pub enum StorageErrorOutcome {
    /// Should abort with the given error.
    Abort(Status),
}

/// Handles storage errors during event persistence.
///
/// # Arguments
/// * `error` - The storage error that occurred
/// * `domain` - The domain name (for logging)
/// * `root_uuid` - The aggregate root UUID (for logging)
///
/// # Returns
/// `StorageErrorOutcome::Abort` with a Status error.
pub fn handle_storage_error(
    error: StorageError,
    _domain: &str,
    _root_uuid: Uuid,
) -> StorageErrorOutcome {
    match error {
        StorageError::SequenceConflict { expected, actual } => {
            StorageErrorOutcome::Abort(Status::failed_precondition(format!(
                "Sequence conflict: expected {}, got {}",
                expected, actual
            )))
        }
        e => StorageErrorOutcome::Abort(Status::internal(format!("Failed to persist events: {e}"))),
    }
}

#[cfg(test)]
#[path = "single_sequence_check.test.rs"]
mod tests;
