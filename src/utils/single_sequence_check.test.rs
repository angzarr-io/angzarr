//! Tests for sequence validation and optimistic concurrency control.
//!
//! Sequence numbers are the foundation of event sourcing consistency:
//! - Commands carry expected sequence (client's view of aggregate state)
//! - Aggregates have actual sequence (true current state)
//! - Mismatch = stale client → reject with FailedPrecondition
//!
//! Why this matters: Without sequence validation, concurrent writes would
//! silently overwrite each other. Sequence numbers enable optimistic
//! concurrency control — last-write-wins detection.
//!
//! Key behaviors verified:
//! - Matching sequences pass validation
//! - Mismatches are detected and reported correctly
//! - Error messages include both expected and actual values for debugging
//! - Storage errors are classified correctly for retry vs abort decisions

use super::*;

// ============================================================================
// Sequence Validation Tests
// ============================================================================

/// Matching sequences validate successfully.
///
/// When command sequence equals aggregate sequence, the client has current
/// state and the command can proceed.
#[test]
fn test_validate_sequence_matching() {
    let result = validate_sequence(5, 5);
    assert_eq!(result, SequenceValidationResult::Valid);
}

/// Sequence mismatch detected and reported with both values.
///
/// When command expects sequence 0 but aggregate is at 5, the client has
/// stale state. The mismatch result includes both values for error reporting.
#[test]
fn test_validate_sequence_mismatch() {
    let result = validate_sequence(0, 5);
    assert_eq!(
        result,
        SequenceValidationResult::Mismatch {
            expected: 0,
            actual: 5
        }
    );
}

/// New aggregate creation: both sequences at zero is valid.
///
/// Creating a new aggregate means command sequence 0 and aggregate
/// sequence 0 (non-existent). This is the bootstrap case.
#[test]
fn test_validate_sequence_new_aggregate() {
    let result = validate_sequence(0, 0);
    assert_eq!(result, SequenceValidationResult::Valid);
}

// ============================================================================
// Error Formatting Tests
// ============================================================================

/// Sequence mismatch error uses FailedPrecondition and includes both values.
///
/// FailedPrecondition indicates a client error — the client sent stale data.
/// The message includes both sequence numbers for debugging.
#[test]
fn test_sequence_mismatch_error_format() {
    let status = sequence_mismatch_error(0, 5);
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(status.message().contains("Sequence mismatch"));
    assert!(status.message().contains("0"));
    assert!(status.message().contains("5"));
}

// ============================================================================
// Storage Error Handling Tests
// ============================================================================

/// Sequence conflict from storage returns FailedPrecondition (retryable).
///
/// When concurrent writers race and storage rejects due to sequence
/// conflict, the error maps to FailedPrecondition. This allows the
/// retry system to fetch fresh state and try again.
#[test]
fn test_handle_storage_error_sequence_conflict() {
    let error = StorageError::SequenceConflict {
        expected: 5,
        actual: 6,
    };
    let root = Uuid::new_v4();

    let outcome = handle_storage_error(error, "test", root);
    match outcome {
        StorageErrorOutcome::Abort(status) => {
            // Sequence conflicts are retryable (FAILED_PRECONDITION)
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            assert!(status.message().contains("Sequence conflict"));
        }
    }
}

/// Non-conflict storage errors return Internal (non-retryable).
///
/// Errors like MissingCover indicate structural problems that won't be
/// fixed by retry. These map to Internal status for immediate abort.
#[test]
fn test_handle_storage_error_other_error() {
    let error = StorageError::MissingCover;
    let root = Uuid::new_v4();

    let outcome = handle_storage_error(error, "test", root);
    match outcome {
        StorageErrorOutcome::Abort(status) => {
            assert_eq!(status.code(), tonic::Code::Internal);
            assert!(status.message().contains("persist events"));
        }
    }
}

// ============================================================================
// EventBook Status Details Tests
// ============================================================================
//
// When sequence mismatches occur, the current aggregate state can be
// attached to the error status. This allows sophisticated clients to
// extract the state and retry without an additional fetch round-trip.

/// Sequence mismatch error with state attaches EventBook as details.
///
/// The EventBook is serialized to binary and attached to the status.
/// Clients can extract it to see current aggregate state for retry.
#[test]
fn test_sequence_mismatch_error_with_state_roundtrip() {
    use crate::proto::{Cover, EventPage, PageHeader, Uuid as ProtoUuid};

    let event_book = EventBook {
        cover: Some(Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid {
                value: vec![1, 2, 3, 4],
            }),
            correlation_id: "corr-123".to_string(),
            edition: None,
        }),
        pages: vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(5)),
            }),
            created_at: None,
            payload: None,
            ..Default::default()
        }],
        snapshot: None,
        ..Default::default()
    };

    let status = sequence_mismatch_error_with_state(0, 5, &event_book);

    // Verify status is correct
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(status.message().contains("0"));
    assert!(status.message().contains("5"));

    // Verify EventBook can be extracted
    let extracted = extract_event_book_from_status(&status);
    assert!(extracted.is_some());

    let extracted = extracted.unwrap();
    assert_eq!(extracted.cover.unwrap().domain, "orders");
    assert_eq!(extracted.pages.len(), 1);
}

/// Empty status details returns None.
///
/// Not all errors include EventBook details. The extract function
/// must handle this gracefully.
#[test]
fn test_extract_event_book_from_status_empty_details() {
    let status = sequence_mismatch_error(0, 5);
    let extracted = extract_event_book_from_status(&status);
    assert!(extracted.is_none());
}

// ============================================================================
// H-38: Module name reflects actual scope
// ============================================================================
//
// The original `sequence_validator` name implies EventBook gap/duplicate
// scanning, but the implementation only compares two `u32` values. To prevent
// future callers from mistaking the contract, the module was renamed to
// `single_sequence_check` (with a doc-comment pointing gap/dup consumers at
// `services::gap_fill`). These tests pin the rename — they reach the API
// through the canonical `single_sequence_check` path.

/// New canonical module path resolves `validate_sequence`.
///
/// Compile-time check: if the module is still named `sequence_validator` (or
/// the public API isn't re-exposed under the new name) this test fails to
/// compile, which is the rename's failing-red signal.
#[test]
fn test_single_sequence_check_module_path_resolves() {
    // Reach through the canonical module name. If the path doesn't exist
    // the file won't compile.
    let result = crate::utils::single_sequence_check::validate_sequence(7, 7);
    assert_eq!(
        result,
        crate::utils::single_sequence_check::SequenceValidationResult::Valid
    );
}

/// `sequence_mismatch_error_with_state` reachable via new module path.
///
/// All four public functions consumed by orchestration call sites must
/// resolve through the new path; this covers the one with the richest
/// signature so the symbol-table miss is unambiguous.
#[test]
fn test_single_sequence_check_module_path_resolves_with_state() {
    use crate::proto::{Cover, Uuid as ProtoUuid};

    let event_book = EventBook {
        cover: Some(Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid {
                value: vec![1, 2, 3, 4],
            }),
            correlation_id: "corr".to_string(),
            edition: None,
        }),
        pages: vec![],
        snapshot: None,
        ..Default::default()
    };
    let status =
        crate::utils::single_sequence_check::sequence_mismatch_error_with_state(0, 3, &event_book);
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

/// Invalid details bytes returns None (not panic).
///
/// Malformed details should be handled gracefully, not crash.
#[test]
fn test_extract_event_book_from_status_invalid_bytes() {
    let status = Status::with_details(
        tonic::Code::FailedPrecondition,
        "test",
        vec![0xFF, 0xFF, 0xFF].into(),
    );
    let extracted = extract_event_book_from_status(&status);
    assert!(extracted.is_none());
}
