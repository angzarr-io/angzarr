//! Tests for correlation ID extraction from commands.
//!
//! Correlation IDs are the primary mechanism for cross-domain workflow tracking.
//! They enable process managers to correlate events across multiple domains and
//! provide end-to-end observability for saga flows. The extraction logic must:
//! - Preserve client-provided correlation IDs exactly as given
//! - Return empty string when not provided (allowing opt-in tracking)
//! - Validate format when non-empty to prevent injection issues

use super::*;
use crate::proto::{
    command_page, page_header, CommandPage, Cover, MergeStrategy, PageHeader, Uuid as ProtoUuid,
};
use prost_types::Any;

fn make_command_book(with_correlation: bool) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: "test".to_string(),
            root: Some(ProtoUuid {
                value: uuid::Uuid::new_v4().as_bytes().to_vec(),
            }),
            correlation_id: if with_correlation {
                "test-correlation-id".to_string()
            } else {
                String::new()
            },
            edition: None,
            ext: None,
        }),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(0)),
            }),
            payload: Some(command_page::Payload::Command(Any {
                type_url: "test.Command".to_string(),
                value: vec![],
            })),
            merge_strategy: MergeStrategy::MergeCommutative as i32,
        }],
    }
}

/// Client-provided correlation ID must be extracted verbatim.
///
/// The framework must never modify or regenerate correlation IDs — they are
/// client-controlled identifiers for cross-domain workflows. Altering them
/// would break event correlation in process managers.
#[test]
fn test_extract_preserves_existing_id() {
    let command = make_command_book(true);
    let result = extract_correlation_id(&command).unwrap();
    assert_eq!(result, "test-correlation-id");
}

/// Missing correlation ID returns empty string, not an error.
///
/// Cross-domain tracking is opt-in. Single-domain operations don't need
/// correlation IDs, and generating them automatically would pollute PM
/// state with unrelated events. Empty correlation_id signals "no PM routing".
#[test]
fn test_extract_returns_empty_when_not_provided() {
    let command = make_command_book(false);
    let result = extract_correlation_id(&command).unwrap();
    assert!(result.is_empty());
}

/// Invalid correlation ID format is rejected.
///
/// Correlation IDs may only contain alphanumerics, underscores, and hyphens.
/// Invalid characters could cause injection issues in downstream systems
/// or break query parsing.
#[test]
fn test_extract_rejects_invalid_format() {
    let command = CommandBook {
        cover: Some(Cover {
            domain: "test".to_string(),
            root: Some(ProtoUuid {
                value: uuid::Uuid::new_v4().as_bytes().to_vec(),
            }),
            correlation_id: "invalid/chars!here".to_string(),
            edition: None,
            ext: None,
        }),
        pages: vec![],
    };

    let result = extract_correlation_id(&command);
    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}
