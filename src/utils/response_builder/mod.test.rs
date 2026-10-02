//! Tests for response building and event publishing utilities.
//!
//! The response builder bridges aggregate business logic and gRPC:
//! - Extracts events from BusinessResponse (success path)
//! - Converts revocations to gRPC status (rejection path)
//! - Publishes events to the event bus
//! - Preserves correlation IDs through the response path
//!
//! Correct response handling is critical — errors here cause lost events
//! or incorrect rejection status codes.

use super::*;
use crate::bus::MockEventBus;
use crate::orchestration::correlation::extract_correlation_id;
use crate::proto::{
    command_page, page_header, CommandBook, CommandPage, Cover, MergeStrategy, PageHeader,
    RevocationResponse, Uuid as ProtoUuid,
};
use prost_types::Any;

// ============================================================================
// Test Helpers
// ============================================================================

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

// ============================================================================
// Correlation ID Extraction Tests
// ============================================================================

/// Existing correlation ID extracted from command cover.
///
/// Commands originating from saga/PM flows carry correlation IDs for
/// cross-domain tracing. The response must preserve this ID.
#[test]
fn test_extract_correlation_id_existing() {
    let command = make_command_book(true);
    let result = extract_correlation_id(&command).unwrap();
    assert_eq!(result, "test-correlation-id");
}

/// Empty correlation ID stays empty — we don't auto-generate.
///
/// Direct API calls may not have correlation IDs. The framework propagates
/// what's given, not inventing IDs for commands that don't need tracing.
#[test]
fn test_extract_correlation_id_empty_stays_empty() {
    let command = make_command_book(false);
    let result = extract_correlation_id(&command).unwrap();
    assert!(result.is_empty());
}

// ============================================================================
// Event Extraction Tests
// ============================================================================

/// Events extracted from BusinessResponse and correlation ID set on cover.
///
/// The response builder stamps the correlation ID from the command onto
/// the event book's cover, ensuring traceability through the event store.
#[test]
fn test_extract_events_from_response_with_events() {
    let event_book = EventBook {
        cover: Some(Cover {
            domain: "test".to_string(),
            root: None,
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        pages: vec![],
        snapshot: None,
        ..Default::default()
    };
    let response = BusinessResponse {
        result: Some(business_response::Result::Events(event_book)),
    };

    let result = extract_events_from_response(response, "test-correlation");
    assert!(result.is_ok());
    let events = result.unwrap();
    // Correlation ID should be set on cover
    assert_eq!(
        events.cover.as_ref().unwrap().correlation_id,
        "test-correlation"
    );
}

/// Revocation response converted to FailedPrecondition status.
///
/// When business logic rejects a command, the revocation reason becomes
/// the gRPC error message. FailedPrecondition signals business rejection
/// (not a system error).
#[test]
fn test_extract_events_from_response_revocation() {
    let response = BusinessResponse {
        result: Some(business_response::Result::Revocation(RevocationResponse {
            emit_system_revocation: false,
            send_to_dead_letter_queue: false,
            escalate: false,
            abort: false,
            reason: "insufficient funds".to_string(),
        })),
    };

    let result = extract_events_from_response(response, "test-correlation");
    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(status.message().contains("insufficient funds"));
}

/// Empty response (no events, no revocation) returns empty EventBook.
///
/// Some commands may succeed without producing events (idempotent
/// operations where state is already correct). This is valid.
#[test]
fn test_extract_events_from_response_empty() {
    let response = BusinessResponse { result: None };

    let result = extract_events_from_response(response, "test-correlation");
    assert!(result.is_ok());
    let events = result.unwrap();
    assert!(events.pages.is_empty());
    // No cover means no correlation ID - that's expected for empty responses
}

/// Notification response returns empty EventBook for upstream propagation.
///
/// When a rejection handler returns a Notification, the framework handles
/// forwarding it separately. The response builder returns an empty EventBook
/// so the command processing completes without blocking.
#[test]
fn test_extract_events_from_response_notification() {
    use crate::proto::Notification;

    let response = BusinessResponse {
        result: Some(business_response::Result::Notification(Notification {
            cover: Some(Cover {
                domain: "test".to_string(),
                root: None,
                correlation_id: "upstream-correlation".to_string(),
                edition: None,
                ext: None,
            }),
            ..Default::default()
        })),
    };

    let result = extract_events_from_response(response, "test-correlation");
    assert!(result.is_ok());
    let events = result.unwrap();
    assert!(events.pages.is_empty());
    // Cover is None since notification handling doesn't produce events directly
    assert!(events.cover.is_none());
}

// ============================================================================
// Publish and Build Response Tests
// ============================================================================

/// Successful publish returns response with events and correlation ID.
///
/// After events are persisted, they're published to the event bus and
/// returned to the caller. The correlation ID is preserved for tracing.
#[tokio::test]
async fn test_publish_and_build_response_success() {
    let event_bus: Arc<dyn EventBus> = Arc::new(MockEventBus::new());
    let event_book = EventBook {
        cover: Some(Cover {
            domain: "test".to_string(),
            root: None,
            correlation_id: "test-correlation".to_string(),
            edition: None,
            ext: None,
        }),
        pages: vec![],
        snapshot: None,
        ..Default::default()
    };

    let result = publish_and_build_response(&event_bus, event_book).await;
    assert!(result.is_ok());

    let response = result.unwrap().into_inner();
    assert!(response.events.is_some());
    let events = response.events.unwrap();
    assert_eq!(
        events.cover.as_ref().unwrap().correlation_id,
        "test-correlation"
    );
}

/// Event bus publish failure returns Internal error.
///
/// If the event bus fails to publish, the operation fails entirely.
/// Events are already persisted, but the caller learns of the publish
/// failure to handle accordingly (e.g., retry or alert).
#[tokio::test]
async fn test_publish_and_build_response_bus_failure() {
    let mock_bus = Arc::new(MockEventBus::new());
    mock_bus.set_fail_on_publish(true).await;
    let event_bus: Arc<dyn EventBus> = mock_bus;

    let event_book = EventBook {
        cover: None,
        pages: vec![],
        snapshot: None,
        ..Default::default()
    };

    let result = publish_and_build_response(&event_bus, event_book).await;
    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::Internal);
}
