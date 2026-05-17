//! Tests for saga compensation handling.
//!
//! Saga compensation handles command rejections in cross-domain workflows:
//! 1. Saga sends command to target domain
//! 2. Target rejects command (business validation fails)
//! 3. Framework must notify source domain of rejection
//! 4. Source domain may need to compensate (undo prior actions)
//!
//! This is critical for maintaining consistency across bounded contexts.
//! Without proper compensation routing, rejected cross-domain commands
//! would leave systems in inconsistent states.
//!
//! Key scenarios tested:
//! - Compensation context creation from rejected commands
//! - Rejection notification building
//! - Business response handling (events, revocations, errors)
//! - Escalation via DLQ or notifications
//! - Fallback behavior when business logic is unavailable

use super::*;
use crate::config::DEFAULT_SAGA_FALLBACK_DOMAIN;
use crate::proto::{command_page, event_page, page_header, CommandPage, MergeStrategy};

// ============================================================================
// Test Helpers
// ============================================================================

fn make_angzarr_deferred() -> AngzarrDeferredSequence {
    AngzarrDeferredSequence {
        source: Some(Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid {
                value: Uuid::new_v4().as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
        }),
        source_seq: 5,
    }
}

fn make_test_command() -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: "customer".to_string(),
            root: Some(ProtoUuid {
                value: Uuid::new_v4().as_bytes().to_vec(),
            }),
            correlation_id: "corr-123".to_string(),
            edition: None,
        }),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::AngzarrDeferred(
                    make_angzarr_deferred(),
                )),
            }),
            payload: Some(command_page::Payload::Command(prost_types::Any {
                type_url: "test.AddPoints".to_string(),
                value: vec![],
            })),
            merge_strategy: MergeStrategy::MergeCommutative as i32,
        }],
    }
}

fn make_context() -> CompensationContext {
    CompensationContext {
        source: make_angzarr_deferred(),
        rejection_reason: "Customer not found".to_string(),
        rejected_command: make_test_command(),
        correlation_id: "corr-123".to_string(),
    }
}

// ============================================================================
// Compensation Context Tests
// ============================================================================

/// Saga command creates compensation context with source info.
///
/// When a saga-originated command is rejected, we extract the angzarr_deferred
/// provenance to route the rejection back to the source aggregate. The context
/// captures everything needed for compensation routing.
#[test]
fn test_compensation_context_from_saga_command() {
    let command = make_test_command();
    let context =
        CompensationContext::from_rejected_command(&command, "rejection reason".to_string());

    assert!(context.is_some());
    let ctx = context.unwrap();
    assert_eq!(ctx.source.source_seq, 5);
    assert_eq!(ctx.source.source.as_ref().unwrap().domain, "orders");
    assert_eq!(ctx.rejection_reason, "rejection reason");
}

/// Non-saga command produces no compensation context.
///
/// Direct API commands (not from sagas/PMs) don't need compensation routing.
/// The rejection is returned directly to the caller via gRPC status.
#[test]
fn test_compensation_context_from_non_saga_command() {
    // Command with explicit sequence (not angzarr_deferred) - no provenance
    let command = CommandBook {
        cover: Some(Cover {
            domain: "customer".to_string(),
            root: Some(ProtoUuid {
                value: Uuid::new_v4().as_bytes().to_vec(),
            }),
            correlation_id: "corr-123".to_string(),
            edition: None,
        }),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(0)),
            }),
            payload: Some(command_page::Payload::Command(prost_types::Any {
                type_url: "test.AddPoints".to_string(),
                value: vec![],
            })),
            merge_strategy: MergeStrategy::MergeCommutative as i32,
        }],
    };

    let context =
        CompensationContext::from_rejected_command(&command, "rejection reason".to_string());

    assert!(context.is_none());
}

// ============================================================================
// Notification Building Tests
// ============================================================================

/// Rejection notification includes rejected command and reason.
///
/// The notification carries the rejection context. Source provenance is
/// embedded in the rejected command's page header (angzarr_deferred).
/// This enables the source aggregate to decide how to compensate.
#[test]
fn test_build_rejection_notification() {
    let context = make_context();
    let notification = build_rejection_notification(&context);

    assert_eq!(notification.rejection_reason, "Customer not found");
    assert!(notification.rejected_command.is_some());

    // Source provenance is in the rejected command's page header
    let cmd = notification.rejected_command.as_ref().unwrap();
    let header = cmd.pages.first().unwrap().header.as_ref().unwrap();
    if let Some(page_header::SequenceType::AngzarrDeferred(ad)) = &header.sequence_type {
        assert_eq!(ad.source_seq, 5);
        assert_eq!(ad.source.as_ref().unwrap().domain, "orders");
    } else {
        panic!("Expected AngzarrDeferred in rejected command header");
    }
}

/// Notification command book targets the source aggregate's domain.
///
/// The notification routes back to the original domain (not the target
/// that rejected). This is how the source aggregate learns of rejection.
#[test]
fn test_build_notification_command_book() {
    let context = make_context();
    let command_book = build_notification_command_book(&context).unwrap();

    assert!(command_book.cover.is_some());
    let cover = command_book.cover.unwrap();
    assert_eq!(cover.domain, "orders");
}

/// Missing source prevents notification routing.
///
/// If angzarr_deferred doesn't include the source Cover, we can't
/// route the notification. This is a configuration error.
#[test]
fn test_build_notification_command_book_missing_source() {
    let mut context = make_context();
    context.source.source = None;

    let result = build_notification_command_book(&context);
    assert!(matches!(result, Err(CompensationError::MissingSource)));
}

// ============================================================================
// Compensation Failed Event Tests
// ============================================================================

/// Compensation failed event captures both rejection and failure reasons.
///
/// Two distinct failure modes:
/// 1. Original rejection: why the target rejected the command
/// 2. Compensation failure: why compensation itself failed
///
/// Both are recorded for debugging and audit.
#[test]
fn test_build_compensation_failed_event() {
    let context = make_context();
    let event = build_compensation_failed_event(&context, "Business declined");

    assert_eq!(event.triggering_event_sequence, 5);
    assert_eq!(
        event.triggering_aggregate.as_ref().unwrap().domain,
        "orders"
    );
    assert_eq!(event.rejection_reason, "Customer not found");
    assert_eq!(event.compensation_failure_reason, "Business declined");
    assert!(event.occurred_at.is_some());
}

/// Compensation failed event book goes to fallback domain with correlation.
///
/// Failed compensations must be recorded somewhere. The fallback domain
/// is a system-level aggregate that collects failures. Correlation ID
/// is preserved for tracing.
#[test]
fn test_build_compensation_failed_event_book() {
    let context = make_context();
    let config = SagaCompensationConfig::default();
    let event_book = build_compensation_failed_event_book(&context, "failure", &config);

    assert!(event_book.cover.is_some());
    let cover = event_book.cover.unwrap();
    assert_eq!(cover.domain, DEFAULT_SAGA_FALLBACK_DOMAIN);
    assert_eq!(event_book.pages.len(), 1);
    assert_eq!(cover.correlation_id, "corr-123");
}

// ============================================================================
// Business Response Handling Tests
// ============================================================================

/// Business logic provides compensation events — events outcome.
///
/// Happy path: source aggregate receives rejection notification, emits
/// compensation events (e.g., "OrderCompensated"). These events are
/// persisted and published normally.
#[tokio::test]
async fn test_handle_business_response_with_events() {
    let context = make_context();
    let config = SagaCompensationConfig::default();
    let handler = NoopEscalationHandler;

    let response = Ok(BusinessResponse {
        result: Some(business_response::Result::Events(EventBook {
            cover: Some(Cover {
                domain: "orders".to_string(),
                root: None,
                correlation_id: "corr-123".to_string(),
                edition: None,
            }),
            pages: vec![EventPage {
                header: Some(PageHeader {
                    sync_mode: None,
                    sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(6)),
                }),
                created_at: None,
                payload: Some(event_page::Payload::Event(prost_types::Any {
                    type_url: "test.Compensated".to_string(),
                    value: vec![],
                })),
                ..Default::default()
            }],
            snapshot: None,
            ..Default::default()
        })),
    });

    let outcome = handle_business_response(response, &context, &config, &handler)
        .await
        .unwrap();
    assert!(matches!(outcome, CompensationOutcome::Events(_)));
}

/// Empty event response triggers fallback to system revocation.
///
/// If business logic returns empty events (acknowledging but not acting),
/// and fallback is configured, we emit a system-level revocation event
/// to record that compensation was handled.
#[tokio::test]
async fn test_handle_business_response_empty_events_uses_fallback() {
    let context = make_context();
    let config = SagaCompensationConfig {
        fallback_emit_system_revocation: true,
        ..Default::default()
    };
    let handler = NoopEscalationHandler;

    let response = Ok(BusinessResponse {
        result: Some(business_response::Result::Events(EventBook {
            cover: None,
            pages: vec![], // Empty!
            snapshot: None,
            ..Default::default()
        })),
    });

    let outcome = handle_business_response(response, &context, &config, &handler)
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        CompensationOutcome::EmitSystemRevocation(_)
    ));
}

/// Revocation with emit_system_revocation flag emits system event.
///
/// Business logic can explicitly request a system-level revocation event
/// instead of handling compensation internally. This is useful when the
/// aggregate acknowledges the rejection but defers handling.
#[tokio::test]
async fn test_handle_business_response_revocation_emit() {
    let context = make_context();
    let config = SagaCompensationConfig::default();
    let handler = NoopEscalationHandler;

    let response = Ok(BusinessResponse {
        result: Some(business_response::Result::Revocation(RevocationResponse {
            emit_system_revocation: true,
            send_to_dead_letter_queue: false,
            escalate: false,
            abort: false,
            reason: "Cannot compensate".to_string(),
        })),
    });

    let outcome = handle_business_response(response, &context, &config, &handler)
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        CompensationOutcome::EmitSystemRevocation(_)
    ));
}

/// Revocation with abort flag causes compensation error.
///
/// Critical failures where compensation cannot proceed and the system
/// should halt. This is the nuclear option — used when continuing would
/// cause data corruption or other serious issues.
#[tokio::test]
async fn test_handle_business_response_revocation_abort() {
    let context = make_context();
    let config = SagaCompensationConfig::default();
    let handler = NoopEscalationHandler;

    let response = Ok(BusinessResponse {
        result: Some(business_response::Result::Revocation(RevocationResponse {
            emit_system_revocation: false,
            send_to_dead_letter_queue: false,
            escalate: false,
            abort: true,
            reason: "Critical failure".to_string(),
        })),
    });

    let result = handle_business_response(response, &context, &config, &handler).await;
    assert!(matches!(result, Err(CompensationError::Aborted(_))));
}

/// Revocation with no flags is a silent decline.
///
/// Business logic acknowledges the rejection but takes no action. This is
/// valid for idempotent scenarios where the system is already in the
/// correct state (e.g., "this order was already cancelled").
#[tokio::test]
async fn test_handle_business_response_revocation_declined() {
    let context = make_context();
    let config = SagaCompensationConfig::default();
    let handler = NoopEscalationHandler;

    let response = Ok(BusinessResponse {
        result: Some(business_response::Result::Revocation(RevocationResponse {
            emit_system_revocation: false,
            send_to_dead_letter_queue: false,
            escalate: false,
            abort: false,
            reason: "Already handled".to_string(),
        })),
    });

    let outcome = handle_business_response(response, &context, &config, &handler)
        .await
        .unwrap();
    assert!(matches!(outcome, CompensationOutcome::Declined { .. }));
}

/// gRPC error triggers fallback when configured.
///
/// If the business logic service is unavailable, we can't get a proper
/// response. With fallback configured, we emit a system revocation to
/// record the compensation attempt and failure.
#[tokio::test]
async fn test_handle_business_response_grpc_error_uses_fallback() {
    let context = make_context();
    let config = SagaCompensationConfig {
        fallback_emit_system_revocation: true,
        ..Default::default()
    };
    let handler = NoopEscalationHandler;

    let response = Err(tonic::Status::unavailable("Service down"));

    let outcome = handle_business_response(response, &context, &config, &handler)
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        CompensationOutcome::EmitSystemRevocation(_)
    ));
}

// ============================================================================
// Escalation Handler Tests
// ============================================================================

/// Noop escalation handler quarantine always succeeds (logs only).
///
/// Used in tests and as a fallback. In production, quarantine would
/// send to DLQ or alerting system.
#[tokio::test]
async fn test_noop_escalation_handler_quarantine() {
    let context = make_context();
    let handler = NoopEscalationHandler;

    // NoopEscalationHandler always succeeds (logs only)
    let result = handler.quarantine(&context, "test reason").await;
    assert!(result.is_ok());
}

/// Noop escalation handler notify always succeeds (logs only).
///
/// Used in tests. In production, would send Slack/PagerDuty/etc. alerts.
#[tokio::test]
async fn test_noop_escalation_handler_notify() {
    let context = make_context();
    let handler = NoopEscalationHandler;

    // NoopEscalationHandler always succeeds (logs only)
    let result = handler.notify(&context, "test reason").await;
    assert!(result.is_ok());
}

// ============================================================================
// Escalation Flag Tests
// ============================================================================

/// Escalate flag triggers notification alongside other actions.
///
/// The escalate flag adds alerting to whatever other action is taken.
/// Here, emit_system_revocation is also set, so we get both: an alert
/// AND a system revocation event.
#[tokio::test]
async fn test_handle_business_response_with_notify_flag() {
    let context = make_context();
    let config = SagaCompensationConfig::default();
    let handler = NoopEscalationHandler;

    // When escalate flag is true, handler.notify() is called
    let response = Ok(BusinessResponse {
        result: Some(business_response::Result::Revocation(RevocationResponse {
            emit_system_revocation: true,
            send_to_dead_letter_queue: false,
            escalate: true, // This triggers notify()
            abort: false,
            reason: "Notification needed".to_string(),
        })),
    });

    let outcome = handle_business_response(response, &context, &config, &handler)
        .await
        .unwrap();
    // Should still return EmitSystemRevocation since that flag is also set
    assert!(matches!(
        outcome,
        CompensationOutcome::EmitSystemRevocation(_)
    ));
}

/// DLQ flag triggers quarantine alongside decline.
///
/// The send_to_dead_letter_queue flag routes to DLQ for manual review.
/// Without emit_system_revocation, the outcome is Declined (no system
/// event), but the DLQ quarantine still happens.
#[tokio::test]
async fn test_handle_business_response_with_quarantine_flag() {
    let context = make_context();
    let config = SagaCompensationConfig::default();
    let handler = NoopEscalationHandler;

    // When send_to_dead_letter_queue flag is true, handler.quarantine() is called
    let response = Ok(BusinessResponse {
        result: Some(business_response::Result::Revocation(RevocationResponse {
            emit_system_revocation: false,
            send_to_dead_letter_queue: true, // This triggers quarantine()
            escalate: false,
            abort: false,
            reason: "Quarantine requested".to_string(),
        })),
    });

    let outcome = handle_business_response(response, &context, &config, &handler)
        .await
        .unwrap();
    // Should return Declined since emit_system_revocation is false
    assert!(matches!(outcome, CompensationOutcome::Declined { .. }));
}
