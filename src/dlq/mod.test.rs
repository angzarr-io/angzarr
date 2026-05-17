//! Tests for Dead Letter Queue (DLQ) infrastructure.
//!
//! DLQ captures failed messages for manual review and replay. This enables
//! debugging without data loss and supports eventual consistency patterns
//! where some failures require human intervention.
//!
//! Why this matters: Without DLQ, sequence mismatches in MERGE_MANUAL mode
//! would silently drop commands. DLQ captures these for operator review,
//! enabling recovery workflows.
//!
//! Key behaviors verified:
//! - Topic naming follows `angzarr.dlq.{domain}` pattern
//! - Dead letter creation from sequence mismatches, event failures, payload failures
//! - Metadata enrichment for debugging context
//! - Proto conversion preserves all fields
//! - Reason type classification for metrics

use super::*;
use crate::proto::{command_page, page_header, CommandPage, PageHeader, Uuid as ProtoUuid};
use uuid::Uuid;

fn make_test_command(domain: &str, root: Uuid) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: "test-corr-123".to_string(),
            edition: None,
        }),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(0)),
            }),
            payload: Some(command_page::Payload::Command(prost_types::Any {
                type_url: "test.Command".to_string(),
                value: vec![1, 2, 3],
            })),
            merge_strategy: MergeStrategy::MergeManual as i32,
        }],
    }
}

// ============================================================================
// Topic Naming Tests
// ============================================================================

/// Topics include domain for routing and filtering.
///
/// Per-domain topics enable domain-specific retention policies and access
/// control. Operators can focus on their domain's dead letters.
#[test]
fn test_dlq_topic_for_domain() {
    assert_eq!(dlq_topic_for_domain("orders"), "angzarr.dlq.orders");
    assert_eq!(dlq_topic_for_domain("inventory"), "angzarr.dlq.inventory");
    assert_eq!(dlq_topic_for_domain("player"), "angzarr.dlq.player");
}

/// Dead letter topic derived from cover domain.
#[test]
fn test_dead_letter_topic() {
    let cmd = make_test_command("orders", Uuid::new_v4());
    let dl = AngzarrDeadLetter::from_sequence_mismatch(
        &cmd,
        0,
        5,
        MergeStrategy::MergeManual,
        "test-agg",
    );
    assert_eq!(dl.topic(), "angzarr.dlq.orders");
}

// ============================================================================
// Dead Letter Creation Tests
// ============================================================================

/// Sequence mismatch captures all details needed for debugging.
///
/// Operators need expected vs actual sequence, strategy, and source
/// component to diagnose why the command was rejected.
#[test]
fn test_from_sequence_mismatch() {
    let root = Uuid::new_v4();
    let cmd = make_test_command("orders", root);

    let dl = AngzarrDeadLetter::from_sequence_mismatch(
        &cmd,
        0,
        5,
        MergeStrategy::MergeManual,
        "orders-agg",
    );

    assert_eq!(dl.domain(), Some("orders"));
    assert!(dl.rejection_reason.contains("0"));
    assert!(dl.rejection_reason.contains("5"));
    assert_eq!(dl.source_component, "orders-agg");
    assert_eq!(dl.source_component_type, "aggregate");

    match &dl.rejection_details {
        Some(RejectionDetails::SequenceMismatch(details)) => {
            assert_eq!(details.expected_sequence, 0);
            assert_eq!(details.actual_sequence, 5);
            assert_eq!(details.merge_strategy, MergeStrategy::MergeManual);
        }
        _ => panic!("Expected SequenceMismatch details"),
    }

    match &dl.payload {
        DeadLetterPayload::Command(c) => {
            assert_eq!(c.cover.as_ref().unwrap().domain, "orders");
        }
        _ => panic!("Expected Command payload"),
    }
}

/// Event processing failures from sagas/projectors captured with retry context.
///
/// Retry count and transient flag help operators decide if manual replay
/// will succeed or if the underlying issue needs fixing first.
#[test]
fn test_from_event_processing_failure() {
    let root = Uuid::new_v4();
    let events = EventBook {
        cover: Some(Cover {
            domain: "orders".to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: "test-corr".to_string(),
            edition: None,
        }),
        pages: vec![],
        snapshot: None,
        ..Default::default()
    };

    let root_cause = CapturedError {
        r#type: "tokio::time::error::Elapsed".to_string(),
        message: "downstream timeout".to_string(),
        frames: vec![StackFrame {
            function: "tokio::time::sleep".to_string(),
            module: "tokio".to_string(),
            file: "tokio/src/time/sleep.rs".to_string(),
            line: 100,
            in_app: false,
            ..StackFrame::default()
        }],
        mechanism: Some(ExceptionMechanism {
            r#type: "chained".to_string(),
            handled: true,
            exception_id: 0,
            parent_id: 0,
            ..ExceptionMechanism::default()
        }),
        release: "angzarr-saga-orders@2.4.1".to_string(),
        server_name: "saga-orders-0".to_string(),
    };
    let outer = CapturedError {
        r#type: "saga::HandlerError".to_string(),
        message: "Saga handler failed".to_string(),
        frames: vec![
            StackFrame {
                function: "saga::handle_event".to_string(),
                module: "orders_saga".to_string(),
                file: "src/saga.rs".to_string(),
                line: 42,
                in_app: true,
                ..StackFrame::default()
            },
            StackFrame {
                function: "dispatch".to_string(),
                module: "angzarr_core".to_string(),
                file: "src/orchestration/saga/mod.rs".to_string(),
                line: 318,
                in_app: false,
                ..StackFrame::default()
            },
        ],
        mechanism: Some(ExceptionMechanism {
            r#type: "generic".to_string(),
            handled: true,
            exception_id: 1,
            parent_id: 0,
            ..ExceptionMechanism::default()
        }),
        release: "angzarr-saga-orders@2.4.1".to_string(),
        server_name: "saga-orders-0".to_string(),
    };
    let chain = vec![root_cause, outer];

    let dl = AngzarrDeadLetter::from_event_processing_failure(
        &events,
        "Saga handler failed",
        3,
        false,
        chain.clone(),
        "saga-order-fulfillment",
        "saga",
    );

    assert_eq!(dl.domain(), Some("orders"));
    assert!(dl.rejection_reason.contains("Saga handler failed"));
    assert!(dl.rejection_reason.contains("3 attempts"));
    assert_eq!(dl.source_component, "saga-order-fulfillment");
    assert_eq!(dl.source_component_type, "saga");

    match &dl.rejection_details {
        Some(RejectionDetails::EventProcessingFailed(details)) => {
            assert_eq!(details.error, "Saga handler failed");
            assert_eq!(details.retry_count, 3);
            assert!(!details.is_transient);
            assert_eq!(details.stack_trace, chain);
            // chain is most-causal-first; the originating caught error
            // is the last element.
            assert_eq!(
                details.stack_trace.last().unwrap().r#type,
                "saga::HandlerError"
            );
            assert_eq!(
                details.stack_trace.first().unwrap().r#type,
                "tokio::time::error::Elapsed"
            );
        }
        _ => panic!("Expected EventProcessingFailed details"),
    }

    match &dl.payload {
        DeadLetterPayload::Events(_) => {}
        _ => panic!("Expected Events payload"),
    }
}

/// Metadata enrichment for debugging context.
///
/// Extra context (timestamps, retry counts, etc.) helps operators understand
/// the failure context without digging through logs.
#[test]
fn test_with_metadata() {
    let cmd = make_test_command("orders", Uuid::new_v4());
    let dl = AngzarrDeadLetter::from_sequence_mismatch(
        &cmd,
        0,
        5,
        MergeStrategy::MergeManual,
        "test-agg",
    )
    .with_metadata("retry_count", "3")
    .with_metadata("original_timestamp", "2024-01-01T00:00:00Z");

    assert_eq!(dl.metadata.get("retry_count"), Some(&"3".to_string()));
    assert_eq!(
        dl.metadata.get("original_timestamp"),
        Some(&"2024-01-01T00:00:00Z".to_string())
    );
}

// ============================================================================
// Noop Publisher Tests
// ============================================================================

/// Noop publisher always succeeds (for when DLQ is disabled).
#[tokio::test]
async fn test_noop_publisher_succeeds() {
    let publisher = NoopDeadLetterPublisher;
    let cmd = make_test_command("orders", Uuid::new_v4());
    let dl = AngzarrDeadLetter::from_sequence_mismatch(
        &cmd,
        0,
        5,
        MergeStrategy::MergeManual,
        "test-agg",
    );

    let result = publisher.publish(dl).await;
    assert!(result.is_ok());
}

/// Noop publisher reports not configured — callers can check before publishing.
#[test]
fn test_noop_publisher_not_configured() {
    let publisher = NoopDeadLetterPublisher;
    assert!(!publisher.is_configured());
}

// ============================================================================
// Channel Publisher Tests
// ============================================================================

/// Channel publisher for in-memory testing and standalone mode.
#[tokio::test]
async fn test_channel_publisher_sends() {
    let (publisher, mut receiver) = ChannelDeadLetterPublisher::new();
    let cmd = make_test_command("orders", Uuid::new_v4());
    let dl = AngzarrDeadLetter::from_sequence_mismatch(
        &cmd,
        0,
        5,
        MergeStrategy::MergeManual,
        "test-agg",
    );

    publisher.publish(dl).await.unwrap();

    let received = receiver.recv().await.expect("Should receive dead letter");
    assert_eq!(received.domain(), Some("orders"));
    assert_eq!(received.source_component, "test-agg");
}

// ============================================================================
// Config Tests
// ============================================================================

/// Default config has no targets (DLQ disabled).
#[test]
fn test_dlq_config_default_not_configured() {
    let config = DlqConfig::default();
    assert!(!config.is_configured());
}

/// AMQP config factory creates correct structure.
#[test]
fn test_dlq_config_amqp_configured() {
    let config = DlqConfig::amqp("amqp://localhost:5672");
    assert!(config.is_configured());
    assert_eq!(config.targets.len(), 1);
    assert_eq!(config.targets[0].dlq_type, "amqp");
}

/// Kafka config factory creates correct structure.
#[test]
fn test_dlq_config_kafka_configured() {
    let config = DlqConfig::kafka("localhost:9092");
    assert!(config.is_configured());
    assert_eq!(config.targets.len(), 1);
    assert_eq!(config.targets[0].dlq_type, "kafka");
}

// ============================================================================
// Error Tests
// ============================================================================

/// Error display messages match error constants.
///
/// Consistent error messages enable log aggregation and alerting.
#[test]
fn test_dlq_error_display() {
    let err = DlqError::NotConfigured;
    assert_eq!(err.to_string(), errmsg::NOT_CONFIGURED);

    let err = DlqError::PublishFailed("connection refused".to_string());
    assert_eq!(
        err.to_string(),
        format!("{}connection refused", errmsg::PUBLISH_FAILED)
    );

    let err = DlqError::UnknownType("custom".to_string());
    assert_eq!(err.to_string(), format!("{}custom", errmsg::UNKNOWN_TYPE));
}

// ============================================================================
// Proto Conversion Tests
// ============================================================================

/// Proto conversion preserves all fields for serialization.
///
/// Dead letters are serialized for transport/storage. All fields must
/// survive the conversion round-trip.
#[test]
fn test_to_proto_sequence_mismatch() {
    let cmd = make_test_command("orders", Uuid::new_v4());
    let dl = AngzarrDeadLetter::from_sequence_mismatch(
        &cmd,
        5,
        10,
        MergeStrategy::MergeManual,
        "test-agg",
    );

    let proto = dl.to_proto();

    assert!(proto.cover.is_some());
    assert!(proto.payload.is_some());
    assert!(proto.rejection_details.is_some());
    assert!(!proto.rejection_reason.is_empty());
    assert_eq!(proto.source_component, "test-agg");
    assert_eq!(proto.source_component_type, "aggregate");
}

/// Proto conversion preserves the flat captured-error chain.
///
/// Operators read the chain from the status console; frames, `in_app`,
/// mechanism linkage (exception_id / parent_id), release, and
/// server_name must all survive the Rust→proto encode. Chain order is
/// most-causal-first; the originating caught error is the LAST element.
#[test]
fn test_to_proto_preserves_captured_stack_trace() {
    let events = EventBook::default();
    let root_cause = CapturedError {
        r#type: "DbError".to_string(),
        message: "inner".to_string(),
        frames: vec![StackFrame {
            function: "lookup".to_string(),
            module: "db".to_string(),
            file: "src/db.rs".to_string(),
            line: 100,
            in_app: false,
            ..StackFrame::default()
        }],
        mechanism: Some(ExceptionMechanism {
            r#type: "chained".to_string(),
            handled: true,
            exception_id: 0,
            parent_id: 0,
            ..ExceptionMechanism::default()
        }),
        release: "angzarr-saga@1.0.0".to_string(),
        server_name: "pod-a".to_string(),
    };
    let outer = CapturedError {
        r#type: "HandlerError".to_string(),
        message: "outer".to_string(),
        frames: vec![StackFrame {
            function: "handle_event".to_string(),
            module: "orders_saga".to_string(),
            file: "src/saga.rs".to_string(),
            line: 42,
            context_line: "    handle(evt)?;".to_string(),
            in_app: true,
            ..StackFrame::default()
        }],
        mechanism: Some(ExceptionMechanism {
            r#type: "generic".to_string(),
            handled: true,
            exception_id: 1,
            parent_id: 0,
            ..ExceptionMechanism::default()
        }),
        release: "angzarr-saga@1.0.0".to_string(),
        server_name: "pod-a".to_string(),
    };

    let dl = AngzarrDeadLetter::from_event_processing_failure(
        &events,
        "outer",
        1,
        false,
        vec![root_cause, outer],
        "saga",
        "saga",
    );

    let proto = dl.to_proto();

    let proto_details = match proto.rejection_details {
        Some(crate::proto::angzarr_dead_letter::RejectionDetails::EventProcessingFailed(d)) => d,
        _ => panic!("Expected EventProcessingFailed proto details"),
    };
    assert_eq!(proto_details.stack_trace.len(), 2);

    let proto_root = &proto_details.stack_trace[0];
    assert_eq!(proto_root.r#type, "DbError");
    assert_eq!(proto_root.message, "inner");
    assert_eq!(proto_root.frames.len(), 1);
    assert_eq!(proto_root.frames[0].function, "lookup");
    assert!(!proto_root.frames[0].in_app);
    assert_eq!(proto_root.mechanism.as_ref().unwrap().exception_id, 0);
    assert_eq!(proto_root.release, "angzarr-saga@1.0.0");
    assert_eq!(proto_root.server_name, "pod-a");

    let proto_outer = &proto_details.stack_trace[1];
    assert_eq!(proto_outer.r#type, "HandlerError");
    assert_eq!(proto_outer.frames[0].context_line, "    handle(evt)?;");
    assert!(proto_outer.frames[0].in_app);
    let mech = proto_outer.mechanism.as_ref().unwrap();
    assert_eq!(mech.exception_id, 1);
    assert_eq!(mech.parent_id, 0);
}

/// Absent capture round-trips as an empty repeated field.
///
/// Producers may legitimately have no backtrace (e.g., the handler
/// returned a bare error). An empty Rust `Vec` must encode as an
/// empty proto repeated field — not a single default-valued entry.
#[test]
fn test_to_proto_omits_absent_stack_trace() {
    let events = EventBook::default();
    let dl = AngzarrDeadLetter::from_event_processing_failure(
        &events,
        "err",
        1,
        false,
        Vec::new(),
        "saga",
        "saga",
    );

    let proto = dl.to_proto();

    let proto_details = match proto.rejection_details {
        Some(crate::proto::angzarr_dead_letter::RejectionDetails::EventProcessingFailed(d)) => d,
        _ => panic!("Expected EventProcessingFailed proto details"),
    };
    assert!(proto_details.stack_trace.is_empty());
}

// ============================================================================
// Reason Type Tests
// ============================================================================

/// Reason type classification for metrics labeling.
///
/// Different failure reasons have different remediation paths. Metrics
/// by reason type help operators prioritize.
#[test]
fn test_reason_type_sequence_mismatch() {
    let cmd = make_test_command("orders", Uuid::new_v4());
    let dl =
        AngzarrDeadLetter::from_sequence_mismatch(&cmd, 0, 5, MergeStrategy::MergeManual, "test");
    assert_eq!(dl.reason_type(), "sequence_mismatch");
}

#[test]
fn test_reason_type_event_processing_failed() {
    let events = EventBook::default();
    let dl = AngzarrDeadLetter::from_event_processing_failure(
        &events,
        "err",
        1,
        false,
        Vec::new(),
        "saga",
        "saga",
    );
    assert_eq!(dl.reason_type(), "event_processing_failed");
}

/// Unknown reason type for manually constructed dead letters.
#[test]
fn test_reason_type_unknown() {
    let dl = AngzarrDeadLetter {
        cover: None,
        payload: DeadLetterPayload::Events(EventBook::default()),
        rejection_reason: "Unknown reason".to_string(),
        rejection_details: None,
        occurred_at: None,
        metadata: HashMap::new(),
        source_component: "test".to_string(),
        source_component_type: "test".to_string(),
    };
    assert_eq!(dl.reason_type(), "unknown");
}
