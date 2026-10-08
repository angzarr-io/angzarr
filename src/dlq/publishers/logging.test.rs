//! Tests for logging-based DLQ publisher.
//!
//! The logging publisher is a last-resort fallback that logs dead letters
//! at WARN level. Unlike NoopDeadLetterPublisher, it reports is_configured()
//! true because it actively logs (observability value).
//!
//! Why this matters: Even when all other DLQ targets fail, logging ensures
//! the dead letter is captured somewhere (log aggregators like Splunk/Datadog
//! can alert on these).
//!
//! Basic publish/is_configured tests are covered by Gherkin contract tests.
//! Only edge cases remain here.

use super::*;
use crate::dlq::{AngzarrDeadLetter, DeadLetterPayload};
use crate::proto::{
    command_page, page_header, CommandBook, CommandPage, Cover, MergeStrategy, PageHeader,
    Uuid as ProtoUuid,
};
use std::collections::HashMap;
use uuid::Uuid;

fn make_test_command(domain: &str) -> CommandBook {
    let root = Uuid::new_v4();
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: "test-corr-123".to_string(),
            edition: None,
            ext: None,
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

fn make_dead_letter(domain: &str, reason: &str) -> AngzarrDeadLetter {
    let cmd = make_test_command(domain);
    AngzarrDeadLetter {
        cover: cmd.cover.clone(),
        payload: DeadLetterPayload::Command(cmd),
        rejection_reason: reason.to_string(),
        rejection_details: None,
        occurred_at: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
        metadata: HashMap::new(),
        source_component: "test-component".to_string(),
        source_component_type: "aggregate".to_string(),
    }
}

/// Missing cover handled gracefully — logs with empty correlation ID.
///
/// Dead letters may come from malformed commands. The publisher shouldn't
/// crash on edge cases.
#[tokio::test]
async fn test_logging_publisher_handles_missing_correlation() {
    let publisher = LoggingDeadLetterPublisher;
    let mut dead_letter = make_dead_letter("orders", "Test rejection");
    dead_letter.cover = None; // No cover means no correlation ID

    let result = publisher.publish(dead_letter).await;

    assert!(result.is_ok(), "Should handle missing cover gracefully");
}

/// is_configured() returns true for logging publisher.
///
/// Unlike noop publisher, logging publisher reports as configured because
/// it actively logs (provides observability value).
#[test]
fn test_logging_is_configured() {
    let publisher = LoggingDeadLetterPublisher;
    assert!(publisher.is_configured());
}

/// publish() always succeeds.
///
/// Logging cannot fail - writes to stdout/stderr are infallible.
#[tokio::test]
async fn test_logging_publish_succeeds() {
    let publisher = LoggingDeadLetterPublisher;
    let dead_letter = make_dead_letter("inventory", "Conflict on sequence");

    let result = publisher.publish(dead_letter).await;

    assert!(result.is_ok());
}
