//! Tests for no-op DLQ publisher.
//!
//! The noop publisher is the fallback when no DLQ is configured. It logs
//! dead letters at WARN level but takes no other action.
//!
//! Why this matters: Even without a configured DLQ, dead letters must not
//! be silently dropped. Logging ensures operators can see failures in logs.
//!
//! Key behaviors verified:
//! - is_configured() returns false (distinguishes from logging publisher)
//! - publish() always succeeds (never fails on logging)

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

/// is_configured() returns false for noop publisher.
///
/// Unlike the logging publisher which reports is_configured = true,
/// noop reports false to indicate no actual DLQ backend is present.
#[test]
fn test_noop_is_not_configured() {
    let publisher = NoopDeadLetterPublisher;
    assert!(!publisher.is_configured());
}

/// publish() always succeeds.
///
/// Noop publisher cannot fail - it just logs and returns Ok.
#[tokio::test]
async fn test_noop_publish_succeeds() {
    let publisher = NoopDeadLetterPublisher;
    let dead_letter = make_dead_letter("orders", "Test rejection");

    let result = publisher.publish(dead_letter).await;

    assert!(result.is_ok());
}

/// publish() handles missing cover gracefully.
///
/// Even with no cover (malformed input), publish should succeed.
#[tokio::test]
async fn test_noop_publish_handles_missing_cover() {
    let publisher = NoopDeadLetterPublisher;
    let mut dead_letter = make_dead_letter("orders", "Test rejection");
    dead_letter.cover = None;

    let result = publisher.publish(dead_letter).await;

    assert!(result.is_ok());
}
