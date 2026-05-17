//! Tests for offload storage DLQ publishers.
//!
//! Offload publishers store entire dead letters to remote storage (filesystem,
//! GCS, S3). Unlike the regular filesystem publisher (which formats as JSON),
//! these store protobuf for efficient recovery and replay.
//!
//! Why this matters: For high-volume systems, protobuf is more compact than
//! JSON. The date-based path structure enables lifecycle policies (delete
//! files older than X days) without custom tooling.
//!
//! Key behaviors:
//! - Date-based path structure for easy browsing/cleanup
//! - Domain included in path for filtering
//! - UUID suffix prevents collisions
//!
//! Basic publish tests covered by Gherkin contracts. Implementation-specific
//! path format tests remain here.

use super::*;
use crate::dlq::{AngzarrDeadLetter, DeadLetterPayload, DeadLetterPublisher};
use crate::proto::{
    command_page, page_header, CommandBook, CommandPage, Cover, MergeStrategy, PageHeader,
    Uuid as ProtoUuid,
};
use std::collections::HashMap;
use tempfile::TempDir;
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

// ============================================================================
// Filesystem Offload Tests
// ============================================================================

/// Domain directory created under base path.
///
/// Files organized by domain for easy filtering during manual recovery.
#[tokio::test]
async fn test_offload_filesystem_file_path_includes_domain() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = OffloadFilesystemDlqConfig {
        base_path: temp_dir.path().to_string_lossy().to_string(),
        prefix: "dlq/".to_string(),
    };

    let publisher = OffloadFilesystemDlqPublisher::new(&config)
        .await
        .expect("Failed to create publisher");

    let dead_letter = make_dead_letter("inventory", "Test");
    publisher
        .publish(dead_letter)
        .await
        .expect("Failed to publish");

    // Check that domain directory exists
    let domain_base = temp_dir.path().join("dlq/inventory");
    assert!(
        domain_base.exists(),
        "Domain directory should exist: {:?}",
        domain_base
    );
}

/// Key format: prefix/domain/YYYY/MM/DD/timestamp_uuid.pb
#[test]
fn test_generate_key_format() {
    let publisher = OffloadFilesystemDlqPublisher {
        base_path: PathBuf::from("/tmp"),
        prefix: "dlq/".to_string(),
    };

    let dead_letter = make_dead_letter("orders", "Test");
    let key = publisher.generate_key(&dead_letter);

    // Should start with prefix
    assert!(
        key.starts_with("dlq/"),
        "Key should start with prefix: {}",
        key
    );
    // Should contain domain
    assert!(key.contains("orders"), "Key should contain domain: {}", key);
    // Should end with .pb
    assert!(key.ends_with(".pb"), "Key should end with .pb: {}", key);
}

/// Date structure enables time-based retention policies.
///
/// Paths like dlq/orders/2024/01/15/ allow simple lifecycle rules
/// (e.g., delete everything older than 30 days).
#[test]
fn test_generate_key_includes_date_structure() {
    let publisher = OffloadFilesystemDlqPublisher {
        base_path: PathBuf::from("/tmp"),
        prefix: "".to_string(),
    };

    let dead_letter = make_dead_letter("orders", "Test");
    let key = publisher.generate_key(&dead_letter);

    // Should contain date structure (YYYY/MM/DD)
    let parts: Vec<&str> = key.split('/').collect();
    // Format: domain/YYYY/MM/DD/timestamp_uuid.pb
    assert!(
        parts.len() >= 5,
        "Should have date structure in path: {}",
        key
    );
}
