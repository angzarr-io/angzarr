//! Tests for filesystem-based DLQ publisher.
//!
//! The filesystem publisher writes dead letters to files for debugging and
//! local persistence. Unlike offload publishers (binary protobuf), this one
//! writes human-readable JSON by default.
//!
//! Why this matters: For local development and debugging, JSON files are
//! easier to inspect than protobuf. The filename includes domain and
//! correlation ID for easy filtering with grep/find.
//!
//! Key behaviors:
//! - Filename generation (domain, timestamp, sanitized correlation ID)
//! - Format-specific output (JSON vs protobuf)
//! - Character sanitization in filenames (prevents path traversal)
//!
//! Basic publish and is_configured() tests are covered by Gherkin contract
//! tests in tests/interfaces/features/dlq_publishers.feature.

use super::*;
use crate::dlq::{AngzarrDeadLetter, DeadLetterPayload};
use crate::proto::{
    command_page, CommandBook, CommandPage, Cover, MergeStrategy, Uuid as ProtoUuid,
};
use crate::proto::{page_header, PageHeader};
use std::collections::HashMap;
use tempfile::TempDir;
use uuid::Uuid;

// ============================================================================
// Test Helpers
// ============================================================================

fn make_test_command(domain: &str, correlation_id: &str) -> CommandBook {
    let root = Uuid::new_v4();
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: correlation_id.to_string(),
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

fn make_dead_letter(domain: &str, correlation_id: &str, reason: &str) -> AngzarrDeadLetter {
    let cmd = make_test_command(domain, correlation_id);
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
// Filename Generation Tests
// ============================================================================

/// Filename includes domain for easy filtering/grep.
#[tokio::test]
async fn test_filesystem_publisher_filename_includes_domain() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = FilesystemDlqConfig {
        path: temp_dir.path().to_string_lossy().to_string(),
        format: "json".to_string(),
        max_files: 0,
    };

    let publisher = FilesystemDeadLetterPublisher::new(&config)
        .await
        .expect("Failed to create publisher");

    let dead_letter = make_dead_letter("inventory", "corr-abc", "Test");
    publisher
        .publish(dead_letter)
        .await
        .expect("Failed to publish");

    // Check filename contains domain
    let mut entries = fs::read_dir(temp_dir.path())
        .await
        .expect("Failed to read dir");
    let entry = entries
        .next_entry()
        .await
        .expect("Failed to read entry")
        .expect("No file found");
    let filename = entry.file_name().to_string_lossy().to_string();

    assert!(
        filename.contains("inventory"),
        "Filename should contain domain: {}",
        filename
    );
}

// ============================================================================
// Format Tests
// ============================================================================

/// JSON format includes all fields needed for debugging/recovery.
#[tokio::test]
async fn test_filesystem_publisher_json_format_contains_fields() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = FilesystemDlqConfig {
        path: temp_dir.path().to_string_lossy().to_string(),
        format: "json".to_string(),
        max_files: 0,
    };

    let publisher = FilesystemDeadLetterPublisher::new(&config)
        .await
        .expect("Failed to create publisher");

    let dead_letter = make_dead_letter("orders", "test-corr", "Sequence mismatch");
    publisher
        .publish(dead_letter)
        .await
        .expect("Failed to publish");

    // Read file and parse JSON
    let mut entries = fs::read_dir(temp_dir.path())
        .await
        .expect("Failed to read dir");
    let entry = entries
        .next_entry()
        .await
        .expect("Failed to read entry")
        .expect("No file found");
    let content = fs::read_to_string(entry.path())
        .await
        .expect("Failed to read file");
    let json: serde_json::Value = serde_json::from_str(&content).expect("Failed to parse JSON");

    assert_eq!(json["domain"], "orders");
    assert_eq!(json["rejection_reason"], "Sequence mismatch");
    assert!(
        json["payload_base64"].is_string(),
        "Should have base64 payload"
    );
}

/// Protobuf format writes binary (not JSON) for compact storage.
#[tokio::test]
async fn test_filesystem_publisher_protobuf_format_writes_binary() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = FilesystemDlqConfig {
        path: temp_dir.path().to_string_lossy().to_string(),
        format: "protobuf".to_string(),
        max_files: 0,
    };

    let publisher = FilesystemDeadLetterPublisher::new(&config)
        .await
        .expect("Failed to create publisher");

    let dead_letter = make_dead_letter("orders", "test-corr", "Test");
    publisher
        .publish(dead_letter)
        .await
        .expect("Failed to publish");

    // Read file and verify it's binary (not valid UTF-8 JSON)
    let mut entries = fs::read_dir(temp_dir.path())
        .await
        .expect("Failed to read dir");
    let entry = entries
        .next_entry()
        .await
        .expect("Failed to read entry")
        .expect("No file found");
    let content = fs::read(entry.path()).await.expect("Failed to read file");

    // Protobuf should not be valid JSON
    assert!(
        serde_json::from_slice::<serde_json::Value>(&content).is_err(),
        "Protobuf format should not be valid JSON"
    );
    // But should have some data
    assert!(!content.is_empty(), "File should not be empty");
}

// ============================================================================
// Filename Sanitization Tests
// ============================================================================

/// Special characters in correlation ID replaced with underscore.
///
/// Prevents path traversal and invalid filenames. Correlation IDs from
/// external systems may contain arbitrary characters.
#[test]
fn test_generate_filename_sanitizes_correlation_id() {
    let publisher = FilesystemDeadLetterPublisher {
        path: PathBuf::from("/tmp"),
        format: "json".to_string(),
    };

    // Create dead letter with special chars in correlation ID
    let mut dead_letter = make_dead_letter("orders", "test/corr:123?bad", "Test");
    dead_letter.cover.as_mut().unwrap().correlation_id = "test/corr:123?bad".to_string();

    let filename = publisher.generate_filename(&dead_letter);

    // Should not contain special chars
    assert!(!filename.contains('/'), "Filename should not contain /");
    assert!(!filename.contains(':'), "Filename should not contain :");
    assert!(!filename.contains('?'), "Filename should not contain ?");
    assert!(filename.contains(".json"), "Should have .json extension");
}

/// Hyphens preserved — common in UUIDs and readable IDs.
#[test]
fn test_generate_filename_preserves_hyphens() {
    let publisher = FilesystemDeadLetterPublisher {
        path: PathBuf::from("/tmp"),
        format: "json".to_string(),
    };

    // Correlation ID with hyphens should be preserved
    let mut dead_letter = make_dead_letter("orders", "test-corr-123", "Test");
    dead_letter.cover.as_mut().unwrap().correlation_id = "test-corr-123".to_string();

    let filename = publisher.generate_filename(&dead_letter);

    // Verify hyphen is preserved (not replaced with _)
    assert!(
        filename.contains("test-corr-123"),
        "Filename should preserve hyphens: {}",
        filename
    );
}

/// Underscores preserved — common in snake_case IDs.
#[test]
fn test_generate_filename_preserves_underscores() {
    let publisher = FilesystemDeadLetterPublisher {
        path: PathBuf::from("/tmp"),
        format: "json".to_string(),
    };

    // Correlation ID with underscores should be preserved
    let mut dead_letter = make_dead_letter("orders", "test_corr_123", "Test");
    dead_letter.cover.as_mut().unwrap().correlation_id = "test_corr_123".to_string();

    let filename = publisher.generate_filename(&dead_letter);

    // Verify underscore is preserved
    assert!(
        filename.contains("test_corr_123"),
        "Filename should preserve underscores: {}",
        filename
    );
}

// ============================================================================
// Extension Tests
// ============================================================================

/// Protobuf format uses .pb extension.
#[test]
fn test_generate_filename_protobuf_extension() {
    let publisher = FilesystemDeadLetterPublisher {
        path: PathBuf::from("/tmp"),
        format: "protobuf".to_string(),
    };

    let dead_letter = make_dead_letter("orders", "test-corr", "Test");
    let filename = publisher.generate_filename(&dead_letter);

    assert!(
        filename.ends_with(".pb"),
        "Protobuf format should have .pb extension: {}",
        filename
    );
}

/// "proto" format alias also uses .pb extension.
#[test]
fn test_generate_filename_proto_extension() {
    let publisher = FilesystemDeadLetterPublisher {
        path: PathBuf::from("/tmp"),
        format: "proto".to_string(),
    };

    let dead_letter = make_dead_letter("orders", "test-corr", "Test");
    let filename = publisher.generate_filename(&dead_letter);

    assert!(
        filename.ends_with(".pb"),
        "Proto format should have .pb extension: {}",
        filename
    );
}
