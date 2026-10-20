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
//! - Filename generation (domain, timestamp, correlation ID)
//! - Format-specific output (JSON vs protobuf)
//!
//! Basic publish and is_configured() tests are covered by Gherkin contract
//! tests in tests/interfaces/features/dlq_publishers.feature.

use super::*;
use crate::dlq::test_support::dead_letter;
use crate::dlq::AngzarrDeadLetter;
use tempfile::TempDir;

fn make_dead_letter(domain: &str, reason: &str) -> AngzarrDeadLetter {
    dead_letter(domain, None, reason)
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

    let dead_letter = make_dead_letter("inventory", "Test");
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

    let dead_letter = make_dead_letter("orders", "Sequence mismatch");
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

    let dead_letter = make_dead_letter("orders", "Test");
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
// Filename Tests
// ============================================================================

/// The correlation's canonical UUID text names the file, so dead letters of
/// one workflow can be found with a glob.
#[test]
fn test_generate_filename_includes_correlation_text() {
    let publisher = FilesystemDeadLetterPublisher {
        path: PathBuf::from("/tmp"),
        format: "json".to_string(),
    };
    let correlation = uuid::Uuid::from_u128(0x0192_0c8e_7b3a_7cc2_9f4e_5a1b_2c3d_4e5f);
    let filename = publisher.generate_filename(&dead_letter("orders", Some(correlation), "Test"));
    assert!(
        filename.contains(&correlation.to_string()),
        "Filename should carry the correlation: {}",
        filename
    );
    assert!(filename.ends_with(".json"));
}

/// Without a correlation the filename says so.
#[test]
fn test_generate_filename_without_correlation() {
    let publisher = FilesystemDeadLetterPublisher {
        path: PathBuf::from("/tmp"),
        format: "json".to_string(),
    };
    let filename = publisher.generate_filename(&make_dead_letter("orders", "Test"));
    assert!(filename.contains("no-correlation"), "{filename}");
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

    let dead_letter = make_dead_letter("orders", "Test");
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

    let dead_letter = make_dead_letter("orders", "Test");
    let filename = publisher.generate_filename(&dead_letter);

    assert!(
        filename.ends_with(".pb"),
        "Proto format should have .pb extension: {}",
        filename
    );
}
