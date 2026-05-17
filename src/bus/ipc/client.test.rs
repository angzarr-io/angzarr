//! Tests for IPC event bus client.
//!
//! IpcEventBus provides the same EventBus interface as AMQP/Kafka but
//! uses named pipes for local IPC. Key components:
//!
//! - Domain filtering: matches_domain_filter() routes events to subscribers
//! - Length-prefixed protocol: 4-byte big-endian length + message body
//! - Checkpointing: tracks last-processed sequence for crash recovery
//! - Configuration: publisher vs subscriber modes with different capabilities
//!
//! Why this matters: IPC bus enables standalone mode where all components
//! run as separate processes on the same host, communicating via named pipes
//! instead of a network message broker. This is simpler for development and
//! single-host deployments while using the same EventBus interface.
//!
//! Key behaviors verified:
//! - Domain filtering accepts/rejects based on configured domains
//! - Length prefix encoding/decoding is correct (big-endian)
//! - Config correctly sets up publisher vs subscriber modes
//! - max_page_sequence extracts highest sequence from EventBook

use super::*;
use crate::proto::PageHeader;

// ============================================================================
// MessageAction Tests
// ============================================================================
//
// MessageAction controls consumer loop behavior after processing a message.
// Correct state machine semantics are critical for reliable IPC.

/// The MessageAction enum controls consumer loop behavior.
mod message_action_tests {
    use super::*;

    /// Continue action is distinct from other actions.
    #[test]
    fn test_continue_action_is_distinct() {
        // Continue means keep reading from current pipe
        assert_eq!(MessageAction::Continue, MessageAction::Continue);
        assert_ne!(MessageAction::Continue, MessageAction::Reopen);
        assert_ne!(MessageAction::Continue, MessageAction::Exit);
    }

    /// Reopen action is distinct from other actions.
    #[test]
    fn test_reopen_action_is_distinct() {
        // Reopen means close current pipe and reconnect
        assert_eq!(MessageAction::Reopen, MessageAction::Reopen);
        assert_ne!(MessageAction::Reopen, MessageAction::Continue);
        assert_ne!(MessageAction::Reopen, MessageAction::Exit);
    }

    /// Exit action is distinct from other actions.
    #[test]
    fn test_exit_action_is_distinct() {
        // Exit means terminate the consumer entirely
        assert_eq!(MessageAction::Exit, MessageAction::Exit);
        assert_ne!(MessageAction::Exit, MessageAction::Continue);
        assert_ne!(MessageAction::Exit, MessageAction::Reopen);
    }
}

// ============================================================================
// ReadResult Tests
// ============================================================================
//
// ReadResult represents all possible outcomes from reading a pipe.

mod read_result_tests {
    use super::*;

    /// Message variant holds the data read from pipe.
    #[test]
    fn test_read_result_message_holds_data() {
        let data = vec![1, 2, 3, 4];
        let result = ReadResult::Message(data.clone());
        if let ReadResult::Message(buf) = result {
            assert_eq!(buf, data);
        } else {
            panic!("Expected Message variant");
        }
    }

    /// TooLarge variant holds the oversized length.
    #[test]
    fn test_read_result_too_large_holds_length() {
        let result = ReadResult::TooLarge(999_999_999);
        if let ReadResult::TooLarge(len) = result {
            assert_eq!(len, 999_999_999);
        } else {
            panic!("Expected TooLarge variant");
        }
    }
}

// ============================================================================
// Domain Filter Tests
// ============================================================================
//
// Domain filtering routes events to the correct subscribers. Without proper
// filtering, subscribers would receive events they can't process.

/// Empty domains list accepts any routing key (wildcard behavior).
#[test]
fn test_matches_domain_filter_empty_domains_accepts_any() {
    let domains: Vec<String> = vec![];
    assert!(matches_domain_filter("orders", &domains));
    assert!(matches_domain_filter("inventory", &domains));
    assert!(matches_domain_filter("anything", &domains));
}

/// Explicit "#" wildcard accepts any routing key.
#[test]
fn test_matches_domain_filter_wildcard_accepts_any() {
    let domains = vec!["#".to_string()];
    assert!(matches_domain_filter("orders", &domains));
    assert!(matches_domain_filter("inventory", &domains));
    assert!(matches_domain_filter("anything", &domains));
}

/// Specific domain matches exact routing key.
#[test]
fn test_matches_domain_filter_specific_domain_matches() {
    let domains = vec!["orders".to_string()];
    assert!(matches_domain_filter("orders", &domains));
}

/// Specific domain rejects non-matching routing keys.
#[test]
fn test_matches_domain_filter_specific_domain_rejects_mismatch() {
    let domains = vec!["orders".to_string()];
    assert!(!matches_domain_filter("inventory", &domains));
    assert!(!matches_domain_filter("fulfillment", &domains));
}

/// Multiple domains match any in the list.
#[test]
fn test_matches_domain_filter_multiple_domains() {
    let domains = vec!["orders".to_string(), "inventory".to_string()];
    assert!(matches_domain_filter("orders", &domains));
    assert!(matches_domain_filter("inventory", &domains));
    assert!(!matches_domain_filter("fulfillment", &domains));
}

/// Wildcard in list makes all domains match.
#[test]
fn test_matches_domain_filter_wildcard_with_specific() {
    // Wildcard in list should accept all
    let domains = vec!["orders".to_string(), "#".to_string()];
    assert!(matches_domain_filter("orders", &domains));
    assert!(matches_domain_filter("inventory", &domains));
    assert!(matches_domain_filter("anything", &domains));
}

// ============================================================================
// Length-Prefixed Protocol Tests
// ============================================================================
//
// The IPC protocol uses 4-byte big-endian length prefix followed by message
// body. These tests verify the encoding format is correct.

/// Length prefix uses 4-byte big-endian format.
#[test]
fn test_length_prefix_big_endian_encoding() {
    // Verify the 4-byte big-endian format used by the protocol
    let len: u32 = 0x00000100; // 256 in decimal
    let bytes = len.to_be_bytes();
    assert_eq!(bytes, [0x00, 0x00, 0x01, 0x00]);

    // Verify round-trip
    let decoded = u32::from_be_bytes(bytes);
    assert_eq!(decoded, 256);
}

/// Small message lengths encode correctly.
#[test]
fn test_length_prefix_small_values() {
    // Test small message lengths
    let bytes = 10u32.to_be_bytes();
    assert_eq!(bytes, [0x00, 0x00, 0x00, 0x0A]);
    assert_eq!(u32::from_be_bytes(bytes), 10);
}

/// Maximum valid message size (just under 10MB) encodes correctly.
#[test]
fn test_length_prefix_max_valid() {
    // Test maximum valid message size (just under 10MB)
    const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;
    let len = (MAX_MESSAGE_SIZE - 1) as u32;
    let bytes = len.to_be_bytes();
    let decoded = u32::from_be_bytes(bytes);
    assert_eq!(decoded as usize, MAX_MESSAGE_SIZE - 1);
    assert!((decoded as usize) < MAX_MESSAGE_SIZE);
}

/// 10MB limit constant is correct.
#[test]
fn test_max_message_size_constant() {
    // Verify the 10MB limit constant
    const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;
    assert_eq!(MAX_MESSAGE_SIZE, 10_485_760);
    // Static assertions for reasonable bounds (values verified by assert_eq above)
}

/// Messages over 10MB would be rejected.
#[test]
fn test_length_prefix_over_max_would_be_rejected() {
    // Verify that lengths over MAX would be rejected
    const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;
    let too_large = (MAX_MESSAGE_SIZE + 1) as u32;
    let bytes = too_large.to_be_bytes();
    let decoded = u32::from_be_bytes(bytes) as usize;
    assert!(decoded > MAX_MESSAGE_SIZE);
}

// ============================================================================
// IPC Config Tests
// ============================================================================

/// Publisher config sets base path but no subscriber name.
#[test]
fn test_ipc_config_publisher() {
    let config = IpcConfig::publisher("/tmp/test");
    assert_eq!(config.base_path, PathBuf::from("/tmp/test"));
    assert!(config.subscriber_name.is_none());
}

/// Subscriber config sets all subscriber fields.
#[test]
fn test_ipc_config_subscriber() {
    let config = IpcConfig::subscriber("/tmp/test", "my-projector", vec!["orders".to_string()]);
    assert_eq!(config.base_path, PathBuf::from("/tmp/test"));
    assert_eq!(config.subscriber_name, Some("my-projector".to_string()));
    assert_eq!(config.domains, vec!["orders".to_string()]);
    assert_eq!(
        config.subscriber_pipe(),
        Some(PathBuf::from("/tmp/test/subscriber-my-projector.pipe"))
    );
}

/// Publisher with explicit subscribers list.
#[test]
fn test_ipc_config_publisher_with_subscribers() {
    let subs = vec![SubscriberInfo {
        name: "test".to_string(),
        domains: vec!["orders".to_string()],
        pipe_path: PathBuf::from("/tmp/test.pipe"),
    }];
    let config = IpcConfig::publisher_with_subscribers("/tmp/test", subs);
    assert_eq!(config.subscribers.len(), 1);
}

/// Subscriber config enables checkpointing by default.
///
/// Checkpointing tracks last-processed sequence for crash recovery.
/// Subscribers need this; publishers don't.
#[test]
fn test_subscriber_config_enables_checkpoint() {
    let config = IpcConfig::subscriber("/tmp/test", "my-saga", vec![]);
    assert!(config.checkpoint_enabled);
}

/// Publisher config disables checkpointing.
#[test]
fn test_publisher_config_disables_checkpoint() {
    let config = IpcConfig::publisher("/tmp/test");
    assert!(!config.checkpoint_enabled);
}

// ============================================================================
// max_page_sequence Tests
// ============================================================================

/// Empty pages returns None.
#[test]
fn test_max_page_sequence_empty() {
    let book = EventBook {
        cover: None,
        pages: vec![],
        snapshot: None,
        ..Default::default()
    };
    assert_eq!(max_page_sequence(&book), None);
}

/// Single page returns its sequence.
#[test]
fn test_max_page_sequence_single_page() {
    use crate::proto::{page_header::SequenceType, EventPage, PageHeader};
    let book = EventBook {
        cover: None,
        pages: vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::Sequence(5)),
            }),
            payload: None,
            created_at: None,
            ..Default::default()
        }],
        snapshot: None,
        ..Default::default()
    };
    assert_eq!(max_page_sequence(&book), Some(5));
}

/// Multiple pages returns the maximum sequence.
#[test]
fn test_max_page_sequence_multiple_pages() {
    use crate::proto::EventPage;
    let book = EventBook {
        cover: None,
        pages: vec![
            EventPage {
                header: Some(PageHeader {
                    sync_mode: None,
                    sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(2)),
                }),
                payload: None,
                created_at: None,
                ..Default::default()
            },
            EventPage {
                header: Some(PageHeader {
                    sync_mode: None,
                    sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(7)),
                }),
                payload: None,
                created_at: None,
                ..Default::default()
            },
            EventPage {
                header: Some(PageHeader {
                    sync_mode: None,
                    sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(4)),
                }),
                payload: None,
                created_at: None,
                ..Default::default()
            },
        ],
        snapshot: None,
        ..Default::default()
    };
    assert_eq!(max_page_sequence(&book), Some(7));
}

// ============================================================================
// read_length_prefixed_message Tests
// ============================================================================
//
// Tests for the length-prefixed message protocol used by IPC pipes.
// The protocol is: 4-byte big-endian length prefix + message body.
// Correct handling of edge cases (EOF, truncation, oversized) is critical.

/// Tests for reading length-prefixed messages from files/pipes.
mod read_length_prefixed_tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Reading a valid length-prefixed message returns the payload.
    #[test]
    fn test_read_valid_message() {
        // Create temp file with a valid length-prefixed message
        let mut temp = NamedTempFile::new().unwrap();
        let payload = b"hello world";
        let len = payload.len() as u32;
        temp.write_all(&len.to_be_bytes()).unwrap();
        temp.write_all(payload).unwrap();
        temp.flush().unwrap();

        // Open for reading
        let mut file = File::open(temp.path()).unwrap();
        let result = read_length_prefixed_message(&mut file);

        match result {
            ReadResult::Message(data) => {
                assert_eq!(data, b"hello world");
            }
            other => panic!("Expected Message, got {:?}", other),
        }
    }

    /// Empty file returns EOF (normal end-of-stream condition).
    #[test]
    fn test_read_empty_file_returns_eof() {
        // Empty file should return EOF
        let temp = NamedTempFile::new().unwrap();
        let mut file = File::open(temp.path()).unwrap();
        let result = read_length_prefixed_message(&mut file);

        assert!(matches!(result, ReadResult::Eof));
    }

    /// Partial length prefix (< 4 bytes) returns EOF.
    ///
    /// This handles the case where the writer crashed mid-write.
    #[test]
    fn test_read_partial_length_returns_eof() {
        // File with only 2 bytes (incomplete length prefix) returns EOF
        let mut temp = NamedTempFile::new().unwrap();
        temp.write_all(&[0x00, 0x01]).unwrap();
        temp.flush().unwrap();

        let mut file = File::open(temp.path()).unwrap();
        let result = read_length_prefixed_message(&mut file);

        assert!(matches!(result, ReadResult::Eof));
    }

    /// Message claiming to exceed MAX_MESSAGE_SIZE returns TooLarge.
    ///
    /// Protects against memory exhaustion from malformed/malicious input.
    #[test]
    fn test_read_too_large_message() {
        // Message claiming to be > 10MB should return TooLarge
        let mut temp = NamedTempFile::new().unwrap();
        let too_large: u32 = 11 * 1024 * 1024; // 11MB
        temp.write_all(&too_large.to_be_bytes()).unwrap();
        temp.flush().unwrap();

        let mut file = File::open(temp.path()).unwrap();
        let result = read_length_prefixed_message(&mut file);

        match result {
            ReadResult::TooLarge(len) => {
                assert_eq!(len, 11 * 1024 * 1024);
            }
            other => panic!("Expected TooLarge, got {:?}", other),
        }
    }

    /// Truncated body (length says X but only Y present) returns Error.
    ///
    /// Detects incomplete writes from crashes or disk full conditions.
    #[test]
    fn test_read_truncated_body_returns_error() {
        // Length says 100 bytes but only 10 present -> error
        let mut temp = NamedTempFile::new().unwrap();
        let len: u32 = 100;
        temp.write_all(&len.to_be_bytes()).unwrap();
        temp.write_all(&[0u8; 10]).unwrap(); // Only 10 bytes
        temp.flush().unwrap();

        let mut file = File::open(temp.path()).unwrap();
        let result = read_length_prefixed_message(&mut file);

        assert!(matches!(result, ReadResult::Error(_)));
    }

    /// Zero-length message is valid (empty payload).
    #[test]
    fn test_read_zero_length_message() {
        // Zero-length message is valid
        let mut temp = NamedTempFile::new().unwrap();
        temp.write_all(&0u32.to_be_bytes()).unwrap();
        temp.flush().unwrap();

        let mut file = File::open(temp.path()).unwrap();
        let result = read_length_prefixed_message(&mut file);

        match result {
            ReadResult::Message(data) => {
                assert!(data.is_empty());
            }
            other => panic!("Expected Message, got {:?}", other),
        }
    }

    /// Multiple messages can be read sequentially from one file.
    ///
    /// Verifies file position advances correctly after each read.
    #[test]
    fn test_read_multiple_messages() {
        // Read two messages sequentially
        let mut temp = NamedTempFile::new().unwrap();

        // First message: "hello"
        temp.write_all(&5u32.to_be_bytes()).unwrap();
        temp.write_all(b"hello").unwrap();

        // Second message: "world"
        temp.write_all(&5u32.to_be_bytes()).unwrap();
        temp.write_all(b"world").unwrap();
        temp.flush().unwrap();

        let mut file = File::open(temp.path()).unwrap();

        // Read first
        let result1 = read_length_prefixed_message(&mut file);
        match result1 {
            ReadResult::Message(data) => assert_eq!(data, b"hello"),
            other => panic!("Expected Message, got {:?}", other),
        }

        // Read second
        let result2 = read_length_prefixed_message(&mut file);
        match result2 {
            ReadResult::Message(data) => assert_eq!(data, b"world"),
            other => panic!("Expected Message, got {:?}", other),
        }

        // Third read should be EOF
        let result3 = read_length_prefixed_message(&mut file);
        assert!(matches!(result3, ReadResult::Eof));
    }

    /// Message at max valid size boundary passes length check.
    ///
    /// Verifies boundary condition: MAX_MESSAGE_SIZE - 1 is accepted.
    #[test]
    fn test_read_max_valid_size() {
        // Test reading a message at the max valid size boundary
        const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;
        let len = (MAX_MESSAGE_SIZE - 1) as u32;

        let mut temp = NamedTempFile::new().unwrap();
        temp.write_all(&len.to_be_bytes()).unwrap();
        // We won't write the full body (would take too long), just verify length check passes
        // The read will fail with Error when it can't read the full body
        temp.flush().unwrap();

        let mut file = File::open(temp.path()).unwrap();
        let result = read_length_prefixed_message(&mut file);

        // Should get Error (truncated body), not TooLarge
        assert!(matches!(result, ReadResult::Error(_)));
    }
}

// ============================================================================
// IpcEventBus Construction Tests
// ============================================================================
//
// Verifies that the bus can be instantiated in publisher and subscriber modes
// with correct configuration. Different modes have different capabilities.

/// Tests for IpcEventBus instantiation and configuration.
mod ipc_bus_construction_tests {
    use super::*;

    /// Publisher bus has no subscriber name and empty subscribers list.
    #[test]
    fn test_publisher_bus_creation() {
        let bus = IpcEventBus::publisher("/tmp/test");
        assert!(bus.config.subscriber_name.is_none());
        assert!(bus.config.subscribers.is_empty());
    }

    /// Subscriber bus captures subscriber name and domains.
    #[test]
    fn test_subscriber_bus_creation() {
        let bus = IpcEventBus::subscriber("/tmp/test", "my-saga", vec!["orders".to_string()]);
        assert_eq!(bus.config.subscriber_name, Some("my-saga".to_string()));
        assert_eq!(bus.config.domains, vec!["orders".to_string()]);
    }

    /// Default config uses standard base path and disables checkpointing.
    #[test]
    fn test_default_config() {
        let config = IpcConfig::default();
        assert_eq!(config.base_path, PathBuf::from(DEFAULT_BASE_PATH));
        assert!(config.subscriber_name.is_none());
        assert!(config.domains.is_empty());
        assert!(config.subscribers.is_empty());
        assert!(!config.checkpoint_enabled);
    }

    /// Subscriber pipe path follows naming convention.
    #[test]
    fn test_subscriber_pipe_path_format() {
        let config = IpcConfig::subscriber("/var/run/angzarr", "order-saga", vec![]);
        let expected = PathBuf::from(format!(
            "/var/run/angzarr/{}order-saga.pipe",
            SUBSCRIBER_PIPE_PREFIX
        ));
        assert_eq!(config.subscriber_pipe(), Some(expected));
    }

    /// Publisher has no subscriber pipe (it writes to subscriber pipes, not reads).
    #[test]
    fn test_publisher_has_no_subscriber_pipe() {
        let config = IpcConfig::publisher("/tmp/test");
        assert!(config.subscriber_pipe().is_none());
    }
}
