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

// ============================================================================
// Handler Failure Checkpoint Gating Tests (C-10)
// ============================================================================
//
// The IPC consumer must NOT advance the checkpoint when a handler returns
// `Err`. Advancing on failure permanently loses the event: a restarted
// subscriber would skip past the failed event entirely (checkpoint says
// "already processed seq=N"), so the broker-equivalent property of
// at-least-once delivery is silently broken.
//
// The canonical correct pattern lives in `src/bus/kafka/bus.rs:149` — the
// Kafka consumer only commits the offset when `dispatch_to_handlers`
// returns `true`. Before C-10, IPC's local `dispatch_to_handlers` helper
// discarded the return value of the shared `bus::dispatch::dispatch_to_handlers`
// (in fact didn't call the shared helper at all — it called handlers
// directly and unconditionally `checkpoint.update`d afterwards).
//
// These tests pin down: handler `Err` ⇒ checkpoint stays at the prior
// value; handler `Ok` ⇒ checkpoint advances.
mod handler_failure_checkpoint_tests {
    use super::*;
    use crate::bus::BusError;
    use crate::proto::{event_page, page_header::SequenceType, Cover, EventPage, PageHeader, Uuid};
    use futures::future::BoxFuture;
    use prost_types::Any;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc as StdArc;
    use tempfile::tempdir;

    /// Test handler that always succeeds, tracking call count.
    struct AlwaysOkHandler {
        call_count: StdArc<AtomicUsize>,
    }

    impl EventHandler for AlwaysOkHandler {
        fn handle(
            &self,
            _book: Arc<EventBook>,
        ) -> BoxFuture<'static, std::result::Result<(), BusError>> {
            let count = self.call_count.clone();
            Box::pin(async move {
                count.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(())
            })
        }
    }

    /// Test handler that always fails, tracking call count.
    struct AlwaysErrHandler {
        call_count: StdArc<AtomicUsize>,
    }

    impl EventHandler for AlwaysErrHandler {
        fn handle(
            &self,
            _book: Arc<EventBook>,
        ) -> BoxFuture<'static, std::result::Result<(), BusError>> {
            let count = self.call_count.clone();
            Box::pin(async move {
                count.fetch_add(1, AtomicOrdering::SeqCst);
                Err(BusError::ProjectorFailed {
                    name: "ipc-c10-test".to_string(),
                    message: "synthetic handler failure".to_string(),
                })
            })
        }
    }

    /// Build an EventBook carrying a stable root and a known sequence number,
    /// so the checkpoint key (`{domain}.{root_hex}` -> last seq) is well-defined.
    fn make_book_with_sequence(domain: &str, root: &[u8], sequence: u32) -> EventBook {
        EventBook {
            cover: Some(Cover {
                domain: domain.to_string(),
                root: Some(Uuid {
                    value: root.to_vec(),
                }),
                correlation_id: "c10-test".to_string(),
                edition: None,
                ext: None,
            }),
            pages: vec![EventPage {
                header: Some(PageHeader {
                    sync_mode: None,
                    sequence_type: Some(SequenceType::Sequence(sequence)),
                }),
                created_at: None,
                payload: Some(event_page::Payload::Event(Any {
                    type_url: "type.example/TestEvent".to_string(),
                    value: vec![1, 2, 3, sequence as u8],
                })),
                ..Default::default()
            }],
            snapshot: None,
            next_sequence: sequence + 1,
        }
    }

    fn make_checkpoint() -> (tempfile::TempDir, StdArc<Checkpoint>) {
        let dir = tempdir().unwrap();
        let config = CheckpointConfig::for_subscriber(dir.path(), "c10-handler-tests");
        let checkpoint = StdArc::new(Checkpoint::new(config));
        (dir, checkpoint)
    }

    /// Run the synchronous `dispatch_to_handlers` helper on a blocking task
    /// so its inner `rt.block_on(...)` doesn't trip "Cannot start a runtime
    /// from within a runtime" when called from a `#[tokio::test]`.
    async fn run_dispatch(
        book: Arc<EventBook>,
        handlers: Arc<RwLock<Vec<Box<dyn EventHandler>>>>,
        checkpoint: StdArc<Checkpoint>,
    ) {
        let rt = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            dispatch_to_handlers(book, &handlers, &checkpoint, &rt);
        })
        .await
        .expect("spawn_blocking join")
    }

    /// Baseline behavior: when ALL handlers succeed, the checkpoint advances
    /// to the event's max sequence. This is the "ack" semantic — equivalent
    /// to Kafka's `commit_message` on a successful dispatch.
    #[tokio::test]
    async fn handler_ok_advances_checkpoint() {
        let (_dir, checkpoint) = make_checkpoint();

        let count = StdArc::new(AtomicUsize::new(0));
        let handlers: Arc<RwLock<Vec<Box<dyn EventHandler>>>> =
            Arc::new(RwLock::new(vec![Box::new(AlwaysOkHandler {
                call_count: count.clone(),
            })]));

        let root = b"c10-root-ok".to_vec();
        let book = Arc::new(make_book_with_sequence("orders", &root, 42));

        run_dispatch(book, handlers, checkpoint.clone()).await;

        assert_eq!(
            count.load(AtomicOrdering::SeqCst),
            1,
            "handler must be invoked exactly once"
        );

        let stored = checkpoint.get("orders", &root).await;
        assert_eq!(
            stored,
            Some(42),
            "successful handler must advance checkpoint to event sequence"
        );
    }

    /// C-10 REGRESSION: when a handler returns `Err`, the checkpoint must
    /// NOT advance — otherwise the consumer permanently skips past a
    /// never-processed event on the next restart.
    ///
    /// Baseline (pre-fix) advances the checkpoint regardless of the handler
    /// result, so this test fails until the fix gates the
    /// `checkpoint.update(...)` call on the dispatch success bool.
    #[tokio::test]
    async fn handler_err_does_not_advance_checkpoint() {
        let (_dir, checkpoint) = make_checkpoint();

        let count = StdArc::new(AtomicUsize::new(0));
        let handlers: Arc<RwLock<Vec<Box<dyn EventHandler>>>> =
            Arc::new(RwLock::new(vec![Box::new(AlwaysErrHandler {
                call_count: count.clone(),
            })]));

        let root = b"c10-root-err".to_vec();
        let book = Arc::new(make_book_with_sequence("orders", &root, 99));

        run_dispatch(book, handlers, checkpoint.clone()).await;

        assert_eq!(
            count.load(AtomicOrdering::SeqCst),
            1,
            "handler must be invoked exactly once even on failure"
        );

        let stored = checkpoint.get("orders", &root).await;
        assert_eq!(
            stored, None,
            "failed handler must NOT advance checkpoint — restarted consumer \
             must re-deliver this event, not skip past it (C-10 regression)"
        );
    }

    /// Mixed: one handler succeeds, one fails. The whole dispatch counts as
    /// failed (the failed handler hasn't processed this event), so the
    /// checkpoint must still NOT advance. This matches the documented
    /// contract on `bus::dispatch::dispatch_to_handlers`: "Returns `true`
    /// if all handlers succeeded, `false` if any failed."
    ///
    /// Operational note: the successful handler will see the event again
    /// on redelivery. Idempotency at the handler level (sequence dedup,
    /// external_id) is what makes this safe — that's the same property
    /// Kafka relies on (per `kafka/bus.rs:148`).
    #[tokio::test]
    async fn mixed_ok_err_does_not_advance_checkpoint() {
        let (_dir, checkpoint) = make_checkpoint();

        let ok_count = StdArc::new(AtomicUsize::new(0));
        let err_count = StdArc::new(AtomicUsize::new(0));
        let handlers: Arc<RwLock<Vec<Box<dyn EventHandler>>>> = Arc::new(RwLock::new(vec![
            Box::new(AlwaysOkHandler {
                call_count: ok_count.clone(),
            }),
            Box::new(AlwaysErrHandler {
                call_count: err_count.clone(),
            }),
        ]));

        let root = b"c10-root-mixed".to_vec();
        let book = Arc::new(make_book_with_sequence("orders", &root, 7));

        run_dispatch(book, handlers, checkpoint.clone()).await;

        assert_eq!(ok_count.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(err_count.load(AtomicOrdering::SeqCst), 1);

        assert_eq!(
            checkpoint.get("orders", &root).await,
            None,
            "any handler failure must block checkpoint advance (C-10)"
        );
    }
}

// ============================================================================
// Concurrent Publisher Framing Tests (C-09)
// ============================================================================
//
// POSIX guarantees write() atomicity on a pipe only for buffers ≤ PIPE_BUF
// (4 KiB on Linux). The publish path performs TWO sequential `write_all` calls
// per message (4-byte length prefix, then body). When multiple tokio tasks
// publish concurrently to the same pipe, the kernel may interleave those
// writes, corrupting the frame and desyncing the reader indefinitely.
//
// The second variant: when the first `write_all(&len_bytes)` succeeds and the
// second `write_all(&serialized)` fails with `WouldBlock`, the publisher
// returns Err but leaves a 4-byte phantom prefix in the pipe. The reader then
// reads the next publisher's frame body as if it were a new message length.
//
// These tests verify that N concurrent publishers each emit intact,
// length-prefixed, decode-clean EventBook frames to a shared pipe.
mod concurrent_publisher_framing_tests {
    use super::*;
    use crate::proto::Cover;
    use std::io::Read as IoRead;
    use std::sync::Arc as StdArc;
    use std::thread;
    use std::time::Duration;
    use tempfile::tempdir;

    /// Build an EventBook whose encoded size is well above PIPE_BUF (4 KiB).
    ///
    /// The cover's correlation_id is the test marker — each publisher uses a
    /// distinct prefix so the reader can verify every message arrived intact
    /// and originated from a known publisher.
    fn make_large_event_book(marker: &str, padding_bytes: usize) -> EventBook {
        // Pad correlation_id with a recognizable filler so the encoded message
        // exceeds PIPE_BUF.
        let mut correlation_id = String::with_capacity(marker.len() + padding_bytes);
        correlation_id.push_str(marker);
        correlation_id.extend(std::iter::repeat_n('x', padding_bytes));
        EventBook {
            cover: Some(Cover {
                domain: "publisher-test".to_string(),
                root: None,
                correlation_id,
                edition: None,
                ext: None,
            }),
            ..Default::default()
        }
    }

    /// Read every length-prefixed frame from `reader` until EOF.
    ///
    /// Returns the list of decoded body bytes for each frame. If a length
    /// prefix points beyond the available bytes (truncation) or the body
    /// cannot be fully read, returns an Err describing the desync.
    fn drain_frames(reader: &mut impl IoRead) -> std::io::Result<Vec<Vec<u8>>> {
        let mut frames = Vec::new();
        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            // 10 MB matches the production MAX_MESSAGE_SIZE — anything larger
            // proves the framing is corrupt (interleaved bytes happened to
            // form a plausible-looking length prefix).
            if len > 10 * 1024 * 1024 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("frame length {} exceeds MAX_MESSAGE_SIZE", len),
                ));
            }
            let mut body = vec![0u8; len];
            reader.read_exact(&mut body)?;
            frames.push(body);
        }
        Ok(frames)
    }

    /// Concurrent publishers writing >PIPE_BUF messages must not corrupt
    /// the length-prefixed framing.
    ///
    /// Spawns N publisher *OS threads* (not tokio tasks — we want real
    /// parallelism on multiple cores, with a barrier to synchronize the
    /// start of each publisher's write). Each publisher publishes one
    /// EventBook ≫ PIPE_BUF (so the kernel cannot serve any single
    /// `write()` atomically). A blocking reader thread reads the pipe
    /// until EOF.
    ///
    /// On baseline (before C-09), the 4-byte length prefix and body are
    /// two separate `write_all` calls on an `O_NONBLOCK` pipe; the kernel
    /// may interleave them so the reader sees a length prefix followed by
    /// the WRONG body and desyncs. Many iterations are used to make the
    /// race deterministic — even a small race window will manifest given
    /// enough trips through the critical section.
    ///
    /// After the C-09 fix (per-pipe mutex + single-buffer write), each
    /// frame lands in the pipe intact regardless of contention.
    #[test]
    fn test_concurrent_publishers_preserve_framing() {
        // Multi-thread tokio runtime so `bus.publish().await` can resolve
        // its synchronous syscalls without forcing tasks onto one OS thread.
        let rt = StdArc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(8)
                .enable_all()
                .build()
                .unwrap(),
        );

        // N=8 publisher threads, each publishing M=32 iterations. The wider
        // the bus contention AND the more trips through the critical
        // section, the more reliably the interleave bug manifests. Even a
        // small per-write race window will fire within 256 frames.
        //
        // Each message is padded to ~6 KiB body — bigger than PIPE_BUF
        // (4 KiB on Linux) so the kernel cannot guarantee atomicity on a
        // single `write()`, but small enough that the kernel typically
        // accepts each `write_all` in one syscall when the pipe has space.
        // The reader drains the pipe in a tight loop so back-pressure
        // (which would trigger the orthogonal WouldBlock phantom-prefix
        // variant) is avoided here — that variant has its own dedicated
        // test below.
        const NUM_PUBLISHERS: usize = 8;
        const ITERATIONS: usize = 32;
        const PADDING: usize = 6 * 1024;
        const TOTAL_FRAMES: usize = NUM_PUBLISHERS * ITERATIONS;

        let dir = tempdir().unwrap();
        let pipe_path = dir.path().join("subscriber-c09.pipe");

        // Create the FIFO. The broker uses mkfifo with S_IRUSR|S_IWUSR.
        {
            use nix::sys::stat::Mode;
            use nix::unistd::mkfifo;
            mkfifo(&pipe_path, Mode::S_IRUSR | Mode::S_IWUSR).expect("mkfifo");
        }

        // Pre-build the SubscriberInfo so the publisher knows where to write.
        let subscriber = SubscriberInfo {
            name: "c09".to_string(),
            domains: vec![], // accept all
            pipe_path: pipe_path.clone(),
        };
        let config = IpcConfig::publisher_with_subscribers(dir.path(), vec![subscriber]);
        let bus = StdArc::new(IpcEventBus::new(config));

        // Reader: open the FIFO in blocking mode in a std thread. The open()
        // call blocks until at least one writer connects.
        let pipe_path_reader = pipe_path.clone();
        let reader_handle = thread::spawn(move || -> std::io::Result<Vec<Vec<u8>>> {
            let mut reader = File::open(&pipe_path_reader)?;
            // Enlarge the kernel pipe buffer so producers don't have to
            // wait for the reader between every write — keeps the test
            // focused on the framing-interleave bug, not throughput.
            // Linux F_SETPIPE_SZ; capped at /proc/sys/fs/pipe-max-size
            // (typically 1 MiB).
            unsafe {
                use std::os::unix::io::AsRawFd;
                const F_SETPIPE_SZ: i32 = 1031;
                let _ = libc::fcntl(reader.as_raw_fd(), F_SETPIPE_SZ, 1024 * 1024);
            }
            drain_frames(&mut reader)
        });

        // Hold one writer FD open from the test thread for the entire
        // duration of the test. The IpcEventBus opens a fresh writer FD
        // per publish; under serialization-by-mutex, there are brief
        // windows where the previous publisher's FD has closed but the
        // next has not yet opened. POSIX FIFO semantics make the reader
        // see EOF in any such window, which would prematurely terminate
        // `drain_frames` even though many more writes are coming. The
        // sentinel writer FD prevents this without interfering with the
        // test's writes (it never writes anything). Retry the open until
        // the reader's open() lands (open with O_NONBLOCK returns ENXIO
        // if no reader is yet attached).
        let sentinel_writer = {
            let mut attempt = 0;
            loop {
                match OpenOptions::new()
                    .write(true)
                    .custom_flags(libc::O_NONBLOCK)
                    .open(&pipe_path)
                {
                    Ok(f) => break f,
                    Err(e) if attempt < 50 => {
                        attempt += 1;
                        thread::sleep(Duration::from_millis(10));
                        if attempt == 50 {
                            panic!("sentinel writer open never succeeded: {}", e);
                        }
                    }
                    Err(e) => panic!("sentinel writer open never succeeded: {}", e),
                }
            }
        };

        // Brief additional pause for stability of the publishers' own opens.
        thread::sleep(Duration::from_millis(20));

        // Barrier so all publisher threads start their write loop together
        // (maximizes race-window overlap).
        let barrier = StdArc::new(std::sync::Barrier::new(NUM_PUBLISHERS));

        let mut worker_handles = Vec::with_capacity(NUM_PUBLISHERS);
        for pub_idx in 0..NUM_PUBLISHERS {
            let bus = bus.clone();
            let rt = rt.clone();
            let barrier = barrier.clone();
            worker_handles.push(thread::spawn(move || -> Result<()> {
                barrier.wait();
                for iter in 0..ITERATIONS {
                    let marker = format!("PUB-{:02}-{:02}:", pub_idx, iter);
                    let book = Arc::new(make_large_event_book(&marker, PADDING));
                    rt.block_on(bus.publish(book))?;
                }
                Ok(())
            }));
        }

        // Collect publisher results.
        for (idx, h) in worker_handles.into_iter().enumerate() {
            h.join()
                .unwrap_or_else(|_| panic!("publisher thread {} panicked", idx))
                .unwrap_or_else(|e| panic!("publisher thread {} failed: {}", idx, e));
        }

        // Drop the bus AND the sentinel writer so all writer FDs close
        // and the reader sees EOF.
        drop(bus);
        drop(sentinel_writer);

        let frames = reader_handle
            .join()
            .expect("reader thread panicked")
            .expect("reader saw desynced framing");

        // Every frame must decode as a valid EventBook AND carry a unique
        // publisher marker. Missing or duplicate frames would point at a
        // fix that drops or duplicates messages.
        assert_eq!(
            frames.len(),
            TOTAL_FRAMES,
            "expected exactly {} intact frames, got {}",
            TOTAL_FRAMES,
            frames.len()
        );

        let mut seen = std::collections::HashSet::new();
        for (idx, body) in frames.iter().enumerate() {
            let book = EventBook::decode(&body[..])
                .unwrap_or_else(|e| panic!("frame {} failed to decode: {}", idx, e));
            let corr = book
                .cover
                .as_ref()
                .map(|c| c.correlation_id.as_str())
                .unwrap_or("");
            // Marker format "PUB-NN-MM:" is 10 chars.
            let marker = corr.get(0..10).unwrap_or("");
            assert!(
                marker.starts_with("PUB-") && marker.ends_with(':'),
                "frame {} marker {:?} not a recognized publisher",
                idx,
                marker
            );
            assert!(
                seen.insert(marker.to_string()),
                "frame {} duplicates publisher iteration {}",
                idx,
                marker
            );
        }
    }

    /// Back-pressure: when the pipe fills (slow reader), publishers must
    /// block rather than leave a half-written frame in the pipe.
    ///
    /// On baseline (before C-09 fix), the publish path opened the FIFO
    /// O_NONBLOCK and did TWO sequential `write_all` calls — when the
    /// pipe filled between the two writes, the publisher returned Err
    /// having already written the 4-byte length prefix. The reader read
    /// those 4 bytes as the length of the NEXT frame and ran off the end
    /// of available data (or worse, decoded garbage indefinitely).
    ///
    /// After the C-09 fix, the FD has O_NONBLOCK cleared before any
    /// `write_all`, so the publisher blocks on a full pipe until the
    /// reader drains — no half-written frame is ever left behind.
    ///
    /// This test fills the pipe with a slow reader, publishes many
    /// frames bigger than the pipe buffer (forcing back-pressure), then
    /// asserts every frame is read intact and in publisher order.
    #[test]
    fn test_publish_blocks_on_full_pipe_without_corruption() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let dir = tempdir().unwrap();
        let pipe_path = dir.path().join("subscriber-c09-bp.pipe");

        {
            use nix::sys::stat::Mode;
            use nix::unistd::mkfifo;
            mkfifo(&pipe_path, Mode::S_IRUSR | Mode::S_IWUSR).expect("mkfifo");
        }

        let subscriber = SubscriberInfo {
            name: "c09-bp".to_string(),
            domains: vec![],
            pipe_path: pipe_path.clone(),
        };
        let config = IpcConfig::publisher_with_subscribers(dir.path(), vec![subscriber]);
        let bus = StdArc::new(IpcEventBus::new(config));

        // Reader: open the FIFO, then drain SLOWLY — 8 ms between every
        // 4-byte read attempt of the length prefix and every body read.
        // This pace is well below the publisher's throughput, so the pipe
        // fills repeatedly and the publisher must block.
        let pipe_path_reader = pipe_path.clone();
        let reader_handle = thread::spawn(move || -> std::io::Result<Vec<Vec<u8>>> {
            let mut reader = File::open(&pipe_path_reader)?;
            let mut frames = Vec::new();
            loop {
                let mut len_buf = [0u8; 4];
                match reader.read_exact(&mut len_buf) {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e),
                }
                let len = u32::from_be_bytes(len_buf) as usize;
                if len > 10 * 1024 * 1024 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("frame length {} exceeds MAX_MESSAGE_SIZE", len),
                    ));
                }
                let mut body = vec![0u8; len];
                reader.read_exact(&mut body)?;
                frames.push(body);
                // Throttle the reader.
                thread::sleep(Duration::from_millis(8));
            }
            Ok(frames)
        });

        thread::sleep(Duration::from_millis(50));

        // Publish many large frames so the pipe stays saturated for the
        // duration of the test. Each frame body is ~32 KiB; default pipe
        // buffer is 64 KiB, so the publisher will routinely have to wait
        // for the reader to drain.
        const NUM_FRAMES: usize = 12;
        const PADDING: usize = 32 * 1024;

        rt.block_on(async {
            for i in 0..NUM_FRAMES {
                let marker = format!("BP-{:02}:", i);
                let book = Arc::new(make_large_event_book(&marker, PADDING));
                bus.publish(book)
                    .await
                    .unwrap_or_else(|e| panic!("publish under back-pressure must not fail: {}", e));
            }
        });

        // Closing all writers releases the reader from its `read_exact`.
        drop(bus);

        let frames = reader_handle
            .join()
            .expect("reader thread panicked")
            .expect("reader saw desynced framing under back-pressure");

        // Every frame must be intact and in publisher order — back-pressure
        // must not drop, duplicate, or reorder messages from a single
        // publisher.
        assert_eq!(
            frames.len(),
            NUM_FRAMES,
            "expected exactly {} intact frames, got {}",
            NUM_FRAMES,
            frames.len()
        );
        for (idx, body) in frames.iter().enumerate() {
            let book = EventBook::decode(&body[..])
                .unwrap_or_else(|e| panic!("frame {} failed to decode: {}", idx, e));
            let corr = book
                .cover
                .as_ref()
                .map(|c| c.correlation_id.as_str())
                .unwrap_or("");
            let expected = format!("BP-{:02}:", idx);
            assert!(
                corr.starts_with(&expected),
                "frame {} marker {:?} does not start with {:?} (ordering broken under back-pressure)",
                idx,
                corr.get(0..expected.len()).unwrap_or(""),
                expected
            );
        }
    }
}

// ============================================================================
// BrokenPipe Subscriber-Pruning Tests (H-04)
// ============================================================================
//
// Per the H-04 finding: a `BrokenPipe` (EPIPE) on `write_all` to one
// subscriber's FIFO means the subscriber's reader closed its end. The
// pre-fix publish path silently swallowed this as "not an error" and
// returned `Ok` overall, leaving the dead subscriber in the routing
// table — every subsequent publish would re-attempt the open+write to
// a pipe whose reader is gone, wasting syscalls and (more importantly)
// silently violating the "all subscribers see all events" contract for
// THAT publish (the dead subscriber missed the event but the bus
// claims everything was fine).
//
// The H-04 decision is OPTION (c): the IPC broker supports explicit
// register/unregister (`broker.rs`), so a BrokenPipe on write is the
// kernel-level equivalent of an unannounced unregister. The publisher
// should:
//   1. Drop the broken subscriber from its in-memory routing list
//      (so subsequent publishes don't re-target the dead pipe).
//   2. Return `Ok` because every surviving subscriber DID receive
//      its copy — the dead subscriber has effectively left the bus.
//
// `ENXIO` on open keeps its existing "subscriber hasn't started yet"
// semantics: the subscriber is NOT pruned because this is a bring-up
// race, not a death. Only a successful open followed by a `BrokenPipe`
// on write triggers pruning.
mod broken_pipe_pruning_tests {
    use super::*;
    use std::io::Read as IoRead;
    use std::sync::Arc as StdArc;
    use std::thread;
    use std::time::Duration;
    use tempfile::tempdir;

    /// Build a small EventBook with a stable correlation marker.
    fn make_book_with_marker(marker: &str) -> EventBook {
        use crate::proto::Cover;
        EventBook {
            cover: Some(Cover {
                domain: "h04-test".to_string(),
                root: None,
                correlation_id: marker.to_string(),
                edition: None,
                ext: None,
            }),
            ..Default::default()
        }
    }

    /// Pure-helper test: `decide_write_error` returns `Prune` for
    /// `BrokenPipe` (EPIPE), and `Err` for every other I/O error.
    ///
    /// This is the H-04 decision boundary in isolation: classifying a
    /// `write_all` failure as either "subscriber left" (prune + Ok)
    /// or "transport-level error" (propagate as `BusError::Publish`).
    /// The end-to-end test below exercises the actual EPIPE path
    /// against a real FIFO; this test pins the decision logic without
    /// the FIFO race.
    #[test]
    fn decide_write_error_classifies_broken_pipe_as_prune() {
        // BrokenPipe → Prune (subscriber's reader closed; treat as left)
        let broken = std::io::Error::from(std::io::ErrorKind::BrokenPipe);
        match decide_write_error(&broken) {
            WriteErrorOutcome::Prune => {}
            other => panic!("BrokenPipe must classify as Prune, got {:?}", other),
        }

        // Everything else → Err (still a real transport failure)
        for kind in [
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::Other,
            std::io::ErrorKind::WouldBlock,
            std::io::ErrorKind::Interrupted,
        ] {
            let err = std::io::Error::from(kind);
            match decide_write_error(&err) {
                WriteErrorOutcome::Err => {}
                other => panic!(
                    "{:?} must classify as Err (not Prune), got {:?}",
                    kind, other
                ),
            }
        }
    }

    /// H-04 REGRESSION (pruning bookkeeping): a BrokenPipe on
    /// `write_all` to one subscriber must remove that subscriber from
    /// the bus's live routing list so subsequent publishes don't
    /// retarget the dead pipe.
    ///
    /// Constructing a deterministic kernel-level EPIPE inside a unit
    /// test is race-prone (the reader's close has to land between the
    /// publisher's open and write, an interleave the kernel does not
    /// expose a synchronization primitive for). We instead exercise
    /// the bookkeeping seam directly: `prune_subscribers(&[name])` is
    /// the same retain-by-name path the publish loop invokes when its
    /// per-subscriber `decide_write_error` classifies the failure as
    /// `Prune`. The classification side is pinned by the pure-helper
    /// test above; this test pins the *effect*: after pruning, the
    /// subscriber is gone from `live_subscribers()` and the OTHER
    /// subscriber survives.
    ///
    /// On baseline (pre-fix) this test does not compile — neither
    /// `prune_subscribers` nor `live_subscribers` exists — which is
    /// the strongest form of test-red: the fix's contract cannot be
    /// expressed against the buggy code. Post-fix the assertions
    /// hold.
    #[test]
    fn prune_subscribers_removes_only_named_entry() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let dir = tempdir().unwrap();
        let subs = vec![
            SubscriberInfo {
                name: "dead".to_string(),
                domains: vec![],
                pipe_path: dir.path().join("dead.pipe"),
            },
            SubscriberInfo {
                name: "alive".to_string(),
                domains: vec![],
                pipe_path: dir.path().join("alive.pipe"),
            },
        ];
        let config = IpcConfig::publisher_with_subscribers(dir.path(), subs);
        let bus = IpcEventBus::new(config);

        // Pre-condition: both subscribers in the live list.
        let before: Vec<String> = rt
            .block_on(bus.live_subscribers())
            .into_iter()
            .map(|s| s.name)
            .collect();
        assert!(before.contains(&"dead".to_string()));
        assert!(before.contains(&"alive".to_string()));

        // Prune the dead subscriber (the same seam the publish loop
        // hits when it observes a BrokenPipe write failure).
        rt.block_on(bus.prune_subscribers(&["dead".to_string()]));

        // Post-condition: dead is gone, alive remains.
        let after: Vec<String> = rt
            .block_on(bus.live_subscribers())
            .into_iter()
            .map(|s| s.name)
            .collect();
        assert!(
            !after.contains(&"dead".to_string()),
            "H-04: subscriber 'dead' must be pruned; live={:?}",
            after
        );
        assert!(
            after.contains(&"alive".to_string()),
            "H-04: subscriber 'alive' must NOT be pruned when only 'dead' was named; live={:?}",
            after
        );

        // Idempotency: pruning a name that's already gone is a no-op.
        rt.block_on(bus.prune_subscribers(&["dead".to_string()]));
        let after2: Vec<String> = rt
            .block_on(bus.live_subscribers())
            .into_iter()
            .map(|s| s.name)
            .collect();
        assert_eq!(after, after2, "prune_subscribers must be idempotent");
    }

    /// H-04 REGRESSION (end-to-end): when one subscriber's reader
    /// closes the pipe (kernel-level EPIPE on `write_all`), publish
    /// must (a) return Ok because every surviving subscriber received
    /// its copy, (b) actually deliver the frame to the surviving
    /// subscriber, and (c) prune the dead subscriber from the routing
    /// list so subsequent publishes don't repeat the syscall to a
    /// dead pipe.
    ///
    /// To force EPIPE deterministically we publish a body MUCH larger
    /// than the default 64 KiB Linux pipe buffer; the reader thread
    /// opens the FIFO but never drains, so the publisher's `write_all`
    /// blocks after filling the kernel buffer. A two-party
    /// `std::sync::Barrier` synchronizes the reader-drop with the
    /// known-blocked writer: the writer signals the barrier from a
    /// pre-spawn point and the reader waits for that signal before
    /// dropping its FD. Combined with a hard `tokio::time::timeout`
    /// on the publish call, the test cannot hang past the timeout
    /// even under pathological scheduler interleavings — failing
    /// instead of OOM-ing, which is what made the previous
    /// `F_SETPIPE_SZ + sleep(150ms)` form unsafe to leave un-ignored.
    ///
    /// On baseline (pre-H-04 fix) this test fails: the BrokenPipe is
    /// silently swallowed and the subscriber stays in
    /// `config.subscribers` forever, forcing every future publish to
    /// repeat the EPIPE syscall.
    #[test]
    fn broken_pipe_prunes_dead_subscriber_and_delivers_to_survivors() {
        use std::sync::Barrier;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let dir = tempdir().unwrap();
        let pipe_a = dir.path().join("subscriber-h04-dead.pipe");
        let pipe_b = dir.path().join("subscriber-h04-alive.pipe");

        {
            use nix::sys::stat::Mode;
            use nix::unistd::mkfifo;
            mkfifo(&pipe_a, Mode::S_IRUSR | Mode::S_IWUSR).expect("mkfifo a");
            mkfifo(&pipe_b, Mode::S_IRUSR | Mode::S_IWUSR).expect("mkfifo b");
        }

        // DEAD pipe first so the EPIPE branch fires before alive
        // delivery, exercising "continue with rest of fan-out".
        let subs = vec![
            SubscriberInfo {
                name: "dead".to_string(),
                domains: vec![],
                pipe_path: pipe_a.clone(),
            },
            SubscriberInfo {
                name: "alive".to_string(),
                domains: vec![],
                pipe_path: pipe_b.clone(),
            },
        ];
        let config = IpcConfig::publisher_with_subscribers(dir.path(), subs);
        let bus = StdArc::new(IpcEventBus::new(config));

        // Sub-B reader: drain everything until EOF.
        let pipe_b_for_reader = pipe_b.clone();
        let reader_b = thread::spawn(move || -> std::io::Result<Vec<Vec<u8>>> {
            let mut reader = File::open(&pipe_b_for_reader)?;
            let mut frames = Vec::new();
            loop {
                let mut len_buf = [0u8; 4];
                match reader.read_exact(&mut len_buf) {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e),
                }
                let len = u32::from_be_bytes(len_buf) as usize;
                let mut body = vec![0u8; len];
                reader.read_exact(&mut body)?;
                frames.push(body);
            }
            Ok(frames)
        });

        // Sub-A reader: open, then BLOCK on a barrier until the test
        // main thread has confirmed the publisher's write_all is
        // saturated. Dropping the FD at that point makes the kernel
        // surface EPIPE on the blocked write, which is the H-04 trigger.
        let drop_barrier = StdArc::new(Barrier::new(2));
        let drop_barrier_reader = drop_barrier.clone();
        let pipe_a_for_reader = pipe_a.clone();
        let reader_a = thread::spawn(move || {
            let reader = File::open(&pipe_a_for_reader).expect("open pipe-a");
            // Park here until the publisher is known to be blocked.
            drop_barrier_reader.wait();
            drop(reader);
        });

        // Give both reader threads time to reach their open() syscall.
        thread::sleep(Duration::from_millis(50));

        // The body must exceed the default Linux pipe buffer (64 KiB).
        // 1 MiB is comfortably above that and short enough that the
        // surviving subscriber drains it promptly. No F_SETPIPE_SZ
        // shenanigans needed — the default buffer + a payload an order
        // of magnitude larger makes blocking inevitable for any
        // non-draining reader.
        let large_marker = "X".repeat(1024 * 1024);
        let book = Arc::new(make_book_with_marker(&large_marker));

        let publish_outcome = rt.block_on(async {
            let bus = bus.clone();
            let publish_handle = tokio::spawn(async move { bus.publish(book).await });

            // Give the publisher ~150ms to take its per-pipe lock, open
            // the FIFO, clear O_NONBLOCK, and saturate the 64 KiB kernel
            // buffer. With a 1 MiB payload that fills in microseconds
            // on a local pipe, this delay is generous. We then trip
            // the barrier so reader-A drops its FD; the publisher's
            // blocked write_all wakes with EPIPE.
            tokio::time::sleep(Duration::from_millis(150)).await;
            drop_barrier.wait();

            // Hard cap so a missed wake-up surfaces as a test failure,
            // not as an OOM/hang. 5s is two orders of magnitude beyond
            // the expected wallclock for this fan-out.
            tokio::time::timeout(Duration::from_secs(5), publish_handle)
                .await
                .expect("publish exceeded 5s timeout — H-04 fan-out is stuck")
                .expect("publish task panicked")
        });

        reader_a.join().expect("reader-a panicked");

        publish_outcome.expect(
            "publish must return Ok when only-dead-subscriber-failed (surviving subs received) (H-04)",
        );

        let live = rt.block_on(bus.live_subscribers());
        let names: Vec<String> = live.iter().map(|s| s.name.clone()).collect();
        assert!(
            !names.contains(&"dead".to_string()),
            "H-04: subscriber 'dead' must be pruned after BrokenPipe; live={:?}",
            names
        );
        assert!(
            names.contains(&"alive".to_string()),
            "H-04: subscriber 'alive' must remain after a different subscriber's BrokenPipe; live={:?}",
            names
        );

        // Drop the bus so reader-B sees EOF and the thread returns.
        drop(bus);

        let frames_b = reader_b
            .join()
            .expect("reader-b panicked")
            .expect("reader-b read error");
        assert!(
            !frames_b.is_empty(),
            "H-04: surviving subscriber must have received at least one frame, got 0"
        );
    }

    /// ENXIO on open (no reader attached at all — subscriber hasn't
    /// started yet) must NOT prune the subscriber. That's a bring-up
    /// race, not a death. Only BrokenPipe on `write` after a
    /// successful open triggers pruning.
    ///
    /// This regression guard locks the asymmetry: ENXIO is silently
    /// tolerated (subscriber stays in the routing list, eligible for
    /// the NEXT publish once the subscriber starts), while
    /// BrokenPipe-on-write is final.
    #[test]
    fn enxio_on_open_does_not_prune_subscriber() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let dir = tempdir().unwrap();
        let pipe_path = dir.path().join("subscriber-h04-not-started.pipe");

        // Create the FIFO but do NOT open a reader. The publisher's
        // O_NONBLOCK open will return ENXIO (errno=6, ERANGE on linux
        // is 34, ENXIO is 6) — current code skips silently in that
        // case. The subscriber must remain in the routing list.
        {
            use nix::sys::stat::Mode;
            use nix::unistd::mkfifo;
            mkfifo(&pipe_path, Mode::S_IRUSR | Mode::S_IWUSR).expect("mkfifo");
        }

        let subs = vec![SubscriberInfo {
            name: "not-started".to_string(),
            domains: vec![],
            pipe_path: pipe_path.clone(),
        }];
        let config = IpcConfig::publisher_with_subscribers(dir.path(), subs);
        let bus = IpcEventBus::new(config);

        // First publish: ENXIO — subscriber not started yet.
        let outcome = rt.block_on(bus.publish(Arc::new(make_book_with_marker("first"))));
        outcome.expect("ENXIO on open must NOT propagate as Err (subscriber not started)");

        // Subscriber must still be in the routing list — eligible to
        // receive the next publish once it actually starts.
        let live = rt.block_on(bus.live_subscribers());
        let names: Vec<String> = live.iter().map(|s| s.name.clone()).collect();
        assert!(
            names.contains(&"not-started".to_string()),
            "H-04: ENXIO on open is a bring-up race, NOT a death — \
             subscriber must remain in the routing list; live={:?}",
            names
        );
    }
}
