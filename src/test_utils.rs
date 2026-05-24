#![allow(dead_code)]
//! Shared test fixture builders for angzarr unit tests.
//!
//! Provides reusable constructors for proto types that appear across many test modules.

use crate::proto::{
    command_page, event_page, page_header::SequenceType, CommandBook, CommandPage, Cover,
    EventBook, EventPage, MergeStrategy, PageHeader, Uuid as ProtoUuid,
};
use futures::future::BoxFuture;
use prost_types::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::bus::{BusError, EventHandler};

/// Create a `ProtoUuid` from a random v4 UUID.
pub fn random_proto_uuid() -> ProtoUuid {
    proto_uuid(Uuid::new_v4())
}

/// Create a `ProtoUuid` from a specific `Uuid`.
pub fn proto_uuid(u: Uuid) -> ProtoUuid {
    ProtoUuid {
        value: u.as_bytes().to_vec(),
    }
}

/// Create a `Cover` with the given domain and a random root UUID.
pub fn make_cover(domain: &str) -> Cover {
    make_cover_with_root(domain, Uuid::new_v4())
}

/// Create a `Cover` with the given domain and specific root UUID.
pub fn make_cover_with_root(domain: &str, root: Uuid) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(proto_uuid(root)),
        correlation_id: String::new(),
        edition: None,
        ext: None,
    }
}

/// Create a `Cover` with domain, root, and correlation ID.
pub fn make_cover_full(domain: &str, root: Uuid, correlation_id: &str) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(proto_uuid(root)),
        correlation_id: correlation_id.to_string(),
        edition: None,
        ext: None,
    }
}

/// Create an `EventPage` with a sequence number and a test type_url.
pub fn make_event_page(seq: u32) -> EventPage {
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(seq)),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: format!("test.Event{}", seq),
            value: vec![],
        })),
        created_at: None,
        cascade_id: None,
        ..Default::default()
    }
}

/// Create an `EventPage` with a specific type_url.
pub fn make_event_page_typed(seq: u32, type_url: &str) -> EventPage {
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(seq)),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: type_url.to_string(),
            value: vec![],
        })),
        created_at: None,
        cascade_id: None,
        ..Default::default()
    }
}

/// Create an uncommitted `EventPage` for 2PC testing.
pub fn make_uncommitted_event_page(seq: u32, cascade_id: &str) -> EventPage {
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(seq)),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: format!("test.Event{}", seq),
            value: vec![],
        })),
        created_at: None,
        no_commit: true,
        cascade_id: Some(cascade_id.to_string()),
    }
}

/// Create an `EventBook` with domain, random root, and provided pages.
pub fn make_event_book(domain: &str, pages: Vec<EventPage>) -> EventBook {
    EventBook {
        cover: Some(make_cover(domain)),
        pages,
        snapshot: None,
        ..Default::default()
    }
}

/// Create an `EventBook` with domain, specific root, and provided pages.
pub fn make_event_book_with_root(domain: &str, root: Uuid, pages: Vec<EventPage>) -> EventBook {
    EventBook {
        cover: Some(make_cover_with_root(domain, root)),
        pages,
        snapshot: None,
        ..Default::default()
    }
}

/// Create an empty `EventBook` for the given domain.
pub fn make_empty_event_book(domain: &str) -> EventBook {
    make_event_book(domain, vec![])
}

/// Create an `EventBook` with domain, random root, correlation ID, and provided pages.
pub fn make_event_book_with_correlation(
    domain: &str,
    correlation_id: &str,
    pages: Vec<EventPage>,
) -> EventBook {
    EventBook {
        cover: Some(make_cover_full(domain, Uuid::new_v4(), correlation_id)),
        pages,
        snapshot: None,
        ..Default::default()
    }
}

/// Create an `EventBook` with a single test event page and correlation ID.
/// Convenience helper for subscription/streaming tests.
pub fn make_test_event_book(correlation_id: &str) -> EventBook {
    make_event_book_with_correlation(
        "test",
        correlation_id,
        vec![make_event_page_typed(0, "test.Event")],
    )
}

/// Create an `EventBook` with multiple pages and correlation ID.
/// Convenience helper for multi-page event tests.
pub fn make_multi_page_event_book(correlation_id: &str, page_count: usize) -> EventBook {
    let pages = (0..page_count)
        .map(|i| make_event_page_typed(i as u32, &format!("type.googleapis.com/test.Event{}", i)))
        .collect();
    make_event_book_with_correlation("test", correlation_id, pages)
}

/// Create a `CommandBook` with domain, specific root, and a test command.
pub fn make_command_book(domain: &str, root: Uuid) -> CommandBook {
    make_command_book_with_sequence(domain, root, 0)
}

/// Create a `CommandBook` with domain, root, and specific sequence.
pub fn make_command_book_with_sequence(domain: &str, root: Uuid, sequence: u32) -> CommandBook {
    CommandBook {
        cover: Some(make_cover_with_root(domain, root)),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::Sequence(sequence)),
            }),
            payload: Some(command_page::Payload::Command(Any {
                type_url: "test.Command".to_string(),
                value: vec![],
            })),
            merge_strategy: MergeStrategy::MergeCommutative as i32,
        }],
    }
}

/// Create a `CommandBook` with optional correlation ID.
pub fn make_command_book_correlated(with_correlation: bool) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: "test".to_string(),
            root: Some(random_proto_uuid()),
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
                sequence_type: Some(SequenceType::Sequence(0)),
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
// Test Handlers
// ============================================================================

/// A test handler that counts how many times it was called.
///
/// Useful for verifying event delivery in bus tests.
///
/// # Example
/// ```ignore
/// let handler = CountingHandler::new();
/// let count = handler.count();
/// bus.subscribe(Box::new(handler)).await?;
/// // ... publish events ...
/// assert_eq!(count.load(Ordering::SeqCst), expected_count);
/// ```
pub struct CountingHandler {
    count: Arc<AtomicUsize>,
}

impl CountingHandler {
    /// Create a new counting handler.
    pub fn new() -> Self {
        Self {
            count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Get a clone of the counter for checking the count.
    pub fn count(&self) -> Arc<AtomicUsize> {
        self.count.clone()
    }
}

impl Default for CountingHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl EventHandler for CountingHandler {
    fn handle(
        &self,
        _book: Arc<EventBook>,
    ) -> BoxFuture<'static, std::result::Result<(), BusError>> {
        let count = self.count.clone();
        Box::pin(async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

/// A test handler that captures received events and sends them to a channel.
///
/// Useful for verifying event content in bus tests. Supports:
/// - Channel-based delivery for async waiting
/// - Optional count tracking
/// - Optional mutex-backed storage for later query
///
/// # Example
/// ```ignore
/// let (tx, mut rx) = tokio::sync::mpsc::channel(10);
/// let handler = CapturingHandler::new(tx);
/// bus.subscribe(Box::new(handler)).await?;
/// // ... publish events ...
/// let received = rx.recv().await.unwrap();
/// assert_eq!(received.domain(), "orders");
/// ```
pub struct CapturingHandler {
    tx: tokio::sync::mpsc::Sender<EventBook>,
    count: Option<Arc<AtomicUsize>>,
    /// Optional storage for later query (used by BDD tests).
    received: Option<Arc<Mutex<Vec<EventBook>>>>,
}

impl CapturingHandler {
    /// Create a handler that sends events to the given channel.
    pub fn new(tx: tokio::sync::mpsc::Sender<EventBook>) -> Self {
        Self {
            tx,
            count: None,
            received: None,
        }
    }

    /// Create a handler that sends events and tracks count.
    pub fn with_count(tx: tokio::sync::mpsc::Sender<EventBook>, count: Arc<AtomicUsize>) -> Self {
        Self {
            tx,
            count: Some(count),
            received: None,
        }
    }

    /// Create a handler with mutex-backed storage for later query.
    /// Used by BDD/interface tests that need to inspect received events.
    pub fn with_storage(
        tx: tokio::sync::mpsc::Sender<EventBook>,
        received: Arc<Mutex<Vec<EventBook>>>,
    ) -> Self {
        Self {
            tx,
            count: None,
            received: Some(received),
        }
    }
}

impl EventHandler for CapturingHandler {
    fn handle(&self, book: Arc<EventBook>) -> BoxFuture<'static, Result<(), BusError>> {
        let tx = self.tx.clone();
        let count = self.count.clone();
        let received = self.received.clone();
        let book = (*book).clone();
        Box::pin(async move {
            if let Some(c) = count {
                c.fetch_add(1, Ordering::SeqCst);
            }
            if let Some(r) = received {
                r.lock().await.push(book.clone());
            }
            tx.send(book)
                .await
                .map_err(|e| BusError::Publish(e.to_string()))?;
            Ok(())
        })
    }
}

/// A test handler that always fails with a configurable error.
///
/// Useful for testing error handling and DLQ scenarios.
pub struct FailingHandler {
    error_message: String,
    /// Optional tracker for verifying error was reported (used by BDD tests).
    error_reported: Option<Arc<Mutex<Option<String>>>>,
}

impl FailingHandler {
    /// Create a handler that fails with the given message.
    pub fn new(error_message: &str) -> Self {
        Self {
            error_message: error_message.to_string(),
            error_reported: None,
        }
    }

    /// Create a handler that fails and tracks the error for verification.
    pub fn with_tracker(error_message: &str, error_reported: Arc<Mutex<Option<String>>>) -> Self {
        Self {
            error_message: error_message.to_string(),
            error_reported: Some(error_reported),
        }
    }
}

impl EventHandler for FailingHandler {
    fn handle(&self, _book: Arc<EventBook>) -> BoxFuture<'static, Result<(), BusError>> {
        let error = self.error_message.clone();
        let error_reported = self.error_reported.clone();
        Box::pin(async move {
            if let Some(tracker) = error_reported {
                *tracker.lock().await = Some(error.clone());
            }
            Err(BusError::Publish(error))
        })
    }
}
