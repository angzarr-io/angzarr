//! Tests for the payload offloading event bus wrapper.
//!
//! The offloading bus implements the "claim check" pattern: large event
//! payloads are stored externally and replaced with references. This
//! enables bus transports with size limits (e.g., Kafka's 1MB default)
//! to handle arbitrarily large events.
//!
//! Why this matters: Without offloading, large aggregates (e.g., with
//! embedded documents, images, or complex state) would fail to publish,
//! breaking event sourcing entirely. The claim check pattern decouples
//! payload size from transport limits.
//!
//! Key behaviors verified:
//! - Small payloads pass through unchanged (no storage overhead)
//! - Large payloads are offloaded and replaced with External references
//! - References are resolved back to inline events on receive
//! - The offloading is transparent to handlers

use super::*;
use crate::bus::MockEventBus;
use crate::payload_store::FilesystemPayloadStore;
use crate::proto::{event_page, PageHeader};
use tempfile::TempDir;

// ============================================================================
// Test Helpers
// ============================================================================

async fn create_test_store() -> (Arc<FilesystemPayloadStore>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(FilesystemPayloadStore::new(temp_dir.path()).await.unwrap());
    (store, temp_dir)
}

fn make_event_book(payload_size: usize) -> EventBook {
    EventBook {
        cover: None,
        pages: vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
            }),
            created_at: None,
            payload: Some(event_page::Payload::Event(prost_types::Any {
                type_url: "test.Event".to_string(),
                value: vec![0u8; payload_size],
            })),
            ..Default::default()
        }],
        snapshot: None,
        next_sequence: 1,
    }
}

// ============================================================================
// Offloading Tests
// ============================================================================

/// Small payloads below threshold pass through without offloading.
///
/// Offloading adds latency (store write) and storage cost. Events that
/// fit within bus limits should be sent inline for efficiency.
#[tokio::test]
async fn test_small_payload_passes_through() {
    let (store, _temp) = create_test_store().await;
    let mock_bus = Arc::new(MockEventBus::new());
    let inner: Arc<dyn EventBus> = Arc::clone(&mock_bus) as Arc<dyn EventBus>;
    let config = OffloadingConfig::new(store).with_threshold(1024);
    let bus = OffloadingEventBus::wrap(inner, config);

    let book = make_event_book(100); // Small payload
    bus.publish(Arc::new(book.clone())).await.unwrap();

    // Should have been published without offloading
    let published = mock_bus.take_published().await;
    assert_eq!(published.len(), 1);
    assert!(matches!(
        &published[0].pages[0].payload,
        Some(event_page::Payload::Event(_))
    ));
}

/// Large payloads above threshold are replaced with External references.
///
/// The actual payload is stored externally; the bus message contains only
/// a URI reference. This keeps bus messages small regardless of event size.
#[tokio::test]
async fn test_large_payload_gets_offloaded() {
    let (store, _temp) = create_test_store().await;
    let mock_bus = Arc::new(MockEventBus::new());
    let inner: Arc<dyn EventBus> = Arc::clone(&mock_bus) as Arc<dyn EventBus>;
    let config = OffloadingConfig::new(store).with_threshold(100);
    let bus = OffloadingEventBus::wrap(inner, config);

    let book = make_event_book(500); // Large payload
    bus.publish(Arc::new(book.clone())).await.unwrap();

    // Should have been offloaded
    let published = mock_bus.take_published().await;
    assert_eq!(published.len(), 1);
    assert!(matches!(
        &published[0].pages[0].payload,
        Some(event_page::Payload::External(_))
    ));
}

/// External references resolve back to original payload.
///
/// Round-trip integrity: offload → publish → receive → resolve produces
/// the original event. Handlers see fully-resolved EventBooks.
#[tokio::test]
async fn test_resolve_external_payload() {
    let (store, _temp) = create_test_store().await;
    let mock_bus = Arc::new(MockEventBus::new());
    let inner: Arc<dyn EventBus> = Arc::clone(&mock_bus) as Arc<dyn EventBus>;
    let config = OffloadingConfig::new(Arc::clone(&store)).with_threshold(100);
    let bus = OffloadingEventBus::wrap(inner, config);

    // Create and publish large event
    let original = make_event_book(500);
    bus.publish(Arc::new(original.clone())).await.unwrap();

    // Get the offloaded version
    let published = mock_bus.take_published().await;
    let offloaded = &published[0];

    // Resolve the payload
    let resolved = bus.resolve_payloads(offloaded).await.unwrap();

    // Should have event restored
    let resolved_event = match &resolved.pages[0].payload {
        Some(event_page::Payload::Event(e)) => e,
        _ => panic!("Expected resolved event payload"),
    };
    let original_event = match &original.pages[0].payload {
        Some(event_page::Payload::Event(e)) => e,
        _ => panic!("Expected original event payload"),
    };
    assert_eq!(original_event.type_url, resolved_event.type_url);
    assert_eq!(original_event.value.len(), resolved_event.value.len());
}

/// No threshold means no offloading — all events pass through.
///
/// When the inner bus has no max_message_size and no explicit threshold
/// is configured, offloading is disabled. Used for buses without limits.
#[tokio::test]
async fn test_no_threshold_passes_all() {
    let (store, _temp) = create_test_store().await;
    let mock_bus = Arc::new(MockEventBus::new());
    let inner: Arc<dyn EventBus> = Arc::clone(&mock_bus) as Arc<dyn EventBus>;
    let config = OffloadingConfig::new(store); // No threshold set
    let bus = OffloadingEventBus::wrap(inner, config);

    let book = make_event_book(10000); // Very large
    bus.publish(Arc::new(book.clone())).await.unwrap();

    // Should pass through since inner bus has no limit
    let published = mock_bus.take_published().await;
    assert_eq!(published.len(), 1);
    assert!(matches!(
        &published[0].pages[0].payload,
        Some(event_page::Payload::Event(_))
    ));
}

// ============================================================================
// ResolvingHandler Tests
// ============================================================================
//
// The ResolvingHandler wraps user handlers and transparently resolves
// External references before delivery. This makes offloading invisible
// to business logic.

/// Test handler that captures received EventBooks for verification.
struct CapturingHandler {
    received: Arc<tokio::sync::RwLock<Vec<EventBook>>>,
}

impl CapturingHandler {
    fn new() -> (Self, Arc<tokio::sync::RwLock<Vec<EventBook>>>) {
        let received = Arc::new(tokio::sync::RwLock::new(Vec::new()));
        (
            Self {
                received: Arc::clone(&received),
            },
            received,
        )
    }
}

impl EventHandler for CapturingHandler {
    fn handle(
        &self,
        book: Arc<EventBook>,
    ) -> BoxFuture<'static, std::result::Result<(), BusError>> {
        let received = Arc::clone(&self.received);
        Box::pin(async move {
            received.write().await.push((*book).clone());
            Ok(())
        })
    }
}

/// External payloads are resolved before handler receives event.
///
/// Handler sees inline Event, not External reference. The business logic
/// doesn't need to know about offloading — it's transparent.
#[tokio::test]
async fn test_resolving_handler_resolves_external_payloads() {
    let (store, _temp) = create_test_store().await;

    // Create an offloaded event by storing payload externally
    let original_event = prost_types::Any {
        type_url: "test.Event".to_string(),
        value: vec![42u8; 500],
    };
    let payload_bytes = original_event.encode_to_vec();
    let reference = store.put(&payload_bytes).await.unwrap();

    // Create EventBook with external reference
    let offloaded_book = EventBook {
        cover: None,
        pages: vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
            }),
            created_at: None,
            payload: Some(event_page::Payload::External(reference)),
            ..Default::default()
        }],
        snapshot: None,
        next_sequence: 1,
    };

    // Set up resolving handler
    let (capturing_handler, received) = CapturingHandler::new();
    let resolving_handler = ResolvingHandler {
        inner: Arc::new(capturing_handler),
        store: Arc::clone(&store),
    };

    // Invoke the resolving handler with offloaded event
    resolving_handler
        .handle(Arc::new(offloaded_book))
        .await
        .unwrap();

    // Verify inner handler received resolved event
    let captured = received.read().await;
    assert_eq!(captured.len(), 1);
    let resolved_payload = &captured[0].pages[0].payload;
    match resolved_payload {
        Some(event_page::Payload::Event(e)) => {
            assert_eq!(e.type_url, "test.Event");
            assert_eq!(e.value.len(), 500);
            assert!(e.value.iter().all(|&b| b == 42));
        }
        _ => panic!(
            "Expected resolved Event payload, got {:?}",
            resolved_payload
        ),
    }
}

/// Inline events pass through without modification.
///
/// Events that were never offloaded (small payloads) should be delivered
/// unchanged. Resolution is a no-op for inline events.
#[tokio::test]
async fn test_resolving_handler_passes_inline_events_unchanged() {
    let (store, _temp) = create_test_store().await;

    // Create EventBook with inline event (no external reference)
    let inline_book = make_event_book(100);

    // Set up resolving handler
    let (capturing_handler, received) = CapturingHandler::new();
    let resolving_handler = ResolvingHandler {
        inner: Arc::new(capturing_handler),
        store: Arc::clone(&store),
    };

    // Invoke the resolving handler
    resolving_handler
        .handle(Arc::new(inline_book.clone()))
        .await
        .unwrap();

    // Verify inner handler received event unchanged
    let captured = received.read().await;
    assert_eq!(captured.len(), 1);
    match &captured[0].pages[0].payload {
        Some(event_page::Payload::Event(e)) => {
            assert_eq!(e.type_url, "test.Event");
            assert_eq!(e.value.len(), 100);
        }
        _ => panic!("Expected inline Event payload"),
    }
}
