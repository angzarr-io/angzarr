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

// ============================================================================
// H-01 — effective_threshold fallback never engages (no backend overrides
// max_message_size). The OffloadingEventBus consults the inner bus's
// `max_message_size()` only as a fallback when no explicit threshold has been
// configured; until every backend implementation returns its real transport
// limit, the wrapper is a no-op for callers who rely on the default.
// ============================================================================

use crate::payload_store::{PayloadStore, PayloadStoreError, Result as PayloadStoreResult};
use crate::proto::{PayloadReference, PayloadStorageType};
use tokio::sync::Mutex;

/// Payload store that always fails the requested operation, with a switch
/// for failing `put`, `get`, or both. Used by H-02/H-03 tests to assert that
/// failures surface as `Err` instead of being silently swallowed.
struct FailingPayloadStore {
    fail_put: bool,
    fail_get: bool,
    inner_put: Mutex<Vec<(Vec<u8>, PayloadReference)>>,
}

impl FailingPayloadStore {
    fn fail_put_only() -> Self {
        Self {
            fail_put: true,
            fail_get: false,
            inner_put: Mutex::new(Vec::new()),
        }
    }

    fn fail_get_only() -> Self {
        Self {
            fail_put: false,
            fail_get: true,
            inner_put: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl PayloadStore for FailingPayloadStore {
    async fn put(&self, payload: &[u8]) -> PayloadStoreResult<PayloadReference> {
        if self.fail_put {
            return Err(PayloadStoreError::StoreFailed(
                "simulated store backend outage".to_string(),
            ));
        }
        // For fail_get_only mode, we still want put() to succeed so a
        // subsequent resolve() can exercise the get() failure branch.
        let hash = crate::payload_store::compute_hash(payload);
        let reference = PayloadReference {
            storage_type: PayloadStorageType::Filesystem as i32,
            uri: format!("memory://{}", crate::payload_store::hash_to_hex(&hash)),
            content_hash: hash,
            original_size: payload.len() as u64,
            stored_at: None,
        };
        self.inner_put
            .lock()
            .await
            .push((payload.to_vec(), reference.clone()));
        Ok(reference)
    }

    async fn get(&self, reference: &PayloadReference) -> PayloadStoreResult<Vec<u8>> {
        if self.fail_get {
            return Err(PayloadStoreError::RetrieveFailed(format!(
                "simulated retrieval outage for {}",
                reference.uri
            )));
        }
        let inner = self.inner_put.lock().await;
        for (bytes, stored_ref) in inner.iter() {
            if stored_ref.uri == reference.uri {
                return Ok(bytes.clone());
            }
        }
        Err(PayloadStoreError::NotFound(reference.uri.clone()))
    }

    async fn delete_older_than(&self, _age: std::time::Duration) -> PayloadStoreResult<usize> {
        Ok(0)
    }

    fn storage_type(&self) -> PayloadStorageType {
        PayloadStorageType::Filesystem
    }
}

/// SNS/SQS 256 KiB hard limit must engage offloading without an explicit
/// `with_threshold`. Today the SnsSqsEventBus inherits the trait default
/// (`None`), so `effective_threshold` returns `None` and a 300 KiB payload
/// publishes inline — straight into AWS's 256 KiB rejection wall.
///
/// We use `MockEventBus::with_max_message_size(256 * 1024)` to stand in for
/// the SNS/SQS backend; the test asserts the wrapper offloads when the
/// inner bus advertises a real limit (which the fix lands).
#[tokio::test]
async fn test_effective_threshold_uses_inner_bus_max_message_size() {
    let (store, _temp) = create_test_store().await;
    let mock_bus = Arc::new(MockEventBus::with_max_message_size(256 * 1024));
    let inner: Arc<dyn EventBus> = Arc::clone(&mock_bus) as Arc<dyn EventBus>;
    // No explicit threshold — must fall back to inner.max_message_size().
    let config = OffloadingConfig::new(store);
    let bus = OffloadingEventBus::wrap(inner, config);

    // 300 KiB payload — over the SNS/SQS 256 KiB cliff but under typical
    // Kafka/Pub/Sub limits. The wrapper must offload it.
    let book = make_event_book(300 * 1024);
    bus.publish(Arc::new(book)).await.unwrap();

    let published = mock_bus.take_published().await;
    assert_eq!(published.len(), 1);
    assert!(
        matches!(
            &published[0].pages[0].payload,
            Some(event_page::Payload::External(_))
        ),
        "expected External reference, got inline payload — the SNS/SQS \
         max_message_size override did not engage offloading"
    );
}

/// SNS/SQS backend must expose its 256 KiB hard limit as a `pub(crate)`
/// constant used by the EventBus impl's `max_message_size()` override.
/// Pure test — no AWS handshake required.
#[cfg(feature = "sns-sqs")]
#[test]
fn test_sns_sqs_advertises_max_message_size() {
    assert_eq!(
        crate::bus::sns_sqs::MAX_MESSAGE_SIZE,
        256 * 1024,
        "SNS/SQS advertised limit should be the 256 KiB hard cap"
    );
}

/// Pub/Sub backend must expose its 10 MB limit.
#[cfg(feature = "pubsub")]
#[test]
fn test_pubsub_advertises_max_message_size() {
    assert_eq!(
        crate::bus::pubsub::MAX_MESSAGE_SIZE,
        10 * 1024 * 1024,
        "Pub/Sub advertised limit should be 10 MB"
    );
}

/// Kafka backend must expose the 1 MB broker default.
#[cfg(feature = "kafka")]
#[test]
fn test_kafka_advertises_max_message_size() {
    assert_eq!(
        crate::bus::kafka::MAX_MESSAGE_SIZE,
        1024 * 1024,
        "Kafka advertised limit should be the 1 MB broker default"
    );
}

/// AMQP backend must expose the 128 MB broker default.
#[cfg(feature = "amqp")]
#[test]
fn test_amqp_advertises_max_message_size() {
    assert_eq!(
        crate::bus::amqp::MAX_MESSAGE_SIZE,
        128 * 1024 * 1024,
        "AMQP advertised limit should be the 128 MB broker default"
    );
}

// ============================================================================
// H-02 — store.put failure must propagate as Err, not silently fall back to
// inline publish. Today the wrapper logs a warn! and tries to inline-publish
// an oversized payload; the result is either an opaque inner-bus error
// (`payload too large`) or a size-bounded contract violation if the inner
// bus accepts it. Either outcome is silent corruption: the offloading
// promise is broken without the caller's knowledge.
// ============================================================================

/// `store.put` failure must surface as `Err(BusError::Publish)`.
///
/// The caller is the one in position to decide retry / DLQ / circuit-break;
/// the wrapper must not silently degrade the contract by inlining the
/// payload after the claim-check store has rejected it.
#[tokio::test]
async fn test_publish_propagates_store_put_failure_as_error() {
    let failing_store = Arc::new(FailingPayloadStore::fail_put_only());
    let mock_bus = Arc::new(MockEventBus::new());
    let inner: Arc<dyn EventBus> = Arc::clone(&mock_bus) as Arc<dyn EventBus>;
    let config = OffloadingConfig::new(failing_store).with_threshold(100);
    let bus = OffloadingEventBus::wrap(inner, config);

    // Large enough to engage offloading (page > threshold/2).
    let book = make_event_book(500);
    let result = bus.publish(Arc::new(book)).await;

    // Must be an explicit error, not Ok with silent inline fallback.
    assert!(
        matches!(result, Err(BusError::Publish(_))),
        "expected BusError::Publish, got {:?}",
        result
    );

    // Inner bus must not have received the inlined oversized event.
    let published = mock_bus.take_published().await;
    assert!(
        published.is_empty(),
        "inner bus must not receive a silent inline fallback when the \
         payload store rejects the put"
    );
}

// ============================================================================
// H-03 — store.get failure must propagate to the handler dispatch path.
// Today, `resolve_payloads_with_store` keeps the original `External` page on
// fetch/decode failure and only emits a warn!. The wrapped handler then
// receives an EventBook with unresolved External references — payloads it
// has no way to decode. The handler appears to succeed; events are lost.
// ============================================================================

/// `store.get` failure must surface as `Err` to the wrapped handler.
///
/// The handler dispatch path sees the failure and the bus transport (AMQP,
/// JetStream, etc.) can nack / redeliver. Silently delivering an unresolved
/// External page is the worst outcome: business logic decodes garbage.
#[tokio::test]
async fn test_resolving_handler_propagates_store_get_failure_as_error() {
    let failing_store = Arc::new(FailingPayloadStore::fail_get_only());

    // Seed a "stored" reference by going through put() (which succeeds in
    // fail_get_only mode). This gives us a realistic External page whose
    // bytes the store will then refuse to return.
    let payload_bytes = prost_types::Any {
        type_url: "test.Event".to_string(),
        value: vec![0u8; 500],
    }
    .encode_to_vec();
    let reference = failing_store.put(&payload_bytes).await.unwrap();

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

    let (capturing_handler, received) = CapturingHandler::new();
    let resolving_handler = ResolvingHandler {
        inner: Arc::new(capturing_handler),
        store: Arc::clone(&failing_store),
    };

    let result = resolving_handler.handle(Arc::new(offloaded_book)).await;

    // Must be an explicit error, not Ok with an unresolved External page.
    assert!(
        result.is_err(),
        "expected Err on store.get failure, got Ok — the handler dispatch \
         path silently received an unresolved External payload"
    );

    // The inner business handler must NOT have been invoked with an
    // unresolved External reference.
    let captured = received.read().await;
    assert!(
        captured.is_empty(),
        "inner business handler must not be invoked when external \
         payloads cannot be resolved (saw {} books)",
        captured.len()
    );
}
