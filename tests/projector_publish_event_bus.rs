//! Integration test: projector publish -> EventBus.publish shape
//! verification.
//!
//! Run with:
//! ```bash
//! cargo test --test projector_publish_event_bus --features "test-utils" -- --nocapture
//! ```
//!
//! Scope note (R2-15 audit, Category B): the original framing was
//! "projector handler -> real EventBus.publish for the projection-as-
//! EventBook streaming path." On closer inspection the gap is
//! smaller than the prior two Category B items:
//!
//! - `MockEventBus` already exercises the publish path end-to-end
//!   (the projector calls `event_bus.publish(...)` and the mock
//!   records the book). The unit tests at
//!   `handlers/core/projector.test.rs` use this and check
//!   `publish_count`.
//! - Real-broker serialization is owned by the `bus_*.rs`
//!   testcontainer suites (AMQP / Kafka / NATS / Pub-Sub /
//!   SNS-SQS). Those exercise the bus contract -- including
//!   wire serialization -- against real backends.
//!
//! What the existing tests don't cover: **deep shape verification**
//! of the published `EventBook`. The unit tests check `publish_count`
//! and at most `published[0].snapshot.is_none()`. They don't decode
//! the projection payload back to a `Projection` proto, don't check
//! the projection-domain prefix is correctly formed, and don't pin
//! correlation_id / edition propagation through to the bus.
//!
//! This test closes that shape-verification gap. It uses
//! `MockEventBus` (same as the unit tests) but inspects the
//! published `EventBook` in detail, ensuring the
//! `create_projection_event_book` -> `publisher.publish(...)` path
//! produces the contract-compliant book the operator-facing
//! streaming subscribers expect to receive.

#![cfg(feature = "test-utils")]

use std::sync::Arc;

use async_trait::async_trait;
use prost::Message;
use prost_types::Any;

use angzarr::bus::{EventBus, EventHandler, MockEventBus};
use angzarr::handlers::core::projector::ProjectorEventHandler;
use angzarr::orchestration::projector::{ProjectionMode, ProjectorHandler};
use angzarr::proto::{event_page, Cover, Edition, EventBook, Projection};
use angzarr::proto_ext::{CoverExt, PROJECTION_DOMAIN_PREFIX, PROJECTION_TYPE_URL};

// ============================================================================
// Test fixtures
// ============================================================================

/// Projector handler that returns a pre-configured Projection.
struct CannedProjectorHandler {
    projection: Projection,
}

#[async_trait]
impl ProjectorHandler for CannedProjectorHandler {
    async fn handle(
        &self,
        _events: &EventBook,
        _mode: ProjectionMode,
    ) -> Result<Projection, tonic::Status> {
        Ok(self.projection.clone())
    }
}

fn projection(
    projector_name: &str,
    sequence: u32,
    source_domain: &str,
    payload_bytes: Vec<u8>,
    edition: Option<&str>,
) -> Projection {
    Projection {
        projector: projector_name.to_string(),
        sequence,
        cover: Some(Cover {
            domain: source_domain.to_string(),
            root: None,
            correlation_id: String::new(),
            edition: edition.map(|name| Edition {
                name: name.to_string(),
                divergences: vec![],
            }),
            ext: None,
        }),
        projection: Some(Any {
            type_url: "test.ProjectionPayload".to_string(),
            value: payload_bytes,
        }),
    }
}

fn source_event_book(domain: &str, correlation_id: &str, edition: Option<&str>) -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: None,
            correlation_id: correlation_id.to_string(),
            edition: edition.map(|name| Edition {
                name: name.to_string(),
                divergences: vec![],
            }),
            ext: None,
        }),
        pages: vec![],
        snapshot: None,
        ..Default::default()
    }
}

// ============================================================================
// Tests
// ============================================================================

/// The published EventBook's domain is the projection-prefixed form
/// `_projection.{projector}.{source_domain}`. Pins the routing-key
/// contract the streaming subscribers depend on -- a regression in
/// `create_projection_event_book`'s domain construction would route
/// projections to the wrong topic / queue.
#[tokio::test]
async fn published_event_book_uses_projection_domain_prefix() {
    let bus = Arc::new(MockEventBus::new());
    let bus_dyn: Arc<dyn EventBus> = bus.clone();
    let handler: Arc<dyn ProjectorHandler> = Arc::new(CannedProjectorHandler {
        projection: projection("order-summary", 7, "orders", vec![1, 2, 3], None),
    });
    let event_handler = ProjectorEventHandler::from_handler(handler, "order-summary".to_string())
        .with_publisher(bus_dyn);

    let book = Arc::new(source_event_book("orders", "corr-123", None));
    event_handler.handle(book).await.expect("handle");

    let published = bus.take_published().await;
    assert_eq!(published.len(), 1, "expected exactly one publish");
    let domain = published[0].domain();
    assert!(
        domain.starts_with(PROJECTION_DOMAIN_PREFIX),
        "domain must start with the projection prefix, got {domain}"
    );
    assert!(
        domain.contains("order-summary"),
        "domain must name the projector, got {domain}"
    );
    assert!(
        domain.contains("orders"),
        "domain must include the source domain, got {domain}"
    );
}

/// The published EventBook's correlation_id matches the source
/// event's correlation_id -- pins end-to-end request tracing from
/// command -> events -> projection.
#[tokio::test]
async fn published_event_book_preserves_correlation_id() {
    let bus = Arc::new(MockEventBus::new());
    let handler: Arc<dyn ProjectorHandler> = Arc::new(CannedProjectorHandler {
        projection: projection("order-summary", 1, "orders", vec![1], None),
    });
    let event_handler = ProjectorEventHandler::from_handler(handler, "order-summary".to_string())
        .with_publisher(bus.clone());

    let book = Arc::new(source_event_book("orders", "my-correlation-123", None));
    event_handler.handle(book).await.expect("handle");

    let published = bus.take_published().await;
    assert_eq!(
        published[0].correlation_id(),
        "my-correlation-123",
        "correlation_id must propagate from source event to projection EventBook"
    );
}

/// The published EventBook's single page carries the projection
/// payload as a prost-encoded `Projection` proto with the canonical
/// type_url. Decoding must round-trip the original projector name,
/// sequence, and payload bytes -- this is the contract streaming
/// subscribers rely on for client-side deserialization.
#[tokio::test]
async fn published_event_book_payload_decodes_to_original_projection() {
    let bus = Arc::new(MockEventBus::new());
    let original = projection(
        "order-summary",
        42,
        "orders",
        b"test-payload-bytes".to_vec(),
        None,
    );
    let handler: Arc<dyn ProjectorHandler> = Arc::new(CannedProjectorHandler {
        projection: original.clone(),
    });
    let event_handler = ProjectorEventHandler::from_handler(handler, "order-summary".to_string())
        .with_publisher(bus.clone());

    let book = Arc::new(source_event_book("orders", "corr-1", None));
    event_handler.handle(book).await.expect("handle");

    let published = bus.take_published().await;
    assert_eq!(published[0].pages.len(), 1);
    let page = &published[0].pages[0];

    let any = match &page.payload {
        Some(event_page::Payload::Event(any)) => any,
        other => panic!("expected Event payload, got {other:?}"),
    };
    assert_eq!(
        any.type_url, PROJECTION_TYPE_URL,
        "wire type_url must be the canonical projection URL so clients can dispatch"
    );

    let decoded = Projection::decode(any.value.as_slice()).expect("decode projection bytes");
    assert_eq!(decoded.projector, original.projector);
    assert_eq!(decoded.sequence, original.sequence);
    assert_eq!(
        decoded.projection.as_ref().map(|a| a.value.clone()),
        original.projection.as_ref().map(|a| a.value.clone()),
        "projection payload bytes must round-trip via the bus"
    );
}

/// The published EventBook's sequence number matches the source
/// `Projection.sequence`. Operators rely on this to track projection
/// freshness.
#[tokio::test]
async fn published_event_book_preserves_projection_sequence() {
    let bus = Arc::new(MockEventBus::new());
    let handler: Arc<dyn ProjectorHandler> = Arc::new(CannedProjectorHandler {
        projection: projection("order-summary", 1234, "orders", vec![], None),
    });
    let event_handler = ProjectorEventHandler::from_handler(handler, "order-summary".to_string())
        .with_publisher(bus.clone());

    let book = Arc::new(source_event_book("orders", "corr-1", None));
    event_handler.handle(book).await.expect("handle");

    let published = bus.take_published().await;
    let page = &published[0].pages[0];
    use angzarr::proto_ext::EventPageExt;
    assert_eq!(page.sequence_num(), 1234);
}

/// Source event with `cover.edition` propagates the edition to the
/// published projection EventBook. Pins the multi-tenant /
/// branched-state contract -- projections for "branch-a" must be
/// routable separately from default-edition projections.
#[tokio::test]
async fn published_event_book_propagates_source_edition_when_projection_has_no_cover() {
    let bus = Arc::new(MockEventBus::new());
    // Projection itself has no cover -- triggers the fallback path
    // in create_projection_event_book that uses the source edition.
    let mut p = projection("order-summary", 5, "orders", vec![1], None);
    p.cover = None;
    let handler: Arc<dyn ProjectorHandler> = Arc::new(CannedProjectorHandler { projection: p });
    let event_handler = ProjectorEventHandler::from_handler(handler, "order-summary".to_string())
        .with_publisher(bus.clone());

    let book = Arc::new(source_event_book("orders", "corr-1", Some("branch-a")));
    event_handler.handle(book).await.expect("handle");

    let published = bus.take_published().await;
    let edition_name = published[0]
        .cover
        .as_ref()
        .and_then(|c| c.edition.as_ref())
        .map(|e| e.name.as_str())
        .unwrap_or("<none>");
    assert_eq!(
        edition_name, "branch-a",
        "edition must propagate from source event when projection lacks its own cover"
    );
}

/// Projection EventBooks never carry a snapshot -- snapshots are
/// for aggregate state, not projection output. Pre-existing unit
/// test pins the no-snapshot contract via `MockEventBus`; this
/// integration test confirms the same invariant survives the
/// `publisher.publish(Arc::new(book))` boundary in the real
/// publish path (the unit test's setup is structurally identical,
/// so this is really an end-to-end smoke test of the same path).
#[tokio::test]
async fn published_event_book_carries_no_snapshot() {
    let bus = Arc::new(MockEventBus::new());
    let handler: Arc<dyn ProjectorHandler> = Arc::new(CannedProjectorHandler {
        projection: projection("order-summary", 5, "orders", vec![1], None),
    });
    let event_handler = ProjectorEventHandler::from_handler(handler, "order-summary".to_string())
        .with_publisher(bus.clone());

    let book = Arc::new(source_event_book("orders", "corr-1", None));
    event_handler.handle(book).await.expect("handle");

    let published = bus.take_published().await;
    assert!(
        published[0].snapshot.is_none(),
        "projection EventBook must never carry a snapshot"
    );
}

/// Confirms the publish path goes through the published bus
/// reference -- not some incidental code that the unit tests
/// happen to exercise. Direct trait-dispatch on the `Arc<dyn EventBus>`
/// the projector holds is what production runs; this catches a
/// regression where the projector started publishing through a
/// different code path.
///
/// Concretely: the `EventHandler` trait that ProjectorEventHandler
/// implements is the same trait the bus consumes, so a refactor
/// that accidentally drops the publish call would leave
/// `bus.published_count() == 0` here.
#[tokio::test]
async fn projector_publish_call_actually_reaches_the_event_bus() {
    let bus = Arc::new(MockEventBus::new());
    let handler: Arc<dyn ProjectorHandler> = Arc::new(CannedProjectorHandler {
        projection: projection("order-summary", 5, "orders", vec![1], None),
    });
    let event_handler = ProjectorEventHandler::from_handler(handler, "order-summary".to_string())
        .with_publisher(bus.clone());

    assert_eq!(bus.published_count().await, 0, "pre-state: no publishes");
    let book = Arc::new(source_event_book("orders", "corr-1", None));
    event_handler.handle(book).await.expect("handle");
    assert_eq!(
        bus.published_count().await,
        1,
        "projector handle must invoke event_bus.publish exactly once"
    );
}
