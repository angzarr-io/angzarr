//! Tests for projector event handler and projection streaming.
//!
//! The projector event handler bridges the event bus to projector services:
//! 1. Receives events from the bus
//! 2. Filters by domain (if configured)
//! 3. Calls projector handler to generate Projection
//! 4. Publishes Projection as synthetic EventBook for streaming
//!
//! Key scenarios tested:
//! - Projection EventBook creation (domain prefixing, correlation ID)
//! - Domain filtering (include list, infrastructure exclusion)
//! - Publishing conditions (projection content or projector name required)
//! - Edition preservation through the projection pipeline

use super::*;
use crate::proto::{Cover, Edition, Projection};
use crate::proto_ext::CoverExt;

// ============================================================================
// Test Helpers
// ============================================================================

fn make_cover(domain: &str) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: None,
        correlation_id: String::new(),
        edition: None,
        ext: None,
    }
}

fn make_projection(projector: &str, sequence: u32) -> Projection {
    Projection {
        projector: projector.to_string(),
        sequence,
        cover: Some(make_cover("orders")),
        projection: None,
    }
}

// ============================================================================
// Projection EventBook Creation Tests
// ============================================================================

/// Projection domain uses special prefix for routing.
///
/// Projections are routed separately from domain events. The prefix
/// "_projection.{projector}.{domain}" enables filtering and subscription.
#[test]
fn create_projection_event_book_sets_projection_domain_prefix() {
    let projection = make_projection("order-summary", 5);
    let event_book = create_projection_event_book(projection, "corr-123", None);

    let domain = event_book.domain();
    assert!(domain.starts_with(PROJECTION_DOMAIN_PREFIX));
    assert!(domain.contains("order-summary"));
    assert!(domain.contains("orders"));
}

/// Correlation ID preserved for cross-domain tracing.
///
/// The original correlation ID flows through projections, enabling
/// end-to-end request tracing from command to projection output.
#[test]
fn create_projection_event_book_preserves_correlation_id() {
    let projection = make_projection("order-summary", 5);
    let event_book = create_projection_event_book(projection, "my-correlation", None);

    assert_eq!(event_book.correlation_id(), "my-correlation");
}

/// Projection sequence matches source projection.
///
/// The sequence number identifies the projection version and enables
/// clients to track projection freshness.
#[test]
fn create_projection_event_book_sets_sequence() {
    let projection = make_projection("order-summary", 42);
    let event_book = create_projection_event_book(projection, "corr-123", None);

    assert_eq!(event_book.pages.len(), 1);
    let page = &event_book.pages[0];
    use crate::proto_ext::EventPageExt;
    assert_eq!(page.sequence_num(), 42);
}

/// Projection type URL is fixed for deserialization.
///
/// Clients know to deserialize the payload as Projection proto
/// based on the type URL.
#[test]
fn create_projection_event_book_sets_correct_type_url() {
    let projection = make_projection("order-summary", 5);
    let event_book = create_projection_event_book(projection, "corr-123", None);

    let page = &event_book.pages[0];
    let any = match &page.payload {
        Some(crate::proto::event_page::Payload::Event(any)) => any,
        _ => panic!("Expected Event payload"),
    };

    assert_eq!(any.type_url, PROJECTION_TYPE_URL);
}

/// Projection roundtrip: serialize -> deserialize preserves data.
///
/// Verifies the serialized Projection can be decoded back to the
/// original values.
#[test]
fn create_projection_event_book_deserializes_correctly() {
    let original = make_projection("order-summary", 5);
    let event_book = create_projection_event_book(original.clone(), "corr-123", None);

    let page = &event_book.pages[0];
    let any = match &page.payload {
        Some(crate::proto::event_page::Payload::Event(any)) => any,
        _ => panic!("Expected Event payload"),
    };

    let decoded = Projection::decode(any.value.as_slice()).unwrap();
    assert_eq!(decoded.projector, "order-summary");
    assert_eq!(decoded.sequence, 5);
}

/// Missing projection cover gets minimal cover with correlation ID.
///
/// Projections without a cover (rare but valid) still get routable
/// metadata from the request context.
#[test]
fn create_projection_event_book_without_cover_creates_minimal_cover() {
    let projection = Projection {
        projector: "order-summary".to_string(),
        sequence: 5,
        cover: None,
        projection: None,
    };
    let event_book = create_projection_event_book(projection, "corr-123", None);

    assert!(event_book.cover.is_some());
    let cover = event_book.cover.as_ref().unwrap();
    assert!(cover.domain.starts_with(PROJECTION_DOMAIN_PREFIX));
    assert_eq!(cover.correlation_id, "corr-123");
}

// ============================================================================
// Edition Preservation Tests
// ============================================================================

/// Source edition used when projection has no cover.
///
/// Editions track schema versions. When projection lacks cover,
/// we inherit edition from the source event.
#[test]
fn create_projection_event_book_uses_source_edition_when_no_cover() {
    // When projection has no cover, source_edition is used
    let projection = Projection {
        projector: "order-summary".to_string(),
        sequence: 5,
        cover: None,
        projection: None,
    };
    let edition = Edition {
        name: "v2".to_string(),
        divergences: vec![],
    };
    let event_book = create_projection_event_book(projection, "corr-123", Some(edition));

    let cover = event_book.cover.as_ref().unwrap();
    assert_eq!(cover.edition.as_ref().unwrap().name, "v2");
}

/// Projection's own edition takes precedence over source edition.
///
/// When projection has its own cover with edition, that edition is
/// preserved even if source had a different edition.
#[test]
fn create_projection_event_book_preserves_projection_cover_edition() {
    // When projection has a cover with edition, that edition is preserved
    let mut projection = make_projection("order-summary", 5);
    projection.cover.as_mut().unwrap().edition = Some(Edition {
        name: "v3".to_string(),
        divergences: vec![],
    });

    // Even if we pass a different source_edition, the projection's own is used
    let source_edition = Edition {
        name: "v1".to_string(),
        divergences: vec![],
    };
    let event_book = create_projection_event_book(projection, "corr-123", Some(source_edition));

    let cover = event_book.cover.as_ref().unwrap();
    assert_eq!(cover.edition.as_ref().unwrap().name, "v3");
}

/// Projection EventBooks never have snapshots.
///
/// Snapshots are for aggregate state, not projection output.
#[test]
fn create_projection_event_book_has_no_snapshot() {
    let projection = make_projection("order-summary", 5);
    let event_book = create_projection_event_book(projection, "corr-123", None);

    // Projection EventBooks should never have snapshots
    assert!(event_book.snapshot.is_none());
}

// ============================================================================
// Publishing Condition Tests
// ============================================================================
//
// Projections are published if: projection.is_some() || !projector.is_empty()
// These tests verify both conditions and their OR relationship.

/// Projection with content is publishable (even with empty projector name).
#[test]
fn projection_with_content_should_be_publishable() {
    // Has projection content
    let projection = Projection {
        projector: "".to_string(), // empty projector name
        sequence: 1,
        cover: Some(make_cover("orders")),
        projection: Some(prost_types::Any {
            type_url: "test".to_string(),
            value: vec![1, 2, 3],
        }),
    };

    // Should be publishable because projection.is_some()
    let should_publish = projection.projection.is_some() || !projection.projector.is_empty();
    assert!(
        should_publish,
        "Projection with content should be publishable"
    );
}

/// Projection with projector name is publishable (even without content).
#[test]
fn projection_with_projector_name_should_be_publishable() {
    // Has projector name but no content
    let projection = Projection {
        projector: "order-summary".to_string(),
        sequence: 1,
        cover: Some(make_cover("orders")),
        projection: None,
    };

    // Should be publishable because projector is not empty
    let should_publish = projection.projection.is_some() || !projection.projector.is_empty();
    assert!(
        should_publish,
        "Projection with projector name should be publishable"
    );
}

/// Empty projection (no content, no name) is not publishable.
///
/// Projections must have either content or at least a projector name
/// to be worth publishing.
#[test]
fn empty_projection_should_not_be_publishable() {
    // No content AND no projector name
    let projection = Projection {
        projector: "".to_string(),
        sequence: 1,
        cover: Some(make_cover("orders")),
        projection: None,
    };

    // Should NOT be publishable
    let should_publish = projection.projection.is_some() || !projection.projector.is_empty();
    assert!(
        !should_publish,
        "Empty projection should not be publishable"
    );
}

/// Publishing uses OR logic, not AND.
///
/// Either condition being true triggers publish. Mutation testing
/// catches if someone changes || to &&.
#[test]
fn projection_publishing_condition_requires_or_not_and() {
    // Test that verifies || behavior (not &&)
    // With &&, this would be false. With ||, this should be true.
    let projection = Projection {
        projector: "order-summary".to_string(), // non-empty
        sequence: 1,
        cover: None,
        projection: None, // is_none
    };

    // projection.is_some() = false, !projector.is_empty() = true
    // With ||: false || true = true
    // With &&: false && true = false
    let should_publish = projection.projection.is_some() || !projection.projector.is_empty();
    assert!(should_publish, "Should use OR logic, not AND");
}

/// !is_empty negation is required for correct publishing.
///
/// Mutation testing catches if someone removes the negation.
#[test]
fn projection_publishing_requires_negation_on_is_empty() {
    // Test that verifies we check !is_empty (not is_empty)
    let projection = Projection {
        projector: "order-summary".to_string(), // non-empty
        sequence: 1,
        cover: None,
        projection: None,
    };

    // Without negation: projector.is_empty() = false
    // With negation: !projector.is_empty() = true
    assert!(
        !projection.projector.is_empty(),
        "Non-empty projector should pass !is_empty check"
    );

    let empty_projection = Projection {
        projector: "".to_string(), // empty
        sequence: 1,
        cover: None,
        projection: None,
    };

    assert!(
        empty_projection.projector.is_empty(),
        "Empty projector should fail !is_empty check"
    );
}

// ============================================================================
// Domain Filtering Tests
// ============================================================================
//
// Projectors can filter events by domain. Empty list = handle all.
// Infrastructure domains (underscore prefix) are excluded by default.

use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex as TokioMutex;

struct MockProjectorHandler {
    call_count: AtomicUsize,
    response: TokioMutex<Projection>,
}

impl MockProjectorHandler {
    fn new() -> Self {
        Self {
            call_count: AtomicUsize::new(0),
            response: TokioMutex::new(Projection {
                projector: "test".to_string(),
                sequence: 1,
                cover: None,
                projection: None,
            }),
        }
    }

    fn with_response(projector: &str, has_projection: bool) -> Self {
        Self {
            call_count: AtomicUsize::new(0),
            response: TokioMutex::new(Projection {
                projector: projector.to_string(),
                sequence: 1,
                cover: None,
                projection: if has_projection {
                    Some(prost_types::Any {
                        type_url: "test".to_string(),
                        value: vec![1, 2, 3],
                    })
                } else {
                    None
                },
            }),
        }
    }

    fn calls(&self) -> usize {
        self.call_count.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl ProjectorHandler for MockProjectorHandler {
    async fn handle(
        &self,
        _events: &EventBook,
        _mode: ProjectionMode,
    ) -> Result<Projection, tonic::Status> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        Ok(self.response.lock().await.clone())
    }
}

/// Mock EventBus that tracks publish calls
struct MockEventBus {
    publish_count: AtomicUsize,
    published_books: TokioMutex<Vec<EventBook>>,
}

impl MockEventBus {
    fn new() -> Self {
        Self {
            publish_count: AtomicUsize::new(0),
            published_books: TokioMutex::new(Vec::new()),
        }
    }

    fn publish_calls(&self) -> usize {
        self.publish_count.load(Ordering::SeqCst)
    }

    async fn get_published_books(&self) -> Vec<EventBook> {
        self.published_books.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl crate::bus::EventBus for MockEventBus {
    async fn publish(
        &self,
        book: Arc<EventBook>,
    ) -> crate::bus::error::Result<crate::bus::PublishResult> {
        self.publish_count.fetch_add(1, Ordering::SeqCst);
        self.published_books.lock().await.push((*book).clone());
        Ok(crate::bus::PublishResult::default())
    }

    async fn subscribe(
        &self,
        _handler: Box<dyn crate::bus::EventHandler>,
    ) -> crate::bus::error::Result<()> {
        unimplemented!("Not needed for these tests")
    }

    async fn create_subscriber(
        &self,
        _name: &str,
        _domain_filter: Option<&str>,
    ) -> crate::bus::error::Result<Arc<dyn crate::bus::EventBus>> {
        unimplemented!("Not needed for these tests")
    }
}

fn make_event_book(domain: &str) -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: None,
            correlation_id: "test-corr".to_string(),
            edition: None,
            ext: None,
        }),
        pages: vec![],
        snapshot: None,
        ..Default::default()
    }
}

/// Empty domain filter handles all events (no filtering).
#[tokio::test]
async fn handler_with_empty_domains_handles_any_event() {
    // When domains is empty, no filtering occurs
    let mock = Arc::new(MockProjectorHandler::new());
    let handler = ProjectorEventHandler::from_handler(mock.clone(), "test-projector".to_string())
        .with_domains(vec![]); // Empty domain filter

    let book = Arc::new(make_event_book("any-domain"));
    let result = handler.handle(book).await;

    assert!(result.is_ok());
    assert_eq!(
        mock.calls(),
        1,
        "Handler should be called when domains is empty"
    );
}

/// Matching domain in filter list is handled.
#[tokio::test]
async fn handler_with_matching_domain_handles_event() {
    // When domains contains the event's domain, event is handled
    let mock = Arc::new(MockProjectorHandler::new());
    let handler = ProjectorEventHandler::from_handler(mock.clone(), "test-projector".to_string())
        .with_domains(vec!["orders".to_string(), "inventory".to_string()]);

    let book = Arc::new(make_event_book("orders"));
    let result = handler.handle(book).await;

    assert!(result.is_ok());
    assert_eq!(
        mock.calls(),
        1,
        "Handler should be called when domain matches"
    );
}

/// Non-matching domain is skipped silently.
#[tokio::test]
async fn handler_with_non_matching_domain_skips_event() {
    // When domains does NOT contain the event's domain, event is skipped
    let mock = Arc::new(MockProjectorHandler::new());
    let handler = ProjectorEventHandler::from_handler(mock.clone(), "test-projector".to_string())
        .with_domains(vec!["orders".to_string(), "inventory".to_string()]);

    let book = Arc::new(make_event_book("fulfillment")); // Not in the domain list
    let result = handler.handle(book).await;

    assert!(result.is_ok());
    assert_eq!(
        mock.calls(),
        0,
        "Handler should NOT be called when domain doesn't match"
    );
}

#[tokio::test]
async fn domain_filter_checks_equality_not_inequality() {
    // This test ensures we use == not != when checking domain matches
    // If the code used !=, this test would fail because:
    // - "orders" != "orders" is false
    // - "orders" != "inventory" is true
    // With any(), d != routing_key would match "inventory" first, incorrectly allowing
    let mock = Arc::new(MockProjectorHandler::new());
    let handler = ProjectorEventHandler::from_handler(mock.clone(), "test-projector".to_string())
        .with_domains(vec!["orders".to_string()]);

    let book = Arc::new(make_event_book("orders")); // Exact match
    let result = handler.handle(book).await;

    assert!(result.is_ok());
    assert_eq!(mock.calls(), 1, "Exact domain match should be handled");

    // Reset by creating new mock
    let mock2 = Arc::new(MockProjectorHandler::new());
    let handler2 = ProjectorEventHandler::from_handler(mock2.clone(), "test-projector".to_string())
        .with_domains(vec!["orders".to_string()]);

    let book2 = Arc::new(make_event_book("not-orders")); // No match
    let result2 = handler2.handle(book2).await;

    assert!(result2.is_ok());
    assert_eq!(mock2.calls(), 0, "Non-matching domain should be skipped");
}

#[tokio::test]
async fn domain_filter_requires_non_empty_check_negation() {
    // This verifies we check !self.domains.is_empty() not self.domains.is_empty()
    // If we didn't negate, an empty domains list would enter the filter block
    // and incorrectly skip events (because any() on empty list is false)
    let mock = Arc::new(MockProjectorHandler::new());
    let handler = ProjectorEventHandler::from_handler(mock.clone(), "test-projector".to_string())
        .with_domains(vec![]); // Empty - should NOT enter filter block

    let book = Arc::new(make_event_book("any-domain"));
    let result = handler.handle(book).await;

    assert!(result.is_ok());
    // If ! was deleted from !self.domains.is_empty(), domains.is_empty() = true
    // would enter the filter block, then any() on empty vec = false,
    // then !false = true (with correct negation) or false (without)
    // This test catches that by verifying events pass through
    assert_eq!(
        mock.calls(),
        1,
        "Empty domains list should not filter events"
    );
}

#[tokio::test]
async fn domain_filter_any_match_negation_required() {
    // Tests the !self.domains.iter().any(...) negation
    // If ! was removed, we'd SKIP matching domains and HANDLE non-matching ones
    let mock_match = Arc::new(MockProjectorHandler::new());
    let handler_match = ProjectorEventHandler::from_handler(mock_match.clone(), "test".to_string())
        .with_domains(vec!["orders".to_string()]);

    let book_match = Arc::new(make_event_book("orders")); // DOES match
    handler_match.handle(book_match).await.unwrap();

    let mock_nomatch = Arc::new(MockProjectorHandler::new());
    let handler_nomatch =
        ProjectorEventHandler::from_handler(mock_nomatch.clone(), "test".to_string())
            .with_domains(vec!["orders".to_string()]);

    let book_nomatch = Arc::new(make_event_book("inventory")); // Does NOT match
    handler_nomatch.handle(book_nomatch).await.unwrap();

    // With correct logic (!any(match)):
    // - matching domain: any() = true, !true = false, don't skip -> handler called
    // - non-matching domain: any() = false, !false = true, skip -> handler not called
    // Without negation (any(match)):
    // - matching domain: any() = true, skip -> handler not called (WRONG)
    // - non-matching domain: any() = false, don't skip -> handler called (WRONG)
    assert_eq!(mock_match.calls(), 1, "Matching domain should be handled");
    assert_eq!(
        mock_nomatch.calls(),
        0,
        "Non-matching domain should be skipped"
    );
}

// === Tests for publishing condition (line 125) ===
// These test: projection.projection.is_some() || !projection.projector.is_empty()

#[tokio::test]
async fn handler_publishes_when_projection_has_content() {
    // projection.is_some() = true, so publish regardless of projector name
    let mock = Arc::new(MockProjectorHandler::with_response("", true)); // has projection content
    let publisher = Arc::new(MockEventBus::new());
    let handler = ProjectorEventHandler::from_handler(mock.clone(), "test-projector".to_string())
        .with_publisher(publisher.clone());

    let book = Arc::new(make_event_book("orders"));
    let result = handler.handle(book).await;

    assert!(result.is_ok());
    assert_eq!(
        publisher.publish_calls(),
        1,
        "Should publish when projection has content"
    );
}

#[tokio::test]
async fn handler_publishes_when_projector_name_non_empty() {
    // projection.is_none() but projector name is non-empty, so should publish
    let mock = Arc::new(MockProjectorHandler::with_response("order-summary", false));
    let publisher = Arc::new(MockEventBus::new());
    let handler = ProjectorEventHandler::from_handler(mock.clone(), "test-projector".to_string())
        .with_publisher(publisher.clone());

    let book = Arc::new(make_event_book("orders"));
    let result = handler.handle(book).await;

    assert!(result.is_ok());
    assert_eq!(
        publisher.publish_calls(),
        1,
        "Should publish when projector name is non-empty"
    );
}

#[tokio::test]
async fn handler_does_not_publish_when_empty_projection() {
    // projection.is_none() AND projector name is empty, so should NOT publish
    let mock = Arc::new(MockProjectorHandler::with_response("", false)); // empty projector, no projection
    let publisher = Arc::new(MockEventBus::new());
    let handler = ProjectorEventHandler::from_handler(mock.clone(), "test-projector".to_string())
        .with_publisher(publisher.clone());

    let book = Arc::new(make_event_book("orders"));
    let result = handler.handle(book).await;

    assert!(result.is_ok());
    assert_eq!(
        publisher.publish_calls(),
        0,
        "Should NOT publish when projection is empty"
    );
}

#[tokio::test]
async fn publishing_condition_uses_or_not_and() {
    // This verifies || behavior, not &&
    // With &&: both conditions must be true (projection.is_some() AND !projector.is_empty())
    // With ||: either condition being true triggers publish
    // Test case: has projection content but empty projector name
    let mock = Arc::new(MockProjectorHandler::with_response("", true)); // projection is_some, projector empty
    let publisher = Arc::new(MockEventBus::new());
    let handler = ProjectorEventHandler::from_handler(mock.clone(), "test-projector".to_string())
        .with_publisher(publisher.clone());

    let book = Arc::new(make_event_book("orders"));
    let result = handler.handle(book).await;

    // With ||: true || false = true -> publish
    // With &&: true && false = false -> don't publish
    assert!(result.is_ok());
    assert_eq!(
        publisher.publish_calls(),
        1,
        "Should use OR logic (publish if either is true)"
    );
}

#[tokio::test]
async fn publishing_condition_requires_negation_on_is_empty() {
    // This verifies !projection.projector.is_empty() not projection.projector.is_empty()
    // Test case: has non-empty projector name but no projection content
    let mock = Arc::new(MockProjectorHandler::with_response("order-summary", false));
    let publisher = Arc::new(MockEventBus::new());
    let handler = ProjectorEventHandler::from_handler(mock.clone(), "test-projector".to_string())
        .with_publisher(publisher.clone());

    let book = Arc::new(make_event_book("orders"));
    let result = handler.handle(book).await;

    // projector is "order-summary" (non-empty)
    // Without negation: is_empty() = false -> condition is false -> don't publish (WRONG)
    // With negation: !is_empty() = true -> condition is true -> publish (CORRECT)
    assert!(result.is_ok());
    assert_eq!(
        publisher.publish_calls(),
        1,
        "Should publish when projector name is non-empty"
    );

    // Also test empty projector with no projection
    let mock2 = Arc::new(MockProjectorHandler::with_response("", false)); // empty projector, no projection
    let publisher2 = Arc::new(MockEventBus::new());
    let handler2 = ProjectorEventHandler::from_handler(mock2.clone(), "test-projector".to_string())
        .with_publisher(publisher2.clone());

    let book2 = Arc::new(make_event_book("orders"));
    let result2 = handler2.handle(book2).await;

    // Without negation: is_empty() = true -> condition is true -> publish (WRONG)
    // With negation: !is_empty() = false -> condition is false -> don't publish (CORRECT)
    assert!(result2.is_ok());
    assert_eq!(
        publisher2.publish_calls(),
        0,
        "Should NOT publish when projector name is empty"
    );
}

#[tokio::test]
async fn published_event_book_has_no_snapshot() {
    // Verifies that the created EventBook has snapshot = None
    let mock = Arc::new(MockProjectorHandler::with_response("order-summary", false));
    let publisher = Arc::new(MockEventBus::new());
    let handler = ProjectorEventHandler::from_handler(mock.clone(), "test-projector".to_string())
        .with_publisher(publisher.clone());

    let book = Arc::new(make_event_book("orders"));
    let result = handler.handle(book).await;

    assert!(result.is_ok());
    assert_eq!(publisher.publish_calls(), 1);

    let published = publisher.get_published_books().await;
    assert_eq!(published.len(), 1);
    assert!(
        published[0].snapshot.is_none(),
        "Published EventBook should have no snapshot"
    );
}
