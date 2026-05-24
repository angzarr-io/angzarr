//! Tests for the mock event bus.
//!
//! MockEventBus is a test double that captures published events without
//! actual transport. It verifies:
//!
//! - Events are captured for assertion
//! - Configurable publish failures for error path testing
//! - Subscribe operations return appropriate errors (not supported)
//!
//! Why this matters: Tests need isolation from real bus infrastructure
//! while still exercising publish logic. MockEventBus provides a simple,
//! deterministic test double that captures published events for assertion.

use super::*;
use crate::proto::{Cover, PageHeader, Uuid as ProtoUuid};
use uuid::Uuid;

fn make_event_book(domain: &str, root: Uuid, event_count: usize) -> EventBook {
    use crate::proto::EventPage;

    EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        pages: (0..event_count)
            .map(|i| EventPage {
                header: Some(PageHeader {
                    sync_mode: None,
                    sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(
                        i as u32,
                    )),
                }),
                payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
                    type_url: format!("test.Event{}", i),
                    value: vec![],
                })),
                created_at: None,
                ..Default::default()
            })
            .collect(),
        snapshot: None,
        ..Default::default()
    }
}

/// Published events are captured for later assertion.
#[tokio::test]
async fn test_mock_event_bus_publish() {
    let bus = MockEventBus::new();
    let book = Arc::new(make_event_book("orders", Uuid::new_v4(), 1));

    bus.publish(book).await.unwrap();

    assert_eq!(bus.published_count().await, 1);
}

/// Configurable failure enables testing error handling paths.
#[tokio::test]
async fn test_mock_event_bus_fail_on_publish() {
    let bus = MockEventBus::new();
    bus.set_fail_on_publish(true).await;

    let book = Arc::new(make_event_book("orders", Uuid::new_v4(), 1));
    let result = bus.publish(book).await;

    assert!(result.is_err());
}

/// Subscribe returns error — mock bus is publish-only.
///
/// Subscribe requires consumer infrastructure. Tests that need subscription
/// behavior should use ChannelEventBus instead.
#[tokio::test]
async fn test_mock_event_bus_subscribe_not_supported() {
    let bus = MockEventBus::new();

    struct DummyHandler;
    impl EventHandler for DummyHandler {
        fn handle(
            &self,
            _book: Arc<EventBook>,
        ) -> futures::future::BoxFuture<'static, std::result::Result<(), BusError>> {
            Box::pin(async { Ok(()) })
        }
    }

    let result = bus.subscribe(Box::new(DummyHandler)).await;
    assert!(matches!(result, Err(BusError::SubscribeNotSupported)));
}
