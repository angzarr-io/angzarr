use super::*;
use crate::proto::{page_header, Cover, EventPage, PageHeader, Uuid as ProtoUuid};
use tokio_stream::StreamExt;

fn make_test_event_book(correlation_id: &str) -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: "test".to_string(),
            root: Some(ProtoUuid {
                value: uuid::Uuid::new_v4().as_bytes().to_vec(),
            }),
            correlation_id: correlation_id.to_string(),
            edition: None,
        }),
        pages: vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(0)),
            }),
            payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
                type_url: "test.Event".to_string(),
                value: vec![],
            })),
            created_at: None,
            ..Default::default()
        }],
        snapshot: None,
        ..Default::default()
    }
}

#[tokio::test]
async fn test_subscribe_creates_subscription() {
    let service = StreamService::new();

    let filter = EventStreamFilter {
        correlation_id: "test-corr-id".to_string(),
    };

    let response = service.subscribe(Request::new(filter)).await.unwrap();
    let _stream = response.into_inner();

    // Verify subscription exists
    let subs = service.subscriptions.read().await;
    assert!(subs.contains_key("test-corr-id"));
    assert_eq!(subs.get("test-corr-id").unwrap().len(), 1);
}

#[tokio::test]
async fn test_subscribe_requires_correlation_id() {
    let service = StreamService::new();

    let filter = EventStreamFilter {
        correlation_id: String::new(),
    };

    let result = service.subscribe(Request::new(filter)).await;
    match result {
        Err(status) => assert_eq!(status.code(), tonic::Code::InvalidArgument),
        Ok(_) => panic!("Expected error for empty correlation_id"),
    }
}

#[tokio::test]
async fn test_subscriber_cleanup_on_disconnect() {
    let service = StreamService::new();

    let filter = EventStreamFilter {
        correlation_id: "cleanup-test".to_string(),
    };

    let response = service.subscribe(Request::new(filter)).await.unwrap();
    let stream = response.into_inner();

    // Verify subscription exists
    {
        let subs = service.subscriptions.read().await;
        assert!(subs.contains_key("cleanup-test"));
    }

    // Drop stream - simulates client disconnect
    drop(stream);

    // Give cleanup task time to run
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Verify subscription was cleaned up
    let subs = service.subscriptions.read().await;
    assert!(
        !subs.contains_key("cleanup-test"),
        "Subscription should be cleaned up after disconnect"
    );
}

#[tokio::test]
async fn test_event_delivery_to_subscriber() {
    let service = StreamService::new();
    let handler = StreamEventHandler::new(&service);

    let filter = EventStreamFilter {
        correlation_id: "delivery-test".to_string(),
    };

    let response = service.subscribe(Request::new(filter)).await.unwrap();
    let mut stream = response.into_inner();

    // Deliver an event
    let book = Arc::new(make_test_event_book("delivery-test"));
    handler.handle(book).await.unwrap();

    // Verify event is received
    let received = tokio::time::timeout(tokio::time::Duration::from_millis(100), stream.next())
        .await
        .expect("Should receive event");

    assert!(received.is_some());
    let event_book = received.unwrap().unwrap();
    assert_eq!(
        event_book.cover.as_ref().unwrap().correlation_id,
        "delivery-test"
    );
}

#[tokio::test]
async fn test_event_dropped_without_subscribers() {
    let service = StreamService::new();
    let handler = StreamEventHandler::new(&service);

    // No subscribers registered for this correlation ID
    let book = Arc::new(make_test_event_book("no-subscriber"));

    // Should not error - events without subscribers are silently dropped
    let result = handler.handle(book).await;
    assert!(result.is_ok());

    // Verify no subscriptions were created
    let subs = service.subscriptions.read().await;
    assert!(!subs.contains_key("no-subscriber"));
}

#[tokio::test]
async fn test_closed_subscriber_removed_on_delivery() {
    let service = StreamService::new();
    let handler = StreamEventHandler::new(&service);

    let filter = EventStreamFilter {
        correlation_id: "closed-sub-test".to_string(),
    };

    let response = service.subscribe(Request::new(filter)).await.unwrap();
    let stream = response.into_inner();

    // Verify subscription exists
    {
        let subs = service.subscriptions.read().await;
        assert!(subs.contains_key("closed-sub-test"));
    }

    // Drop stream to close the receiver
    drop(stream);

    // Give a moment for the closed state to propagate
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Try to deliver an event - this should clean up the closed subscriber
    let book = Arc::new(make_test_event_book("closed-sub-test"));
    handler.handle(book).await.unwrap();

    // Verify subscription was cleaned up
    let subs = service.subscriptions.read().await;
    assert!(
        !subs.contains_key("closed-sub-test"),
        "Closed subscriber should be removed during event delivery"
    );
}

#[tokio::test]
async fn test_multiple_subscribers_same_correlation() {
    let service = StreamService::new();
    let handler = StreamEventHandler::new(&service);

    let filter1 = EventStreamFilter {
        correlation_id: "multi-sub".to_string(),
    };
    let filter2 = EventStreamFilter {
        correlation_id: "multi-sub".to_string(),
    };

    let response1 = service.subscribe(Request::new(filter1)).await.unwrap();
    let response2 = service.subscribe(Request::new(filter2)).await.unwrap();
    let mut stream1 = response1.into_inner();
    let mut stream2 = response2.into_inner();

    // Verify both subscriptions exist
    {
        let subs = service.subscriptions.read().await;
        assert_eq!(subs.get("multi-sub").unwrap().len(), 2);
    }

    // Deliver an event
    let book = Arc::new(make_test_event_book("multi-sub"));
    handler.handle(book).await.unwrap();

    // Both subscribers should receive the event
    let received1 = tokio::time::timeout(tokio::time::Duration::from_millis(100), stream1.next())
        .await
        .expect("Subscriber 1 should receive event");

    let received2 = tokio::time::timeout(tokio::time::Duration::from_millis(100), stream2.next())
        .await
        .expect("Subscriber 2 should receive event");

    assert!(received1.is_some());
    assert!(received2.is_some());
}
