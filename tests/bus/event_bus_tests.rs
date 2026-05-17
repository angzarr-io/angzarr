//! EventBus interface tests.
//!
//! These tests verify the contract of the EventBus trait.
//! Each bus implementation should run these tests.
//!
//! Requires the `test-utils` feature to be enabled.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use angzarr::bus::EventBus;
use angzarr::proto::{
    event_page, page_header::SequenceType, Cover, EventBook, EventPage, PageHeader, Uuid,
};
#[cfg(feature = "test-utils")]
use angzarr::test_utils::CapturingHandler;
use prost_types::Any;
use tokio::sync::mpsc;

/// Create a test EventBook for a given domain.
pub fn make_event_book(domain: &str) -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(Uuid {
                value: uuid::Uuid::new_v4().as_bytes().to_vec(),
            }),
            correlation_id: format!("test-{}", uuid::Uuid::new_v4()),
            edition: None,
        }),
        pages: vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::Sequence(0)),
            }),
            created_at: None,
            payload: Some(event_page::Payload::Event(Any {
                type_url: "type.googleapis.com/test.TestEvent".to_string(),
                value: vec![1, 2, 3],
            })),
            ..Default::default()
        }],
        snapshot: None,
        ..Default::default()
    }
}

// =============================================================================
// EventBus publish/subscribe tests
// =============================================================================

/// Test basic publish and subscribe roundtrip.
pub async fn test_publish_subscribe_roundtrip<B: EventBus>(
    publisher: &B,
    domain: &str,
    subscriber_name: &str,
) {
    let subscriber = publisher
        .create_subscriber(subscriber_name, Some(domain))
        .await
        .expect("Failed to create subscriber");

    let count = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::channel(10);

    subscriber
        .subscribe(Box::new(CapturingHandler::with_count(tx, count.clone())))
        .await
        .expect("Failed to subscribe");

    subscriber
        .start_consuming()
        .await
        .expect("Failed to start consuming");

    // Give consumer time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish event
    let book = make_event_book(domain);
    publisher
        .publish(Arc::new(book.clone()))
        .await
        .expect("Failed to publish");

    // Wait for message
    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("Timed out waiting for message")
        .expect("Channel closed");

    assert_eq!(
        received.cover.as_ref().unwrap().domain,
        domain,
        "Received event should have correct domain"
    );
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "Handler should be called once"
    );
}

/// Test that publish works without any subscribers.
pub async fn test_publish_only<B: EventBus>(publisher: &B, domain: &str) {
    let book = make_event_book(domain);

    // Should succeed without error
    publisher
        .publish(Arc::new(book))
        .await
        .expect("Publish should succeed without subscribers");
}

/// Test receiving multiple messages.
pub async fn test_multiple_messages<B: EventBus>(
    publisher: &B,
    domain: &str,
    subscriber_name: &str,
    message_count: usize,
) {
    let subscriber = publisher
        .create_subscriber(subscriber_name, Some(domain))
        .await
        .expect("Failed to create subscriber");

    let count = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::channel(100);

    subscriber
        .subscribe(Box::new(CapturingHandler::with_count(tx, count.clone())))
        .await
        .expect("Failed to subscribe");

    subscriber
        .start_consuming()
        .await
        .expect("Failed to start consuming");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish multiple messages
    for _ in 0..message_count {
        let book = make_event_book(domain);
        publisher
            .publish(Arc::new(book))
            .await
            .expect("Failed to publish");
    }

    // Wait for all messages
    for i in 0..message_count {
        tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect(&format!("Timed out waiting for message {}", i))
            .expect("Channel closed");
    }

    assert_eq!(
        count.load(Ordering::SeqCst),
        message_count,
        "Handler should be called {} times",
        message_count
    );
}

/// Test domain filtering - subscriber should only receive events for its domain.
pub async fn test_domain_filtering<B: EventBus>(
    publisher: &B,
    target_domain: &str,
    other_domain: &str,
    subscriber_name: &str,
) {
    let subscriber = publisher
        .create_subscriber(subscriber_name, Some(target_domain))
        .await
        .expect("Failed to create subscriber");

    let (tx, mut rx) = mpsc::channel(10);

    subscriber
        .subscribe(Box::new(CapturingHandler::new(tx)))
        .await
        .expect("Failed to subscribe");

    subscriber
        .start_consuming()
        .await
        .expect("Failed to start consuming");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish to target domain - should be received
    publisher
        .publish(Arc::new(make_event_book(target_domain)))
        .await
        .expect("Failed to publish to target domain");

    // Publish to other domain - should NOT be received
    publisher
        .publish(Arc::new(make_event_book(other_domain)))
        .await
        .expect("Failed to publish to other domain");

    // Should receive only the target domain event
    let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("Timed out waiting for target domain event")
        .expect("Channel closed");

    assert_eq!(
        received.cover.as_ref().unwrap().domain,
        target_domain,
        "Should receive event from target domain"
    );

    // Should NOT receive another event (other domain was filtered)
    let timeout_result: Result<Option<EventBook>, _> =
        tokio::time::timeout(Duration::from_millis(500), rx.recv()).await;
    assert!(
        timeout_result.is_err(),
        "Should not receive event from other domain"
    );
}

// =============================================================================
// Multi-domain and multi-handler tests
// =============================================================================

/// Test subscribing to all domains (no filter).
pub async fn test_multi_domain_subscription<B: EventBus>(
    publisher: &B,
    domain1: &str,
    domain2: &str,
    subscriber_name: &str,
) {
    // Create subscriber with no domain filter - receives all domains
    let subscriber = publisher
        .create_subscriber(subscriber_name, None)
        .await
        .expect("Failed to create subscriber");

    let (tx, mut rx) = mpsc::channel(10);

    subscriber
        .subscribe(Box::new(CapturingHandler::new(tx)))
        .await
        .expect("Failed to subscribe");

    subscriber
        .start_consuming()
        .await
        .expect("Failed to start consuming");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish to domain1
    publisher
        .publish(Arc::new(make_event_book(domain1)))
        .await
        .expect("Failed to publish to domain1");

    // Publish to domain2
    publisher
        .publish(Arc::new(make_event_book(domain2)))
        .await
        .expect("Failed to publish to domain2");

    // Should receive both events
    let first = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("Timed out")
        .expect("Channel closed");

    let second = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("Timed out")
        .expect("Channel closed");

    let domains: Vec<_> = [&first, &second]
        .iter()
        .map(|b| b.cover.as_ref().unwrap().domain.as_str())
        .collect();

    assert!(domains.contains(&domain1), "should receive from domain1");
    assert!(domains.contains(&domain2), "should receive from domain2");
}

/// Test multiple independent handlers on same event.
pub async fn test_multiple_handlers_independent<B: EventBus>(
    publisher: &B,
    domain: &str,
    subscriber1_name: &str,
    subscriber2_name: &str,
) {
    // Create two separate subscribers
    let subscriber1 = publisher
        .create_subscriber(subscriber1_name, Some(domain))
        .await
        .expect("Failed to create subscriber1");

    let subscriber2 = publisher
        .create_subscriber(subscriber2_name, Some(domain))
        .await
        .expect("Failed to create subscriber2");

    let count1 = Arc::new(AtomicUsize::new(0));
    let count2 = Arc::new(AtomicUsize::new(0));

    let (tx1, _rx1) = mpsc::channel(10);
    let (tx2, _rx2) = mpsc::channel(10);

    subscriber1
        .subscribe(Box::new(CapturingHandler::with_count(tx1, count1.clone())))
        .await
        .expect("Failed to subscribe");

    subscriber2
        .subscribe(Box::new(CapturingHandler::with_count(tx2, count2.clone())))
        .await
        .expect("Failed to subscribe");

    subscriber1
        .start_consuming()
        .await
        .expect("Failed to start");
    subscriber2
        .start_consuming()
        .await
        .expect("Failed to start");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish single event
    publisher
        .publish(Arc::new(make_event_book(domain)))
        .await
        .expect("Failed to publish");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Both handlers should receive the same event independently
    assert_eq!(
        count1.load(Ordering::SeqCst),
        1,
        "subscriber1 should receive 1 event"
    );
    assert_eq!(
        count2.load(Ordering::SeqCst),
        1,
        "subscriber2 should receive 1 event"
    );
}

// =============================================================================
// Metadata and payload preservation tests
// =============================================================================

/// Test that correlation_id is preserved through transport.
pub async fn test_routing_metadata_preserved<B: EventBus>(
    publisher: &B,
    domain: &str,
    subscriber_name: &str,
) {
    let subscriber = publisher
        .create_subscriber(subscriber_name, Some(domain))
        .await
        .expect("Failed to create subscriber");

    let (tx, mut rx) = mpsc::channel(10);

    subscriber
        .subscribe(Box::new(CapturingHandler::new(tx)))
        .await
        .expect("Failed to subscribe");

    subscriber.start_consuming().await.expect("Failed to start");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create event with specific correlation_id
    let correlation_id = format!("corr-{}", uuid::Uuid::new_v4());
    let mut book = make_event_book(domain);
    book.cover.as_mut().unwrap().correlation_id = correlation_id.clone();

    publisher
        .publish(Arc::new(book))
        .await
        .expect("Failed to publish");

    let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("Timed out")
        .expect("Channel closed");

    assert_eq!(
        received.cover.as_ref().unwrap().correlation_id,
        correlation_id,
        "correlation_id should be preserved"
    );
}

/// Test that binary payload is preserved exactly.
pub async fn test_payload_bytes_exact<B: EventBus>(
    publisher: &B,
    domain: &str,
    subscriber_name: &str,
) {
    let subscriber = publisher
        .create_subscriber(subscriber_name, Some(domain))
        .await
        .expect("Failed to create subscriber");

    let (tx, mut rx) = mpsc::channel(10);

    subscriber
        .subscribe(Box::new(CapturingHandler::new(tx)))
        .await
        .expect("Failed to subscribe");

    subscriber.start_consuming().await.expect("Failed to start");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create event with specific binary payload
    let payload_bytes: Vec<u8> = (0..256).map(|i| i as u8).collect();
    let book = EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(Uuid {
                value: uuid::Uuid::new_v4().as_bytes().to_vec(),
            }),
            correlation_id: "test".to_string(),
            edition: None,
        }),
        pages: vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::Sequence(0)),
            }),
            created_at: None,
            payload: Some(event_page::Payload::Event(Any {
                type_url: "type.test/BinaryPayload".to_string(),
                value: payload_bytes.clone(),
            })),
            ..Default::default()
        }],
        snapshot: None,
        ..Default::default()
    };

    publisher
        .publish(Arc::new(book))
        .await
        .expect("Failed to publish");

    let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("Timed out")
        .expect("Channel closed");

    if let Some(event_page::Payload::Event(event)) = &received.pages[0].payload {
        assert_eq!(
            event.value, payload_bytes,
            "payload bytes should match exactly"
        );
    } else {
        panic!("Expected Event payload");
    }
}

// =============================================================================
// Concurrency tests
// =============================================================================

/// Test parallel publishing doesn't lose messages.
pub async fn test_concurrent_publish_no_loss<B: EventBus + Clone + 'static>(
    publisher: &B,
    domain: &str,
    subscriber_name: &str,
    message_count: usize,
) {
    let subscriber = publisher
        .create_subscriber(subscriber_name, Some(domain))
        .await
        .expect("Failed to create subscriber");

    let count = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::channel(message_count * 2);

    subscriber
        .subscribe(Box::new(CapturingHandler::with_count(tx, count.clone())))
        .await
        .expect("Failed to subscribe");

    subscriber.start_consuming().await.expect("Failed to start");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish messages concurrently
    let mut handles = Vec::new();
    for _ in 0..message_count {
        let pub_clone = publisher.clone();
        let domain_owned = domain.to_string();
        handles.push(tokio::spawn(async move {
            pub_clone
                .publish(Arc::new(make_event_book(&domain_owned)))
                .await
                .expect("Failed to publish");
        }));
    }

    for handle in handles {
        handle.await.expect("Task panicked");
    }

    // Wait for all messages to be received
    let mut received = 0;
    loop {
        match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Some(_)) => received += 1,
            _ => break,
        }
        if received >= message_count {
            break;
        }
    }

    assert_eq!(
        received, message_count,
        "should receive all {} messages without loss",
        message_count
    );
}

// =============================================================================
// Test runner macro
// =============================================================================

/// Run all EventBus interface tests against a bus implementation.
///
/// # Arguments
/// * `$publisher` - The bus instance configured as publisher
/// * `$prefix` - Unique prefix for subscriber names (for test isolation)
/// * `$dlq_config` - Optional DlqConfig for DLQ tests
#[macro_export]
macro_rules! run_event_bus_tests {
    ($publisher:expr, $prefix:expr) => {
        use $crate::bus::event_bus_tests::*;

        // Publish/subscribe roundtrip
        test_publish_subscribe_roundtrip(
            $publisher,
            &format!("{}-roundtrip", $prefix),
            &format!("{}-sub-roundtrip", $prefix),
        )
        .await;
        println!("  test_publish_subscribe_roundtrip: PASSED");

        // Publish only
        test_publish_only($publisher, &format!("{}-publish-only", $prefix)).await;
        println!("  test_publish_only: PASSED");

        // Multiple messages
        test_multiple_messages(
            $publisher,
            &format!("{}-multi", $prefix),
            &format!("{}-sub-multi", $prefix),
            5,
        )
        .await;
        println!("  test_multiple_messages: PASSED");

        // Domain filtering
        test_domain_filtering(
            $publisher,
            &format!("{}-target", $prefix),
            &format!("{}-other", $prefix),
            &format!("{}-sub-filter", $prefix),
        )
        .await;
        println!("  test_domain_filtering: PASSED");

        // Multi-domain subscription
        test_multi_domain_subscription(
            $publisher,
            &format!("{}-md1", $prefix),
            &format!("{}-md2", $prefix),
            &format!("{}-sub-multi-domain", $prefix),
        )
        .await;
        println!("  test_multi_domain_subscription: PASSED");

        // Multiple handlers independent
        test_multiple_handlers_independent(
            $publisher,
            &format!("{}-mh", $prefix),
            &format!("{}-sub-mh1", $prefix),
            &format!("{}-sub-mh2", $prefix),
        )
        .await;
        println!("  test_multiple_handlers_independent: PASSED");

        // Metadata preservation
        test_routing_metadata_preserved(
            $publisher,
            &format!("{}-meta", $prefix),
            &format!("{}-sub-meta", $prefix),
        )
        .await;
        println!("  test_routing_metadata_preserved: PASSED");

        // Payload bytes exact
        test_payload_bytes_exact(
            $publisher,
            &format!("{}-bytes", $prefix),
            &format!("{}-sub-bytes", $prefix),
        )
        .await;
        println!("  test_payload_bytes_exact: PASSED");
    };
}
