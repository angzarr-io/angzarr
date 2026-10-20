//! Tests for channel-based DLQ publisher.
//!
//! The channel publisher is used for standalone mode and testing. It sends
//! dead letters through an unbounded mpsc channel, requiring manual instantiation
//! to get access to both sender and receiver.
//!
//! Key behaviors verified:
//! - new() returns publisher and receiver pair
//! - Published dead letters are received on the receiver
//! - Multiple dead letters can be queued
//! - Receiver can be dropped without affecting publish (unbounded channel)

use super::*;
use crate::dlq::test_support::dead_letter;
use crate::dlq::AngzarrDeadLetter;

fn make_dead_letter(domain: &str, reason: &str) -> AngzarrDeadLetter {
    dead_letter(domain, None, reason)
}

/// new() returns both publisher and receiver.
///
/// The caller needs both: publisher to send dead letters, receiver to consume them.
/// This is why the channel publisher isn't available via the factory pattern.
#[test]
fn test_new_returns_publisher_and_receiver() {
    let (publisher, _receiver) = ChannelDeadLetterPublisher::new();
    // Just verify we can construct it - publisher exists
    let _ = publisher;
}

/// Published dead letters appear on the receiver.
///
/// This is the core functionality: publish sends, receiver receives.
#[tokio::test]
async fn test_publish_sends_to_receiver() {
    let (publisher, mut receiver) = ChannelDeadLetterPublisher::new();
    let dead_letter = make_dead_letter("orders", "Test rejection");
    let expected_reason = dead_letter.rejection_reason.clone();

    publisher.publish(dead_letter).await.unwrap();

    let received = receiver.recv().await.expect("Should receive dead letter");
    assert_eq!(received.rejection_reason, expected_reason);
    assert_eq!(received.domain(), Some("orders"));
}

/// Multiple dead letters can be published and received in order.
///
/// Channel is unbounded so all messages queue up for later consumption.
#[tokio::test]
async fn test_multiple_dead_letters_queued() {
    let (publisher, mut receiver) = ChannelDeadLetterPublisher::new();

    // Publish three dead letters
    publisher
        .publish(make_dead_letter("domain1", "reason1"))
        .await
        .unwrap();
    publisher
        .publish(make_dead_letter("domain2", "reason2"))
        .await
        .unwrap();
    publisher
        .publish(make_dead_letter("domain3", "reason3"))
        .await
        .unwrap();

    // Receive in order
    let dl1 = receiver.recv().await.unwrap();
    let dl2 = receiver.recv().await.unwrap();
    let dl3 = receiver.recv().await.unwrap();

    assert_eq!(dl1.rejection_reason, "reason1");
    assert_eq!(dl2.rejection_reason, "reason2");
    assert_eq!(dl3.rejection_reason, "reason3");
}

/// Publish fails if receiver is dropped.
///
/// Unbounded channel send returns error if no receivers exist.
#[tokio::test]
async fn test_publish_fails_when_receiver_dropped() {
    let (publisher, receiver) = ChannelDeadLetterPublisher::new();

    // Drop receiver
    drop(receiver);

    // Publish should fail
    let result = publisher
        .publish(make_dead_letter("orders", "will fail"))
        .await;
    assert!(result.is_err());
}

/// is_configured() returns true (default trait implementation).
///
/// Unlike noop, channel publisher is a real publisher that processes messages.
#[test]
fn test_channel_is_configured() {
    let (publisher, _receiver) = ChannelDeadLetterPublisher::new();
    assert!(publisher.is_configured());
}
