//! Tests for logging-based DLQ publisher.
//!
//! The logging publisher is a last-resort fallback that logs dead letters
//! at WARN level. Unlike NoopDeadLetterPublisher, it reports is_configured()
//! true because it actively logs (observability value).
//!
//! Why this matters: Even when all other DLQ targets fail, logging ensures
//! the dead letter is captured somewhere (log aggregators like Splunk/Datadog
//! can alert on these).
//!
//! Covers publishing, is_configured and a dead letter without a correlation.

use super::*;
use crate::dlq::test_support::dead_letter;
use crate::dlq::AngzarrDeadLetter;

fn make_dead_letter(domain: &str, reason: &str) -> AngzarrDeadLetter {
    dead_letter(domain, None, reason)
}

/// A dead letter without a correlation still logs.
///
/// A command refused before ids were assigned has no correlation; the
/// publisher must not fail on it.
#[tokio::test]
async fn test_logging_publisher_handles_missing_correlation() {
    let publisher = LoggingDeadLetterPublisher;
    let dead_letter = make_dead_letter("orders", "Test rejection");
    assert!(dead_letter.correlation_id().is_none());

    let result = publisher.publish(dead_letter).await;

    assert!(result.is_ok(), "Should handle a missing correlation");
}

/// is_configured() returns true for logging publisher.
///
/// Unlike noop publisher, logging publisher reports as configured because
/// it actively logs (provides observability value).
#[test]
fn test_logging_is_configured() {
    let publisher = LoggingDeadLetterPublisher;
    assert!(publisher.is_configured());
}

/// publish() always succeeds.
///
/// Logging cannot fail - writes to stdout/stderr are infallible.
#[tokio::test]
async fn test_logging_publish_succeeds() {
    let publisher = LoggingDeadLetterPublisher;
    let dead_letter = make_dead_letter("inventory", "Conflict on sequence");

    let result = publisher.publish(dead_letter).await;

    assert!(result.is_ok());
}
