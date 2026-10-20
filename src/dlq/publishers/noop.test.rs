//! Tests for no-op DLQ publisher.
//!
//! The noop publisher is the fallback when no DLQ is configured. It logs
//! dead letters at WARN level but takes no other action.
//!
//! Why this matters: Even without a configured DLQ, dead letters must not
//! be silently dropped. Logging ensures operators can see failures in logs.
//!
//! Key behaviors verified:
//! - is_configured() returns false (distinguishes from logging publisher)
//! - publish() always succeeds (never fails on logging)

use super::*;
use crate::dlq::test_support::dead_letter;

fn make_dead_letter(domain: &str, reason: &str) -> crate::dlq::AngzarrDeadLetter {
    dead_letter(domain, None, reason)
}

/// is_configured() returns false for noop publisher.
///
/// Unlike the logging publisher which reports is_configured = true,
/// noop reports false to indicate no actual DLQ backend is present.
#[test]
fn test_noop_is_not_configured() {
    let publisher = NoopDeadLetterPublisher;
    assert!(!publisher.is_configured());
}

/// publish() always succeeds.
///
/// Noop publisher cannot fail - it just logs and returns Ok.
#[tokio::test]
async fn test_noop_publish_succeeds() {
    let publisher = NoopDeadLetterPublisher;
    let dead_letter = make_dead_letter("orders", "Test rejection");

    let result = publisher.publish(dead_letter).await;

    assert!(result.is_ok());
}
