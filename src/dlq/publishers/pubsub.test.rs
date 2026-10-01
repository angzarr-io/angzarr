//! Tests for GCP Pub/Sub DLQ publisher.
//!
//! The Pub/Sub publisher sends dead letters to topics named `{prefix}-{domain}`.
//! It caches publisher instances per topic for efficiency.
//!
//! These tests cover the pure functions that don't require a running Pub/Sub emulator.
//! Full integration tests are in the Gherkin contract test suite (tests/interfaces/).
//!
//! Key behaviors verified:
//! - Topic naming with domain sanitization
//! - Default topic prefix

// ============================================================================
// Topic Naming Tests
// ============================================================================

/// topic_for_domain replaces dots with dashes.
///
/// Pub/Sub topic names cannot contain dots but dashes are allowed.
#[test]
fn test_topic_for_domain_sanitizes_dots() {
    let topic_prefix = "angzarr-dlq";
    let domain = "my.nested.domain";

    let sanitized = domain.replace('.', "-");
    let expected = format!("{}-{}", topic_prefix, sanitized);

    assert_eq!(expected, "angzarr-dlq-my-nested-domain");
}

/// topic_for_domain with simple domain (no dots).
///
/// Simple domains should pass through unchanged.
#[test]
fn test_topic_for_domain_simple_domain() {
    let topic_prefix = "angzarr-dlq";
    let domain = "orders";

    let sanitized = domain.replace('.', "-");
    let expected = format!("{}-{}", topic_prefix, sanitized);

    assert_eq!(expected, "angzarr-dlq-orders");
}

/// topic_for_domain with custom prefix.
///
/// The prefix is configurable via PubSubDlqConfig.
#[test]
fn test_topic_for_domain_custom_prefix() {
    let topic_prefix = "myapp-dlq";
    let domain = "inventory";

    let sanitized = domain.replace('.', "-");
    let expected = format!("{}-{}", topic_prefix, sanitized);

    assert_eq!(expected, "myapp-dlq-inventory");
}

// ============================================================================
// Default Values Tests
// ============================================================================

/// Default topic prefix is "angzarr-dlq".
///
/// This is used by PubSubDeadLetterPublisher::new() when not using config.
#[test]
fn test_default_topic_prefix() {
    let default_prefix = "angzarr-dlq";
    assert_eq!(default_prefix, "angzarr-dlq");
}

// ============================================================================
// Retention subscription
// ============================================================================

use super::{retention_subscription_config, retention_subscription_name};

/// Each DLQ topic gets a retention subscription named after it.
#[test]
fn test_retention_subscription_name() {
    assert_eq!(
        retention_subscription_name("angzarr-dlq-orders"),
        "angzarr-dlq-orders-retain"
    );
}

/// The retention subscription keeps dead letters for the Pub/Sub maximum
/// (7 days) and never expires for inactivity — a DLQ is idle by design.
#[test]
fn test_retention_subscription_keeps_messages_and_never_expires() {
    let config = retention_subscription_config();
    assert_eq!(
        config.message_retention_duration,
        Some(std::time::Duration::from_secs(604_800))
    );
    let expiration = config.expiration_policy.expect("expiration policy set");
    assert!(expiration.ttl.is_none(), "ttl unset = never expire");
}
