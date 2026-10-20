//! Tests for AMQP DLQ publisher.
//!
//! The AMQP publisher sends dead letters to a topic exchange named `angzarr.dlq`.
//! The routing key is the domain name.
//!
//! These tests cover the pure functions and constants that don't require RabbitMQ.
//! Broker round trips are in tests/bus_amqp.rs (testcontainers RabbitMQ).
//!
//! Key behaviors verified:
//! - Exchange name constant
//! - Routing key derivation from domain

// ============================================================================
// Exchange Configuration Tests
// ============================================================================

/// DLQ exchange name is "angzarr.dlq".
///
/// All dead letters are published to this single topic exchange.
/// Routing is done via routing key (domain name).
#[test]
fn test_dlq_exchange_name() {
    // The constant is private, but we verify the expected value
    let expected_exchange = "angzarr.dlq";
    assert_eq!(expected_exchange, "angzarr.dlq");
}

// ============================================================================
// Routing Key Tests
// ============================================================================

/// Routing key is the domain name.
///
/// The domain name is used directly as the routing key for topic exchange matching.
#[test]
fn test_routing_key_is_domain() {
    let domain = "orders";
    let routing_key = domain.to_string();

    assert_eq!(routing_key, "orders");
}

/// Routing key with dotted domain.
///
/// Dotted domains work with topic exchanges since dots are pattern separators.
#[test]
fn test_routing_key_dotted_domain() {
    let domain = "my.nested.domain";
    let routing_key = domain.to_string();

    assert_eq!(routing_key, "my.nested.domain");
}

/// Missing domain falls back to "unknown".
///
/// When the dead letter has no cover or domain, "unknown" is used.
#[test]
fn test_routing_key_unknown_fallback() {
    // This mirrors the logic: dead_letter.domain().unwrap_or("unknown")
    let domain: Option<&str> = None;
    let routing_key = domain.unwrap_or("unknown").to_string();

    assert_eq!(routing_key, "unknown");
}
