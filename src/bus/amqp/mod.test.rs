//! Tests for AMQP event bus configuration and routing.
//!
//! These are unit tests for the configuration and routing logic.
//! Integration tests (requiring RabbitMQ) are in tests/bus_amqp.rs.
//!
//! Key behaviors verified:
//! - Routing key generation from EventBook (domain + root_id)
//! - Publisher config has no queue binding
//! - Subscriber config sets routing key pattern
//! - H-06: queue args include `x-dead-letter-exchange` so the
//!   decode-reject path lands in the DLQ rather than being silently
//!   dropped.
//! - H-07: the consume_with_reconnect backoff is reset only after a
//!   real (post-handshake) message delivery, not on handshake success.

use super::*;
use crate::proto::{Cover, Uuid};
use lapin::types::{AMQPValue, ShortString};

/// Routing key = "{domain}.{root_id_hex}".
///
/// This routing format enables topic exchange routing:
/// - "orders.*" matches all order aggregate events
/// - "#" matches all events
#[test]
fn test_routing_key_generation() {
    let book = EventBook {
        cover: Some(Cover {
            domain: "orders".to_string(),
            root: Some(Uuid {
                value: b"test-123".to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
        }),
        pages: vec![],
        snapshot: None,
        ..Default::default()
    };

    // "test-123" as bytes becomes "746573742d313233" in hex
    assert_eq!(AmqpEventBus::routing_key(&book), "orders.746573742d313233");
}

/// Publisher config declares exchange but no queue binding.
///
/// Publishers don't consume — they just need the exchange name.
#[test]
fn test_publisher_config() {
    let config = AmqpConfig::publisher("amqp://localhost:5672");
    assert_eq!(config.exchange, "angzarr.events");
    assert!(config.queue.is_none());
}

/// Subscriber config sets queue and routing key pattern.
///
/// Pattern "{domain}.*" routes all events for that domain to this queue.
#[test]
fn test_subscriber_config() {
    let config = AmqpConfig::subscriber("amqp://localhost:5672", "orders-projector", "orders");
    assert_eq!(config.routing_key, Some("orders.*".to_string()));
    assert_eq!(config.queue, Some("orders-projector".to_string()));
}

// ----------------------------------------------------------------------------
// H-06: DLX naming + queue-argument table
// ----------------------------------------------------------------------------

/// DLX naming convention is `{queue}.dlx`.
///
/// Operators and dashboards need a deterministic name to discover the
/// per-queue dead-letter exchange. Mutating this format is observable on
/// the broker side (queue declarations with `x-dead-letter-exchange`)
/// and must therefore be pinned by a test.
#[test]
fn test_dead_letter_exchange_name_convention() {
    assert_eq!(
        AmqpEventBus::dead_letter_exchange_name("orders-projector"),
        "orders-projector.dlx"
    );
    assert_eq!(AmqpEventBus::dead_letter_exchange_name(""), ".dlx");
}

/// DLQ naming convention is `{queue}.dlq`.
///
/// Mirrored from `dead_letter_exchange_name`; same rationale.
#[test]
fn test_dead_letter_queue_name_convention() {
    assert_eq!(
        AmqpEventBus::dead_letter_queue_name("orders-projector"),
        "orders-projector.dlq"
    );
    assert_eq!(AmqpEventBus::dead_letter_queue_name(""), ".dlq");
}

/// H-06 regression: `build_queue_args` must insert `x-dead-letter-exchange`
/// when the caller supplies a DLX name.
///
/// Baseline (pre-H-06) `setup_consumer` inlined the queue-arg construction
/// and never touched `x-dead-letter-exchange`. Rejected messages
/// (`delivery.reject(requeue: false)` from the decode-error branch in
/// `process_delivery`) therefore had nowhere to go and the broker dropped
/// them on the floor — silent data loss. The fix surfaces the DLX as an
/// explicit argument so unit tests can pin its presence without standing
/// up RabbitMQ.
#[test]
fn test_build_queue_args_includes_dlx_when_provided() {
    let args = AmqpEventBus::build_queue_args(Some(3_600_000), Some(100_000), Some("orders.dlx"));

    let dlx_key = ShortString::from("x-dead-letter-exchange");
    let dlx_value = args
        .inner()
        .get(&dlx_key)
        .expect("x-dead-letter-exchange must be set on the queue args (H-06)");
    match dlx_value {
        AMQPValue::LongString(s) => assert_eq!(s.as_str(), "orders.dlx"),
        other => panic!(
            "x-dead-letter-exchange must be a LongString carrying the DLX \
             exchange name; got {:?} (H-06)",
            other
        ),
    }
}

/// `build_queue_args` omits `x-dead-letter-exchange` when the caller does
/// not pass one.
///
/// This pins the helper's "no DLX requested → no argument" branch so a
/// publisher-only configuration (no consumer wiring) does not inherit
/// surprise DLX state.
#[test]
fn test_build_queue_args_omits_dlx_when_not_provided() {
    let args = AmqpEventBus::build_queue_args(Some(3_600_000), Some(100_000), None);
    let dlx_key = ShortString::from("x-dead-letter-exchange");
    assert!(
        args.inner().get(&dlx_key).is_none(),
        "x-dead-letter-exchange must NOT be set when caller passes None"
    );
}

/// `build_queue_args` still inserts `x-message-ttl` and `x-max-length`
/// when supplied — regression guard for the pre-H-06 behavior the
/// extraction is supposed to preserve.
#[test]
fn test_build_queue_args_preserves_ttl_and_max_length() {
    let args = AmqpEventBus::build_queue_args(Some(60_000), Some(500), None);

    let ttl_key = ShortString::from("x-message-ttl");
    match args.inner().get(&ttl_key) {
        Some(AMQPValue::LongInt(n)) => assert_eq!(*n, 60_000),
        other => panic!("x-message-ttl missing or wrong type: {:?}", other),
    }

    let max_len_key = ShortString::from("x-max-length");
    match args.inner().get(&max_len_key) {
        Some(AMQPValue::LongInt(n)) => assert_eq!(*n, 500),
        other => panic!("x-max-length missing or wrong type: {:?}", other),
    }
}

/// `build_queue_args` omits both TTL and max-length when neither is
/// supplied, but still installs the DLX if the caller asked for it.
///
/// Pins the per-arg independence of the helper — mutating any single
/// `if let Some(...)` arm should fail at least one branch of the matrix.
#[test]
fn test_build_queue_args_independent_arms() {
    let args = AmqpEventBus::build_queue_args(None, None, Some("foo.dlx"));
    assert!(args
        .inner()
        .get(&ShortString::from("x-message-ttl"))
        .is_none());
    assert!(args
        .inner()
        .get(&ShortString::from("x-max-length"))
        .is_none());
    assert!(args
        .inner()
        .get(&ShortString::from("x-dead-letter-exchange"))
        .is_some());
}

// ----------------------------------------------------------------------------
// H-07: backoff reset decision
// ----------------------------------------------------------------------------

/// H-07 regression: `should_reset_backoff_on_delivery` returns true ONLY
/// on the first delivery in a consumer session.
///
/// Baseline (pre-H-07) reset the backoff inside `setup_consumer`-success
/// — i.e. on handshake, BEFORE any delivery confirmed the consumer was
/// healthy. A stream that immediately ended (broker queue churn, idle
/// reap, transient network blip after handshake) caused the agent to
/// fall through to `backoff_iter.next()` for the post-stream-end pause,
/// then re-enter setup_consumer. Each cycle advanced the iterator
/// without ever delivering a message, so the back-off grew exponentially
/// on what were morally "benign reconnects" — observable as a slowly
/// stalling consumer with no error log.
///
/// The fix gates the reset on real deliveries. The exact "first delivery
/// triggers reset" semantics matter: a >=1 mutation that resets on every
/// delivery is functionally equivalent (idempotent in the happy path) but
/// adds an unnecessary reset call per message; the test pins the
/// one-shot-per-session shape.
#[test]
fn test_should_reset_backoff_on_first_delivery_only() {
    // Before any delivery — handshake-only — must NOT trigger reset.
    assert!(
        !AmqpEventBus::should_reset_backoff_on_delivery(0),
        "handshake-only (no delivery observed) MUST NOT reset the backoff \
         — that is the exact H-07 bug. The pre-fix code reset on \
         setup_consumer success, which is count==0 at this checkpoint."
    );

    // First delivery flips the switch.
    assert!(
        AmqpEventBus::should_reset_backoff_on_delivery(1),
        "first delivery in the consumer session is the post-H-07 trigger \
         for backoff reset (consumer proved healthy by emitting a real \
         message)"
    );

    // Subsequent deliveries must not re-trigger reset; the per-session
    // reset is a one-shot.
    for n in 2..=10 {
        assert!(
            !AmqpEventBus::should_reset_backoff_on_delivery(n),
            "delivery #{n} must NOT re-trigger backoff reset (the per-session \
             reset is a one-shot at count==1; idempotent resets would be \
             functionally equivalent but would let a `== 1` → `>= 1` \
             mutation slip through unobserved)"
        );
    }
}
