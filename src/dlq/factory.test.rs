//! Tests for DLQ publisher factory.
//!
//! The factory uses self-registration (inventory crate) to discover available
//! DLQ backends at compile time. This enables modular backend support without
//! explicit registration.
//!
//! Why this matters: New DLQ backends can be added without modifying the
//! factory code. Feature flags control which backends are compiled in.
//!
//! Key behaviors verified:
//! - Empty config returns noop publisher
//! - Unknown backend types are rejected
//! - Single target returns publisher directly (no chaining overhead)

use super::*;

/// Empty config returns noop publisher.
///
/// No DLQ configured means dead letters are silently discarded.
/// is_configured() returns false to signal this to callers.
#[tokio::test]
async fn test_init_dlq_publisher_empty_config() {
    let config = DlqConfig::default();
    let publisher = init_dlq_publisher(&config).await.unwrap();
    assert!(!publisher.is_configured());
}

// ---------------------------------------------------------------------------
// R2-15 hard-fail boot contract
// ---------------------------------------------------------------------------
//
// When dlq.targets is configured but a target cannot be constructed (unknown
// type, unreachable broker, missing credentials, etc.), `init_dlq_publisher`
// must return Err so the calling bin can fail boot loudly. Falling back to a
// noop or chained-with-noop publisher here would silently drop dead letters
// for the duration of the bin's lifetime -- exactly the failure mode R2-15
// is trying to eliminate.
//
// The contract is enforced by the `?` operator on `create_single_publisher`
// in the factory; these tests pin it so a future refactor that swaps `?`
// for `unwrap_or_else(|_| noop)` (or similar) gets caught immediately.

/// Unknown backend type propagates as `DlqError::UnknownType` up to the
/// caller -- not silently swallowed into a noop publisher. Triggered when
/// an operator typos the dlq_type (e.g., "ampq" for "amqp") or when a
/// build is missing a feature flag that registers the named backend.
#[tokio::test]
async fn init_dlq_publisher_unknown_backend_returns_err() {
    use super::super::config::DlqTargetConfig;

    let config = DlqConfig {
        targets: vec![DlqTargetConfig {
            dlq_type: "no-such-backend".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    };

    let result = init_dlq_publisher(&config).await;
    assert!(
        result.is_err(),
        "init_dlq_publisher must hard-fail on an unknown backend type; \
         falling back to noop would silently drop dead letters"
    );
}

/// Unreachable AMQP broker -- the lapin client returns an error from
/// `AmqpDeadLetterPublisher::new`, which the backend's inventory closure
/// surfaces as `Some(Err(...))`, which `create_single_publisher` returns,
/// which `init_dlq_publisher` propagates via `?`. Gated on the `amqp`
/// feature because the AMQP backend's inventory registration is gated
/// the same way; without the feature, the test would hit the
/// UnknownType arm (already covered above) instead of the actual
/// connection-refused path. Uses 127.0.0.1:9999 (no DNS, connection
/// refused in ~ms) rather than a remote unreachable host to keep the
/// test fast even on offline machines.
#[cfg(feature = "amqp")]
#[tokio::test]
async fn init_dlq_publisher_unreachable_amqp_returns_err() {
    use super::super::config::{AmqpDlqConfig, DlqTargetConfig};

    let config = DlqConfig {
        targets: vec![DlqTargetConfig {
            dlq_type: "amqp".to_string(),
            amqp: Some(AmqpDlqConfig {
                url: "amqp://127.0.0.1:9999/".to_string(),
            }),
            ..Default::default()
        }],
        ..Default::default()
    };

    let result = init_dlq_publisher(&config).await;
    assert!(
        result.is_err(),
        "init_dlq_publisher must hard-fail when the configured AMQP broker \
         is unreachable; falling back to noop would silently drop dead \
         letters until an operator notices the missing audit trail"
    );
}
