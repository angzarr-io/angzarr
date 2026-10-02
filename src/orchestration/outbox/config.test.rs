//! The outbox configuration block.

use super::*;

/// The shared defaults: 10 attempts, 200ms initial backoff, 30s cap,
/// jitter on, a 1s drain interval.
#[test]
fn defaults_match_the_documented_schedule() {
    let config = OutboxConfig::default();
    let policy = config.policy("any");
    assert_eq!(policy, RetryPolicy::default());
    assert_eq!(policy.max_attempts, 10);
    assert_eq!(policy.initial_backoff, Duration::from_millis(200));
    assert_eq!(policy.max_backoff, Duration::from_secs(30));
    assert!(policy.jitter);
    assert_eq!(config.drain_interval(), Duration::from_secs(1));
}

/// An override replaces only the fields it sets, for its outbox only.
#[test]
fn overrides_apply_per_outbox() {
    let mut config = OutboxConfig {
        max_attempts: 7,
        initial_backoff_ms: 50,
        max_backoff_ms: 5_000,
        jitter: false,
        drain_interval_ms: 250,
        overrides: HashMap::new(),
    };
    config.overrides.insert(
        "order-fulfillment".to_string(),
        OutboxOverride {
            max_attempts: Some(3),
            jitter: Some(true),
            ..Default::default()
        },
    );

    let shared = config.policy("payments");
    assert_eq!(shared.max_attempts, 7);
    assert_eq!(shared.initial_backoff, Duration::from_millis(50));
    assert_eq!(shared.max_backoff, Duration::from_secs(5));
    assert!(!shared.jitter);

    let overridden = config.policy("order-fulfillment");
    assert_eq!(overridden.max_attempts, 3);
    assert!(overridden.jitter);
    assert_eq!(overridden.initial_backoff, Duration::from_millis(50));
    assert_eq!(overridden.max_backoff, Duration::from_secs(5));
    assert_eq!(config.drain_interval(), Duration::from_millis(250));
}

/// A zero budget still makes one attempt; a zero interval still ticks.
#[test]
fn degenerate_values_are_clamped() {
    let config = OutboxConfig {
        max_attempts: 0,
        drain_interval_ms: 0,
        ..OutboxConfig::default()
    };
    assert_eq!(config.policy("x").max_attempts, 1);
    assert_eq!(config.drain_interval(), Duration::from_millis(1));
}

/// The block deserializes from YAML with partial fields and overrides.
#[test]
fn deserializes_partial_yaml() {
    let config: OutboxConfig =
        from_json(r#"{"max_attempts": 4, "overrides": {"inventory": {"max_backoff_ms": 900}}}"#);
    assert_eq!(config.max_attempts, 4);
    assert_eq!(config.initial_backoff_ms, 200, "unset fields keep defaults");
    assert_eq!(
        config.policy("inventory").max_backoff,
        Duration::from_millis(900)
    );
}

fn from_json(text: &str) -> OutboxConfig {
    serde_json::from_str(text).unwrap()
}
