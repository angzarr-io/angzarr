//! Tests for bus configuration helpers.

use super::*;

/// A saga/PM/projector that names its source domains must get a
/// domain-scoped subscription. Subscribing to every domain instead makes
/// the sidecar invoke its client for every event on the bus, and on
/// per-domain-topic transports (Pub/Sub, SNS/SQS) an all-domains
/// subscription has no topic to attach to at all.
#[test]
fn for_targets_scopes_subscription_to_target_domains() {
    let targets = vec![
        Target::new("order", vec!["OrderCreated"]),
        Target::domain("inventory"),
    ];

    match EventBusMode::for_targets("saga-fulfillment", &targets) {
        EventBusMode::Subscriber { queue, domains } => {
            assert_eq!(queue, "saga-fulfillment");
            assert_eq!(domains, vec!["order".to_string(), "inventory".to_string()]);
        }
        other => panic!("expected domain-scoped Subscriber, got {other:?}"),
    }
}

/// Two targets on the same domain (different event types) must not bind
/// the same domain twice — a duplicate binding would create a second
/// per-domain topic subscription on Pub/Sub/SNS and double-deliver.
#[test]
fn for_targets_deduplicates_domains_in_first_seen_order() {
    let targets = vec![
        Target::new("order", vec!["OrderCreated"]),
        Target::domain("payment"),
        Target::new("order", vec!["OrderShipped"]),
    ];

    match EventBusMode::for_targets("pm-checkout", &targets) {
        EventBusMode::Subscriber { domains, .. } => {
            assert_eq!(domains, vec!["order".to_string(), "payment".to_string()]);
        }
        other => panic!("expected domain-scoped Subscriber, got {other:?}"),
    }
}

/// No targets is the documented "receive everything" projector default.
#[test]
fn for_targets_without_targets_subscribes_to_all_domains() {
    match EventBusMode::for_targets("projector-audit", &[]) {
        EventBusMode::SubscriberAll { queue } => assert_eq!(queue, "projector-audit"),
        other => panic!("expected SubscriberAll, got {other:?}"),
    }
}

// ============================================================================
// DeliveryConfig
// ============================================================================

/// The budget is exhausted exactly at `max_attempts` failures — one
/// earlier dead-letters an event that still had a retry left, one later
/// lets a poison event block its key for an extra round.
#[test]
fn delivery_budget_exhausts_at_max_attempts() {
    let cfg = DeliveryConfig {
        max_attempts: 3,
        ..Default::default()
    };
    assert!(!cfg.is_exhausted(2));
    assert!(cfg.is_exhausted(3));
    assert!(cfg.is_exhausted(4));
}

/// `max_attempts = 0` is the explicit opt-out: never dead-letter.
#[test]
fn delivery_budget_zero_means_unlimited() {
    let cfg = DeliveryConfig {
        max_attempts: 0,
        ..Default::default()
    };
    assert!(!cfg.is_exhausted(0));
    assert!(!cfg.is_exhausted(1_000_000));
}

/// Backoff doubles from the initial delay and is capped, so a transient
/// downstream outage is ridden out without a hot redelivery loop.
#[test]
fn delivery_backoff_doubles_and_caps() {
    let cfg = DeliveryConfig {
        max_attempts: 10,
        initial_backoff_ms: 100,
        max_backoff_ms: 450,
    };
    assert_eq!(cfg.backoff(1).as_millis(), 100);
    assert_eq!(cfg.backoff(2).as_millis(), 200);
    assert_eq!(cfg.backoff(3).as_millis(), 400);
    assert_eq!(cfg.backoff(4).as_millis(), 450);
    assert_eq!(cfg.backoff(64).as_millis(), 450);
}

/// Defaults cap poison retries (the review's "blocks its key forever")
/// while leaving roughly a minute for transient outages.
#[test]
fn delivery_defaults_cap_retries() {
    let cfg = DeliveryConfig::default();
    assert_eq!(cfg.max_attempts, 10);
    assert_eq!(cfg.initial_backoff_ms, 200);
    assert_eq!(cfg.max_backoff_ms, 10_000);
}
