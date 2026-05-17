//! Tests for the Pub/Sub consumer's subscription-creation invariants.
//!
//! These tests are unit tests — they exercise [`build_subscription_config`]
//! directly without spinning up an emulator. The behavioural ordered-delivery
//! check belongs in `tests/bus_pubsub.rs` against the emulator, but it cannot
//! reliably reproduce bug C-11 on the failing path: the emulator's
//! single-process broker tends to deliver in publish order even when
//! `enable_message_ordering` is OFF, so a behavioural test flake-passes on
//! baseline. The deterministic invariant is the config flag itself.

use super::*;

/// The publisher in `bus.rs` stamps `ordering_key = root_id` on every message
/// so GCP Pub/Sub serializes delivery per aggregate root. Pub/Sub honors that
/// key ONLY when the subscription has `enable_message_ordering == true`. With
/// the flag off, the broker is free to reorder messages for the same root,
/// breaking the CQRS-ES per-root ordering invariant the rest of the framework
/// assumes.
///
/// Bug C-11: `ensure_subscription_exists` previously created subscriptions
/// with `SubscriptionConfig::default()` (flag = `false`), silently throwing
/// the ordering key away. This test pins the flag down so any future refactor
/// that drops it fails CI.
#[test]
fn build_subscription_config_enables_message_ordering() {
    let config = build_subscription_config();
    assert!(
        config.enable_message_ordering,
        "Pub/Sub SubscriptionConfig must set enable_message_ordering=true so \
         the broker honors the publisher's ordering_key=root_id. With the flag \
         off (the gcloud-pubsub default), events for the same aggregate root \
         are delivered out of order, violating the CQRS-ES per-root ordering \
         invariant (bug C-11)."
    );
}
