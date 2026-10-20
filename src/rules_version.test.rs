//! Tests for the snapshot rule.
//!
//! Why this matters: an incompatible snapshot folded by business code is
//! silently wrong state; an over-strict rule costs only replay time. The
//! contract scenarios (C-0522..C-0530, C-0553) pin the rule end to end; these
//! pin its edges directly.

use super::*;
use crate::test_utils::aggregate_descriptor;

fn registration(version: &str) -> Registration {
    Registration::new(aggregate_descriptor(
        "order",
        "/orders.v1.OrderState",
        version,
        false,
    ))
    .unwrap()
}

fn snapshot(state: &str, version: &str) -> Snapshot {
    Snapshot {
        sequence: 5,
        state: Some(prost_types::Any {
            type_url: state.into(),
            value: vec![],
        }),
        rules_version: version.into(),
    }
}

#[test]
fn test_default_rule_major_minor() {
    let rules = SnapshotRules::default();
    assert!(rules.compatible("order", "1.4.9+build.12", "1.4.2+build.7"));
    assert!(rules.compatible("order", "1.4.3-rc.1", "1.4.2"));
    assert!(!rules.compatible("order", "1.5.0", "1.4.2"));
    assert!(!rules.compatible("order", "2.4.2", "1.4.2"));
}

#[test]
fn test_default_rule_strips_one_v() {
    let rules = SnapshotRules::default();
    assert!(rules.compatible("order", "v1.4.0", "1.4.2"));
    assert!(rules.compatible("order", "V1.4.9", "1.4.2"));
    assert!(rules.compatible("order", "1.4.0", "v1.4.2"));
    assert!(!rules.compatible("order", "vv1.4.2", "1.4.2"));
    assert!(rules.compatible("order", "vv1.4.2", "vv1.4.2"));
}

#[test]
fn test_non_semver_needs_exact_equality() {
    let rules = SnapshotRules::default();
    assert!(rules.compatible("order", "build-3", "build-3"));
    assert!(!rules.compatible("order", "build-4", "build-3"));
    assert!(!rules.compatible("order", "1.4", "1.4.2"));
    assert!(!rules.compatible("order", "", "1.4.2"));
}

#[test]
fn test_override_replaces_default_and_reject_wins() {
    let config = SnapshotCompatibilityConfig::from([(
        "order".to_string(),
        SnapshotRuleConfig {
            accept: Some(r"1\..*".into()),
            reject: Some(r"1\.3\.0(\+.*)?".into()),
        },
    )]);
    let rules = SnapshotRules::from_config(&config).unwrap();
    // The registered version no longer matters for an overridden domain.
    assert!(rules.compatible("order", "1.9.0", "7.0.0"));
    assert!(!rules.compatible("order", "1.3.0+build.2", "1.3.0"));
    assert!(!rules.compatible("order", "2.0.0", "2.0.0"));
    // Other domains keep the default rule.
    assert!(rules.compatible("inventory", "2.0.1", "2.0.0"));
}

#[test]
fn test_patterns_match_the_whole_version() {
    let config = SnapshotCompatibilityConfig::from([(
        "order".to_string(),
        SnapshotRuleConfig {
            accept: Some(r"1\.4".into()),
            reject: None,
        },
    )]);
    let rules = SnapshotRules::from_config(&config).unwrap();
    assert!(rules.compatible("order", "1.4", "x"));
    assert!(!rules.compatible("order", "1.4.2", "x"));
    assert!(!rules.compatible("order", "21.4", "x"));
}

#[test]
fn test_config_refusals_name_the_key() {
    let lookaround = SnapshotCompatibilityConfig::from([(
        "order".to_string(),
        SnapshotRuleConfig {
            accept: Some(r"1\.(?!3).*".into()),
            reject: None,
        },
    )]);
    let err = SnapshotRules::from_config(&lookaround).unwrap_err();
    assert!(err.contains("snapshot_compatibility.order.accept"), "{err}");

    let bad_reject = SnapshotCompatibilityConfig::from([(
        "order".to_string(),
        SnapshotRuleConfig {
            accept: Some(".*".into()),
            reject: Some(r"(a)\1".into()),
        },
    )]);
    let err = SnapshotRules::from_config(&bad_reject).unwrap_err();
    assert!(err.contains("snapshot_compatibility.order.reject"), "{err}");

    let missing =
        SnapshotCompatibilityConfig::from([("order".to_string(), SnapshotRuleConfig::default())]);
    let err = SnapshotRules::from_config(&missing).unwrap_err();
    assert!(err.contains("snapshot_compatibility.order.accept"), "{err}");
}

#[test]
fn test_offerable_needs_the_registered_state_type() {
    let rules = SnapshotRules::default();
    let reg = registration("1.4.2");
    assert!(rules.offerable("order", &snapshot("/orders.v1.OrderState", "1.4.0"), &reg));
    assert!(rules.offerable(
        "order",
        &snapshot("type.googleapis.com/orders.v1.OrderState", "1.4.0"),
        &reg
    ));
    assert!(!rules.offerable(
        "order",
        &snapshot("/orders.v1.LegacyOrderState", "1.4.2"),
        &reg
    ));
    let mut stateless = snapshot("/orders.v1.OrderState", "1.4.2");
    stateless.state = None;
    assert!(!rules.offerable("order", &stateless, &reg));
}
