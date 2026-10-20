//! Tests for conflict-resolution configuration (backlog HD-2).
//!
//! Why this matters (types.proto, Handling): the resolution a stale command
//! is treated under decides whether it is ABORTED or delivered with its
//! window; the precedence (Handling where allowed, command type, component,
//! built-in default) and the support check against the registered state
//! must hold exactly, or a command is treated under a resolution its state
//! cannot honour.

use super::*;
use crate::test_utils::aggregate_descriptor;

fn registration(commutative: bool) -> Registration {
    Registration::new(aggregate_descriptor(
        "order",
        "/orders.v1.OrderState",
        "1.0.0",
        commutative,
    ))
    .unwrap()
}

fn yaml(text: &str) -> ConflictConfig {
    serde_yaml::from_str(text).unwrap()
}

const ADD: &str = "/orders.v1.AddItem";

#[test]
fn test_unconfigured_uses_the_built_in_default() {
    let policy = ConflictPolicy::from_config(&ConflictConfig::default()).unwrap();
    use ConflictResolution::*;
    assert_eq!(
        policy.resolve(Unspecified, ADD, &registration(true)),
        Commutative
    );
    assert_eq!(
        policy.resolve(Unspecified, ADD, &registration(false)),
        Strict
    );
    assert!(policy.allows_override(ADD));
}

#[test]
fn test_precedence_handling_then_type_then_component() {
    let policy = ConflictPolicy::from_config(&yaml(
        "default: AGGREGATE_HANDLES\ncommands:\n  - type: orders.v1.AddItem\n    resolution: STRICT\n",
    ))
    .unwrap();
    let reg = registration(true);
    use ConflictResolution::*;
    assert_eq!(policy.resolve(Commutative, ADD, &reg), Commutative);
    assert_eq!(policy.resolve(Unspecified, ADD, &reg), Strict);
    assert_eq!(
        policy.resolve(Unspecified, "type.googleapis.com/orders.v1.AddItem", &reg),
        Strict,
        "type URLs compare by full name"
    );
    assert_eq!(
        policy.resolve(Unspecified, "/orders.v1.Close", &reg),
        AggregateHandles
    );
}

#[test]
fn test_allow_override_per_component_and_per_type() {
    let policy = ConflictPolicy::from_config(&yaml(
        "allow_override: false\ncommands:\n  - type: orders.v1.AddItem\n    allow_override: true\n",
    ))
    .unwrap();
    assert!(policy.allows_override(ADD));
    assert!(!policy.allows_override("/orders.v1.Close"));
    let open = ConflictPolicy::from_config(&yaml(
        "commands:\n  - type: orders.v1.AddItem\n    allow_override: false\n",
    ))
    .unwrap();
    assert!(!open.allows_override(ADD));
    assert!(open.allows_override("/orders.v1.Close"));
}

#[test]
fn test_config_load_refusals_name_the_key() {
    let err = ConflictPolicy::from_config(&yaml("default: MANUAL\n")).unwrap_err();
    assert!(err.contains("conflict_resolution.default"), "{err}");
    let err = ConflictPolicy::from_config(&yaml(
        "commands:\n  - type: orders.v1.AddItem\n    resolution: strict\n",
    ))
    .unwrap_err();
    assert!(
        err.contains("conflict_resolution.commands[0].resolution"),
        "{err}"
    );
    let err = ConflictPolicy::from_config(&yaml(
        "commands:\n  - type: orders.v1.AddItem\n  - type: /orders.v1.AddItem\n",
    ))
    .unwrap_err();
    assert!(err.contains("listed twice"), "{err}");
    let err = ConflictPolicy::from_config(&yaml("commands:\n  - type: \"\"\n")).unwrap_err();
    assert!(
        err.contains("conflict_resolution.commands[0].type"),
        "{err}"
    );
}

#[test]
fn test_configured_resolutions_are_checked_against_the_registered_state() {
    let policy = ConflictPolicy::from_config(&yaml(
        "default: STRICT\ncommands:\n  - type: orders.v1.AddItem\n    resolution: COMMUTATIVE\n",
    ))
    .unwrap();
    let err = policy
        .check_supported(&aggregate_descriptor(
            "order",
            "/orders.v1.OrderState",
            "1.0.0",
            false,
        ))
        .unwrap_err();
    assert!(
        err.contains("COMMUTATIVE") && err.contains("orders.v1.AddItem"),
        "{err}"
    );
    policy
        .check_supported(&aggregate_descriptor(
            "order",
            "/orders.v1.OrderState",
            "1.0.0",
            true,
        ))
        .unwrap();
}
