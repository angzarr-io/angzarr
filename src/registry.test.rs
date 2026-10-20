//! Tests for [`ComponentRegistry`].
//!
//! Why this matters: snapshot selection and version-skew refusal read the
//! registrations held here; an instance's registration must be found under
//! the domain it serves, replaced when the instance re-registers, and gone
//! when it deregisters, or queries would vet snapshots against a build that
//! no longer runs.

use super::*;
use crate::proto::{ComponentKind, StateOptions};

fn descriptor(version: &str) -> ComponentDescriptor {
    ComponentDescriptor {
        name: "order".into(),
        kind: ComponentKind::Aggregate as i32,
        inputs: vec![],
        rules_version: version.into(),
        state: Some(StateDescriptor {
            type_url: "/orders.v1.OrderState".into(),
            options: Some(StateOptions {
                conflict_resolutions: vec![
                    ConflictResolution::Strict as i32,
                    ConflictResolution::AggregateHandles as i32,
                ],
            }),
        }),
    }
}

#[test]
fn test_register_get_and_all() {
    let registry = ComponentRegistry::new();
    registry
        .register("order", "a", descriptor("1.4.2"))
        .unwrap();
    registry
        .register("order", "b", descriptor("1.5.0"))
        .unwrap();
    assert_eq!(registry.get("order", "a").unwrap().rules_version(), "1.4.2");
    let versions: Vec<_> = registry
        .all("order")
        .iter()
        .map(|r| r.rules_version().to_string())
        .collect();
    assert_eq!(versions, ["1.4.2", "1.5.0"]);
    assert!(registry.all("inventory").is_empty());
}

#[test]
fn test_reregistration_replaces_and_deregistration_forgets() {
    let registry = ComponentRegistry::new();
    registry
        .register("order", "a", descriptor("1.4.2"))
        .unwrap();
    registry
        .register("order", "a", descriptor("1.4.3"))
        .unwrap();
    assert_eq!(registry.all("order").len(), 1);
    assert_eq!(registry.get("order", "a").unwrap().rules_version(), "1.4.3");
    registry.deregister("order", "a");
    assert!(registry.get("order", "a").is_none());
    assert!(registry.all("order").is_empty());
}

#[test]
fn test_supports_reads_the_state_options() {
    let registration = Registration::new(descriptor("1.4.2")).unwrap();
    assert!(registration.supports(ConflictResolution::Strict));
    assert!(!registration.supports(ConflictResolution::Commutative));
    assert_eq!(registration.name(), "order");
    assert_eq!(
        registration.state().unwrap().type_url,
        "/orders.v1.OrderState"
    );
}

/// A descriptor without a rules version is never held.
#[test]
fn test_versionless_descriptor_is_refused() {
    let registry = ComponentRegistry::new();
    let err = registry.register("order", "a", descriptor("")).unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(registry.get("order", "a").is_none());
}
