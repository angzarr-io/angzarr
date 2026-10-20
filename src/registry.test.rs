//! Tests for [`ComponentRegistry`].
//!
//! Why this matters: one coordinator serves one component; snapshot
//! selection, version-skew refusal and conflict-resolution defaults read
//! the registration held here, which must be replaced when the component is
//! described again, gone when it is lost, and never versionless.

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
fn test_register_replace_and_forget() {
    let registry = ComponentRegistry::new();
    assert!(registry.get().is_none());
    registry.register(descriptor("1.4.2")).unwrap();
    assert_eq!(registry.get().unwrap().rules_version(), "1.4.2");
    registry.register(descriptor("1.4.3")).unwrap();
    assert_eq!(registry.get().unwrap().rules_version(), "1.4.3");
    registry.deregister();
    assert!(registry.get().is_none());
}

/// A descriptor without a rules version is never held, and does not
/// displace the one already held.
#[test]
fn test_versionless_descriptor_is_refused() {
    let registry = ComponentRegistry::new();
    registry.register(descriptor("1.4.2")).unwrap();
    let err = registry.register(descriptor("")).unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(registry.get().unwrap().rules_version(), "1.4.2");
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
