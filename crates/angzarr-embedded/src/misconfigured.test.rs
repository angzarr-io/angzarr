//! The one validator every component meets (decision 14.65): each of the
//! descriptor rules a `kind` oneof leaves to check, under its spec key.

use angzarr_diagnostic::Diagnostic;
use angzarr_proto::v1::component_descriptor::Kind;
use angzarr_proto::v1::{
    AggregateDescriptor, ComponentDescriptor, ComponentKind, PolicyDescriptor,
    ProcessManagerDescriptor, ProjectorDescriptor, StateDescriptor,
};

use super::{kind_of, validate_descriptor};

fn state() -> Option<StateDescriptor> {
    Some(StateDescriptor {
        type_name: "toy.State".to_string(),
    })
}

fn named(kind: Kind) -> ComponentDescriptor {
    ComponentDescriptor {
        name: "order".to_string(),
        rules_version: "1.0.0".to_string(),
        kind: Some(kind),
    }
}

fn aggregate() -> AggregateDescriptor {
    AggregateDescriptor {
        domain: "order".to_string(),
        state: state(),
    }
}

fn process_manager() -> ProcessManagerDescriptor {
    ProcessManagerDescriptor {
        domain: "order".to_string(),
        state: state(),
        starting_types: vec!["toy.Start".to_string()],
    }
}

fn policy() -> PolicyDescriptor {
    PolicyDescriptor {
        input_domain: "cart".to_string(),
        output_domain: "inventory".to_string(),
    }
}

fn projector() -> ProjectorDescriptor {
    ProjectorDescriptor {
        input_domains: vec!["cart".to_string()],
    }
}

fn refused(descriptor: ComponentDescriptor) -> String {
    validate_descriptor(&descriptor)
        .expect_err("refused")
        .code()
        .to_string()
}

#[test]
fn a_well_formed_descriptor_of_each_kind_passes() {
    for kind in [
        Kind::Aggregate(aggregate()),
        Kind::ProcessManager(process_manager()),
        Kind::Policy(policy()),
        Kind::Projector(projector()),
    ] {
        assert_eq!(
            validate_descriptor(&named(kind.clone())),
            Ok(()),
            "{kind:?}"
        );
    }
}

#[test]
fn the_kind_is_the_oneofs_arm() {
    for (kind, tag) in [
        (Kind::Aggregate(aggregate()), ComponentKind::Aggregate),
        (
            Kind::ProcessManager(process_manager()),
            ComponentKind::ProcessManager,
        ),
        (Kind::Policy(policy()), ComponentKind::Policy),
        (Kind::Projector(projector()), ComponentKind::Projector),
    ] {
        assert_eq!(kind_of(&named(kind)), tag);
    }
    let none = ComponentDescriptor {
        kind: None,
        ..named(Kind::Aggregate(aggregate()))
    };
    assert_eq!(kind_of(&none), ComponentKind::Unspecified);
}

#[test]
fn each_rule_refuses_under_its_key() {
    for (descriptor, key) in [
        (
            ComponentDescriptor {
                rules_version: String::new(),
                ..named(Kind::Aggregate(aggregate()))
            },
            "ANGZARR_REASON_MISSING_RULES_VERSION",
        ),
        (
            ComponentDescriptor {
                kind: None,
                ..named(Kind::Aggregate(aggregate()))
            },
            "ANGZARR_REASON_DESCRIPTOR_NO_KIND",
        ),
        (
            named(Kind::Aggregate(AggregateDescriptor {
                domain: String::new(),
                ..aggregate()
            })),
            "ANGZARR_REASON_DESCRIPTOR_NO_DOMAIN",
        ),
        (
            named(Kind::ProcessManager(ProcessManagerDescriptor {
                domain: String::new(),
                ..process_manager()
            })),
            "ANGZARR_REASON_DESCRIPTOR_NO_DOMAIN",
        ),
        (
            named(Kind::Aggregate(AggregateDescriptor {
                state: None,
                ..aggregate()
            })),
            "ANGZARR_REASON_MISSING_STATE_TYPE",
        ),
        (
            named(Kind::ProcessManager(ProcessManagerDescriptor {
                state: Some(StateDescriptor::default()),
                ..process_manager()
            })),
            "ANGZARR_REASON_MISSING_STATE_TYPE",
        ),
        (
            named(Kind::Policy(PolicyDescriptor {
                input_domain: String::new(),
                ..policy()
            })),
            "ANGZARR_REASON_NO_INPUT_DOMAIN",
        ),
        (
            named(Kind::Policy(PolicyDescriptor {
                output_domain: String::new(),
                ..policy()
            })),
            "ANGZARR_REASON_POLICY_NO_OUTPUT_DOMAIN",
        ),
        (
            named(Kind::Projector(ProjectorDescriptor {
                input_domains: Vec::new(),
            })),
            "ANGZARR_REASON_NO_INPUT_DOMAIN",
        ),
    ] {
        assert_eq!(refused(descriptor.clone()), key, "{descriptor:?}");
    }
}

#[test]
fn a_refusal_names_the_component_and_its_kind() {
    let refusal = validate_descriptor(&named(Kind::Projector(ProjectorDescriptor {
        input_domains: Vec::new(),
    })))
    .unwrap_err();
    assert_eq!(
        refusal,
        super::Misconfigured::NoInputDomain {
            component: "order".to_string(),
            kind: "COMPONENT_KIND_PROJECTOR".to_string(),
        }
    );
}
