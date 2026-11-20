//! The registry: angzarr-embedded's mirror of each component's
//! descriptor (its kind, its state's type, its rules version), looked up
//! by `cover.domain`. These run.

use crate::v1::Snapshot;
use crate::{pack, Misconfigured, Registry, Upcaster};

use crate::toys::{account, registry, shelf, Account};

#[test]
fn registration_fills_the_state_descriptor_from_the_generated_impl() {
    let registry = registry();
    let shelf = registry.catalog().state("shelf").unwrap();
    assert_eq!(shelf.type_name, "toy.shelf.v1.ShelfState");
    assert_eq!(registry.catalog().state("nowhere"), None);
    assert_eq!(
        registry.catalog().rules_version("account").as_deref(),
        Some("v2")
    );
    assert_eq!(
        registry.catalog().domains().collect::<Vec<_>>(),
        ["account", "ledger", "shelf"]
    );
}

#[test]
fn misconfiguration_is_refused_at_registration() {
    let mut registry = registry();
    assert_eq!(
        registry
            .register::<Account>(
                Upcaster::new("u", "account"),
                crate::toys::handlers::<crate::toys::Account>()
            )
            .err(),
        Some(Misconfigured::AlreadyRegistered {
            domain: "account".to_string()
        })
    );

    let mut fresh = Registry::new();
    assert_eq!(
        fresh
            .register::<Account>(
                Upcaster::new("u", "ledger"),
                crate::toys::handlers::<crate::toys::Account>()
            )
            .err(),
        Some(Misconfigured::UpcasterForAnotherDomain {
            domain: "account".to_string(),
            upcaster: "ledger".to_string()
        })
    );
    /// An account built without a version.
    struct Unversioned;
    impl crate::Domain for Unversioned {
        type State = account::AccountState;
        type Command = std::convert::Infallible;
        type Fact = std::convert::Infallible;
        type Event = crate::toys::AccountEvent;
        type Rejection = std::convert::Infallible;
        const RULES_VERSION: &'static str = "";
        const NAME: &'static str = "unversioned";
        fn apply(_: &mut account::AccountState, _: &Self::Event, _: &crate::PageContext<'_>) {}
    }
    assert_eq!(
        fresh
            .register::<Unversioned>(Upcaster::new("u", "unversioned"), |t| t)
            .err(),
        Some(Misconfigured::MissingRulesVersion {
            component: "unversioned".to_string()
        })
    );
    assert!(
        fresh.catalog().domains().next().is_none(),
        "nothing half-registered"
    );
}

#[test]
fn a_snapshot_is_usable_only_under_a_compatible_version_and_the_state_type() {
    let registry = registry();
    let state = pack(&account::AccountState::default());
    let snapshot = |rules: &str, state: &crate::Any| Snapshot {
        sequence: 3,
        state: Some(state.clone()),
        rules_version: rules.to_string(),
    };
    assert!(registry
        .catalog()
        .snapshot_usable("account", &snapshot("v2", &state)));
    assert!(
        !registry
            .catalog()
            .snapshot_usable("account", &snapshot("v1", &state)),
        "another build's, not SemVer: not offered"
    );
    assert!(
        !registry.catalog().snapshot_usable(
            "account",
            &snapshot("v2", &pack(&shelf::ShelfState::default()))
        ),
        "another state type: discarded"
    );
    assert!(!registry
        .catalog()
        .snapshot_usable("nowhere", &snapshot("v2", &state)));
}

/// A hand-written handler, registered by its descriptor: its kind is the
/// descriptor's.
struct Hello;

impl angzarr_router::router::CommandHandler for Hello {
    fn name(&self) -> &str {
        "hello"
    }

    fn command_types(&self) -> Vec<String> {
        Vec::new()
    }

    async fn dispatch(
        &self,
        _: &angzarr_router::hpb::HandleRequest,
    ) -> Result<angzarr_router::hpb::HandleResponse, angzarr_router::error::CodedError> {
        Ok(Default::default())
    }
}

/// `hello`'s descriptor, an aggregate of `state`.
fn hello(state: Option<crate::v1::StateDescriptor>) -> angzarr_proto::v1::ComponentDescriptor {
    named("hello", aggregate_of("hello", state))
}

/// A descriptor named `name` of `kind`, under rules version h1.
fn named(
    name: &str,
    kind: angzarr_proto::v1::component_descriptor::Kind,
) -> angzarr_proto::v1::ComponentDescriptor {
    angzarr_proto::v1::ComponentDescriptor {
        name: name.to_string(),
        rules_version: "h1".to_string(),
        kind: Some(kind),
    }
}

/// An aggregate of `domain` holding `state`.
fn aggregate_of(
    domain: &str,
    state: Option<crate::v1::StateDescriptor>,
) -> angzarr_proto::v1::component_descriptor::Kind {
    angzarr_proto::v1::component_descriptor::Kind::Aggregate(
        angzarr_proto::v1::AggregateDescriptor {
            domain: domain.to_string(),
            state,
        },
    )
}

/// A process manager of `domain` holding `state`, started by `starting`.
fn process_manager_of(
    domain: &str,
    state: Option<crate::v1::StateDescriptor>,
    starting: &[&str],
) -> angzarr_proto::v1::component_descriptor::Kind {
    angzarr_proto::v1::component_descriptor::Kind::ProcessManager(
        angzarr_proto::v1::ProcessManagerDescriptor {
            domain: domain.to_string(),
            state,
            starting_types: starting.iter().map(|t| t.to_string()).collect(),
        },
    )
}

fn base_state() -> crate::v1::StateDescriptor {
    crate::v1::StateDescriptor {
        type_name: "toy.hello.v1.State".to_string(),
    }
}

/// A handler registers on what its descriptor says: refused without a
/// rules version or a state type, or for a descriptor of another domain
/// than the one it is registered as; a process manager's kind kept.
#[test]
fn a_command_handler_registers_by_its_descriptor() {
    use angzarr_proto::v1::ComponentKind;
    let refused = |descriptor| {
        crate::Registry::new()
            .command_handler("hello", Hello, descriptor)
            .err()
    };
    let unversioned = angzarr_proto::v1::ComponentDescriptor {
        rules_version: String::new(),
        ..hello(Some(base_state()))
    };
    assert_eq!(
        refused(unversioned),
        Some(Misconfigured::MissingRulesVersion {
            component: "hello".to_string()
        })
    );
    assert_eq!(
        refused(hello(Some(crate::v1::StateDescriptor::default()))),
        Some(Misconfigured::MissingStateType {
            component: "hello".to_string(),
            kind: "COMPONENT_KIND_AGGREGATE".to_string(),
        })
    );
    assert_eq!(
        refused(hello(None)),
        Some(Misconfigured::MissingStateType {
            component: "hello".to_string(),
            kind: "COMPONENT_KIND_AGGREGATE".to_string(),
        })
    );
    assert_eq!(
        refused(named(
            "hello",
            aggregate_of("elsewhere", Some(base_state()))
        )),
        Some(Misconfigured::DescriptorNoDomain {
            component: "hello".to_string(),
            kind: "COMPONENT_KIND_AGGREGATE".to_string(),
        }),
        "a descriptor of another domain"
    );

    let mut registry = crate::Registry::new();
    registry
        .command_handler(
            "hello",
            Hello,
            named(
                "hello",
                process_manager_of("hello", Some(base_state()), &[]),
            ),
        )
        .unwrap();
    assert!(registry.catalog().is_process_manager("hello"));
    assert_eq!(
        registry.catalog().kind("hello"),
        ComponentKind::ProcessManager
    );
    assert_eq!(
        registry.catalog().kind("nowhere"),
        ComponentKind::Unspecified
    );
    assert_eq!(
        registry.catalog().rules_version("hello").as_deref(),
        Some("h1")
    );
    let mut aggregate = crate::Registry::new();
    aggregate
        .command_handler("hello", Hello, hello(Some(base_state())))
        .unwrap();
    assert!(!aggregate.catalog().is_process_manager("hello"));
    assert_eq!(aggregate.catalog().kind("hello"), ComponentKind::Aggregate);
}

/// C-0563: a domain registered by its descriptor is described again
/// through its [`Description`](crate::Description): the new
/// descriptor replaces the old whole (its rules version, its state, a
/// process manager's starting types), and one the registry refuses
/// leaves the old in place.
#[test]
fn a_described_domain_is_replaced_whole_by_its_new_descriptor() {
    let mut registry = crate::Registry::new();
    let description = registry
        .described_handler(
            "hello",
            Hello,
            named(
                "hello",
                process_manager_of("hello", Some(base_state()), &["toy.hello.v1.Start"]),
            ),
        )
        .unwrap();
    assert_eq!(
        format!("{description:?}"),
        "Description { domain: \"hello\", .. }"
    );
    let next = crate::v1::StateDescriptor {
        type_name: "toy.hello.v2.State".to_string(),
    };
    description
        .replace(angzarr_proto::v1::ComponentDescriptor {
            rules_version: "h2".to_string(),
            ..named(
                "hello",
                process_manager_of("hello", Some(next.clone()), &["toy.hello.v1.Begin"]),
            )
        })
        .unwrap();
    assert_eq!(
        registry.catalog().rules_version("hello").as_deref(),
        Some("h2")
    );
    assert_eq!(registry.catalog().state("hello"), Some(next));
    assert_eq!(
        registry.catalog().starting_types("hello"),
        Some(["toy.hello.v1.Begin".to_string()].into())
    );

    let refused = description.replace(angzarr_proto::v1::ComponentDescriptor {
        rules_version: String::new(),
        ..named(
            "hello",
            process_manager_of("hello", Some(base_state()), &[]),
        )
    });
    assert_eq!(
        refused,
        Err(Misconfigured::MissingRulesVersion {
            component: "hello".to_string()
        })
    );
    assert!(
        description.replace(hello(Some(base_state()))).is_err(),
        "another kind is no description of it again"
    );
    assert_eq!(
        registry.catalog().rules_version("hello").as_deref(),
        Some("h2"),
        "the old kept"
    );
    assert!(registry
        .described_handler("hello", Hello, hello(Some(base_state())))
        .is_err());
}

/// A domain's name as a component of an application is decision 14.50's
/// default: an aggregate's is its domain, a process manager's `pmg-` and
/// its domain; a domain not registered here has none.
#[test]
fn a_domain_is_named_as_a_component_by_its_kind() {
    let names = |kind| {
        let mut registry = crate::Registry::new();
        registry
            .command_handler("hello", Hello, named("hello", kind))
            .unwrap();
        (
            registry.catalog().component_name("hello"),
            registry.catalog().component_name("nowhere"),
        )
    };
    assert_eq!(
        names(aggregate_of("hello", Some(base_state()))),
        (Some("hello".to_string()), None)
    );
    assert_eq!(
        names(process_manager_of("hello", Some(base_state()), &[])),
        (Some("pmg-hello".to_string()), None)
    );
}

/// A projector, as the router's `ProjectorHandler`, named `name`, that
/// projects nothing.
struct Named(&'static str);

impl angzarr_router::router::ProjectorHandler for Named {
    fn name(&self) -> &str {
        self.0
    }

    async fn dispatch(
        &self,
        _: &angzarr_router::hpb::ProjectRequest,
    ) -> Result<angzarr_router::hpb::ProjectResponse, angzarr_router::error::CodedError> {
        Ok(angzarr_router::hpb::ProjectResponse::default())
    }
}

/// The registry names its policies and its projectors, each in
/// registration order.
#[test]
fn the_registry_names_its_policies_and_projectors_in_order() {
    use angzarr_router::policy::PolicyDispatch;
    let mut registry = Registry::new();
    assert_eq!(registry.catalog().policy_names().count(), 0);
    assert_eq!(registry.catalog().projector_names().count(), 0);
    registry
        .policy_dispatch(PolicyDispatch::new("b-policy", "x", "y"))
        .unwrap()
        .policy_dispatch(PolicyDispatch::new("a-policy", "x", "y"))
        .unwrap()
        .projector_handler(&["x"], Named("b-view"))
        .unwrap()
        .projector_handler(&["x", "y"], Named("a-view"))
        .unwrap();
    assert_eq!(
        registry.catalog().policy_names().collect::<Vec<_>>(),
        ["b-policy", "a-policy"]
    );
    assert_eq!(
        registry.catalog().projector_names().collect::<Vec<_>>(),
        ["b-view", "a-view"]
    );
}

/// types.proto's Snapshot region, the default rule: a snapshot is offered
/// when its version, one leading "v" or "V" stripped, has the registered
/// version's SemVer major and minor (C-0522, C-0523); a version that is
/// not SemVer, so stripped, only under exact equality (C-0524).
#[test]
fn a_snapshot_is_usable_under_the_registered_major_and_minor() {
    let mut registry = crate::Registry::new();
    for (domain, version) in [("hello", "1.4.2+build.7"), ("label", "release-7")] {
        registry
            .command_handler(
                domain,
                Hello,
                angzarr_proto::v1::ComponentDescriptor {
                    rules_version: version.to_string(),
                    ..named(domain, aggregate_of(domain, Some(base_state())))
                },
            )
            .unwrap();
    }
    let snapshot = |rules: &str| Snapshot {
        sequence: 3,
        state: Some(crate::Any {
            type_url: "/toy.hello.v1.State".to_string(),
            value: Vec::new(),
        }),
        rules_version: rules.to_string(),
    };
    for usable in [
        "1.4.2+build.7",
        "1.4.0",
        "v1.4.9",
        "V1.4.2",
        "1.4.3-rc.1",
        "1.4.3-rc-1.x-y",
        "1.4.3+build-7.a-b",
        "1.4.3-0",
    ] {
        assert!(
            registry
                .catalog()
                .snapshot_usable("hello", &snapshot(usable)),
            "{usable}"
        );
    }
    for unusable in [
        "1.5.0",
        "2.4.2",
        "0.4.2",
        "vv1.4.2",
        "1.4",
        "",
        "1.04.2",
        "x1.4.2",
        "1.4.2.0",
        // Not SemVer, so equal only to themselves: an empty or invalid
        // build or pre-release identifier, a numeric one with a leading 0.
        "1.4.2+",
        "1.4.2+build..7",
        "1.4.2+bu_ild",
        "1.4.2-",
        "1.4.2-rc_1",
        "1.4.2-01",
    ] {
        assert!(
            !registry
                .catalog()
                .snapshot_usable("hello", &snapshot(unusable)),
            "{unusable}"
        );
    }
    assert!(registry
        .catalog()
        .snapshot_usable("label", &snapshot("release-7")));
    assert!(!registry
        .catalog()
        .snapshot_usable("label", &snapshot("release-8")));
    assert!(!registry
        .catalog()
        .snapshot_usable("label", &snapshot("vrelease-7")));
}
