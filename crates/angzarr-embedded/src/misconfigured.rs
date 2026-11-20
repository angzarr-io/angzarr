//! Why a component could not be registered, and the one validator that
//! decides it (decision 14.65, the owner's ruling on decision 21 of the
//! 2026-10-31 run): a domain, policy or projector registered in Rust and a
//! child's descriptor are held to the same rules, each refusal under a spec
//! key, verbatim, with that key's args (decision 14.60).

use std::fmt;

use angzarr_diagnostic::{Arg, Diagnostic};
use angzarr_proto::v1::reason_args as args;
use angzarr_proto::v1::{component_descriptor, AngzarrReason, ComponentDescriptor, ComponentKind};

/// Why a component could not be registered: angzarr refuses the same
/// configuration when it is loaded or the component registers. Every
/// registration answers one of these; none panics.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Misconfigured {
    /// `domain` is registered already: `ANGZARR_REASON_DUPLICATE_COMPONENT`,
    /// naming it as the one registered and the one refused.
    AlreadyRegistered { domain: String },
    /// A policy or a projector named `component` is registered already:
    /// `ANGZARR_REASON_DUPLICATE_COMPONENT`, as a domain's is.
    ComponentRegistered { component: String },
    /// An upcaster is given for `domain`, which is not registered:
    /// `ANGZARR_REASON_UPCASTER_UNKNOWN_DOMAIN`.
    UpcasterUnknownDomain { domain: String },
    /// The upcaster names another domain:
    /// `ANGZARR_REASON_UPCASTER_OTHER_DOMAIN`.
    UpcasterForAnotherDomain { domain: String, upcaster: String },
    /// An empty rules version: every event is stamped with it, so it must
    /// say something. `ANGZARR_REASON_MISSING_RULES_VERSION`.
    MissingRulesVersion { component: String },
    /// A descriptor that says no kind (its `kind` oneof unset):
    /// `ANGZARR_REASON_DESCRIPTOR_NO_KIND`.
    DescriptorNoKind { component: String },
    /// An aggregate or process manager naming no domain:
    /// `ANGZARR_REASON_DESCRIPTOR_NO_DOMAIN`.
    DescriptorNoDomain { component: String, kind: String },
    /// An aggregate or process manager with no state (or a state with no
    /// type): `ANGZARR_REASON_MISSING_STATE_TYPE`.
    MissingStateType { component: String, kind: String },
    /// A policy or a projector that names no domain it reads: there is no
    /// wildcard domain (decision 14.53). `ANGZARR_REASON_NO_INPUT_DOMAIN`.
    NoInputDomain { component: String, kind: String },
    /// A policy with no output domain:
    /// `ANGZARR_REASON_POLICY_NO_OUTPUT_DOMAIN`.
    PolicyNoOutputDomain { component: String },
    /// A policy or projector here, `component`, handles the events of
    /// `domain`, a remote domain ([`Registry::remote`](super::Registry::remote)):
    /// a remote domain's events are its coordinator's, never written or
    /// read here, so a local component of them would never run. Remote
    /// stays remote. `ANGZARR_REASON_REMOTE_SOURCE`.
    RemoteSource { domain: String, component: String },
    /// A policy subscribed on a bus with its positions kept in memory
    /// (`Subscription::ephemeral`): a policy's position must move with its
    /// outputs in the store's one write, which a position in memory cannot.
    /// `angzarr-embedded-ephemeral-policy`.
    EphemeralPolicy { component: String },
}

impl fmt::Display for Misconfigured {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for Misconfigured {}

impl Diagnostic for Misconfigured {
    fn code(&self) -> &str {
        use Misconfigured as M;
        let key = match self {
            M::AlreadyRegistered { .. } | M::ComponentRegistered { .. } => {
                AngzarrReason::DuplicateComponent
            }
            M::UpcasterUnknownDomain { .. } => AngzarrReason::UpcasterUnknownDomain,
            M::UpcasterForAnotherDomain { .. } => AngzarrReason::UpcasterOtherDomain,
            M::MissingRulesVersion { .. } => AngzarrReason::MissingRulesVersion,
            M::DescriptorNoKind { .. } => AngzarrReason::DescriptorNoKind,
            M::DescriptorNoDomain { .. } => AngzarrReason::DescriptorNoDomain,
            M::MissingStateType { .. } => AngzarrReason::MissingStateType,
            M::NoInputDomain { .. } => AngzarrReason::NoInputDomain,
            M::PolicyNoOutputDomain { .. } => AngzarrReason::PolicyNoOutputDomain,
            M::RemoteSource { .. } => AngzarrReason::RemoteSource,
            M::EphemeralPolicy { .. } => return "angzarr-embedded-ephemeral-policy",
        };
        key.as_str_name()
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        use Misconfigured as M;
        let text = |name, value: &str| (name, Arg::from(value));
        match self {
            M::AlreadyRegistered { domain: name } | M::ComponentRegistered { component: name } => {
                vec![text(args::REGISTERED, name), text(args::REFUSED, name)]
            }
            M::UpcasterUnknownDomain { domain } => vec![text(args::DOMAIN, domain)],
            M::UpcasterForAnotherDomain { domain, upcaster } => {
                vec![text(args::DOMAIN, domain), text(args::UPCASTER, upcaster)]
            }
            M::MissingRulesVersion { component }
            | M::DescriptorNoKind { component }
            | M::PolicyNoOutputDomain { component } => vec![text(args::COMPONENT, component)],
            M::DescriptorNoDomain { component, kind }
            | M::MissingStateType { component, kind }
            | M::NoInputDomain { component, kind } => vec![
                text(args::COMPONENT, component),
                text(args::COMPONENT_KIND, kind),
            ],
            M::RemoteSource { domain, component } => {
                vec![text(args::COMPONENT, component), text(args::DOMAIN, domain)]
            }
            M::EphemeralPolicy { component } => vec![text(args::COMPONENT, component)],
        }
    }
}

/// The kind a descriptor's `kind` oneof says, as the bare tag
/// (`ComponentKind`); `UNSPECIFIED` for none.
pub fn kind_of(descriptor: &ComponentDescriptor) -> ComponentKind {
    use component_descriptor::Kind;
    match &descriptor.kind {
        Some(Kind::Aggregate(_)) => ComponentKind::Aggregate,
        Some(Kind::ProcessManager(_)) => ComponentKind::ProcessManager,
        Some(Kind::Policy(_)) => ComponentKind::Policy,
        Some(Kind::Projector(_)) => ComponentKind::Projector,
        None => ComponentKind::Unspecified,
    }
}

/// `descriptor`, held to the coordinator's rules
/// (docs/design/component-descriptors.md §4), in order: a rules version; a
/// kind; an aggregate's or a process manager's domain and state type; a
/// policy's input and output domains; a projector's input domains. The one
/// check every component meets: a Rust registration's descriptor as a
/// child's (decision 14.65).
pub fn validate_descriptor(descriptor: &ComponentDescriptor) -> Result<(), Misconfigured> {
    use component_descriptor::Kind;
    let component = || descriptor.name.clone();
    let kind_name = || kind_of(descriptor).as_str_name().to_string();
    if descriptor.rules_version.is_empty() {
        return Err(Misconfigured::MissingRulesVersion {
            component: component(),
        });
    }
    let (domain, state) = match &descriptor.kind {
        None => {
            return Err(Misconfigured::DescriptorNoKind {
                component: component(),
            })
        }
        Some(Kind::Aggregate(a)) => (&a.domain, a.state.as_ref()),
        Some(Kind::ProcessManager(pm)) => (&pm.domain, pm.state.as_ref()),
        Some(Kind::Policy(policy)) => {
            if policy.input_domain.is_empty() {
                return Err(Misconfigured::NoInputDomain {
                    component: component(),
                    kind: kind_name(),
                });
            }
            if policy.output_domain.is_empty() {
                return Err(Misconfigured::PolicyNoOutputDomain {
                    component: component(),
                });
            }
            return Ok(());
        }
        Some(Kind::Projector(projector)) => {
            if projector.input_domains.is_empty() {
                return Err(Misconfigured::NoInputDomain {
                    component: component(),
                    kind: kind_name(),
                });
            }
            return Ok(());
        }
    };
    if domain.is_empty() {
        return Err(Misconfigured::DescriptorNoDomain {
            component: component(),
            kind: kind_name(),
        });
    }
    if state.is_none_or(|s| s.type_name.is_empty()) {
        return Err(Misconfigured::MissingStateType {
            component: component(),
            kind: kind_name(),
        });
    }
    Ok(())
}

#[cfg(test)]
#[path = "misconfigured.test.rs"]
mod tests;
