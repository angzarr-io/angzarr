//! What is registered, by name: angzarr-embedded's mirror of the
//! `ComponentDescriptor`s angzarr's coordinator holds, and of the
//! configuration it reads. Read-only to everything but the [`Registry`]
//! that fills it ([`Registry::catalog`]).
//!
//! The pipeline's domain-blind steps ([`check`](super::check), the
//! coordinator's) look a domain up here by `cover.domain` and see only
//! what angzarr's coordinator sees: the state's `StateDescriptor` (its
//! type's name), the rules version, a process manager's starting types,
//! and what its payloads are upcast through. A policy and a projector are
//! listed by their descriptors too: their names and the domains they read.
//! Every entry is made from a `ComponentDescriptor`, generated for a Rust
//! component (from its markup or its `Domain`) or answered by a child to
//! `Describe`, and is held to the same rules either way; the handlers
//! behind them are the [`Registry`]'s.
//!
//! [`Registry`]: super::Registry
//! [`Registry::catalog`]: super::Registry::catalog

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use angzarr_proto::v1::{
    component_descriptor, ComponentDescriptor, ComponentKind, PolicyDescriptor,
    ProjectorDescriptor, Snapshot, StateDescriptor,
};

use super::misconfigured::{kind_of, validate_descriptor, Misconfigured};
use super::payload::type_name;
use super::upcast::UpcastPort;

/// What a domain's descriptor tells the kernel: replaced whole, under
/// its lock, when the domain is described again ([`Description::replace`]).
struct Described {
    /// The state's descriptor.
    state: StateDescriptor,
    /// The registered rules version.
    rules_version: String,
    /// A process manager's starting input types, by full name, as its
    /// descriptor lists them (`ComponentDescriptor.starting_types`): the
    /// kernel's start rule. `None` for an aggregate.
    starting: Option<BTreeSet<String>>,
}

/// One domain handled here.
struct Listed {
    /// What its descriptor says, shared with its [`Description`].
    described: Arc<RwLock<Described>>,
    /// What its incoming payloads are upcast through (decision 14.39): a
    /// typed domain's [`Upcaster`](super::Upcaster), or the port
    /// [`Registry::upcast_with`](super::Registry::upcast_with) gave.
    /// `None`: nothing is upcast.
    upcast: Option<Arc<dyn UpcastPort>>,
}

impl Listed {
    /// What its descriptor says, read under its lock: never held across an
    /// await.
    fn described(&self) -> RwLockReadGuard<'_, Described> {
        self.described
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// A domain listed by its descriptor: what its coordinator holds to
/// describe it again. A business process that reconnects is described
/// again (component.proto, Registration; C-0563), and
/// [`Description::replace`] lists its new descriptor whole, at once,
/// before anything more is delivered to it.
#[derive(Clone)]
pub struct Description {
    domain: String,
    kind: ComponentKind,
    described: Arc<RwLock<Described>>,
}

impl fmt::Debug for Description {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Description")
            .field("domain", &self.domain)
            .finish_non_exhaustive()
    }
}

impl Description {
    /// List `descriptor` as the domain's in place of the one it was
    /// registered by: its state, its rules version and a process manager's
    /// starting types, all at once. Refused, the old
    /// one kept, as registering a domain refuses a descriptor.
    pub fn replace(&self, descriptor: ComponentDescriptor) -> Result<(), Misconfigured> {
        let described = described(&self.domain, self.kind, descriptor)?;
        *self
            .described
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = described;
        Ok(())
    }
}

/// What `descriptor` says of `domain`, a handler of `kind`, refused as
/// [`validate_descriptor`] refuses it, and when it describes another kind
/// or another domain than the one it lists again.
fn described(
    domain: &str,
    kind: ComponentKind,
    descriptor: ComponentDescriptor,
) -> Result<Described, Misconfigured> {
    validate_descriptor(&descriptor)?;
    use component_descriptor::Kind;
    let (named, state, starting) = match descriptor.kind {
        Some(Kind::Aggregate(a)) if kind == ComponentKind::Aggregate => (a.domain, a.state, None),
        Some(Kind::ProcessManager(pm)) if kind == ComponentKind::ProcessManager => (
            pm.domain,
            pm.state,
            // A listed type URL counts by the name it ends in.
            Some(
                (pm.starting_types.iter())
                    .map(|t| type_name(t).to_string())
                    .collect(),
            ),
        ),
        _ => {
            return Err(Misconfigured::DescriptorNoKind {
                component: descriptor.name,
            })
        }
    };
    if named != domain {
        return Err(Misconfigured::DescriptorNoDomain {
            component: descriptor.name,
            kind: kind.as_str_name().to_string(),
        });
    }
    Ok(Described {
        state: state.unwrap_or_default(),
        rules_version: descriptor.rules_version,
        starting,
    })
}

/// The domain a domain's descriptor (an aggregate's or a process
/// manager's) names; `None` for another kind.
pub(crate) fn domain_of(descriptor: &ComponentDescriptor) -> Option<&str> {
    use component_descriptor::Kind;
    match &descriptor.kind {
        Some(Kind::Aggregate(a)) => Some(&a.domain),
        Some(Kind::ProcessManager(pm)) => Some(&pm.domain),
        _ => None,
    }
}

/// types.proto's default snapshot compatibility rule: strip ONE leading
/// "v" or "V" from each version; if both then parse as SemVer 2.0.0, their
/// major and minor numbers are equal (patch, pre-release and build
/// metadata ignored); otherwise the versions, as recorded, are equal.
pub(crate) fn compatible(snapshot: &str, registered: &str) -> bool {
    let stripped = |v: &str| v.strip_prefix(['v', 'V']).unwrap_or(v).to_string();
    match (
        major_minor(&stripped(snapshot)),
        major_minor(&stripped(registered)),
    ) {
        (Some(a), Some(b)) => a == b,
        _ => snapshot == registered,
    }
}

/// `version`'s major and minor numbers, if it is a SemVer 2.0.0 version:
/// `MAJOR.MINOR.PATCH`, each a number with no leading zero, then an
/// optional `-` pre-release and an optional `+` build, each of
/// dot-separated non-empty `[0-9A-Za-z-]` identifiers (a numeric
/// pre-release identifier with no leading zero).
fn major_minor(version: &str) -> Option<(u64, u64)> {
    let (rest, build) = match version.split_once('+') {
        Some((rest, build)) => (rest, Some(build)),
        None => (version, None),
    };
    let (core, pre) = match rest.split_once('-') {
        Some((core, pre)) => (core, Some(pre)),
        None => (rest, None),
    };
    let identifier =
        |id: &str| !id.is_empty() && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-');
    let numeric = |n: &str| {
        !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) && (n == "0" || !n.starts_with('0'))
    };
    if let Some(build) = build {
        if !build.split('.').all(identifier) {
            return None;
        }
    }
    if let Some(pre) = pre {
        let valid =
            |id: &str| identifier(id) && (!id.bytes().all(|b| b.is_ascii_digit()) || numeric(id));
        if !pre.split('.').all(valid) {
            return None;
        }
    }
    let mut parts = core.split('.');
    let (major, minor, patch) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() || ![major, minor, patch].into_iter().all(numeric) {
        return None;
    }
    Some((major.parse().ok()?, minor.parse().ok()?))
}

/// Every component an application runs, by name, as its descriptor
/// describes it: its domains (handled here, or by a coordinator elsewhere),
/// its policies and its projectors.
#[derive(Default)]
pub struct Catalog {
    domains: BTreeMap<String, Listed>,
    /// The domains whose streams a coordinator elsewhere owns
    /// ([`Registry::remote`](super::Registry::remote)): never one of
    /// `domains`.
    remotes: BTreeSet<String>,
    /// The policies' descriptors, in registration order.
    policies: Vec<ComponentDescriptor>,
    /// The projectors' descriptors, in registration order.
    projectors: Vec<ComponentDescriptor>,
}

impl Catalog {
    /// List the domain `descriptor` names by its descriptor, its payloads
    /// upcast through `upcast`. Refused when the domain is taken (here or
    /// remote), and as [`validate_descriptor`] refuses the descriptor;
    /// refused, nothing is listed.
    pub(crate) fn list_domain(
        &mut self,
        descriptor: ComponentDescriptor,
        upcast: Option<Arc<dyn UpcastPort>>,
    ) -> Result<Description, Misconfigured> {
        validate_descriptor(&descriptor)?;
        let kind = kind_of(&descriptor);
        let domain = domain_of(&descriptor)
            .ok_or_else(|| Misconfigured::DescriptorNoKind {
                component: descriptor.name.clone(),
            })?
            .to_string();
        self.free(&domain)?;
        let described = Arc::new(RwLock::new(described(&domain, kind, descriptor)?));
        self.domains.insert(
            domain.clone(),
            Listed {
                described: Arc::clone(&described),
                upcast,
            },
        );
        Ok(Description {
            domain,
            kind,
            described,
        })
    }

    /// Upcast `domain`'s incoming payloads through `port`; refused for a
    /// domain not listed here.
    pub(crate) fn upcast_with(
        &mut self,
        domain: &str,
        port: Arc<dyn UpcastPort>,
    ) -> Result<(), Misconfigured> {
        let listed =
            self.domains
                .get_mut(domain)
                .ok_or_else(|| Misconfigured::UpcasterUnknownDomain {
                    domain: domain.to_string(),
                })?;
        listed.upcast = Some(port);
        Ok(())
    }

    /// List `domain` as a coordinator elsewhere's. Refused when the name is
    /// taken, and when a policy or projector here reads its events.
    pub(crate) fn list_remote(&mut self, domain: &str) -> Result<(), Misconfigured> {
        self.free(domain)?;
        let reader = self
            .policies
            .iter()
            .chain(&self.projectors)
            .find(|reader| reads(reader, domain));
        if let Some(reader) = reader {
            return Err(Misconfigured::RemoteSource {
                domain: domain.to_string(),
                component: reader.name.clone(),
            });
        }
        self.remotes.insert(domain.to_string());
        Ok(())
    }

    /// List policy `name`, of `source`'s events, for `output`. Refused for
    /// a name listed already and for a remote source.
    pub(crate) fn list_policy(
        &mut self,
        name: &str,
        source: &str,
        output: &str,
    ) -> Result<(), Misconfigured> {
        self.reader(name, &[source], &self.policies)?;
        self.policies.push(ComponentDescriptor {
            name: name.to_string(),
            kind: Some(component_descriptor::Kind::Policy(PolicyDescriptor {
                input_domain: source.to_string(),
                output_domain: output.to_string(),
            })),
            ..ComponentDescriptor::default()
        });
        Ok(())
    }

    /// List projector `name`, of the events of `sources`, at least one.
    /// Refused for no source, a name listed already and a remote source.
    pub(crate) fn list_projector(
        &mut self,
        name: &str,
        sources: &[&str],
    ) -> Result<(), Misconfigured> {
        if sources.is_empty() {
            return Err(Misconfigured::NoInputDomain {
                component: name.to_string(),
                kind: ComponentKind::Projector.as_str_name().to_string(),
            });
        }
        self.reader(name, sources, &self.projectors)?;
        self.projectors.push(ComponentDescriptor {
            name: name.to_string(),
            kind: Some(component_descriptor::Kind::Projector(ProjectorDescriptor {
                input_domains: sources.iter().map(|s| s.to_string()).collect(),
            })),
            ..ComponentDescriptor::default()
        });
        Ok(())
    }

    /// Refused when `listed` holds a reader (a policy, a projector) named
    /// `name`, or one of `sources` is remote.
    fn reader(
        &self,
        name: &str,
        sources: &[&str],
        listed: &[ComponentDescriptor],
    ) -> Result<(), Misconfigured> {
        if listed.iter().any(|reader| reader.name == name) {
            return Err(Misconfigured::ComponentRegistered {
                component: name.to_string(),
            });
        }
        if let Some(remote) = sources.iter().find(|s| self.remotes.contains(**s)) {
            return Err(Misconfigured::RemoteSource {
                domain: remote.to_string(),
                component: name.to_string(),
            });
        }
        Ok(())
    }

    /// Refused when `domain` is handled somewhere already: here or remote.
    fn free(&self, domain: &str) -> Result<(), Misconfigured> {
        match self.is_here(domain) || self.is_remote(domain) {
            true => Err(Misconfigured::AlreadyRegistered {
                domain: domain.to_string(),
            }),
            false => Ok(()),
        }
    }

    /// Whether `domain` is handled here.
    pub(crate) fn is_here(&self, domain: &str) -> bool {
        self.domains.contains_key(domain)
    }

    /// Whether `domain` is a coordinator elsewhere's.
    pub(crate) fn is_remote(&self, domain: &str) -> bool {
        self.remotes.contains(domain)
    }

    /// Whether `domain` is a process manager's.
    pub(crate) fn is_process_manager(&self, domain: &str) -> bool {
        self.kind(domain) == ComponentKind::ProcessManager
    }

    /// `domain`'s kind: a process manager's or an aggregate's, as
    /// listed here; `UNSPECIFIED` for a domain handled elsewhere (its
    /// descriptor is its coordinator's) or not registered.
    pub fn kind(&self, domain: &str) -> ComponentKind {
        match self.domains.get(domain) {
            Some(listed) if listed.described().starting.is_some() => ComponentKind::ProcessManager,
            Some(_) => ComponentKind::Aggregate,
            None => ComponentKind::Unspecified,
        }
    }

    /// `domain`'s state descriptor, if registered: what [`check`](super::check)
    /// reads capabilities from.
    pub fn state(&self, domain: &str) -> Option<StateDescriptor> {
        self.domains
            .get(domain)
            .map(|listed| listed.described().state.clone())
    }

    /// `domain`'s registered rules version: what its events are stamped
    /// with.
    pub fn rules_version(&self, domain: &str) -> Option<String> {
        self.domains
            .get(domain)
            .map(|listed| listed.described().rules_version.clone())
    }

    /// `domain`'s starting input types, by full name, if it is a process
    /// manager's: what the kernel's start rule admits with no instance.
    pub(crate) fn starting_types(&self, domain: &str) -> Option<BTreeSet<String>> {
        self.domains.get(domain)?.described().starting.clone()
    }

    /// What `domain`'s incoming payloads are upcast through, if anything.
    pub(crate) fn upcast_port(&self, domain: &str) -> Option<&dyn UpcastPort> {
        self.domains.get(domain)?.upcast.as_deref()
    }

    /// Whether a load may start from `snapshot` of a `domain` stream
    /// (docs/design/snapshots.md §3): its state's type, by the name its
    /// type URL ends in, is the registered state's (decision 14.101), and its rules version passes the default
    /// compatibility rule against the registered one ([`compatible`]). A
    /// snapshot that fails is not offered: the stream is replayed from its
    /// events, never migrated.
    pub fn snapshot_usable(&self, domain: &str, snapshot: &Snapshot) -> bool {
        let Some(listed) = self.domains.get(domain) else {
            return false;
        };
        let described = listed.described();
        compatible(&snapshot.rules_version, &described.rules_version)
            && snapshot
                .state
                .as_ref()
                .is_some_and(|s| type_name(&s.type_url) == described.state.type_name)
    }

    /// The registered policies' names, in registration order: what a
    /// host holds its config's in-process policies to.
    pub fn policy_names(&self) -> impl Iterator<Item = &str> {
        self.policies.iter().map(|policy| policy.name.as_str())
    }

    /// The registered projectors' names, in registration order: what a
    /// host holds its config's in-process projectors to.
    pub fn projector_names(&self) -> impl Iterator<Item = &str> {
        self.projectors
            .iter()
            .map(|projector| projector.name.as_str())
    }

    /// The domains handled here, in name order.
    pub fn domains(&self) -> impl Iterator<Item = &str> {
        self.domains.keys().map(String::as_str)
    }

    /// The domains a coordinator elsewhere handles, in name order.
    pub fn remotes(&self) -> impl Iterator<Item = &str> {
        self.remotes.iter().map(String::as_str)
    }

    /// `domain`'s name as a component of an application, what a host's
    /// `component` entry lists it by (decision 14.50's default
    /// `component_name`): an aggregate's is its domain, a process
    /// manager's `pmg-` and its domain. `None` for a domain not handled
    /// here.
    pub fn component_name(&self, domain: &str) -> Option<String> {
        match self.kind(domain) {
            ComponentKind::ProcessManager => Some(format!("pmg-{domain}")),
            ComponentKind::Unspecified => None,
            _ => Some(domain.to_string()),
        }
    }
}

/// Whether reader `descriptor` (a policy's or a projector's) reads
/// `domain`'s events.
fn reads(descriptor: &ComponentDescriptor, domain: &str) -> bool {
    use component_descriptor::Kind;
    match &descriptor.kind {
        Some(Kind::Policy(policy)) => policy.input_domain == domain,
        Some(Kind::Projector(projector)) => projector.input_domains.iter().any(|d| d == domain),
        _ => false,
    }
}

#[cfg(test)]
#[path = "catalog.test.rs"]
mod tests;
