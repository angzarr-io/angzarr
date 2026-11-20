//! Every component an application runs: what each is, its [`Catalog`]
//! entry, and the handler behind it, which the kernel dispatches through.
//!
//! A registration lists the component in the catalog by its descriptor
//! (generated for a Rust component, a child's for a remote one) and keeps
//! its handler: a domain's angzarr-router `CommandHandler` and, for a
//! typed [`Domain`], its upcaster and factory; a policy's
//! `PolicyComponent`; a projector's `ProjectorHandler`; a remote domain's
//! [`CoordinatorPort`]. Every registration answers `Result<_,`
//! [`Misconfigured`]`>`: a refused one lists and keeps nothing.
//!
//! Built once at start-up and shared: it is `Send + Sync`.

use std::any::Any as AnyType;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use angzarr_proto::v1::{
    component_descriptor, AggregateDescriptor, ComponentDescriptor, ProcessManagerDescriptor,
};
use angzarr_router::domain::DomainDispatch;
use angzarr_router::policy::PolicyDispatch;
use angzarr_router::router::{
    self as router, DynCommandHandler, DynPolicyComponent, DynProjectorHandler,
};

use super::catalog::{domain_of, Catalog, Description};
use super::domain::{Domain, State};
use super::error::UpcastChainError;
use super::markup::{self, AsyncProjector};
use super::misconfigured::Misconfigured;
use super::payload::Payload;
use super::route;
use super::target::{CoordinatorPort, Target};
use super::upcast::{UpcastPort, Upcaster};

/// A typed [`Domain`]'s parts, erased.
struct Typed {
    upcaster: Arc<Upcaster>,
    /// The current event, command and fact type URLs: what an upcast chain
    /// must end at.
    current: fn() -> Vec<String>,
    /// Every fresh `D::State`: a `Factory<D::State>`, boxed.
    fresh: Box<dyn AnyType + Send + Sync>,
    /// Whether a snapshot's state, of the anchor's type, is one a fresh
    /// `D::State` restores ([`State::restore`]): a state that takes no
    /// snapshots restores none.
    restores: Box<dyn Fn(&prost_types::Any) -> bool + Send + Sync>,
}

/// A factory of `S`.
type Factory<S> = Arc<dyn Fn() -> S + Send + Sync>;

/// What dispatches a domain's inputs here.
struct Handler {
    /// angzarr-router's.
    dispatch: Box<dyn DynCommandHandler>,
    /// What only a typed domain has: its upcaster and factory.
    typed: Option<Typed>,
}

/// A registered projector: the projector port, angzarr-router's
/// `ProjectorHandler` (a marked-up one through [`markup::Projected`]),
/// with the domains whose committed events it is run for.
pub(crate) struct ProjectorEntry {
    pub(crate) name: String,
    pub(crate) sources: Vec<String>,
    pub(crate) handler: Box<dyn DynProjectorHandler>,
}

/// A registered policy, as angzarr-router dispatches it, held in the
/// registry's one list in registration order.
pub(crate) struct PolicyEntry {
    pub(crate) dispatch: Box<dyn DynPolicyComponent>,
    /// The one domain its commands may go to (`PolicyDescriptor.output_domain`):
    /// what the kernel addresses them to (decision 14.42).
    pub(crate) output_domain: String,
}

impl PolicyEntry {
    /// Its name: its commands' component and its position's handler.
    pub(crate) fn name(&self) -> &str {
        self.dispatch.name()
    }

    /// The domain whose events it handles.
    pub(crate) fn source(&self) -> &str {
        self.dispatch.input_domain()
    }
}

/// Every component an application runs: the [`Catalog`] of what each is,
/// and the handlers behind them.
#[derive(Default)]
pub struct Registry {
    catalog: Catalog,
    /// The domains handled here, by name.
    handlers: BTreeMap<String, Handler>,
    /// The domains whose streams a coordinator elsewhere owns, by name
    /// ([`Registry::remote`]): never one of `handlers`.
    remotes: BTreeMap<String, Box<dyn CoordinatorPort>>,
    /// The registered policies, in registration order: the one list.
    policies: Vec<PolicyEntry>,
    projectors: Vec<ProjectorEntry>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// What is registered, read-only: every component's descriptor and
    /// configuration.
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Register `D` under `D::NAME`, at its rules version
    /// (`D::RULES_VERSION`, stamped on every event), its upcaster (for
    /// events and, discouraged, commands), and its handlers: `handlers`
    /// registers them on the router's table for `D` (`DomainDispatch`,
    /// over `D::State`, already folding through `D`, and offering and
    /// restoring snapshots through `D::State`), with `on_command` and `on_fact`. Listed by the descriptor `D`
    /// generates (its state its anchor's, `D::State::Anchor`), and refused as a
    /// child's descriptor is.
    pub fn register<D: Domain>(
        &mut self,
        upcaster: Upcaster,
        handlers: impl FnOnce(DomainDispatch<D::State>) -> DomainDispatch<D::State>,
    ) -> Result<&mut Self, Misconfigured> {
        let fresh: Factory<D::State> = Arc::new(D::State::default);
        self.typed::<D>(upcaster, fresh, None, handlers)
    }

    /// Register typed `D`, built fresh by `fresh`, a process manager if it
    /// has `starting` types (its descriptor's), its handlers registered by
    /// `handlers`.
    fn typed<D: Domain>(
        &mut self,
        upcaster: Upcaster,
        fresh: Factory<D::State>,
        starting: Option<BTreeSet<String>>,
        handlers: impl FnOnce(DomainDispatch<D::State>) -> DomainDispatch<D::State>,
    ) -> Result<&mut Self, Misconfigured> {
        let domain = D::NAME.to_string();
        if upcaster.domain() != domain {
            return Err(Misconfigured::UpcasterForAnotherDomain {
                domain,
                upcaster: upcaster.domain().to_string(),
            });
        }
        let state = markup::state_descriptor::<<D::State as State>::Anchor>();
        let process_manager = starting.is_some();
        let kind = match starting {
            Some(starting) => {
                component_descriptor::Kind::ProcessManager(ProcessManagerDescriptor {
                    domain: domain.clone(),
                    state: Some(state),
                    starting_types: starting.into_iter().collect(),
                })
            }
            None => component_descriptor::Kind::Aggregate(AggregateDescriptor {
                domain: domain.clone(),
                state: Some(state),
            }),
        };
        let descriptor = ComponentDescriptor {
            name: domain.clone(),
            rules_version: D::RULES_VERSION.to_string(),
            kind: Some(kind),
        };
        let upcaster = Arc::new(upcaster);
        let port: Arc<dyn UpcastPort> = upcaster.clone();
        self.catalog.list_domain(descriptor, Some(port))?;
        let table = route::table::<D>(Arc::clone(&fresh), process_manager);
        let probe = Arc::clone(&fresh);
        let restores = move |any: &prost_types::Any| {
            use prost::Message;
            let anchor = <<D::State as State>::Anchor as prost::Name>::full_name();
            super::payload::type_name(&any.type_url) == anchor
                && <D::State as State>::Anchor::decode(&any.value[..])
                    .is_ok_and(|wire| probe().restore(wire).is_ok())
        };
        let handler = Handler {
            dispatch: Box::new(route::Offering(handlers(table))),
            typed: Some(Typed {
                upcaster,
                current: || {
                    let mut urls = D::Event::type_urls();
                    urls.extend(D::Command::type_urls());
                    urls.extend(D::Fact::type_urls());
                    urls
                },
                fresh: Box::new(fresh),
                restores: Box::new(restores),
            }),
        };
        self.handlers.insert(domain, handler);
        Ok(self)
    }

    /// Register `handler` as domain `domain`: any angzarr-router
    /// `CommandHandler` (a `DomainDispatch` built by hand, or a handler in
    /// another process behind one), described by `descriptor`, its
    /// generated `ComponentDescriptor` (decision 14.42: angzarr-build's
    /// `angzarr-descriptors.rs`, or the one a child answers `Describe`
    /// with). The kernel knows only what the descriptor says: its kind,
    /// its state's descriptor, its rules version, which its events are
    /// stamped with, and a process manager's starting types. Refused as
    /// [`validate_descriptor`](super::validate_descriptor) refuses it, and
    /// for a descriptor of another domain than `domain`. Its streams are dispatched like any
    /// domain's; with no typed [`Domain`] behind it, what a chain recorded
    /// of it is its pages alone ([`Recorded::events`](super::Recorded::events)
    /// decodes a typed domain's).
    pub fn command_handler(
        &mut self,
        domain: &str,
        handler: impl router::CommandHandler + 'static,
        descriptor: ComponentDescriptor,
    ) -> Result<&mut Self, Misconfigured> {
        self.described_handler(domain, handler, descriptor)?;
        Ok(self)
    }

    /// [`Registry::command_handler`], answering the domain's
    /// [`Description`]: what its coordinator replaces its descriptor
    /// through when the domain is described again (a child restarted,
    /// C-0563).
    pub fn described_handler(
        &mut self,
        domain: &str,
        handler: impl router::CommandHandler + 'static,
        descriptor: ComponentDescriptor,
    ) -> Result<Description, Misconfigured> {
        if domain_of(&descriptor).is_some_and(|named| named != domain) {
            return Err(Misconfigured::DescriptorNoDomain {
                component: descriptor.name.clone(),
                kind: super::misconfigured::kind_of(&descriptor)
                    .as_str_name()
                    .to_string(),
            });
        }
        let description = self.catalog.list_domain(descriptor, None)?;
        let handler = Handler {
            dispatch: Box::new(handler),
            typed: None,
        };
        self.handlers.insert(domain.to_string(), handler);
        Ok(description)
    }

    /// Upcast `domain`'s incoming payloads through `port` (decision
    /// 14.39): its upcaster sidecar, for a domain registered by
    /// [`Registry::command_handler`], or one in-process. Refused for a
    /// domain not registered ([`Misconfigured::UpcasterUnknownDomain`]).
    pub fn upcast_with(
        &mut self,
        domain: &str,
        port: impl UpcastPort + 'static,
    ) -> Result<&mut Self, Misconfigured> {
        self.catalog.upcast_with(domain, Arc::new(port))?;
        Ok(self)
    }

    /// Register the marked-up aggregate `D` (angzarr-embedded-macros' `#[aggregate]`)
    /// with `factory`, which builds every fresh instance: `D::default` for
    /// one with no configuration, or a closure over immutable reference
    /// configuration (`registry.aggregate::<Wallet>(move || Wallet::with(config.clone()))`).
    /// Replay starts from its output, and resuming from a snapshot builds
    /// from it, then [restores](super::State::restore), so the configuration
    /// survives. The factory is called once per stream and reads nothing
    /// that changes.
    ///
    /// Its handlers are the router's table its markup generates
    /// ([`Routed`](super::markup::Routed)), its rules version its crate's
    /// build version (`D::RULES_VERSION`, which the markup gives as
    /// `CARGO_PKG_VERSION`), and its upcaster an empty one
    /// ([`Registry::aggregate_upcast`] gives one). Refused as
    /// [`Registry::register`] refuses a domain.
    pub fn aggregate<D>(
        &mut self,
        factory: impl Fn() -> D + Send + Sync + 'static,
    ) -> Result<&mut Self, Misconfigured>
    where
        D: Domain<State = D> + markup::Routed,
    {
        self.aggregate_upcast::<D>(factory, Upcaster::new(D::NAME, D::NAME))
    }

    /// [`Registry::aggregate`], its old events read through `upcaster`
    /// (whose domain must be `D::NAME`).
    pub fn aggregate_upcast<D>(
        &mut self,
        factory: impl Fn() -> D + Send + Sync + 'static,
        upcaster: Upcaster,
    ) -> Result<&mut Self, Misconfigured>
    where
        D: Domain<State = D> + markup::Routed,
    {
        self.typed::<D>(upcaster, Arc::new(factory), None, D::route)
    }

    /// Register the marked-up process manager `P` (`#[process_manager]`)
    /// with `factory`, which builds every fresh instance. A process manager
    /// is a domain, registered as [`Registry::aggregate`] registers one
    /// (its crate's build version, an empty upcaster), on the router's
    /// process-manager table: only a starting input type may start an
    /// instance.
    pub fn process_manager<P>(
        &mut self,
        factory: impl Fn() -> P + Send + Sync + 'static,
    ) -> Result<&mut Self, Misconfigured>
    where
        P: super::markup::ProcessManager + markup::Routed,
    {
        // Its starting types are its markup's (the inputs marked `starts`),
        // as its generated descriptor lists them.
        let starting = P::STARTS.iter().map(|t| t.full_name()).collect();
        self.typed::<P>(
            Upcaster::new(P::NAME, P::NAME),
            Arc::new(factory),
            Some(starting),
            P::route,
        )
    }

    /// Register `policy`, written by hand or marked up (`#[policy]`), as the
    /// router's table for it ([`PolicyDispatch`]): ONE list, in
    /// registration order, which is the order the kernel runs every policy
    /// of a source in, for each of its events. Refused for a name
    /// registered already ([`Misconfigured::ComponentRegistered`]) and for
    /// a remote domain's events ([`Misconfigured::RemoteSource`]).
    pub fn policy<P: super::Policy>(&mut self, policy: P) -> Result<&mut Self, Misconfigured> {
        let dispatch = super::policy::dispatch(policy);
        let output_domain = dispatch.output_domain().to_string();
        self.push_policy(Box::new(dispatch), output_domain)
    }

    /// Register a policy built as angzarr-router's dispatch table, written
    /// by hand: in the ONE list, beside every other policy, its source its
    /// input domain and its target its output domain. Its refused commands
    /// are answered by its Translate. Refused as [`Registry::policy`] is.
    pub fn policy_dispatch(
        &mut self,
        dispatch: PolicyDispatch,
    ) -> Result<&mut Self, Misconfigured> {
        let output_domain = dispatch.output_domain().to_string();
        self.push_policy(Box::new(dispatch), output_domain)
    }

    /// Register a policy written as angzarr-router's `PolicyComponent`: a
    /// policy in another process, behind its `PolicyService` (the host's
    /// remote policy), or any other. In the ONE list, beside every other
    /// policy, its source its input domain and its commands held to
    /// `output_domain` (its descriptor's); its refused commands are
    /// answered by its Translate. Refused as [`Registry::policy`] is.
    pub fn policy_component(
        &mut self,
        policy: impl router::PolicyComponent + 'static,
        output_domain: &str,
    ) -> Result<&mut Self, Misconfigured> {
        self.push_policy(Box::new(policy), output_domain.to_string())
    }

    fn push_policy(
        &mut self,
        dispatch: Box<dyn DynPolicyComponent>,
        output_domain: String,
    ) -> Result<&mut Self, Misconfigured> {
        self.catalog
            .list_policy(dispatch.name(), dispatch.input_domain(), &output_domain)?;
        self.policies.push(PolicyEntry {
            dispatch,
            output_domain,
        });
        Ok(self)
    }

    /// The policies of `source`'s events, in registration order.
    pub(crate) fn policies<'r>(
        &'r self,
        source: &'r str,
    ) -> impl Iterator<Item = &'r PolicyEntry> + 'r {
        self.policies
            .iter()
            .filter(move |entry| entry.source() == source)
    }

    /// Every registered policy, in registration order.
    pub(crate) fn all_policies(&self) -> impl Iterator<Item = &PolicyEntry> {
        self.policies.iter()
    }

    /// `domain`'s handler: what the kernel dispatches its inputs through.
    pub(crate) fn handler(&self, domain: &str) -> Option<&dyn DynCommandHandler> {
        self.handlers.get(domain).map(|h| &*h.dispatch)
    }

    /// Register `projector`, the ONE instance of a marked-up projector
    /// (`#[projector]`) this process runs, holding its ports, on the
    /// projector port ([`markup::Projected`]), for its sources
    /// ([`AsyncProjector::SOURCES`]): as [`Registry::projector_handler`]
    /// registers any projector.
    pub fn projector<P: AsyncProjector>(
        &mut self,
        projector: P,
    ) -> Result<&mut Self, Misconfigured> {
        self.projector_handler(P::SOURCES, markup::Projected::new(projector))
    }

    /// Register `projector`, on the projector port, angzarr-router's
    /// `ProjectorHandler` (one in another process, behind its
    /// `ProjectorService`: the host's remote projector; one written by
    /// hand; a marked-up one, [`Registry::projector`]), projecting the
    /// events of every domain of `sources`, at least one. The kernel runs
    /// it inline: after each commit that records events of any of them, it
    /// hands it each committed book of them, a stream's book at a time, and
    /// awaits it before the call returns. A failure does not stop the other
    /// projectors, and is the caller's error
    /// ([`Failure::Projection`](super::Failure::Projection); the events
    /// stay committed), each by its class (decision 14.37, 14.41): a
    /// retryable one and an ABORTED one are run again under the kernel's
    /// [`OnRetryable`](super::OnRetryable), then dead-lettered, and any
    /// other (a bug) is dead-lettered at once. To run one caught up from
    /// the store instead, see [`CatchUp`](super::CatchUp). Refused for no
    /// source ([`Misconfigured::NoInputDomain`]), a name
    /// registered already and a remote source.
    pub fn projector_handler(
        &mut self,
        sources: &[&str],
        projector: impl router::ProjectorHandler + 'static,
    ) -> Result<&mut Self, Misconfigured> {
        let name = projector.name().to_string();
        self.catalog.list_projector(&name, sources)?;
        self.projectors.push(ProjectorEntry {
            name,
            sources: sources.iter().map(|source| source.to_string()).collect(),
            handler: Box::new(projector),
        });
        Ok(self)
    }

    /// A fresh `D` state: the registered factory's, else the default.
    /// Whether a load of a `domain` stream may start from `snapshot`
    /// (docs/design/snapshots.md section 3): the catalog's rules (its
    /// state type and rules version, [`Catalog::snapshot_usable`]), and,
    /// for a typed domain, its state restores it ([`State::restore`]), so
    /// one a state that takes no snapshots, or one it refuses, is not
    /// offered: the stream is replayed from its first event. A domain
    /// handled elsewhere is held to the catalog's rules alone.
    pub(crate) fn snapshot_usable(
        &self,
        domain: &str,
        snapshot: &angzarr_proto::v1::Snapshot,
    ) -> bool {
        let restores = || match self.handlers.get(domain).and_then(|h| h.typed.as_ref()) {
            Some(typed) => snapshot
                .state
                .as_ref()
                .is_some_and(|any| (typed.restores)(any)),
            None => true,
        };
        self.catalog.snapshot_usable(domain, snapshot) && restores()
    }

    pub(crate) fn fresh<D: Domain>(&self) -> D::State {
        self.handlers
            .get(D::NAME)
            .and_then(|handler| handler.typed.as_ref())
            .and_then(|typed| typed.fresh.downcast_ref::<Factory<D::State>>())
            .map_or_else(D::State::default, |factory| factory())
    }

    /// Every projector, in registration order.
    pub(crate) fn projectors(&self) -> &[ProjectorEntry] {
        &self.projectors
    }

    /// `domain`'s upcaster, if it is a typed domain's.
    pub fn upcaster(&self, domain: &str) -> Option<&Upcaster> {
        self.handlers
            .get(domain)?
            .typed
            .as_ref()
            .map(|t| &*t.upcaster)
    }

    /// Check every typed domain's upcaster against the types it decodes
    /// ([`Upcaster::check`]). For a test that runs in CI beside the golden
    /// replay fixtures ([`assert_replay_fixtures`](super::assert_replay_fixtures)).
    pub fn check(&self) -> Result<(), Vec<UpcastChainError>> {
        let problems: Vec<UpcastChainError> = self
            .handlers
            .values()
            .filter_map(|handler| handler.typed.as_ref())
            .filter_map(|typed| typed.upcaster.check(&(typed.current)()).err())
            .flatten()
            .collect();
        if problems.is_empty() {
            Ok(())
        } else {
            Err(problems)
        }
    }

    /// Route `domain` to `coordinator`, a coordinator elsewhere that owns
    /// its streams ([`super::target`]): every command naming it, the
    /// application's and every policy step's, is handed to the port, and
    /// nothing of it is loaded or written here. Refused for a domain
    /// registered already, here or remote, and for one whose events a
    /// policy or projector here handles ([`Misconfigured::RemoteSource`]):
    /// a remote domain stays remote, its events never here.
    pub fn remote(
        &mut self,
        domain: &str,
        coordinator: impl CoordinatorPort + 'static,
    ) -> Result<&mut Self, Misconfigured> {
        self.catalog.list_remote(domain)?;
        self.remotes
            .insert(domain.to_string(), Box::new(coordinator));
        Ok(self)
    }

    /// Where a command naming `domain` is handled: here, through its
    /// handler; remote, through its coordinator; `None` when it is
    /// neither (the caller's [`Malformed::UnknownDomain`](super::Malformed::UnknownDomain)).
    pub fn target(&self, domain: &str) -> Option<Target<'_>> {
        match self.handlers.get(domain) {
            Some(handler) => Some(Target::Local(&*handler.dispatch)),
            None => self.remotes.get(domain).map(|port| Target::Remote(&**port)),
        }
    }
}

#[cfg(test)]
#[path = "registry.test.rs"]
mod tests;
