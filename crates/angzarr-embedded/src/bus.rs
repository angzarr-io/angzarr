//! The bus: angzarr core's generic bus ([`angzarr_bus::Hub`]) hosted on
//! the kernel, its handlers reached through the kernel, over the store's
//! own transport ([`angzarr_bus::StoreTransport`]) or any other
//! ([`Bus::over`]). The trait is the interface, the distributed
//! coordinator's as well (angzarr-project's `docs/design/bus-interface.md`);
//! angzarr core's conformance suite holds this bus to it
//! (`bus_contract.test.rs`).
//!
//! What the generic bus does (positions as a mark and a window, every
//! delivery settled after its answer, dedup by the window, order and
//! references from the store, credit, idleness) is angzarr core's. What
//! this module adds is the kernel:
//!
//! - **A projector's item** is upcast (a failed upcast is held), handed as
//!   one event (a fork page is passed over, its sequence counted), and its
//!   failure classed as an inline projector's is: a retryable one retried
//!   to the kernel's cap, then dead-lettered and passed; any other
//!   dead-lettered at once (`Kernel::project_served`). A gone stream is
//!   handed its gone book.
//! - **A policy's item is a step through the outbox**
//!   (`Kernel::react_served`): the kernel reacts, routes and stamps the
//!   outputs (their ids `ids::policy_command_id`'s), writes them to the
//!   outbox with the policy's position past the event in one write, then
//!   relays them (each a step in its target, then cleared), and settles
//!   what they wrote (the targets' inline policies and the inline
//!   projectors). A crash after the write leaves the entries pending:
//!   they are relayed when the policy is next served, and the policy is
//!   not run again for that event. Its failure is the kernel's
//!   (dead-lettered, the position past it under
//!   [`OnPolicyBug::MovePast`](super::OnPolicyBug), or held).
//! - **A policy on the bus** ([`BusPolicy`]) is one the registry does not
//!   hold, never both: no chain runs it, and
//!   [`Kernel::resume`](super::Kernel::resume) and
//!   [`Kernel::forget_gone`](super::Kernel::forget_gone) never touch it. Its
//!   position is an inline policy's (the next sequence, under its name), so
//!   a policy moved between the two keeps its place.

use std::future::Future;
use std::time::Duration;

use angzarr_bus::{
    Admitted, Answer, Handler, Hub, Item, Pumped, RetryPolicy, Scope, StoreRead, StoreTransport,
    Transport,
};
use angzarr_ports::BoxFuture;
use angzarr_proto::v1::Cover;
use angzarr_router::policy::PolicyDispatch;
use angzarr_router::router::{DynProjectorHandler, PolicyComponent};
use coded_diagnostic::Report;
use prost_types::Timestamp;

use super::error::{Failure, KernelError};
use super::handle::{Kernel, Wait};
use super::misconfigured::Misconfigured;
use super::projector::{gone_request, project_request};
use super::registry::PolicyEntry;
use super::route::Rejected;
use super::store::{
    CommitNotify, Outbox, PositionKey, Processed, ReadStore, Store, StoreError, StreamRef, Watch,
};

/// A policy a bus serves, never the registry: written by hand or marked up
/// ([`BusPolicy::new`]), as angzarr-router's dispatch table
/// ([`BusPolicy::dispatch`]) or its `PolicyComponent`
/// ([`BusPolicy::component`]), as [`Registry::policy`](super::Registry::policy)
/// and its twins take one.
pub struct BusPolicy {
    entry: PolicyEntry,
}

impl BusPolicy {
    /// `policy`, as the router's table for it.
    pub fn new<P: super::Policy>(policy: P) -> BusPolicy {
        BusPolicy::dispatch(super::policy::dispatch(policy))
    }

    /// A policy built as angzarr-router's dispatch table: its source its
    /// input domain and its target its output domain.
    pub fn dispatch(dispatch: PolicyDispatch) -> BusPolicy {
        let output_domain = dispatch.output_domain().to_string();
        BusPolicy {
            entry: PolicyEntry {
                dispatch: Box::new(dispatch),
                output_domain,
            },
        }
    }

    /// A policy written as angzarr-router's `PolicyComponent`, its outputs
    /// held to `output_domain`.
    pub fn component(policy: impl PolicyComponent + 'static, output_domain: &str) -> BusPolicy {
        BusPolicy {
            entry: PolicyEntry {
                dispatch: Box::new(policy),
                output_domain: output_domain.to_string(),
            },
        }
    }

    /// Its name: its outputs' component, and its position's handler.
    pub fn name(&self) -> &str {
        self.entry.name()
    }

    /// The domain whose events it handles.
    pub fn source(&self) -> &str {
        self.entry.source()
    }
}

/// A served policy's step's failure, as a report.
fn step_report(failure: Failure<std::convert::Infallible>) -> Report {
    let failure: Failure<Rejected> = match failure {
        Failure::Rejected(never) => match never {},
        Failure::Downstream {
            path,
            error,
            dead_letters,
        } => Failure::Downstream {
            path,
            error,
            dead_letters,
        },
        Failure::Kernel(error) => Failure::Kernel(error),
        Failure::Projection {
            committed,
            failures,
            dead_letters,
        } => Failure::Projection {
            committed,
            failures,
            dead_letters,
        },
    };
    Report::of(&failure)
}

/// What a bus is served on: the store, the kernel its handlers' failures
/// and its policies' steps go through, the clock they are stamped by, the
/// timer, and the push. See [`Bus::new`].
pub struct Host<'h, S> {
    store: &'h S,
    kernel: &'h Kernel<'h>,
    clock: &'h Clock,
    wait: &'h dyn Wait,
    every: Duration,
    push: Option<&'h dyn CommitNotify>,
    following: angzarr_bus::Following<'h>,
    retry: Option<RetryPolicy>,
}

/// The instant a served policy's step is recorded at, and a served
/// handler's dead letters stamped with, as the application samples it.
pub type Clock = dyn Fn() -> Timestamp + Send + Sync;

impl<'h, S> Host<'h, S> {
    /// Served from `store`, through `kernel` (whose registry's catalog
    /// upcasts, and whose retry and bug settings hold), at `clock`'s instant,
    /// polling the store's change token every `every` on `wait` (the
    /// caller's timer).
    pub fn new(
        store: &'h S,
        kernel: &'h Kernel<'h>,
        clock: &'h Clock,
        wait: &'h dyn Wait,
        every: Duration,
        following: angzarr_bus::Following<'h>,
    ) -> Host<'h, S> {
        Host {
            store,
            kernel,
            clock,
            wait,
            every,
            push: None,
            following,
            retry: None,
        }
    }

    /// Woken by every commit `notify` pushes as well as by the poll, which
    /// still sees what the push does not (another process's commits).
    pub fn pushed_by(mut self, notify: &'h dyn CommitNotify) -> Host<'h, S> {
        self.push = Some(notify);
        self
    }

    /// Held deliveries retried as `retry` says, not as the kernel retries
    /// a step within it ([`Bus::over`]).
    pub fn retrying(mut self, retry: RetryPolicy) -> Host<'h, S> {
        self.retry = Some(retry);
        self
    }
}

/// The kernel's [`Wait`] as the store transport's timer.
impl angzarr_bus::Timer for dyn Wait + '_ {
    fn after(&self, after: Duration) -> BoxFuture<'_, ()> {
        self.wait(after)
    }
}

/// The store's own transport, polled on the host's timer, pumped: a pull
/// feed woken by the store's commits.
pub type StoreFed<'h, S> = Pumped<StoreTransport<'h, S, dyn Wait + 'h>>;

/// How the generic bus reaches the kernel's handlers.
pub struct KernelDispatch<'h, S> {
    store: &'h S,
    kernel: &'h Kernel<'h>,
    clock: &'h Clock,
}

/// Subscriptions served from one store on a kernel, each by its own
/// instances: angzarr core's [`angzarr_bus::Bus`], serving projector
/// subscriptions (a `DynProjectorHandler`, boxed) and policy subscriptions
/// ([`BusPolicy`]), delivered by `T` (the store's own transport unless
/// [`Bus::over`] says). See the module.
pub struct Bus<'h, S: Store + Outbox, T = StoreFed<'h, S>> {
    hub: Hub<'h, T, S, KernelDispatch<'h, S>, StoreRead<'h, S>>,
}

impl<'h, S: Store + Outbox + ReadStore + Processed + Watch> Bus<'h, S> {
    /// A bus with no subscription, served on `host` over the store's own
    /// transport: woken by `host`'s push, polled on its timer.
    pub fn new(host: Host<'h, S>) -> Bus<'h, S> {
        let mut transport = StoreTransport::new(host.store, host.wait, host.every, host.following);
        if let Some(push) = host.push {
            transport = transport.pushed_by(push);
        }
        Bus::over(Pumped::new(transport), host)
    }
}

impl<'h, S: Store + Outbox, T> Bus<'h, S, T> {
    /// A bus with no subscription, served on `host` (its timer and push
    /// unused), its deliveries made by `transport`. Its held deliveries are
    /// retried as the host says ([`Host::retrying`]), else as the kernel
    /// retries ([`Kernel::retryable`]): its retries the cap (none: never
    /// given up), its backoff each delay, its timer's draw the jitter.
    pub fn over(transport: T, host: Host<'h, S>) -> Bus<'h, S, T> {
        let dispatch = KernelDispatch {
            store: host.store,
            kernel: host.kernel,
            clock: host.clock,
        };
        let retry = host.kernel.retryable();
        let policy = host.retry.unwrap_or(RetryPolicy {
            cap: (retry.max_retries > 0).then_some(retry.max_retries),
            initial: retry.initial_backoff,
            multiplier: retry.multiplier,
            max: retry.max_backoff,
            jitter: retry.jitter,
        });
        let wait = host.wait;
        let reads = StoreRead::new(host.store);
        let hub = Hub::reading(transport, host.store, dispatch, reads);
        Bus {
            hub: hub.retrying(policy, move || wait.draw()),
        }
    }
}

impl<S, T> angzarr_bus::Bus for Bus<'_, S, T>
where
    S: Store + Outbox,
    T: Transport<Error = StoreError>,
{
    type Projector = Box<dyn DynProjectorHandler>;
    type Policy = BusPolicy;
    type Refused = Misconfigured;
    type Error = KernelError;

    /// Refused for a name another subscription on the bus has
    /// ([`Misconfigured::ComponentRegistered`]: the name keys its
    /// positions), and for a remote source ([`Misconfigured::RemoteSource`]:
    /// its events are its coordinator's); a policy also for a name the
    /// kernel's registry runs inline (never both). A policy whose positions
    /// are kept in memory is refused too
    /// ([`Misconfigured::EphemeralPolicy`]): its position moves with its
    /// outputs in the store's one write. A projector may keep its positions
    /// in memory (`Subscription::ephemeral`): it is handed every stream from
    /// 0 each serve, nothing is stored under its name, and it must be
    /// idempotent.
    fn subscribe(
        &mut self,
        subscription: angzarr_bus::Subscription<Self::Projector, BusPolicy>,
    ) -> Result<(), Misconfigured> {
        self.hub.subscribe(subscription)
    }

    fn serve<M: FnMut(&angzarr_bus::Served) -> bool + angzarr_bus::MaybeSend>(
        &mut self,
        more: M,
    ) -> impl Future<Output = Result<(), KernelError>> + angzarr_bus::MaybeSend {
        self.hub.serve(more)
    }
}

impl<S: Store + Outbox> angzarr_bus::Dispatch for KernelDispatch<'_, S> {
    type Projector = Box<dyn DynProjectorHandler>;
    type Policy = BusPolicy;
    type Refused = Misconfigured;
    type Error = KernelError;

    fn admit(
        &self,
        handler: &Handler<Self::Projector, BusPolicy>,
    ) -> Result<Admitted, Misconfigured> {
        let catalog = self.kernel.registry().catalog();
        let admitted = match handler {
            Handler::Projector { scope, projector } => Admitted {
                name: projector.name().to_string(),
                scope: scope.clone(),
            },
            Handler::Policy(policy) => {
                let name = policy.name().to_string();
                if catalog.policy_names().any(|inline| inline == name) {
                    return Err(Misconfigured::ComponentRegistered { component: name });
                }
                Admitted {
                    name,
                    scope: Scope::domain(policy.source()),
                }
            }
        };
        let domains = admitted.scope.domains();
        if let Some(remote) = domains.iter().find(|source| catalog.is_remote(source)) {
            return Err(Misconfigured::RemoteSource {
                domain: remote.clone(),
                component: admitted.name,
            });
        }
        Ok(admitted)
    }

    fn taken(&self, name: &str) -> Misconfigured {
        Misconfigured::ComponentRegistered {
            component: name.to_string(),
        }
    }

    fn non_atomic_policy(&self, name: &str) -> Misconfigured {
        Misconfigured::EphemeralPolicy {
            component: name.to_string(),
        }
    }

    async fn opened<'a>(
        &'a self,
        handler: &'a Handler<Self::Projector, BusPolicy>,
        _: &'a str,
    ) -> Result<(), KernelError> {
        match handler {
            Handler::Policy(policy) => {
                let now = (self.clock)();
                self.kernel
                    .relay_pending(self.store, &policy.entry, now)
                    .await
            }
            Handler::Projector { .. } => Ok(()),
        }
    }

    async fn project<'a>(
        &'a self,
        projector: &'a Self::Projector,
        name: &'a str,
        item: Item<'a>,
    ) -> Answer {
        let (book, at) = match item.page {
            None => (gone_request(item.stream).events.unwrap_or_default(), None),
            Some(page) => {
                let catalog = self.kernel.registry().catalog();
                let next = item.sequence + 1;
                let pages = vec![page.clone()];
                match project_request(catalog, item.stream, pages, next).await {
                    Err(error) => return Answer::Held(Report::of(&error)),
                    Ok(request) => {
                        let book = request.events.unwrap_or_default();
                        // A fork page is never handed, but its sequence counts.
                        if book.pages.is_empty() {
                            return Answer::Handled;
                        }
                        (book, Some((item.stream, item.sequence)))
                    }
                }
            }
        };
        let now = (self.clock)();
        let served = self
            .kernel
            .project_served(
                self.store,
                (name, &**projector),
                &book,
                (at, item.attempt, item.last),
                now,
            )
            .await;
        match served {
            Ok(()) => Answer::Handled,
            Err((failure, true)) => Answer::Passed(failure),
            Err((failure, false)) => Answer::Held(failure),
        }
    }

    async fn missing<'a>(
        &'a self,
        handler: &'a Handler<Self::Projector, BusPolicy>,
        name: &'a str,
        stream: &'a Cover,
        sequence: u64,
        missing: u64,
    ) -> Result<(), KernelError> {
        let policy = matches!(handler, Handler::Policy(_));
        let now = (self.clock)();
        let waits = (stream, sequence, missing);
        let kept = self
            .kernel
            .missing_served(self.store, name, policy, waits, now);
        kept.await.map_err(KernelError::Store)
    }

    async fn react<'a>(&'a self, policy: &'a BusPolicy, name: &'a str, item: Item<'a>) -> Answer {
        let Some(page) = item.page else {
            return Answer::Handled;
        };
        let now = (self.clock)();
        let reacted = self
            .kernel
            .react_served(self.store, &policy.entry, item.stream, page, now)
            .await;
        let Err(failure) = reacted else {
            return Answer::Handled;
        };
        let failure = step_report(failure);
        let at = match StreamRef::try_from(item.stream) {
            Ok(stream) => {
                let position = PositionKey {
                    handler: name,
                    stream,
                };
                self.store.position(position).await
            }
            Err(error) => Err(error),
        };
        match at {
            Ok(at) if at > item.sequence => Answer::Passed(failure),
            _ => Answer::Held(failure),
        }
    }
}

#[cfg(test)]
#[path = "bus.test.rs"]
mod tests;
