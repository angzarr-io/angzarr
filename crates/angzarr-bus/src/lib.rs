//! angzarr's bus interface: subscriptions served from a stream store, ONE
//! trait for the in-process bus over the event store (angzarr-embedded's
//! `Bus`) and the distributed one (the coordinator's), so either stands in
//! for the other. Normative: angzarr-project's
//! `docs/design/bus-interface.md`.
//!
//! A [`Bus`] serves [`Subscription`]s. Each is a handler of one of two
//! kinds ([`Handler`]) and how much of its scope it may have in flight
//! ([`Credit`]):
//!
//! - **a projector**, a sink over every stream of a [`Scope`]: its answer is
//!   handled or a failure, and the bus routes nothing (its instances may
//!   record through a runtime as a side effect, each record with its own
//!   identity);
//! - **a policy**, over its one input domain: its answer is outputs, which
//!   the bus routes (addressed, stamped, checked, their command ids
//!   derived by `ids::policy_command_id`) and commits with the policy's
//!   position.
//!
//! What every implementation guarantees:
//!
//! - **At least once.** A position is written after its handler answers; a
//!   crash hands again exactly what the stored position does not cover.
//! - **A position is a mark** per (subscription, stream): every sequence
//!   below it handled, the watermark of the distributed `Subscribe`.
//! - **One event of a stream in flight, in order**; nothing orders two
//!   streams.
//! - **Events missing or out of order are recovered and applied in stream
//!   order**; the transport's own copies, arriving later, are dropped as
//!   duplicates (settled handled, never handed again). A sequence is never
//!   skipped: what cannot be recovered holds its stream.
//! - **A policy's position moves with its outputs**: never past a trigger
//!   whose outputs are not all durable, and a re-run delivers nothing
//!   twice.
//! - **A failure is held and handed again**, or, for a policy, what its
//!   router's bug setting says (dead-lettered, the position past it, or
//!   held).
//! - **A purged stream** is no special case: its tombstone
//!   ([`angzarr_ports::tombstone`]) is an event delivered at its sequence, a
//!   projector's reset and nothing a policy is called for.
//!
//! **Two layers.** [`Bus`] is the application's interface. Under it, a
//! [`Transport`] carries each subscription's deliveries (a broker, the
//! coordinator's message bus, or the event store itself,
//! [`StoreTransport`]) and takes back each one's settlement: every delivery
//! carries a token, its stream, sequence, attempt and [`OrderingKey`], and
//! the event or a reference to it, and is settled handled, held or
//! dead-lettered, never dropped. [`Hub`] is the one generic bus over any
//! transport, the coordinator's role whether in process or distributed:
//! positions, dedup by the mark, order and gaps recovered through a
//! [`Recover`] (the event store's, [`StoreRead`], unless the transport
//! re-sends), references resolved through a [`Resolve`], attempts and the
//! [`RetryPolicy`], credit, and idleness; its handlers are reached through
//! a [`Dispatch`]. A transport is assembled from shared components: the
//! [`CreditGate`], [`Delayed`] for held deliveries, and the recovery and
//! resolution above.
//!
//! The handler types are the implementation's ([`Bus::Projector`],
//! [`Bus::Policy`]): angzarr-router's `ProjectorHandler` and
//! `PolicyComponent` depend on this workspace, so nothing here names them.
//!
//! # Features
//!
//! - `contract`: the conformance suite, [`contract`] and
//!   [`bus_contract!`](crate::bus_contract) for a bus, and
//!   [`contract::transport`] and
//!   [`transport_contract!`](crate::transport_contract) for a transport.

mod credit;
mod hub;
mod positions;
mod pump;
mod recovery;
mod retry;
mod served;
mod store;
mod subscription;
mod transport;

#[cfg(feature = "contract")]
pub mod contract;

use std::future::Future;

pub use credit::CreditGate;
pub use hub::{Admitted, Answer, Dispatch, Hub, Item, AHEAD, MISSING};
pub use positions::{DurablePositions, MemoryPositions, Positions};
pub use pump::Pumped;
pub use recovery::{Pages, Recover, Recovered, Resolve, StorePages, StoreRead};
pub use retry::{Delayed, RetryPolicy};
pub use served::{Failed, Outcome, Served};
pub use store::{ChangeSource, Following, StoreFeed, StoreToken, StoreTransport};
pub use subscription::{Credit, Handler, Scope, Subscription};
pub use transport::{
    Body, Delivery, Feed, Flow, Next, Opening, OrderingKey, PullTransport, Settle, Sink, Timer,
    Transport,
};

/// The ports' bounds that differ by target, as the bus uses them.
pub use angzarr_ports::{MaybeSend, MaybeSync};
/// The wire types, as the bus names them.
pub use angzarr_proto;
/// The diagnostic crate a failure's cause is, as the bus uses it.
pub use coded_diagnostic;

/// A bus: subscriptions served from a stream store. See the crate.
pub trait Bus {
    /// What a projector subscription is served by: a sink.
    type Projector;
    /// What a policy subscription is served by: a policy, whose outputs
    /// the bus routes.
    type Policy;
    /// Why a subscription is refused: a name another subscription on the
    /// bus has (the name keys its positions and, for a policy, its
    /// outputs' ids), or a handler the bus cannot serve.
    type Refused: coded_diagnostic::Diagnostic;
    /// Why serving stopped: what the bus cannot hold and hand again (a
    /// store that fails).
    type Error: coded_diagnostic::Diagnostic;

    /// Serve `subscription` too, from the next [`Bus::serve`] on.
    fn subscribe(
        &mut self,
        subscription: Subscription<Self::Projector, Self::Policy>,
    ) -> Result<(), Self::Refused>;

    /// Serve every subscription until `more`, asked with what was served
    /// since it was last asked each time the bus is idle (nothing in
    /// flight, nothing owed but what failed), says no; or until an error
    /// the bus cannot hold. Dropping the future is a crash: the next serve
    /// hands again what the stored positions do not cover.
    fn serve<M: FnMut(&Served) -> bool + MaybeSend>(
        &mut self,
        more: M,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;
}
