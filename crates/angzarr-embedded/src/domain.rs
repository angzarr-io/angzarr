//! What the kernel knows of a domain's types: its state and how its events
//! fold into it.

use coded_diagnostic::Diagnostic;

use super::payload::Payload;
use angzarr_router::PageContext;

/// A domain's state: a domain type, registered as its component's anchor
/// ([`State::Anchor`]): an aggregate's or a process manager's anchor is
/// its event-sourced state (docs/design/component-options.md §1), so the
/// anchor's full name is the state type its descriptor registers, and what
/// a snapshot of it holds.
///
/// Snapshots are a framework optimisation, never business data
/// (docs/design/snapshots.md): a state opts in by overriding
/// [`State::snapshot`] and [`State::restore`], and offers itself with every
/// answer; the kernel stores what is offered as the domain's
/// [`SnapshotPolicy`](super::SnapshotPolicy) says, and starts a load from
/// it only when [`Catalog::snapshot_usable`](super::Catalog::snapshot_usable)
/// passes it and the state restores it. A state that keeps both defaults
/// offers none and restores none, so its streams are always replayed from
/// the start, a snapshot an earlier build of it stored ignored. angzarr's generator
/// (`angzarr_build`) and the markup (`#[aggregate]`, through
/// [`markup::Snapshots`](super::markup::Snapshots)) write the impl.
pub trait State: Default + Clone + Send + 'static {
    /// The component's anchor, the prost message the state is registered
    /// and snapshotted as.
    type Anchor: prost::Message + prost::Name + Default;

    /// The state as its anchor, offered as a snapshot with every answer;
    /// `None` offers none. The default offers none: a state that takes
    /// snapshots overrides it, and [`State::restore`].
    fn snapshot(&self) -> Option<Self::Anchor> {
        None
    }

    /// Overwrite this instance's state with what `anchor`, a snapshot's,
    /// holds, keeping whatever it does not (a marked-up aggregate's
    /// immutable configuration, from the registry's factory): resuming
    /// builds a fresh instance, then restores it. A wire value need not be
    /// a valid state (a required field absent, an enum value the domain
    /// does not define, a broken invariant), so a restore may refuse it: a
    /// snapshot the state refuses is not offered (docs/design/snapshots.md
    /// section 3), and the load replays the stream from its first event.
    /// The default refuses every snapshot ([`NotSnapshotted`]), so a state
    /// that takes none always replays. ([`Stream::resume`](super::Stream::resume),
    /// handed a snapshot directly, reports a refusal as the snapshot
    /// unreadable, [`PayloadError::Invalid`](super::PayloadError).)
    fn restore(&mut self, _anchor: Self::Anchor) -> Result<(), coded_diagnostic::Report> {
        Err(coded_diagnostic::Report::of(&NotSnapshotted::of::<
            Self::Anchor,
        >()))
    }
}

/// A snapshot handed to a state that takes none ([`State::restore`]'s
/// default, and [`markup::Snapshots::restore`](super::markup::Snapshots::restore)'s):
/// its anchor, by full name. A load never meets it: a snapshot its state
/// does not restore is not offered, and the stream is replayed. Only
/// [`Stream::resume`](super::Stream::resume), handed one directly, reports
/// it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NotSnapshotted {
    pub anchor: String,
}

impl NotSnapshotted {
    /// The refusal of a snapshot of anchor `A`.
    pub fn of<A: prost::Name>() -> NotSnapshotted {
        NotSnapshotted {
            anchor: A::full_name(),
        }
    }
}

impl std::fmt::Display for NotSnapshotted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        coded_diagnostic::display(self, f)
    }
}

impl Diagnostic for NotSnapshotted {
    fn code(&self) -> &str {
        coded_diagnostic::code!("not-snapshotted")
    }

    fn args(&self) -> Vec<(&str, coded_diagnostic::Arg)> {
        vec![("anchor", self.anchor.as_str().into())]
    }
}

/// One kind of event stream, as the kernel reads it: its state, its
/// commands and facts, its events and how each folds into the state. Its
/// handlers are angzarr-router's: a domain is dispatched by the router's
/// `DomainDispatch` over its state, built by the kernel from this
/// description ([`Registry::register`](super::Registry::register)), its
/// command and fact handlers registered on it (`#[aggregate]` and
/// `#[process_manager]` generate them).
///
/// - **Payloads are prost messages.** Commands, facts and events are
///   [`Payload`]s, packed as `Any` by type URL; the state is a [`State`].
/// - **Business output is payloads only.** A handler emits events; the
///   kernel builds the pages: their sequences and their `EventMeta`.
/// - everything `'static` and `Send`.
pub trait Domain: Sized + 'static {
    /// Folded from the events. Never stored, except as a snapshot.
    type State: State;
    /// A request to change the stream. Decided, never stored.
    type Command: Payload + Send + 'static;
    /// Something that already happened outside the system (a webhook, an
    /// integration). Recorded, never refused.
    /// [`std::convert::Infallible`] for a domain that takes none.
    type Fact: Payload + Send + 'static;
    /// What a stream stores.
    type Event: Payload + Clone + Send + Sync + 'static;
    /// Why its handlers refuse a command, for the person who gave it. The
    /// caller receives it as a [`Rejected`](super::Rejected): its code, args
    /// and typed refusal object, and, from a handler in this process, the
    /// rejection itself ([`Rejected::rejection`](super::Rejected::rejection)).
    /// Kernel faults are never folded into it: see [`Failure`](super::Failure).
    type Rejection: Diagnostic + Send + Sync + 'static;
    /// The domain's name: the `Cover.domain` of every stream of this
    /// domain, and its key in the [`Registry`](super::Registry). Stable
    /// across releases; never contains `':'`.
    const NAME: &'static str;

    /// The full version of the build the domain is (types.proto,
    /// `EventMeta.rules_version`): its crate's `CARGO_PKG_VERSION`, the
    /// version angzarr-build writes into its descriptor, never a label
    /// chosen by hand (`#[aggregate]` and `#[process_manager]` give
    /// `env!("CARGO_PKG_VERSION")`). The domain is registered at it, and its
    /// answers report it; the kernel reads it only to choose which
    /// snapshots to offer ([`Catalog::snapshot_usable`](super::Catalog::snapshot_usable)).
    const RULES_VERSION: &'static str;

    /// Fold one event into the state, given where it sits: `page`, the
    /// router's page context, its stream's cover, its sequence and its
    /// whole metadata ([`EventMeta`](angzarr_proto::v1::EventMeta): its
    /// cause, correlation, fact origin, rules version and `recorded_at`).
    /// An event of the decision being made is folded with the metadata it
    /// will be stamped with when it is committed
    /// ([`CommandContext::event_meta`](angzarr_router::domain::CommandContext::event_meta) of the handler's context: its
    /// instant is the handler's `Context::recorded_at`), so the state
    /// decided is the state replayed. Pure: the same state, event and page
    /// always give the same result.
    fn apply(state: &mut Self::State, event: &Self::Event, page: &PageContext<'_>);
}

#[cfg(test)]
#[path = "domain.test.rs"]
mod tests;
