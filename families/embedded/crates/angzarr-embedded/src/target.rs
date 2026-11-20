//! Where a command for a domain is handled: the routing seam.
//!
//! Every command the kernel handles, the application's own and every
//! policy step's, names its domain in `cover.domain`, and the registry
//! resolves that name to a [`Target`]: a domain handled here (its handler
//! in this process, or in a child behind a local socket, the kernel's
//! pipeline over the kernel's store either way), or a domain whose streams
//! another coordinator owns, reached through a [`CoordinatorPort`] (a
//! distributed angzarr's `CommandHandlerCoordinatorService`, say). The
//! kernel knows only the port: what sits behind it is the composition
//! root's, so a transport is added without touching the kernel.
//!
//! A command for a remote domain is checked and identified here (its
//! command id fixed before it leaves, so a retry is deduplicated there; a
//! correlation its sender left unset is left unset, for the remote
//! coordinator's own default), then handed to the port (the coordinator's
//! `forward`);
//! nothing of its stream is loaded, admitted, decided or written here, and
//! no policy or projector here handles its events
//! ([`Misconfigured::RemoteSource`](super::Misconfigured::RemoteSource)). Its answer
//! comes back as a local domain's would: its events, a duplicate, or a
//! refusal under the class its status gives. A policy's step into a remote
//! domain goes the same way, the policy's position written here.

use std::future::Future;
use std::pin::Pin;

use angzarr_proto::v1::{CommandBook, DeliveryOutcome, EventBook, ReactionDelivery};
use angzarr_router::error::CodedError;
use angzarr_router::router::DynCommandHandler;

/// What a coordinator answers for one command, in core's messages: the
/// addressed stream's persisted events and what it did with the delivery
/// (APPLIED or DUPLICATE), as `CommandHandlerCoordinatorService.Handle`
/// and `Deliver` answer. That answer is the distributed transport's
/// `CommandResult` (decisions 14.64 and 14.105), which the kernel never
/// names: a port over that transport builds this from its `events` and
/// its `delivery`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Answered {
    /// The addressed stream's events: those persisted, or, for a duplicate,
    /// those the stream already held; its cover unset means the command's.
    pub events: Option<EventBook>,
    /// What the coordinator did with the delivery.
    pub delivery: DeliveryOutcome,
}

/// What a [`CoordinatorPort`]'s call is: a `Send` future, so the kernel's
/// own future stays `Send` across it.
pub type Coordinated<'a> = Pin<Box<dyn Future<Output = Result<Answered, CodedError>> + Send + 'a>>;

/// A coordinator elsewhere that owns a domain's streams, as the kernel
/// reaches it: angzarr's `CommandHandlerCoordinatorService.Handle`. It is
/// handed the book as the kernel identified it (its command id fixed
/// here, so a retry is deduplicated there; its correlation as its sender
/// set it, or unset), or a reaction as the kernel stamped it, and answers
/// as `Handle` does ([`Answered`]): the persisted events, under APPLIED or
/// DUPLICATE; or an error status, as the router carries one
/// ([`CodedError`]: a typed refusal, a bug, a retryable failure), which
/// the kernel treats as it treats a local handler's.
pub trait CoordinatorPort: Send + Sync {
    /// Handle `book`, a client's command or fact for one of this
    /// coordinator's domains (`Handle`).
    fn handle(&self, book: CommandBook) -> Coordinated<'_>;

    /// Deliver `delivery`, a policy's output or a refusal fact the kernel
    /// stamped, for one of this coordinator's domains (`Deliver`, the
    /// framework-only RPC; decision 14.79).
    fn deliver(&self, delivery: ReactionDelivery) -> Coordinated<'_>;
}

/// What a domain name resolves to ([`Registry::target`](super::Registry::target)).
pub enum Target<'r> {
    /// Handled here: the kernel's pipeline over its own store, deciding
    /// through this handler (in this process, or a child's behind its
    /// socket).
    Local(&'r dyn DynCommandHandler),
    /// Handled by the coordinator that owns the domain's streams: nothing
    /// of it is loaded or written here.
    Remote(&'r dyn CoordinatorPort),
}

impl std::fmt::Debug for Target<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Target::Local(handler) => f.debug_tuple("Local").field(&handler.name()).finish(),
            Target::Remote(_) => f.write_str("Remote"),
        }
    }
}

#[cfg(test)]
#[path = "target.test.rs"]
mod tests;
