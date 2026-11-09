//! The command half: what a frontend implements to take commands, who is
//! calling, and why a request was not served.

use angzarr_coordinator_base::{Answered, Code, Coded, Refusal};
use angzarr_ports::{BoxFuture, MaybeSend, MaybeSync};
use angzarr_proto::v1::{CommandBook, Cover, Projection, ReactionDelivery};

use crate::{Arrival, Hidden, Reach, Served, Unimplemented};

/// Who is calling: a client, or a framework component (a policy's or a
/// process manager's coordinator, by its own identity). A frontend's `check`
/// refuses a client's `causation_id`; `deliver` is a framework's alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Caller {
    Client,
    Framework,
}

/// What `handle_sync` answers: the command's [`Answered`] and the named
/// projectors' projections (empty where the endpoint has none).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Synced {
    pub answered: Answered,
    pub projections: Vec<Projection>,
}

/// The command half of a coordinator, the server direction of the same
/// `Handle`: what a frontend (the embedded host's `App`, the distributed
/// pipeline) implements. `handle` is required; the others answer
/// UNIMPLEMENTED with a key ([`Unimplemented`](crate::Unimplemented)) until
/// the frontend takes them. Object-safe: the futures are boxed, `Send` on
/// every target but wasm32.
pub trait Commands: MaybeSend + MaybeSync {
    /// What a refused or failed command is: it answers as a [`Coded`], and a
    /// refusal of the edge's own (an unimplemented operation) is one.
    type Failure: Refusal + From<Coded> + MaybeSend;

    /// The frontend's own answer for a domain it does not serve: what a
    /// hidden domain is answered as, so a caller cannot tell hidden from
    /// missing.
    fn unknown_domain(&self, domain: &str) -> Self::Failure;

    /// DECISION: the whole pipeline for one command or fact.
    fn handle<'a>(
        &'a self,
        book: CommandBook,
        caller: Caller,
    ) -> BoxFuture<'a, Result<Answered, Self::Failure>>;

    /// ASYNC: validate, enqueue and return; no decision comes back.
    fn handle_async<'a>(
        &'a self,
        book: CommandBook,
        caller: Caller,
    ) -> BoxFuture<'a, Result<(), Self::Failure>> {
        let _ = (book, caller);
        Box::pin(async {
            Err(Unimplemented {
                operation: "handle_async",
            }
            .coded()
            .into())
        })
    }

    /// SIMPLE: as `handle`, and waits for the named projectors.
    fn handle_sync<'a>(
        &'a self,
        book: CommandBook,
        projectors: Vec<String>,
        caller: Caller,
    ) -> BoxFuture<'a, Result<Synced, Self::Failure>> {
        let _ = (book, projectors, caller);
        Box::pin(async {
            Err(Unimplemented {
                operation: "handle_sync",
            }
            .coded()
            .into())
        })
    }

    /// A policy's output or refusal fact; the coordinator lets only a
    /// framework caller reach it.
    fn deliver<'a>(
        &'a self,
        delivery: ReactionDelivery,
    ) -> BoxFuture<'a, Result<Answered, Self::Failure>> {
        let _ = delivery;
        Box::pin(async {
            Err(Unimplemented {
                operation: "deliver",
            }
            .coded()
            .into())
        })
    }
}

/// A coordinator with no command half: what the edge is generic over where a
/// frontend serves reads alone. Every command answers UNIMPLEMENTED,
/// `DISTRIBUTED_REASON_COMMANDS_NOT_SERVED`, with the domain.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NoCommands;

/// A coordinator with no command half serves no domain: every command is
/// refused as unserved, and answered `commands-not-served` with its domain.
impl Served for NoCommands {
    fn reach(&self, domain: &str) -> Reach {
        let _ = domain;
        Reach::None
    }
}

impl Commands for NoCommands {
    type Failure = Coded;

    fn unknown_domain(&self, domain: &str) -> Coded {
        not_served(domain)
    }

    fn handle<'a>(
        &'a self,
        book: CommandBook,
        caller: Caller,
    ) -> BoxFuture<'a, Result<Answered, Coded>> {
        let _ = caller;
        Box::pin(async move { Err(not_served(domain_of(book.cover.as_ref()))) })
    }

    fn handle_async<'a>(
        &'a self,
        book: CommandBook,
        caller: Caller,
    ) -> BoxFuture<'a, Result<(), Coded>> {
        let _ = caller;
        Box::pin(async move { Err(not_served(domain_of(book.cover.as_ref()))) })
    }

    fn handle_sync<'a>(
        &'a self,
        book: CommandBook,
        projectors: Vec<String>,
        caller: Caller,
    ) -> BoxFuture<'a, Result<Synced, Coded>> {
        let _ = (projectors, caller);
        Box::pin(async move { Err(not_served(domain_of(book.cover.as_ref()))) })
    }

    fn deliver<'a>(&'a self, delivery: ReactionDelivery) -> BoxFuture<'a, Result<Answered, Coded>> {
        Box::pin(async move { Err(not_served(domain_of(delivery.cover.as_ref()))) })
    }
}

/// The domain a cover names; none is the empty domain.
fn domain_of(cover: Option<&Cover>) -> &str {
    cover.map_or("", |cover| cover.domain.as_str())
}

/// The key a coordinator with no command half answers every command under:
/// `DISTRIBUTED_REASON_COMMANDS_NOT_SERVED` (UNIMPLEMENTED, arg `domain`),
/// the spec's key for a server that takes no commands. The edge links no
/// transport, so the key is spelled here.
pub const COMMANDS_NOT_SERVED: &str = "DISTRIBUTED_REASON_COMMANDS_NOT_SERVED";

fn not_served(domain: &str) -> Coded {
    Coded::new(
        Code::Unimplemented,
        COMMANDS_NOT_SERVED,
        [("domain".to_string(), domain.to_string())],
    )
}

/// Why a request was not served: what its caller is answered, and, for a
/// hidden domain, why, which only the frontend's own log may say.
#[derive(Clone, Debug, PartialEq)]
pub enum Unserved<F> {
    /// Its domain is not served to where it arrived from: answered
    /// `answer`, exactly as an unknown domain is; `hidden` is the real
    /// reason, the frontend's to log, never the caller's.
    Hidden { answer: F, hidden: Hidden },
    /// The endpoint's own failure, answered as it is.
    Failed(F),
}

impl<F> Unserved<F> {
    /// What the caller is answered.
    pub fn answer(&self) -> &F {
        match self {
            Unserved::Hidden { answer, .. } | Unserved::Failed(answer) => answer,
        }
    }

    /// Why the domain was hidden, for the frontend's own log: `None` for
    /// the endpoint's own failure.
    pub fn hidden(&self) -> Option<&Hidden> {
        match self {
            Unserved::Hidden { hidden, .. } => Some(hidden),
            Unserved::Failed(_) => None,
        }
    }
}

/// `endpoint` serves `domain` to `arrival`, or the request is
/// [`Unserved::Hidden`]: answered as the endpoint's own unknown domain, the
/// real reason beside it.
pub fn route_command<E: Commands + Served>(
    endpoint: &E,
    domain: &str,
    arrival: Arrival,
) -> Result<(), Unserved<E::Failure>> {
    endpoint
        .route(domain, arrival)
        .map_err(|hidden| Unserved::Hidden {
            answer: endpoint.unknown_domain(domain),
            hidden,
        })
}

#[cfg(test)]
#[path = "commands.test.rs"]
mod tests;
