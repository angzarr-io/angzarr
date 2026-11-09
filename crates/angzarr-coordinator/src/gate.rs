//! The gate: who may reach which streams. The application implements
//! [`Gate`] (four duties: `identify`, `owns`, an optional `stamp`, a
//! defaulted `still_valid`) and the platform enforces the rest from data:
//! the [`Principal`]'s [`Allowlist`] is closed by construction.

use angzarr_ports::{BoxFuture, MaybeSend, MaybeSync};
use angzarr_proto::v1::{CommandBook, Cover};

use crate::{Allowlist, Arrival, Credential, Denied, Unauthenticated};

#[cfg(feature = "commands")]
use crate::Caller;

/// A caller the application has identified: its own identity value, what it
/// may do (the [`Allowlist`], data the platform enforces), and, where the
/// command half is built, whether it is a client or a framework component.
/// Immutable once built: nothing the gate or a request does can widen it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal<I> {
    identity: I,
    allowlist: Allowlist,
    #[cfg(feature = "commands")]
    caller: Caller,
}

impl<I> Principal<I> {
    /// `identity`, allowed what `allowlist` lists, and (where the command
    /// half is built) a client.
    pub fn new(identity: I, allowlist: Allowlist) -> Principal<I> {
        Principal {
            identity,
            allowlist,
            #[cfg(feature = "commands")]
            caller: Caller::Client,
        }
    }

    /// This principal is a framework component (a policy's or a process
    /// manager's coordinator, by its own identity), not a client.
    #[cfg(feature = "commands")]
    pub fn framework(mut self) -> Principal<I> {
        self.caller = Caller::Framework;
        self
    }

    /// The application's identity value.
    pub fn identity(&self) -> &I {
        &self.identity
    }

    /// What this principal may send and read.
    pub fn allowlist(&self) -> &Allowlist {
        &self.allowlist
    }

    /// Client or framework.
    #[cfg(feature = "commands")]
    pub fn caller(&self) -> Caller {
        self.caller
    }
}

/// What the application implements to say who may reach which streams. The
/// platform never mints, stores, compares or revokes a credential: revocation
/// takes effect because `identify` runs on every request and every stream
/// open, against current state.
pub trait Gate: MaybeSend + MaybeSync {
    /// The application's identity value (a run, a user, a service).
    type Identity: MaybeSend + MaybeSync;

    /// Turn the credential presented (it may be absent: the application
    /// decides what that means) into a principal, or refuse it
    /// ([`Unauthenticated`], shown to the caller). It may read the store;
    /// the platform passes only where the request arrived from and the
    /// credential.
    fn identify<'a>(
        &'a self,
        arrival: Arrival,
        credential: Option<&'a Credential>,
    ) -> BoxFuture<'a, Result<Principal<Self::Identity>, Unauthenticated>>;

    /// Whether `identity` owns the stream `cover` names. Asked for every
    /// stream a request names; for the list, correlation and follow reads it
    /// is a filter, and a stream not owned is dropped, never errored.
    fn owns(&self, identity: &Self::Identity, cover: &Cover) -> bool;

    /// The platform's stamp of the sender onto a command, from the
    /// identity, never the payload. Runs after the allowlist and ownership
    /// and before the frontend's own check; the result is checked again as
    /// the request was (a stamp cannot redirect a command), and the
    /// frontend still validates it. Default: the book unchanged.
    fn stamp(&self, identity: &Self::Identity, book: CommandBook) -> CommandBook {
        let _ = identity;
        book
    }

    /// Whether `identity` is still good: re-asked on every request of a
    /// long-lived session and by a long-lived subscription every few wakes,
    /// so a token dies when its run ends. Default: yes.
    fn still_valid<'a>(&'a self, identity: &'a Self::Identity) -> BoxFuture<'a, bool> {
        let _ = identity;
        Box::pin(async { true })
    }
}

/// Where a gate's real reasons go: an audit log, never the caller. Nothing
/// it is given holds a credential.
pub trait AuditSink<I>: MaybeSend + MaybeSync {
    /// `principal`, arrived from `arrival`, was refused for the reason in
    /// `denied` (and the caller answered as for a missing domain).
    fn denied(&self, principal: &I, arrival: Arrival, denied: &Denied);

    /// A credential presented from `arrival` was missing, stale or revoked.
    fn unauthenticated(&self, arrival: Arrival) {
        let _ = arrival;
    }
}

/// A gate that identifies nobody and admits what reach admits: today's
/// single-user local server, and tests. Every caller is a client with an
/// unrestricted allowlist that owns every stream; the credential is ignored.
/// Never the gate of a server where more than one party may connect.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Trusted;

impl Gate for Trusted {
    type Identity = ();

    fn identify<'a>(
        &'a self,
        arrival: Arrival,
        credential: Option<&'a Credential>,
    ) -> BoxFuture<'a, Result<Principal<()>, Unauthenticated>> {
        let _ = (arrival, credential);
        Box::pin(async { Ok(Principal::new((), Allowlist::unrestricted())) })
    }

    fn owns(&self, identity: &(), cover: &Cover) -> bool {
        let _ = (identity, cover);
        true
    }
}

#[cfg(test)]
#[path = "gate.test.rs"]
mod tests;
