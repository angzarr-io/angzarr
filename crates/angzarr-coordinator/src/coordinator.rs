//! [`Coordinator`]: the commands, the reads and the gate, composed; and
//! [`Session`], one identified caller's requests through it.
//!
//! Every request is evaluated in one order: identify; the operation is
//! allowed (the principal's [`Allowlist`](crate::Allowlist)); reach admits
//! every domain named; the gate owns every stream named; stamp; handle. Any
//! failure after identify is [`Denied`]: the audit sink gets the real reason
//! and the caller is answered exactly as the endpoint answers a domain it
//! does not serve. Only a bad credential ([`Unauthenticated`]) and a call a
//! client may not make (`deliver`) are shown.

use std::sync::Arc;

use angzarr_proto::v1::Cover;

use crate::{
    Allowlist, Arrival, AuditSink, Credential, Denied, Gate, Principal, ReadOp, Served,
    Unauthenticated,
};

/// The commands, the reads and the gate. `C` and `R` are the halves
/// ([`NoCommands`](crate::NoCommands) and [`NoReads`](crate::NoReads) where
/// one is absent, so the absence is in the type).
pub struct Coordinator<C, R, G: Gate> {
    commands: C,
    reads: R,
    gate: G,
    audit: Option<Arc<dyn AuditSink<G::Identity>>>,
}

impl<C, R, G: Gate> Coordinator<C, R, G> {
    /// `commands` and `reads` behind `gate`, with no audit sink.
    pub fn new(commands: C, reads: R, gate: G) -> Coordinator<C, R, G> {
        Coordinator {
            commands,
            reads,
            gate,
            audit: None,
        }
    }

    /// Real denial reasons go to `sink`.
    pub fn with_audit(mut self, sink: Arc<dyn AuditSink<G::Identity>>) -> Coordinator<C, R, G> {
        self.audit = Some(sink);
        self
    }

    /// The command half.
    pub fn commands(&self) -> &C {
        &self.commands
    }

    /// The read half.
    pub fn reads(&self) -> &R {
        &self.reads
    }

    /// The gate.
    pub fn gate(&self) -> &G {
        &self.gate
    }

    /// Identify the caller from what it presented, and open a [`Session`]
    /// for it; a missing, stale or revoked credential is [`Unauthenticated`].
    pub async fn open(
        &self,
        arrival: Arrival,
        credential: Option<&Credential>,
    ) -> Result<Session<'_, C, R, G>, Unauthenticated> {
        match self.gate.identify(arrival, credential).await {
            Ok(principal) => Ok(self.open_as(arrival, principal)),
            Err(unauthenticated) => {
                if let Some(sink) = &self.audit {
                    sink.unauthenticated(arrival);
                }
                Err(unauthenticated)
            }
        }
    }

    /// A [`Session`] for a principal the caller already holds, with no
    /// credential to present (an in-process frontend that identified its
    /// caller itself): every check after identify applies as it does to a
    /// caller identified by `open`.
    pub fn open_as(
        &self,
        arrival: Arrival,
        principal: Principal<G::Identity>,
    ) -> Session<'_, C, R, G> {
        Session {
            coordinator: self,
            arrival,
            principal,
        }
    }
}

/// One identified caller's requests, from one arrival, through the gate.
pub struct Session<'a, C, R, G: Gate> {
    coordinator: &'a Coordinator<C, R, G>,
    arrival: Arrival,
    principal: Principal<G::Identity>,
}

impl<C, R, G: Gate> Session<'_, C, R, G> {
    /// Who this session is.
    pub fn principal(&self) -> &Principal<G::Identity> {
        &self.principal
    }

    /// Where its requests arrive from.
    pub fn arrival(&self) -> Arrival {
        self.arrival
    }

    /// The principal, the session given up: for a caller that carries an
    /// identified caller past the session's borrow (a long-lived
    /// subscription, served on a task of its own, opens it again with
    /// [`Coordinator::open_as`] and the same arrival).
    pub fn into_principal(self) -> Principal<G::Identity> {
        let _ = self.principal;
        todo!("resolute-sleet: the principal given up")
    }

    /// Whether the gate still holds this identity good: what a long-lived
    /// session asks before each request, and a subscription every few
    /// wakes.
    pub async fn still_valid(&self) -> bool {
        self.coordinator
            .gate
            .still_valid(self.principal.identity())
            .await
    }

    /// The real reason `denied` goes to the audit sink, and back, for the
    /// caller to answer as a missing domain.
    pub(crate) fn refuse(&self, denied: Denied) -> Denied {
        if let Some(sink) = &self.coordinator.audit {
            sink.denied(self.principal.identity(), self.arrival, &denied);
        }
        denied
    }

    /// A revoked identity: audited, and shown as it is.
    pub(crate) fn revoked(&self) -> Unauthenticated {
        if let Some(sink) = &self.coordinator.audit {
            sink.unauthenticated(self.arrival);
        }
        Unauthenticated
    }

    /// Whether the gate owns the stream `cover` names.
    pub(crate) fn owns(&self, cover: &Cover) -> bool {
        self.coordinator.gate.owns(self.principal.identity(), cover)
    }

    /// The principal's allowlist.
    pub(crate) fn allowlist(&self) -> &Allowlist {
        self.principal.allowlist()
    }
}

impl<C, R: Served, G: Gate> Session<'_, C, R, G> {
    /// Whether the read operation `op` may be used on every stream `covers`
    /// names, or the [`Denied`] for the first step that fails, audited: the
    /// operation is allowed; reach admits every cover's domain; the gate
    /// owns EVERY cover. All or nothing: one cover refused refuses the
    /// request, so nothing is ever answered for the others.
    pub fn authorize_streams(&self, op: ReadOp, covers: &[Cover]) -> Result<(), Denied> {
        self.allowlist()
            .permits_read(op)
            .map_err(|denied| self.refuse(denied))?;
        for cover in covers {
            self.coordinator
                .reads
                .route(&cover.domain, self.arrival)
                .map_err(|hidden| self.refuse(Denied::NotExposed(hidden)))?;
        }
        for cover in covers {
            if !self.owns(cover) {
                return Err(self.refuse(Denied::NotOwner {
                    domain: cover.domain.clone(),
                }));
            }
        }
        Ok(())
    }
}

#[cfg(feature = "commands")]
mod commands {
    use angzarr_coordinator_base::Answered;
    use angzarr_proto::v1::{CommandBook, ReactionDelivery};

    use super::Session;
    use crate::{Caller, Commands, Denied, FrameworkOnly, Gate, Served, Synced};

    impl<C: Commands + Served, R, G: Gate> Session<'_, C, R, G> {
        /// DECISION: the command or fact, through the gate, to the
        /// endpoint's `handle`.
        pub async fn handle(&self, book: CommandBook) -> Result<Answered, C::Failure> {
            let book = self.admit(book).await?;
            self.coordinator
                .commands
                .handle(book, self.principal.caller())
                .await
        }

        /// ASYNC: as `handle`, to the endpoint's `handle_async`.
        pub async fn handle_async(&self, book: CommandBook) -> Result<(), C::Failure> {
            let book = self.admit(book).await?;
            self.coordinator
                .commands
                .handle_async(book, self.principal.caller())
                .await
        }

        /// SIMPLE: as `handle`, to the endpoint's `handle_sync`.
        pub async fn handle_sync(
            &self,
            book: CommandBook,
            projectors: Vec<String>,
        ) -> Result<Synced, C::Failure> {
            let book = self.admit(book).await?;
            self.coordinator
                .commands
                .handle_sync(book, projectors, self.principal.caller())
                .await
        }

        /// A policy's output or refusal fact: a framework caller's alone (a
        /// client is PERMISSION_DENIED, shown), then checked as a book is.
        pub async fn deliver(&self, delivery: ReactionDelivery) -> Result<Answered, C::Failure> {
            self.ensure_valid().await?;
            if self.principal.caller() != Caller::Framework {
                return Err(FrameworkOnly {
                    operation: "deliver",
                }
                .coded()
                .into());
            }
            let domain = domain_of(delivery.cover.as_ref());
            self.allowlist()
                .permits_delivery(&delivery)
                .and_then(|()| self.check_cover(delivery.cover.as_ref()))
                .map_err(|denied| self.unserved(&domain, denied))?;
            self.coordinator.commands.deliver(delivery).await
        }

        /// The request's own failure as the caller is answered: a revoked
        /// identity is UNAUTHENTICATED, shown.
        async fn ensure_valid(&self) -> Result<(), C::Failure> {
            if self.still_valid().await {
                Ok(())
            } else {
                Err(self.revoked().coded().into())
            }
        }

        /// The gate's order for a book: still valid; the body allowed;
        /// reach; ownership; stamp; and the stamped book checked again.
        async fn admit(&self, book: CommandBook) -> Result<CommandBook, C::Failure> {
            self.ensure_valid().await?;
            let domain = domain_of(book.cover.as_ref());
            self.check_book(&book)
                .map_err(|denied| self.unserved(&domain, denied))?;
            let stamped = self.coordinator.gate.stamp(self.principal.identity(), book);
            self.check_book(&stamped)
                .map_err(|denied| self.unserved(&domain, denied))?;
            Ok(stamped)
        }

        fn check_book(&self, book: &CommandBook) -> Result<(), Denied> {
            self.allowlist().permits(book)?;
            self.check_cover(book.cover.as_ref())
        }

        /// Reach admits the cover's domain, and the gate owns its stream.
        fn check_cover(&self, cover: Option<&angzarr_proto::v1::Cover>) -> Result<(), Denied> {
            let cover = cover.cloned().unwrap_or_default();
            self.coordinator
                .commands
                .route(&cover.domain, self.arrival)
                .map_err(Denied::NotExposed)?;
            if self.owns(&cover) {
                Ok(())
            } else {
                Err(Denied::NotOwner {
                    domain: cover.domain,
                })
            }
        }

        /// `denied` audited, and the caller answered as the endpoint answers
        /// a domain it does not serve.
        fn unserved(&self, domain: &str, denied: Denied) -> C::Failure {
            self.refuse(denied);
            self.coordinator.commands.unknown_domain(domain)
        }
    }

    /// The domain a cover names; none is the empty domain.
    fn domain_of(cover: Option<&angzarr_proto::v1::Cover>) -> String {
        cover.map(|cover| cover.domain.clone()).unwrap_or_default()
    }
}

#[cfg(test)]
#[path = "coordinator.test.rs"]
mod tests;

#[cfg(all(test, feature = "commands"))]
#[path = "session.test.rs"]
mod session_tests;
