//! A subscription: its handler, of one of two kinds, and its credit.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use angzarr_proto::v1::Cover;

use super::positions::{MemoryPositions, Positions};

/// Every stream of a set of domains, at least one, every edition's streams
/// as main's. A stream that appears later is in it from its start.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Scope {
    domains: Vec<String>,
}

impl Scope {
    /// `first` and `rest`, in order, each once.
    pub fn of(first: &str, rest: &[&str]) -> Scope {
        let mut domains = vec![first.to_string()];
        for domain in rest {
            if !domains.iter().any(|held| held == domain) {
                domains.push(domain.to_string());
            }
        }
        Scope { domains }
    }

    /// `domain` alone.
    pub fn domain(domain: &str) -> Scope {
        Scope::of(domain, &[])
    }

    /// Its domains, in order.
    pub fn domains(&self) -> &[String] {
        &self.domains
    }

    /// Whether `stream` is one of its domains'.
    pub fn holds(&self, stream: &Cover) -> bool {
        self.domains.contains(&stream.domain)
    }
}

/// How much of its scope a subscription has in flight: at most `instances`
/// events at once, and of one stream one, in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Credit {
    instances: NonZeroUsize,
}

impl Credit {
    /// `instances` at once.
    pub fn new(instances: NonZeroUsize) -> Credit {
        Credit { instances }
    }

    /// How many events it has in its handler at once, at most.
    pub fn instances(&self) -> NonZeroUsize {
        self.instances
    }
}

/// A subscription's handler: a projector over its scope, or a policy over
/// its one input domain. Its name is the handler's own: what keys its
/// positions and, for a policy, derives its outputs' ids.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Handler<P, Q> {
    /// A sink: its answer is handled or a failure, and the bus routes
    /// nothing.
    Projector {
        /// The streams it is served.
        scope: Scope,
        /// The sink.
        projector: P,
    },
    /// A policy, served its input domain's streams: its answer is outputs,
    /// which the bus routes and commits with its position.
    Policy(Q),
}

/// One handler on a bus, at its credit, its positions in the hub's store
/// unless it says otherwise ([`Subscription::positions`]).
pub struct Subscription<P, Q> {
    handler: Handler<P, Q>,
    credit: Credit,
    positions: Option<Arc<dyn Positions>>,
}

impl<P: Clone, Q: Clone> Clone for Subscription<P, Q> {
    fn clone(&self) -> Self {
        Subscription {
            handler: self.handler.clone(),
            credit: self.credit,
            positions: self.positions.clone(),
        }
    }
}

impl<P: fmt::Debug, Q: fmt::Debug> fmt::Debug for Subscription<P, Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription")
            .field("handler", &self.handler)
            .field("credit", &self.credit)
            .field("positions", &self.positions.as_ref().map(|_| "its own"))
            .finish()
    }
}

impl<P, Q> Subscription<P, Q> {
    /// `projector` over the streams of `scope`, at `credit`.
    pub fn projector(scope: Scope, credit: Credit, projector: P) -> Subscription<P, Q> {
        Subscription {
            handler: Handler::Projector { scope, projector },
            credit,
            positions: None,
        }
    }

    /// `policy` over its input domain's streams, as `instances` at once.
    pub fn policy(instances: NonZeroUsize, policy: Q) -> Subscription<P, Q> {
        Subscription {
            handler: Handler::Policy(policy),
            credit: Credit::new(instances),
            positions: None,
        }
    }

    /// Its positions kept by `tracker` and not in the hub's store: chosen
    /// per subscription, so one bus serves subscriptions of several kinds.
    /// A policy's position must commit with its outputs, so a bus refuses
    /// a policy on a tracker that cannot ([`Positions::atomic_with_outputs`]).
    pub fn positions(mut self, tracker: Arc<dyn Positions>) -> Subscription<P, Q> {
        self.positions = Some(tracker);
        self
    }

    /// Its positions in memory only, empty at the start of every serve
    /// ([`MemoryPositions`]): it is handed every stream from sequence 0 each
    /// time it runs, and writes no position to any store. Its handler is
    /// delivered at least once, and the whole history again after a
    /// restart, so it must be idempotent. A projector's alone; a bus
    /// refuses a policy.
    pub fn ephemeral(self) -> Subscription<P, Q> {
        self.positions(Arc::new(MemoryPositions::new()))
    }

    /// Its own tracker, if it chose one.
    pub fn tracker(&self) -> Option<&Arc<dyn Positions>> {
        self.positions.as_ref()
    }

    /// Its handler.
    pub fn handler(&self) -> &Handler<P, Q> {
        &self.handler
    }

    /// Its credit.
    pub fn credit(&self) -> Credit {
        self.credit
    }

    /// Whether it is a policy's.
    pub fn is_policy(&self) -> bool {
        matches!(self.handler, Handler::Policy(_))
    }

    /// Its handler, credit and own tracker.
    pub fn into_parts(self) -> (Handler<P, Q>, Credit, Option<Arc<dyn Positions>>) {
        (self.handler, self.credit, self.positions)
    }
}

#[cfg(test)]
#[path = "subscription.test.rs"]
mod tests;
