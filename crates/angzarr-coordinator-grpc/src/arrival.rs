//! [`Arrival`]: how a request reached a listener, declared per listener,
//! and [`Reach`]: how far a domain is exposed.
//!
//! These are this slice's stand-ins for the coordinator edge's types of the
//! same names (the edge crate is a sibling slice this crate does not depend
//! on yet): the same two arrivals, the same admission rule. When the
//! services land, the edge's types replace these and [`arrival_of`] reads
//! them.

use std::net::SocketAddr;

use tonic::Request;

/// How a request reached the coordinator. Declared by the listener it came
/// in on, never derived from a connection's peer address.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Arrival {
    /// From this machine: a Unix socket, or a TCP listener bound to a
    /// loopback address.
    Loopback,
    /// From the network: a TCP listener on a wider bind, or any listener
    /// declared so (a container bridge).
    Ip,
}

/// How far a domain is exposed: the ceiling an arrival is held to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Reach {
    /// Reached by no one.
    None,
    /// Reached from this machine only.
    Loopback,
    /// Reached from the network, and so from this machine too.
    Ip,
}

impl Arrival {
    /// The arrival's name, as it is logged and tested.
    pub fn as_str(self) -> &'static str {
        match self {
            Arrival::Loopback => "loopback",
            Arrival::Ip => "ip",
        }
    }

    /// Whether a domain of this `reach` is reached by this arrival: a
    /// loopback arrival reaches loopback and ip domains, an ip arrival only
    /// ip domains, and a domain of no reach is reached by none.
    pub fn admitted_by(self, reach: Reach) -> bool {
        match (self, reach) {
            (_, Reach::None) => false,
            (Arrival::Loopback, Reach::Loopback | Reach::Ip) => true,
            (Arrival::Ip, Reach::Ip) => true,
            (Arrival::Ip, Reach::Loopback) => false,
        }
    }

    /// What a TCP listener bound to `addr` declares unless told otherwise:
    /// loopback for a loopback address, ip for any other.
    pub fn for_bind(addr: SocketAddr) -> Arrival {
        if addr.ip().is_loopback() {
            Arrival::Loopback
        } else {
            Arrival::Ip
        }
    }
}

/// The arrival the listener `request` came in on declared. A request that
/// carries none (a service called without a [`Server`](crate::Server)) is
/// [`Arrival::Ip`], the arrival that reaches least.
pub fn arrival_of<T>(request: &Request<T>) -> Arrival {
    request
        .extensions()
        .get::<Arrival>()
        .copied()
        .unwrap_or(Arrival::Ip)
}

#[cfg(test)]
#[path = "arrival.test.rs"]
mod tests;
