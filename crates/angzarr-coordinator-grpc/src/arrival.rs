//! The arrival a request came in under, as its listener declared it.
//!
//! [`Arrival`] and [`Reach`] are the coordinator edge's own types (this
//! crate re-exports them): the same two arrivals and the same admission
//! rule the gate applies. [`arrival_for_bind`] is the listener's default.

use std::net::SocketAddr;

pub use angzarr_coordinator::{Arrival, Reach};
use tonic::Request;

/// What a TCP listener bound to `addr` declares unless told otherwise:
/// loopback for a loopback address, ip for any other.
pub fn arrival_for_bind(addr: SocketAddr) -> Arrival {
    if addr.ip().is_loopback() {
        Arrival::Loopback
    } else {
        Arrival::Ip
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
