//! The coordinators' gRPC server, shared by the embedded and the
//! distributed runtimes: the one core crate that links tonic and tokio.
//! This slice holds the listener and the status mapping; the services
//! (commands, queries, subscriptions) come in later slices, behind the
//! `commands` and `reads` features.
//!
//! - [`Server`]: the builder. Services are added once and served on every
//!   [`Listener`]; `grpc.health.v1.Health` is always served (SERVING once
//!   bound, NOT_SERVING when shutdown begins). [`Server::bind`] gives a
//!   [`Running`] server, its [`Bound`] endpoints reported, stopped by
//!   [`Running::shutdown`] or by dropping it.
//! - Listener policy. A Unix socket is mode 0600 in a directory of mode
//!   0700 that we own (created if missing); a stale socket file is replaced,
//!   a live one refused, a file that is not a socket never touched; the
//!   socket is removed on shutdown and on drop. TCP listens where it is
//!   told. The [`Arrival`] is declared per listener: loopback by default
//!   for a Unix socket, from the bind address for TCP (loopback bind:
//!   loopback, wider: ip); declaring loopback on a wider bind is a
//!   [`ListenError`] at bind, declaring wider than the bind is allowed (the
//!   container bridge is TCP declared [`Arrival::Ip`]). The arrival of a
//!   request is [`arrival_of`]; it is never derived from a peer address.
//! - [`status`]: a [`Coded`](angzarr_coordinator_base::Coded) or any
//!   [`Refusal`](angzarr_coordinator_base::Refusal) as a `tonic::Status`:
//!   its code, its key as the message, a `google.rpc.ErrorInfo` with the key
//!   as reason and the args as metadata; [`unauthenticated`] for a
//!   credential that is missing, stale or malformed.
//! - [`bearer`]: the bearer credential read from a request's metadata,
//!   opaque and redacted ([`Bearer`]).
//!
//! [`Arrival`] and [`Reach`] are stand-ins for the coordinator edge's types
//! (see their module): the edge crate is a sibling this one does not depend
//! on yet.

mod arrival;
mod bearer;
mod edge;
mod error;
mod server;
mod status;
#[cfg(unix)]
mod uds;

#[cfg(feature = "commands")]
mod command;
#[cfg(feature = "reads")]
mod query;

pub use arrival::{arrival_of, Arrival, Reach};
pub use bearer::{bearer, AuthorizationError, Bearer};
pub use edge::Edge;
pub use error::ListenError;
#[cfg(feature = "reads")]
pub use query::{Queries, ServedReads};
pub use server::{Bound, BoundEndpoint, Listener, Running, Server};
pub use status::{
    error_info, status, status_of, tonic_code, unauthenticated, ErrorInfo, ERROR_DOMAIN,
    ERROR_INFO_TYPE_URL,
};

/// This crate's en-US Fluent catalog, its name first: what a runtime that
/// shows these errors embeds to render them (`locales/en-US/messages.ftl`,
/// its ids under the crate's package name; `angzarr_diagnostic::build`).
pub const EN_US: (&str, &str) = (
    env!("CARGO_PKG_NAME"),
    include_str!(concat!(env!("OUT_DIR"), "/en-US.ftl")),
);

/// This crate's catalogs by locale, under the same name: every
/// `locales/<locale>/messages.ftl`. en-US alone ships.
pub const LOCALES: (&str, &[(&str, &str)]) = (
    env!("CARGO_PKG_NAME"),
    include!(concat!(env!("OUT_DIR"), "/locales.rs")),
);

/// One value of every error variant, each once, for the catalog parity
/// test: each one's code must have a message in every locale, using
/// exactly its args ([`angzarr_diagnostic::catalog::check`]).
pub fn diagnostic_samples() -> Vec<Box<dyn angzarr_diagnostic::Diagnostic>> {
    let listen = error::samples()
        .into_iter()
        .map(|e| Box::new(e) as Box<dyn angzarr_diagnostic::Diagnostic>);
    let authorization = bearer::samples()
        .into_iter()
        .map(|e| Box::new(e) as Box<dyn angzarr_diagnostic::Diagnostic>);
    listen.chain(authorization).collect()
}

/// The coordinators' shared vocabulary, as this crate uses it.
pub use angzarr_coordinator_base;
/// The diagnostic crate every error here implements, as this crate uses it.
pub use angzarr_diagnostic;
