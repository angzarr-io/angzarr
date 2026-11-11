//! The coordinators' gRPC server, shared by the embedded and the
//! distributed runtimes: the one core crate that links tonic and tokio.
//!
//! - [`Edge`]: an [`angzarr_coordinator::Coordinator`] (commands, reads and
//!   gate) served. With the `commands` feature it is a
//!   `CommandHandlerCoordinatorService` (`Handle` through the gate; the
//!   other RPCs are UNIMPLEMENTED, keyed); with `reads`, an
//!   `EventQueryService` (`GetEvents`; the other RPCs are UNIMPLEMENTED,
//!   keyed). On every request the arrival is the one its listener declared
//!   ([`arrival_of`]) and the credential is the `authorization` bearer
//!   ([`bearer`]), handed to the gate's `identify`; a refused request is the
//!   status the gate's contract names (UNAUTHENTICATED for a bad or revoked
//!   credential, the endpoint's own unknown-domain for anything else the
//!   gate forbids). [`Server::add_edge`] adds both services.
//! - With `subscribe` (off by default; it needs `reads`), an
//!   `EventStreamService`: `Subscribe` to ONE stream (`StreamFrom`), served
//!   by [`Server::add_event_stream`] for a read half that [`Follows`]. The
//!   gate is asked as for a follow read (`ReadOp::Follow`); the stream's
//!   pages from `from_sequence` (inclusive) are delivered through the
//!   GetEvents read path (catch-up), then every page committed after
//!   (live), with no gap and no duplicate: the subscription keeps the next
//!   sequence it owes, reads from it, and drops what it has delivered. It
//!   goes live on the store's commit push (`angzarr_ports::CommitNotify`,
//!   armed before every read) and polls the store's change token as the
//!   fallback for commits the push cannot see (another process's), every
//!   [`Follow::poll`]. The gate's `still_valid` is asked again every
//!   [`Follow::revalidate_every`] wakes, and a revoked caller's stream ends
//!   UNAUTHENTICATED. A stream the caller may not follow is answered as an
//!   absent one that never grows: the call is accepted, nothing is
//!   delivered, and the stream stays open until the caller leaves or is
//!   revoked. Each subscriber's server-side buffer holds at most
//!   [`Follow::buffer`] books; when it is full the subscription reads no
//!   further until the subscriber takes one (the store is the buffer:
//!   nothing is dropped, and a commit push is a flag, never queued). A
//!   connection closed under it (its age cap) ends the stream; the
//!   subscriber resumes from the last sequence it received + 1.
//!   `CorrelationFrom` is UNIMPLEMENTED.
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
//! - TLS (the `tls` feature, off by default; tonic's `tls-ring`, ring the one
//!   crypto provider): `Listener::tcp(addr).tls(ServerTlsConfig)` makes the
//!   server do the handshake on that listener. On a Unix socket it is
//!   [`ListenError::TlsOnUds`], and a config the TLS layer rejects is
//!   [`ListenError::TlsConfig`], both at bind. Without the feature, or
//!   without the option, a listener serves as it always did.
//! - Limits, per listener, each with a default (`Listener::DEFAULT_*`) and a
//!   builder method, on TCP (plain and TLS) and Unix sockets alike:
//!   `Listener::tls_handshake_timeout`
//!   (10 s, the `tls` feature; a client that never completes its handshake
//!   is dropped), [`Listener::keepalive`] (an HTTP/2 PING every 60 s, the
//!   connection closed when one goes unanswered for 20 s),
//!   [`Listener::max_connection_age`] with its mandatory, never-zero
//!   [`Listener::max_connection_age_grace`] (1 hour plus 30 s; tonic sends
//!   no GOAWAY and hard-closes the connection then, cutting calls in flight;
//!   a zero grace is [`ListenError::ZeroConnectionAgeGrace`] at bind, since
//!   tonic panics on an age without a grace),
//!   [`Listener::request_timeout`] (none: the client's `grpc-timeout`
//!   alone; when set, a handler slower than it is CANCELLED) and
//!   [`Listener::max_concurrent_streams`] (200 per connection, advertised in
//!   SETTINGS; a stream beyond it is REFUSED_STREAM). Against tonic's own
//!   defaults, what changes is: a handshake timeout, keepalive pings, a
//!   connection age, and a stream limit where tonic advertises none. Each is tonic's own knob.
//! - [`status`]: a [`Coded`](angzarr_coordinator_base::Coded) or any
//!   [`Refusal`](angzarr_coordinator_base::Refusal) as a `tonic::Status`:
//!   its code, its key as the message, a `google.rpc.ErrorInfo` with the key
//!   as reason and the args as metadata; [`unauthenticated`] for a
//!   credential that is malformed.
//! - [`bearer`]: the bearer credential read from a request's metadata,
//!   opaque and redacted ([`Bearer`]).
//!
//! [`Arrival`] and [`Reach`] are the coordinator edge's own types,
//! re-exported.

mod arrival;
mod bearer;
mod edge;
mod error;
mod limits;
mod server;
mod status;
#[cfg(unix)]
mod uds;

#[cfg(feature = "commands")]
mod command;
#[cfg(feature = "reads")]
mod query;
#[cfg(feature = "subscribe")]
mod subscribe;

pub use arrival::{arrival_for_bind, arrival_of, Arrival, Reach};
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
#[cfg(feature = "subscribe")]
pub use subscribe::{Follow, Follows};

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
