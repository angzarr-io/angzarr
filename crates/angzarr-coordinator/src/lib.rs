//! The coordinators' edge, transport-free: no tonic, no tokio. (The word
//! "coordinator" here is the serving edge of a runtime; the embedded
//! kernel's private `coordinator` module is its in-process pipeline.)
//!
//! - Routing and reach: [`Arrival`] (where a request came from), [`Reach`]
//!   (how far a domain is served), [`Served`] (what a frontend says per
//!   domain) and [`Hidden`] (why a domain was answered as an unknown one:
//!   the log's, never the caller's).
//! - With the `commands` feature: [`Commands`] (what a frontend implements
//!   to take commands), [`Caller`], [`Unserved`], [`NoCommands`].
//! - With the `reads` feature: [`NoReads`], the typed absent read half, and
//!   the reads engine (`angzarr_reads`).
//! - The gate, in every build: [`Gate`] (what the application implements:
//!   `identify`, `owns`, an optional `stamp`, a defaulted `still_valid`),
//!   [`Credential`], [`Principal`] with its closed [`Allowlist`], [`Denied`],
//!   [`AuditSink`] and [`Trusted`]; and [`Coordinator`], the commands, the
//!   reads and the gate composed, whose [`Session`] runs every request in
//!   one order: identify, operation allowed, reach, owns every stream named,
//!   stamp, handle. A refusal after identify is answered as the endpoint
//!   answers a domain it does not serve, its real reason going to the
//!   [`AuditSink`]; only a bad credential and a client's `deliver` are shown.
//!
//! A read-only container builds with `default-features = false, features =
//! ["reads"]` and links no command code.

mod allowlist;
mod coordinator;
mod credential;
mod denied;
mod gate;
mod refusal;
mod routing;

#[cfg(test)]
#[path = "support.test.rs"]
mod support;

#[cfg(feature = "commands")]
mod commands;
#[cfg(feature = "reads")]
mod reads;

pub use allowlist::{Allowlist, BodyKind, ReadOp};
pub use coordinator::{Coordinator, Session};
pub use credential::{constant_time_eq, Credential, Scheme};
pub use denied::Denied;
pub use gate::{AuditSink, Gate, Principal, Trusted};
pub use refusal::{FrameworkOnly, Unauthenticated, Unimplemented};
pub use routing::{Arrival, Hidden, Reach, Served};

#[cfg(feature = "commands")]
pub use commands::{
    route_command, Caller, Commands, NoCommands, Synced, Unserved, COMMANDS_NOT_SERVED,
};
#[cfg(feature = "reads")]
pub use reads::NoReads;

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

/// One value of every error this crate defines, each once, for the catalog
/// parity test: each one's code must have a message in every locale, using
/// exactly its args ([`angzarr_diagnostic::catalog::check`]).
pub fn diagnostic_samples() -> Vec<Box<dyn angzarr_diagnostic::Diagnostic>> {
    let mut samples = routing::samples();
    samples.extend(refusal::samples());
    samples.extend(denied::samples());
    samples
}

/// The base vocabulary, as this crate uses it (`Coded`, `Refusal`, `Answered`).
pub use angzarr_coordinator_base;
/// The diagnostic crate every error here implements, as this crate uses it.
pub use angzarr_diagnostic;
/// The ports, as this crate uses them (`BoxFuture`).
pub use angzarr_ports;
/// The wire types, as this crate uses them.
pub use angzarr_proto;
/// The reads engine, as this crate uses it (`Reads`, `Visible`).
#[cfg(feature = "reads")]
pub use angzarr_reads;
