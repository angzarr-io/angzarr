//! The coordinators' shared vocabulary, transport-free: no tonic, no tokio,
//! nothing above angzarr-ports (the crates that build on it are the reads
//! engine, the pipeline and the edge, each beside this one, never in it).
//!
//! - [`Code`] and [`Coded`]: a status, a KEY and an ARGUMENT MAP (decision
//!   14.37), the shape every refusal takes before it crosses a boundary. The
//!   status table is the router's, duplicated here so a read-only container
//!   needs no router; `tests/status_table.rs` holds it to a copy of the
//!   router's, kept as data. [`Refusal`] is what answers as one, and a
//!   [`StoreError`](angzarr_ports::StoreError) converts ([`store`]).
//! - [`Upcast`], the port a component's upcaster sits behind, on core's
//!   messages (`EventPage`, `Any`), with its two drivers
//!   [`upcast_pages`] and [`upcast_input`]; a sidecar that cannot be reached
//!   is `ANGZARR_REASON_UPCASTER_UNAVAILABLE`.
//! - [`Wait`]: the injected timer and jitter source a retry's backoff uses.
//! - [`Answered`]: what a coordinator answers for one command, the events
//!   and the [`DeliveryOutcome`](angzarr_proto::v1::DeliveryOutcome) (the
//!   distributed transport's `CommandResult`, without its transport).

mod answered;
mod code;
mod coded;
pub mod store;
mod upcast;
mod wait;

#[cfg(test)]
#[path = "support.test.rs"]
mod support;

pub use answered::Answered;
pub use code::Code;
pub use coded::{flat_args, Coded, Refusal};
pub use upcast::{upcast_input, upcast_pages, Input, Key, Upcast, UpcastError, Upcasted};
pub use wait::Wait;

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

/// One value of every [`UpcastError`] variant, each once, for the catalog
/// parity test: each one's code must have a message in every locale, using
/// exactly its args ([`angzarr_diagnostic::catalog::check`]).
pub fn diagnostic_samples() -> Vec<Box<dyn angzarr_diagnostic::Diagnostic>> {
    upcast::samples()
        .into_iter()
        .map(|e| Box::new(e) as Box<dyn angzarr_diagnostic::Diagnostic>)
        .collect()
}

/// The diagnostic crate every error here implements, as this crate uses it.
pub use angzarr_diagnostic;
/// The ports, as this crate uses them (`StoreError`, `BoxFuture`).
pub use angzarr_ports;
/// The wire types, as this crate uses them.
pub use angzarr_proto;
