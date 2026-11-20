//! The coordinators' read engine, transport-free. [`Reads`] answers the
//! four reads of the query service (GetEvents, GetEventBook,
//! GetCorrelatedEvents, ListStreams) over the store's read ports alone
//! ([`ReadPorts`]: `EventRead`, `ReadByCorrelation`, `ListStreams`,
//! `EditionKnown`, with `ReadUntilTime` optional), in core's messages: a
//! frontend maps its transport's request onto [`Query`] and [`Select`], and
//! the answer onto its transport.
//!
//! **No command code.** This crate links angzarr-coordinator-base, ports,
//! proto and diagnostic, and nothing above them: no pipeline, no edge, no
//! tonic, no tokio, no router (`tests/dependencies.rs` pins it), so a
//! read-only container links it alone. It never writes: a store handle that
//! can only read serves every read.
//!
//! - [`Select`]: all, a range, a set of sequences, as of a sequence or a
//!   time, and an optional correlation.
//! - Cover validation, before any store call: a malformed cover or
//!   correlation is [`Invalid`], INVALID_ARGUMENT.
//! - An unknown edition is NOT_FOUND (`EditionKnown`), asked before any read.
//! - [`Visible`]: a stream the caller may not read answers as an absent one,
//!   empty at 0, with no store call; a list or a correlation's books leave
//!   it out.
//! - [`Limits`]: GetEvents answers books of bounded size; GetEventBook is
//!   one book, or RESOURCE_EXHAUSTED.
//! - An optional [`Upcast`](angzarr_coordinator_base::Upcast) port: every
//!   page answered is upcast first. No snapshots: this engine never
//!   returns one, so every answer carries every page from the start of the
//!   selection.

mod error;
mod limits;
mod reads;
mod select;
mod visible;

#[cfg(test)]
#[path = "support.test.rs"]
mod support;

pub use error::{Invalid, ReadError};
pub use limits::Limits;
pub use reads::{Query, ReadPorts, Reads};
pub use select::Select;
pub use visible::Visible;

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

/// One value of every [`Invalid`] variant, each once, for the catalog parity
/// test: each one's code must have a message in every locale, using exactly
/// its args ([`angzarr_diagnostic::catalog::check`]).
pub fn diagnostic_samples() -> Vec<Box<dyn angzarr_diagnostic::Diagnostic>> {
    error::samples()
        .into_iter()
        .map(|e| Box::new(e) as Box<dyn angzarr_diagnostic::Diagnostic>)
        .collect()
}

/// The base vocabulary (`Coded`, `Refusal`, `Upcast`), as this crate uses it.
pub use angzarr_coordinator_base;
/// The diagnostic crate every error here implements, as this crate uses it.
pub use angzarr_diagnostic;
/// The ports, as this crate uses them.
pub use angzarr_ports;
/// The wire types, as this crate uses them.
pub use angzarr_proto;
