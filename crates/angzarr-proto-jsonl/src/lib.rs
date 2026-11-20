//! Protobuf messages as JSON Lines: one message per line, in the canonical
//! proto3 JSON mapping (64-bit integers as strings, bytes as base64, enums
//! by name, the well-known types per the spec), through prost-reflect's
//! `DynamicMessage`.
//!
//! A pure codec: no I/O of its own (a [`Writer`] and a [`Reader`] borrow the
//! caller's `Write` and `BufRead`), no clock, no globals. The caller
//! supplies the descriptors: a line carries no type tag, so a [`Reader`] is
//! told the type it decodes.
//!
//! - **Compact and deterministic.** A line holds no line feed (a string's is
//!   escaped), and the same message always encodes to the same bytes: object
//!   keys are sorted, map entries included (prost-reflect's own order for
//!   a map is a hash order).
//! - **Strict.** A line that names a field the type lacks, holds a value of
//!   the wrong type, or has text after the message is refused, as
//!   angzarr-yaml's decode is.
//! - **Errors** are [`Error`]s, localizable (`Diagnostic`), with the
//!   catalog in [`EN_US`].

mod error;
mod line;
mod reader;
mod writer;

pub use error::{Error, Problem};
pub use line::{decode_line, encode_line};
pub use reader::Reader;
pub use writer::Writer;

/// This crate's en-US Fluent catalog, its name first: what a dependent
/// embeds to render this crate's diagnostics. The text of
/// `locales/en-US/messages.ftl`, its ids under the crate's package name
/// (`angzarr_diagnostic::build`).
pub const EN_US: (&str, &str) = (
    env!("CARGO_PKG_NAME"),
    include_str!(concat!(env!("OUT_DIR"), "/en-US.ftl")),
);

/// This crate's catalogs by locale, under the same name: every
/// `locales/<locale>/messages.ftl`, from the table the build writes
/// (`angzarr_diagnostic::build`). en-US alone ships.
pub const LOCALES: (&str, &[(&str, &str)]) = (
    env!("CARGO_PKG_NAME"),
    include!(concat!(env!("OUT_DIR"), "/locales.rs")),
);

/// One value of every error this crate defines, each variant once, for the
/// catalog parity test: each one's code must have a message in every locale,
/// using exactly its args. Its messages are in [`EN_US`].
pub fn diagnostic_samples() -> Vec<Box<dyn angzarr_diagnostic::Diagnostic>> {
    let text = |s: &str| s.to_string();
    [
        Problem::EncodeFailed {
            ext_err_txt: text("unknown type"),
        },
        Problem::MalformedLine {
            ext_err_txt: text("expected value"),
        },
        Problem::BlankLine,
        Problem::NotOneLine,
        Problem::NotUtf8,
        Problem::TornTail { bytes: 2 },
        Problem::ReadFailed {
            ext_err_txt: text("broken pipe"),
        },
        Problem::WriteFailed {
            ext_err_txt: text("disk full"),
        },
    ]
    .into_iter()
    .map(|problem| Box::new(Error { line: 3, problem }) as _)
    .collect()
}

#[cfg(test)]
#[path = "fixture.test.rs"]
mod fixture;

#[cfg(test)]
#[path = "lib.test.rs"]
mod tests;
