//! Presentation at the driving edge, for any domain: a command line, a
//! server. The core hands up typed values; this crate turns them into one
//! of two formats, [`Format::Json`] for jq and other programs and
//! [`Format::Text`] (UTF-8) for a person, and renders diagnostics from
//! Fluent catalogs.
//!
//! It is domain-free (no crate of angzarr-core's workspace but
//! `angzarr-diagnostic`; its tests hold it to that), so any binary reuses
//! it. A domain's shapes and views live with that domain's driving adapter
//! and implement [`Render`].
//!
//! - [`Format`]: which format, from flags and whether stdout is a terminal.
//! - [`render`] and [`write_json`]: one JSON document per call, fields in a
//!   fixed order; [`write_json_line`], one line of a JSON Lines stream.
//! - [`Catalog`]: a diagnostic's message from each crate's embedded
//!   catalogs, and its JSON [`Envelope`] with nested causes.
//!   [`Catalog::en_us`] takes each crate's `EN_US` ([`Source`]);
//!   [`Catalog::for_locale`] takes each crate's `LOCALES` ([`Locales`],
//!   every `locales/<locale>/messages.ftl` it has, from the table its build
//!   writes), serves the requested locale from the nearest one offered
//!   (Fluent's negotiation), and falls back to en-US message by message.
//!   Asked for en-US it is [`Catalog::en_us`]. Only en-US catalogs ship.
//! - [`locale`]: the locale a person asked for, [`locale::detect`]ed from
//!   `LC_ALL`, `LC_MESSAGES` and `LANG` as the binary passes them in
//!   (`locale::detect(|name| std::env::var(name).ok())`; nothing here reads
//!   the environment), and en-US, [`locale::fallback`].
//! - [`text`]: display-width columns and headlines.
//! - `english` (behind the `test-support` feature): a diagnostic's en-US
//!   message from catalogs, for the tests that hold its English to an
//!   expected text (`english_for!`).
//!
//! Output goes to an injected `&mut dyn Write`, never straight to stdout.

mod catalog;
#[cfg(any(test, feature = "test-support"))]
pub mod english;
mod envelope;
mod error;
mod format;
pub mod locale;
pub mod text;

use std::io::{self, Write};

use serde::Serialize;

pub use catalog::{Catalog, Locales, Source};
pub use envelope::Envelope;
#[cfg(any(test, feature = "test-support"))]
pub use error::diagnostic_samples;
pub use error::{CatalogError, UnknownFormat, Unrendered, EN_US, LOCALES};
pub use format::Format;
pub use unic_langid::LanguageIdentifier;

/// A value a driving adapter answers with, in both formats.
pub trait Render {
    /// The value's JSON shape: stable field names in a fixed order, absent
    /// values explicit (`[]`, `""`, `null`), so a jq filter holds between
    /// calls.
    fn json(&self) -> impl Serialize + '_;

    /// The value as a person reads it, in UTF-8.
    fn text(&self, out: &mut dyn Write) -> io::Result<()>;
}

/// Write `view` to `out` in `format`.
pub fn render(out: &mut dyn Write, format: Format, view: &impl Render) -> io::Result<()> {
    match format {
        Format::Text => view.text(out),
        Format::Json => write_json(out, &view.json()),
    }
}

/// `value` as one indented JSON document and a newline.
pub fn write_json<T: Serialize + ?Sized>(out: &mut dyn Write, value: &T) -> io::Result<()> {
    serde_json::to_writer_pretty(&mut *out, value)?;
    writeln!(out)
}

/// `value` as one line of a JSON Lines stream (one compact document and a
/// newline), flushed, so a reader following the stream sees it at once.
pub fn write_json_line<T: Serialize + ?Sized>(out: &mut dyn Write, value: &T) -> io::Result<()> {
    serde_json::to_writer(&mut *out, value)?;
    writeln!(out)?;
    out.flush()
}

#[cfg(test)]
#[path = "lib.test.rs"]
mod tests;
