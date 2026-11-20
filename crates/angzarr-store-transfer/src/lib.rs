//! A store carried across a proto generation: every row a store keeps,
//! written as JSONL in proto3's JSON mapping, so a build whose protos are
//! renumbered can read it back by name (the store export/import plan,
//! session inept-sixth-stick). This build holds the EXPORT half; the
//! import, its generation mappings and its verification come with the
//! rewrite's build.
//!
//! # The format (a Rust contract, D7: not yet a spec document)
//!
//! One UTF-8 JSON object per line, each with exactly one key, its kind
//! ([`record::Record`]). The proto messages inside are proto3 JSON:
//! fields keyed by their lowerCamelCase names, enums by name, `uint64` as
//! a decimal string, `bytes` as base64, a `Timestamp` as RFC 3339, an
//! `Any` expanded (`{"@type": url, ...its fields}`), fields at their
//! defaults omitted (a page at sequence 0 has no `sequence`), and every
//! object's keys in sorted order, so one store always exports to the same
//! bytes.
//!
//! | Record | Shape | One per |
//! |---|---|---|
//! | `header` | `{"header": {"format": "angzarr-store-jsonl", "formatVersion": 1, "generation": "20261019", "store": "<backend>", "ledger": [1, ...]}}` | file, first line |
//! | `page` | `{"page": {"cover": Cover, "page": EventPage}}` | a stream's own page |
//! | `snapshot` | `{"snapshot": {"cover": Cover, "snapshot": Snapshot}}` | stream with a snapshot |
//! | `position` | `{"position": {"cover": Cover, "handler": "...", "next": "N"}}` | position above 0 |
//! | `deadLetter` | `{"deadLetter": {"cover": Cover, "at": {"sequence": "N"} or {"commandId": UUID}, "letter": AngzarrDeadLetter}}` | dead letter |
//! | `end` | `{"end": {"records": "N", "streams": [{"cover": Cover, "pages": "n", "head": "h", "snapshot": "s", "positions": "n", "deadLetters": "n"}]}}` | file, last line |
//!
//! - **The rows are in the store's [`Export`](angzarr_ports::Export) order**: one stream at a
//!   time, in `StreamRef`'s order (main before its editions), each its
//!   pages by sequence, its snapshot, its positions by handler, its dead
//!   letters in key order. The order across streams orders nothing.
//! - **`generation`** names the proto generation the JSON is shaped by:
//!   [`GENERATION`], the date of the spec draft this build's protos were
//!   cut from. An importer chooses its mapping by it.
//! - **`end`** holds the counts as the store reported them, so a
//!   truncated file is told from a whole one; `records` counts the lines
//!   between the header and it.
//! - **Not exported:** a store's own bookkeeping (its migration ledger's
//!   rows beyond their versions, the columns it copies out of a page). A
//!   pending outbox entry refuses the export (D5 (a)): deliver first.
//!
//! # The re-fold digest
//!
//! [`write_digest`] writes the other half of the crossing's check (the
//! plan's section 5.3.2): each build folds every stream of its own store
//! (the runtime's to do: angzarr-embedded's `Kernel::fold_digest`), and
//! writes one line per stream, `{"cover": Cover, "head": "h", "state":
//! Any}` ([`record::DigestLine`]), in the same JSON mapping, keys sorted.
//! The old build's digest of the source and the new build's of the import
//! must be the same bytes. A domain that takes no snapshot (a marked-up
//! aggregate with no `snapshot = ..`) has no form of its state to show:
//! its lines hold its cover and head alone, so its fold is not compared.
//!
//! # Refused, nothing written
//!
//! An export refuses ([`ExportError`]) a store whose ledger is not the
//! build's migration set, a pending outbox, a stored row that does not
//! re-encode to its own bytes (prost drops a field it does not know: the
//! row would lose it), an `Any` whose type no descriptor names, and a
//! message holding a field its descriptor does not know, at any depth.
//! Business payloads are expanded through the descriptors the app gives
//! ([`descriptor_pool`]), so the app's `FileDescriptorSet` is part of an
//! export (D3 (a)).

mod error;
mod export;
pub mod record;

pub use error::{ExportError, RowAt};
pub use export::{check_ledger, descriptor_pool, export, write_digest, Digested};

/// The ports and their wire types, as this crate uses them.
pub use angzarr_ports;
/// The descriptor pool type [`descriptor_pool`] answers.
pub use prost_reflect::DescriptorPool;

/// What the file is: a header's `format`.
pub const FORMAT: &str = "angzarr-store-jsonl";

/// The format's version: a header's `formatVersion`.
pub const FORMAT_VERSION: u32 = 1;

/// The proto generation this build's stored messages are shaped by: the
/// date of the spec draft its protos were cut from (`spec/20261019-draft`,
/// before the renumbering rewrite). A constant of the build, not a switch.
pub const GENERATION: &str = "20261019";

/// This crate's en-US Fluent catalog, its name first (its ids under the
/// crate's package name: `angzarr_diagnostic::build`).
pub const EN_US: (&str, &str) = (
    env!("CARGO_PKG_NAME"),
    include_str!(concat!(env!("OUT_DIR"), "/en-US.ftl")),
);

/// This crate's catalogs by locale, under the same name.
pub const LOCALES: (&str, &[(&str, &str)]) = (
    env!("CARGO_PKG_NAME"),
    include!(concat!(env!("OUT_DIR"), "/locales.rs")),
);

/// One value of every [`ExportError`] variant, each once, for the catalog
/// parity test.
pub fn diagnostic_samples() -> Vec<Box<dyn angzarr_diagnostic::Diagnostic>> {
    error::samples()
        .into_iter()
        .map(|e| Box::new(e) as Box<dyn angzarr_diagnostic::Diagnostic>)
        .collect()
}

#[cfg(test)]
#[path = "lib.test.rs"]
mod tests;
