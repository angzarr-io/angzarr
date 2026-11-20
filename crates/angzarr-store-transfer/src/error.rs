//! Why an export refuses: a code and its args (angzarr decision 14.37),
//! each rendered from the crate's catalog.

use std::fmt;

use angzarr_diagnostic::{code, Arg, Diagnostic};
use angzarr_ports::StoreError;

/// Which stored row a refusal is about: its kind, its stream and where on
/// the stream (a page's sequence, a position's handler, a letter's
/// place), each as data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowAt {
    /// `page`, `snapshot`, `position` or `dead-letter`.
    pub kind: &'static str,
    /// The stream: `domain:edition:root`, the edition empty for main.
    pub stream: String,
    /// Where on the stream: a page's sequence, a letter's sequence or
    /// command id, a position's handler; empty for a snapshot.
    pub place: String,
}

/// Why an export refuses. Nothing is written when it does.
#[derive(Clone, Debug, PartialEq)]
pub enum ExportError {
    /// The store answered an error.
    Store(StoreError),
    /// The store's ledger is not exactly the build's migration set: a
    /// store older or newer than the build is not this build's shape.
    LedgerNotTheBuilds { applied: String, build: String },
    /// The outbox holds pending entries: deliver them first (the plan's
    /// D5 (a)).
    PendingOutbox { entries: u64 },
    /// A stored row that does not decode as its message.
    Undecodable { row: RowAt, message: String },
    /// A stored row that decodes, but re-encodes to other bytes: it holds
    /// a field this build does not know, or a non-canonical encoding.
    ReencodingDiffers { row: RowAt, message: String },
    /// An `Any` whose type the descriptors cannot resolve.
    UnresolvedType { row: RowAt, type_url: String },
    /// A message holding fields its descriptor does not know, at `path`
    /// (field names from the row's message down, `.`-separated).
    UnknownFields {
        row: RowAt,
        message: String,
        path: String,
    },
    /// A message proto3's JSON mapping could not write.
    Unwritable { row: RowAt, message: String },
    /// The descriptors lack a message the format writes.
    MissingDescriptor { message: String },
    /// A descriptor set that does not decode, or does not join the pool
    /// (its index among those given; 0 is angzarr's own).
    DescriptorSet { index: u64 },
    /// The file could not be written: the I/O error's kind.
    Write { kind: String },
}

impl fmt::Display for ExportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for ExportError {}

impl From<StoreError> for ExportError {
    fn from(e: StoreError) -> ExportError {
        ExportError::Store(e)
    }
}

/// A row's args, under the names every row-error uses.
fn row_args(row: &RowAt) -> [(&'static str, Arg); 3] {
    [
        ("kind", Arg::from(row.kind)),
        ("stream", Arg::from(row.stream.as_str())),
        ("place", Arg::from(row.place.as_str())),
    ]
}

impl Diagnostic for ExportError {
    fn code(&self) -> &str {
        match self {
            ExportError::Store(_) => code!("store"),
            ExportError::LedgerNotTheBuilds { .. } => code!("ledger-not-the-builds"),
            ExportError::PendingOutbox { .. } => code!("pending-outbox"),
            ExportError::Undecodable { .. } => code!("undecodable"),
            ExportError::ReencodingDiffers { .. } => code!("reencoding-differs"),
            ExportError::UnresolvedType { .. } => code!("unresolved-type"),
            ExportError::UnknownFields { .. } => code!("unknown-fields"),
            ExportError::Unwritable { .. } => code!("unwritable"),
            ExportError::MissingDescriptor { .. } => code!("missing-descriptor"),
            ExportError::DescriptorSet { .. } => code!("descriptor-set"),
            ExportError::Write { .. } => code!("write"),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            ExportError::Store(e) => vec![("cause", Arg::nested(e))],
            ExportError::LedgerNotTheBuilds { applied, build } => vec![
                ("applied", Arg::from(applied.as_str())),
                ("build", Arg::from(build.as_str())),
            ],
            ExportError::PendingOutbox { entries } => vec![("entries", Arg::from(*entries))],
            ExportError::Undecodable { row, message }
            | ExportError::ReencodingDiffers { row, message }
            | ExportError::Unwritable { row, message } => {
                let mut args = Vec::from(row_args(row));
                args.push(("message", Arg::from(message.as_str())));
                args
            }
            ExportError::UnresolvedType { row, type_url } => {
                let mut args = Vec::from(row_args(row));
                args.push(("type_url", Arg::from(type_url.as_str())));
                args
            }
            ExportError::UnknownFields { row, message, path } => {
                let mut args = Vec::from(row_args(row));
                args.push(("message", Arg::from(message.as_str())));
                args.push(("path", Arg::from(path.as_str())));
                args
            }
            ExportError::MissingDescriptor { message } => {
                vec![("message", Arg::from(message.as_str()))]
            }
            ExportError::DescriptorSet { index } => vec![("index", Arg::from(*index))],
            ExportError::Write { kind } => vec![("kind", Arg::from(kind.as_str()))],
        }
    }
}

/// One value of every variant, each once, for the catalog parity test.
pub(crate) fn samples() -> Vec<ExportError> {
    let row = RowAt {
        kind: "page",
        stream: "tasks::00000000-0000-0000-0000-000000000000".to_string(),
        place: "3".to_string(),
    };
    vec![
        ExportError::Store(StoreError::EditionNameRequired),
        ExportError::LedgerNotTheBuilds {
            applied: "1,2".to_string(),
            build: "1,2,3".to_string(),
        },
        ExportError::PendingOutbox { entries: 2 },
        ExportError::Undecodable {
            row: row.clone(),
            message: "io.angzarr.v1.EventPage".to_string(),
        },
        ExportError::ReencodingDiffers {
            row: row.clone(),
            message: "io.angzarr.v1.EventPage".to_string(),
        },
        ExportError::UnresolvedType {
            row: row.clone(),
            type_url: "/nowhere.v1.Unknown".to_string(),
        },
        ExportError::UnknownFields {
            row: row.clone(),
            message: "io.angzarr.v1.FactOrigin".to_string(),
            path: "payload".to_string(),
        },
        ExportError::Unwritable {
            row,
            message: "io.angzarr.v1.EventPage".to_string(),
        },
        ExportError::MissingDescriptor {
            message: "io.angzarr.v1.Cover".to_string(),
        },
        ExportError::DescriptorSet { index: 1 },
        ExportError::Write {
            kind: "BrokenPipe".to_string(),
        },
    ]
}
