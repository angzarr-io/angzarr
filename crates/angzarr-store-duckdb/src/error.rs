//! Why DuckDB could not answer, and how that reaches the ports.
//!
//! In angzarr-ports' style, through its re-export of angzarr-diagnostic: every
//! error is a `Diagnostic`, a stable kebab-case `code` starting with this
//! crate's name and named `args` that carry data, and its `Display` is the
//! code then the args, never English. What the store answers through the
//! ports is angzarr-ports' [`StoreError`], whose `Unavailable` and
//! `Unreadable` carry this crate's failure as a [`Cause`]. A third party's
//! error (DuckDB's, prost's) is carried as it said it: its text verbatim as
//! `ext_err_txt`.

use std::fmt;

use angzarr_ports::angzarr_diagnostic::{self, code, Diagnostic};
use angzarr_ports::{Arg, Cause, StoreError};

/// The database's own failures, as causes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum DuckError {
    /// The database failed: the store could not say. `detail` is DuckDB's
    /// text, verbatim.
    Unavailable { detail: String },
    /// The file is open elsewhere: DuckDB lets one process own a database
    /// file for writing, and a second open of it is refused.
    HeldElsewhere { detail: String },
    /// A stored value's bytes do not decode as what was written: `detail`
    /// is the decoder's text, verbatim.
    Undecodable { detail: String },
    /// A stored root of `length` bytes, not a UUID's 16.
    BadRoot { length: usize },
    /// A dead letter's stored place, of `kind` and `length` bytes: neither
    /// a sequence (kind 0, eight bytes) nor a command id (kind 1, sixteen).
    BadDeadLetterPlace { kind: u64, length: usize },
    /// The file was written by a newer build: its format is `found`, and
    /// this build reads up to `supported`. Left untouched.
    NewerFormat { found: u64, supported: u64 },
    /// The file is a DuckDB database with tables, but not this store's.
    /// Left untouched.
    NotAStore { detail: String },
}

impl DuckError {
    pub fn code(&self) -> &'static str {
        match self {
            DuckError::Unavailable { .. } => code!("unavailable"),
            DuckError::HeldElsewhere { .. } => code!("held-elsewhere"),
            DuckError::Undecodable { .. } => code!("undecodable"),
            DuckError::BadRoot { .. } => code!("bad-root"),
            DuckError::BadDeadLetterPlace { .. } => code!("bad-dead-letter-place"),
            DuckError::NewerFormat { .. } => code!("newer-format"),
            DuckError::NotAStore { .. } => code!("not-a-store"),
        }
    }

    /// The message's args, by name.
    pub fn args(&self) -> Vec<(&'static str, Arg)> {
        let text = |detail: &str| ("ext_err_txt", Arg::Str(detail.to_string()));
        match self {
            DuckError::Unavailable { detail }
            | DuckError::HeldElsewhere { detail }
            | DuckError::Undecodable { detail }
            | DuckError::NotAStore { detail } => vec![text(detail)],
            DuckError::BadRoot { length } => vec![("length", Arg::Uint(*length as u64))],
            DuckError::BadDeadLetterPlace { kind, length } => vec![
                ("kind", Arg::Uint(*kind)),
                ("length", Arg::Uint(*length as u64)),
            ],
            DuckError::NewerFormat { found, supported } => vec![
                ("found", Arg::Uint(*found)),
                ("supported", Arg::Uint(*supported)),
            ],
        }
    }

    /// As a [`Cause`]: the code and the args.
    pub fn cause(&self) -> Cause {
        Cause::new(self.code(), self.args())
    }
}

/// `args` as angzarr-diagnostic's: angzarr-ports' `From`, each by name.
fn diagnostic_args(args: Vec<(&'static str, Arg)>) -> Vec<(&'static str, angzarr_diagnostic::Arg)> {
    args.into_iter()
        .map(|(name, arg)| (name, arg.into()))
        .collect()
}

impl Diagnostic for DuckError {
    fn code(&self) -> &str {
        DuckError::code(self)
    }

    fn args(&self) -> Vec<(&str, angzarr_diagnostic::Arg)> {
        diagnostic_args(DuckError::args(self))
    }
}

/// The code then the args: angzarr-diagnostic's display.
impl fmt::Display for DuckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for DuckError {}

/// One of each of this crate's errors, for the catalog test.
pub fn errors() -> Vec<DuckError> {
    let detail = || "x".to_string();
    Vec::from([
        DuckError::Unavailable { detail: detail() },
        DuckError::HeldElsewhere { detail: detail() },
        DuckError::Undecodable { detail: detail() },
        DuckError::BadRoot { length: 3 },
        DuckError::BadDeadLetterPlace { kind: 0, length: 2 },
        DuckError::NewerFormat {
            found: 9,
            supported: 1,
        },
        DuckError::NotAStore { detail: detail() },
    ])
}

/// Every cause this crate can put in a [`StoreError`], one each, for the
/// catalog test.
pub fn causes() -> Vec<Cause> {
    errors().iter().map(DuckError::cause).collect()
}

/// Whether DuckDB's text says the file is held by another owner: its
/// open refuses with "Could not set lock on file" naming the holder.
fn held(text: &str) -> bool {
    text.contains("Could not set lock") || text.contains("Conflicting lock")
}

/// The database's failure, as the port's: the store could not say
/// (`Recovery::Retry`), with DuckDB's text.
pub(crate) fn unavailable(error: duckdb::Error) -> StoreError {
    let detail = error.to_string();
    StoreError::Unavailable(DuckError::Unavailable { detail }.cause())
}

/// An open's failure: a lock another owner holds is its own cause, the
/// rest are the database's.
pub(crate) fn opening(error: duckdb::Error) -> StoreError {
    let detail = error.to_string();
    let error = if held(&detail) {
        DuckError::HeldElsewhere { detail }
    } else {
        DuckError::Unavailable { detail }
    };
    StoreError::Unavailable(error.cause())
}

/// A stored root of `length` bytes, not 16.
pub(crate) fn bad_root(length: usize) -> StoreError {
    StoreError::Unavailable(DuckError::BadRoot { length }.cause())
}

/// A stored value that does not decode (`Recovery::Bug`, as `Unreadable`),
/// with prost's text.
pub(crate) fn undecodable(error: prost::DecodeError) -> Cause {
    DuckError::Undecodable {
        detail: error.to_string(),
    }
    .cause()
}

#[cfg(test)]
#[path = "error.test.rs"]
mod tests;
