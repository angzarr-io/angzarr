//! Why a journal cannot be opened, what a store says when its journal fails
//! it, and the notice of a torn tail truncated; and this crate's catalog.

use std::fmt;
use std::path::{Path, PathBuf};

use angzarr_diagnostic::{code, Arg, Diagnostic};
use angzarr_ports::{Arg as PortArg, Cause, StoreError};

/// Why [`JsonlStore::open`](crate::JsonlStore::open) refused. The journal is
/// left as it was found, but for the lock file beside it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum OpenError {
    /// The journal's path is relative, which would land wherever the process
    /// happens to run.
    RelativePath { path: String },
    /// Another store (in this process or another) holds the journal: a
    /// journal has one store at a time.
    InUse { path: String },
    /// The lock beside the journal could not be taken, for another reason
    /// than being held.
    Lock { path: String, ext_err_txt: String },
    /// The journal could not be read, made or truncated.
    Io { path: String, ext_err_txt: String },
    /// A line of the journal is not an entry of this format. Not repaired:
    /// only a torn last line is, and a line with its line feed is not torn.
    Corrupt {
        path: String,
        line: usize,
        reason: String,
    },
    /// The journal's first line is not its header: this file is not a
    /// journal of this store.
    NotAJournal { path: String },
    /// The journal's header names a format this build does not read.
    UnsupportedFormat {
        path: String,
        found: u32,
        supported: u32,
    },
    /// The journal records an operation the store refused on replay, or
    /// one that is no operation: it was not written by this store, or was
    /// edited.
    ReplayRefused {
        path: String,
        line: usize,
        reason: String,
    },
}

impl OpenError {
    pub(crate) fn io(path: &Path, error: impl fmt::Display) -> OpenError {
        OpenError::Io {
            path: path.display().to_string(),
            ext_err_txt: error.to_string(),
        }
    }
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for OpenError {}

impl Diagnostic for OpenError {
    fn code(&self) -> &str {
        match self {
            OpenError::RelativePath { .. } => code!("relative-path"),
            OpenError::InUse { .. } => code!("in-use"),
            OpenError::Lock { .. } => code!("lock-failed"),
            OpenError::Io { .. } => code!("io"),
            OpenError::Corrupt { .. } => code!("corrupt"),
            OpenError::NotAJournal { .. } => code!("not-a-journal"),
            OpenError::UnsupportedFormat { .. } => code!("unsupported-format"),
            OpenError::ReplayRefused { .. } => code!("replay-refused"),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        let s = |v: &String| Arg::from(v.as_str());
        match self {
            OpenError::RelativePath { path }
            | OpenError::InUse { path }
            | OpenError::NotAJournal { path } => vec![("path", s(path))],
            OpenError::Lock { path, ext_err_txt } | OpenError::Io { path, ext_err_txt } => {
                vec![("path", s(path)), ("ext_err_txt", s(ext_err_txt))]
            }
            OpenError::Corrupt { path, line, reason }
            | OpenError::ReplayRefused { path, line, reason } => vec![
                ("path", s(path)),
                ("line", Arg::from(*line)),
                ("reason", s(reason)),
            ],
            OpenError::UnsupportedFormat {
                path,
                found,
                supported,
            } => vec![
                ("path", s(path)),
                ("found", Arg::from(u64::from(*found))),
                ("supported", Arg::from(u64::from(*supported))),
            ],
        }
    }
}

/// A notice, not an error: opening found the journal's last line cut short
/// (the write that made it never returned, so it was never acknowledged) and
/// removed it. Nothing before it was touched.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TornTail {
    pub path: PathBuf,
    /// The torn line's number, 1-based.
    pub line: usize,
    /// How many bytes were removed.
    pub bytes: usize,
}

impl fmt::Display for TornTail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for TornTail {
    fn code(&self) -> &str {
        code!("torn-tail-truncated")
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![
            ("path", Arg::from(self.path.display().to_string())),
            ("line", Arg::from(self.line)),
            ("bytes", Arg::from(self.bytes)),
        ]
    }
}

/// Why an operation on an open store failed in the journal, not in the
/// store's rules. Reaches a caller as
/// [`StoreError::Unavailable`], this value's code and args as its cause.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Failure {
    /// The entry could not be written as a line, so the operation was not
    /// attempted. The store is unchanged.
    EncodeFailed { ext_err_txt: String },
    /// The operation was accepted in memory but its line could not be
    /// appended or synced, so whether it is in the journal is unknown. The
    /// store refuses everything from now on: open the journal again to see
    /// what it holds.
    AppendFailed { ext_err_txt: String },
    /// An earlier append failed: the store's memory may be ahead of its
    /// journal, so it answers nothing until the journal is opened again.
    Poisoned,
    /// The in-memory store did not complete at once, which it always does:
    /// a bug, not a state of the journal.
    Suspended,
}

impl Failure {
    /// The error a port method answers: the store is unavailable, for this
    /// reason.
    pub fn into_store_error(self) -> StoreError {
        let args = self
            .texts()
            .into_iter()
            .map(|(name, text)| (name, PortArg::Str(text)));
        StoreError::Unavailable(Cause::new(self.static_code(), args))
    }

    fn static_code(&self) -> &'static str {
        match self {
            Failure::EncodeFailed { .. } => code!("encode-failed"),
            Failure::AppendFailed { .. } => code!("append-failed"),
            Failure::Poisoned => code!("poisoned"),
            Failure::Suspended => code!("suspended"),
        }
    }

    /// The args, all text.
    fn texts(&self) -> Vec<(&'static str, String)> {
        match self {
            Failure::EncodeFailed { ext_err_txt } | Failure::AppendFailed { ext_err_txt } => {
                vec![("ext_err_txt", ext_err_txt.clone())]
            }
            Failure::Poisoned | Failure::Suspended => vec![],
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for Failure {}

impl Diagnostic for Failure {
    fn code(&self) -> &str {
        self.static_code()
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        self.texts()
            .into_iter()
            .map(|(name, text)| (name, Arg::Str(text)))
            .collect()
    }
}

/// This crate's en-US messages, its name first: what an edge's catalog
/// embeds to render its diagnostics. The text of `locales/en-US/messages.ftl`,
/// its ids under the crate's package name (`angzarr_diagnostic::build`).
pub const EN_US: (&str, &str) = (
    env!("CARGO_PKG_NAME"),
    include_str!(concat!(env!("OUT_DIR"), "/en-US.ftl")),
);

/// One value of every diagnostic this crate defines, for the catalog test.
#[cfg(any(test, feature = "test-support"))]
pub fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    let path = || "/home/u/data/store.jsonl".to_string();
    let text = |s: &str| s.to_string();
    vec![
        Box::new(OpenError::RelativePath {
            path: text("data/store.jsonl"),
        }),
        Box::new(OpenError::InUse { path: path() }),
        Box::new(OpenError::Lock {
            path: path(),
            ext_err_txt: text("Permission denied (os error 13)"),
        }),
        Box::new(OpenError::Io {
            path: path(),
            ext_err_txt: text("No space left on device (os error 28)"),
        }),
        Box::new(OpenError::Corrupt {
            path: path(),
            line: 7,
            reason: text("store-jsonl-codec: malformed line"),
        }),
        Box::new(OpenError::NotAJournal { path: path() }),
        Box::new(OpenError::UnsupportedFormat {
            path: path(),
            found: 3,
            supported: 2,
        }),
        Box::new(OpenError::ReplayRefused {
            path: path(),
            line: 12,
            reason: text("angzarr-ports-store-conflict"),
        }),
        Box::new(TornTail {
            path: PathBuf::from(path()),
            line: 31,
            bytes: 118,
        }),
        Box::new(Failure::EncodeFailed {
            ext_err_txt: text("an Any whose type the pool lacks"),
        }),
        Box::new(Failure::AppendFailed {
            ext_err_txt: text("No space left on device (os error 28)"),
        }),
        Box::new(Failure::Poisoned),
        Box::new(Failure::Suspended),
    ]
}

#[cfg(test)]
#[path = "error.test.rs"]
mod tests;
