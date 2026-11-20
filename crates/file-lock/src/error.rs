//! Why a lock could not be taken; and this crate's catalog.

use std::fmt;
use std::path::Path;

use angzarr_diagnostic::{code, Arg, Diagnostic};

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum LockError {
    /// A lock file, or its directory, could not be made or opened.
    Io { path: String, detail: String },
    /// A lock path that is relative, which would land wherever the process
    /// happens to run.
    RelativePath { path: String },
    /// The lock path is something other than a regular file (a FIFO, a
    /// directory): locking it is not locking at all.
    NotRegular { path: String },
    /// The lock was held by another process for longer than the wait.
    Timeout { path: String, waited_ms: u64 },
    /// Each lock taken was on a lock file unlinked meanwhile.
    Replaced { path: String, attempts: u32 },
}

impl LockError {
    pub(crate) fn io(path: &Path, error: impl fmt::Display) -> LockError {
        LockError::Io {
            path: path.display().to_string(),
            detail: error.to_string(),
        }
    }
}

impl fmt::Display for LockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for LockError {}

impl Diagnostic for LockError {
    fn code(&self) -> &str {
        match self {
            LockError::Io { .. } => code!("io"),
            LockError::RelativePath { .. } => code!("relative-path"),
            LockError::NotRegular { .. } => code!("not-regular"),
            LockError::Timeout { .. } => code!("timeout"),
            LockError::Replaced { .. } => code!("replaced"),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        let s = |v: &String| Arg::from(v.as_str());
        match self {
            LockError::Io { path, detail } => vec![("path", s(path)), ("ext_err_txt", s(detail))],
            LockError::RelativePath { path } | LockError::NotRegular { path } => {
                vec![("path", s(path))]
            }
            LockError::Timeout { path, waited_ms } => {
                vec![("path", s(path)), ("waited_ms", Arg::from(*waited_ms))]
            }
            LockError::Replaced { path, attempts } => {
                vec![("path", s(path)), ("attempts", Arg::from(*attempts))]
            }
        }
    }
}

/// This crate's en-US messages, its name first: what an edge's catalog
/// embeds to render its diagnostics. The text of `locales/en-US/messages.ftl`,
/// its ids under the crate's package name (`angzarr_diagnostic::build`).
pub const EN_US: (&str, &str) = (
    env!("CARGO_PKG_NAME"),
    include_str!(concat!(env!("OUT_DIR"), "/en-US.ftl")),
);

/// One value of every error this crate defines, for the catalog test.
#[cfg(any(test, feature = "test-support"))]
pub fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    let path = || "/home/u/.ctxloom/locks/u__.claude.json__d07667d34aae78ed.lock".to_string();
    vec![
        Box::new(LockError::Io {
            path: path(),
            detail: "Permission denied (os error 13)".to_string(),
        }),
        Box::new(LockError::RelativePath {
            path: "locks/x.lock".to_string(),
        }),
        Box::new(LockError::NotRegular { path: path() }),
        Box::new(LockError::Timeout {
            path: path(),
            waited_ms: 10_000,
        }),
        Box::new(LockError::Replaced {
            path: path(),
            attempts: 16,
        }),
    ]
}

#[cfg(test)]
#[path = "error.test.rs"]
mod tests;
