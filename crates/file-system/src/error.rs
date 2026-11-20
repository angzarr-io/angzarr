//! Why a filesystem has no working directory.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use angzarr_diagnostic::{code, Arg, Diagnostic};

/// The working directory the filesystem was made over could not be read
/// (it was deleted, or is not searchable): [`crate::Fs::cwd`] says so,
/// and a relative path, which has nothing to resolve against, fails with
/// it (an `io::Error` carrying this).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CwdError {
    /// Why it could not be read, as the OS said.
    pub reason: String,
}

impl CwdError {
    /// Why the working directory could not be read, as the OS said.
    pub(crate) fn of(why: &io::Error) -> CwdError {
        CwdError {
            reason: why.to_string(),
        }
    }

    /// This, as the error an operation on a relative path fails with.
    pub(crate) fn to_io(&self) -> io::Error {
        io::Error::new(io::ErrorKind::NotFound, self.clone())
    }
}

impl fmt::Display for CwdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for CwdError {}

impl Diagnostic for CwdError {
    fn code(&self) -> &str {
        code!("no-cwd")
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![("reason", self.reason.as_str().into())]
    }
}

/// A rename that must replace nothing ([`crate::Fs::rename_noreplace`])
/// found something at its target, which it left as it was: the error kind
/// is `AlreadyExists`, and this the error it carries.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TargetExists {
    /// The target, the caller's path.
    pub path: PathBuf,
}

impl TargetExists {
    /// The [`TargetExists`] `error` carries, if it is one.
    pub fn of(error: &io::Error) -> Option<&TargetExists> {
        error.get_ref()?.downcast_ref()
    }

    /// `error` as the caller's, at `to`: an `AlreadyExists` becomes this,
    /// any other error is left as it is.
    pub(crate) fn at(error: io::Error, to: &Path) -> io::Error {
        if error.kind() == io::ErrorKind::AlreadyExists {
            let exists = TargetExists {
                path: to.to_path_buf(),
            };
            return io::Error::new(io::ErrorKind::AlreadyExists, exists);
        }
        error
    }
}

impl fmt::Display for TargetExists {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for TargetExists {}

impl Diagnostic for TargetExists {
    fn code(&self) -> &str {
        code!("target-exists")
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![("path", self.path.display().to_string().into())]
    }
}

/// This crate's en-US messages, its name first: what an edge's catalog
/// embeds to render its diagnostics. The text of `locales/en-US/messages.ftl`,
/// its ids under the crate's package name (`angzarr_diagnostic::build`).
pub const EN_US: (&str, &str) = (
    env!("CARGO_PKG_NAME"),
    include_str!(concat!(env!("OUT_DIR"), "/en-US.ftl")),
);

/// One value of every error, for the catalog test.
#[cfg(any(test, feature = "test-support"))]
pub fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    vec![
        Box::new(CwdError {
            reason: "No such file or directory (os error 2)".to_string(),
        }),
        Box::new(TargetExists {
            path: PathBuf::from("/work/to"),
        }),
    ]
}

#[cfg(test)]
#[path = "error.test.rs"]
mod tests;
