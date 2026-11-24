//! Why a project's store could not be opened, beyond what the store reports
//! itself. A third party's text (the OS's, through std's `io::Error`) is the
//! arg `ext_err_txt`, verbatim.

use std::fmt;

use angzarr_diagnostic::{code, Arg, Diagnostic};
use angzarr_ports::{self, StoreError};

/// This crate's en-US messages, its name first: what an edge's catalog
/// embeds to render its diagnostics.
pub const EN_US: (&str, &str) = (
    env!("CARGO_PKG_NAME"),
    include_str!(concat!(env!("OUT_DIR"), "/en-US.ftl")),
);

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum OpenError {
    /// The directory the database goes in could not be made.
    CreateDir { path: String, detail: String },
    /// The filesystem could not say where the database is on disk, for
    /// SQLite to open.
    Unplaced { path: String, detail: String },
    /// The store at `path` was written before the embedded store existed
    /// (an earlier event store's): it is not migrated in place, but moved
    /// aside to `backup` and started afresh.
    PredatesEmbedded { path: String, backup: String },
    /// The store's directory, file, log or shared memory at `path` could not
    /// be made its owner's alone (0700, 0600), so it is not opened.
    Restrict { path: String, detail: String },
    /// The store itself refused or failed, its error as it reports it (its
    /// code, its args, its catalog).
    Store(StoreError),
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for OpenError {}

impl OpenError {
    /// The error as the store's own error: the store's error as it was, or
    /// this one carried in `Unavailable` as a cause, its code and args, for
    /// a caller whose port speaks `StoreError`.
    pub fn into_store_error(self) -> StoreError {
        use angzarr_ports::{Arg as PortArg, Cause};
        let text = PortArg::Str;
        let cause = match self {
            OpenError::Store(error) => return error,
            OpenError::CreateDir { path, detail } => Cause::new(
                code!("create-dir"),
                [("path", text(path)), ("ext_err_txt", text(detail))],
            ),
            OpenError::Unplaced { path, detail } => Cause::new(
                code!("unplaced"),
                [("path", text(path)), ("ext_err_txt", text(detail))],
            ),
            OpenError::PredatesEmbedded { path, backup } => Cause::new(
                code!("predates-embedded"),
                [("path", text(path)), ("backup", text(backup))],
            ),
            OpenError::Restrict { path, detail } => Cause::new(
                code!("restrict"),
                [("path", text(path)), ("ext_err_txt", text(detail))],
            ),
        };
        StoreError::Unavailable(cause)
    }
}

impl Diagnostic for OpenError {
    fn code(&self) -> &str {
        match self {
            OpenError::CreateDir { .. } => code!("create-dir"),
            OpenError::Unplaced { .. } => code!("unplaced"),
            OpenError::PredatesEmbedded { .. } => code!("predates-embedded"),
            OpenError::Restrict { .. } => code!("restrict"),
            OpenError::Store(error) => Diagnostic::code(error),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            OpenError::CreateDir { path, detail }
            | OpenError::Unplaced { path, detail }
            | OpenError::Restrict { path, detail } => vec![
                ("path", path.as_str().into()),
                ("ext_err_txt", detail.as_str().into()),
            ],
            OpenError::PredatesEmbedded { path, backup } => vec![
                ("path", path.as_str().into()),
                ("backup", backup.as_str().into()),
            ],
            OpenError::Store(error) => Diagnostic::args(error),
        }
    }
}

/// One value of every error, for the catalog test.
#[cfg(any(test, feature = "test-support"))]
pub fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    let dir = || "/home/u/.demo/projects/jolly-mute-magma".to_string();
    vec![
        Box::new(OpenError::CreateDir {
            path: dir(),
            detail: "permission denied".to_string(),
        }),
        Box::new(OpenError::Unplaced {
            path: format!("{}/store.db", dir()),
            detail: "not an absolute path: C:store.db".to_string(),
        }),
        Box::new(OpenError::PredatesEmbedded {
            path: format!("{}/store.db", dir()),
            backup: format!("{}/store.db.pre-embedded", dir()),
        }),
        Box::new(OpenError::Restrict {
            path: format!("{}/store.db-wal", dir()),
            detail: "operation not permitted".to_string(),
        }),
        Box::new(OpenError::Store(StoreError::Unavailable(
            angzarr_ports::Cause::new(
                "angzarr-store-sqlite-unavailable",
                [("ext_err_txt", angzarr_ports::Arg::Str("locked".to_string()))],
            ),
        ))),
    ]
}

#[cfg(test)]
#[path = "error.test.rs"]
mod tests;
