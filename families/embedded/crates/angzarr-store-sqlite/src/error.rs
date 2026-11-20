//! Why SQLite could not answer, and how that reaches the ports.
//!
//! In angzarr-ports' style, through its re-export of angzarr-diagnostic (no
//! dependency of its own): every error is a `Diagnostic`, a stable
//! kebab-case `code` starting with this crate's name and named `args` that
//! carry data, and its `Display` is the code then the args (` name=value`
//! each), never English (angzarr decision 14.37). What the
//! store answers through the ports is angzarr-ports' [`StoreError`], whose
//! `Unavailable` and `Unreadable` carry this crate's failure as a
//! [`Cause`], its code and args. A third party's error (SQLite's, the
//! driver's, prost's) is carried as it said it: its text verbatim as
//! `ext_err_txt`, beside SQLite's primary result code as `errno` where it
//! gave one.

use std::fmt;

use angzarr_ports::angzarr_diagnostic::{self, code, Diagnostic};
use angzarr_ports::{Arg, Cause, StoreError};

/// The database's own failures, as causes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SqlError {
    /// The database itself failed: the store could not say. `errno` is
    /// SQLite's primary result code, when the driver gave one; `detail`
    /// the driver's text, verbatim. With an errno it is SQLite's answer
    /// (`angzarr-store-sqlite-unavailable`); without one the driver failed
    /// before SQLite answered, an I/O error or a pool timing out, under a
    /// key of its own whose message names no errno
    /// (`angzarr-store-sqlite-driver-failed`).
    Unavailable { errno: Option<u64>, detail: String },
    /// A stored value's bytes do not decode as what was written: `detail`
    /// is the decoder's text, verbatim.
    Undecodable { detail: String },
    /// A backup's destination, `path`, that could not be created: `detail`
    /// is the operating system's text, verbatim. (Something already there
    /// is angzarr-ports' `BackupExists`.)
    BackupUncreatable { path: String, detail: String },
    /// A file too damaged to open: SQLite finds it malformed or no database
    /// at all. `errno` is SQLite's primary result code, `detail` its text.
    DamagedFile { errno: u64, detail: String },
    /// SQLite answered no row to `PRAGMA data_version`, which always has
    /// one.
    NoDataVersion,
    /// A stored root of `length` bytes, not a UUID's 16.
    BadRoot { length: usize },
    /// A dead letter's stored place, of `kind` and `length` bytes: neither
    /// a sequence (kind 0, eight bytes) nor a command id (kind 1, sixteen).
    BadDeadLetterPlace { kind: u64, length: usize },
}

impl SqlError {
    pub fn code(&self) -> &'static str {
        match self {
            SqlError::Unavailable { errno: Some(_), .. } => code!("unavailable"),
            SqlError::Unavailable { errno: None, .. } => code!("driver-failed"),
            SqlError::Undecodable { .. } => code!("undecodable"),
            SqlError::BackupUncreatable { .. } => code!("backup-uncreatable"),
            SqlError::DamagedFile { .. } => code!("damaged-file"),
            SqlError::NoDataVersion => code!("no-data-version"),
            SqlError::BadRoot { .. } => code!("bad-root"),
            SqlError::BadDeadLetterPlace { .. } => code!("bad-dead-letter-place"),
        }
    }

    /// The message's args, by name.
    pub fn args(&self) -> Vec<(&'static str, Arg)> {
        let text = |detail: &str| ("ext_err_txt", Arg::Str(detail.to_string()));
        match self {
            SqlError::Unavailable { errno, detail } => errno
                .map(|errno| ("errno", Arg::Uint(errno)))
                .into_iter()
                .chain([text(detail)])
                .collect(),
            SqlError::Undecodable { detail } => vec![text(detail)],
            SqlError::BackupUncreatable { path, detail } => {
                vec![("path", Arg::Str(path.clone())), text(detail)]
            }
            SqlError::DamagedFile { errno, detail } => {
                vec![("errno", Arg::Uint(*errno)), text(detail)]
            }
            SqlError::NoDataVersion => Vec::new(),
            SqlError::BadRoot { length } => vec![("length", Arg::Uint(*length as u64))],
            SqlError::BadDeadLetterPlace { kind, length } => vec![
                ("kind", Arg::Uint(*kind)),
                ("length", Arg::Uint(*length as u64)),
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

impl Diagnostic for SqlError {
    fn code(&self) -> &str {
        SqlError::code(self)
    }

    fn args(&self) -> Vec<(&str, angzarr_diagnostic::Arg)> {
        diagnostic_args(SqlError::args(self))
    }
}

/// The code then the args (decision 14.37): angzarr-diagnostic's display.
impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for SqlError {}

/// What an integrity check found: the database file is sound, or it is
/// damaged.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Integrity {
    /// The database reported no problem.
    Intact,
    /// The database reported at least one problem.
    Damaged(Damage),
}

/// The problems the database reported in its own file, as it worded them:
/// at least one. A database keeps its own wording, so each problem is text
/// that was read, not a message to translate.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Damage {
    pub(crate) problems: Vec<String>,
}

impl Damage {
    /// Every problem reported, in the database's order; never empty.
    pub fn problems(&self) -> &[String] {
        &self.problems
    }

    fn first(&self) -> &str {
        self.problems.first().map_or("", String::as_str)
    }

    fn more(&self) -> usize {
        self.problems.len().saturating_sub(1)
    }

    pub fn code(&self) -> &'static str {
        code!("damaged")
    }

    pub fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![
            ("first", Arg::Str(self.first().to_string())),
            ("more", Arg::Uint(self.more() as u64)),
        ]
    }
}

impl Diagnostic for Damage {
    fn code(&self) -> &str {
        Damage::code(self)
    }

    fn args(&self) -> Vec<(&str, angzarr_diagnostic::Arg)> {
        diagnostic_args(Damage::args(self))
    }
}

/// The code then the args (decision 14.37): angzarr-diagnostic's display.
impl fmt::Display for Damage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

/// Every cause this crate can put in a [`StoreError`], one each, for the
/// catalog test; and a [`Damage`].
pub fn causes() -> Vec<Cause> {
    errors().iter().map(SqlError::cause).collect()
}

/// One of each of this crate's errors, for the catalog test.
pub fn errors() -> Vec<SqlError> {
    let detail = || "x".to_string();
    Vec::from([
        SqlError::Unavailable {
            errno: Some(1),
            detail: detail(),
        },
        SqlError::Unavailable {
            errno: None,
            detail: detail(),
        },
        SqlError::Undecodable { detail: detail() },
        SqlError::BackupUncreatable {
            path: "/x/state.db.pre-4".to_string(),
            detail: detail(),
        },
        SqlError::DamagedFile {
            errno: 26,
            detail: detail(),
        },
        SqlError::NoDataVersion,
        SqlError::BadRoot { length: 3 },
        SqlError::BadDeadLetterPlace { kind: 0, length: 2 },
    ])
}

/// A damage report, for the catalog test.
pub fn damage_sample() -> Damage {
    Damage {
        problems: vec!["x".to_string(), "y".to_string()],
    }
}

/// A third party's error the store meets: its text, and SQLite's primary
/// result code where it has one.
pub(crate) trait Driven: fmt::Display {
    /// SQLite's primary result code, when the error is the database's.
    fn errno(&self) -> Option<u64> {
        None
    }

    /// Its text, verbatim: the database's own message, when it is the
    /// database's.
    fn text(&self) -> String {
        self.to_string()
    }
}

impl Driven for sqlx::Error {
    fn errno(&self) -> Option<u64> {
        self.as_database_error()
            .and_then(|e| e.code())
            .and_then(|code| code.parse::<u64>().ok())
            .map(|code| code & 0xff)
    }

    fn text(&self) -> String {
        match self.as_database_error() {
            Some(database) => database.message().to_string(),
            None => self.to_string(),
        }
    }
}

impl Driven for sqlx::migrate::MigrateError {
    fn errno(&self) -> Option<u64> {
        self.driver().and_then(Driven::errno)
    }

    fn text(&self) -> String {
        self.driver().map_or_else(|| self.to_string(), Driven::text)
    }
}

/// The migrator's error's own source, the driver's error, when it has one.
trait Migrating {
    fn driver(&self) -> Option<&sqlx::Error>;
}

impl Migrating for sqlx::migrate::MigrateError {
    fn driver(&self) -> Option<&sqlx::Error> {
        match self {
            sqlx::migrate::MigrateError::Execute(error)
            | sqlx::migrate::MigrateError::ExecuteMigration(error, _) => Some(error),
            _ => None,
        }
    }
}

impl Driven for sea_query::error::Error {}

impl Driven for &str {}

/// The database's failure, as the port's: the store could not say
/// (`Recovery::Retry`), with the driver's code and text.
pub(crate) fn unavailable(error: impl Driven) -> StoreError {
    StoreError::Unavailable(
        SqlError::Unavailable {
            errno: error.errno(),
            detail: error.text(),
        }
        .cause(),
    )
}

/// A stored root of `length` bytes, not 16.
pub(crate) fn bad_root(length: usize) -> StoreError {
    StoreError::Unavailable(SqlError::BadRoot { length }.cause())
}

/// SQLite answered no row to `PRAGMA data_version`.
pub(crate) fn no_data_version() -> StoreError {
    StoreError::Unavailable(SqlError::NoDataVersion.cause())
}

/// A backup's destination that could not be created, with the operating
/// system's text.
pub(crate) fn backup_uncreatable(path: &std::path::Path, error: &std::io::Error) -> StoreError {
    StoreError::Unavailable(
        SqlError::BackupUncreatable {
            path: path.display().to_string(),
            detail: error.to_string(),
        }
        .cause(),
    )
}

/// A database file too damaged to open, as the port's failure
/// (`angzarr-store-sqlite-damaged-file`): SQLite's primary result code
/// `errno` and its text.
pub(crate) fn damaged(errno: u64, detail: String) -> StoreError {
    StoreError::Unavailable(SqlError::DamagedFile { errno, detail }.cause())
}

/// A stored value that does not decode (`Recovery::Bug`, as `Unreadable`),
/// with prost's text.
pub(crate) fn undecodable(error: prost::DecodeError) -> Cause {
    SqlError::Undecodable {
        detail: error.to_string(),
    }
    .cause()
}

#[cfg(test)]
#[path = "error.test.rs"]
mod tests;
