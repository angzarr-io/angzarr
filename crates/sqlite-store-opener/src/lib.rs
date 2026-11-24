//! Places and opens a project's SQLite event store under a caller-chosen
//! file name: angzarr-store-sqlite's [`SqliteStore`] at a path the caller
//! names (a project's directory, from the shared `project-dir` crate, with
//! the opener's file name: [`Opener::file_in`]).
//!
//! The file name and the directory are the caller's arguments; this crate
//! names neither. It holds nothing open: the caller keeps the store it is
//! lent, as it likes.
//!
//! - The path is the caller's, on the [`Fs`] the opener is given: its
//!   directory is made through the seam, and SQLite opens the file where the
//!   seam says it is on disk ([`Fs::real_path`]), so a re-rooted filesystem
//!   holds the database (and its write-ahead log) too.
//! - [`Opener::open_for_write`] opens it to write, creating its directory and
//!   the file; [`Opener::open_existing`] opens it read-only and never
//!   creates it, finding no store where there is no file.
//! - Both migrate a store older than the build (angzarr-store-sqlite's every
//!   open does), backing the store up beside itself first, and refusing,
//!   untouched, a store newer than the build or one a migration failed or
//!   changed in. This crate writes no SQL and takes no backup of its own.
//! - A store written before the embedded store existed (an earlier event
//!   store's, its ledger only its first migration) is refused by both, keyed
//!   [`OpenError::PredatesEmbedded`] with its `path` and the `backup` to move
//!   it to (`<path>.pre-embedded`), and left exactly as it was: never
//!   migrated in place. Its ledger is read before either open
//!   ([`SqliteStore::ledger_at`]), which migrates nothing.
//! - A store is its owner's alone: its directory 0700 (each one the open
//!   makes, and the store's own made so if it was not) and the file, with
//!   the `-wal` and `-shm` SQLite keeps beside it, 0600, set on every open,
//!   to write or to read, whatever the umask, so an existing store open to
//!   others is made its owner's on its next open. SQLite makes the log and
//!   shared memory with the file's own mode. The modes are set through the
//!   seam, which sets none on Windows: there the profile's ACL holds the
//!   store. One that cannot be set fails the open
//!   ([`OpenError::Restrict`]), the store closed.
//! - [`Opener::check`] checks a database whole
//!   ([`SqliteStore::integrity_check`]), opened afresh and read-only. Damage
//!   is a key and its args, never English; a file too damaged to open is
//!   damage too. No database is no damage ([`Checked::Absent`]).
//! - Errors are [`OpenError`]; [`OpenError::into_store_error`] carries one in
//!   angzarr-ports' `StoreError` for a caller whose port speaks that.

mod error;

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use angzarr_diagnostic::Report;
use angzarr_ports::{self, StoreError};
use angzarr_store_sqlite::{Integrity, SqlError, SqliteStore};
use file_system::Fs;
use os::Mode;

#[cfg(any(test, feature = "test-support"))]
pub use error::diagnostic_samples;
pub use error::{OpenError, EN_US};

/// A directory only its owner may enter: a store's, and each one its open
/// makes.
const PRIVATE_DIR: Mode = Mode::from_bits(0o700);

/// The files SQLite keeps beside a store in WAL mode, by the suffix on its
/// file name: its log and its shared memory.
const COMPANIONS: [&str; 2] = ["-wal", "-shm"];

/// What [`Opener::check`] found at a path.
#[derive(Debug, PartialEq)]
pub enum Checked {
    /// There is no database there.
    Absent,
    /// The database is whole.
    Intact,
    /// The database is damaged: the store's code and args for it.
    Damaged(Report),
}

/// Opens the stores of one file name on one filesystem.
pub struct Opener {
    fs: Arc<dyn Fs>,
    file_name: String,
}

impl Opener {
    /// An opener of stores called `file_name`, on `fs`.
    pub fn new(fs: Arc<dyn Fs>, file_name: impl Into<String>) -> Opener {
        Opener {
            fs,
            file_name: file_name.into(),
        }
    }

    /// Where the store is in `dir` (a project's directory): the opener's
    /// file name there.
    pub fn file_in(&self, dir: &Path) -> PathBuf {
        dir.join(&self.file_name)
    }

    /// The store at `path`, opened to write: its directory and file created
    /// and migrated if need be.
    pub async fn open_for_write(&self, path: &Path) -> Result<SqliteStore, OpenError> {
        if let Some(dir) = path.parent() {
            self.fs
                .create_dir_all(dir, PRIVATE_DIR)
                .map_err(|e| OpenError::CreateDir {
                    path: dir.display().to_string(),
                    detail: e.to_string(),
                })?;
        }
        let real = self.real(path)?;
        refuse_if_predates_embedded(&real, path).await?;
        let store = SqliteStore::open(&real).await.map_err(OpenError::Store)?;
        self.private(path, store).await
    }

    /// The store at `path`, opened read-only (and migrated if need be);
    /// `None` if there is no file there, and nothing is made.
    pub async fn open_existing(&self, path: &Path) -> Result<Option<SqliteStore>, OpenError> {
        let real = self.real(path)?;
        refuse_if_predates_embedded(&real, path).await?;
        match SqliteStore::open_existing(&real)
            .await
            .map_err(OpenError::Store)?
        {
            Some(store) => self.private(path, store).await.map(Some),
            None => Ok(None),
        }
    }

    /// `store`, opened at `path`, once its directory is 0700 and its file,
    /// log and shared memory 0600; closed, and the failure, if a mode
    /// cannot be set. A log or shared memory that is not there is none to
    /// set.
    async fn private(&self, path: &Path, store: SqliteStore) -> Result<SqliteStore, OpenError> {
        let restricted = self.restrict(path);
        match restricted {
            Ok(()) => Ok(store),
            Err(error) => {
                // The failure to make it private is the one said; the
                // store is closed, never dropped, either way.
                let _closed = store.close().await;
                Err(error)
            }
        }
    }

    /// The store at `path` made its owner's: see [`Opener::private`].
    fn restrict(&self, path: &Path) -> Result<(), OpenError> {
        if let Some(dir) = path.parent() {
            self.set_mode(dir, PRIVATE_DIR)?;
        }
        self.set_mode(path, Mode::PRIVATE_FILE)?;
        for suffix in COMPANIONS {
            let mut companion = path.as_os_str().to_owned();
            companion.push(suffix);
            let companion = Path::new(&companion);
            match self.fs.set_permissions(companion, Mode::PRIVATE_FILE) {
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                set => set.map_err(|e| restrict_failed(companion, &e))?,
            }
        }
        Ok(())
    }

    /// `path`'s mode set to `mode`.
    fn set_mode(&self, path: &Path, mode: Mode) -> Result<(), OpenError> {
        self.fs
            .set_permissions(path, mode)
            .map_err(|e| restrict_failed(path, &e))
    }

    /// The database at `path`, opened afresh and read-only, checked whole;
    /// absent if there is none. A file too damaged to open is damage, not a
    /// failure to check. Async: a caller with no runtime of its own runs it
    /// on one.
    pub async fn check(&self, path: &Path) -> Result<Checked, OpenError> {
        let real = self.real(path)?;
        let damaged_file = SqlError::DamagedFile {
            errno: 0,
            detail: String::new(),
        }
        .code();
        let mut store = match SqliteStore::open_existing(&real).await {
            Ok(Some(store)) => store,
            Ok(None) => return Ok(Checked::Absent),
            Err(StoreError::Unavailable(cause)) if cause.code == damaged_file => {
                return Ok(Checked::Damaged(ported(&cause)));
            }
            Err(error) => return Err(OpenError::Store(error)),
        };
        let integrity = store.integrity_check().await.map_err(OpenError::Store)?;
        Ok(match integrity {
            Integrity::Intact => Checked::Intact,
            Integrity::Damaged(damage) => Checked::Damaged(ported(&angzarr_ports::Cause::new(
                damage.code(),
                damage.args(),
            ))),
        })
    }

    /// Where the database at `path` is on disk, for SQLite to open.
    fn real(&self, path: &Path) -> Result<PathBuf, OpenError> {
        self.fs.real_path(path).map_err(|e| OpenError::Unplaced {
            path: path.display().to_string(),
            detail: e.to_string(),
        })
    }
}

/// The failure to set `path`'s mode.
fn restrict_failed(path: &Path, error: &io::Error) -> OpenError {
    OpenError::Restrict {
        path: path.display().to_string(),
        detail: error.to_string(),
    }
}

/// The store at `real` (`path` as the caller names it), refused if it is an
/// earlier event store's, naming its backup: its ledger, read without
/// opening it, holds only the first migration, which that store's first is
/// blob-identical to.
async fn refuse_if_predates_embedded(real: &Path, path: &Path) -> Result<(), OpenError> {
    let ledger = SqliteStore::ledger_at(real)
        .await
        .map_err(OpenError::Store)?;
    let predates = ledger.is_some_and(|ledger| ledger.iter().map(|r| r.version).eq([1]));
    if !predates {
        return Ok(());
    }
    let mut backup = path.as_os_str().to_owned();
    backup.push(".pre-embedded");
    Err(OpenError::PredatesEmbedded {
        path: path.display().to_string(),
        backup: PathBuf::from(backup).display().to_string(),
    })
}

/// angzarr-ports' cause as angzarr-diagnostic's report: its code, and its
/// args with each cause nested.
fn ported(cause: &angzarr_ports::Cause) -> Report {
    cause
        .args
        .iter()
        .fold(Report::new(cause.code), |report, (name, arg)| match arg {
            angzarr_ports::Arg::Uint(n) => report.arg(*name, *n),
            angzarr_ports::Arg::Str(s) => report.arg(*name, s.as_str()),
            angzarr_ports::Arg::Cause(c) => {
                report.arg(*name, angzarr_diagnostic::Arg::Nested(Box::new(ported(c))))
            }
        })
}

#[cfg(test)]
#[path = "lib.test.rs"]
mod tests;
