//! `--write-upgrades`: the one helper every binary of the family uses to
//! persist an in-memory upgrade (owner ruling D8), beside the loader.
//!
//! Loading never writes ([`crate::versioned`]). A binary run with
//! [`WRITE_UPGRADES_FLAG`] calls [`write_back`] for each versioned file it
//! loaded: a file already current is left alone, and an older one is written
//! back at the current version, through [`Loader::upgrade_keeping`] so its
//! comments stay where the migration did not change them. With
//! [`Backup::Keep`] the file's old bytes are first written to
//! `<file>`[`BACKUP_SUFFIX`]; when that fails the file is not touched. With
//! [`Action::Print`] nothing is written and the upgraded text comes back for
//! the caller to show.
//!
//! The file system is the caller's, behind [`Files`]: two calls, so the
//! helper stays pure and testable, and builds for any target. Atomic
//! replacement and keeping a file's permission bits are the adapter's to
//! promise, as Go schemaver's `WriteBack` does through its safefs.

use std::fmt;

use angzarr_diagnostic::{Arg, Diagnostic, Report};

use crate::versioned::{LoadError, Loader};

/// The flag every binary that loads versioned files carries, one spelling
/// for the family: with it, a load that migrated a file persists the
/// migration; without it the file on disk never changes.
pub const WRITE_UPGRADES_FLAG: &str = "write-upgrades";

/// Appended to a file's path for the copy of its pre-upgrade bytes.
pub const BACKUP_SUFFIX: &str = ".bak";

/// The file system a write-back reads and writes. Errors are the adapter's
/// own diagnostics (an errno, the OS's text as `ext_err_txt`), captured.
pub trait Files {
    /// The file's text.
    fn read(&mut self, path: &str) -> Result<String, Report>;

    /// Replace the file's text with `text`, creating it if it is missing:
    /// atomically, keeping an existing file's permission bits.
    fn write(&mut self, path: &str, text: &str) -> Result<(), Report>;
}

/// Whether the old bytes are kept beside the file, per file kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backup {
    /// Write `<file>.bak` first: for a file nothing else versions (a
    /// config), the only way back.
    Keep,
    /// Nothing beside the file: for version-controlled content, whose
    /// history holds the old bytes.
    None,
}

/// What to do with an upgraded file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Write,
    /// Write nothing; hand back the text that would be written.
    Print,
}

/// How [`write_back`] works.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Options {
    pub backup: Backup,
    pub action: Action,
    /// For values the migration rewrites, as [`crate::write`] takes it.
    pub flow_depth: usize,
}

/// Keep a backup, write, flow depth 2.
impl Default for Options {
    fn default() -> Options {
        Options {
            backup: Backup::Keep,
            action: Action::Write,
            flow_depth: 2,
        }
    }
}

/// What [`write_back`] did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The file is current: nothing was written.
    Current,
    /// The file was replaced at the current version, its old bytes at
    /// `backup` if one was kept.
    Written {
        backup: Option<String>,
        /// Whether the comments outside the migrated entries were kept
        /// ([`crate::versioned::Upgraded::comments_kept`]).
        comments_kept: bool,
    },
    /// [`Action::Print`]: the text the file would be replaced with.
    Printed { text: String, comments_kept: bool },
}

/// Why a write-back failed, and what it left.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteBackError {
    /// The file could not be read; nothing was written.
    Read { path: String, cause: Report },
    /// The file does not load ([`LoadError`], with the file as its source);
    /// nothing was written.
    Load(LoadError),
    /// The backup could not be written; the file was not touched.
    Backup { path: String, cause: Report },
    /// The file could not be replaced; its backup, if kept, was written.
    Write { path: String, cause: Report },
}

impl fmt::Display for WriteBackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for WriteBackError {}

impl Diagnostic for WriteBackError {
    fn code(&self) -> &str {
        match self {
            WriteBackError::Read { .. } => "angzarr-yaml-write-back-read",
            WriteBackError::Load(e) => e.code(),
            WriteBackError::Backup { .. } => "angzarr-yaml-write-back-backup",
            WriteBackError::Write { .. } => "angzarr-yaml-write-back-write",
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            WriteBackError::Load(e) => e.args(),
            WriteBackError::Read { path, cause }
            | WriteBackError::Backup { path, cause }
            | WriteBackError::Write { path, cause } => vec![
                ("path", Arg::from(path.as_str())),
                ("cause", Arg::nested(cause)),
            ],
        }
    }
}

/// Bring the versioned file at `path` to `loader`'s current version on disk,
/// as `options` say: see the module docs.
pub fn write_back(
    files: &mut impl Files,
    loader: &Loader,
    path: &str,
    options: Options,
) -> Result<Outcome, WriteBackError> {
    let old = files.read(path).map_err(|cause| WriteBackError::Read {
        path: path.to_string(),
        cause,
    })?;
    let Some(up) = loader
        .upgrade_keeping(Some(path), &old, options.flow_depth)
        .map_err(WriteBackError::Load)?
    else {
        return Ok(Outcome::Current);
    };
    if options.action == Action::Print {
        return Ok(Outcome::Printed {
            text: up.text,
            comments_kept: up.comments_kept,
        });
    }
    let backup = match options.backup {
        Backup::Keep => {
            let bak = format!("{path}{BACKUP_SUFFIX}");
            files
                .write(&bak, &old)
                .map_err(|cause| WriteBackError::Backup {
                    path: bak.clone(),
                    cause,
                })?;
            Some(bak)
        }
        Backup::None => None,
    };
    files
        .write(path, &up.text)
        .map_err(|cause| WriteBackError::Write {
            path: path.to_string(),
            cause,
        })?;
    Ok(Outcome::Written {
        backup,
        comments_kept: up.comments_kept,
    })
}

/// One value of every write-back error of this crate's own, for the
/// catalog parity test ([`WriteBackError::Load`] is the loader's).
pub(crate) fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    let cause = || Report::new("adapter-io").arg("errno", 13u32);
    let path = || "/cfg/app.yaml".to_string();
    vec![
        Box::new(WriteBackError::Read {
            path: path(),
            cause: cause(),
        }),
        Box::new(WriteBackError::Backup {
            path: format!("{}{BACKUP_SUFFIX}", path()),
            cause: cause(),
        }),
        Box::new(WriteBackError::Write {
            path: path(),
            cause: cause(),
        }),
    ]
}

#[cfg(test)]
#[path = "write_back.test.rs"]
mod tests;
