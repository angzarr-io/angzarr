//! The filesystem seam: every filesystem operation taskloom (and, later,
//! ctxloom) makes goes through an [`Fs`], held as `Arc<dyn Fs>`.
//!
//! Two implementors, both `std::fs` underneath:
//!
//! - [`Os`]: the real filesystem, paths as given. Production.
//! - [`Rooted`]: every path re-rooted under one directory (afero's
//!   `BasePathFs` over `OsFs`). Tests, through [`Rooted::temp`].
//!
//! And, behind `test-support`, `Recording`: another filesystem's calls
//! handed on, each that changes the disk kept in order.
//!
//! Errors are `std::io::Error` with the kind the OS gave (`NotFound`,
//! `AlreadyExists`, ...), plus the seam's own refusals:
//!
//! - a RELATIVE path resolves against the working directory the
//!   filesystem was made with ([`Os::new`], [`Rooted::new`]), an absolute
//!   path the caller gives (the composition root reads the process's once):
//!   no operation reads the process's working directory itself. One made
//!   over a working directory that could not be read ([`Os::without_cwd`],
//!   [`Rooted::without_cwd`]) has none: [`Fs::cwd`] says why, a relative
//!   path fails with that [`CwdError`], and an absolute one is unaffected.
//! - a path given or returned is the CALLER'S path: [`Rooted`] never shows
//!   its root, except through [`Fs::real_path`], which exists for what
//!   must open a path itself (SQLite, advisory locks).

// The workspace's clippy.toml forbids file I/O outside this crate: every
// other crate reaches the filesystem through an `Fs`.
#![allow(
    clippy::disallowed_methods,
    reason = "this crate is the file-system seam: the one place std::fs and tempfile are called"
)]

mod error;
mod os;
mod real;
#[cfg(any(test, feature = "test-support"))]
mod recording;
mod rooted;

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use ::os::Mode;

#[cfg(any(test, feature = "test-support"))]
pub use error::diagnostic_samples;
pub use error::{CwdError, TargetExists, EN_US};
pub use os::Os;
#[cfg(any(test, feature = "test-support"))]
pub use recording::{Call, Recording};
pub use rooted::Rooted;

/// The permissions an atomic write leaves the file with.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Perms {
    /// Exactly this mode, past the umask (Go's `WriteFile`).
    Exact(Mode),
    /// An existing file's own mode; this one for a file the write creates
    /// (Go's `WriteFileKeepMode`, agent-configs' `write`).
    KeepOr(Mode),
}

/// How [`Fs::open`] opens a file. A mode is the created file's, before
/// the umask; an existing file's mode is never changed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Open {
    /// An existing file, read-only.
    Read,
    /// Created if missing, never truncated; for writing (a lock file).
    Create(Mode),
    /// Created only if missing (`O_EXCL`); `AlreadyExists` otherwise.
    CreateNew(Mode),
    /// Created if missing; every write lands at the end (`O_APPEND`).
    Append(Mode),
}

/// What a path is, links not followed (in [`Fs::symlink_metadata`] and a
/// directory's entries).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    File,
    Dir,
    Symlink,
    /// A FIFO, socket or device.
    Other,
}

/// What [`Fs::metadata`] and [`Fs::symlink_metadata`] report.
#[derive(Clone, PartialEq, Eq, Debug)]
#[non_exhaustive]
pub struct Meta {
    pub kind: Kind,
    /// In bytes.
    pub len: u64,
    /// A file the host would run: any execute bit on unix, any file on
    /// Windows ([`::os::Flavor::is_executable`]).
    pub executable: bool,
    /// The permission bits on unix; `None` elsewhere ([`::os::Os::mode_of`]).
    pub mode: Option<Mode>,
    pub modified: SystemTime,
}

/// One entry of a directory.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Entry {
    /// The entry's name in its directory (not a path).
    pub name: std::ffi::OsString,
    /// The entry itself, its link not followed.
    pub kind: Kind,
}

/// The filesystem. Every path is the caller's own, a relative one resolved
/// against the filesystem's working directory; see the crate's
/// documentation.
pub trait Fs: Send + Sync + std::fmt::Debug {
    /// The working directory relative paths resolve against, the caller's
    /// path; or why there is none (it could not be read when this was
    /// made). Asks the OS nothing: it is what the filesystem was made with.
    fn cwd(&self) -> Result<&Path, CwdError>;

    /// The host's syscalls this filesystem runs on, and through
    /// [`::os::Os::flavor`] the logic that differs between operating
    /// systems: what an adapter holding only an `Fs` needs of the OS.
    fn os(&self) -> &dyn ::os::Os;

    /// The file's bytes.
    fn read(&self, path: &Path) -> io::Result<Vec<u8>>;

    /// The file's text; `InvalidData` when it is not UTF-8.
    fn read_to_string(&self, path: &Path) -> io::Result<String>;

    /// Replace the file at `path` with `bytes` whole: a unique temporary
    /// file beside it, written, synced, given `perms`, renamed over it, and
    /// its directory synced (unix). A reader sees all of the old file or
    /// all of the new; a failure leaves no temporary file. The directory
    /// must exist (`NotFound`).
    fn write_atomic(&self, path: &Path, bytes: &[u8], perms: Perms) -> io::Result<()>;

    /// The file, opened as `how` says: a handle for what needs one (an
    /// advisory lock, an appended log).
    fn open(&self, path: &Path, how: Open) -> io::Result<File>;

    /// The directory and its missing parents, each made with `mode` (before
    /// the umask). One already there is left as it is.
    fn create_dir_all(&self, path: &Path, mode: Mode) -> io::Result<()>;

    /// Remove the file (or link: never what it links to).
    fn remove_file(&self, path: &Path) -> io::Result<()>;

    /// Remove the directory and everything in it, following no link.
    fn remove_dir_all(&self, path: &Path) -> io::Result<()>;

    /// Rename `from` to `to`, replacing a file at `to`.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Rename `from` to `to` only if nothing is at `to`, atomically
    /// ([`::os::Os::rename_noreplace`]: no other process can put a file
    /// there between the check and the rename). Something at `to` is
    /// `AlreadyExists` carrying a [`TargetExists`] (its path the caller's
    /// `to`), and both are left as they were. Provided for every `Fs`
    /// through [`Fs::os`] and [`Fs::real_path`], so a wrapper (a test's
    /// fault-injecting `Fs`) has it without writing it.
    fn rename_noreplace(&self, from: &Path, to: &Path) -> io::Result<()> {
        let (real_from, real_to) = (self.real_path(from)?, self.real_path(to)?);
        self.os()
            .rename_noreplace(&real_from, &real_to)
            .map_err(|e| TargetExists::at(e, to))
    }

    /// Make the directory's entries durable, as a rename into or out of it
    /// leaves them ([`::os::Os::sync_dir`]: fsync on unix; nothing to do on
    /// Windows, where a rename is durable with its file). Nothing at `dir`
    /// is `NotFound` on unix. Provided for every `Fs` through [`Fs::os`]
    /// and [`Fs::real_path`], so a wrapper has it without writing it.
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.os().sync_dir(&self.real_path(dir)?)
    }

    /// Set the mode exactly.
    fn set_permissions(&self, path: &Path, mode: Mode) -> io::Result<()>;

    /// The directory's entries, sorted by name.
    fn read_dir(&self, path: &Path) -> io::Result<Vec<Entry>>;

    /// What is at `path`, links followed.
    fn metadata(&self, path: &Path) -> io::Result<Meta>;

    /// What is at `path`, a link itself reported as one.
    fn symlink_metadata(&self, path: &Path) -> io::Result<Meta>;

    /// Whether anything is at `path`, links followed (a dangling link is
    /// `false`); an error is a failure to find out, never "absent".
    fn exists(&self, path: &Path) -> io::Result<bool>;

    /// `path` with every link and `.`/`..` resolved, as the caller names
    /// it, and as a person (and Go's `EvalSymlinks`) spells it: on Windows
    /// without the verbatim prefix (`\\?\`) the OS's resolution carries.
    fn canonicalize(&self, path: &Path) -> io::Result<PathBuf>;

    /// Make `link` a link to the directory `target`, written as given
    /// (relative to `link`'s parent when relative). On Windows it is a
    /// junction, which names its target absolutely.
    fn symlink_dir(&self, target: &Path, link: &Path) -> io::Result<()>;

    /// The target `link` was made with, as the caller gave it.
    fn read_link(&self, link: &Path) -> io::Result<PathBuf>;

    /// A new, empty directory in `parent`, its name starting `prefix`.
    /// Removing it is the caller's business.
    fn make_temp_dir(&self, parent: &Path, prefix: &str) -> io::Result<PathBuf>;

    /// The OS path `path` is at, for what must open a path itself (SQLite's
    /// database, an advisory lock file): `path` itself for [`Os`], its
    /// re-rooted place for [`Rooted`]. Nothing else may use it but
    /// the provided bodies of [`Fs::rename_noreplace`] and
    /// [`Fs::sync_dir`], which hand it to the OS.
    fn real_path(&self, path: &Path) -> io::Result<PathBuf>;

    /// Whether `file` is the file at `path` now (links followed): the same
    /// device and inode on unix, the same volume and file index on Windows.
    /// A file renamed over `path` since `file` was opened is not;
    /// `NotFound` when nothing is at `path`.
    fn same_file(&self, file: &File, path: &Path) -> io::Result<bool>;
}

#[cfg(test)]
#[path = "lib.test.rs"]
mod tests;
