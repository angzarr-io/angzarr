//! Everything that differs between operating systems, in one place
//! (decisions 3, 4, 5 and 7 of passthrough-os-review.plan.md, as the
//! 2026-10-26 review ruled them).
//!
//! Two abstractions, because they are tested differently:
//!
//! - [`Flavor`]: the LOGIC that differs (home directory, environment
//!   names, PATH and PATHEXT, path display and comparison keys, a path's
//!   top, how a lock file records a path). Pure functions over strings, so
//!   BOTH flavors compile and are tested on every host: Windows behaviour
//!   is checked on Linux. Windows paths are parsed with typed-path, since
//!   `std::path` parses only the compile target's.
//! - [`Os`]: the SYSCALLS that differ (modes, directory links, file
//!   identity, directory sync, advisory locks, the signals that ask the
//!   process to stop, heard by name or as one stop, the account's home
//!   directory). One
//!   implementor per host, in a cfg-gated module, made by the one
//!   constructor, [`host`]: at the composition root for the real
//!   filesystem, which holds it (`file_system::Os::new`); the re-rooted
//!   test filesystem takes the host's itself. Adapters reach it through
//!   the seam (`Fs::os`), and the logic through its [`Flavor`]
//!   ([`Os::flavor`]).
//!
//! Beside them, [`Top`]: where an absolute path of the compile target
//! starts (a drive or share on Windows), as `std::path` parses it, for the
//! file-system seam's re-rooting.
//!
//! Nothing outside this crate names `cfg(unix|windows|target_os|..)`,
//! `cfg!(..)`, `std::os::` or `split_paths` (decision 4): `just lint-os`
//! checks it, with an allowlist (`scripts/os-cfg-allow.txt`).
//!
//! Not here: an owner-only DACL on Windows (task crispy-woof). A private
//! [`Mode`] is the owner's alone on unix and is not enforced on Windows,
//! as before this crate.

// This crate is below the file-system seam (file-system depends on it), so
// its syscall implementors call std::fs themselves, as file-system does.
#![allow(
    clippy::disallowed_methods,
    reason = "the OS layer under the file-system seam: its syscall implementors call std::fs"
)]

mod detach;
mod flavor;
mod host;
pub mod process;
mod signal;
pub mod socket;
mod top;

use std::ffi::OsString;
use std::fmt;
use std::fs::{DirBuilder, File, Metadata, OpenOptions};
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
#[cfg(any(test, feature = "test-support"))]
use std::process::{Child, Command};
use std::sync::Arc;

pub use flavor::{Flavor, DEFAULT_PATHEXT};
#[cfg(any(test, feature = "test-support"))]
pub use signal::Raise;
pub use signal::{Signal, Signals};
pub use top::Top;

/// Permission bits (`0o777` at most). On unix they are the mode. On
/// Windows, which has no mode bits, they are ignored (an owner-only DACL
/// for the private modes is task crispy-woof).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Mode(u32);

impl Mode {
    /// A file only its owner may read or write.
    pub const PRIVATE_FILE: Mode = Mode(0o600);
    /// A directory only its owner may enter. Test support.
    #[cfg(any(test, feature = "test-support"))]
    pub const PRIVATE_DIR: Mode = Mode(0o700);
    /// A file anyone may read, its owner write.
    pub const FILE: Mode = Mode(0o644);
    /// A directory anyone may enter, its owner write.
    pub const DIR: Mode = Mode(0o755);

    /// The mode with these permission bits; anything past `0o777` is
    /// dropped.
    pub const fn from_bits(bits: u32) -> Mode {
        Mode(bits & 0o777)
    }

    /// The permission bits.
    pub const fn bits(self) -> u32 {
        self.0
    }
}

/// The kind of advisory lock [`Os::try_lock`] takes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LockKind {
    /// Many may hold it at once.
    Shared,
    /// Only one may hold it.
    Exclusive,
}

/// Resolves when the process is asked to stop: SIGINT or SIGTERM on
/// unix; Ctrl-C, Ctrl-Break, or the console's close, logoff or shutdown on
/// Windows.
pub type StopSignal = Pin<Box<dyn Future<Output = ()> + Send>>;

/// The host's syscalls that differ between operating systems. Made once,
/// by [`host`], at the composition root.
pub trait Os: Send + Sync + fmt::Debug {
    /// Which flavor's logic holds here.
    fn flavor(&self) -> Flavor;

    /// Set `path`'s mode exactly. Windows has no modes: there it only
    /// refuses what is missing (`NotFound`).
    fn set_mode(&self, path: &Path, mode: Mode) -> io::Result<()>;

    /// Set the mode of the open `file` exactly (nothing on Windows).
    fn set_file_mode(&self, file: &File, mode: Mode) -> io::Result<()>;

    /// `meta`'s mode on unix; `None` on Windows.
    fn mode_of(&self, meta: &Metadata) -> Option<Mode>;

    /// Make a file `options` creates get `mode`, before the umask (nothing
    /// on Windows).
    fn open_mode(&self, options: &mut OpenOptions, mode: Mode);

    /// Make each directory `builder` creates get `mode`, before the umask
    /// (nothing on Windows).
    fn dir_mode(&self, builder: &mut DirBuilder, mode: Mode);

    /// Make `link` a link to the directory `target`: a symlink written as
    /// given on unix (relative to `link`'s directory when relative); a
    /// junction on Windows, which names its target absolutely, so a
    /// relative one is resolved against `link`'s directory.
    fn link_dir(&self, target: &Path, link: &Path) -> io::Result<()>;

    /// What `link` links to, as [`Os::link_dir`] made it; `InvalidInput`
    /// for what is not a link.
    fn read_link(&self, link: &Path) -> io::Result<PathBuf>;

    /// Remove the file at `path`, or the link there itself, never what it
    /// links to (a junction is a directory to Windows, a symlink a file to
    /// unix).
    fn remove_link(&self, path: &Path) -> io::Result<()>;

    /// Whether `file` is the file at `path` now (links followed): device
    /// and inode on unix, volume and file index on Windows. `NotFound`
    /// when nothing is there.
    fn same_file(&self, file: &File, path: &Path) -> io::Result<bool>;

    /// Rename `from` to `to` only if nothing is at `to`, the check and the
    /// rename one atomic step, so no other process can put a file there
    /// between them: `renameat2(RENAME_NOREPLACE)` on Linux,
    /// `renamex_np(RENAME_EXCL)` on macOS, `MoveFileExW` without
    /// `MOVEFILE_REPLACE_EXISTING` on Windows. Something at `to` is
    /// `AlreadyExists`, and both are left as they were; a filesystem that
    /// cannot rename so, or another unix, is the OS's refusal
    /// (`Unsupported` where there is no such call).
    fn rename_noreplace(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Make a directory's entries durable (a rename into it): fsync on
    /// unix; nothing to do on Windows, where a directory cannot be opened
    /// to sync.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;

    /// Take `kind` of advisory lock on `file` without waiting: `false`
    /// when another holds a conflicting one (flock on unix, LockFileEx on
    /// Windows). Held until the file is closed.
    fn try_lock(&self, file: &File, kind: LockKind) -> io::Result<bool>;

    /// A future resolving when the process is asked to stop, as Go's
    /// os.Interrupt and SIGTERM are: SIGINT or SIGTERM on unix; Ctrl-C,
    /// Ctrl-Break, or the console's close, logoff or shutdown on Windows.
    /// Listening
    /// from the moment it is made, so a signal before its first poll is
    /// not lost to the default disposition. Made on the caller's tokio
    /// runtime, whose drivers must be enabled. It is [`Os::signals`] of
    /// those, whichever arrives.
    fn stop_signal(&self) -> io::Result<StopSignal>;

    /// The signals of `heard`, each named as it arrives ([`Signal`] says
    /// what each is on Windows), for a caller that must tell them apart:
    /// stop on some, carry on through others, exit with the status of the
    /// one that stopped it. Listening from the moment it is made, as
    /// [`Os::stop_signal`] is; made on the caller's tokio runtime, whose
    /// drivers must be enabled.
    fn signals(&self, heard: &[Signal]) -> io::Result<Signals>;

    /// The home directory of the account the process runs as, as the
    /// system keeps it, never a variable (`HOME`, `USERPROFILE`), so a
    /// process whose `HOME` was moved still finds the account's: on unix
    /// the account database's (`getpwuid_r` of the effective uid, its
    /// `pw_dir`); on Windows the user's profile folder
    /// (`SHGetKnownFolderPath(FOLDERID_Profile)`). `NotFound` when the
    /// account has no entry or the entry no home.
    fn user_home(&self) -> io::Result<PathBuf>;
}

/// The host's [`Os`]: the one constructor, and the one place that picks
/// an implementor by the compile target.
pub fn host() -> Arc<dyn Os> {
    host::make()
}

/// Text that is not Unicode, starting `prefix`: a variable's value or a
/// path no UTF-8 reader can take (an invalid byte on unix, an unpaired
/// surrogate on Windows). For tests of what refuses or skips such text.
pub fn not_unicode(prefix: &str) -> OsString {
    host::not_unicode(prefix)
}

/// Start `command` in a process group of its own, so a console event sent
/// to that group ([`ctrl_break`]) reaches the child alone, never the
/// process starting it: CREATE_NEW_PROCESS_GROUP on Windows. Nothing on
/// unix, whose stop signals are sent to one process by pid. For tests
/// that stop a child as a console would. Test support.
#[cfg(any(test, feature = "test-support"))]
pub fn own_group(command: &mut Command) -> &mut Command {
    host::own_group(command)
}

/// Send a Ctrl-Break to `child`'s process group, which [`own_group`]
/// started it in: GenerateConsoleCtrlEvent on Windows. Unix has no console
/// events: `Unsupported`. Test support.
#[cfg(any(test, feature = "test-support"))]
pub fn ctrl_break(child: &Child) -> io::Result<()> {
    host::ctrl_break(child)
}

/// A test fixture's absolute path, written the unix way (`/home/u`), as
/// the host spells it ([`Flavor::from_top`]: `C:\home\u` on Windows): the
/// one way a test names a path that is absolute on every host. Test
/// support.
#[cfg(any(test, feature = "test-support"))]
pub fn fixture(path: &str) -> PathBuf {
    PathBuf::from(host().flavor().from_top(path))
}

#[cfg(test)]
#[path = "lib.test.rs"]
mod tests;
