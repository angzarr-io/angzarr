//! ctxloom's advisory file locks, taken the one way every ctxloom-family
//! binary takes them, so binaries take turns on one file. Every lock file
//! is opened, made and checked through the file-system seam ([`Fs`]); the
//! lock itself is the host's advisory lock ([`os::Os::try_lock`]: `flock`
//! on unix) on the handle the seam opens.
//!
//!
//! - a lock is an exclusive `flock(2)` on a lock file, created
//!   with its directory if missing, refused when it is not a regular file;
//! - once granted it is re-checked against the file now at its path, and
//!   taken again while it is not ([`LOCK_ATTEMPTS`] tries): `ctxloom
//!   clean` reaps lock files, and a lock on an unlinked file excludes
//!   nobody. That re-check is [`LOCK_PROTOCOL`] 2;
//! - it is waited for at most a bounded time, then refused naming the
//!   lock.

mod error;

use std::fs::File;
use std::io::ErrorKind;
use std::path::Path;
use std::time::{Duration, Instant};

use file_system::{Fs, Open};
use os::{LockKind, Mode};

#[cfg(any(test, feature = "test-support"))]
pub use error::diagnostic_samples;
pub use error::{LockError, EN_US};

/// The lock-taking protocol every taker here follows, as every
/// ctxloom-family binary reports it (`version --format json`'s
/// `lock_protocol`): 2 is the re-check, a lock granted on a file no longer
/// at its path taken again. ctxloom removes a lock file only while every
/// running binary reports at least this.
pub const LOCK_PROTOCOL: u32 = 2;

/// How many times a lock granted on a file no longer at its path is taken
/// again before giving up.
pub const LOCK_ATTEMPTS: u32 = 16;

/// How long a taker waits for whoever holds a lock before giving up, so a
/// process that hangs holding it does not stall every other.
pub const LOCK_WAIT: Duration = Duration::from_secs(10);

/// How often a wait for a lock tries again.
const LOCK_POLL: Duration = Duration::from_millis(20);

/// A lock file's mode, before umask. Not group- or world-writable, since
/// who can open it is who can take it.
const LOCK_FILE_MODE: Mode = Mode::FILE;

/// A lock directory's mode, before umask.
const LOCK_DIR_MODE: Mode = Mode::DIR;

/// A held lock, released when dropped.
#[derive(Debug)]
pub struct Locked {
    _file: File,
}

/// Take the exclusive lock at `lock_path`, an absolute path, waiting up to
/// `wait` for whoever holds it.
pub fn lock_within(fs: &dyn Fs, lock_path: &Path, wait: Duration) -> Result<Locked, LockError> {
    lock(fs, lock_path, &mut until(lock_path, wait))
}

/// Take the exclusive lock at `lock_path`, an absolute path, calling
/// `pause` each time another holds it, until `pause` gives up.
pub fn lock(
    fs: &dyn Fs,
    lock_path: &Path,
    pause: &mut impl FnMut() -> Result<(), LockError>,
) -> Result<Locked, LockError> {
    if !lock_path.is_absolute() {
        return Err(LockError::RelativePath {
            path: lock_path.display().to_string(),
        });
    }
    if let Some(dir) = lock_path.parent() {
        fs.create_dir_all(dir, LOCK_DIR_MODE)
            .map_err(|e| LockError::io(dir, e))?;
    }
    take_current(fs, lock_path, || take_once(fs, lock_path, pause))
}

/// The lock `take` grants, taken again while what it granted is no longer
/// the file at `lock_path`: `ctxloom clean` reaps lock files, and a lock
/// on a file since unlinked excludes nobody, because the next taker
/// creates a new file at the path and locks that.
pub fn take_current(
    fs: &dyn Fs,
    lock_path: &Path,
    mut take: impl FnMut() -> Result<File, LockError>,
) -> Result<Locked, LockError> {
    for _ in 0..LOCK_ATTEMPTS {
        let file = take()?;
        // Nothing there, or nothing to tell, is not the file held.
        if fs.same_file(&file, lock_path).unwrap_or(false) {
            return Ok(Locked { _file: file });
        }
    }
    Err(LockError::Replaced {
        path: lock_path.display().to_string(),
        attempts: LOCK_ATTEMPTS,
    })
}

/// The lock file at `lock_path`, opened (created if need be) and locked
/// exclusively, calling `pause` each time another holds it, until `pause`
/// gives up.
pub fn take_once(
    fs: &dyn Fs,
    lock_path: &Path,
    pause: &mut impl FnMut() -> Result<(), LockError>,
) -> Result<File, LockError> {
    let file = open(fs, lock_path)?;
    loop {
        match fs.os().try_lock(&file, LockKind::Exclusive) {
            Ok(true) => return Ok(file),
            Ok(false) => pause()?,
            Err(e) => return Err(LockError::io(lock_path, e)),
        }
    }
}

/// A pause between tries for the lock at `lock_path` that sleeps a poll,
/// and gives up once `wait` has passed since it was made.
pub fn until(lock_path: &Path, wait: Duration) -> impl FnMut() -> Result<(), LockError> {
    let deadline = Instant::now() + wait;
    let path = lock_path.display().to_string();
    move || {
        let now = Instant::now();
        // None exactly while the deadline is still ahead.
        match now.checked_duration_since(deadline) {
            None => {
                std::thread::sleep((deadline - now).min(LOCK_POLL));
                Ok(())
            }
            Some(_) => Err(LockError::Timeout {
                path: path.clone(),
                waited_ms: u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
            }),
        }
    }
}

/// The lock file at `lock_path`, opened to be locked: read-only when it is
/// there (a lock needs no write access), created when it is not, and
/// refused unless it is a regular file, checked before the open (a FIFO's
/// open would block) and on the open handle.
fn open(fs: &dyn Fs, lock_path: &Path) -> Result<File, LockError> {
    let not_regular = || LockError::NotRegular {
        path: lock_path.display().to_string(),
    };
    if fs
        .metadata(lock_path)
        .is_ok_and(|m| m.kind != file_system::Kind::File)
    {
        return Err(not_regular());
    }
    let file = match fs.open(lock_path, Open::Read) {
        Err(e) if e.kind() == ErrorKind::NotFound => {
            fs.open(lock_path, Open::Create(LOCK_FILE_MODE))
        }
        opened => opened,
    }
    .map_err(|e| LockError::io(lock_path, e))?;
    require_regular(&file, lock_path)?;
    Ok(file)
}

/// `file`, the open handle on `lock_path`, refused unless it is a regular
/// file: the path can be swapped between a check and the open.
fn require_regular(file: &File, lock_path: &Path) -> Result<(), LockError> {
    let meta = file.metadata().map_err(|e| LockError::io(lock_path, e))?;
    if meta.is_file() {
        Ok(())
    } else {
        Err(LockError::NotRegular {
            path: lock_path.display().to_string(),
        })
    }
}

#[cfg(test)]
#[path = "lib.test.rs"]
mod tests;
