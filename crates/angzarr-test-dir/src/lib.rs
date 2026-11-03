//! [`TestDir`]: a test's directory under the temporary directory, removed
//! when the guard drops, on a pass and on a panic alike, so a failing (or
//! mutated) test leaves nothing behind; and [`TestFile`], one file in a
//! `TestDir` of its own (a fake child's call log, a control socket, a
//! mark), removed with it.
//!
//! The one guard every angzarr repository's tests use (core's shared
//! crates, angzarr-embedded, angzarr-cli), taken as a dev-dependency only:
//!
//! - A directory is `<base>/<prefix>-<pid>-<n>`, `n` counting within the
//!   test binary, `base` the temporary directory ([`TestDir::new`]) or one
//!   the test names ([`TestDir::under`]: a short one, for a socket). It is
//!   short, for the tests that put a socket in it, and fresh (a leftover of
//!   that name is removed first).
//! - It is removed when the guard drops. Bind the guard for as long as the
//!   path is used: `let dir = TestDir::new(..)`, never
//!   `TestDir::new(..).to_path_buf()`, whose guard drops at once.
//! - To keep a failing test's files for debugging, set [`KEEP`]
//!   (`ANGZARR_KEEP_TEST_DIRS`, to anything): a guard dropped while its test
//!   panics then keeps the directory and names it on stderr. A passing
//!   test's directory goes either way.

use std::ffi::OsStr;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

/// The environment variable that keeps a panicking test's directory.
pub const KEEP: &str = "ANGZARR_KEEP_TEST_DIRS";

/// What a guard dropped while its test panics does with its directory. A
/// guard dropped otherwise removes it either way.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OnPanic {
    /// Keep it, and name it on stderr.
    Keep,
    /// Remove it, as on a pass.
    Remove,
}

impl OnPanic {
    /// [`OnPanic::Keep`] when [`KEEP`] is set, else [`OnPanic::Remove`].
    pub fn from_env() -> OnPanic {
        todo!("red: the environment's choice")
    }
}

/// A directory removed when this drops, unless it drops in a panic and is
/// kept [`OnPanic::Keep`].
#[derive(Debug)]
pub struct TestDir {
    path: PathBuf,
    on_panic: OnPanic,
}

impl TestDir {
    /// A fresh, empty `<temp_dir>/<prefix>-<pid>-<n>`, kept on a panic when
    /// [`KEEP`] is set ([`OnPanic::from_env`]).
    pub fn new(prefix: &str) -> TestDir {
        TestDir::under(&std::env::temp_dir(), prefix, OnPanic::from_env())
    }

    /// A fresh, empty `<base>/<prefix>-<pid>-<n>`, doing `on_panic` with it
    /// when dropped in a panic.
    pub fn under(base: &Path, prefix: &str, on_panic: OnPanic) -> TestDir {
        let _ = (
            base,
            prefix,
            on_panic,
            AtomicUsize::new(0),
            Ordering::Relaxed,
        );
        todo!("red: a fresh directory")
    }

    /// The directory.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        // red: nothing removed yet
        let _ = (self.on_panic, &self.path);
    }
}

impl Deref for TestDir {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<Path> for TestDir {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<OsStr> for TestDir {
    fn as_ref(&self) -> &OsStr {
        self.path.as_os_str()
    }
}

/// `<TestDir>/<name>`, not created: the file is the test's (or its
/// child's) to write. It goes with its directory, on the directory's rule.
#[derive(Debug)]
pub struct TestFile {
    path: PathBuf,
    // Held for its drop: the directory, and the file in it, go with this.
    _dir: TestDir,
}

impl TestFile {
    /// `name` in a fresh [`TestDir`] of `prefix`.
    pub fn new(prefix: &str, name: &str) -> TestFile {
        TestFile::in_dir(TestDir::new(prefix), name)
    }

    /// `name` in `dir`, which this then owns.
    pub fn in_dir(dir: TestDir, name: &str) -> TestFile {
        TestFile {
            path: dir.join(name),
            _dir: dir,
        }
    }
}

impl Deref for TestFile {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<Path> for TestFile {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<OsStr> for TestFile {
    fn as_ref(&self) -> &OsStr {
        self.path.as_os_str()
    }
}

#[cfg(test)]
#[path = "lib.test.rs"]
mod tests;
