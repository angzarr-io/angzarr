//! [`TestDir`]: a test's directory under the temporary directory, removed
//! when the guard drops, on a pass and on a panic alike, so a failing (or
//! mutated) test leaves nothing behind.
//!
//! The rule is angzarr-cli's (its `test-support/test_dir.rs`) and
//! angzarr-embedded's (`angzarr-embedded-host`'s `test_dir.test.rs`), so a
//! test's scratch goes the same way in all three:
//!
//! - A directory is `<temp_dir>/<prefix>-<pid>-<n>`, `n` counting within
//!   the test binary, and fresh (a leftover of that name is removed first).
//! - It is removed when the guard drops. Bind the guard for as long as the
//!   path is used: `let dir = TestDir::new(..)`, never
//!   `TestDir::new(..).to_path_buf()`, whose guard drops at once.
//! - To keep a failing test's files for debugging, set
//!   `ANGZARR_KEEP_TEST_DIRS` (to anything): a guard dropped while its test
//!   panics then keeps the directory and names it on stderr. A passing
//!   test's directory goes either way.
//!
//! One file for every crate in this workspace that needs it: angzarr-proto's
//! integration tests mount it by path, with no dependency between the two.

use std::ffi::OsStr;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

/// The environment variable that keeps a panicking test's directory.
pub(crate) const KEEP: &str = "ANGZARR_KEEP_TEST_DIRS";

/// A directory removed when this drops, unless it drops in a panic with
/// [`KEEP`] set.
#[derive(Debug)]
pub(crate) struct TestDir {
    path: PathBuf,
    keep_on_panic: bool,
}

impl TestDir {
    /// A fresh, empty `<temp_dir>/<prefix>-<pid>-<n>`, kept on a panic when
    /// [`KEEP`] is set.
    pub(crate) fn new(prefix: &str) -> TestDir {
        TestDir::keeping_on_panic(prefix, std::env::var_os(KEEP).is_some())
    }

    /// [`TestDir::new`], kept on a panic when `keep_on_panic`, whatever the
    /// environment holds.
    pub(crate) fn keeping_on_panic(prefix: &str, keep_on_panic: bool) -> TestDir {
        static RUN: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "{prefix}-{}-{}",
            std::process::id(),
            RUN.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path)
            .unwrap_or_else(|e| panic!("creating {}: {e}", path.display()));
        TestDir {
            path,
            keep_on_panic,
        }
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        if self.keep_on_panic && std::thread::panicking() {
            eprintln!("{KEEP}: kept {}", self.path.display());
            return;
        }
        let _ = std::fs::remove_dir_all(&self.path);
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

mod tests {
    use std::panic::{catch_unwind, AssertUnwindSafe};

    use super::*;

    /// This package's prefix for the guard's own directories.
    const PREFIX: &str = concat!(env!("CARGO_PKG_NAME"), "-test-dir");

    /// A file written in `hold()`'s directory, then a panic while it is
    /// held: the file's path, the guard dropped in the unwind.
    fn panicking_with(hold: impl FnOnce() -> TestDir) -> PathBuf {
        let mut seen = None;
        let unwound = catch_unwind(AssertUnwindSafe(|| {
            let dir = hold();
            let file = dir.join("file");
            std::fs::write(&file, "x").unwrap();
            seen = Some(file);
            panic!("the test fails while its directory is held");
        }));
        assert!(unwound.is_err(), "the closure panicked");
        seen.expect("the file was written")
    }

    #[test]
    fn a_test_dir_is_fresh_and_empty_under_the_temporary_directory() {
        let dir = TestDir::new(PREFIX);
        assert!(dir.is_dir());
        assert!(std::fs::read_dir(&*dir).unwrap().next().is_none());
        assert_eq!(dir.parent(), Some(std::env::temp_dir().as_path()));
        let name = dir.file_name().unwrap().to_string_lossy().into_owned();
        let pid = format!("{PREFIX}-{}-", std::process::id());
        assert!(name.starts_with(&pid), "{name}");
        let other = TestDir::new(PREFIX);
        assert_ne!(dir.path, other.path, "each guard has its own");
    }

    #[test]
    fn a_test_dir_and_its_contents_go_when_it_drops() {
        let dir = TestDir::new(PREFIX);
        std::fs::create_dir_all(dir.join("a/b")).unwrap();
        std::fs::write(dir.join("a/b/file"), "x").unwrap();
        let path = dir.to_path_buf();
        drop(dir);
        assert!(!path.exists(), "{}", path.display());
    }

    /// The leak this guards against: a test's scratch, written, then its
    /// test panicking (an assert failing, a mutant caught). It is gone.
    #[test]
    fn a_test_dir_goes_when_its_test_panics() {
        let file = panicking_with(|| TestDir::keeping_on_panic(PREFIX, false));
        assert!(!file.parent().unwrap().exists(), "{}", file.display());
    }

    #[test]
    fn a_test_dir_kept_on_panic_stays_when_its_test_panics() {
        let file = panicking_with(|| TestDir::keeping_on_panic(PREFIX, true));
        let kept = file.is_file();
        std::fs::remove_dir_all(file.parent().unwrap()).unwrap();
        assert!(kept, "{}", file.display());
    }

    #[test]
    fn a_test_dir_kept_on_panic_still_goes_when_its_test_passes() {
        let dir = TestDir::keeping_on_panic(PREFIX, true);
        let path = dir.to_path_buf();
        drop(dir);
        assert!(!path.exists(), "{}", path.display());
    }

    #[test]
    fn a_test_dir_reads_as_its_path() {
        let dir = TestDir::new(PREFIX);
        let as_path: &Path = dir.as_ref();
        let as_os: &OsStr = dir.as_ref();
        assert_eq!(as_path, dir.path.as_path());
        assert_eq!(as_os, dir.path.as_os_str());
    }
}
