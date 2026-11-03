//! A test's directory is fresh, under its base, removed when its guard
//! drops, on a pass and on a panic, and kept only for a panic with keeping
//! asked for; a test's file goes with its directory.

use std::panic::{catch_unwind, AssertUnwindSafe};

use super::*;

/// This package's prefix for the guard's own directories.
const PREFIX: &str = concat!(env!("CARGO_PKG_NAME"), "-test-dir");

/// A directory under the temporary one, doing `on_panic` in a panic.
fn temp(on_panic: OnPanic) -> TestDir {
    TestDir::under(&std::env::temp_dir(), PREFIX, on_panic)
}

/// `hold()`'s file written (in its directory, or the file it names), its
/// path recorded, then a panic while it is held: the path, the guard
/// dropped in the unwind.
fn panicking_with<G: Deref<Target = Path>>(hold: impl FnOnce() -> G) -> PathBuf {
    let mut seen = None;
    let unwound = catch_unwind(AssertUnwindSafe(|| {
        let guard = hold();
        let file = if guard.is_dir() {
            guard.join("file")
        } else {
            guard.to_path_buf()
        };
        std::fs::write(&file, "x").unwrap();
        seen = Some(file);
        panic!("the test fails while its file is held");
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
    assert_ne!(dir.path(), other.path(), "each guard has its own");
}

#[test]
fn a_test_dir_under_a_base_is_in_it() {
    let base = TestDir::new(PREFIX);
    let dir = TestDir::under(&base, PREFIX, OnPanic::Remove);
    assert!(dir.is_dir());
    assert_eq!(dir.parent(), Some(base.path()));
    let name = dir.file_name().unwrap().to_string_lossy().into_owned();
    assert!(
        name.starts_with(&format!("{PREFIX}-{}-", std::process::id())),
        "{name}"
    );
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

/// The leak this guards against: a test's scratch, written, then its test
/// panicking (an assert failing, a mutant caught). It is gone.
#[test]
fn a_test_dir_goes_when_its_test_panics() {
    let file = panicking_with(|| temp(OnPanic::Remove));
    assert!(!file.parent().unwrap().exists(), "{}", file.display());
}

#[test]
fn a_test_dir_kept_on_panic_stays_when_its_test_panics() {
    let file = panicking_with(|| temp(OnPanic::Keep));
    let kept = file.is_file();
    std::fs::remove_dir_all(file.parent().unwrap()).unwrap();
    assert!(kept, "{}", file.display());
}

#[test]
fn a_test_dir_kept_on_panic_still_goes_when_its_test_passes() {
    let dir = temp(OnPanic::Keep);
    let path = dir.to_path_buf();
    drop(dir);
    assert!(!path.exists(), "{}", path.display());
}

#[test]
fn a_test_dir_reads_as_its_path() {
    let dir = TestDir::new(PREFIX);
    let as_path: &Path = dir.as_ref();
    let as_os: &OsStr = dir.as_ref();
    assert_eq!(as_path, dir.path());
    assert_eq!(as_os, dir.path().as_os_str());
    assert_eq!(&*dir, dir.path());
}

#[test]
fn a_test_file_is_named_in_a_test_dir_of_its_own_and_not_made() {
    let file = TestFile::new(PREFIX, "calls.log");
    assert_eq!(file.file_name(), Some(OsStr::new("calls.log")));
    let dir = file.parent().unwrap();
    assert!(dir.is_dir());
    assert_eq!(dir.parent(), Some(std::env::temp_dir().as_path()));
    assert!(!file.exists(), "the file is the test's to write");
    let as_path: &Path = file.as_ref();
    let as_os: &OsStr = file.as_ref();
    assert_eq!(as_path, &*file);
    assert_eq!(as_os, file.as_os_str());
}

#[test]
fn a_test_file_goes_with_its_dir_when_it_drops() {
    let file = TestFile::new(PREFIX, "calls.log");
    std::fs::write(&file, "x").unwrap();
    let path = file.to_path_buf();
    drop(file);
    assert!(!path.exists(), "{}", path.display());
    assert!(!path.parent().unwrap().exists(), "{}", path.display());
}

/// The leak this guards against: a fake child's call log, written, then its
/// test panicking. The log and its directory are gone.
#[test]
fn a_test_file_goes_when_its_test_panics() {
    let file = panicking_with(|| TestFile::in_dir(temp(OnPanic::Remove), "calls.log"));
    assert!(!file.exists(), "{}", file.display());
    assert!(!file.parent().unwrap().exists(), "{}", file.display());
}
