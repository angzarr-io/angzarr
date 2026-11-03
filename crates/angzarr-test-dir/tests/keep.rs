//! [`KEEP`] keeps a panicking test's directory, and nothing else does.
//! Alone in its binary, as it sets the variable for the process.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;

use angzarr_test_dir::{OnPanic, TestDir, KEEP};

/// A [`TestDir::new`] held while its test panics: its path, the guard
/// dropped in the unwind.
fn panicking() -> PathBuf {
    let mut seen = None;
    let unwound = catch_unwind(AssertUnwindSafe(|| {
        let dir = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-keep"));
        seen = Some(dir.to_path_buf());
        panic!("the test fails while its directory is held");
    }));
    assert!(unwound.is_err(), "the closure panicked");
    seen.expect("the directory was made")
}

#[test]
fn the_keep_variable_keeps_a_panicking_tests_directory() {
    // This binary's one test; nothing else reads the environment.
    std::env::remove_var(KEEP);
    assert_eq!(OnPanic::from_env(), OnPanic::Remove);
    let removed = panicking();
    assert!(!removed.exists(), "{}", removed.display());

    std::env::set_var(KEEP, "1");
    assert_eq!(OnPanic::from_env(), OnPanic::Keep);
    let kept = panicking();
    let was_kept = kept.is_dir();
    let _ = std::fs::remove_dir_all(&kept);
    assert!(was_kept, "{}", kept.display());
}
