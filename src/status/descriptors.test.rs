//! Tests for the descriptor-mount env resolver.
//!
//! WHY: the Helm chart's `descriptors.configMapName` knob sets
//! `ANGZARR_STATUS_DESCRIPTORS_DIR` on the status container. The
//! resolver must treat "unset" and "empty" identically — both mean
//! "no mount". A mistake here would silently turn an unset env var
//! into a non-empty PathBuf("") and the loader would chase a phantom
//! directory.

use super::descriptors_dir_from_env;
use super::DESCRIPTORS_DIR_ENV;

/// Mutex serializes tests that mutate `DESCRIPTORS_DIR_ENV` so they
/// don't race in `cargo test --lib` (which uses one process for all
/// tests within a single test binary).
fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(|p| p.into_inner())
}

#[test]
fn unset_env_returns_none() {
    let _g = env_lock();
    // SAFETY: the lock above serializes env mutation across tests.
    unsafe { std::env::remove_var(DESCRIPTORS_DIR_ENV) };
    assert_eq!(descriptors_dir_from_env(), None);
}

#[test]
fn empty_env_returns_none() {
    let _g = env_lock();
    // Operator who set `descriptors.configMapName: ""` should land
    // here — same effect as omitting the var entirely. Pinning this
    // catches the easy bug where `Ok("".into())` slips past a
    // bare `.ok()`.
    unsafe { std::env::set_var(DESCRIPTORS_DIR_ENV, "") };
    assert_eq!(descriptors_dir_from_env(), None);
    unsafe { std::env::remove_var(DESCRIPTORS_DIR_ENV) };
}

#[test]
fn non_empty_env_returns_path() {
    let _g = env_lock();
    unsafe { std::env::set_var(DESCRIPTORS_DIR_ENV, "/etc/angzarr/descriptors") };
    assert_eq!(
        descriptors_dir_from_env().as_deref(),
        Some(std::path::Path::new("/etc/angzarr/descriptors"))
    );
    unsafe { std::env::remove_var(DESCRIPTORS_DIR_ENV) };
}

#[test]
fn nonexistent_directory_path_still_returned() {
    // Documentation test: the resolver does NOT validate existence.
    // That's the loader's job (deferred to a later phase) so a
    // missing directory degrades gracefully rather than blocking
    // boot.
    let _g = env_lock();
    unsafe { std::env::set_var(DESCRIPTORS_DIR_ENV, "/this/path/does/not/exist") };
    assert!(descriptors_dir_from_env().is_some());
    unsafe { std::env::remove_var(DESCRIPTORS_DIR_ENV) };
}

// ============================================================================
// load_protoset_files — tolerant filesystem scan
// ============================================================================

use super::load_protoset_files;

#[test]
fn load_protoset_files_returns_empty_on_missing_dir() {
    // Per the scaffold's tolerance contract: missing mount → log+proceed,
    // not boot failure. Helper returns Vec::new() with no panic.
    let result = load_protoset_files(std::path::Path::new("/definitely/not/a/dir"));
    assert!(result.is_empty());
}

#[test]
fn load_protoset_files_returns_empty_on_empty_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let result = load_protoset_files(tmp.path());
    assert!(result.is_empty());
}

#[test]
fn load_protoset_files_reads_protoset_files() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("a.protoset"), b"\x00\x01\x02").unwrap();
    std::fs::write(tmp.path().join("b.protoset"), b"\x03\x04").unwrap();
    // Distractor: non-.protoset files are ignored entirely.
    std::fs::write(tmp.path().join("readme.txt"), b"ignore me").unwrap();

    let result = load_protoset_files(tmp.path());
    assert_eq!(
        result.len(),
        2,
        ".protoset files loaded; non-.protoset files ignored"
    );
    // Order isn't guaranteed by readdir, so collect and sort by content len.
    let mut sizes: Vec<usize> = result.iter().map(|b| b.len()).collect();
    sizes.sort();
    assert_eq!(sizes, vec![2, 3]);
}

#[test]
fn load_protoset_files_skips_files_it_cant_read() {
    // A protoset whose bytes are absent (file vanished mid-scan, or
    // ACL change) must not abort the whole load. Tolerance contract:
    // log+skip individual files.
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("good.protoset"), b"ok").unwrap();
    // Simulate "unreadable" by creating a subdirectory with the .protoset
    // extension — fs::read returns Err on directories.
    std::fs::create_dir(tmp.path().join("dir.protoset")).unwrap();

    let result = load_protoset_files(tmp.path());
    assert_eq!(result.len(), 1, "good file loaded; dir.protoset skipped");
    assert_eq!(result[0], b"ok");
}
