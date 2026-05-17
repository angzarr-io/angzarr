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
