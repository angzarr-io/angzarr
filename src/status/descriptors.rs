//! Pre-staged proto descriptor loading for the status console
//! (Phase 0 scaffold).
//!
//! The status binary builds its `DescriptorPool` from three sources per
//! the plan's P8 / P7 decisions:
//!
//!   1. **Framework descriptors** — compiled in via
//!      [`crate::proto_reflect::EMBEDDED_DESCRIPTOR`] (already wired in
//!      Phase 0).
//!   2. **Mounted user descriptors** — `.protoset` files under
//!      [`DESCRIPTORS_DIR_ENV`] (typically `/etc/angzarr/descriptors/`
//!      from a Helm-managed ConfigMap). *This module owns the load
//!      path.*
//!   3. **Pulled-from-coordinators** — via standard gRPC Server
//!      Reflection against discovered coordinator endpoints. Lands in
//!      a later phase when the discovery wiring exists.
//!
//! Phase 0 scope: provide the env-var-driven mount-path resolver and
//! the no-op watcher placeholder. Actual pool-merge / inotify
//! refresh-on-change lands in Phase 3 when the descriptors first get
//! consumed (event browser payload rendering).
//!
//! Tolerance contract (plan's degradation section): a missing or
//! empty mount is *not an error*. Log a warning; proceed with
//! framework descriptors only. The UI degrades to JSON+base64 for
//! unknown `Any` payloads — never crashes.

use std::path::{Path, PathBuf};

use tracing::warn;

/// Environment variable the Helm chart sets when the operator mounts
/// a `descriptors` ConfigMap. Empty / unset → no pre-staged mount.
pub const DESCRIPTORS_DIR_ENV: &str = "ANGZARR_STATUS_DESCRIPTORS_DIR";

/// File extension we recognize for `FileDescriptorSet` blobs.
/// Convention: `protoc --descriptor_set_out=foo.protoset ...`.
pub const PROTOSET_EXTENSION: &str = "protoset";

/// Resolve the pre-staged descriptors directory from the environment.
///
/// Returns `None` when the env var is unset OR set to an empty
/// string — both mean "operator did not configure a mount, proceed
/// without one." Does NOT check whether the directory actually
/// exists; that check is deferred to the loader so a missing
/// directory degrades gracefully rather than refusing to boot.
pub fn descriptors_dir_from_env() -> Option<PathBuf> {
    match std::env::var(DESCRIPTORS_DIR_ENV) {
        Ok(s) if !s.is_empty() => Some(PathBuf::from(s)),
        _ => None,
    }
}

/// Read `.protoset` (FileDescriptorSet) blobs from a mounted directory.
///
/// Tolerant per the scaffold's resilience contract:
/// - Missing directory → empty vec + warning (operator didn't mount,
///   or the mount hasn't appeared yet).
/// - Per-file read failure (permission, mid-scan removal, a
///   `.protoset` that's actually a directory) → skip that file with a
///   warning; continue the scan.
/// - Non-`.protoset` files in the directory are silently ignored.
///
/// Returns the raw bytes of each file; the caller decodes them into a
/// `DescriptorPool` so this helper stays I/O-only and easy to unit-test.
pub fn load_protoset_files(dir: &Path) -> Vec<Vec<u8>> {
    let read_dir = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) => {
            warn!(
                path = %dir.display(),
                error = %e,
                "descriptors dir not readable; proceeding without pre-staged descriptors"
            );
            return Vec::new();
        }
    };

    let mut out = Vec::new();
    for entry in read_dir.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some(PROTOSET_EXTENSION) {
            continue;
        }
        match std::fs::read(&path) {
            Ok(bytes) => out.push(bytes),
            Err(e) => {
                warn!(
                    path = %path.display(),
                    error = %e,
                    "failed to read protoset; skipping"
                );
            }
        }
    }
    out
}

#[cfg(test)]
#[path = "descriptors.test.rs"]
mod tests;
