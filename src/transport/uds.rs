//! Unix Domain Socket helpers.

use std::path::{Path, PathBuf};

use tracing::info;

/// RAII guard for cleaning up UDS socket files.
pub struct UdsCleanupGuard {
    path: PathBuf,
}

impl UdsCleanupGuard {
    /// Create a new cleanup guard for the given socket path.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Get the socket path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for UdsCleanupGuard {
    fn drop(&mut self) {
        if self.path.exists() {
            if let Err(e) = std::fs::remove_file(&self.path) {
                tracing::warn!(
                    path = %self.path.display(),
                    error = %e,
                    "Failed to clean up UDS socket"
                );
            } else {
                tracing::debug!(
                    path = %self.path.display(),
                    "Cleaned up UDS socket"
                );
            }
        }
    }
}

/// Prepare a UDS socket path for binding.
///
/// - Creates the parent directory if it does not exist, owner-only (0700).
///   An existing directory's permissions are left alone: it may be shared
///   (`/tmp`, a mounted volume) and is not ours to restrict.
/// - Removes a stale socket left at the path by a previous run. Any other
///   kind of file at the path is an error, never deleted.
/// - Returns a cleanup guard that removes the socket on drop
pub fn prepare_uds_socket(path: &Path) -> std::io::Result<UdsCleanupGuard> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
            }
        }
    }

    match std::fs::symlink_metadata(path) {
        Ok(meta) if is_socket(&meta) => {
            info!(path = %path.display(), "Removing stale UDS socket");
            std::fs::remove_file(path)?;
        }
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!(
                    "{} exists and is not a socket; refusing to replace it",
                    path.display()
                ),
            ));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }

    Ok(UdsCleanupGuard::new(path))
}

#[cfg(unix)]
fn is_socket(meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::FileTypeExt;
    meta.file_type().is_socket()
}

#[cfg(not(unix))]
fn is_socket(_meta: &std::fs::Metadata) -> bool {
    false
}
