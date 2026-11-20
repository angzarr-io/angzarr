//! The Unix socket policy: a socket of mode 0600 in a directory of mode 0700
//! that we own; a stale socket file replaced, a live one refused, a file
//! that is not a socket never touched; the socket removed when its guard
//! drops.

use std::fs::{self, DirBuilder, OpenOptions};
use std::io::ErrorKind;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use tokio::net::UnixListener;

use crate::ListenError;

/// A socket file this process created: removed when it drops, if the path
/// still holds that very file (another listener's socket, put there after
/// ours was removed, is left alone).
#[derive(Debug)]
pub(crate) struct OwnedSocket {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl OwnedSocket {
    fn holds_our_file(&self) -> bool {
        fs::symlink_metadata(&self.path).is_ok_and(|meta| {
            meta.file_type().is_socket() && meta.dev() == self.device && meta.ino() == self.inode
        })
    }
}

impl Drop for OwnedSocket {
    fn drop(&mut self) {
        if self.holds_our_file() {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Bind a listener at `path` under the policy.
pub(crate) fn bind(path: &Path) -> Result<(UnixListener, OwnedSocket), ListenError> {
    let dir = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => return Err(ListenError::no_directory(path)),
    };
    ensure_private_directory(dir)?;
    clear_stale(path)?;
    let listener = UnixListener::bind(path).map_err(|error| match error.kind() {
        ErrorKind::AddrInUse => ListenError::socket_in_use(path),
        _ => ListenError::io("bind", path.display())(error),
    })?;
    let meta = fs::symlink_metadata(path).map_err(ListenError::io("stat", path.display()))?;
    // From here the guard removes the file again if anything fails.
    let guard = OwnedSocket {
        path: path.to_path_buf(),
        device: meta.dev(),
        inode: meta.ino(),
    };
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .map_err(ListenError::io("chmod", path.display()))?;
    Ok((listener, guard))
}

/// `dir` exists (created with mode 0700 when it does not) and is a
/// directory we own with mode 0700.
fn ensure_private_directory(dir: &Path) -> Result<(), ListenError> {
    match fs::metadata(dir) {
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
            .map_err(ListenError::io("create-directory", dir.display()))?,
        Err(error) => return Err(ListenError::io("stat", dir.display())(error)),
    }
    let meta = fs::metadata(dir).map_err(ListenError::io("stat", dir.display()))?;
    if !meta.is_dir() || meta.mode() & 0o777 != 0o700 {
        return Err(ListenError::directory_not_private(dir));
    }
    judge_owner(dir, meta.uid(), our_uid(dir))
}

/// Whether `dir`, owned by `owner`, is ours, given what writing a file in it
/// said our own uid is: a directory of mode 0700 that is not ours is one we
/// cannot even write a file in (or, as root, one whose owner differs).
fn judge_owner(dir: &Path, owner: u32, ours: std::io::Result<u32>) -> Result<(), ListenError> {
    match ours {
        Ok(uid) if uid == owner => Ok(()),
        Ok(_) => Err(ListenError::directory_not_private(dir)),
        Err(error) if error.kind() == ErrorKind::PermissionDenied => {
            Err(ListenError::directory_not_private(dir))
        }
        Err(error) => Err(ListenError::io("probe-owner", dir.display())(error)),
    }
}

/// Our effective user id, as the owner of a file we create in `dir`
/// (std has no way to ask).
fn our_uid(dir: &Path) -> std::io::Result<u32> {
    let probe = dir.join(format!(".angzarr-owner-{}", std::process::id()));
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&probe)?;
    let uid = file.metadata().map(|meta| meta.uid());
    drop(file);
    let _ = fs::remove_file(&probe);
    uid
}

/// Nothing at `path`, or a stale socket removed: a file that is not a
/// socket is refused untouched, a socket someone listens on is refused.
fn clear_stale(path: &Path) -> Result<(), ListenError> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ListenError::io("stat", path.display())(error)),
        Ok(meta) if !meta.file_type().is_socket() => Err(ListenError::not_a_socket(path)),
        Ok(_) => judge_probe(path, UnixStream::connect(path).map(drop)),
    }
}

/// What to do with the socket file at `path`, given what connecting to it
/// said: someone answered (refused), nobody listens (the stale file is
/// removed), it vanished meanwhile (nothing to do), or anything else (the
/// probe failed: refused, the file kept).
fn judge_probe(path: &Path, probe: std::io::Result<()>) -> Result<(), ListenError> {
    match probe {
        Ok(()) => Err(ListenError::socket_in_use(path)),
        Err(error) if error.kind() == ErrorKind::ConnectionRefused => {
            fs::remove_file(path).map_err(ListenError::io("remove-stale", path.display()))
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ListenError::io("probe", path.display())(error)),
    }
}

#[cfg(test)]
#[path = "uds.test.rs"]
mod tests;
