//! The host's syscall implementor, chosen by the compile target: the one
//! place in the workspace that does so.

use std::ffi::OsString;
use std::fs::File;
use std::io;
#[cfg(any(test, feature = "test-support"))]
use std::process::{Child, Command};
use std::sync::Arc;

use process_wrap::std::{ChildWrapper, CommandWrap};

use crate::detach::Detach;
use crate::{LockKind, Os};

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
use unix as imp;
#[cfg(all(test, unix))]
pub(crate) use unix::{account_home, home_of};
#[cfg(windows)]
use windows as imp;

pub(crate) fn make() -> Arc<dyn Os> {
    Arc::new(imp::Host)
}

pub(crate) fn not_unicode(prefix: &str) -> OsString {
    imp::not_unicode(prefix)
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn own_group(command: &mut Command) -> &mut Command {
    imp::own_group(command)
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn ctrl_break(child: &Child) -> io::Result<()> {
    imp::ctrl_break(child)
}

/// Make `wrap`'s child lead a new process group (unix) or belong to a new
/// job object (Windows).
pub(crate) fn wrap_tree(wrap: &mut CommandWrap) {
    imp::wrap_tree(wrap);
}

/// A detached process: std's child on unix, its handle and id on Windows.
pub(crate) use imp::Process;

/// Start `detach` outside the spawner's session, holding no handle of the
/// spawner's but its standard streams: a new session on unix (setsid,
/// every other descriptor close-on-exec), a detached process in a new
/// process group on Windows, its inherited handles named. The caller's
/// ends of the piped streams are returned beside it.
pub(crate) fn spawn_detached(detach: Detach) -> io::Result<(Process, [Option<File>; 3])> {
    imp::spawn_detached(detach)
}

/// Ask `child`'s tree to stop, gracefully; whether the host has such a
/// request and it was sent (a tree already gone is not an error: the kill
/// that follows finds it so).
pub(crate) fn request_stop(child: &dyn ChildWrapper) -> bool {
    imp::request_stop(child)
}

/// Ask the one process `pid` to stop, gracefully, never its group; whether
/// the host has such a request and it was sent.
pub(crate) fn request_stop_pid(pid: u32) -> bool {
    imp::request_stop_pid(pid)
}

/// Post `bytes` to the local stream socket at `path` (`crate::socket`).
pub(crate) fn post_local(
    path: &std::path::Path,
    bytes: &[u8],
    write_timeout: std::time::Duration,
) -> io::Result<()> {
    imp::post_local(path, bytes, write_timeout)
}

/// Take `kind` of lock on `file` without waiting: fs4's flock on unix,
/// LockFileEx on Windows, which name no OS here.
fn try_lock(file: &File, kind: LockKind) -> io::Result<bool> {
    let taken = match kind {
        LockKind::Exclusive => fs4::FileExt::try_lock(file),
        LockKind::Shared => fs4::FileExt::try_lock_shared(file),
    };
    match taken {
        Ok(()) => Ok(true),
        Err(fs4::TryLockError::WouldBlock) => Ok(false),
        Err(fs4::TryLockError::Error(e)) => Err(e),
    }
}
