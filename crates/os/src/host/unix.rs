//! The unix syscalls.

use std::ffi::{CStr, CString, OsString};
use std::fs::{self, DirBuilder, File, Metadata, OpenOptions};
use std::io;
use std::os::fd::OwnedFd;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::task::Poll;

use process_wrap::std::{ChildWrapper, CommandWrap, ProcessGroup};
use tokio::signal::unix::{signal, SignalKind};

use crate::detach::{Detach, Stream};
use crate::{Flavor, LockKind, Mode, Os, Signal, Signals, StopSignal};

#[derive(Debug)]
pub(crate) struct Host;

impl Os for Host {
    fn flavor(&self) -> Flavor {
        Flavor::Unix
    }

    fn set_mode(&self, path: &Path, mode: Mode) -> io::Result<()> {
        fs::set_permissions(path, fs::Permissions::from_mode(mode.bits()))
    }

    fn set_file_mode(&self, file: &File, mode: Mode) -> io::Result<()> {
        file.set_permissions(fs::Permissions::from_mode(mode.bits()))
    }

    fn mode_of(&self, meta: &Metadata) -> Option<Mode> {
        Some(Mode::from_bits(meta.permissions().mode()))
    }

    fn open_mode(&self, options: &mut OpenOptions, mode: Mode) {
        options.mode(mode.bits());
    }

    fn dir_mode(&self, builder: &mut DirBuilder, mode: Mode) {
        builder.mode(mode.bits());
    }

    fn link_dir(&self, target: &Path, link: &Path) -> io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    fn read_link(&self, link: &Path) -> io::Result<PathBuf> {
        fs::read_link(link)
    }

    fn remove_link(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn same_file(&self, file: &File, path: &Path) -> io::Result<bool> {
        let (held, there) = (file.metadata()?, fs::metadata(path)?);
        Ok(held.dev() == there.dev() && held.ino() == there.ino())
    }

    fn rename_noreplace(&self, from: &Path, to: &Path) -> io::Result<()> {
        rename_exclusive(&c_path(from)?, &c_path(to)?)
    }

    // Durability across a crash is not observable from a test: only that
    // the directory is opened (a missing one is NotFound) is.
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        File::open(dir)?.sync_all()
    }

    fn try_lock(&self, file: &File, kind: LockKind) -> io::Result<bool> {
        super::try_lock(file, kind)
    }

    fn stop_signal(&self) -> io::Result<StopSignal> {
        let mut signals = self.signals(&[Signal::Interrupt, Signal::Terminate])?;
        Ok(Box::pin(async move {
            signals.recv().await;
        }))
    }

    fn signals(&self, heard: &[Signal]) -> io::Result<Signals> {
        let mut streams = Vec::new();
        for &kind in Signal::ALL.iter().filter(|kind| heard.contains(kind)) {
            streams.push((kind, signal(kind_of(kind))?));
        }
        // In order, stopping at the first heard: each one passed has
        // registered its waker, and a caller polls again after a signal.
        Ok(Signals::from_source(move |cx| {
            for (kind, stream) in &mut streams {
                if stream.poll_recv(cx).is_ready() {
                    return Poll::Ready(*kind);
                }
            }
            Poll::Pending
        }))
    }

    fn user_home(&self) -> io::Result<PathBuf> {
        account_home(FIRST_BUFFER, LAST_BUFFER)
    }
}

/// `path` as the C string a system call takes; `InvalidInput` when it
/// holds a NUL.
fn c_path(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "a path holding a NUL"))
}

/// Rename `from` to `to` unless something is at `to` (`EEXIST`), in one
/// system call: `renameat2` with `RENAME_NOREPLACE` on Linux (3.15, by
/// number, so no libc wrapper's version is needed).
#[cfg(target_os = "linux")]
fn rename_exclusive(from: &CStr, to: &CStr) -> io::Result<()> {
    // SAFETY: both pointers are to NUL-terminated strings the caller
    // holds for the call; the directories are the working directory's
    // (both paths are absolute or the caller's relative ones).
    let status = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            from.as_ptr(),
            libc::AT_FDCWD,
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Rename `from` to `to` unless something is at `to` (`EEXIST`), in one
/// call: `renamex_np` with `RENAME_EXCL` on macOS (10.12).
// Compiled only on macOS, where CI's macOS runner tests it; mutation runs
// on Linux, where its mutants cannot be killed.
#[cfg(target_os = "macos")]
#[cfg_attr(test, mutants::skip)]
fn rename_exclusive(from: &CStr, to: &CStr) -> io::Result<()> {
    // SAFETY: both pointers are to NUL-terminated strings the caller
    // holds for the call.
    let status = unsafe { libc::renamex_np(from.as_ptr(), to.as_ptr(), libc::RENAME_EXCL) };
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Another unix has no rename that refuses a target in one call:
/// `Unsupported`, never a check and a rename that could race.
// Compiled on no host CI or mutation runs on: its mutants cannot be killed.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
#[cfg_attr(test, mutants::skip)]
fn rename_exclusive(_from: &CStr, _to: &CStr) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "no rename that refuses an existing target on this system",
    ))
}

/// getpwuid_r's first buffer for the entry's strings: what glibc's
/// `_SC_GETPW_R_SIZE_MAX` says, and more than an ordinary entry needs.
const FIRST_BUFFER: usize = 1024;

/// The most getpwuid_r's buffer is grown to: an entry needing more is
/// not an account's (one MiB).
const LAST_BUFFER: usize = 1_048_576;

/// The effective uid's home directory from the account database, with a
/// buffer of `start` bytes (at least one) for the entry's strings,
/// doubled while the entry does not fit (`ERANGE`) and the buffer is
/// shorter than `last`; past that, the `ERANGE`. No entry, or one with no
/// home or an empty one, is `NotFound`.
pub(crate) fn account_home(start: usize, last: usize) -> io::Result<PathBuf> {
    // SAFETY: a plain call; it cannot fail.
    let uid = unsafe { libc::geteuid() };
    let mut buffer = vec![0u8; start];
    loop {
        // SAFETY: an all-zero passwd is a valid value of the plain struct
        // (null pointers, zero ids); getpwuid_r fills it.
        let mut entry: libc::passwd = unsafe { std::mem::zeroed() };
        let mut found: *mut libc::passwd = std::ptr::null_mut();
        // SAFETY: every pointer is to memory this frame owns, `buffer`'s
        // length is the length given, and the entry's strings point into
        // `buffer`, read below while it lives.
        let status = unsafe {
            libc::getpwuid_r(
                uid,
                &mut entry,
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                &mut found,
            )
        };
        match status {
            libc::ERANGE if buffer.len() < last => buffer.resize(buffer.len() * 2, 0),
            // SAFETY: `found`, when not null, is `entry`, whose pw_dir is
            // null or a NUL-terminated string in `buffer`.
            0 => return unsafe { home_of(found.as_ref().map(|entry| entry.pw_dir.cast_const())) },
            error => return Err(io::Error::from_raw_os_error(error)),
        }
    }
}

/// The home directory an account entry names: `dir`, the entry's
/// pw_dir, or `None` for no entry. No entry, a null or an empty pw_dir is
/// `NotFound`.
///
/// # Safety
///
/// A `dir` that is not null is a NUL-terminated string, valid while this
/// runs.
pub(crate) unsafe fn home_of(dir: Option<*const libc::c_char>) -> io::Result<PathBuf> {
    let dir = match dir {
        // SAFETY: the caller's promise.
        Some(dir) if !dir.is_null() => unsafe { CStr::from_ptr(dir) },
        _ => return Err(no_home()),
    };
    if dir.is_empty() {
        return Err(no_home());
    }
    Ok(PathBuf::from(OsString::from_vec(dir.to_bytes().to_vec())))
}

fn no_home() -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, "the account has no home directory")
}

/// A detached process: std's child, its stdio already taken.
#[derive(Debug)]
pub(crate) struct Process(Child);

impl Process {
    pub(crate) fn id(&self) -> u32 {
        self.0.id()
    }

    pub(crate) fn wait(&mut self) -> io::Result<ExitStatus> {
        self.0.wait()
    }

    pub(crate) fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.0.try_wait()
    }

    pub(crate) fn kill(&mut self) -> io::Result<()> {
        self.0.kill()
    }
}

/// The highest descriptor a process with no `close_range` makes
/// close-on-exec, one past it: its open-file limit, at most this many.
const DESCRIPTOR_CAP: libc::c_int = 1 << 16;

/// One past the highest descriptor this process can have open: its soft
/// open-file limit, capped at [`DESCRIPTOR_CAP`] (a limit that is not read
/// is the cap).
fn descriptor_limit() -> libc::c_int {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: a plain call filling `limit`, which outlives it.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return DESCRIPTOR_CAP;
    }
    libc::c_int::try_from(limit.rlim_cur).map_or(DESCRIPTOR_CAP, |cur| cur.min(DESCRIPTOR_CAP))
}

/// Make every descriptor above stderr close-on-exec, in a forked child
/// before its exec: `close_range` on Linux, else [`cloexec_below`] `limit`.
/// Async-signal-safe: system calls alone, nothing allocated.
// On Linux, where mutation runs, close_range succeeds, so a mutant that
// also runs the loop, or skips it, changes nothing: it cannot be killed.
#[cfg_attr(test, mutants::skip)]
fn cloexec_above_stderr(limit: libc::c_int) {
    if !close_range_cloexec() {
        cloexec_below(limit);
    }
}

/// Whether `close_range` made every descriptor above stderr close-on-exec:
/// Linux 5.11 or later; never elsewhere.
// A mutant that says it failed only sends the caller to `cloexec_below`,
// which does the same: it is equivalent.
#[cfg_attr(test, mutants::skip)]
fn close_range_cloexec() -> bool {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: a plain system call taking integers.
        let all = unsafe {
            libc::syscall(
                libc::SYS_close_range,
                3 as libc::c_uint,
                libc::c_uint::MAX,
                libc::CLOSE_RANGE_CLOEXEC,
            )
        };
        all == 0
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// Make each descriptor from 3 up to `limit` (not included) close-on-exec;
/// one not open is an error it ignores.
fn cloexec_below(limit: libc::c_int) {
    for fd in 3..limit {
        // SAFETY: a plain call on a descriptor number; one not open fails.
        unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
    }
}

fn stdio(stream: Stream) -> Stdio {
    match stream {
        Stream::Null => Stdio::null(),
        Stream::Piped => Stdio::piped(),
        Stream::File(file) => Stdio::from(file),
    }
}

/// Start `detach` leading a new session (and so a new process group),
/// with no controlling terminal: `setsid` in the child, before the exec,
/// and every descriptor above stderr made close-on-exec. A forked child
/// never leads a group, so the call is not refused; one that did not make
/// the child its session's leader fails the spawn.
pub(crate) fn spawn_detached(detach: Detach) -> io::Result<(Process, [Option<File>; 3])> {
    let mut command = Command::new(&detach.program);
    command.args(&detach.args);
    if detach.env_clear {
        command.env_clear();
    }
    for (key, value) in &detach.env {
        match value {
            Some(value) => command.env(key, value),
            None => command.env_remove(key),
        };
    }
    if let Some(dir) = &detach.dir {
        command.current_dir(dir);
    }
    command
        .stdin(stdio(detach.stdin))
        .stdout(stdio(detach.stdout))
        .stderr(stdio(detach.stderr));
    let limit = descriptor_limit();
    // SAFETY: the closure runs in the forked child before the exec, and
    // calls only setsid, getpid, close_range and fcntl, which are
    // async-signal-safe, allocating nothing.
    unsafe {
        command.pre_exec(move || {
            // The new session's id is the caller's pid; -1 on failure.
            if libc::setsid() != libc::getpid() {
                return Err(io::Error::last_os_error());
            }
            cloexec_above_stderr(limit);
            Ok(())
        });
    }
    let mut child = command.spawn()?;
    let ends = [
        child.stdin.take().map(|e| File::from(OwnedFd::from(e))),
        child.stdout.take().map(|e| File::from(OwnedFd::from(e))),
        child.stderr.take().map(|e| File::from(OwnedFd::from(e))),
    ];
    Ok((Process(child), ends))
}

fn kind_of(signal: Signal) -> SignalKind {
    match signal {
        Signal::Hangup => SignalKind::hangup(),
        Signal::Interrupt => SignalKind::interrupt(),
        Signal::Terminate => SignalKind::terminate(),
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn own_group(command: &mut Command) -> &mut Command {
    command
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn ctrl_break(_child: &Child) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "unix has no console events",
    ))
}

pub(crate) fn not_unicode(prefix: &str) -> OsString {
    let mut bytes = prefix.as_bytes().to_vec();
    bytes.push(0xff);
    OsString::from_vec(bytes)
}

/// SIGTERM's number, which POSIX leaves to the system but every unix
/// target of this workspace (Linux, macOS, the BSDs) gives as 15.
const SIGTERM: i32 = 15;

pub(crate) fn wrap_tree(wrap: &mut CommandWrap) {
    wrap.wrap(ProcessGroup::leader());
}

pub(crate) fn request_stop(child: &dyn ChildWrapper) -> bool {
    child.signal(SIGTERM).is_ok()
}

/// SIGTERM to the one process `pid` (`kill`, never `killpg`). A number
/// over `pid_t`'s range is no process id and is never signalled (cast, it
/// would be negative: a whole group). A child's id is never 0.
pub(crate) fn request_stop_pid(pid: u32) -> bool {
    match libc::pid_t::try_from(pid) {
        // SAFETY: a plain call taking a pid and a signal number.
        Ok(pid) => unsafe { libc::kill(pid, SIGTERM) == 0 },
        Err(_) => false,
    }
}

/// A unix domain socket's post (`crate::socket::post_local`).
pub(crate) fn post_local(
    path: &Path,
    bytes: &[u8],
    write_timeout: std::time::Duration,
) -> io::Result<()> {
    use std::io::Write;
    let mut socket = std::os::unix::net::UnixStream::connect(path)?;
    socket.set_write_timeout(Some(write_timeout))?;
    socket.write_all(bytes)
}

#[cfg(test)]
#[path = "unix.test.rs"]
mod tests;
