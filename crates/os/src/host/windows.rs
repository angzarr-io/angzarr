//! The Windows syscalls. Compiled only on Windows, where CI's Windows
//! runner tests them (decision 5). Mutation testing runs on unix, where
//! their mutants cannot be killed, hence each `mutants::skip`.

use std::ffi::OsString;
use std::fs::{self, DirBuilder, File, Metadata, OpenOptions};
use std::io;
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::os::windows::fs::FileTypeExt;
#[cfg(any(test, feature = "test-support"))]
use std::os::windows::process::CommandExt;
use std::path::{Path, PathBuf};
#[cfg(any(test, feature = "test-support"))]
use std::process::{Child, Command};
use std::task::Poll;

use process_wrap::std::{ChildWrapper, CommandWrap, JobObject};
use tokio::signal::windows as signal;

use windows_sys::Win32::Storage::FileSystem::MoveFileExW;
use windows_sys::Win32::System::Com::CoTaskMemFree;
#[cfg(any(test, feature = "test-support"))]
use windows_sys::Win32::System::Console::{GenerateConsoleCtrlEvent, CTRL_BREAK_EVENT};
#[cfg(any(test, feature = "test-support"))]
use windows_sys::Win32::System::Threading::CREATE_NEW_PROCESS_GROUP;
use windows_sys::Win32::UI::Shell::{FOLDERID_Profile, SHGetKnownFolderPath, KF_FLAG_DEFAULT};

use crate::{Flavor, LockKind, Mode, Os, Signal, Signals, StopSignal};

mod detach;
pub(crate) use detach::{spawn_detached, Process};

#[derive(Debug)]
pub(crate) struct Host;

#[cfg_attr(test, mutants::skip)]
impl Os for Host {
    fn flavor(&self) -> Flavor {
        Flavor::Windows
    }

    fn set_mode(&self, path: &Path, _mode: Mode) -> io::Result<()> {
        // No modes: only what is missing is refused.
        fs::metadata(path).map(drop)
    }

    fn set_file_mode(&self, _file: &File, _mode: Mode) -> io::Result<()> {
        Ok(())
    }

    fn mode_of(&self, _meta: &Metadata) -> Option<Mode> {
        None
    }

    fn open_mode(&self, _options: &mut OpenOptions, _mode: Mode) {}

    fn dir_mode(&self, _builder: &mut DirBuilder, _mode: Mode) {}

    fn link_dir(&self, target: &Path, link: &Path) -> io::Result<()> {
        let parent = link.parent().unwrap_or(Path::new(""));
        junction::create(parent.join(target), link)
    }

    fn read_link(&self, link: &Path) -> io::Result<PathBuf> {
        if !fs::symlink_metadata(link)?.file_type().is_symlink() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "not a link"));
        }
        junction::get_target(link)
    }

    fn remove_link(&self, path: &Path) -> io::Result<()> {
        if fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink_dir()) {
            // A junction is a directory: removing it removes the link alone.
            return fs::remove_dir(path);
        }
        fs::remove_file(path)
    }

    fn same_file(&self, file: &File, path: &Path) -> io::Result<bool> {
        let there = same_file::Handle::from_path(path)?;
        Ok(same_file::Handle::from_file(file.try_clone()?)? == there)
    }

    // MoveFileExW without MOVEFILE_REPLACE_EXISTING: a file at `to` is
    // ERROR_ALREADY_EXISTS (or ERROR_FILE_EXISTS), which std reads as
    // AlreadyExists. Without MOVEFILE_COPY_ALLOWED, as std's rename, it
    // never copies across volumes.
    fn rename_noreplace(&self, from: &Path, to: &Path) -> io::Result<()> {
        let (from, to) = (wide_path(from)?, wide_path(to)?);
        // SAFETY: both pointers are to NUL-terminated wide strings held
        // for the call.
        let moved = unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), 0) };
        if moved == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    fn sync_dir(&self, _dir: &Path) -> io::Result<()> {
        // NTFS makes a rename durable with the file; a directory cannot be
        // opened to sync.
        Ok(())
    }

    fn try_lock(&self, file: &File, kind: LockKind) -> io::Result<bool> {
        super::try_lock(file, kind)
    }

    // Go's os.Interrupt and SIGTERM on Windows: Ctrl-C and Ctrl-Break, and
    // the console's close, the user's logoff and the system's shutdown.
    fn stop_signal(&self) -> io::Result<StopSignal> {
        let mut signals = self.signals(&Signal::ALL)?;
        Ok(Box::pin(async move {
            signals.recv().await;
        }))
    }

    // Each console event is heard as the signal of its intent (the
    // `signal` module's docs).
    fn signals(&self, heard: &[Signal]) -> io::Result<Signals> {
        let mut events: Vec<(Signal, Event)> = Vec::new();
        if heard.contains(&Signal::Hangup) {
            events.push((Signal::Hangup, Event::Close(signal::ctrl_close()?)));
        }
        if heard.contains(&Signal::Interrupt) {
            events.push((Signal::Interrupt, Event::C(signal::ctrl_c()?)));
            events.push((Signal::Interrupt, Event::Break(signal::ctrl_break()?)));
        }
        if heard.contains(&Signal::Terminate) {
            events.push((Signal::Terminate, Event::Logoff(signal::ctrl_logoff()?)));
            events.push((Signal::Terminate, Event::Shutdown(signal::ctrl_shutdown()?)));
        }
        Ok(Signals::from_source(move |cx| {
            for (kind, event) in &mut events {
                if event.poll_recv(cx).is_ready() {
                    return Poll::Ready(*kind);
                }
            }
            Poll::Pending
        }))
    }

    // The profile folder the system keeps for the user (the registry's
    // ProfileImagePath), not USERPROFILE: the known folder reads no
    // variable.
    fn user_home(&self) -> io::Result<PathBuf> {
        let mut path: *mut u16 = std::ptr::null_mut();
        // SAFETY: the GUID is a constant's, the token null (the calling
        // user), and `path` receives a string the call allocates.
        let result = unsafe {
            SHGetKnownFolderPath(
                &FOLDERID_Profile,
                KF_FLAG_DEFAULT as u32,
                std::ptr::null_mut(),
                &mut path,
            )
        };
        let home = if result < 0 || path.is_null() {
            Err(io::Error::from_raw_os_error(result))
        } else {
            // SAFETY: on success `path` is a NUL-terminated wide string.
            Ok(PathBuf::from(unsafe { wide_string(path) }))
        };
        // SAFETY: the string is freed once, by the allocator that made it,
        // as the call asks even when it failed (null is a no-op).
        unsafe { CoTaskMemFree(path.cast()) };
        home
    }
}

/// `path` as the NUL-terminated wide string a call takes; `InvalidInput`
/// when it holds a NUL.
#[cfg_attr(test, mutants::skip)]
fn wide_path(path: &Path) -> io::Result<Vec<u16>> {
    let units: Vec<u16> = path.as_os_str().encode_wide().collect();
    if units.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a path holding a NUL",
        ));
    }
    Ok(units.into_iter().chain([0]).collect())
}

/// The NUL-terminated wide string at `at`.
///
/// # Safety
///
/// `at` is a NUL-terminated wide string, valid while this runs.
#[cfg_attr(test, mutants::skip)]
unsafe fn wide_string(at: *const u16) -> OsString {
    let mut len = 0;
    while unsafe { *at.add(len) } != 0 {
        len += 1;
    }
    OsString::from_wide(unsafe { std::slice::from_raw_parts(at, len) })
}

/// One console event's listener.
enum Event {
    C(signal::CtrlC),
    Break(signal::CtrlBreak),
    Close(signal::CtrlClose),
    Logoff(signal::CtrlLogoff),
    Shutdown(signal::CtrlShutdown),
}

impl Event {
    #[cfg_attr(test, mutants::skip)]
    fn poll_recv(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Option<()>> {
        match self {
            Event::C(e) => e.poll_recv(cx),
            Event::Break(e) => e.poll_recv(cx),
            Event::Close(e) => e.poll_recv(cx),
            Event::Logoff(e) => e.poll_recv(cx),
            Event::Shutdown(e) => e.poll_recv(cx),
        }
    }
}

#[cfg_attr(test, mutants::skip)]
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn own_group(command: &mut Command) -> &mut Command {
    command.creation_flags(CREATE_NEW_PROCESS_GROUP)
}

#[cfg_attr(test, mutants::skip)]
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn ctrl_break(child: &Child) -> io::Result<()> {
    // SAFETY: a plain call taking two integers; a group that is not there
    // is an error it returns.
    if unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, child.id()) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

pub(crate) fn not_unicode(prefix: &str) -> OsString {
    let mut wide: Vec<u16> = prefix.encode_utf16().collect();
    wide.push(0xD800);
    OsString::from_wide(&wide)
}

#[cfg_attr(test, mutants::skip)]
pub(crate) fn wrap_tree(wrap: &mut CommandWrap) {
    wrap.wrap(JobObject);
}

// A job has no graceful stop: the kill that follows is the stop.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn request_stop(_child: &dyn ChildWrapper) -> bool {
    false
}

// Windows has no graceful stop of one process: the caller's kill is the stop.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn request_stop_pid(_pid: u32) -> bool {
    false
}

// No endpoint is measured on Windows (`crate::socket`): the post is said
// unsupported, never tried over another transport.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn post_local(
    _path: &Path,
    _bytes: &[u8],
    _write_timeout: std::time::Duration,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "no local stream socket is posted to on Windows",
    ))
}
