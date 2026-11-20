//! A detached process, created here rather than by std: std creates every
//! child with `bInheritHandles` TRUE and no list, so it holds every
//! inheritable handle of its caller's (its standard handles, when it was
//! given them so, and any it inherited unnamed); stable std has no way to
//! name the ones it should. This one is created with
//! `PROC_THREAD_ATTRIBUTE_HANDLE_LIST` naming its three standard handles
//! alone.
//!
//! Compiled only on Windows, where CI's Windows runner tests it; mutation
//! testing runs on unix, where its mutants cannot be killed, hence each
//! `mutants::skip`.

use std::ffi::{c_void, OsStr, OsString};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::os::windows::process::ExitStatusExt;
use std::process::ExitStatus;
use std::ptr;

use windows_sys::Win32::Foundation::{
    SetHandleInformation, ERROR_ACCESS_DENIED, HANDLE, HANDLE_FLAG_INHERIT, WAIT_OBJECT_0,
    WAIT_TIMEOUT,
};
use windows_sys::Win32::System::Threading::{
    CreateProcessW, DeleteProcThreadAttributeList, GetExitCodeProcess,
    InitializeProcThreadAttributeList, TerminateProcess, UpdateProcThreadAttribute,
    WaitForSingleObject, CREATE_BREAKAWAY_FROM_JOB, CREATE_NEW_PROCESS_GROUP,
    CREATE_UNICODE_ENVIRONMENT, DETACHED_PROCESS, EXTENDED_STARTUPINFO_PRESENT, INFINITE,
    LPPROC_THREAD_ATTRIBUTE_LIST, PROCESS_INFORMATION, PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
    STARTF_USESTDHANDLES, STARTUPINFOEXW, STARTUPINFOW,
};

use crate::detach::{Detach, Stream};

/// A detached process: its handle and id.
#[derive(Debug)]
pub(crate) struct Process {
    handle: OwnedHandle,
    id: u32,
    status: Option<ExitStatus>,
}

impl Process {
    #[cfg_attr(test, mutants::skip)]
    pub(crate) fn id(&self) -> u32 {
        self.id
    }

    #[cfg_attr(test, mutants::skip)]
    pub(crate) fn wait(&mut self) -> io::Result<ExitStatus> {
        if let Some(status) = self.status {
            return Ok(status);
        }
        // SAFETY: a plain call on the handle this owns.
        if unsafe { WaitForSingleObject(self.raw(), INFINITE) } != WAIT_OBJECT_0 {
            return Err(io::Error::last_os_error());
        }
        self.exit_status()
    }

    #[cfg_attr(test, mutants::skip)]
    pub(crate) fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        if let Some(status) = self.status {
            return Ok(Some(status));
        }
        // SAFETY: as in `wait`.
        match unsafe { WaitForSingleObject(self.raw(), 0) } {
            WAIT_OBJECT_0 => self.exit_status().map(Some),
            WAIT_TIMEOUT => Ok(None),
            _ => Err(io::Error::last_os_error()),
        }
    }

    /// One that has already exited refuses the termination with
    /// ERROR_ACCESS_DENIED, which is then no error.
    #[cfg_attr(test, mutants::skip)]
    pub(crate) fn kill(&mut self) -> io::Result<()> {
        // SAFETY: as in `wait`.
        if unsafe { TerminateProcess(self.raw(), 1) } != 0 {
            return Ok(());
        }
        let refused = io::Error::last_os_error();
        if refused.raw_os_error() == Some(ERROR_ACCESS_DENIED as i32) && self.try_wait()?.is_some()
        {
            return Ok(());
        }
        Err(refused)
    }

    #[cfg_attr(test, mutants::skip)]
    fn raw(&self) -> HANDLE {
        self.handle.as_raw_handle()
    }

    #[cfg_attr(test, mutants::skip)]
    fn exit_status(&mut self) -> io::Result<ExitStatus> {
        let mut code = 0;
        // SAFETY: as in `wait`; `code` outlives the call.
        if unsafe { GetExitCodeProcess(self.raw(), &mut code) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let status = ExitStatus::from_raw(code);
        self.status = Some(status);
        Ok(status)
    }
}

/// `handle` made inheritable, so a list may name it.
#[cfg_attr(test, mutants::skip)]
fn inheritable(handle: OwnedHandle) -> io::Result<OwnedHandle> {
    // SAFETY: a plain call on a handle `handle` owns.
    if unsafe {
        SetHandleInformation(
            handle.as_raw_handle(),
            HANDLE_FLAG_INHERIT,
            HANDLE_FLAG_INHERIT,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(handle)
}

/// The process's end of `stream` (inheritable) and the caller's end of a
/// pipe. `input` is whether the process reads it (stdin).
#[cfg_attr(test, mutants::skip)]
fn ends(stream: Stream, input: bool) -> io::Result<(OwnedHandle, Option<File>)> {
    match stream {
        Stream::Null => {
            let null = OpenOptions::new()
                .read(input)
                .write(!input)
                .open(r"\\.\NUL")?;
            Ok((inheritable(OwnedHandle::from(null))?, None))
        }
        Stream::Piped => {
            let (reader, writer) = io::pipe()?;
            let (theirs, ours) = if input {
                (OwnedHandle::from(reader), OwnedHandle::from(writer))
            } else {
                (OwnedHandle::from(writer), OwnedHandle::from(reader))
            };
            Ok((inheritable(theirs)?, Some(File::from(ours))))
        }
        Stream::File(file) => Ok((inheritable(OwnedHandle::from(file))?, None)),
    }
}

/// `text` as a NUL-terminated wide string.
#[cfg_attr(test, mutants::skip)]
fn wide(text: &OsStr) -> Vec<u16> {
    text.encode_wide().chain([0]).collect()
}

/// Append `arg` to `line` quoted as the C runtime's parser reads it back
/// (std's rules): quoted when empty or holding a space or tab, each run of
/// backslashes doubled before a quote, each quote escaped.
#[cfg_attr(test, mutants::skip)]
fn append_arg(line: &mut Vec<u16>, arg: &OsStr) {
    let (space, tab, quote, backslash) = (b' ' as u16, b'\t' as u16, b'"' as u16, b'\\' as u16);
    let units: Vec<u16> = arg.encode_wide().collect();
    let quoted = units.is_empty() || units.iter().any(|u| *u == space || *u == tab);
    if quoted {
        line.push(quote);
    }
    let mut backslashes = 0;
    for unit in units {
        if unit == backslash {
            backslashes += 1;
        } else {
            if unit == quote {
                line.extend(std::iter::repeat_n(backslash, backslashes + 1));
            }
            backslashes = 0;
        }
        line.push(unit);
    }
    if quoted {
        line.extend(std::iter::repeat_n(backslash, backslashes));
        line.push(quote);
    }
}

#[cfg_attr(test, mutants::skip)]
fn invalid(why: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, why.to_owned())
}

/// The command line: the program in quotes (it may hold no quote), then
/// each argument; NUL-terminated.
#[cfg_attr(test, mutants::skip)]
fn command_line(detach: &Detach) -> io::Result<Vec<u16>> {
    let program: Vec<u16> = detach.program.encode_wide().collect();
    if program.contains(&(b'"' as u16)) || program.contains(&0) {
        return Err(invalid("a program's name holds a quote or a NUL"));
    }
    let lower = detach.program.to_string_lossy().to_ascii_lowercase();
    if lower.ends_with(".bat") || lower.ends_with(".cmd") {
        return Err(invalid("a batch file is not started detached: run cmd /c"));
    }
    let mut line = vec![b'"' as u16];
    line.extend(program);
    line.push(b'"' as u16);
    for arg in &detach.args {
        if arg.encode_wide().any(|u| u == 0) {
            return Err(invalid("an argument holds a NUL"));
        }
        line.push(b' ' as u16);
        append_arg(&mut line, arg);
    }
    line.push(0);
    Ok(line)
}

/// The environment block: the caller's (unless cleared) with `detach`'s
/// changes, names compared as Windows compares them (ASCII case ignored),
/// sorted by the upper-cased name, each `NAME=value` NUL-terminated, and
/// the block ended by one more NUL.
#[cfg_attr(test, mutants::skip)]
fn environment(detach: &Detach) -> io::Result<Vec<u16>> {
    let mut vars: Vec<(OsString, OsString)> = if detach.env_clear {
        Vec::new()
    } else {
        std::env::vars_os().collect()
    };
    for (key, value) in &detach.env {
        if key.is_empty() || key.encode_wide().any(|u| u == b'=' as u16 || u == 0) {
            return Err(invalid("a variable's name is empty or holds = or a NUL"));
        }
        vars.retain(|(k, _)| !k.eq_ignore_ascii_case(key));
        if let Some(value) = value {
            vars.push((key.clone(), value.clone()));
        }
    }
    vars.sort_by_key(|(k, _)| k.to_ascii_uppercase());
    let mut block = Vec::new();
    for (key, value) in vars {
        if value.encode_wide().any(|u| u == 0) {
            return Err(invalid("a variable's value holds a NUL"));
        }
        block.extend(key.encode_wide());
        block.push(b'=' as u16);
        block.extend(value.encode_wide());
        block.push(0);
    }
    if block.is_empty() {
        block.push(0);
    }
    block.push(0);
    Ok(block)
}

/// A process-thread attribute list naming `handles` as the only ones the
/// process inherits; deleted when dropped. `handles` must outlive it.
struct HandleList {
    buffer: Vec<u64>,
}

impl HandleList {
    #[cfg_attr(test, mutants::skip)]
    fn new(handles: &[HANDLE]) -> io::Result<Self> {
        let mut size = 0;
        // SAFETY: a null list asks only for the size it needs, which it
        // writes to `size` (and fails, as asked).
        unsafe { InitializeProcThreadAttributeList(ptr::null_mut(), 1, 0, &mut size) };
        if size == 0 {
            return Err(io::Error::last_os_error());
        }
        let mut list = HandleList {
            buffer: vec![0; size.div_ceil(size_of::<u64>())],
        };
        // SAFETY: the buffer is `size` bytes at least, aligned for any
        // field the list holds.
        if unsafe { InitializeProcThreadAttributeList(list.as_ptr(), 1, 0, &mut size) } == 0 {
            // Not initialised: nothing to delete.
            let failed = io::Error::last_os_error();
            std::mem::forget(list);
            return Err(failed);
        }
        // SAFETY: the list is initialised; `handles` outlives it (the
        // caller's promise), and its size is given in bytes.
        let updated = unsafe {
            UpdateProcThreadAttribute(
                list.as_ptr(),
                0,
                PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                handles.as_ptr().cast::<c_void>(),
                std::mem::size_of_val(handles),
                ptr::null_mut(),
                ptr::null(),
            )
        };
        if updated == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(list)
    }

    #[cfg_attr(test, mutants::skip)]
    fn as_ptr(&mut self) -> LPPROC_THREAD_ATTRIBUTE_LIST {
        self.buffer.as_mut_ptr().cast()
    }
}

impl Drop for HandleList {
    #[cfg_attr(test, mutants::skip)]
    fn drop(&mut self) {
        // SAFETY: the list was initialised (`new` forgets one that was not).
        unsafe { DeleteProcThreadAttributeList(self.as_ptr()) };
    }
}

/// Create the process: no console, a process group of its own, breakaway
/// from the caller's job asked first and, refused (ERROR_ACCESS_DENIED: the
/// job does not allow it, and nothing was started), asked again without.
#[cfg_attr(test, mutants::skip)]
fn create(
    line: &[u16],
    env: &[u16],
    dir: Option<&[u16]>,
    startup: &STARTUPINFOEXW,
) -> io::Result<PROCESS_INFORMATION> {
    let detached = DETACHED_PROCESS
        | CREATE_NEW_PROCESS_GROUP
        | CREATE_UNICODE_ENVIRONMENT
        | EXTENDED_STARTUPINFO_PRESENT;
    let attempt = |flags| {
        // CreateProcessW may write to the command line: a copy each time.
        let mut line = line.to_vec();
        let mut info = PROCESS_INFORMATION::default();
        // SAFETY: every pointer is valid for the call: the NUL-terminated
        // command line and directory, the environment block, the startup
        // information (an extended one, as the flag says) whose attribute
        // list names live handles, and `info`, which it fills.
        let created = unsafe {
            CreateProcessW(
                ptr::null(),
                line.as_mut_ptr(),
                ptr::null(),
                ptr::null(),
                1,
                flags,
                env.as_ptr().cast::<c_void>(),
                dir.map_or(ptr::null(), <[u16]>::as_ptr),
                ptr::from_ref(startup).cast::<STARTUPINFOW>(),
                &mut info,
            )
        };
        if created == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(info)
        }
    };
    match attempt(detached | CREATE_BREAKAWAY_FROM_JOB) {
        Err(e) if e.raw_os_error() == Some(ERROR_ACCESS_DENIED as i32) => attempt(detached),
        created => created,
    }
}

#[cfg_attr(test, mutants::skip)]
pub(crate) fn spawn_detached(detach: Detach) -> io::Result<(Process, [Option<File>; 3])> {
    let line = command_line(&detach)?;
    let env = environment(&detach)?;
    let dir = detach.dir.as_ref().map(|d| wide(d.as_os_str()));
    let (stdin, stdin_ours) = ends(detach.stdin, true)?;
    let (stdout, stdout_ours) = ends(detach.stdout, false)?;
    let (stderr, stderr_ours) = ends(detach.stderr, false)?;
    let handles = [
        stdin.as_raw_handle(),
        stdout.as_raw_handle(),
        stderr.as_raw_handle(),
    ];
    let mut list = HandleList::new(&handles)?;
    let mut startup = STARTUPINFOEXW::default();
    startup.StartupInfo.cb = u32::try_from(size_of::<STARTUPINFOEXW>()).expect("a small size");
    startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    startup.StartupInfo.hStdInput = handles[0];
    startup.StartupInfo.hStdOutput = handles[1];
    startup.StartupInfo.hStdError = handles[2];
    startup.lpAttributeList = list.as_ptr();
    let info = create(&line, &env, dir.as_deref(), &startup)?;
    drop(list);
    // The process holds its own copies; these close here.
    drop((stdin, stdout, stderr));
    // SAFETY: CreateProcessW gave both handles to this caller, to close.
    let (handle, thread) = unsafe {
        (
            OwnedHandle::from_raw_handle(info.hProcess),
            OwnedHandle::from_raw_handle(info.hThread),
        )
    };
    drop(thread);
    let process = Process {
        handle,
        id: info.dwProcessId,
        status: None,
    };
    Ok((process, [stdin_ours, stdout_ours, stderr_ours]))
}

#[cfg(test)]
#[path = "detach.test.rs"]
mod tests;
