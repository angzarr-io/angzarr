//! A process started DETACHED ([`spawn_detached`]): meant to outlive its
//! caller, out of the caller's session, process group and console, and
//! holding no handle of the caller's but the stdio it is given.
//!
//! What it is started as is a [`Detach`], not a [`std::process::Command`]:
//! on Windows the process is created here, not by std (std creates every
//! child inheriting every inheritable handle of its caller's, and has no
//! stable way to name the ones it should), and a `Command`'s environment
//! cannot be read back whole (whether it was cleared).

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;

use crate::host;

/// A detached process's standard stream: null unless set.
#[derive(Debug, Default)]
pub enum Stream {
    /// The null device: reads see an end, writes are discarded.
    #[default]
    Null,
    /// A new pipe, the caller's end taken from the [`Detached`]
    /// ([`Detached::take_stdin`], ...).
    Piped,
    /// This file (or pipe end), given to the process; the caller's copy is
    /// closed once it is started.
    File(File),
}

/// What [`spawn_detached`] starts: a program, its arguments, its
/// environment and working directory, and its three standard streams,
/// each [`Stream::Null`] unless set. A builder, as
/// [`std::process::Command`] is.
#[derive(Debug)]
pub struct Detach {
    pub(crate) program: OsString,
    pub(crate) args: Vec<OsString>,
    pub(crate) env_clear: bool,
    /// Each change in the order made: a value set, or `None` removed.
    pub(crate) env: Vec<(OsString, Option<OsString>)>,
    pub(crate) dir: Option<PathBuf>,
    pub(crate) stdin: Stream,
    pub(crate) stdout: Stream,
    pub(crate) stderr: Stream,
}

impl Detach {
    /// Start `program`: a path, or a name the host searches for (unix: the
    /// `PATH` the process is given; Windows: `CreateProcess`'s search, the
    /// caller's directories and `PATH`, `.exe` added to a name without an
    /// extension). A full path is the one sure way on every host.
    pub fn new(program: impl AsRef<OsStr>) -> Self {
        Self {
            program: program.as_ref().to_owned(),
            args: Vec::new(),
            env_clear: false,
            env: Vec::new(),
            dir: None,
            stdin: Stream::Null,
            stdout: Stream::Null,
            stderr: Stream::Null,
        }
    }

    /// Add one argument.
    pub fn arg(&mut self, arg: impl AsRef<OsStr>) -> &mut Self {
        self.args.push(arg.as_ref().to_owned());
        self
    }

    /// Add arguments, in order.
    pub fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.args
            .extend(args.into_iter().map(|a| a.as_ref().to_owned()));
        self
    }

    /// Set one variable of the process's environment.
    pub fn env(&mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> &mut Self {
        self.env
            .push((key.as_ref().to_owned(), Some(value.as_ref().to_owned())));
        self
    }

    /// Set variables of the process's environment, in order.
    pub fn envs<I, K, V>(&mut self, vars: I) -> &mut Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        for (key, value) in vars {
            self.env(key, value);
        }
        self
    }

    /// Leave one variable out of the process's environment.
    pub fn env_remove(&mut self, key: impl AsRef<OsStr>) -> &mut Self {
        self.env.push((key.as_ref().to_owned(), None));
        self
    }

    /// Start from an empty environment, not the caller's, and forget the
    /// variables set so far: only those set after it are set.
    pub fn env_clear(&mut self) -> &mut Self {
        self.env_clear = true;
        self.env.clear();
        self
    }

    /// The process's working directory (else the caller's).
    pub fn current_dir(&mut self, dir: impl AsRef<Path>) -> &mut Self {
        self.dir = Some(dir.as_ref().to_owned());
        self
    }

    /// The process's stdin.
    pub fn stdin(&mut self, stream: Stream) -> &mut Self {
        self.stdin = stream;
        self
    }

    /// The process's stdout.
    pub fn stdout(&mut self, stream: Stream) -> &mut Self {
        self.stdout = stream;
        self
    }

    /// The process's stderr.
    pub fn stderr(&mut self, stream: Stream) -> &mut Self {
        self.stderr = stream;
        self
    }
}

/// A process started by [`spawn_detached`]. Dropping it leaves the process
/// running (on unix it is reaped by init once the caller has exited, a
/// zombie until then if it exits first and is not waited for).
#[derive(Debug)]
pub struct Detached {
    process: host::Process,
    stdin: Option<File>,
    stdout: Option<File>,
    stderr: Option<File>,
}

impl Detached {
    /// The process's id.
    pub fn id(&self) -> u32 {
        self.process.id()
    }

    /// The caller's end of the process's stdin, once; `None` unless it was
    /// [`Stream::Piped`], or once taken.
    pub fn take_stdin(&mut self) -> Option<File> {
        self.stdin.take()
    }

    /// The caller's end of the process's stdout, once; `None` unless it was
    /// [`Stream::Piped`], or once taken.
    pub fn take_stdout(&mut self) -> Option<File> {
        self.stdout.take()
    }

    /// The caller's end of the process's stderr, once; `None` unless it was
    /// [`Stream::Piped`], or once taken.
    pub fn take_stderr(&mut self) -> Option<File> {
        self.stderr.take()
    }

    /// Wait for the process to exit and return its status; the same status
    /// ever after.
    pub fn wait(&mut self) -> io::Result<ExitStatus> {
        self.process.wait()
    }

    /// The process's exit status if it has exited, without waiting.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.process.try_wait()
    }

    /// Kill the process (SIGKILL on unix, `TerminateProcess` on Windows);
    /// one already exited is `Ok`. It is not waited for.
    pub fn kill(&mut self) -> io::Result<()> {
        self.process.kill()
    }
}

/// Start `detach` DETACHED: a process meant to outlive the caller, in
/// none of its session, process group or console, so nothing that ends
/// them ends it (a terminal's hangup or Ctrl-C, a kill of the caller's
/// group), and holding no handle of the caller's but its three standard
/// streams, so a reader of a pipe the caller holds (its own output, or one
/// it inherited unnamed) sees its end when the caller exits, not when the
/// process does.
///
/// - unix: the process calls `setsid` before the exec, so it leads a new
///   session and a new process group, with no controlling terminal; every
///   descriptor above stderr is made close-on-exec, so none crosses the
///   exec (std opens its own so; one the caller inherited without it would
///   cross).
/// - Windows: created here with `DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP`
///   (no console, no console events of the caller's), with
///   `CREATE_BREAKAWAY_FROM_JOB` so it leaves a job the caller is in, and
///   an explicit `PROC_THREAD_ATTRIBUTE_HANDLE_LIST` naming its three
///   standard handles alone. A job that does not allow breakaway refuses
///   that, and the process is then started without it, in the caller's
///   job: a [`crate::process::Tree`] of this crate is such a job (its kill
///   reaches the process; its close does not, as it does not kill on
///   close). The three handles are inheritable while it is created, and a
///   child another thread of the caller's starts through std meanwhile
///   inherits them too. A batch file (`.bat`, `.cmd`) is refused
///   (`InvalidInput`): cmd.exe quotes by rules of its own; run `cmd /c`.
///
/// A program that cannot be started is the `io::Error` the host gives
/// (`NotFound` for one that is not there).
pub fn spawn_detached(detach: Detach) -> io::Result<Detached> {
    let (process, [stdin, stdout, stderr]) = host::spawn_detached(detach)?;
    Ok(Detached {
        process,
        stdin,
        stdout,
        stderr,
    })
}

#[cfg(test)]
#[path = "detach.test.rs"]
mod tests;
