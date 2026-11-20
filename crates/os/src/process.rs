//! A child started as a TREE the caller can kill: the child and everything
//! it starts, however deep, in one call ([`Tree::kill_tree`]); or started
//! DETACHED ([`spawn_detached`], from a [`Detach`]), to outlive the
//! caller, out of its session and group, holding no handle of the
//! caller's but its stdio; and ONE child asked to stop ([`request_stop`]),
//! whatever group it is in.
//!
//! A process group on unix (the child leads a new one; the kill is a
//! `killpg`), a job object on Windows (the child is assigned to a new one;
//! the kill is `TerminateJobObject`). Both are `process-wrap`'s, behind
//! this module, so a caller needs no `unsafe` and names no third-party
//! type.
//!
//! This module never reads the child's pipes: a descendant that outlives
//! the child and holds its stdout open does not delay [`Tree::wait`], and
//! the caller's own bounded read of the pipe is unaffected. The kill is
//! what ends such a descendant, and so closes the pipe.
//!
//! What a tree does not hold:
//!
//! - A descendant that leaves the tree escapes it: on unix, one that calls
//!   `setsid` or `setpgid`; on Windows, one created with
//!   `CREATE_BREAKAWAY_FROM_JOB` (a job allows that only if its limits do,
//!   and these do not forbid it). A kill reaches what stayed.
//! - Dropping a [`Tree`] does not kill it, as dropping a
//!   [`std::process::Child`] does not: a caller that is done with the tree
//!   kills it, or leaves it running on purpose.
//! - A new process group is not the terminal's foreground group, so a
//!   Ctrl-C at the terminal does not reach the tree. A caller that wants
//!   the tree to stop on Ctrl-C hears it itself ([`crate::Os::stop_signal`])
//!   and ends the tree ([`Tree::terminate_tree`], [`Tree::kill_tree`]).
//! - On unix a group's id is the leader's pid, and a kill signals that
//!   number. While any member lives (a zombie leader included) the number
//!   is not reused; a [`Tree`] kept after its whole group has gone and the
//!   leader been reaped could signal a group that has the number now. A
//!   caller ends a tree, then drops it.

use std::io;
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus};

pub use crate::detach::{spawn_detached, Detach, Detached, Stream};
use std::time::{Duration, Instant};

use process_wrap::std::{ChildWrapper, CommandWrap};

use crate::host;

/// How often [`Tree::terminate_tree`] looks for the child's exit while it
/// is given time to stop.
const POLL: Duration = Duration::from_millis(5);

/// How long a child whose kill was refused is given to finish exiting
/// before the refusal is taken for real ([`Tree::kill_tree`]).
const EXITING: Duration = Duration::from_secs(1);

/// A running child and its descendants, started by [`spawn_tree`].
#[derive(Debug)]
pub struct Tree {
    child: Box<dyn ChildWrapper>,
}

/// Start `command` as a tree: its own process group on unix, a job object
/// on Windows. Its stdio is whatever `command` set; the pipes it made are
/// taken with [`Tree::take_stdin`], [`Tree::take_stdout`] and
/// [`Tree::take_stderr`]. A program that cannot be started is the
/// `io::Error` `Command::spawn` gives.
pub fn spawn_tree(command: Command) -> io::Result<Tree> {
    let mut wrap = CommandWrap::from(command);
    host::wrap_tree(&mut wrap);
    Ok(Tree {
        child: wrap.spawn()?,
    })
}

/// Ask `child`, ONE process, to stop, and say whether the ask was sent.
///
/// - unix: SIGTERM to the child's pid alone, never its process group, so
///   a child sharing the caller's group (a terminal's program) is asked
///   without the caller being asked too. `false` if the signal could not
///   be sent (the process has gone and been reaped).
/// - Windows: there is no ask; nothing is sent, and the answer is `false`.
///
/// A caller that gets `false`, or whose child does not stop in its own
/// time, ends it with [`Child::kill`]. The ask is by pid: a child already
/// waited for has given up its pid, which the system may reuse, so a
/// caller asks only a child it has not yet reaped (an exited child not
/// yet waited for keeps its pid as a zombie, and the ask is harmless).
pub fn request_stop(child: &Child) -> bool {
    host::request_stop_pid(child.id())
}

impl Tree {
    /// The child's process id (the tree's leader).
    pub fn id(&self) -> u32 {
        self.child.id()
    }

    /// The child's stdin pipe, once; `None` if `command` made none or it
    /// was taken.
    pub fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.child.stdin().take()
    }

    /// The child's stdout pipe, once; `None` if `command` made none or it
    /// was taken.
    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.stdout().take()
    }

    /// The child's stderr pipe, once; `None` if `command` made none or it
    /// was taken.
    pub fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.child.stderr().take()
    }

    /// The child's exit status if it has exited (reaping it), without
    /// waiting; the same status ever after.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    /// Wait for the child to exit and return its status; the same status
    /// ever after. It does not wait for a descendant that outlives the
    /// child.
    pub fn wait(&mut self) -> io::Result<ExitStatus> {
        self.child.wait()
    }

    /// Kill the whole tree at once (SIGKILL to the group on unix,
    /// `TerminateJobObject` on Windows) and reap the child. Idempotent: a
    /// tree that has already gone is `Ok`, and so is a second call. After
    /// the child has exited, a call still kills a descendant that outlived
    /// it.
    pub fn kill_tree(&mut self) -> io::Result<()> {
        let killed = self.child.start_kill();
        if killed.is_err() && !self.exits_within(EXITING)? {
            // The kill failed and the child is still there: not a tree
            // that had gone.
            return killed;
        }
        self.child.wait().map(drop)
    }

    /// Whether the child has exited, or does within `grace`. A kill can be
    /// refused for a child that is exiting: macOS's killpg answers EPERM
    /// for a group whose leader is past its last write and not yet a
    /// zombie, where Linux's succeeds.
    // Reached only when a kill is refused, which no Linux test can make
    // happen for a child of its own; CI's macOS runner runs it.
    #[cfg_attr(test, mutants::skip)]
    fn exits_within(&mut self, grace: Duration) -> io::Result<bool> {
        let deadline = Instant::now() + grace;
        loop {
            if self.child.try_wait()?.is_some() {
                return Ok(true);
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            std::thread::sleep(POLL);
        }
    }

    /// Ask the tree to stop, give the child up to `grace` to do so, then
    /// [`kill_tree`](Self::kill_tree) whatever is left. On unix the ask is
    /// SIGTERM to the whole group; on Windows, which has none, the kill is
    /// immediate. The child's exit ends the grace, not its descendants':
    /// they are killed at that point either way.
    pub fn terminate_tree(&mut self, grace: Duration) -> io::Result<()> {
        if host::request_stop(self.child.as_ref()) {
            let deadline = Instant::now() + grace;
            let time_left = || !deadline.saturating_duration_since(Instant::now()).is_zero();
            while self.child.try_wait()?.is_none() && time_left() {
                std::thread::sleep(POLL);
            }
        }
        self.kill_tree()
    }
}

#[cfg(test)]
#[path = "process.test.rs"]
mod tests;
