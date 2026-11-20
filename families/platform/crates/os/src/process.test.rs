//! A tree's kill reaches every descendant, however the leader ended; none
//! of it by sleeping: a descendant's death is seen as the EOF of a pipe it
//! held, and a hang is a failure by a bounded wait.

use std::io::ErrorKind;
use std::process::Command;

use super::spawn_tree;

#[test]
fn test_a_program_that_cannot_start_is_the_spawn_error() {
    let missing = Command::new("/nonexistent/angzarr-os-process-test");
    let err = spawn_tree(missing).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
}

/// Windows has no ask: nothing is sent and the call says so.
#[cfg(windows)]
#[test]
fn test_request_stop_sends_nothing_on_windows() {
    let mut command = Command::new("cmd");
    command.args(["/C", "exit 0"]);
    let mut child = command.spawn().unwrap();
    assert!(!super::request_stop(&child));
    assert!(child.wait().unwrap().success());
}

#[cfg(unix)]
mod unix {
    use std::io::{BufRead, BufReader, Read};
    use std::os::unix::process::ExitStatusExt;
    use std::process::{ChildStdout, Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;

    use crate::process::{request_stop, spawn_tree, Tree};

    const SIGKILL: i32 = 9;

    /// How long a test waits for what must be prompt before it calls it a
    /// hang: far longer than a kill takes, far shorter than the 30 seconds
    /// a surviving descendant would live.
    const HANG: Duration = Duration::from_secs(15);

    fn sh(script: &str) -> Tree {
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg(script)
            .stdin(Stdio::null())
            .stdout(Stdio::piped());
        spawn_tree(command).unwrap()
    }

    /// The pipe's next line, a pid.
    fn pid_line(out: &mut BufReader<ChildStdout>) -> u32 {
        let mut line = String::new();
        out.read_line(&mut line).unwrap();
        line.trim().parse().expect("a pid line")
    }

    /// Run `f` and fail, rather than hang, if it does not return in time.
    fn promptly<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(f());
        });
        rx.recv_timeout(HANG).expect("did not finish promptly")
    }

    /// Everything left on the pipe, to its EOF: which comes only when no
    /// process holds the write end any more.
    fn to_eof(mut out: BufReader<ChildStdout>) -> Vec<u8> {
        let mut rest = Vec::new();
        out.read_to_end(&mut rest).unwrap();
        rest
    }

    #[test]
    fn test_kill_tree_kills_a_descendant_and_reaps_the_child_promptly() {
        // The leader waits on a background `sleep` that inherits stdout.
        let mut tree = sh("sleep 30 & echo $!; wait");
        let mut out = BufReader::new(tree.take_stdout().unwrap());
        let grandchild = pid_line(&mut out);
        assert_ne!(grandchild, tree.id());
        assert_eq!(tree.try_wait().unwrap(), None);

        promptly(move || {
            tree.kill_tree().unwrap();
            // The leader was killed by the group's SIGKILL, and is reaped.
            let status = tree.try_wait().unwrap().expect("reaped by the kill");
            assert_eq!(status.signal(), Some(SIGKILL));
            // The write end closes only when the `sleep` is gone too.
            assert!(to_eof(out).is_empty());
        });
    }

    #[test]
    fn test_kill_tree_reaches_a_descendant_after_the_child_has_exited() {
        // The leader exits at once; the `sleep` it started holds the pipe.
        let mut tree = sh("sleep 30 & echo $!");
        let mut out = BufReader::new(tree.take_stdout().unwrap());
        let grandchild = pid_line(&mut out);
        assert_ne!(grandchild, 0);

        promptly(move || {
            // wait() does not wait for the descendant holding the pipe.
            assert!(tree.wait().unwrap().success());
            tree.kill_tree().unwrap();
            assert!(to_eof(out).is_empty());
        });
    }

    #[test]
    fn test_kill_tree_is_idempotent_and_keeps_the_exit_status() {
        let mut tree = sh("exit 3");
        assert_eq!(tree.wait().unwrap().code(), Some(3));
        tree.kill_tree().unwrap();
        tree.kill_tree().unwrap();
        assert_eq!(tree.wait().unwrap().code(), Some(3));
        assert_eq!(tree.try_wait().unwrap().unwrap().code(), Some(3));
    }

    #[test]
    fn test_kill_tree_of_a_child_that_has_exited_unreaped_is_ok() {
        // `cat` of a closed stdin exits at once; its status is read only
        // after the kill, which reaps.
        let mut tree = sh("cat");
        drop(tree.take_stdin());
        let mut out = BufReader::new(tree.take_stdout().unwrap());
        // EOF on stdout: the child has closed it by exiting.
        assert!(out.fill_buf().unwrap().is_empty());
        tree.kill_tree().unwrap();
        assert!(tree.try_wait().unwrap().is_some());
    }

    #[test]
    fn test_terminate_tree_lets_a_child_that_stops_on_sigterm_stop() {
        // Ready is printed after the trap is set; a descendant is left.
        let mut tree = sh("trap 'exit 7' TERM; sleep 30 & echo $!; wait");
        let mut out = BufReader::new(tree.take_stdout().unwrap());
        pid_line(&mut out);

        promptly(move || {
            tree.terminate_tree(HANG).unwrap();
            // Its own exit, not the kill's, and the descendant is gone.
            let status = tree.try_wait().unwrap().unwrap();
            assert_eq!(status.code(), Some(7));
            assert!(to_eof(out).is_empty());
        });
    }

    #[test]
    fn test_terminate_tree_kills_a_child_that_ignores_sigterm_when_the_grace_is_up() {
        let mut tree = sh("trap '' TERM; echo ready; sleep 30 & echo $!; wait");
        let mut out = BufReader::new(tree.take_stdout().unwrap());
        let mut ready = String::new();
        out.read_line(&mut ready).unwrap();
        assert_eq!(ready, "ready\n");
        pid_line(&mut out);

        promptly(move || {
            // No grace: the ignored SIGTERM is followed by the kill.
            tree.terminate_tree(Duration::ZERO).unwrap();
            let status = tree.try_wait().unwrap().unwrap();
            assert_eq!(status.signal(), Some(SIGKILL));
            assert!(to_eof(out).is_empty());
        });
    }

    #[test]
    fn test_request_stop_asks_one_child_which_stops_on_its_own() {
        // Ready is printed after the trap is set, so the ask finds it set.
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("trap 'exit 7' TERM; echo ready; i=0; while [ $i -lt 30 ]; do sleep 1; i=$((i+1)); done")
            .stdin(Stdio::null())
            .stdout(Stdio::piped());
        let mut child = command.spawn().unwrap();
        let mut out = BufReader::new(child.stdout.take().unwrap());
        let mut ready = String::new();
        out.read_line(&mut ready).unwrap();
        assert_eq!(ready, "ready\n");

        assert!(request_stop(&child));
        promptly(move || {
            // Its own exit (the trap's 7), not a kill: nothing else was sent.
            let status = child.wait().unwrap();
            assert_eq!(status.code(), Some(7));
        });
    }

    #[test]
    fn test_request_stop_asks_the_child_alone_not_its_group() {
        // The child is in OUR group (a plain spawn); a SIGTERM to the
        // group would reach this test process too, and end it.
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("trap 'exit 7' TERM; echo ready; i=0; while [ $i -lt 30 ]; do sleep 1; i=$((i+1)); done")
            .stdin(Stdio::null())
            .stdout(Stdio::piped());
        let mut child = command.spawn().unwrap();
        let mut out = BufReader::new(child.stdout.take().unwrap());
        let mut ready = String::new();
        out.read_line(&mut ready).unwrap();
        assert_eq!(session_and_group(child.id()).1, session_and_group(0).1);

        assert!(request_stop(&child));
        promptly(move || assert_eq!(child.wait().unwrap().code(), Some(7)));
    }

    #[test]
    fn test_request_stop_never_signals_a_number_that_is_no_pid() {
        // Cast to pid_t it would be -1: every process the caller may signal.
        assert!(!crate::host::request_stop_pid(u32::MAX));
    }

    #[test]
    fn test_the_pipes_the_command_made_are_taken_once() {
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("exit 0")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut tree = spawn_tree(command).unwrap();
        assert!(tree.take_stdin().is_some());
        assert!(tree.take_stdin().is_none());
        assert!(tree.take_stdout().is_some());
        assert!(tree.take_stdout().is_none());
        assert!(tree.take_stderr().is_some());
        assert!(tree.take_stderr().is_none());
        tree.kill_tree().unwrap();
    }

    #[test]
    fn test_pipes_the_command_did_not_make_are_not_there() {
        let mut command = Command::new("sh");
        command.arg("-c").arg("exit 0");
        command.stdin(Stdio::null()).stdout(Stdio::null());
        command.stderr(Stdio::null());
        let mut tree = spawn_tree(command).unwrap();
        assert!(tree.take_stdin().is_none());
        assert!(tree.take_stdout().is_none());
        assert!(tree.take_stderr().is_none());
        tree.kill_tree().unwrap();
    }

    #[test]
    fn test_id_is_the_childs_own_pid() {
        let mut tree = sh("echo $$; wait");
        let mut out = BufReader::new(tree.take_stdout().unwrap());
        assert_eq!(pid_line(&mut out), tree.id());
        tree.kill_tree().unwrap();
    }

    /// The session and process group of `pid` (0: this process).
    fn session_and_group(pid: u32) -> (i32, i32) {
        let pid = i32::try_from(pid).unwrap();
        // SAFETY: plain calls taking a pid; a pid not there is -1.
        let (sid, pgid) = unsafe { (libc::getsid(pid), libc::getpgid(pid)) };
        assert!(sid > 0 && pgid > 0, "pid {pid} has a session and group");
        (sid, pgid)
    }
}
