//! A detached process runs as it was given, outside its caller's session,
//! and holds no handle of its caller's but its stdio; none of it by
//! sleeping: a handle's release is seen as the EOF of a pipe, and a hang is
//! a failure by a bounded wait.

use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use super::{spawn_detached, Detach, Stream};

/// How long the caller's pipes may stay open once the middle process has
/// exited before a detached process is said to hold them: far longer than
/// a process takes to exit, far shorter than the sleeper's minute.
const HELD: Duration = Duration::from_secs(10);

/// What makes [`test_detached_middle`] the middle process of
/// [`test_a_detached_child_holds_no_handle_of_its_callers_but_its_stdio`]
/// rather than a no-op.
const MIDDLE: &str = "ANGZARR_OS_DETACHED_MIDDLE";

/// `script` run by the host's shell: `cmd /C` on Windows, `sh -c` on unix.
fn shell(windows: &str, unix: &str) -> Detach {
    if cfg!(windows) {
        let mut cmd = Detach::new("cmd");
        cmd.args(["/C", windows]);
        cmd
    } else {
        let mut sh = Detach::new("sh");
        sh.args(["-c", unix]);
        sh
    }
}

/// A process that sleeps for a minute, writing nothing.
fn sleeper() -> Detach {
    if cfg!(windows) {
        let mut ping = Detach::new("ping");
        ping.args(["-n", "61", "127.0.0.1"]);
        ping
    } else {
        let mut sleep = Detach::new("sleep");
        sleep.arg("60");
        sleep
    }
}

/// `f`'s answer, or a failure once [`HELD`] has passed.
fn promptly<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(HELD).expect("did not finish promptly")
}

fn read_all(mut pipe: impl Read) -> String {
    let mut text = String::new();
    pipe.read_to_string(&mut text).unwrap();
    text
}

#[test]
fn test_a_detached_program_that_cannot_start_is_the_spawn_error() {
    let missing = Detach::new("/nonexistent/angzarr-os-process-test");
    let err = spawn_detached(missing).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
}

/// A detached process runs as it was given (its arguments) and its exit
/// status is the caller's to read, the same ever after.
#[test]
fn test_a_detached_child_runs_and_its_status_is_read() {
    let mut child = spawn_detached(shell("exit 3", "exit 3")).unwrap();
    assert_eq!(child.wait().unwrap().code(), Some(3));
    assert_eq!(child.wait().unwrap().code(), Some(3));
    assert_eq!(child.try_wait().unwrap().map(|s| s.code()), Some(Some(3)));
}

/// Every stream is null unless set: reads see an end, writes vanish, and
/// no pipe is there to take.
#[test]
fn test_a_detached_childs_streams_are_null_unless_set() {
    let mut child =
        spawn_detached(shell("set /p x= & echo gone", "read x; echo gone; exit 0")).unwrap();
    assert!(child.take_stdin().is_none());
    assert!(child.take_stdout().is_none());
    assert!(child.take_stderr().is_none());
    promptly(move || assert!(child.wait().unwrap().success()));
}

/// A piped stream's caller end is taken once; the process reads what is
/// written to its stdin and writes to its stdout and stderr.
#[test]
fn test_piped_streams_are_the_callers_to_take_once() {
    let mut detach = shell(
        "set /p line= & call echo out %line% & call echo err %line% 1>&2",
        "read line; echo out $line; echo err $line >&2",
    );
    detach
        .stdin(Stream::Piped)
        .stdout(Stream::Piped)
        .stderr(Stream::Piped);
    let mut child = spawn_detached(detach).unwrap();
    let mut stdin = child.take_stdin().unwrap();
    assert!(child.take_stdin().is_none());
    stdin.write_all(b"word\n").unwrap();
    drop(stdin);
    let out = child.take_stdout().unwrap();
    let err = child.take_stderr().unwrap();
    assert!(child.take_stdout().is_none() && child.take_stderr().is_none());
    let (out, err) = promptly(move || (read_all(out), read_all(err)));
    assert_eq!(out.trim_end(), "out word");
    assert_eq!(err.trim_end(), "err word");
    assert!(child.wait().unwrap().success());
}

/// A file given as a stream is the process's: what it writes lands there.
#[test]
fn test_a_file_stream_is_the_processs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out");
    let mut detach = shell("echo to the file", "echo to the file");
    detach.stdout(Stream::File(std::fs::File::create(&path).unwrap()));
    let mut child = spawn_detached(detach).unwrap();
    promptly(move || assert!(child.wait().unwrap().success()));
    assert_eq!(
        std::fs::read_to_string(&path).unwrap().trim_end(),
        "to the file"
    );
}

/// The environment is the caller's with the changes made, or only what
/// was set after it was cleared; the working directory is the one given.
#[test]
fn test_the_environment_and_directory_are_as_given() {
    let dir = tempfile::tempdir().unwrap();
    let mut detach = shell(
        "echo [%ANGZARR_A%][%ANGZARR_B%] & cd",
        "echo \"[$ANGZARR_A][${ANGZARR_B-unset}]\"; pwd",
    );
    detach
        .env("ANGZARR_A", "a")
        .env("ANGZARR_B", "b")
        .env_remove("ANGZARR_B")
        .current_dir(dir.path())
        .stdout(Stream::Piped);
    let mut child = spawn_detached(detach).unwrap();
    let out = child.take_stdout().unwrap();
    let out = promptly(move || read_all(out));
    let mut lines = out.lines();
    let shown = lines.next().unwrap().trim_end();
    if cfg!(windows) {
        // cmd shows an unset variable as its own name.
        assert_eq!(shown, "[a][%ANGZARR_B%]");
    } else {
        assert_eq!(shown, "[a][unset]");
    }
    let here = std::path::PathBuf::from(lines.next().unwrap().trim_end());
    assert_eq!(
        here.canonicalize().unwrap(),
        dir.path().canonicalize().unwrap()
    );
    assert!(child.wait().unwrap().success());

    // Cleared: only what was set after.
    let mut detach = if cfg!(windows) {
        let mut cmd = Detach::new(std::env::var_os("COMSPEC").unwrap());
        cmd.args(["/C", "set"]);
        cmd
    } else {
        Detach::new("/usr/bin/env")
    };
    detach
        .env("ANGZARR_DROPPED", "x")
        .env_clear()
        .env("ANGZARR_KEPT", "k")
        .stdout(Stream::Piped);
    let mut child = spawn_detached(detach).unwrap();
    let out = child.take_stdout().unwrap();
    let out = promptly(move || read_all(out));
    assert!(
        out.lines().any(|l| l.trim_end() == "ANGZARR_KEPT=k"),
        "{out}"
    );
    assert!(!out.contains("ANGZARR_DROPPED"), "{out}");
    assert!(child.wait().unwrap().success());
}

/// A detached process that is killed is gone, and a second kill, of one
/// already exited, is `Ok`.
#[test]
fn test_a_kill_ends_the_process_and_is_idempotent() {
    let mut child = spawn_detached(sleeper()).unwrap();
    assert!(child.try_wait().unwrap().is_none(), "still sleeping");
    child.kill().unwrap();
    let status = promptly(move || {
        let status = child.wait().unwrap();
        child.kill().unwrap();
        status
    });
    assert!(!status.success());
}

/// Run by the test below alone, as the middle process (the caller of
/// [`spawn_detached`]), whose standard handles are that test's pipes and
/// which holds, unnamed, an inheritable pipe of that test's: it starts a
/// detached sleeper with null stdio, prints its pid, and exits.
#[test]
#[ignore = "the middle process of a test, which runs it"]
fn test_detached_middle() {
    if std::env::var_os(MIDDLE).is_none() {
        return;
    }
    let child = spawn_detached(sleeper()).unwrap();
    println!("detached-pid={}", child.id());
}

/// `writer` made inheritable, so a child this process starts holds it
/// without being given it as any stdio: the unnamed handle a process gets
/// from its parent.
fn make_inheritable(writer: &std::io::PipeWriter) {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: a plain call on a descriptor `writer` owns.
        let cleared = unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_SETFD, 0) };
        assert_eq!(cleared, 0, "the pipe is made inheritable");
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Foundation::{SetHandleInformation, HANDLE_FLAG_INHERIT};
        // SAFETY: a plain call on a handle `writer` owns.
        let set = unsafe {
            SetHandleInformation(
                writer.as_raw_handle(),
                HANDLE_FLAG_INHERIT,
                HANDLE_FLAG_INHERIT,
            )
        };
        assert_ne!(set, 0, "the pipe is made inheritable");
    }
}

/// What a reader of one of the middle process's pipes saw.
enum Seen {
    Line(String),
    Eof(&'static str),
}

/// A detached process holds no handle of its caller's but the stdio it is
/// given: once the caller has exited, a reader of the caller's stdout and
/// stderr, and of a pipe the caller inherited and never named, sees each
/// one's EOF at once, though the process lives on. (std on Windows creates
/// every child inheriting every inheritable handle of its caller's; a unix
/// descriptor without close-on-exec crosses every exec.)
#[test]
fn test_a_detached_child_holds_no_handle_of_its_callers_but_its_stdio() {
    let (unnamed, writer) = std::io::pipe().unwrap();
    make_inheritable(&writer);
    let name = module_path!().split_once("::").unwrap().1;
    let mut middle = Command::new(std::env::current_exe().unwrap());
    middle
        .args(["--ignored", "--exact", "--nocapture", "--test-threads=1"])
        .arg(format!("{name}::test_detached_middle"))
        .env(MIDDLE, "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut middle = middle.spawn().unwrap();
    // The middle process holds its own copy; this one would keep it open.
    drop(writer);
    let (tx, rx) = mpsc::channel();
    let read = |what: &'static str, pipe: Box<dyn Read + Send>, tx: mpsc::Sender<Seen>| {
        std::thread::spawn(move || {
            for line in BufReader::new(pipe).lines().map_while(Result::ok) {
                let _ = tx.send(Seen::Line(line));
            }
            let _ = tx.send(Seen::Eof(what));
        })
    };
    read(
        "stdout",
        Box::new(middle.stdout.take().unwrap()),
        tx.clone(),
    );
    read(
        "stderr",
        Box::new(middle.stderr.take().unwrap()),
        tx.clone(),
    );
    read("the unnamed pipe", Box::new(unnamed), tx);
    assert!(middle.wait().unwrap().success(), "the middle process ran");

    let mut pid = None;
    let mut open = vec!["stdout", "stderr", "the unnamed pipe"];
    while !open.is_empty() {
        match rx.recv_timeout(HELD) {
            Ok(Seen::Line(line)) => {
                // libtest may print the test's name on the same line.
                if let Some((_, id)) = line.split_once("detached-pid=") {
                    pid = Some(id.trim().to_owned());
                }
            }
            Ok(Seen::Eof(what)) => open.retain(|o| *o != what),
            Err(_) => break,
        }
    }
    let pid = pid.expect("the middle process printed the detached pid");
    // Never 0 or 1 to a kill: `kill -9 0` is this whole process group.
    assert!(
        pid.parse::<u32>().is_ok_and(|p| p > 1),
        "a detached pid, not {pid}"
    );
    let mut kill = if cfg!(windows) {
        let mut taskkill = Command::new("taskkill");
        taskkill.args(["/F", "/PID", &pid]);
        taskkill
    } else {
        let mut kill = Command::new("kill");
        kill.args(["-9", &pid]);
        kill
    };
    let _ = kill.stdout(Stdio::null()).stderr(Stdio::null()).status();
    assert!(
        open.is_empty(),
        "{open:?} of the caller's still open {HELD:?} after it exited: \
         the detached process holds them"
    );
}

#[cfg(unix)]
mod unix {
    use super::{promptly, read_all, spawn_detached, Detach, Stream};

    /// The session and process group of `pid` (0: this process).
    fn session_and_group(pid: u32) -> (i32, i32) {
        let pid = i32::try_from(pid).unwrap();
        // SAFETY: plain calls taking a pid; a pid not there is -1.
        let (sid, pgid) = unsafe { (libc::getsid(pid), libc::getpgid(pid)) };
        assert!(sid > 0 && pgid > 0, "pid {pid} has a session and group");
        (sid, pgid)
    }

    #[test]
    fn test_a_detached_child_leads_a_session_and_group_of_its_own() {
        // The child waits on its stdin, so it is there to be asked about.
        let mut detach = Detach::new("sh");
        detach.args(["-c", "cat"]).stdin(Stream::Piped);
        let mut child = spawn_detached(detach).unwrap();
        let (session, group) = session_and_group(child.id());
        let ours = session_and_group(0);
        // The setsid ran before the exec, so it is done once spawn returns.
        assert_eq!(session, i32::try_from(child.id()).unwrap());
        assert_eq!(group, session, "a session's leader leads its group");
        assert_ne!(session, ours.0, "not in the spawner's session");
        assert_ne!(group, ours.1, "not in the spawner's group");
        drop(child.take_stdin());
        promptly(move || assert!(child.wait().unwrap().success()));
    }

    /// The id is the process's own pid, as it reports it.
    #[test]
    fn test_the_id_is_the_processs_own_pid() {
        let mut detach = Detach::new("sh");
        detach.args(["-c", "echo $$"]).stdout(Stream::Piped);
        let mut child = spawn_detached(detach).unwrap();
        let out = child.take_stdout().unwrap();
        let shown = promptly(move || read_all(out));
        assert_eq!(shown.trim_end(), child.id().to_string());
        assert!(child.wait().unwrap().success());
    }

    #[test]
    fn test_a_detached_child_has_no_controlling_terminal() {
        // A new session has none: opening /dev/tty fails, whether or not
        // the spawner has one.
        let mut detach = Detach::new("sh");
        detach.args([
            "-c",
            "if (exec 3</dev/tty) 2>/dev/null; then exit 1; fi; exit 0",
        ]);
        let mut child = spawn_detached(detach).unwrap();
        promptly(move || assert!(child.wait().unwrap().success()));
    }

    /// No descriptor above stderr is open in the process, whatever the
    /// caller held without close-on-exec.
    #[test]
    fn test_a_detached_child_has_no_descriptor_above_stderr() {
        let (_reader, writer) = std::io::pipe().unwrap();
        super::make_inheritable(&writer);
        let mut detach = Detach::new("sh");
        // The shell's own descriptors are below 10; list the rest.
        detach
            .args([
                "-c",
                "for fd in 3 4 5 6 7 8 9; do (: <&$fd) 2>/dev/null && echo $fd; done; :",
            ])
            .stdout(Stream::Piped);
        let mut child = spawn_detached(detach).unwrap();
        let out = child.take_stdout().unwrap();
        assert_eq!(promptly(move || read_all(out)), "");
        assert!(child.wait().unwrap().success());
    }
}
