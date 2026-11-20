//! The host's syscalls, each held to what it promises, on whatever host
//! runs the tests: unix here; Windows on CI's Windows runner (decision 5).

use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use tempfile::TempDir;

use super::{host, Flavor, LockKind, Mode, Signal};

fn dir() -> TempDir {
    TempDir::new().unwrap()
}

fn file(at: &Path) -> File {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(at)
        .unwrap()
}

/// The mode `path` has, as the host reads it.
fn mode(path: &Path) -> Option<Mode> {
    host().mode_of(&fs::metadata(path).unwrap())
}

/// `bits` where the host has modes; `None` where it has none.
fn expected(bits: u32) -> Option<Mode> {
    (host().flavor() == Flavor::Unix).then_some(Mode::from_bits(bits))
}

#[test]
fn test_the_host_reports_the_flavor_it_was_built_for() {
    let expected = if cfg!(windows) {
        Flavor::Windows
    } else {
        Flavor::Unix
    };
    assert_eq!(host().flavor(), expected);
}

#[test]
fn test_a_mode_set_is_the_mode_read_where_there_are_modes() {
    let tmp = dir();
    let path = tmp.path().join("f");
    file(&path);
    host().set_mode(&path, Mode::from_bits(0o640)).unwrap();
    assert_eq!(mode(&path), expected(0o640));
    host().set_mode(&path, Mode::from_bits(0o604)).unwrap();
    assert_eq!(mode(&path), expected(0o604));
}

#[test]
fn test_setting_the_mode_of_what_is_missing_is_not_found() {
    let tmp = dir();
    let e = host()
        .set_mode(&tmp.path().join("missing"), Mode::FILE)
        .unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::NotFound);
}

#[test]
fn test_an_open_files_mode_is_set_through_its_handle() {
    let tmp = dir();
    let path = tmp.path().join("f");
    let held = file(&path);
    host().set_file_mode(&held, Mode::from_bits(0o620)).unwrap();
    assert_eq!(mode(&path), expected(0o620));
}

#[test]
fn test_a_file_created_with_a_mode_has_it_and_a_directory_too() {
    let tmp = dir();
    let path = tmp.path().join("private");
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    host().open_mode(&mut options, Mode::PRIVATE_FILE);
    options.open(&path).unwrap().write_all(b"x").unwrap();
    assert_eq!(mode(&path), expected(0o600));

    let sub = tmp.path().join("a").join("b");
    let mut builder = DirBuilder::new();
    builder.recursive(true);
    host().dir_mode(&mut builder, Mode::PRIVATE_DIR);
    builder.create(&sub).unwrap();
    assert_eq!(mode(&sub), expected(0o700));
    assert_eq!(mode(&tmp.path().join("a")), expected(0o700));
}

#[test]
fn test_a_directory_link_reads_back_and_is_removed_alone() {
    let os = host();
    let tmp = dir();
    let (target, link) = (tmp.path().join("target"), tmp.path().join("link"));
    fs::create_dir(&target).unwrap();
    fs::write(target.join("kept"), "x").unwrap();
    os.link_dir(&target, &link).unwrap();
    assert!(link.join("kept").exists(), "the link reaches the directory");
    assert_eq!(os.read_link(&link).unwrap(), target);
    os.remove_link(&link).unwrap();
    assert!(!link.exists());
    assert!(
        fs::symlink_metadata(&link).is_err(),
        "the link itself is gone"
    );
    assert!(target.join("kept").exists(), "what it linked to stays");
}

#[test]
fn test_a_plain_file_is_removed_as_a_link_is() {
    let tmp = dir();
    let path = tmp.path().join("plain");
    file(&path);
    host().remove_link(&path).unwrap();
    assert!(!path.exists());
}

#[test]
fn test_read_link_of_what_is_not_a_link_is_invalid_input() {
    let tmp = dir();
    let path = tmp.path().join("plain");
    file(&path);
    let e = host().read_link(&path).unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn test_a_file_is_the_file_at_its_path_until_another_replaces_it() {
    let os = host();
    let tmp = dir();
    let (path, other) = (tmp.path().join("lock"), tmp.path().join("other"));
    let held = file(&path);
    assert!(os.same_file(&held, &path).unwrap());
    file(&other);
    assert!(!os.same_file(&held, &other).unwrap(), "another file");
    fs::rename(&other, &path).unwrap();
    assert!(!os.same_file(&held, &path).unwrap(), "renamed over");
    fs::remove_file(&path).unwrap();
    let gone = os.same_file(&held, &path).unwrap_err();
    assert_eq!(gone.kind(), io::ErrorKind::NotFound);
}

#[test]
fn test_a_rename_that_replaces_nothing_moves_the_file() {
    let tmp = dir();
    let (from, to) = (tmp.path().join("from"), tmp.path().join("to"));
    fs::write(&from, b"moved").unwrap();
    host().rename_noreplace(&from, &to).unwrap();
    assert_eq!(fs::read(&to).unwrap(), b"moved");
    assert!(!from.exists(), "the source is gone");
}

#[test]
fn test_a_rename_onto_a_file_is_already_exists_and_changes_neither() {
    let tmp = dir();
    let (from, to) = (tmp.path().join("from"), tmp.path().join("to"));
    fs::write(&from, b"new").unwrap();
    fs::write(&to, b"old").unwrap();
    let e = host().rename_noreplace(&from, &to).unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::AlreadyExists, "{e}");
    assert_eq!(fs::read(&from).unwrap(), b"new");
    assert_eq!(fs::read(&to).unwrap(), b"old");
}

#[test]
fn test_a_rename_onto_a_directory_is_refused_and_changes_neither() {
    let tmp = dir();
    let (from, to) = (tmp.path().join("from"), tmp.path().join("to"));
    fs::write(&from, b"new").unwrap();
    fs::create_dir(&to).unwrap();
    host().rename_noreplace(&from, &to).unwrap_err();
    assert_eq!(fs::read(&from).unwrap(), b"new");
    assert!(to.is_dir(), "the directory is left");
}

#[test]
fn test_a_rename_of_a_directory_that_replaces_nothing_moves_it() {
    let tmp = dir();
    let (from, to) = (tmp.path().join("from"), tmp.path().join("to"));
    fs::create_dir(&from).unwrap();
    fs::write(from.join("inside"), b"x").unwrap();
    host().rename_noreplace(&from, &to).unwrap();
    assert_eq!(fs::read(to.join("inside")).unwrap(), b"x");
    assert!(!from.exists(), "the source is gone");
}

#[test]
fn test_a_rename_of_what_is_missing_is_not_found() {
    let tmp = dir();
    let (from, to) = (tmp.path().join("missing"), tmp.path().join("to"));
    let e = host().rename_noreplace(&from, &to).unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::NotFound, "{e}");
    assert!(!to.exists());
}

#[test]
fn test_a_rename_of_a_path_holding_a_nul_is_invalid_input() {
    let tmp = dir();
    let from = tmp.path().join("from");
    fs::write(&from, b"x").unwrap();
    let to = tmp.path().join("t\0o");
    let e = host().rename_noreplace(&from, &to).unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::InvalidInput, "{e}");
    let e = host().rename_noreplace(&to, &from).unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::InvalidInput, "{e}");
    assert_eq!(fs::read(&from).unwrap(), b"x");
}

#[test]
fn test_a_directory_syncs_and_a_missing_one_is_not_found_where_it_is_opened() {
    let tmp = dir();
    host().sync_dir(tmp.path()).unwrap();
    let missing = host().sync_dir(&tmp.path().join("missing"));
    if host().flavor() == Flavor::Unix {
        assert_eq!(missing.unwrap_err().kind(), io::ErrorKind::NotFound);
    }
}

#[test]
fn test_an_exclusive_lock_excludes_and_shared_locks_share() {
    let os = host();
    let tmp = dir();
    let path = tmp.path().join("lock");
    let (first, second) = (file(&path), file(&path));
    assert!(os.try_lock(&first, LockKind::Exclusive).unwrap());
    assert!(!os.try_lock(&second, LockKind::Exclusive).unwrap());
    assert!(!os.try_lock(&second, LockKind::Shared).unwrap());
    drop(first);
    let third = file(&path);
    // A process another test of this run is starting (`cargo test` runs
    // them as threads of one process) holds a copy of `first` until it
    // execs, so the release can be a moment late.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !os.try_lock(&second, LockKind::Shared).unwrap() {
        assert!(Instant::now() < deadline, "the lock was never released");
        std::thread::yield_now();
    }
    assert!(os.try_lock(&third, LockKind::Shared).unwrap());
    assert!(
        !os.try_lock(&file(&path), LockKind::Exclusive).unwrap(),
        "a shared lock excludes an exclusive one"
    );
}

/// The one test that registers for signals, so no other test can see the
/// signals it sends: a stop signal, made, listens and has not resolved; a
/// SIGTERM, then (on a second one) a SIGINT, sent to this process resolves
/// it. Then the signals of a set are each heard by name, in the order
/// sent, and one outside the set is not heard.
#[test]
fn test_a_stop_signal_waits_for_a_signal_and_resolves_at_one() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let os = host();
    let mut stop = runtime.block_on(async { os.stop_signal() }).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(stop.as_mut().poll(&mut cx), Poll::Pending);
    if os.flavor() != Flavor::Unix {
        return;
    }
    for signal in ["-TERM", "-INT"] {
        let stop = runtime.block_on(async { os.stop_signal() }).unwrap();
        send(signal);
        let resolved =
            runtime.block_on(async { tokio::time::timeout(Duration::from_secs(10), stop).await });
        assert!(resolved.is_ok(), "{signal} did not resolve the stop signal");
    }
    let mut named = runtime
        .block_on(async { os.signals(&[Signal::Hangup, Signal::Terminate]) })
        .unwrap();
    let mut hangup_only = runtime
        .block_on(async { os.signals(&[Signal::Hangup]) })
        .unwrap();
    for (sent, heard) in [("-HUP", Signal::Hangup), ("-TERM", Signal::Terminate)] {
        send(sent);
        let got = runtime
            .block_on(async { tokio::time::timeout(Duration::from_secs(10), named.recv()).await });
        assert_eq!(got.ok(), Some(heard), "{sent} was not heard by name");
    }
    let own = runtime.block_on(async {
        tokio::time::timeout(Duration::from_millis(200), hangup_only.recv()).await
    });
    assert_eq!(own.ok(), Some(Signal::Hangup), "the hangup was not heard");
    let outside = runtime.block_on(async {
        tokio::time::timeout(Duration::from_millis(200), hangup_only.recv()).await
    });
    assert!(outside.is_err(), "a SIGTERM was heard by a set without it");
}

/// Unix has no console events: a Ctrl-Break is refused as unsupported,
/// and starting a child in its own group changes nothing of it. (On
/// Windows, the watch's clean stop at a Ctrl-Break is the binary's
/// acceptance test.)
#[test]
fn test_a_ctrl_break_is_unsupported_on_unix() {
    if host().flavor() != Flavor::Unix {
        return;
    }
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    let mut child = super::own_group(&mut command)
        .args(["--list", "--format", "terse"])
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let sent = super::ctrl_break(&child);
    assert_eq!(sent.unwrap_err().kind(), io::ErrorKind::Unsupported);
    assert!(child.wait().unwrap().success());
}

/// Send `signal` to this process, as `kill` does.
fn send(signal: &str) {
    let status = std::process::Command::new("kill")
        .arg(signal)
        .arg(std::process::id().to_string())
        .status()
        .unwrap();
    assert!(status.success(), "kill {signal}");
}

#[test]
fn test_text_that_is_not_unicode_is_not_unicode_and_keeps_its_prefix() {
    let odd = super::not_unicode("x-");
    assert!(odd.to_str().is_none());
    assert!(odd.as_encoded_bytes().starts_with(b"x-"));
}

#[test]
fn test_a_fixture_is_absolute_on_the_host_and_spelled_by_its_flavor() {
    let path = super::fixture("/home/u");
    assert!(path.is_absolute(), "{path:?}");
    assert_eq!(
        path.to_str(),
        Some(super::host().flavor().from_top("/home/u").as_str())
    );
}

#[test]
fn test_the_user_home_is_an_absolute_directory() {
    let home = host().user_home().unwrap();
    assert!(home.is_absolute(), "{home:?}");
    assert!(home.is_dir(), "{home:?}");
}

/// The account database's word, never `HOME`'s: the test compares the
/// answer with getpwuid's own (which reads no variable), and a `HOME`
/// pointing elsewhere does not move it (`user_home_is_not_the_home_variable`).
#[cfg(unix)]
#[test]
fn test_the_user_home_is_the_accounts_home_directory() {
    use std::ffi::{CStr, OsStr};
    use std::os::unix::ffi::OsStrExt;
    // SAFETY: getpwuid's entry is read at once, on this thread, before any
    // other call could overwrite it.
    let expected = unsafe {
        let entry = libc::getpwuid(libc::geteuid());
        assert!(!entry.is_null(), "the test's account has an entry");
        Path::new(OsStr::from_bytes(
            CStr::from_ptr((*entry).pw_dir).to_bytes(),
        ))
        .to_path_buf()
    };
    assert_eq!(host().user_home().unwrap(), expected);
}

/// Every reading of the entry, however small the first buffer: one too
/// small for it is doubled until the entry fits.
#[cfg(unix)]
#[test]
fn test_the_user_home_is_read_whatever_the_first_buffer() {
    let expected = host().user_home().unwrap();
    for start in [1, 2, 3, 17, 64] {
        let home = super::host::account_home(start, 1 << 20).unwrap();
        assert_eq!(home, expected, "from {start}");
    }
}

/// The buffer grows by doubling, and not past its last size: at the
/// smallest size the entry fits, one byte less and no room to grow is the
/// `ERANGE`, and half of it with room for one doubling is enough.
#[cfg(unix)]
#[test]
fn test_the_users_entry_buffer_doubles_up_to_its_last_size() {
    let fits = |size: usize| super::host::account_home(size, size).is_ok();
    let least = (1..4096).find(|&size| fits(size)).expect("an entry fits");
    assert!(least > 4, "an entry is longer than a few bytes: {least}");
    let short = super::host::account_home(least - 1, least - 1).unwrap_err();
    assert_eq!(short.raw_os_error(), Some(libc::ERANGE));
    let half = least.div_ceil(2);
    assert!(super::host::account_home(half, half + 1).is_ok());
}

/// What an entry names as its home: none, a null or an empty one is
/// `NotFound`; a path is that path, byte for byte.
#[cfg(unix)]
#[test]
fn test_an_entry_with_no_home_is_not_found() {
    use super::host::home_of;
    // SAFETY: each pointer is null or a NUL-terminated literal.
    unsafe {
        for dir in [None, Some(std::ptr::null()), Some(c"".as_ptr())] {
            let err = home_of(dir).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::NotFound, "{dir:?}");
        }
        let home = home_of(Some(c"/home/u\xff".as_ptr())).unwrap();
        assert_eq!(home, crate::not_unicode("/home/u"));
    }
}

#[cfg(unix)]
#[test]
fn test_the_user_home_is_not_the_home_variable() {
    // In a child, so this process's environment is never changed: the
    // test binary runs this one test with HOME pointing elsewhere, and the
    // test there asserts what it printed is the account's.
    if std::env::var_os("ANGZARR_OS_USER_HOME_CHILD").is_some() {
        println!("user_home={}", host().user_home().unwrap().display());
        return;
    }
    let expected = host().user_home().unwrap();
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tests::test_the_user_home_is_not_the_home_variable",
            "--nocapture",
        ])
        .env("ANGZARR_OS_USER_HOME_CHILD", "1")
        .env("HOME", "/nonexistent/angzarr-os-home")
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
    let printed = String::from_utf8(out.stdout).unwrap();
    let line = printed
        .lines()
        .find_map(|l| l.strip_prefix("user_home="))
        .expect("the child printed its answer");
    assert_eq!(Path::new(line), expected);
}
