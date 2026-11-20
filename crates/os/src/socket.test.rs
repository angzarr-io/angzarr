//! A post to a local stream socket, against a listener of the test's own.

use std::path::PathBuf;
use std::time::Duration;

use super::post_local;

const TIMEOUT: Duration = Duration::from_secs(5);

fn socket_in(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join("post.sock")
}

// The listener reads every byte, then the post's end: the post closes its
// connection, so a reader to EOF returns rather than waiting on the poster.
#[cfg(unix)]
#[test]
fn a_post_is_read_whole_then_its_end() {
    use std::io::Read;
    use std::os::unix::net::UnixListener;
    use std::sync::mpsc;

    let dir = tempfile::tempdir().unwrap();
    let path = socket_in(&dir);
    let listener = UnixListener::bind(&path).unwrap();
    let (read, got) = mpsc::channel();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_read_timeout(Some(TIMEOUT)).unwrap();
        let mut bytes = Vec::new();
        let whole = stream.read_to_end(&mut bytes).map(|_| bytes).ok();
        read.send(whole).unwrap();
    });
    post_local(&path, b"{\"type\":\"user\"}\n", TIMEOUT).unwrap();
    assert_eq!(
        got.recv_timeout(TIMEOUT),
        Ok(Some(b"{\"type\":\"user\"}\n".to_vec()))
    );
}

// Nothing at the path is NotFound; a socket file no one listens on is
// ConnectionRefused: each the OS's own refusal, never a panic.
#[cfg(unix)]
#[test]
fn a_post_to_no_listener_is_the_os_refusal() {
    use std::os::unix::net::UnixListener;

    let dir = tempfile::tempdir().unwrap();
    let path = socket_in(&dir);
    let missing = post_local(&path, b"x", TIMEOUT).unwrap_err();
    assert_eq!(missing.kind(), std::io::ErrorKind::NotFound, "{missing}");

    drop(UnixListener::bind(&path).unwrap());
    let refused = post_local(&path, b"x", TIMEOUT).unwrap_err();
    assert_eq!(
        refused.kind(),
        std::io::ErrorKind::ConnectionRefused,
        "{refused}"
    );
}

// A listener that accepts and never reads fails the post within its write
// timeout, once the socket's buffer is full: never a hang.
#[cfg(unix)]
#[test]
fn a_listener_that_never_reads_fails_the_post_in_time() {
    use std::os::unix::net::UnixListener;
    use std::time::Instant;

    let dir = tempfile::tempdir().unwrap();
    let path = socket_in(&dir);
    let listener = UnixListener::bind(&path).unwrap();
    let holder = std::thread::spawn(move || {
        let held = listener.accept().unwrap();
        std::thread::sleep(Duration::from_secs(3));
        drop(held);
    });
    let started = Instant::now();
    let failed = post_local(&path, &vec![0u8; 64 << 20], Duration::from_millis(100)).unwrap_err();
    assert!(
        matches!(
            failed.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        ),
        "{failed}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    holder.join().unwrap();
}

// No endpoint is measured on Windows: a post there is Unsupported.
#[cfg(windows)]
#[test]
fn a_post_on_windows_is_unsupported() {
    let dir = tempfile::tempdir().unwrap();
    let failed = post_local(&socket_in(&dir), b"x", TIMEOUT).unwrap_err();
    assert_eq!(failed.kind(), std::io::ErrorKind::Unsupported, "{failed}");
}
