//! The Unix socket policy, on real sockets under a `TestDir`: mode, the
//! directory, stale and live sockets, removal.

mod support;

use std::fs;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::UnixListener as StdListener;
use std::path::Path;

use angzarr_coordinator_grpc::{Arrival, BoundEndpoint, ListenError, Listener, Server};
use angzarr_test_dir::TestDir;

fn mode(path: &Path) -> u32 {
    fs::metadata(path).unwrap().mode() & 0o777
}

async fn serve(path: &Path) -> Result<angzarr_coordinator_grpc::Running, ListenError> {
    Server::builder()
        .add_service(support::Probe)
        .listen(Listener::uds(path))
        .bind()
        .await
}

/// The socket is 0600, whatever the process's umask says; a directory made
/// for it is 0700, and one that exists at 0700 is used as it is.
#[tokio::test]
async fn the_socket_is_0600_in_a_0700_directory() {
    let dir = TestDir::new("grpc-mode");
    // A directory that does not exist is created, at 0700.
    let made = dir.path().join("run");
    let socket = made.join("a.sock");
    let running = serve(&socket).await.expect("binds");
    assert!(fs::metadata(&socket).unwrap().file_type().is_socket());
    assert_eq!(mode(&socket), 0o600);
    assert_eq!(mode(&made), 0o700);
    // It is served, and its requests declare loopback.
    assert_eq!(
        support::arrival(support::uds(&socket).await).await,
        "loopback"
    );
    drop(running);

    // A directory that exists, ours at 0700, is taken as it is.
    let existing = dir.path().join("existing");
    fs::create_dir(&existing).unwrap();
    fs::set_permissions(&existing, fs::Permissions::from_mode(0o700)).unwrap();
    let socket = existing.join("b.sock");
    let _running = serve(&socket).await.expect("binds");
    assert_eq!(mode(&socket), 0o600);
    assert_eq!(mode(&existing), 0o700);
}

/// A directory that is not private is refused as it is: not chmod-ed into
/// shape (it is not ours to decide), and nothing is bound in it.
#[tokio::test]
async fn a_directory_that_is_not_private_is_refused_and_left_as_it_is() {
    let dir = TestDir::new("grpc-open");
    for open in [0o755, 0o750, 0o770, 0o701, 0o500] {
        let shared = dir.path().join(format!("m{open:o}"));
        fs::create_dir(&shared).unwrap();
        fs::set_permissions(&shared, fs::Permissions::from_mode(open)).unwrap();
        let socket = shared.join("a.sock");
        let error = serve(&socket).await.err().expect("refused");
        assert!(
            matches!(error, ListenError::DirectoryNotPrivate { .. }),
            "{open:o}: {error:?}"
        );
        assert_eq!(mode(&shared), open);
        assert!(!socket.exists());
        fs::set_permissions(&shared, fs::Permissions::from_mode(0o700)).unwrap();
    }
}

/// The directory is a directory: a file where it should be is refused, and
/// a path with no directory at all is.
#[tokio::test]
async fn a_socket_path_without_a_usable_directory_is_refused() {
    let dir = TestDir::new("grpc-nodir");
    let file = support::socket_in(&dir, "file");
    fs::write(&file, b"x").unwrap();
    let error = serve(&file.join("a.sock")).await.err().expect("refused");
    assert!(matches!(
        error,
        ListenError::Io { .. } | ListenError::DirectoryNotPrivate { .. }
    ));
    // Under a file the directory cannot even be looked at.
    let error = serve(&file.join("sub").join("a.sock"))
        .await
        .err()
        .expect("refused");
    assert!(
        matches!(error, ListenError::Io { op: "stat", .. }),
        "{error:?}"
    );
    let error = serve(Path::new("a.sock")).await.err().expect("refused");
    assert!(
        matches!(error, ListenError::NoDirectory { .. }),
        "{error:?}"
    );
    let error = serve(Path::new("")).await.err().expect("refused");
    assert!(
        matches!(error, ListenError::NoDirectory { .. }),
        "{error:?}"
    );
}

/// What is at the path decides: a socket nobody listens on is replaced; one
/// a live listener holds is refused, and that listener still serves; a file
/// that is not a socket is refused and left alone.
#[tokio::test]
async fn a_stale_socket_file_is_replaced_but_a_live_one_is_refused() {
    let dir = TestDir::new("grpc-stale");
    let socket = support::socket_in(&dir, "a.sock");

    // Stale: a listener that was dropped without removing its file.
    drop(StdListener::bind(&socket).unwrap());
    assert!(fs::metadata(&socket).unwrap().file_type().is_socket());
    let stale = fs::metadata(&socket).unwrap().ino();
    let first = serve(&socket).await.expect("a stale socket is replaced");
    assert_eq!(mode(&socket), 0o600);
    assert_ne!(
        fs::metadata(&socket).unwrap().ino(),
        stale,
        "a new socket file"
    );
    assert_eq!(
        support::arrival(support::uds(&socket).await).await,
        "loopback"
    );

    // Live: the listener above, bound over while it serves.
    let live = fs::metadata(&socket).unwrap().ino();
    let error = serve(&socket)
        .await
        .err()
        .expect("a live socket is refused");
    assert!(
        matches!(error, ListenError::SocketInUse { .. }),
        "{error:?}"
    );
    assert_eq!(
        fs::metadata(&socket).unwrap().ino(),
        live,
        "its file is untouched"
    );
    assert_eq!(
        support::arrival(support::uds(&socket).await).await,
        "loopback"
    );
    drop(first);

    // Not a socket: never removed.
    let plain = support::socket_in(&dir, "plain");
    fs::write(&plain, b"mine").unwrap();
    let error = serve(&plain).await.err().expect("refused");
    assert!(matches!(error, ListenError::NotASocket { .. }), "{error:?}");
    assert_eq!(fs::read(&plain).unwrap(), b"mine");
}

/// A graceful shutdown removes the socket, and so does dropping the server.
#[tokio::test]
async fn the_socket_is_removed_on_shutdown_and_on_drop() {
    let dir = TestDir::new("grpc-remove");
    let socket = support::socket_in(&dir, "a.sock");

    let running = serve(&socket).await.unwrap();
    assert!(socket.exists());
    running.shutdown().await.expect("shuts down");
    assert!(!socket.exists(), "removed on shutdown");

    let running = serve(&socket).await.unwrap();
    assert!(socket.exists());
    drop(running);
    assert!(!socket.exists(), "removed on drop");
}

/// The socket is ours to remove only while it is ours: if another listener
/// has taken the path since (ours removed, theirs bound), dropping ours
/// leaves theirs.
#[tokio::test]
async fn a_socket_that_is_no_longer_ours_is_left_when_we_stop() {
    let dir = TestDir::new("grpc-theirs");
    let socket = support::socket_in(&dir, "a.sock");
    let running = serve(&socket).await.unwrap();
    fs::remove_file(&socket).unwrap();
    let theirs = StdListener::bind(&socket).unwrap();
    let theirs_inode = fs::metadata(&socket).unwrap().ino();
    drop(running);
    assert_eq!(fs::metadata(&socket).unwrap().ino(), theirs_inode);
    drop(theirs);
}

/// A failed bind leaves nothing behind of the listeners bound before it.
#[tokio::test]
async fn a_failed_bind_leaves_no_socket_behind() {
    let dir = TestDir::new("grpc-failed");
    let first = support::socket_in(&dir, "first.sock");
    let busy = support::socket_in(&dir, "busy.sock");
    let _live = serve(&busy).await.unwrap();
    let error = Server::builder()
        .add_service(support::Probe)
        .listen(Listener::uds(&first))
        .listen(Listener::uds(&busy))
        .bind()
        .await
        .err()
        .expect("the second is refused");
    assert!(
        matches!(error, ListenError::SocketInUse { .. }),
        "{error:?}"
    );
    assert!(!first.exists(), "the first listener's socket is removed");
    assert!(busy.exists(), "the live one is untouched");
}

/// The report says each listener, in order, with the arrival it declares.
#[tokio::test]
async fn bound_reports_each_listener_in_order_with_its_arrival() {
    let dir = TestDir::new("grpc-bound");
    let (a, b) = (
        support::socket_in(&dir, "a.sock"),
        support::socket_in(&dir, "b.sock"),
    );
    let running = Server::builder()
        .listen(Listener::uds(&a))
        .listen(Listener::uds(&b).declare(Arrival::Ip))
        .bind()
        .await
        .unwrap();
    assert_eq!(
        running.bound().endpoints(),
        [
            BoundEndpoint::Uds {
                path: a,
                arrival: Arrival::Loopback
            },
            BoundEndpoint::Uds {
                path: b,
                arrival: Arrival::Ip
            },
        ]
    );
    assert_eq!(running.bound().endpoints()[1].arrival(), Arrival::Ip);
}
