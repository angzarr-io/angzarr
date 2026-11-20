use std::fs;
use std::io::{Error, ErrorKind};
use std::os::unix::net::UnixListener;

use angzarr_test_dir::TestDir;

use super::*;

fn private(dir: &TestDir) -> &Path {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    dir.path()
}

/// The owner is ours when writing in the directory says our uid is its
/// owner's; another uid, or no permission to write there, is not ours; any
/// other failure is the probe's own.
#[test]
fn a_directory_is_ours_only_when_its_owner_is_our_uid() {
    let dir = Path::new("/run/x");
    assert_eq!(judge_owner(dir, 1000, Ok(1000)), Ok(()));
    let not_ours = ListenError::directory_not_private(dir);
    assert_eq!(judge_owner(dir, 0, Ok(1000)), Err(not_ours.clone()));
    let denied = Err(Error::from(ErrorKind::PermissionDenied));
    assert_eq!(judge_owner(dir, 1000, denied), Err(not_ours));
    let other = Err(Error::from(ErrorKind::OutOfMemory));
    assert!(matches!(
        judge_owner(dir, 1000, other),
        Err(ListenError::Io {
            op: "probe-owner",
            ..
        })
    ));
}

/// What connecting to an existing socket file said decides it: an answer
/// refuses, a refusal to connect removes the stale file, a vanished file is
/// nothing to do, and any other failure refuses and keeps the file.
#[test]
fn a_socket_file_is_judged_by_what_connecting_to_it_said() {
    let dir = TestDir::new("grpc-judge");
    let path = private(&dir).join("a.sock");
    let make = || drop(UnixListener::bind(&path).unwrap());

    make();
    assert_eq!(
        judge_probe(&path, Ok(())),
        Err(ListenError::socket_in_use(&path))
    );
    assert!(path.exists(), "a live socket is kept");

    let refused = Err(Error::from(ErrorKind::ConnectionRefused));
    assert_eq!(judge_probe(&path, refused), Ok(()));
    assert!(!path.exists(), "a stale socket is removed");

    make();
    let gone = Err(Error::from(ErrorKind::NotFound));
    assert_eq!(judge_probe(&path, gone), Ok(()));
    assert!(path.exists(), "nothing is removed for a vanished file");

    let denied = Err(Error::from(ErrorKind::PermissionDenied));
    assert!(matches!(
        judge_probe(&path, denied),
        Err(ListenError::Io { op: "probe", .. })
    ));
    assert!(path.exists(), "a failed probe keeps the file");
}
