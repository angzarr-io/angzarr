//! What only the real filesystem does: name each path as given.

use std::io::{self, ErrorKind};
use std::path::Path;

use super::Os;
use ::os::{host, Flavor, Mode};

use crate::{CwdError, Fs, Perms};

fn top() -> &'static Path {
    Path::new(host().flavor().top())
}

#[test]
fn test_a_working_directory_must_be_absolute() {
    let e = Os::new(host(), Path::new("relative/cwd")).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::InvalidInput);
}

/// Why a working directory could not be read, as the OS might say.
fn gone() -> io::Error {
    io::Error::new(ErrorKind::NotFound, "gone")
}

/// The [`CwdError`] an `io::Error` carries, if it carries one.
fn cwd_error(e: &io::Error) -> Option<&CwdError> {
    e.get_ref()
        .and_then(|inner| inner.downcast_ref::<CwdError>())
}

#[test]
fn test_the_working_directory_is_the_one_given() {
    assert_eq!(Os::new(host(), top()).unwrap().cwd(), Ok(top()));
}

#[test]
fn test_an_os_over_an_unreadable_working_directory_says_why() {
    assert_eq!(
        Os::without_cwd(host(), gone()).cwd(),
        Err(CwdError {
            reason: "gone".to_string()
        })
    );
}

#[test]
fn test_a_relative_path_without_a_working_directory_fails_with_why() {
    let os = Os::without_cwd(host(), gone());
    let e = os.read(Path::new("x")).unwrap_err();
    assert_eq!(
        cwd_error(&e),
        Some(&CwdError {
            reason: "gone".to_string()
        }),
        "{e}"
    );
    let e = os.real_path(Path::new("x")).unwrap_err();
    assert!(cwd_error(&e).is_some(), "{e}");
}

#[test]
fn test_an_absolute_path_without_a_working_directory_is_as_given() {
    let os = Os::without_cwd(host(), gone());
    let temp = tempfile::tempdir().unwrap();
    let x = temp.path().join("x");
    os.write_atomic(&x, b"x", Perms::Exact(Mode::FILE)).unwrap();
    assert_eq!(os.read(&x).unwrap(), b"x");
}

#[test]
fn test_real_path_is_the_path_as_given() {
    let os = Os::new(host(), top()).unwrap();
    let temp = tempfile::tempdir().unwrap();
    // Not resolved: a lock is named by the path it guards, however its
    // links stand, and SQLite opens what it was handed.
    let given = temp.path().join("not-yet").join("..").join("state.db");
    assert_eq!(os.real_path(&given).unwrap(), given);
    let plain = Path::new(if host().flavor() == Flavor::Windows {
        r"C:\x\y"
    } else {
        "/x/y"
    });
    assert_eq!(os.real_path(plain).unwrap(), plain);
}
