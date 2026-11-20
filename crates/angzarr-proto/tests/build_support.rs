//! The proto root build.rs hands protoc is a path protoc can join its
//! file names to on every platform: `canonicalize` gives a verbatim path on
//! Windows (`\\?\C:\...`), in which `/` is not a separator, so the root is
//! taken without that prefix (`build_support::without_verbatim`). Pure
//! text, so it is tested here whatever the platform.

use std::path::PathBuf;

/// The build scripts' shared helpers, as build.rs includes them; only the
/// path helper is tested here.
#[allow(dead_code)]
#[path = "../build_support.rs"]
mod build_support;

use build_support::without_verbatim;

#[test]
fn a_verbatim_drive_path_loses_its_prefix() {
    assert_eq!(
        without_verbatim(PathBuf::from(r"\\?\C:\work\angzarr\angzarr-project\proto")),
        PathBuf::from(r"C:\work\angzarr\angzarr-project\proto")
    );
}

#[test]
fn a_verbatim_share_path_is_a_share_path() {
    assert_eq!(
        without_verbatim(PathBuf::from(r"\\?\UNC\host\share\angzarr-project\proto")),
        PathBuf::from(r"\\host\share\angzarr-project\proto")
    );
}

/// A verbatim path that names no drive (a volume GUID) has no plain form:
/// it is kept as it is.
#[test]
fn a_verbatim_path_without_a_drive_is_kept() {
    let volume = PathBuf::from(r"\\?\Volume{0b1c2d3e-0000-0000-0000-000000000000}\proto");
    assert_eq!(without_verbatim(volume.clone()), volume);
}

/// A verbatim prefix followed by a letter that is not a drive's (no colon)
/// is not a drive path either.
#[test]
fn a_verbatim_path_whose_first_name_is_not_a_drive_is_kept() {
    let path = PathBuf::from(r"\\?\C\proto");
    assert_eq!(without_verbatim(path.clone()), path);
}

/// Nor is one whose first name is two characters ending in a colon that
/// is not a letter's.
#[test]
fn a_verbatim_path_whose_first_name_is_no_drive_letter_is_kept() {
    let path = PathBuf::from(r"\\?\1:\proto");
    assert_eq!(without_verbatim(path.clone()), path);
}

/// What `canonicalize` gives on Unix, and a plain Windows path, are
/// already plain.
#[test]
fn a_plain_path_is_kept() {
    for plain in [
        "/home/u/angzarr/angzarr-project/proto",
        r"C:\work\proto",
        r"\\host\share\proto",
    ] {
        assert_eq!(without_verbatim(PathBuf::from(plain)), PathBuf::from(plain));
    }
}
