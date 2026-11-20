//! What only re-rooting decides: where a caller's path lands under the
//! root, that nothing climbs out of it, and that the root never shows.
//! These reach past the seam (std on the real paths) to see where things
//! landed.

use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};

use super::Rooted;
use ::os::{host, Flavor, Mode};

use crate::{CwdError, Fs, Perms};

fn on_windows() -> bool {
    host().flavor() == Flavor::Windows
}

/// `parts` joined under `root`.
fn under(root: &Path, parts: &[&str]) -> PathBuf {
    parts.iter().fold(root.to_path_buf(), |p, c| p.join(c))
}

fn caller(unix: &str, windows: &str) -> PathBuf {
    PathBuf::from(if on_windows() { windows } else { unix })
}

// --- Construction --------------------------------------------------------

fn top() -> PathBuf {
    caller("/", r"C:\")
}

#[test]
fn test_a_root_must_be_an_absolute_existing_directory() {
    let e = Rooted::new(Path::new("relative/root"), &top()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::InvalidInput);
    let temp = tempfile::tempdir().unwrap();
    let e = Rooted::new(&temp.path().join("missing"), &top()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::NotFound);
}

#[test]
fn test_a_working_directory_must_be_absolute() {
    let temp = tempfile::tempdir().unwrap();
    let rel = Path::new("relative/cwd");
    let e = Rooted::new(temp.path(), rel).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::InvalidInput);
    let e = Rooted::temp_with_cwd(rel).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::InvalidInput);
}

// --- The working directory -----------------------------------------------

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
fn test_the_working_directory_is_the_callers_path() {
    let cwd = caller("/home/u", r"C:\home\u");
    let fs = Rooted::temp_with_cwd(&cwd).unwrap();
    assert_eq!(fs.cwd(), Ok(cwd.as_path()));
}

#[test]
fn test_a_filesystem_over_an_unreadable_working_directory_says_why() {
    let owner = Rooted::temp().unwrap();
    let fs = Rooted::without_cwd(owner.root(), gone()).unwrap();
    assert_eq!(
        fs.cwd(),
        Err(CwdError {
            reason: "gone".to_string()
        })
    );
}

#[test]
fn test_a_root_without_a_working_directory_must_still_be_absolute() {
    let e = Rooted::without_cwd(Path::new("relative/root"), gone()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::InvalidInput);
}

#[test]
fn test_a_relative_path_without_a_working_directory_fails_with_why() {
    let owner = Rooted::temp().unwrap();
    let fs = Rooted::without_cwd(owner.root(), gone()).unwrap();
    let e = fs
        .write_atomic(Path::new("x"), b"x", Perms::Exact(Mode::FILE))
        .unwrap_err();
    assert_eq!(
        cwd_error(&e),
        Some(&CwdError {
            reason: "gone".to_string()
        }),
        "{e}"
    );
    let e = fs.read(Path::new("x")).unwrap_err();
    assert!(cwd_error(&e).is_some(), "{e}");
}

#[test]
fn test_an_absolute_path_without_a_working_directory_is_rerooted_as_ever() {
    let owner = Rooted::temp().unwrap();
    let fs = Rooted::without_cwd(owner.root(), gone()).unwrap();
    let x = top().join("x");
    fs.write_atomic(&x, b"x", Perms::Exact(Mode::FILE)).unwrap();
    assert_eq!(owner.read(&x).unwrap(), b"x");
}

#[test]
fn test_a_temporary_root_works_at_the_callers_top() {
    let fs = Rooted::temp().unwrap();
    assert_eq!(
        fs.real_path(Path::new("x")).unwrap(),
        fs.real_path(&top().join("x")).unwrap()
    );
}

#[test]
fn test_the_working_directory_is_rerooted_and_cleaned_like_any_path() {
    let cwd = caller("/home/u/../v", r"C:\home\u\..\v");
    let fs = Rooted::temp_with_cwd(&cwd).unwrap();
    let want = if on_windows() {
        under(fs.root(), &["C", "home", "v", "x"])
    } else {
        under(fs.root(), &["home", "v", "x"])
    };
    assert_eq!(fs.real_path(Path::new("x")).unwrap(), want);
    // A relative `..` stops at the root as an absolute one does.
    let climbed = fs.real_path(Path::new("../../../../y")).unwrap();
    let want = if on_windows() {
        under(fs.root(), &["C", "y"])
    } else {
        under(fs.root(), &["y"])
    };
    assert_eq!(climbed, want);
}

#[test]
fn test_a_root_is_held_with_its_links_resolved() {
    let temp = tempfile::tempdir().unwrap();
    let fs = Rooted::new(temp.path(), &top()).unwrap();
    let resolved = std::fs::canonicalize(temp.path()).unwrap();
    let plain = host().flavor().display_path(resolved.to_str().unwrap());
    assert_eq!(fs.root(), Path::new(&plain));
    assert!(
        !fs.root().to_string_lossy().starts_with(r"\\?\"),
        "no verbatim prefix: {:?}",
        fs.root()
    );
}

#[test]
fn test_a_temporary_root_goes_when_the_filesystem_is_dropped() {
    let fs = Rooted::temp().unwrap();
    let root = fs.root().to_path_buf();
    assert!(root.is_dir());
    let file = caller("/home/u/file", r"C:\Users\u\file");
    fs.create_dir_all(file.parent().unwrap(), Mode::DIR)
        .unwrap();
    fs.write_atomic(&file, b"x", Perms::Exact(Mode::FILE))
        .unwrap();
    drop(fs);
    assert!(!root.exists());
}

#[test]
fn test_two_roots_see_nothing_of_each_other() {
    let one = Rooted::temp().unwrap();
    let two = Rooted::temp().unwrap();
    let path = caller("/shared/name", r"C:\shared\name");
    one.create_dir_all(path.parent().unwrap(), Mode::DIR)
        .unwrap();
    one.write_atomic(&path, b"one's", Perms::Exact(Mode::FILE))
        .unwrap();
    assert!(!two.exists(&path).unwrap());
}

// --- Decision: an absolute path lands under the root ------------------------

#[test]
fn test_an_absolute_path_lands_at_the_same_path_under_the_root() {
    if on_windows() {
        return;
    }
    let fs = Rooted::temp().unwrap();
    let real = fs
        .real_path(Path::new("/home/u/.ctxloom/state.db"))
        .unwrap();
    assert_eq!(
        real,
        under(fs.root(), &["home", "u", ".ctxloom", "state.db"])
    );
}

#[test]
fn test_a_drive_path_lands_under_its_drive_letter() {
    if !on_windows() {
        return;
    }
    let fs = Rooted::temp().unwrap();
    let real = fs.real_path(Path::new(r"C:\Users\u\x")).unwrap();
    assert_eq!(real, under(fs.root(), &["C", "Users", "u", "x"]));
}

#[test]
fn test_a_drive_letter_of_either_case_is_one_directory() {
    if !on_windows() {
        return;
    }
    let fs = Rooted::temp().unwrap();
    assert_eq!(
        fs.real_path(Path::new(r"c:\x")).unwrap(),
        fs.real_path(Path::new(r"C:\x")).unwrap()
    );
}

#[test]
fn test_a_unc_path_lands_under_unc_server_share() {
    if !on_windows() {
        return;
    }
    let fs = Rooted::temp().unwrap();
    let real = fs.real_path(Path::new(r"\\server\share\dir\x")).unwrap();
    assert_eq!(
        real,
        under(fs.root(), &["UNC", "server", "share", "dir", "x"])
    );
}

#[test]
fn test_a_verbatim_path_lands_where_its_plain_spelling_does() {
    if !on_windows() {
        return;
    }
    let fs = Rooted::temp().unwrap();
    assert_eq!(
        fs.real_path(Path::new(r"\\?\C:\x")).unwrap(),
        fs.real_path(Path::new(r"C:\x")).unwrap()
    );
    assert_eq!(
        fs.real_path(Path::new(r"\\?\UNC\server\share\x")).unwrap(),
        fs.real_path(Path::new(r"\\server\share\x")).unwrap()
    );
}

// --- Decision: `..` cannot climb above the root, as `/..` is `/` ------------

#[test]
fn test_dot_dot_stops_at_the_root() {
    let fs = Rooted::temp().unwrap();
    let climbing = caller("/../../etc/passwd", r"C:\..\..\etc\passwd");
    let landed = fs.real_path(&climbing).unwrap();
    let want = if on_windows() {
        under(fs.root(), &["C", "etc", "passwd"])
    } else {
        under(fs.root(), &["etc", "passwd"])
    };
    assert_eq!(landed, want);
    let deeper = caller("/a/../../b", r"C:\a\..\..\b");
    assert!(fs.real_path(&deeper).unwrap().starts_with(fs.root()));
}

#[test]
fn test_a_write_that_climbs_lands_inside_the_root() {
    let fs = Rooted::temp().unwrap();
    let top = caller("/", r"C:\");
    fs.create_dir_all(&top, Mode::DIR).unwrap();
    let climbing = caller("/../escaped", r"C:\..\escaped");
    fs.write_atomic(&climbing, b"kept in", Perms::Exact(Mode::FILE))
        .unwrap();
    assert_eq!(
        std::fs::read(fs.real_path(&caller("/escaped", r"C:\escaped")).unwrap()).unwrap(),
        b"kept in"
    );
    assert!(!fs.root().parent().unwrap().join("escaped").exists());
}

// --- Decision: a link with an absolute target is re-rooted on disk ----------

#[test]
fn test_an_absolute_link_target_is_rerooted_on_disk_and_reads_back_as_given() {
    let fs = Rooted::temp().unwrap();
    let target = caller("/data/real", r"C:\data\real");
    let link = caller("/data/alias", r"C:\data\alias");
    fs.create_dir_all(&target, Mode::DIR).unwrap();
    fs.symlink_dir(&target, &link).unwrap();
    let on_disk = std::fs::read_link(fs.real_path(&link).unwrap()).unwrap();
    assert_eq!(on_disk, fs.real_path(&target).unwrap());
    assert_eq!(fs.read_link(&link).unwrap(), target);
}

// --- Decision: the root never shows in what the caller is given -------------

#[test]
fn test_a_canonical_path_is_the_callers_never_the_roots() {
    let fs = Rooted::temp().unwrap();
    let dir = caller("/work/real", r"C:\work\real");
    fs.create_dir_all(&dir, Mode::DIR).unwrap();
    let got = fs.canonicalize(&dir.join("..").join("real")).unwrap();
    assert_eq!(got, dir);
}

#[test]
fn test_a_temporary_directory_is_named_as_the_caller_names_it() {
    let fs = Rooted::temp().unwrap();
    let parent = caller("/tmp", r"C:\Temp");
    fs.create_dir_all(&parent, Mode::DIR).unwrap();
    let made = fs.make_temp_dir(&parent, "t-").unwrap();
    assert_eq!(made.parent(), Some(parent.as_path()));
    assert!(fs.real_path(&made).unwrap().is_dir());
}

#[test]
fn test_a_root_reached_through_a_link_still_never_shows() {
    if on_windows() {
        return;
    }
    // macOS's temporary directories are under /var, a link to
    // /private/var: the root as given and as resolved both stay hidden.
    let temp = tempfile::tempdir().unwrap();
    let real_root = temp.path().join("root");
    std::fs::create_dir(&real_root).unwrap();
    let linked_root = temp.path().join("linked");
    host().link_dir(&real_root, &linked_root).unwrap();
    let fs = Rooted::new(&linked_root, &top()).unwrap();
    fs.create_dir_all(Path::new("/a/b"), Mode::DIR).unwrap();
    assert_eq!(
        fs.canonicalize(Path::new("/a/./b")).unwrap(),
        Path::new("/a/b")
    );
}

#[test]
fn test_canonicalizing_out_of_the_root_is_refused() {
    if on_windows() {
        return;
    }
    // A link the seam would not make (planted by std) pointing outside the
    // root: its resolution has no caller's path, so it is refused rather
    // than the real path leaking.
    let outside = tempfile::tempdir().unwrap();
    let fs = Rooted::temp().unwrap();
    fs.create_dir_all(Path::new("/w"), Mode::DIR).unwrap();
    let link = fs.real_path(Path::new("/w/out")).unwrap();
    host().link_dir(outside.path(), &link).unwrap();
    let e = fs.canonicalize(Path::new("/w/out")).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::PermissionDenied);
}
