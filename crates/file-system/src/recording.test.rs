use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ::os::{Flavor, Mode};

use super::{Call, Recording};
use crate::{Fs, Kind, Open, Perms, Rooted};

fn base() -> PathBuf {
    let rooted = Rooted::temp().unwrap();
    if rooted.os().flavor() == Flavor::Windows {
        PathBuf::from(r"C:\work")
    } else {
        PathBuf::from("/work")
    }
}

/// A recording over a re-rooted filesystem working in `base()`, made.
fn recording() -> (Recording, PathBuf) {
    let base = base();
    let inner = Rooted::temp_with_cwd(&base).unwrap();
    inner.create_dir_all(&base, Mode::DIR).unwrap();
    (Recording::over(Arc::new(inner)), base)
}

#[test]
fn test_every_call_that_changes_the_disk_is_handed_on_and_kept_in_order() {
    let (fs, base) = recording();
    let (dir, a, b, c) = (
        base.join("d"),
        base.join("d/a"),
        base.join("d/b"),
        base.join("d/c"),
    );
    fs.create_dir_all(&dir, Mode::DIR).unwrap();
    fs.write_atomic(&a, b"one", Perms::Exact(Mode::FILE))
        .unwrap();
    fs.rename(&a, &b).unwrap();
    fs.sync_dir(&dir).unwrap();
    fs.rename_noreplace(&b, &c).unwrap();
    drop(fs.open(&a, Open::CreateNew(Mode::FILE)).unwrap());
    fs.set_permissions(&a, Mode::from_bits(0o600)).unwrap();
    fs.remove_file(&a).unwrap();
    let link = base.join("link");
    fs.symlink_dir(&dir, &link).unwrap();
    let temp = fs.make_temp_dir(&base, "t").unwrap();
    fs.remove_dir_all(&temp).unwrap();

    assert_eq!(fs.read(&c).unwrap(), b"one");
    assert!(!fs.exists(&a).unwrap() && !fs.exists(&b).unwrap());
    assert_eq!(fs.symlink_metadata(&link).unwrap().kind, Kind::Symlink);
    assert!(!fs.exists(&temp).unwrap());
    assert_eq!(
        fs.calls(),
        vec![
            Call::CreateDirAll(dir.clone()),
            Call::WriteAtomic(a.clone()),
            Call::Rename {
                from: a.clone(),
                to: b.clone()
            },
            Call::SyncDir(dir.clone()),
            Call::RenameNoreplace {
                from: b.clone(),
                to: c.clone()
            },
            Call::Create(a.clone()),
            Call::SetPermissions(a.clone()),
            Call::RemoveFile(a.clone()),
            Call::SymlinkDir {
                target: dir.clone(),
                link: link.clone()
            },
            Call::MakeTempDir(base.clone()),
            Call::RemoveDirAll(temp.clone()),
        ]
    );
}

#[test]
fn test_a_read_is_handed_on_and_kept_nowhere() {
    let (fs, base) = recording();
    let path = base.join("f");
    fs.inner
        .write_atomic(&path, b"text", Perms::Exact(Mode::FILE))
        .unwrap();
    assert_eq!(fs.cwd().unwrap(), base.as_path());
    assert_eq!(fs.read(&path).unwrap(), b"text");
    assert_eq!(fs.read_to_string(&path).unwrap(), "text");
    drop(fs.open(&path, Open::Read).unwrap());
    assert_eq!(fs.read_dir(&base).unwrap().len(), 1);
    assert_eq!(fs.metadata(&path).unwrap().len, 4);
    assert_eq!(fs.symlink_metadata(&path).unwrap().kind, Kind::File);
    assert!(fs.exists(&path).unwrap());
    assert_eq!(fs.canonicalize(&path).unwrap(), path);
    assert_eq!(
        fs.real_path(&path).unwrap(),
        fs.inner.real_path(&path).unwrap()
    );
    let file = fs.open(&path, Open::Read).unwrap();
    assert!(fs.same_file(&file, &path).unwrap());
    assert_eq!(fs.os().flavor(), fs.inner.os().flavor());
    assert_eq!(fs.calls(), vec![]);
}

#[test]
fn test_a_link_is_read_through_the_inner_filesystem() {
    let (fs, base) = recording();
    let link = base.join("link");
    fs.inner.symlink_dir(&base, &link).unwrap();
    assert_eq!(
        fs.read_link(&link).unwrap(),
        fs.inner.read_link(&link).unwrap()
    );
}

#[test]
fn test_a_call_that_fails_is_kept_and_its_failure_returned() {
    let (fs, base) = recording();
    let missing = base.join("missing");
    let e = fs.rename(&missing, Path::new("x")).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::NotFound, "{e}");
    assert_eq!(
        fs.calls(),
        vec![Call::Rename {
            from: missing,
            to: PathBuf::from("x")
        }]
    );
}
