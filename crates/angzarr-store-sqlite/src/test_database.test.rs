//! A test's database file: `state.db` in a directory made for it alone
//! under the temporary directory, removed with everything in it when its
//! guard drops, on a pass and on a panic alike. Every test of this crate
//! that puts a file on disk takes its path from here.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A path for a database file of this test run's own, `state.db` in a
/// directory made for it alone, removed with what is in it (the file and
/// its WAL files, or a directory made at the path) when the guard drops,
/// the test passing or not. Declared before the stores on it, it drops
/// after them.
pub(crate) fn database() -> Database {
    static RUN: AtomicUsize = AtomicUsize::new(0);
    database_in(&std::env::temp_dir(), || {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos());
        format!(
            concat!(env!("CARGO_PKG_NAME"), "-test-{}-{}-{}"),
            std::process::id(),
            nanos,
            RUN.fetch_add(1, Ordering::Relaxed)
        )
    })
}

/// `state.db` in a directory made in `root` under the first name from
/// `name` that nothing in `root` holds. The directory is made only if none
/// is there (`create_dir`), so no leftover is ever reused: a pid comes
/// round and a counter restarts in every process, and a run killed before
/// its guard dropped, or one from before the guards removed their files,
/// leaves its database under a name that is given again.
pub(crate) fn database_in(root: &Path, mut name: impl FnMut() -> String) -> Database {
    loop {
        let dir = root.join(name());
        match std::fs::create_dir(&dir) {
            Ok(()) => {
                let file = dir.join("state.db");
                return Database { dir, file };
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => panic!("cannot make {}: {e}", dir.display()),
        }
    }
}

/// A database file's path, and the directory made for it, removed on drop.
pub(crate) struct Database {
    dir: PathBuf,
    file: PathBuf,
}

impl std::ops::Deref for Database {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.file
    }
}

impl AsRef<Path> for Database {
    fn as_ref(&self) -> &Path {
        &self.file
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// The database file at `path` and its WAL files.
fn with_wal(path: &Path) -> [PathBuf; 3] {
    ["", "-wal", "-shm"].map(|suffix| {
        let mut file = path.as_os_str().to_owned();
        file.push(suffix);
        PathBuf::from(file)
    })
}

#[test]
fn a_database_and_its_wal_files_are_gone_once_dropped() {
    let path = database();
    let dir = path.parent().unwrap().to_path_buf();
    let files = with_wal(&path);
    for file in &files {
        std::fs::write(file, "").unwrap();
    }
    drop(path);
    for file in &files {
        assert!(!file.exists(), "{} was left behind", file.display());
    }
    assert!(!dir.exists(), "{} was left behind", dir.display());
}

#[test]
fn a_directory_made_at_a_database_path_is_gone_once_dropped() {
    let dir = database();
    std::fs::create_dir(&dir).unwrap();
    std::fs::write(dir.join("state.db"), "").unwrap();
    let path = dir.to_path_buf();
    drop(dir);
    assert!(!path.exists(), "{} was left behind", path.display());
}
