//! A test's database file: `state.db` in a directory made for it alone
//! under the temporary directory, removed with everything in it when its
//! guard drops, on a pass and on a panic alike. Every test of this crate
//! that puts a file on disk takes its path from here.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Once;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The prefix of every directory `database` makes, up to the pid.
const PREFIX: &str = concat!(env!("CARGO_PKG_NAME"), "-test-");

/// A leftover is swept only once it is this old, so a directory of a live
/// process in another pid namespace sharing the temporary directory is
/// never taken from under it.
const STALE_AFTER: Duration = Duration::from_secs(600);

/// A path for a database file of this test run's own, `state.db` in a
/// directory made for it alone, removed with what is in it (the file and
/// its WAL files, or a directory made at the path) when the guard drops,
/// the test passing or not. Declared before the stores on it, it drops
/// after them.
pub(crate) fn database() -> Database {
    static RUN: AtomicUsize = AtomicUsize::new(0);
    static SWEPT: Once = Once::new();
    // A guard cannot drop in a process nothing let run to its end (killed
    // by a timeout or a memory cap), so the next run takes its leftovers.
    SWEPT.call_once(|| {
        sweep_stale(&std::env::temp_dir(), STALE_AFTER, process_is_alive);
    });
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

/// Whether process `pid` exists. Where that cannot be told (no procfs) it
/// says yes, so nothing is swept there.
fn process_is_alive(pid: u32) -> bool {
    if cfg!(target_os = "linux") {
        Path::new("/proc").join(pid.to_string()).exists()
    } else {
        true
    }
}

/// Remove, from `root`, each `<crate>-test-<pid>-...` directory whose
/// process `alive` says is gone and that has not been touched for
/// `min_age`: what a run killed before its guards dropped left behind.
/// Answers the number removed.
pub(crate) fn sweep_stale(root: &Path, min_age: Duration, alive: impl Fn(u32) -> bool) -> usize {
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    let mut removed = 0;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(rest) = name.to_str().and_then(|n| n.strip_prefix(PREFIX)) else {
            continue;
        };
        let Some(pid) = rest.split('-').next().and_then(|p| p.parse::<u32>().ok()) else {
            continue;
        };
        let old = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .is_some_and(|age| age >= min_age);
        if entry.path().is_dir()
            && old
            && !alive(pid)
            && std::fs::remove_dir_all(entry.path()).is_ok()
        {
            removed += 1;
        }
    }
    removed
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

#[test]
fn a_run_killed_before_its_guards_dropped_has_its_directories_swept() {
    let root = database();
    std::fs::create_dir(&root).unwrap();
    let make = |name: &str| {
        let dir = root.join(name);
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("state.db"), "").unwrap();
        dir
    };
    let dead = make(&format!("{PREFIX}4000000-1-0"));
    let alive = make(&format!("{PREFIX}4000001-1-0"));
    let other = make("some-other-4000000-1-0");
    let unparsed = make(&format!("{PREFIX}notapid-1-0"));
    let young = make(&format!("{PREFIX}4000000-2-0"));

    // Nothing is old enough yet.
    assert_eq!(sweep_stale(&root, Duration::from_secs(3600), |_| false), 0);
    assert!(dead.exists());

    let removed = sweep_stale(&root, Duration::ZERO, |pid| pid == 4_000_001);
    assert!(!dead.exists(), "a dead process's directory was left");
    assert!(!young.exists(), "a dead process's directory was left");
    assert!(alive.exists(), "a live process's directory was taken");
    assert!(other.exists(), "another name was taken");
    assert!(unparsed.exists(), "a name with no pid was taken");
    assert_eq!(removed, 2);
}
