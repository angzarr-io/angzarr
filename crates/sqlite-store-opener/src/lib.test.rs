//! Opening a project's store on a filesystem of the test's own
//! (`Rooted::temp`, which SQLite reaches through the seam's real path), and
//! checking a database for damage. Fixtures are written with `SqliteStore`
//! itself, never through the opener under test, or frozen in `testdata/`.
//! Every path is under a temporary root; nothing touches a real home.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use angzarr_ports::{ListStreams, StoreError};
use angzarr_store_sqlite::{SqlError, SqliteStore};
use coded_diagnostic::{code, Arg, Diagnostic};
use file_system::{Fs, Os, Perms, Rooted};
use os::Mode;

use super::{Checked, OpenError, Opener};

#[path = "damage.test.rs"]
pub(crate) mod damage;

/// The file name the tests' stores have; made up.
const FILE: &str = "store.db";
/// The app directory the tests' stores are under; made up.
const APP: &str = ".demo";

fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("a runtime")
        .block_on(future)
}

/// A project's store as the path policy places it:
/// `<home>/.demo/projects/<id>/store.db`.
fn store_in(home: &Path, id: &str) -> PathBuf {
    home.join(APP).join("projects").join(id).join(FILE)
}

/// A home, `/home/u`, on a filesystem of its own.
struct Home {
    rooted: Arc<Rooted>,
    path: PathBuf,
}

impl Home {
    fn new() -> Home {
        let home = Home {
            rooted: Arc::new(Rooted::temp().unwrap()),
            path: os::fixture("/home/u"),
        };
        home.mkdir(&home.path);
        home
    }

    fn path(&self) -> &Path {
        &self.path
    }

    /// The opener on the home's filesystem.
    fn opener(&self) -> Opener {
        Opener::new(Arc::clone(&self.rooted) as Arc<dyn Fs>, FILE)
    }

    fn exists(&self, path: &Path) -> bool {
        self.rooted.exists(path).unwrap()
    }

    fn mkdir(&self, path: &Path) {
        self.rooted.create_dir_all(path, Mode::DIR).unwrap();
    }

    fn write(&self, path: &Path, bytes: &[u8]) {
        self.rooted
            .write_atomic(path, bytes, Perms::Exact(Mode::FILE))
            .unwrap();
    }

    fn read(&self, path: &Path) -> Vec<u8> {
        self.rooted.read(path).unwrap()
    }

    /// Where SQLite finds `path`.
    fn real(&self, path: &Path) -> PathBuf {
        self.rooted.real_path(path).unwrap()
    }
}

/// A database on the embedded store's schema at `path`, written by
/// `SqliteStore` itself (not the opener under test), closed, so the file
/// holds everything. The store's connection closes on its own thread after
/// the store is dropped, and the last connection's close checkpoints the log
/// into the file and removes it: that removal is waited for, up to a
/// deadline, as the sign the file is whole.
fn written(home: &Home, path: &Path) {
    home.mkdir(path.parent().unwrap());
    let real = home.real(path);
    block_on(async { drop(SqliteStore::open(&real).await.unwrap()) });
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while home.exists(Path::new(&wal)) {
        assert!(
            std::time::Instant::now() < deadline,
            "the store's connection never closed: its log is still beside the file"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// An arg's value as text, a nested cause as its code.
fn text(arg: &Arg) -> String {
    match arg {
        Arg::Int(n) => n.to_string(),
        Arg::Uint(n) => n.to_string(),
        Arg::Str(s) => s.clone(),
        Arg::Nested(cause) => cause.code().to_string(),
    }
}

/// A diagnostic's args, each as text, by name, in its order.
fn args(diagnostic: &dyn Diagnostic) -> Vec<(&str, String)> {
    diagnostic
        .args()
        .iter()
        .map(|(name, arg)| (*name, text(arg)))
        .collect()
}

/// The store's cause of `error` (its `Unavailable`'s), as the store reports
/// it: panics if `error` is not the store's own.
fn store_cause(error: &OpenError) -> &StoreError {
    match error {
        OpenError::Store(inner) => inner,
        other => panic!("not the store's own: {other:?}"),
    }
}

// ------------------------------------------------------------ the path

#[test]
fn test_the_opener_places_its_file_name_in_a_directory() {
    let home = Home::new();
    assert_eq!(
        home.opener().file_in(Path::new("/h/.demo/projects/p")),
        Path::new("/h/.demo/projects/p/store.db")
    );
    let other = Opener::new(Arc::clone(&home.rooted) as Arc<dyn Fs>, "other.db");
    assert_eq!(
        other.file_in(Path::new("rel/dir")),
        Path::new("rel/dir/other.db")
    );
}

// ------------------------------------------------------------ opening

#[test]
fn test_a_fresh_store_is_created_where_the_path_policy_puts_it_and_migrated() {
    let home = Home::new();
    let path = store_in(home.path(), "jolly-mute-magma");
    block_on(home.opener().open_for_write(&path)).expect("opens");
    // The caller's top under the root: the root itself on unix, its `C`
    // directory on Windows (`C:\` re-rooted).
    let top = match os::host().flavor() {
        os::Flavor::Unix => home.rooted.root().to_path_buf(),
        os::Flavor::Windows => home.rooted.root().join("C"),
    };
    let real = ["home", "u", ".demo", "projects", "jolly-mute-magma", FILE]
        .iter()
        .fold(top, |path, part| path.join(part));
    assert_eq!(home.real(&path), real);
    let os = Os::new(os::host(), Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap();
    assert!(os.exists(&real).unwrap(), "under the filesystem's root");
    // Migrated: a read-only open refuses a store with a migration pending.
    let read = block_on(SqliteStore::open_existing(&real)).unwrap();
    assert!(read.is_some(), "every migration applied");
}

#[test]
fn test_a_store_on_the_embedded_schema_reopens_to_write_and_to_read() {
    let home = Home::new();
    let path = store_in(home.path(), "jolly-mute-magma");
    written(&home, &path);
    let before = home.read(&path);
    block_on(home.opener().open_for_write(&path)).expect("reopens to write");
    let found = block_on(home.opener().open_existing(&path)).expect("reopens to read");
    assert!(found.is_some());
    assert_eq!(home.read(&path).len(), before.len(), "no new file made");
}

#[test]
fn test_reading_a_project_with_no_database_finds_none_and_creates_nothing() {
    let home = Home::new();
    let path = store_in(home.path(), "tpyo");
    let found = block_on(home.opener().open_existing(&path)).map(|s| s.is_some());
    assert_eq!(found, Ok(false));
    assert!(
        !home.exists(&home.path().join(APP)),
        "no directory, no file"
    );
}

#[test]
fn test_reading_with_the_directory_but_no_database_finds_none_and_makes_none() {
    let home = Home::new();
    let path = store_in(home.path(), "p");
    home.mkdir(path.parent().unwrap());
    let found = block_on(home.opener().open_existing(&path)).map(|s| s.is_some());
    assert_eq!(found, Ok(false));
    assert!(!home.exists(&path));
}

#[test]
fn test_the_openers_futures_are_send_so_a_server_can_spawn_them() {
    fn is_send<T: Send>(_: &T) {}
    let home = Home::new();
    let opener = home.opener();
    let path = PathBuf::from("/nowhere/store.db");
    is_send(&opener.open_for_write(&path));
    is_send(&opener.open_existing(&path));
    is_send(&opener.check(&path));
}

// ------------------------------------- a store from before the embedded store
//
// A store written before the embedded store existed (an earlier event
// store's, its events under that store's rules) is not migrated. Opened, it
// is refused, keyed `OpenError::PredatesEmbedded` with its path and the path
// to back it up to (`<path>.pre-embedded`), so the user moves it aside and
// starts afresh; and it is left exactly as it was, never migrated in place
// (the store's open would migrate it, one way). Its fixture is that
// store's file, frozen in testdata.

/// A store as an earlier event store wrote one: one stream of two events
/// under its rules, its only migration its first. Frozen from what that
/// store wrote, closed so the file holds everything, so the test needs
/// nothing of it once the store leaves it.
const PRE_EMBEDDED: &[u8] = include_bytes!("../testdata/pre-embedded.db");

#[test]
fn test_a_store_that_predates_the_embedded_store_is_refused_naming_its_backup() {
    let home = Home::new();
    let path = store_in(home.path(), "jolly-mute-magma");
    home.mkdir(path.parent().unwrap());
    home.write(&path, PRE_EMBEDDED);
    let before = home.read(&path);
    let mut backup = path.as_os_str().to_owned();
    backup.push(".pre-embedded");
    let expected = vec![
        ("path".to_string(), path.display().to_string()),
        (
            "backup".to_string(),
            PathBuf::from(backup).display().to_string(),
        ),
    ];
    let refusal = |refusal: Option<OpenError>| {
        let refusal = refusal.expect("refused, never opened");
        assert_eq!(refusal.code(), code!("predates-embedded"));
        args(&refusal)
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect::<Vec<_>>()
    };

    let opener = home.opener();
    let written = block_on(opener.open_for_write(&path)).map(drop).err();
    assert_eq!(refusal(written), expected, "to write");
    let read = block_on(opener.open_existing(&path)).map(drop).err();
    assert_eq!(refusal(read), expected, "to read");
    assert_eq!(home.read(&path), before, "the file as it was: not migrated");
}

// ------------------------------------------------------------ opening, failing

/// A `Rooted` filesystem with one fault.
#[derive(Debug)]
struct Faulty {
    fs: Rooted,
    fault: Fault,
}

/// What a [`Faulty`] filesystem cannot do.
#[derive(Debug)]
enum Fault {
    /// Say where anything is on disk, as a real path a Windows prefix
    /// leaves relative cannot be.
    Unplaced,
    /// Set the mode of this path (the caller's).
    NoMode(PathBuf),
}

impl Faulty {
    fn new(fault: Fault) -> Faulty {
        Faulty {
            fs: Rooted::temp().unwrap(),
            fault,
        }
    }
}

impl Fs for Faulty {
    fn cwd(&self) -> Result<&Path, file_system::CwdError> {
        self.fs.cwd()
    }
    fn os(&self) -> &dyn os::Os {
        self.fs.os()
    }
    fn read(&self, path: &Path) -> std::io::Result<Vec<u8>> {
        self.fs.read(path)
    }
    fn read_to_string(&self, path: &Path) -> std::io::Result<String> {
        self.fs.read_to_string(path)
    }
    fn write_atomic(&self, path: &Path, bytes: &[u8], perms: Perms) -> std::io::Result<()> {
        self.fs.write_atomic(path, bytes, perms)
    }
    fn open(&self, path: &Path, how: file_system::Open) -> std::io::Result<std::fs::File> {
        self.fs.open(path, how)
    }
    fn create_dir_all(&self, path: &Path, mode: Mode) -> std::io::Result<()> {
        self.fs.create_dir_all(path, mode)
    }
    fn remove_file(&self, path: &Path) -> std::io::Result<()> {
        self.fs.remove_file(path)
    }
    fn remove_dir_all(&self, path: &Path) -> std::io::Result<()> {
        self.fs.remove_dir_all(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
        self.fs.rename(from, to)
    }
    fn set_permissions(&self, path: &Path, mode: Mode) -> std::io::Result<()> {
        match &self.fault {
            Fault::NoMode(refused) if refused == path => Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "no mode set",
            )),
            _ => self.fs.set_permissions(path, mode),
        }
    }
    fn read_dir(&self, path: &Path) -> std::io::Result<Vec<file_system::Entry>> {
        self.fs.read_dir(path)
    }
    fn metadata(&self, path: &Path) -> std::io::Result<file_system::Meta> {
        self.fs.metadata(path)
    }
    fn symlink_metadata(&self, path: &Path) -> std::io::Result<file_system::Meta> {
        self.fs.symlink_metadata(path)
    }
    fn exists(&self, path: &Path) -> std::io::Result<bool> {
        self.fs.exists(path)
    }
    fn canonicalize(&self, path: &Path) -> std::io::Result<PathBuf> {
        self.fs.canonicalize(path)
    }
    fn symlink_dir(&self, target: &Path, link: &Path) -> std::io::Result<()> {
        self.fs.symlink_dir(target, link)
    }
    fn read_link(&self, link: &Path) -> std::io::Result<PathBuf> {
        self.fs.read_link(link)
    }
    fn make_temp_dir(&self, parent: &Path, prefix: &str) -> std::io::Result<PathBuf> {
        self.fs.make_temp_dir(parent, prefix)
    }
    fn real_path(&self, path: &Path) -> std::io::Result<PathBuf> {
        match self.fault {
            Fault::Unplaced => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "no place on disk",
            )),
            Fault::NoMode(_) => self.fs.real_path(path),
        }
    }
    fn same_file(&self, file: &std::fs::File, path: &Path) -> std::io::Result<bool> {
        self.fs.same_file(file, path)
    }
}

/// A database older than this build: one table committed, and none of the
/// store's migrations applied. Made with Python's sqlite3:
/// `PRAGMA page_size=512; CREATE TABLE older (x)`.
const OLDER: &[u8] = include_bytes!("../testdata/older.db");

#[test]
fn test_a_directory_that_cannot_be_made_fails_naming_it() {
    let home = Home::new();
    let blocker = home.path().join("projects");
    home.write(&blocker, b"a file where the directory goes");
    let path = blocker.join("jolly-mute-magma").join(FILE);
    let error = block_on(home.opener().open_for_write(&path))
        .map(drop)
        .unwrap_err();
    assert_eq!(error.code(), code!("create-dir"));
    let args = args(&error);
    assert_eq!(args[0].0, "path");
    assert!(
        args[0].1.contains(&blocker.display().to_string()),
        "{error}"
    );
    assert_eq!(args[1].0, "ext_err_txt");
    assert!(!args[1].1.is_empty());
}

#[test]
fn test_a_database_that_does_not_open_fails_with_the_stores_error() {
    let home = Home::new();
    // A directory where the database file goes.
    let path = home.path().join(FILE);
    home.mkdir(&path);
    let error = block_on(home.opener().open_for_write(&path))
        .map(drop)
        .unwrap_err();
    assert!(
        matches!(store_cause(&error), StoreError::Unavailable(cause) if cause.code == sqlite_unavailable()),
        "the store's: {error}"
    );
}

#[test]
fn test_a_store_the_filesystem_cannot_place_is_refused_naming_it() {
    let opener = Opener::new(Arc::new(Faulty::new(Fault::Unplaced)), FILE);
    let path = PathBuf::from("/home/u/store.db");
    let expected = [
        ("path", "/home/u/store.db".to_string()),
        ("ext_err_txt", "no place on disk".to_string()),
    ];
    let unplaced = |error: OpenError| {
        assert_eq!(error.code(), code!("unplaced"));
        assert_eq!(args(&error), expected);
    };
    unplaced(
        block_on(opener.open_for_write(&path))
            .map(drop)
            .unwrap_err(),
    );
    unplaced(block_on(opener.open_existing(&path)).map(drop).unwrap_err());
    unplaced(block_on(opener.check(&path)).map(drop).unwrap_err());
}

#[test]
fn test_reading_an_existing_database_opens_it_read_only() {
    let home = Home::new();
    let path = store_in(home.path(), "jolly-mute-magma");
    block_on(home.opener().open_for_write(&path)).unwrap();
    let store = block_on(home.opener().open_existing(&path))
        .unwrap()
        .expect("found");
    let streams = block_on(store.list_streams()).unwrap();
    assert!(streams.len() < 2, "readable: {}", streams.len());
}

// ------------------------------------------ a store older than the build
//
// Both a read and a write migrate a store on opening it: the store's every
// open does, backing the store up beside itself first
// (`<file>.pre-<applied level>-<UTC time>`); a store newer than the build,
// and one from before the embedded store, are refused and left as they were.

/// A store as an earlier build wrote one (migrations 1 to 4 applied, so 5
/// is pending here), with two application streams beside its schema stream,
/// closed so the file holds everything (its log empty).
const BEFORE_0005: &[u8] = include_bytes!("../testdata/before-0005.db");

/// A store newer than this build: [`BEFORE_0005`] with a migration this
/// build does not have (version 99) recorded as applied, added with
/// Python's sqlite3 and closed (its log checkpointed and removed).
const NEWER: &[u8] = include_bytes!("../testdata/newer.db");

/// The backups beside the store at `path`, by name.
fn backups(home: &Home, path: &Path) -> Vec<PathBuf> {
    let dir = path.parent().unwrap();
    home.rooted
        .read_dir(dir)
        .unwrap()
        .into_iter()
        .filter(|e| e.name.to_string_lossy().starts_with("store.db.pre-"))
        .map(|e| dir.join(e.name))
        .collect()
}

/// How many streams `store` holds.
async fn streams(store: &SqliteStore) -> usize {
    store.list_streams().await.unwrap().len()
}

/// The versions of the ledger of the store at `path` on disk, read
/// without opening it.
fn ledger(home: &Home, path: &Path) -> Vec<u64> {
    block_on(SqliteStore::ledger_at(&home.real(path)))
        .unwrap()
        .expect("a store there")
        .iter()
        .map(|r| r.version)
        .collect()
}

/// The one backup beside the store at `path`, named for `level`.
fn the_backup(home: &Home, path: &Path, level: u64) -> PathBuf {
    let backups = backups(home, path);
    let [backup] = backups.as_slice() else {
        panic!("one backup beside the store: {backups:?}");
    };
    let name = backup.file_name().unwrap().to_string_lossy().into_owned();
    assert!(
        name.starts_with(&format!("store.db.pre-{level}-")),
        "named for its level: {name}"
    );
    backup.clone()
}

/// The versions of the store's own migrations, in order: what a fully
/// migrated ledger holds and the newest one the build knows. Read from the
/// store, so adding a migration there changes no test here.
fn build_versions() -> Vec<u64> {
    SqliteStore::build_migrations()
        .iter()
        .map(|migration| migration.version)
        .collect()
}

/// A store at `path` holding `bytes`.
fn seeded(home: &Home, path: &Path, bytes: &[u8]) {
    home.mkdir(path.parent().unwrap());
    home.write(path, bytes);
}

#[test]
fn test_reading_a_store_with_a_migration_pending_migrates_it_and_reads_it() {
    let home = Home::new();
    let path = store_in(home.path(), "jolly-mute-magma");
    seeded(&home, &path, BEFORE_0005);
    let store = block_on(home.opener().open_existing(&path))
        .expect("migrated, not refused")
        .expect("found");
    assert_eq!(block_on(streams(&store)), 3, "both streams read back");
    assert_eq!(
        ledger(&home, &path),
        build_versions(),
        "every migration applied"
    );
}

#[test]
fn test_a_read_backs_a_pending_store_up_as_it_was_before_migrating_it() {
    let home = Home::new();
    let path = store_in(home.path(), "jolly-mute-magma");
    seeded(&home, &path, BEFORE_0005);
    block_on(home.opener().open_existing(&path))
        .expect("migrated")
        .expect("found");
    let backup = the_backup(&home, &path, 4);
    assert_eq!(
        ledger(&home, &backup),
        [1, 2, 3, 4],
        "the store before migrating"
    );
}

#[test]
fn test_a_write_backs_a_pending_store_up_before_migrating_it() {
    let home = Home::new();
    let path = store_in(home.path(), "jolly-mute-magma");
    seeded(&home, &path, BEFORE_0005);
    let store = block_on(home.opener().open_for_write(&path)).expect("migrated");
    assert_eq!(block_on(streams(&store)), 3);
    let backup = the_backup(&home, &path, 4);
    assert_eq!(ledger(&home, &backup), [1, 2, 3, 4]);
}

#[test]
fn test_a_current_store_is_neither_backed_up_nor_migrated() {
    let home = Home::new();
    let path = store_in(home.path(), "jolly-mute-magma");
    written(&home, &path);
    block_on(home.opener().open_existing(&path))
        .unwrap()
        .expect("found");
    block_on(home.opener().open_for_write(&path)).unwrap();
    assert_eq!(backups(&home, &path), Vec::<PathBuf>::new());
}

#[test]
fn test_reading_a_database_older_than_every_migration_backs_it_up_and_migrates_it() {
    // A file holding something committed but none of the migrations.
    let home = Home::new();
    let path = home.path().join(FILE);
    home.write(&path, OLDER);
    let found = block_on(home.opener().open_existing(&path)).map(|found| found.is_some());
    assert_eq!(found, Ok(true));
    let backup = the_backup(&home, &path, 0);
    assert_eq!(
        ledger(&home, &backup),
        Vec::<u64>::new(),
        "as it was: no ledger"
    );
}

#[test]
fn test_a_store_newer_than_the_build_is_refused_and_left_as_it_was() {
    let home = Home::new();
    let path = store_in(home.path(), "jolly-mute-magma");
    seeded(&home, &path, NEWER);
    let refusal = OpenError::Store(StoreError::NewerThanBuild {
        applied: 99,
        build: *build_versions().last().expect("the build has migrations"),
    });
    let read = block_on(home.opener().open_existing(&path)).map(|found| found.is_some());
    assert_eq!(read, Err(refusal.clone()), "to read");
    let written = block_on(home.opener().open_for_write(&path)).map(drop);
    assert_eq!(written, Err(refusal), "to write");
    assert_eq!(home.read(&path), NEWER, "the file as it was");
    assert_eq!(backups(&home, &path), Vec::<PathBuf>::new(), "no backup");
}

#[test]
fn test_a_database_that_cannot_be_read_fails_a_read() {
    let home = Home::new();
    let path = home.path().join(FILE);
    home.write(&path, b"not a database");
    let error = block_on(home.opener().open_existing(&path))
        .map(|found| found.is_some())
        .unwrap_err();
    assert!(
        matches!(store_cause(&error), StoreError::Unavailable(_)),
        "{error:?}"
    );
}

#[test]
fn test_a_read_that_fails_fails_with_the_read_only_opens_error() {
    // A directory where the database goes: the read-only open fails, and
    // its error is passed through as it is.
    let home = Home::new();
    let path = home.path().join(FILE);
    home.mkdir(&path);
    let error = block_on(home.opener().open_existing(&path))
        .map(|found| found.is_some())
        .unwrap_err();
    let read_only = block_on(SqliteStore::open_existing(&home.real(&path)))
        .map(|found| found.is_some())
        .unwrap_err();
    assert_eq!(store_cause(&error), &read_only);
}

// ------------------------------------------------------------ the check

/// A copy of the database at `path`, at `copy`, its command index's root
/// page changed by `tamper`.
fn damaged_copy(home: &Home, path: &Path, copy: &Path, tamper: impl FnOnce(&mut [u8])) {
    let file = damage::damaged(&home.read(path), "events_by_command", tamper);
    home.mkdir(copy.parent().unwrap());
    home.write(copy, &file);
}

fn checked(home: &Home, path: &Path) -> Result<Checked, OpenError> {
    block_on(home.opener().check(path))
}

#[test]
fn test_a_sound_database_checks_intact() {
    let home = Home::new();
    let path = store_in(home.path(), "p");
    written(&home, &path);
    assert_eq!(checked(&home, &path), Ok(Checked::Intact));
}

#[test]
fn test_damage_is_reported_as_the_stores_key_and_args_and_left_as_it_was() {
    let home = Home::new();
    let (path, copy) = (store_in(home.path(), "p"), store_in(home.path(), "copy"));
    written(&home, &path);
    damaged_copy(&home, &path, &copy, |page| page.fill(0xff));
    let before = home.read(&copy);
    let Ok(Checked::Damaged(damage)) = checked(&home, &copy) else {
        panic!("the damage went unseen");
    };
    assert_eq!(damage.code(), damaged());
    let said = args(&damage);
    assert_eq!(
        said.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
        ["first", "more"],
        "a key and its args: {damage}"
    );
    assert!(
        damage
            .to_string()
            .starts_with(&format!("{} first=\"", damaged())),
        "its Display is its key and args, the problem quoted, never English: {damage}"
    );
    assert_eq!(home.read(&copy), before, "never repaired");
    assert_eq!(
        checked(&home, &path),
        Ok(Checked::Intact),
        "the original is sound"
    );
}

#[test]
fn test_a_file_too_damaged_to_open_is_damage_with_sqlites_text_as_ext_err_txt() {
    let home = Home::new();
    let path = home.path().join(FILE);
    home.write(&path, b"not a database at all, as SQLite reads a file");
    let Ok(Checked::Damaged(damage)) = checked(&home, &path) else {
        panic!("damage, not a failure to check");
    };
    assert_eq!(
        damage.code(),
        SqlError::DamagedFile {
            errno: 0,
            detail: String::new()
        }
        .code()
    );
    assert_eq!(
        args(&damage),
        [
            ("errno", "26".to_string()),
            ("ext_err_txt", "file is not a database".to_string())
        ],
        "SQLITE_NOTADB, and SQLite's text verbatim: {damage}"
    );
}

#[test]
fn test_a_check_that_cannot_run_fails_with_the_stores_error() {
    let home = Home::new();
    // A directory where the database file goes.
    let path = home.path().join(FILE);
    home.mkdir(&path);
    let failed = checked(&home, &path).unwrap_err();
    let direct = block_on(SqliteStore::open_existing(&home.real(&path)))
        .map(|found| found.is_some())
        .unwrap_err();
    assert_eq!(store_cause(&failed), &direct);
    assert!(
        matches!(&direct, StoreError::Unavailable(cause) if cause.code == sqlite_unavailable()),
        "{direct}"
    );
}

#[test]
fn test_no_database_is_absent_not_damage_and_creates_nothing() {
    let home = Home::new();
    let path = store_in(home.path(), "p");
    assert_eq!(checked(&home, &path), Ok(Checked::Absent));
    assert!(!home.exists(&home.path().join(APP)), "nothing created");
}

/// angzarr-store-sqlite's code for a damaged database.
fn damaged() -> &'static str {
    angzarr_store_sqlite::damage_sample().code()
}

/// angzarr-store-sqlite's code for SQLite's own failure, its errno given.
fn sqlite_unavailable() -> &'static str {
    SqlError::Unavailable {
        errno: Some(0),
        detail: String::new(),
    }
    .code()
}

// ------------------------------------------------------------ private to its owner
//
// A store is its owner's alone (owner ruling, 2026-11-12): its directory
// 0700, the file and the `-wal` and `-shm` SQLite keeps beside it 0600,
// whatever the umask; an existing store open to others is made so on its
// next open, to write or to read. On Windows no mode is set: the profile's
// ACL holds the store (ruling M8), so those assertions are a named SKIP.

/// Whether this host keeps mode bits; says the named SKIP when it does not.
fn modes_kept(test: &str) -> bool {
    if os::host().flavor() == os::Flavor::Windows {
        println!("SKIP {test}: Windows keeps no mode bits; the profile's ACL holds the store (ruling M8)");
        return false;
    }
    true
}

impl Home {
    /// `path`'s permission bits.
    fn mode(&self, path: &Path) -> u32 {
        self.rooted
            .metadata(path)
            .unwrap()
            .mode
            .expect("a mode on unix")
            .bits()
    }

    fn chmod(&self, path: &Path, mode: Mode) {
        self.rooted.set_permissions(path, mode).unwrap();
    }
}

/// `path` with `suffix` after its file name: the `-wal` and `-shm` beside a
/// store.
fn beside(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

#[test]
fn test_a_new_store_and_every_directory_its_open_makes_are_its_owners_alone() {
    let home = Home::new();
    let path = store_in(home.path(), "jolly-mute-magma");
    // Held open: a write's log and shared memory are beside it.
    let store = block_on(home.opener().open_for_write(&path)).expect("opens");
    if !modes_kept("test_a_new_store_and_every_directory_its_open_makes_are_its_owners_alone") {
        return;
    }
    let dir = path.parent().unwrap();
    for made in [dir, dir.parent().unwrap(), &home.path().join(APP)] {
        assert_eq!(home.mode(made), 0o700, "{}", made.display());
    }
    assert_eq!(home.mode(&path), 0o600, "the store");
    for companion in ["-wal", "-shm"] {
        let companion = beside(&path, companion);
        assert!(home.exists(&companion), "{} beside it", companion.display());
        assert_eq!(home.mode(&companion), 0o600, "{}", companion.display());
    }
    drop(store);
}

#[test]
fn test_an_existing_store_open_to_others_is_made_its_owners_on_a_write_open() {
    let home = Home::new();
    let path = store_in(home.path(), "jolly-mute-magma");
    written(&home, &path);
    let dir = path.parent().unwrap();
    home.chmod(dir, Mode::DIR);
    home.chmod(&path, Mode::FILE);
    let store = block_on(home.opener().open_for_write(&path)).expect("opens");
    if !modes_kept("test_an_existing_store_open_to_others_is_made_its_owners_on_a_write_open") {
        return;
    }
    assert_eq!(home.mode(dir), 0o700, "its directory");
    assert_eq!(home.mode(&path), 0o600, "the store");
    drop(store);
}

#[test]
fn test_an_existing_store_open_to_others_is_made_its_owners_on_a_read_open() {
    let home = Home::new();
    let path = store_in(home.path(), "jolly-mute-magma");
    written(&home, &path);
    let dir = path.parent().unwrap();
    home.chmod(dir, Mode::DIR);
    home.chmod(&path, Mode::FILE);
    let store = block_on(home.opener().open_existing(&path))
        .expect("opens")
        .expect("found");
    if !modes_kept("test_an_existing_store_open_to_others_is_made_its_owners_on_a_read_open") {
        return;
    }
    assert_eq!(home.mode(dir), 0o700, "its directory");
    assert_eq!(home.mode(&path), 0o600, "the store");
    for companion in ["-wal", "-shm"] {
        let companion = beside(&path, companion);
        assert!(home.exists(&companion), "{} beside it", companion.display());
        assert_eq!(home.mode(&companion), 0o600, "{}", companion.display());
    }
    drop(store);
}

#[test]
fn test_a_log_and_shared_memory_open_to_others_are_made_the_owners_on_the_next_open() {
    let home = Home::new();
    let path = store_in(home.path(), "jolly-mute-magma");
    // Held open, so its log and shared memory stay, as an earlier build
    // left them: open to others.
    let held = block_on(home.opener().open_for_write(&path)).expect("opens");
    let (wal, shm) = (beside(&path, "-wal"), beside(&path, "-shm"));
    if !modes_kept(
        "test_a_log_and_shared_memory_open_to_others_are_made_the_owners_on_the_next_open",
    ) {
        return;
    }
    home.chmod(&wal, Mode::FILE);
    home.chmod(&shm, Mode::FILE);
    let written = block_on(home.opener().open_for_write(&path)).expect("opens to write");
    assert_eq!(
        (home.mode(&wal), home.mode(&shm)),
        (0o600, 0o600),
        "to write"
    );
    home.chmod(&wal, Mode::FILE);
    home.chmod(&shm, Mode::FILE);
    let read = block_on(home.opener().open_existing(&path)).expect("opens to read");
    assert_eq!(
        (home.mode(&wal), home.mode(&shm)),
        (0o600, 0o600),
        "to read"
    );
    drop((held, written, read));
}

impl Home {
    /// The opener on this home's filesystem, which cannot set `refused`'s
    /// mode.
    fn refusing_mode(&self, refused: &Path) -> Opener {
        let fs = Faulty {
            fs: Rooted::new(self.rooted.root(), self.path()).unwrap(),
            fault: Fault::NoMode(refused.to_path_buf()),
        };
        Opener::new(Arc::new(fs), FILE)
    }
}

/// What a store at `path` is made its owner's through: its directory, the
/// file, its log and its shared memory.
fn made_private(path: &Path) -> [PathBuf; 4] {
    [
        path.parent().unwrap().to_path_buf(),
        path.to_path_buf(),
        beside(path, "-wal"),
        beside(path, "-shm"),
    ]
}

/// `error` is the opener's refusal to open what it could not make private,
/// `refused`, with the filesystem's text.
fn refused_private(error: &OpenError, refused: &Path) {
    assert_eq!(error.code(), code!("restrict"), "{refused:?}: {error}");
    assert_eq!(
        args(error),
        [
            ("path", refused.display().to_string()),
            ("ext_err_txt", "no mode set".to_string()),
        ]
    );
}

#[test]
fn test_a_write_open_of_a_store_that_cannot_be_made_its_owners_fails_naming_what() {
    let home = Home::new();
    let path = store_in(home.path(), "jolly-mute-magma");
    for refused in made_private(&path) {
        let error = block_on(home.refusing_mode(&refused).open_for_write(&path))
            .map(drop)
            .unwrap_err();
        refused_private(&error, &refused);
    }
}

#[test]
fn test_a_read_open_of_a_store_that_cannot_be_made_its_owners_fails_naming_what() {
    let home = Home::new();
    let path = store_in(home.path(), "jolly-mute-magma");
    written(&home, &path);
    for refused in made_private(&path) {
        let error = block_on(home.refusing_mode(&refused).open_existing(&path))
            .map(drop)
            .unwrap_err();
        refused_private(&error, &refused);
    }
}
