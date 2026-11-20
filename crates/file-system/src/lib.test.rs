//! The contract every [`Fs`] keeps: each test runs once against [`Os`]
//! (under a temporary directory, by its real path) and once against
//! [`Rooted`] (at a caller's path that names no real directory, `/work`),
//! with the same assertions. Each is made working in that directory. A test that reaches past the seam (to plant a
//! link the seam would not make, or to check what std sees) says so.

use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ::os::{host, Flavor, Mode};

use super::{Entry, Fs, Kind, Open, Os, Perms, Rooted, TargetExists};

/// Whether the host has unix modes (Windows has none).
fn has_modes(case: &Case) -> bool {
    case.fs.os().flavor() == Flavor::Unix
}

/// A filesystem under test and the absolute directory its test works in.
struct Case {
    fs: Arc<dyn Fs>,
    base: PathBuf,
    _temp: Option<tempfile::TempDir>,
}

impl Case {
    fn at(&self, rel: &str) -> PathBuf {
        rel.split('/').fold(self.base.clone(), |p, c| p.join(c))
    }

    fn write(&self, rel: &str, bytes: &[u8]) -> PathBuf {
        let path = self.at(rel);
        self.fs
            .write_atomic(&path, bytes, Perms::KeepOr(Mode::FILE))
            .unwrap();
        path
    }

    fn dir(&self, rel: &str) -> PathBuf {
        let path = self.at(rel);
        self.fs.create_dir_all(&path, Mode::DIR).unwrap();
        path
    }
}

fn os_case() -> Case {
    let temp = tempfile::tempdir().unwrap();
    // The temporary directory as its links resolve (macOS's /var is
    // /private/var) and as a person spells it (no verbatim prefix on
    // Windows), so canonicalize has nothing of its own to add.
    let base = crate::real::canonicalize(&*host(), temp.path()).unwrap();
    Case {
        fs: Arc::new(Os::new(host(), &base).unwrap()),
        base,
        _temp: Some(temp),
    }
}

/// A caller's absolute path naming no real directory: only re-rooting
/// makes it writable.
fn caller_base() -> PathBuf {
    if host().flavor() == Flavor::Windows {
        PathBuf::from(r"C:\work")
    } else {
        PathBuf::from("/work")
    }
}

fn rooted_case() -> Case {
    let base = caller_base();
    let fs = Rooted::temp_with_cwd(&base).unwrap();
    fs.create_dir_all(&base, Mode::DIR).unwrap();
    Case {
        fs: Arc::new(fs),
        base,
        _temp: None,
    }
}

/// Each named test function, run once against each implementor.
macro_rules! contract {
    ($($name:ident),* $(,)?) => {$(
        mod $name {
            #[test]
            fn os() {
                super::$name(&super::os_case());
            }

            #[test]
            fn rooted() {
                super::$name(&super::rooted_case());
            }
        }
    )*};
}

// --- Decision: object-safe, shared as Arc<dyn Fs> -------------------------

#[test]
fn test_an_fs_is_held_as_a_shared_trait_object() {
    fn shared<T: Send + Sync + 'static>(_: &T) {}
    let os: Arc<dyn Fs> = Arc::new(Os::new(host(), &caller_base()).unwrap());
    shared(&os);
}

// --- Decision: a relative path resolves against the injected working ------
// --- directory; no op reads the process's ---------------------------------

fn test_a_relative_path_resolves_against_the_working_directory(case: &Case) {
    // The case's working directory is `case.base`, never the process's:
    // what a relative path names is found by its absolute spelling there.
    let fs = &case.fs;
    let rel = Path::new;
    fs.create_dir_all(rel("sub/deeper"), Mode::DIR).unwrap();
    assert_eq!(fs.metadata(&case.at("sub/deeper")).unwrap().kind, Kind::Dir);

    fs.write_atomic(rel("sub/file"), b"relative", Perms::Exact(Mode::FILE))
        .unwrap();
    assert_eq!(fs.read(&case.at("sub/file")).unwrap(), b"relative");
    assert_eq!(fs.read_to_string(rel("sub/file")).unwrap(), "relative");
    assert_eq!(fs.read(rel("sub/file")).unwrap(), b"relative");
    assert_eq!(fs.metadata(rel("sub/file")).unwrap().len, 8);
    assert_eq!(
        fs.symlink_metadata(rel("sub/file")).unwrap().kind,
        Kind::File
    );
    assert!(fs.exists(rel("sub/file")).unwrap());
    let file = fs.open(rel("sub/file"), Open::Read).unwrap();
    assert!(fs.same_file(&file, rel("sub/file")).unwrap());
    assert_eq!(
        fs.canonicalize(rel("sub/./deeper/../file")).unwrap(),
        case.at("sub/file")
    );
    assert_eq!(
        fs.real_path(rel("sub/file")).unwrap(),
        fs.real_path(&case.at("sub/file")).unwrap()
    );

    fs.rename(rel("sub/file"), rel("sub/moved")).unwrap();
    assert_eq!(fs.read(&case.at("sub/moved")).unwrap(), b"relative");
    fs.remove_file(rel("sub/moved")).unwrap();
    assert!(!fs.exists(&case.at("sub/moved")).unwrap());

    let entries = fs.read_dir(rel("sub")).unwrap();
    let want = [Entry {
        name: "deeper".into(),
        kind: Kind::Dir,
    }];
    assert_eq!(entries, want);
    fs.symlink_dir(&case.at("sub/deeper"), rel("link")).unwrap();
    assert_eq!(fs.read_link(rel("link")).unwrap(), case.at("sub/deeper"));

    let made = fs.make_temp_dir(rel("sub"), "t-").unwrap();
    assert_eq!(made.parent(), Some(case.at("sub").as_path()));
    assert_eq!(fs.metadata(&made).unwrap().kind, Kind::Dir);

    fs.remove_dir_all(rel("sub")).unwrap();
    assert!(!fs.exists(&case.at("sub")).unwrap());
}

fn test_a_relative_path_sets_permissions_in_the_working_directory(case: &Case) {
    if !has_modes(case) {
        return;
    }
    let path = case.write("file", b"x");
    case.fs
        .set_permissions(Path::new("file"), Mode::from_bits(0o640))
        .unwrap();
    assert_eq!(
        case.fs.metadata(&path).unwrap().mode,
        Some(Mode::from_bits(0o640))
    );
}

// --- Decision: errors are io::Error with the OS's own kinds ---------------

fn test_what_is_missing_is_not_found(case: &Case) {
    let fs = &case.fs;
    let missing = case.at("missing");
    let not_found = |r: std::io::Result<()>, op: &str| {
        let e = r.expect_err(op);
        assert_eq!(e.kind(), ErrorKind::NotFound, "{op}: {e}");
    };
    not_found(fs.read(&missing).map(drop), "read");
    not_found(fs.read_to_string(&missing).map(drop), "read_to_string");
    not_found(fs.open(&missing, Open::Read).map(drop), "open read");
    not_found(fs.remove_file(&missing), "remove_file");
    not_found(fs.remove_dir_all(&missing), "remove_dir_all");
    not_found(fs.read_dir(&missing).map(drop), "read_dir");
    not_found(fs.metadata(&missing).map(drop), "metadata");
    not_found(fs.symlink_metadata(&missing).map(drop), "symlink_metadata");
    not_found(fs.canonicalize(&missing).map(drop), "canonicalize");
    not_found(fs.read_link(&missing).map(drop), "read_link");
    assert!(!fs.exists(&missing).unwrap(), "exists");
}

fn test_text_that_is_not_utf8_is_invalid_data(case: &Case) {
    let path = case.write("bytes", &[0xff, 0xfe, 0x00]);
    let e = case.fs.read_to_string(&path).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::InvalidData);
    assert_eq!(case.fs.read(&path).unwrap(), [0xff, 0xfe, 0x00]);
}

// --- Decision: an atomic write replaces whole and leaves nothing beside ---

fn test_an_atomic_write_replaces_the_file_whole_and_leaves_no_temporary(case: &Case) {
    let path = case.write("config.json", b"first, and longer");
    case.fs
        .write_atomic(&path, b"second", Perms::KeepOr(Mode::FILE))
        .unwrap();
    assert_eq!(case.fs.read_to_string(&path).unwrap(), "second");
    let names: Vec<_> = case
        .fs
        .read_dir(&case.base)
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(names, ["config.json"]);
}

fn test_an_atomic_write_does_not_make_its_directory(case: &Case) {
    let path = case.at("absent/config.json");
    let e = case
        .fs
        .write_atomic(&path, b"x", Perms::Exact(Mode::FILE))
        .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::NotFound);
    assert!(!case.fs.exists(&case.at("absent")).unwrap());
}

fn test_an_exact_mode_is_applied_past_the_umask(case: &Case) {
    if !has_modes(case) {
        return;
    }
    // 0o664 is narrowed by any umask with a group- or other-write bit
    // (the usual 022 gives 0o644), so only a chmod after the create
    // leaves it.
    let path = case.at("shared");
    case.fs
        .write_atomic(&path, b"x", Perms::Exact(Mode::from_bits(0o664)))
        .unwrap();
    assert_eq!(
        case.fs.metadata(&path).unwrap().mode,
        Some(Mode::from_bits(0o664))
    );
}

fn test_keep_or_keeps_an_existing_files_mode_and_gives_a_new_one_its_own(case: &Case) {
    if !has_modes(case) {
        return;
    }
    let new = case.at("new");
    case.fs
        .write_atomic(&new, b"x", Perms::KeepOr(Mode::PRIVATE_FILE))
        .unwrap();
    assert_eq!(
        case.fs.metadata(&new).unwrap().mode,
        Some(Mode::PRIVATE_FILE)
    );

    let authored = case.at("authored");
    case.fs
        .write_atomic(&authored, b"x", Perms::Exact(Mode::from_bits(0o640)))
        .unwrap();
    case.fs
        .write_atomic(&authored, b"y", Perms::KeepOr(Mode::PRIVATE_FILE))
        .unwrap();
    assert_eq!(
        case.fs.metadata(&authored).unwrap().mode,
        Some(Mode::from_bits(0o640))
    );
}

fn test_keep_or_refuses_a_file_whose_mode_it_cannot_read(case: &Case) {
    if !has_modes(case) {
        return;
    }
    // A link to itself: its mode cannot be read, and the write must not
    // guess one and replace the link.
    let path = case.at("loop");
    case.fs.symlink_dir(&path, &path).unwrap();
    case.fs
        .write_atomic(&path, b"x", Perms::KeepOr(Mode::FILE))
        .unwrap_err();
    assert_eq!(case.fs.symlink_metadata(&path).unwrap().kind, Kind::Symlink);
}

// --- Decision: open hands back a std File (locks, appended logs) ----------

fn test_create_new_refuses_a_file_already_there(case: &Case) {
    let path = case.write("taken", b"mine");
    let e = case
        .fs
        .open(&path, Open::CreateNew(Mode::FILE))
        .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::AlreadyExists);
    assert_eq!(case.fs.read(&path).unwrap(), b"mine");

    let fresh = case.at("fresh");
    let mut file = case.fs.open(&fresh, Open::CreateNew(Mode::FILE)).unwrap();
    file.write_all(b"made").unwrap();
    drop(file);
    assert_eq!(case.fs.read(&fresh).unwrap(), b"made");
}

fn test_create_opens_a_file_there_without_truncating_it(case: &Case) {
    let path = case.write("lock", b"guarded path\n");
    drop(case.fs.open(&path, Open::Create(Mode::FILE)).unwrap());
    assert_eq!(case.fs.read(&path).unwrap(), b"guarded path\n");

    let missing = case.at("new.lock");
    drop(case.fs.open(&missing, Open::Create(Mode::FILE)).unwrap());
    assert_eq!(case.fs.read(&missing).unwrap(), b"");
}

fn test_append_writes_after_what_is_there_and_creates_what_is_not(case: &Case) {
    let path = case.at("log");
    for line in [&b"one\n"[..], b"two\n"] {
        let mut file = case.fs.open(&path, Open::Append(Mode::FILE)).unwrap();
        file.write_all(line).unwrap();
    }
    assert_eq!(case.fs.read(&path).unwrap(), b"one\ntwo\n");
}

fn test_read_opens_an_existing_file_for_reading_only(case: &Case) {
    let path = case.write("ro", b"text");
    let mut file = case.fs.open(&path, Open::Read).unwrap();
    let mut got = String::new();
    file.read_to_string(&mut got).unwrap();
    assert_eq!(got, "text");
    assert!(file.write_all(b"more").is_err(), "a read handle writes");
}

fn test_a_created_file_gets_its_mode(case: &Case) {
    if !has_modes(case) {
        return;
    }
    let path = case.at("private");
    drop(
        case.fs
            .open(&path, Open::CreateNew(Mode::PRIVATE_FILE))
            .unwrap(),
    );
    assert_eq!(
        case.fs.metadata(&path).unwrap().mode,
        Some(Mode::PRIVATE_FILE)
    );
}

// --- Decision: directories --------------------------------------------------

fn test_create_dir_all_makes_the_parents_and_may_be_repeated(case: &Case) {
    let deep = case.at("a/b/c");
    case.fs.create_dir_all(&deep, Mode::DIR).unwrap();
    case.fs.create_dir_all(&deep, Mode::DIR).unwrap();
    assert_eq!(case.fs.metadata(&deep).unwrap().kind, Kind::Dir);
    assert_eq!(case.fs.metadata(&case.at("a")).unwrap().kind, Kind::Dir);
}

fn test_a_private_directory_is_owner_only(case: &Case) {
    if !has_modes(case) {
        return;
    }
    let dir = case.at("private/inner");
    case.fs.create_dir_all(&dir, Mode::PRIVATE_DIR).unwrap();
    for d in [case.at("private"), dir] {
        assert_eq!(case.fs.metadata(&d).unwrap().mode, Some(Mode::PRIVATE_DIR));
    }
}

fn test_a_directory_lists_sorted_by_name_each_entry_as_itself(case: &Case) {
    case.write("b-file", b"x");
    let target = case.dir("c-dir");
    case.fs.symlink_dir(&target, &case.at("a-link")).unwrap();
    let entries = case.fs.read_dir(&case.base).unwrap();
    let want = [
        ("a-link", Kind::Symlink),
        ("b-file", Kind::File),
        ("c-dir", Kind::Dir),
    ]
    .map(|(name, kind)| Entry {
        name: name.into(),
        kind,
    });
    assert_eq!(entries, want);
}

fn test_remove_dir_all_removes_a_link_and_not_what_it_links_to(case: &Case) {
    let kept = case.dir("kept");
    let inside = case.write("kept/file", b"keep me");
    case.dir("doomed");
    case.fs.symlink_dir(&kept, &case.at("doomed/link")).unwrap();
    case.fs.remove_dir_all(&case.at("doomed")).unwrap();
    assert!(!case.fs.exists(&case.at("doomed")).unwrap());
    assert_eq!(case.fs.read(&inside).unwrap(), b"keep me");
}

fn test_remove_file_removes_a_link_and_not_what_it_links_to(case: &Case) {
    let target = case.dir("target");
    let link = case.at("link");
    case.fs.symlink_dir(&target, &link).unwrap();
    case.fs.remove_file(&link).unwrap();
    assert!(!case.fs.exists(&link).unwrap());
    assert!(case.fs.exists(&target).unwrap());
}

// --- Decision: metadata ------------------------------------------------------

fn test_metadata_follows_a_link_and_symlink_metadata_does_not(case: &Case) {
    let target = case.dir("target");
    let link = case.at("link");
    case.fs.symlink_dir(&target, &link).unwrap();
    assert_eq!(case.fs.metadata(&link).unwrap().kind, Kind::Dir);
    assert_eq!(case.fs.symlink_metadata(&link).unwrap().kind, Kind::Symlink);
}

fn test_metadata_reports_the_length_and_when_it_was_modified(case: &Case) {
    let before = std::time::SystemTime::now() - std::time::Duration::from_secs(5);
    let path = case.write("sized", b"12345");
    let meta = case.fs.metadata(&path).unwrap();
    assert_eq!(meta.kind, Kind::File);
    assert_eq!(meta.len, 5);
    assert!(meta.modified >= before, "{:?} < {before:?}", meta.modified);
}

fn test_a_file_with_any_execute_bit_is_executable(case: &Case) {
    if !has_modes(case) {
        return;
    }
    let path = case.write("tool", b"#!/bin/sh\n");
    assert!(!case.fs.metadata(&path).unwrap().executable);
    case.fs
        .set_permissions(&path, Mode::from_bits(0o610))
        .unwrap();
    assert!(case.fs.metadata(&path).unwrap().executable);
    let dir = case.dir("dir");
    assert!(!case.fs.metadata(&dir).unwrap().executable, "a directory");
}

fn test_exists_follows_links_and_a_dangling_one_is_absent(case: &Case) {
    let file = case.write("file", b"x");
    let gone = case.dir("gone");
    let link = case.at("link");
    case.fs.symlink_dir(&gone, &link).unwrap();
    assert!(case.fs.exists(&file).unwrap());
    assert!(case.fs.exists(&link).unwrap());
    case.fs.remove_dir_all(&gone).unwrap();
    assert!(!case.fs.exists(&link).unwrap(), "a dangling link");
    assert_eq!(case.fs.symlink_metadata(&link).unwrap().kind, Kind::Symlink);
}

// --- Decision: rename and permissions ---------------------------------------

fn test_rename_replaces_a_file_at_the_target(case: &Case) {
    let from = case.write("from", b"new");
    let to = case.write("to", b"old");
    case.fs.rename(&from, &to).unwrap();
    assert_eq!(case.fs.read(&to).unwrap(), b"new");
    assert!(!case.fs.exists(&from).unwrap());
}

fn test_rename_noreplace_moves_a_file_to_a_free_target(case: &Case) {
    let from = case.write("from", b"moved");
    let to = case.at("to");
    case.fs.rename_noreplace(&from, &to).unwrap();
    assert_eq!(case.fs.read(&to).unwrap(), b"moved");
    assert!(!case.fs.exists(&from).unwrap());
}

fn test_rename_noreplace_refuses_a_file_at_the_target_as_the_callers_path(case: &Case) {
    let from = case.write("from", b"new");
    let to = case.write("to", b"old");
    let e = case.fs.rename_noreplace(&from, &to).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::AlreadyExists, "{e}");
    assert_eq!(
        TargetExists::of(&e),
        Some(&TargetExists { path: to.clone() })
    );
    assert_eq!(case.fs.read(&from).unwrap(), b"new");
    assert_eq!(case.fs.read(&to).unwrap(), b"old");
}

fn test_rename_noreplace_of_what_is_missing_is_not_found(case: &Case) {
    let e = case
        .fs
        .rename_noreplace(&case.at("missing"), &case.at("to"))
        .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::NotFound, "{e}");
    assert_eq!(TargetExists::of(&e), None);
}

fn test_rename_noreplace_resolves_relative_paths_against_the_working_directory(case: &Case) {
    case.write("from", b"moved");
    case.fs
        .rename_noreplace(Path::new("from"), Path::new("to"))
        .unwrap();
    assert_eq!(case.fs.read(&case.at("to")).unwrap(), b"moved");
}

fn test_sync_dir_syncs_a_directory_and_a_missing_one_is_not_found_on_unix(case: &Case) {
    let dir = case.dir("sub");
    case.fs.sync_dir(&dir).unwrap();
    let missing = case.fs.sync_dir(&case.at("missing"));
    if case.fs.os().flavor() == Flavor::Unix {
        assert_eq!(missing.unwrap_err().kind(), ErrorKind::NotFound);
    }
}

fn test_sync_dir_resolves_a_relative_path_against_the_working_directory(case: &Case) {
    case.dir("sub");
    case.fs.sync_dir(Path::new("sub")).unwrap();
    let missing = case.fs.sync_dir(Path::new("missing"));
    if case.fs.os().flavor() == Flavor::Unix {
        assert_eq!(missing.unwrap_err().kind(), ErrorKind::NotFound);
    }
}

fn test_set_permissions_sets_the_mode_exactly(case: &Case) {
    if !has_modes(case) {
        return;
    }
    let path = case.write("file", b"x");
    case.fs
        .set_permissions(&path, Mode::from_bits(0o666))
        .unwrap();
    assert_eq!(
        case.fs.metadata(&path).unwrap().mode,
        Some(Mode::from_bits(0o666))
    );
}

// --- Decision: links read back as made; canonical paths are the caller's --

// Unix only: a Windows directory link is a junction, which names its
// target absolutely.
fn test_a_relative_link_reads_back_as_written_and_is_followed(case: &Case) {
    if case.fs.os().flavor() != Flavor::Unix {
        return;
    }
    case.dir("real");
    let link = case.at("alias");
    case.fs.symlink_dir(Path::new("real"), &link).unwrap();
    assert_eq!(case.fs.read_link(&link).unwrap(), Path::new("real"));
    case.write("alias/through", b"via the link");
    assert_eq!(
        case.fs.read(&case.at("real/through")).unwrap(),
        b"via the link"
    );
}

fn test_an_absolute_link_reads_back_as_written_and_is_followed(case: &Case) {
    let real = case.dir("real");
    let link = case.at("alias");
    case.fs.symlink_dir(&real, &link).unwrap();
    assert_eq!(case.fs.read_link(&link).unwrap(), real);
    case.write("alias/through", b"via the link");
    assert_eq!(
        case.fs.read(&case.at("real/through")).unwrap(),
        b"via the link"
    );
}

fn test_read_link_of_what_is_not_a_link_is_invalid_input(case: &Case) {
    let file = case.write("plain", b"x");
    let e = case.fs.read_link(&file).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::InvalidInput);
}

fn test_canonicalize_resolves_links_and_dots_into_the_callers_path(case: &Case) {
    let real = case.dir("real");
    case.write("real/file", b"x");
    case.fs.symlink_dir(&real, &case.at("alias")).unwrap();
    let spelled = case.at("alias").join(".").join("file");
    assert_eq!(
        case.fs.canonicalize(&spelled).unwrap(),
        case.at("real/file")
    );
}

// --- Decision: temporary directories are caller-visible and fresh ----------

fn test_a_temporary_directory_is_new_empty_and_in_its_parent(case: &Case) {
    let one = case.fs.make_temp_dir(&case.base, ".stage-").unwrap();
    let two = case.fs.make_temp_dir(&case.base, ".stage-").unwrap();
    assert_ne!(one, two);
    for dir in [&one, &two] {
        assert_eq!(dir.parent(), Some(case.base.as_path()));
        let name = dir.file_name().unwrap().to_string_lossy();
        assert!(name.starts_with(".stage-"), "{name}");
        assert!(case.fs.read_dir(dir).unwrap().is_empty());
    }
}

// --- Decision: real_path is where std (SQLite, flock) finds the same file --

fn test_real_path_is_where_std_finds_what_the_seam_wrote(case: &Case) {
    // Reaches past the seam on purpose: what real_path is for.
    let path = case.write("state.db", b"sqlite would open this");
    let real = case.fs.real_path(&path).unwrap();
    assert!(real.is_absolute());
    assert_eq!(std::fs::read(&real).unwrap(), b"sqlite would open this");
    // A path not there yet is still named (SQLite creates its database).
    let later = case.fs.real_path(&case.at("later.db")).unwrap();
    std::fs::write(&later, b"made by std").unwrap();
    assert_eq!(case.fs.read(&case.at("later.db")).unwrap(), b"made by std");
}

// --- Decision: same_file tells a handle's file from what is at a path now --

fn test_same_file_is_true_for_the_file_at_the_path(case: &Case) {
    let path = case.write("lock", b"x");
    let other = case.write("other", b"x");
    let file = case.fs.open(&path, Open::Read).unwrap();
    assert!(case.fs.same_file(&file, &path).unwrap());
    assert!(!case.fs.same_file(&file, &other).unwrap());
}

fn test_same_file_is_false_once_a_rename_replaces_the_file(case: &Case) {
    let path = case.write("lock", b"old");
    let file = case.fs.open(&path, Open::Read).unwrap();
    let next = case.write("lock.next", b"new");
    case.fs.rename(&next, &path).unwrap();
    assert!(!case.fs.same_file(&file, &path).unwrap());
}

fn test_same_file_with_nothing_at_the_path_is_not_found(case: &Case) {
    let path = case.write("lock", b"x");
    let file = case.fs.open(&path, Open::Read).unwrap();
    case.fs.remove_file(&path).unwrap();
    let e = case.fs.same_file(&file, &path).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::NotFound);
}

contract!(
    test_a_relative_path_resolves_against_the_working_directory,
    test_what_is_missing_is_not_found,
    test_text_that_is_not_utf8_is_invalid_data,
    test_an_atomic_write_replaces_the_file_whole_and_leaves_no_temporary,
    test_an_atomic_write_does_not_make_its_directory,
    test_create_new_refuses_a_file_already_there,
    test_create_opens_a_file_there_without_truncating_it,
    test_append_writes_after_what_is_there_and_creates_what_is_not,
    test_read_opens_an_existing_file_for_reading_only,
    test_create_dir_all_makes_the_parents_and_may_be_repeated,
    test_a_directory_lists_sorted_by_name_each_entry_as_itself,
    test_remove_dir_all_removes_a_link_and_not_what_it_links_to,
    test_remove_file_removes_a_link_and_not_what_it_links_to,
    test_metadata_follows_a_link_and_symlink_metadata_does_not,
    test_metadata_reports_the_length_and_when_it_was_modified,
    test_exists_follows_links_and_a_dangling_one_is_absent,
    test_rename_replaces_a_file_at_the_target,
    test_rename_noreplace_moves_a_file_to_a_free_target,
    test_rename_noreplace_refuses_a_file_at_the_target_as_the_callers_path,
    test_rename_noreplace_of_what_is_missing_is_not_found,
    test_rename_noreplace_resolves_relative_paths_against_the_working_directory,
    test_sync_dir_syncs_a_directory_and_a_missing_one_is_not_found_on_unix,
    test_sync_dir_resolves_a_relative_path_against_the_working_directory,
    test_an_absolute_link_reads_back_as_written_and_is_followed,
    test_read_link_of_what_is_not_a_link_is_invalid_input,
    test_canonicalize_resolves_links_and_dots_into_the_callers_path,
    test_a_temporary_directory_is_new_empty_and_in_its_parent,
    test_real_path_is_where_std_finds_what_the_seam_wrote,
    test_same_file_is_true_for_the_file_at_the_path,
    test_same_file_is_false_once_a_rename_replaces_the_file,
    test_same_file_with_nothing_at_the_path_is_not_found,
);

contract!(
    test_an_exact_mode_is_applied_past_the_umask,
    test_keep_or_keeps_an_existing_files_mode_and_gives_a_new_one_its_own,
    test_keep_or_refuses_a_file_whose_mode_it_cannot_read,
    test_a_created_file_gets_its_mode,
    test_a_private_directory_is_owner_only,
    test_a_file_with_any_execute_bit_is_executable,
    test_set_permissions_sets_the_mode_exactly,
    test_a_relative_path_sets_permissions_in_the_working_directory,
    test_a_relative_link_reads_back_as_written_and_is_followed,
);
