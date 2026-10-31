//! The shared `--write-upgrades` helper: an older file is written back at
//! the current version, its old bytes kept as `<file>.bak` first when asked,
//! or the upgraded text is handed back to print; a current file is never
//! touched. Go schemaver's WriteBack tests, ported.

use std::collections::BTreeMap;

use angzarr_diagnostic::{Diagnostic, Report};

use super::*;
use crate::versioned::{rename_key, Loader, MigrationError, Migrations};
use crate::Value;

/// Files in memory, every call recorded; a path in `refuse` fails to write.
#[derive(Default)]
struct Memory {
    files: BTreeMap<String, String>,
    refuse: Vec<String>,
    calls: Vec<String>,
}

impl Memory {
    fn with(path: &str, text: &str) -> Memory {
        let mut m = Memory::default();
        m.files.insert(path.to_string(), text.to_string());
        m
    }
}

impl Files for Memory {
    fn read(&mut self, path: &str) -> Result<String, Report> {
        self.calls.push(format!("read {path}"));
        self.files
            .get(path)
            .cloned()
            .ok_or_else(|| Report::new("memory-not-found").arg("errno", 2u32))
    }

    fn write(&mut self, path: &str, text: &str) -> Result<(), Report> {
        self.calls.push(format!("write {path}"));
        if self.refuse.iter().any(|p| p == path) {
            return Err(Report::new("memory-read-only").arg("errno", 30u32));
        }
        self.files.insert(path.to_string(), text.to_string());
        Ok(())
    }
}

fn step(doc: &mut Value) -> Result<(), MigrationError> {
    rename_key(doc, "hp", "health")
}

fn loader() -> Loader {
    Migrations::new("toy", 2).step(1, step).build().unwrap()
}

const OLD: &str = "# a toy\nschema_version: 1\nhp: 3   # hit points\n";
const NEW: &str = "# a toy\nschema_version: 2\nhealth: 3\n";
const PATH: &str = "/cfg/toy.yaml";

#[test]
fn an_older_file_is_backed_up_then_replaced() {
    let mut fs = Memory::with(PATH, OLD);
    let done = write_back(&mut fs, &loader(), PATH, Options::default()).unwrap();
    assert_eq!(
        done,
        Outcome::Written {
            backup: Some(format!("{PATH}{BACKUP_SUFFIX}")),
            comments_kept: true
        }
    );
    assert_eq!(fs.files[PATH], NEW);
    assert_eq!(fs.files["/cfg/toy.yaml.bak"], OLD);
    // The backup first, then the file.
    assert_eq!(
        fs.calls,
        [
            "read /cfg/toy.yaml",
            "write /cfg/toy.yaml.bak",
            "write /cfg/toy.yaml"
        ]
    );
    // Now current: nothing more to do, nothing written.
    fs.calls.clear();
    assert_eq!(
        write_back(&mut fs, &loader(), PATH, Options::default()).unwrap(),
        Outcome::Current
    );
    assert_eq!(fs.calls, ["read /cfg/toy.yaml"]);
}

#[test]
fn without_a_backup_nothing_is_left_beside_the_file() {
    let mut fs = Memory::with(PATH, OLD);
    let options = Options {
        backup: Backup::None,
        ..Options::default()
    };
    assert_eq!(
        write_back(&mut fs, &loader(), PATH, options).unwrap(),
        Outcome::Written {
            backup: None,
            comments_kept: true
        }
    );
    assert_eq!(fs.files[PATH], NEW);
    assert_eq!(fs.files.len(), 1, "{:?}", fs.files.keys());
}

#[test]
fn printing_writes_nothing_and_hands_back_the_text() {
    let mut fs = Memory::with(PATH, OLD);
    let options = Options {
        action: Action::Print,
        ..Options::default()
    };
    assert_eq!(
        write_back(&mut fs, &loader(), PATH, options).unwrap(),
        Outcome::Printed {
            text: NEW.to_string(),
            comments_kept: true
        }
    );
    assert_eq!(fs.files[PATH], OLD);
    assert_eq!(fs.calls, ["read /cfg/toy.yaml"]);
}

#[test]
fn when_the_backup_cannot_be_written_the_file_is_not_touched() {
    let mut fs = Memory::with(PATH, OLD);
    fs.refuse.push(format!("{PATH}{BACKUP_SUFFIX}"));
    let e = write_back(&mut fs, &loader(), PATH, Options::default()).unwrap_err();
    assert_eq!(
        e,
        WriteBackError::Backup {
            path: "/cfg/toy.yaml.bak".into(),
            cause: Report::new("memory-read-only").arg("errno", 30u32)
        }
    );
    assert_eq!(
        e.to_string(),
        "angzarr-yaml-write-back-backup path=/cfg/toy.yaml.bak cause=memory-read-only cause.errno=30"
    );
    assert_eq!(fs.files[PATH], OLD);
    assert_eq!(fs.calls, ["read /cfg/toy.yaml", "write /cfg/toy.yaml.bak"]);
}

#[test]
fn every_other_failure_is_coded_and_names_its_file() {
    // Unreadable.
    let mut fs = Memory::default();
    let e = write_back(&mut fs, &loader(), PATH, Options::default()).unwrap_err();
    assert_eq!(e.code(), "angzarr-yaml-write-back-read");
    assert_eq!(
        e.to_string(),
        "angzarr-yaml-write-back-read path=/cfg/toy.yaml cause=memory-not-found cause.errno=2"
    );
    // Not writable: the backup was written, the file was not replaced.
    let mut fs = Memory::with(PATH, OLD);
    fs.refuse.push(PATH.to_string());
    let e = write_back(&mut fs, &loader(), PATH, Options::default()).unwrap_err();
    assert_eq!(
        e,
        WriteBackError::Write {
            path: PATH.into(),
            cause: Report::new("memory-read-only").arg("errno", 30u32)
        }
    );
    assert_eq!(e.code(), "angzarr-yaml-write-back-write");
    assert_eq!(fs.files[PATH], OLD);
    // A file that does not load is the loader's error, untouched.
    let mut fs = Memory::with(PATH, "schema_version: 3\n");
    let e = write_back(&mut fs, &loader(), PATH, Options::default()).unwrap_err();
    assert!(matches!(&e, WriteBackError::Load(l) if l.source.as_deref() == Some(PATH)));
    assert_eq!(e.code(), "angzarr-yaml-load-failed-at");
    assert_eq!(
        e.to_string(),
        "angzarr-yaml-load-failed-at kind=toy source=/cfg/toy.yaml \
         problem=angzarr-yaml-too-new problem.found=3 problem.current=2"
    );
    assert_eq!(fs.files[PATH], "schema_version: 3\n");
}

#[test]
fn the_flag_and_the_suffix_are_the_familys() {
    assert_eq!(WRITE_UPGRADES_FLAG, "write-upgrades");
    assert_eq!(BACKUP_SUFFIX, ".bak");
    assert_eq!(
        Options::default(),
        Options {
            backup: Backup::Keep,
            action: Action::Write,
            flow_depth: 2
        }
    );
}
