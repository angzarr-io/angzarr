//! Unit tests for all-or-nothing batch writes.
//!
//! A failed `add` must not leave a partial command in the store, so the
//! undo order, the units undone, and the error surfaced are all pinned.

use std::sync::Mutex;

use async_trait::async_trait;

use super::*;
use crate::storage::StorageError;

/// Records every write/undo; fails the write of `fail_on` and the undo of
/// `fail_undo_on`.
#[derive(Default)]
struct Recorder {
    log: Mutex<Vec<String>>,
    fail_on: Option<u32>,
    fail_undo_on: Option<u32>,
}

#[async_trait]
impl UnitWriter for Recorder {
    type Unit = u32;

    async fn write(&self, unit: &u32) -> Result<()> {
        self.log.lock().unwrap().push(format!("write {unit}"));
        if self.fail_on == Some(*unit) {
            return Err(StorageError::SequenceConflict {
                expected: *unit,
                actual: *unit,
            });
        }
        Ok(())
    }

    async fn undo(&self, unit: &u32) -> Result<()> {
        self.log.lock().unwrap().push(format!("undo {unit}"));
        if self.fail_undo_on == Some(*unit) {
            return Err(StorageError::Backend("undo failed".to_string()));
        }
        Ok(())
    }
}

fn log(recorder: &Recorder) -> Vec<String> {
    recorder.log.lock().unwrap().clone()
}

/// Every unit is written in order and nothing is undone on success.
#[tokio::test]
async fn writes_all_units_in_order() {
    let recorder = Recorder::default();
    write_all_or_undo(&recorder, &[1, 2, 3]).await.unwrap();
    assert_eq!(log(&recorder), vec!["write 1", "write 2", "write 3"]);
}

/// A failure undoes the already-written units, newest first, stops writing,
/// and surfaces the write's own error.
#[tokio::test]
async fn failure_undoes_written_units_newest_first() {
    let recorder = Recorder {
        fail_on: Some(3),
        ..Default::default()
    };
    let error = write_all_or_undo(&recorder, &[1, 2, 3, 4])
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        StorageError::SequenceConflict {
            expected: 3,
            actual: 3
        }
    ));
    assert_eq!(
        log(&recorder),
        vec!["write 1", "write 2", "write 3", "undo 2", "undo 1"]
    );
}

/// A failure on the first unit has nothing to undo.
#[tokio::test]
async fn failure_on_first_unit_undoes_nothing() {
    let recorder = Recorder {
        fail_on: Some(1),
        ..Default::default()
    };
    assert!(write_all_or_undo(&recorder, &[1, 2]).await.is_err());
    assert_eq!(log(&recorder), vec!["write 1"]);
}

/// A failing undo does not stop the remaining undos and does not replace
/// the original error.
#[tokio::test]
async fn failing_undo_continues_and_keeps_original_error() {
    let recorder = Recorder {
        fail_on: Some(3),
        fail_undo_on: Some(2),
        ..Default::default()
    };
    let error = write_all_or_undo(&recorder, &[1, 2, 3]).await.unwrap_err();
    assert!(matches!(error, StorageError::SequenceConflict { .. }));
    assert_eq!(
        log(&recorder),
        vec!["write 1", "write 2", "write 3", "undo 2", "undo 1"]
    );
}

/// An empty batch writes nothing.
#[tokio::test]
async fn empty_batch_is_a_no_op() {
    let recorder = Recorder::default();
    write_all_or_undo(&recorder, &[]).await.unwrap();
    assert!(log(&recorder).is_empty());
}
