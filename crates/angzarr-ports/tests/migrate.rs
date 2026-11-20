//! `migrate::info` and `migrate::validate`: a build's migration set laid
//! over a store's ledger, as `flyway info` and sqlx's migrator judge it
//! (`src/migrate.rs`), and the plans of a run and an undo to a target.

use std::time::Duration;

use angzarr_ports::migrate::{info, latest, plan_run, plan_undo, validate};
use angzarr_ports::{AppliedMigration, Migration, MigrationStatus, StoreError};
use prost_types::Timestamp;

/// Migration `version` of the build: checksum `[version]`, reversible
/// when its version is even.
fn migration(version: u64) -> Migration {
    Migration {
        version,
        description: format!("step {version}"),
        checksum: vec![version as u8],
        reversible: version.is_multiple_of(2),
    }
}

/// The build's set: `versions`.
fn build(versions: &[u64]) -> Vec<Migration> {
    versions.iter().copied().map(migration).collect()
}

/// Migration `version` applied as the build has it, a success.
fn applied(version: u64) -> AppliedMigration {
    AppliedMigration {
        version,
        description: format!("step {version}"),
        checksum: vec![version as u8],
        success: true,
        installed_on: Timestamp {
            seconds: version as i64,
            nanos: 0,
        },
        execution_time: Duration::from_millis(version),
    }
}

/// `info`'s lines as (version, status).
fn statuses(build: &[Migration], ledger: &[AppliedMigration]) -> Vec<(u64, MigrationStatus)> {
    info(build, ledger)
        .into_iter()
        .map(|line| (line.version, line.status))
        .collect()
}

use MigrationStatus::*;

#[test]
fn a_current_ledger_is_every_migration_applied_and_validates() {
    let set = build(&[1, 2, 3]);
    let ledger = vec![applied(1), applied(2), applied(3)];
    assert_eq!(
        statuses(&set, &ledger),
        [(1, Applied), (2, Applied), (3, Applied)]
    );
    assert_eq!(validate(&set, &ledger), Ok(()));
}

#[test]
fn what_the_ledger_lacks_is_pending_and_no_refusal() {
    let set = build(&[1, 2, 3]);
    let ledger = vec![applied(1)];
    assert_eq!(
        statuses(&set, &ledger),
        [(1, Applied), (2, Pending), (3, Pending)]
    );
    assert_eq!(validate(&set, &ledger), Ok(()));
    assert_eq!(
        statuses(&set, &[]),
        [(1, Pending), (2, Pending), (3, Pending)],
        "a store never migrated"
    );
}

#[test]
fn a_row_not_a_success_is_failed_and_refused_dirty() {
    let set = build(&[1, 2, 3]);
    let mut dirty = applied(2);
    dirty.success = false;
    let ledger = vec![applied(1), dirty];
    assert_eq!(
        statuses(&set, &ledger),
        [(1, Applied), (2, Failed), (3, Pending)]
    );
    assert_eq!(
        validate(&set, &ledger),
        Err(StoreError::MigrationFailed { version: 2 })
    );
}

#[test]
fn a_checksum_not_the_builds_is_changed_and_refused() {
    let set = build(&[1, 2]);
    let mut changed = applied(1);
    changed.checksum = vec![0xFF];
    let ledger = vec![changed, applied(2)];
    assert_eq!(statuses(&set, &ledger), [(1, Changed), (2, Applied)]);
    assert_eq!(
        validate(&set, &ledger),
        Err(StoreError::MigrationChanged { version: 1 })
    );
}

#[test]
fn an_unknown_version_beyond_the_builds_highest_is_newer_and_refused() {
    let set = build(&[1, 2]);
    let ledger = vec![applied(1), applied(2), applied(9)];
    let lines = info(&set, &ledger);
    assert_eq!(
        lines
            .iter()
            .map(|l| (l.version, l.status))
            .collect::<Vec<_>>(),
        [(1, Applied), (2, Applied), (9, Newer)]
    );
    assert_eq!(
        lines[2].description, "step 9",
        "a version the build lacks takes the ledger's description"
    );
    assert_eq!(lines[2].applied, Some(applied(9)), "with its row");
    assert_eq!(lines[0].applied, Some(applied(1)));
    assert_eq!(
        validate(&set, &ledger),
        Err(StoreError::NewerThanBuild {
            applied: 9,
            build: 2
        })
    );
}

#[test]
fn an_unknown_version_below_the_builds_highest_is_missing_and_refused() {
    let set = build(&[1, 3]);
    let ledger = vec![applied(1), applied(2), applied(3)];
    assert_eq!(
        statuses(&set, &ledger),
        [(1, Applied), (2, Missing), (3, Applied)]
    );
    assert_eq!(
        validate(&set, &ledger),
        Err(StoreError::MigrationMissing { version: 2 })
    );
}

/// Failed, then newer, then changed, then missing; and within one, the
/// lowest version.
#[test]
fn the_refusals_take_precedence_failed_newer_changed_missing() {
    let set = build(&[1, 3, 4]);
    let mut dirty = applied(4);
    dirty.success = false;
    let mut changed = applied(3);
    changed.checksum = vec![0xFF];
    let all = vec![applied(1), applied(2), changed.clone(), dirty, applied(9)];
    assert_eq!(
        validate(&set, &all),
        Err(StoreError::MigrationFailed { version: 4 })
    );
    let no_dirty = vec![applied(1), applied(2), changed.clone(), applied(9)];
    assert_eq!(
        validate(&set, &no_dirty),
        Err(StoreError::NewerThanBuild {
            applied: 9,
            build: 4
        })
    );
    let changed_and_missing = vec![applied(1), applied(2), changed];
    assert_eq!(
        validate(&set, &changed_and_missing),
        Err(StoreError::MigrationChanged { version: 3 })
    );
    let mut first = applied(1);
    first.checksum = vec![0xFE];
    let mut third = applied(3);
    third.checksum = vec![0xFF];
    assert_eq!(
        validate(&set, &[first, third]),
        Err(StoreError::MigrationChanged { version: 1 }),
        "the lowest changed version"
    );
}

/// In version order whatever the ledger's order; a backend with no schema
/// has no lines and validates.
#[test]
fn info_is_in_version_order_and_empty_for_no_schema() {
    let set = build(&[1, 2, 3]);
    let ledger = vec![applied(3), applied(1)];
    assert_eq!(
        statuses(&set, &ledger),
        [(1, Applied), (2, Pending), (3, Applied)]
    );
    assert_eq!(info(&[], &[]), vec![]);
    assert_eq!(validate(&[], &[]), Ok(()));
}

/// A run to a target plans what is pending at or below it, in order; to 0
/// nothing; and refuses a target not in the build, or what validate does.
#[test]
fn a_run_plans_what_is_pending_up_to_its_target() {
    let set = build(&[1, 2, 3, 4]);
    let ledger = vec![applied(1)];
    assert_eq!(plan_run(&set, &ledger, 3), Ok(vec![2, 3]));
    assert_eq!(plan_run(&set, &ledger, 4), Ok(vec![2, 3, 4]));
    assert_eq!(plan_run(&set, &ledger, 1), Ok(vec![]));
    assert_eq!(plan_run(&set, &ledger, 0), Ok(vec![]));
    assert_eq!(
        plan_run(&set, &[applied(1), applied(3)], 4),
        Ok(vec![2, 4]),
        "a gap below an applied one is pending too"
    );
    assert_eq!(
        plan_run(&set, &ledger, 5),
        Err(StoreError::MigrationUnknown { version: 5 })
    );
    let sparse = build(&[1, 3]);
    assert_eq!(
        plan_run(&sparse, &[], 2),
        Err(StoreError::MigrationUnknown { version: 2 }),
        "a version between the build's is not one of them"
    );
    let mut dirty = applied(1);
    dirty.success = false;
    assert_eq!(
        plan_run(&set, &[dirty], 4),
        Err(StoreError::MigrationFailed { version: 1 }),
        "validated first"
    );
    assert_eq!(latest(&set), 4);
    assert_eq!(latest(&[]), 0);
}

/// An undo to a target plans what is applied above it, highest first, and
/// refuses the whole undo past one with no down, naming the highest.
#[test]
fn an_undo_plans_what_is_applied_above_its_target_highest_first() {
    let set = build(&[1, 2, 3, 4]);
    let all = vec![applied(1), applied(2), applied(3), applied(4)];
    assert_eq!(plan_undo(&set, &all, 3), Ok(vec![4]));
    assert_eq!(plan_undo(&set, &all, 4), Ok(vec![]));
    assert_eq!(
        plan_undo(&set, &all, 1),
        Err(StoreError::MigrationIrreversible { version: 3 }),
        "3 has no down"
    );
    assert_eq!(
        plan_undo(&set, &all, 0),
        Err(StoreError::MigrationIrreversible { version: 3 }),
        "the highest with no down"
    );
    let reversible = build(&[2, 4, 6]);
    let applied_all = vec![applied(2), applied(4), applied(6)];
    assert_eq!(plan_undo(&reversible, &applied_all, 0), Ok(vec![6, 4, 2]));
    assert_eq!(
        plan_undo(&reversible, &[applied(2), applied(6)], 2),
        Ok(vec![6]),
        "only what is applied"
    );
    assert_eq!(
        plan_undo(&set, &all, 7),
        Err(StoreError::MigrationUnknown { version: 7 })
    );
    let mut changed = applied(4);
    changed.checksum = vec![0xFF];
    assert_eq!(
        plan_undo(&set, &[applied(1), applied(2), applied(3), changed], 3),
        Err(StoreError::MigrationChanged { version: 4 }),
        "validated first"
    );
}
