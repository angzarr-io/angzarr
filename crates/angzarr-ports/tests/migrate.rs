//! `migrate::info` and `migrate::validate`: a build's migration set laid
//! over a store's ledger, as `flyway info` and sqlx's migrator judge it
//! (`src/migrate.rs`). RED while the port awaits the owner's review
//! (2026-10-31): `#[ignore]`d, `cargo test --all-features --test migrate
//! -- --ignored` shows them.

use std::time::Duration;

use angzarr_ports::migrate::{info, validate};
use angzarr_ports::{AppliedMigration, Migration, MigrationStatus, StoreError};
use prost_types::Timestamp;

/// Migration `version` of the build: checksum `[version]`.
fn migration(version: u64) -> Migration {
    Migration {
        version,
        description: format!("step {version}"),
        checksum: vec![version as u8],
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
#[ignore = "red: the Migrations port awaits the owner's review (2026-10-31)"]
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
#[ignore = "red: the Migrations port awaits the owner's review (2026-10-31)"]
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
#[ignore = "red: the Migrations port awaits the owner's review (2026-10-31)"]
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
#[ignore = "red: the Migrations port awaits the owner's review (2026-10-31)"]
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
#[ignore = "red: the Migrations port awaits the owner's review (2026-10-31)"]
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
#[ignore = "red: the Migrations port awaits the owner's review (2026-10-31)"]
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
#[ignore = "red: the Migrations port awaits the owner's review (2026-10-31)"]
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
#[ignore = "red: the Migrations port awaits the owner's review (2026-10-31)"]
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
