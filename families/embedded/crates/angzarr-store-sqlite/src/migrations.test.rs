//! The adapter's own parts of migrating: sqlx's ledger's time read back,
//! a backup's name, and a backup that fails leaving nothing.

use std::time::{Duration, UNIX_EPOCH};

use super::{backup_path, timestamp, utc_stamp, vacuum_into};
use crate::test_database::database;

#[test]
fn sqlites_timestamp_reads_as_utc_seconds() {
    assert_eq!(timestamp("1970-01-01 00:00:00").seconds, 0);
    assert_eq!(timestamp("2026-10-31 21:45:07").seconds, 1_793_483_107);
    assert_eq!(timestamp("2000-02-29 12:00:00").seconds, 951_825_600);
    assert_eq!(timestamp("1969-12-31 23:59:59").seconds, -1);
    assert_eq!(
        timestamp("not a time").seconds,
        0,
        "anything else is the epoch"
    );
}

#[test]
fn a_backups_name_is_its_level_and_its_utc_time() {
    let at = UNIX_EPOCH + Duration::from_millis(1_793_483_107_042);
    assert_eq!(utc_stamp(at), "20261031T214507.042Z");
    assert_eq!(
        utc_stamp(UNIX_EPOCH + Duration::from_secs(951_825_600)),
        "20000229T120000.000Z"
    );
    assert_eq!(utc_stamp(UNIX_EPOCH), "19700101T000000.000Z");
    assert_eq!(
        backup_path(std::path::Path::new("/p/state.db"), 4, at),
        std::path::Path::new("/p/state.db.pre-4-20261031T214507.042Z")
    );
}

/// A copy that fails part way, here because the source is no database,
/// leaves nothing at the destination.
#[tokio::test]
async fn a_backup_that_fails_leaves_nothing_behind() {
    use sqlx::{ConnectOptions, Connection};
    let source = database();
    std::fs::write(&source, vec![0x5Au8; 8192]).unwrap();
    let dest = source.with_file_name("state.db.backup");
    let mut conn = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&source)
        .read_only(true)
        .connect()
        .await
        .unwrap();
    let copied = vacuum_into(&mut conn, &dest).await;
    conn.close().await.unwrap();
    assert!(copied.is_err(), "{copied:?}");
    assert!(!dest.exists(), "nothing left at the destination");
}

/// Days and dates convert both ways, every day of six centuries, the
/// ends of four-year, hundred-year and four-hundred-year cycles included.
#[test]
fn a_day_and_its_date_convert_both_ways() {
    use super::{civil_from_days, days_from_civil};
    for days in days_from_civil(1900, 1, 1)..days_from_civil(2500, 1, 1) {
        let (y, m, d) = civil_from_days(days);
        assert_eq!(days_from_civil(y, m, d), days, "{y}-{m}-{d}");
    }
    assert_eq!(civil_from_days(days_from_civil(2400, 2, 29)), (2400, 2, 29));
    assert_eq!(civil_from_days(days_from_civil(2100, 3, 1)), (2100, 3, 1));
    assert_eq!(civil_from_days(days_from_civil(2004, 2, 29)), (2004, 2, 29));
    assert_eq!(civil_from_days(0), (1970, 1, 1));
}

/// A run to a version below the backfill's migration runs no backfill,
/// whose table is not there yet.
#[tokio::test]
async fn a_run_below_the_backfills_migration_runs_no_backfill() {
    use sqlx::ConnectOptions;
    use std::str::FromStr;
    let mut conn = sqlx::sqlite::SqliteConnectOptions::from_str("sqlite::memory:")
        .unwrap()
        .connect()
        .await
        .unwrap();
    let applied = super::run_to(&mut conn, None, &crate::sqlite::MIGRATOR, 2)
        .await
        .unwrap();
    let versions: Vec<u64> = applied.iter().map(|r| r.version).collect();
    assert_eq!(versions, [1, 2]);
}
