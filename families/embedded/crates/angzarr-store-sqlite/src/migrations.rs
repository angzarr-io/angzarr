//! angzarr-ports' [`Migrations`] and [`Backup`] on SQLite, through sqlx's
//! migrator: the build's set is the [`Migrator`]'s, a migration is applied
//! with `Migrate::apply` and undone with `Migrate::revert` (each recording
//! itself in sqlx's ledger, `_sqlx_migrations`), and the port's own
//! `validate`, `plan_run` and `plan_undo` judge what to do, so a newer
//! store is told from a missing migration (sqlx's one `VersionMissing`).
//! Two things are this adapter's own SQL: reading the ledger whole (sqlx's
//! `list_applied_migrations` gives a version and a checksum alone), and the
//! backup, `VACUUM INTO`.
//!
//! Every open migrates (the owner, 2026-10-31): to write or to read, a
//! store with migrations pending is backed up beside it, then migrated,
//! under `BEGIN IMMEDIATE`, so migrators take turns and each after the
//! first finds nothing to do. A store refused (newer than the build, a
//! migration failed or changed) is refused before anything is changed.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use angzarr_ports::migrate::{latest, plan_run, plan_undo};
use angzarr_ports::{AppliedMigration, Backup, Migration, Migrations, StoreError};
use prost_types::Timestamp;
use sqlx::migrate::{Migrate, Migrator};
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use sqlx::{ConnectOptions, Connection};

use crate::error::{backup_uncreatable, unavailable};
use crate::sqlite::{backfill, empty, into_wal, opening, SqliteStore, BUSY_TIMEOUT};

/// The migration that adds what the backfill fills: once it is applied,
/// a migration runs the backfill (which runs once).
const BACKFILLED_FROM: u64 = 3;

/// `migrator`'s set, as the port has it: its up migrations, each
/// reversible when the migrator has its down.
pub(crate) fn build(migrator: &Migrator) -> Vec<Migration> {
    migrator
        .iter()
        .filter(|m| m.migration_type.is_up_migration())
        .map(|m| Migration {
            version: m.version as u64,
            description: m.description.to_string(),
            checksum: m.checksum.to_vec(),
            reversible: down(migrator, m.version as u64).is_some(),
        })
        .collect()
}

/// `migrator`'s up migration of `version`.
fn up(migrator: &Migrator, version: u64) -> Option<&sqlx::migrate::Migration> {
    migrator
        .iter()
        .find(|m| m.version as u64 == version && m.migration_type.is_up_migration())
}

/// `migrator`'s down migration of `version`.
fn down(migrator: &Migrator, version: u64) -> Option<&sqlx::migrate::Migration> {
    migrator
        .iter()
        .find(|m| m.version as u64 == version && m.migration_type.is_down_migration())
}

/// One row of sqlx's ledger as stored: version, description, installed
/// on (SQLite's `CURRENT_TIMESTAMP`, UTC), success, checksum, execution
/// time in nanoseconds.
type LedgerRow = (i64, String, String, bool, Vec<u8>, i64);

/// The ledger, `_sqlx_migrations`, whole and in version order: empty when
/// sqlx has made none. A read.
pub(crate) async fn ledger(
    conn: &mut SqliteConnection,
) -> Result<Vec<AppliedMigration>, StoreError> {
    let made: Option<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations'",
    )
    .fetch_optional(&mut *conn)
    .await
    .map_err(opening)?;
    if made.is_none() {
        return Ok(Vec::new());
    }
    let rows: Vec<LedgerRow> = sqlx::query_as(
        "SELECT version, description, installed_on, success, checksum, execution_time \
         FROM _sqlx_migrations ORDER BY version",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(opening)?;
    Ok(rows
        .into_iter()
        .map(
            |(version, description, installed_on, success, checksum, nanos)| AppliedMigration {
                version: version as u64,
                description,
                checksum,
                success,
                installed_on: timestamp(&installed_on),
                execution_time: Duration::from_nanos(nanos.max(0) as u64),
            },
        )
        .collect())
}

/// SQLite's `YYYY-MM-DD HH:MM:SS`, UTC, as a timestamp; the epoch for
/// anything else.
fn timestamp(text: &str) -> Timestamp {
    let field = |range: std::ops::Range<usize>| text.get(range).and_then(|s| s.parse::<i64>().ok());
    let parts = (
        field(0..4),
        field(5..7),
        field(8..10),
        field(11..13),
        field(14..16),
        field(17..19),
    );
    let seconds = match parts {
        (Some(y), Some(mo), Some(d), Some(h), Some(mi), Some(s)) => {
            days_from_civil(y, mo, d) * 86_400 + h * 3_600 + mi * 60 + s
        }
        _ => 0,
    };
    Timestamp { seconds, nanos: 0 }
}

/// Days since 1970-01-01 of the proleptic Gregorian date (Howard
/// Hinnant's `days_from_civil`).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The date of `days` since 1970-01-01 (Hinnant's `civil_from_days`).
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// `at` as a backup's name gives it: `YYYYMMDDTHHMMSS.mmmZ`, UTC, which
/// sorts as it happened.
pub(crate) fn utc_stamp(at: SystemTime) -> String {
    let since = at.duration_since(UNIX_EPOCH).unwrap_or_default();
    let seconds = since.as_secs() as i64;
    let (year, month, day) = civil_from_days(seconds.div_euclid(86_400));
    let of_day = seconds.rem_euclid(86_400);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}.{:03}Z",
        of_day / 3_600,
        of_day % 3_600 / 60,
        of_day % 60,
        since.subsec_millis()
    )
}

/// Where the backup of the store at `path`, at level `applied`, taken at
/// `at`, is written: beside it, `<name>.pre-<applied>-<UTC time>`.
pub(crate) fn backup_path(path: &Path, applied: u64, at: SystemTime) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_owned();
    name.push(format!(".pre-{applied}-{}", utc_stamp(at)));
    path.with_file_name(name)
}

/// Copy the database `conn` reads to `dest`, as one moment of it, by
/// `VACUUM INTO`, which runs on a read-only connection. `dest` is made
/// first, empty, with `create_new`, so something already there, empty or
/// not, refuses the backup at once and nothing is overwritten (`VACUUM
/// INTO` writes into an empty file as into a new one); a copy that fails
/// removes the file it made.
pub(crate) async fn vacuum_into(
    conn: &mut SqliteConnection,
    dest: &Path,
) -> Result<(), StoreError> {
    let Some(into) = dest.to_str() else {
        let error = std::io::Error::from(std::io::ErrorKind::InvalidFilename);
        return Err(backup_uncreatable(dest, &error));
    };
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dest)
    {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(StoreError::BackupExists {
                target: dest.display().to_string(),
            })
        }
        Err(error) => return Err(backup_uncreatable(dest, &error)),
    }
    if let Err(error) = sqlx::query("VACUUM INTO ?").bind(into).execute(conn).await {
        // The file is this call's own, made above: nothing else is removed.
        let _ = std::fs::remove_file(dest);
        return Err(opening(error));
    }
    Ok(())
}

/// A read-only connection to the database at `path`.
pub(crate) async fn reading(path: &Path) -> Result<SqliteConnection, StoreError> {
    SqliteConnectOptions::new()
        .filename(path)
        .busy_timeout(BUSY_TIMEOUT)
        .read_only(true)
        .connect()
        .await
        .map_err(opening)
}

/// A connection to write the database at `path`, made if `create` and
/// absent.
async fn writing(path: &Path, create: bool) -> Result<SqliteConnection, StoreError> {
    SqliteConnectOptions::new()
        .filename(path)
        .busy_timeout(BUSY_TIMEOUT)
        .create_if_missing(create)
        .connect()
        .await
        .map_err(opening)
}

/// Back the store at `path` up beside it, as it stands, through a
/// connection of its own: the caller holds the write lock on another,
/// with nothing written yet, so this reads the store as it was.
async fn back_up(path: &Path, applied: &[AppliedMigration]) -> Result<(), StoreError> {
    let level = applied.iter().map(|r| r.version).max().unwrap_or(0);
    let mut conn = reading(path).await?;
    let copied = vacuum_into(&mut conn, &backup_path(path, level, SystemTime::now())).await;
    conn.close().await.map_err(unavailable)?;
    copied
}

/// Run `migrator`'s pending migrations at or below `target` on `conn`,
/// under `BEGIN IMMEDIATE`: the ledger is read again under the lock and
/// planned by the port's `plan_run` (a refusal changes nothing), the store
/// at `path` (none for one in memory) backed up first if anything is to be
/// applied to what it holds, each migration applied by sqlx's
/// `Migrate::apply`, the backfill run once its columns exist. Answers the
/// rows added.
pub(crate) async fn run_to(
    conn: &mut SqliteConnection,
    path: Option<&Path>,
    migrator: &Migrator,
    target: u64,
) -> Result<Vec<AppliedMigration>, StoreError> {
    let build = build(migrator);
    let mut tx = conn
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(unavailable)?;
    let before = ledger(&mut tx).await?;
    let planned = plan_run(&build, &before, target)?;
    if planned.is_empty() {
        tx.commit().await.map_err(unavailable)?;
        return Ok(Vec::new());
    }
    if let Some(path) = path {
        // A new store has nothing to back up.
        if !empty(&mut tx).await? {
            back_up(path, &before).await?;
        }
    }
    tx.ensure_migrations_table().await.map_err(unavailable)?;
    for version in &planned {
        let migration = up(migrator, *version).expect("a planned version is the build's");
        tx.apply(migration).await.map_err(unavailable)?;
    }
    let after = ledger(&mut tx).await?;
    if after.iter().any(|r| r.version == BACKFILLED_FROM) {
        backfill(&mut tx).await?;
    }
    tx.commit().await.map_err(unavailable)?;
    Ok(after
        .into_iter()
        .filter(|r| planned.contains(&r.version))
        .collect())
}

/// Undo `migrator`'s applied migrations above `target` on `conn`, highest
/// first, under `BEGIN IMMEDIATE`, as the port's `plan_undo` plans them (a
/// refusal changes nothing), the store at `path` backed up first; each
/// reverted by sqlx's `Migrate::revert`, which drops its row. Answers the
/// rows dropped, highest first.
pub(crate) async fn undo_to(
    conn: &mut SqliteConnection,
    path: Option<&Path>,
    migrator: &Migrator,
    target: u64,
) -> Result<Vec<AppliedMigration>, StoreError> {
    let build = build(migrator);
    let mut tx = conn
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(unavailable)?;
    let before = ledger(&mut tx).await?;
    let planned = plan_undo(&build, &before, target)?;
    if planned.is_empty() {
        tx.commit().await.map_err(unavailable)?;
        return Ok(Vec::new());
    }
    if let Some(path) = path {
        back_up(path, &before).await?;
    }
    for version in &planned {
        let migration = down(migrator, *version).expect("a planned version has its down");
        tx.revert(migration).await.map_err(unavailable)?;
    }
    tx.commit().await.map_err(unavailable)?;
    Ok(planned
        .iter()
        .filter_map(|v| before.iter().find(|r| r.version == *v).cloned())
        .collect())
}

/// Whether the ledger has anything `migrator` would apply, after the
/// port's `validate`: read without a lock, so an open of a current store
/// takes none.
fn pending(migrator: &Migrator, ledger: &[AppliedMigration]) -> Result<bool, StoreError> {
    let build = build(migrator);
    Ok(!plan_run(&build, ledger, latest(&build))?.is_empty())
}

/// The database at `path`, made if `create` and absent, in WAL, and
/// migrated to `migrator`'s latest: refused, untouched, if the port's
/// `validate` refuses its ledger.
pub(crate) async fn open_writing(
    path: &Path,
    migrator: &'static Migrator,
    create: bool,
) -> Result<SqliteStore, StoreError> {
    let mut conn = writing(path, create).await?;
    // A read first, so the connection knows a file already in WAL is,
    // and moving it there is a no-op that waits on no writer; and so a
    // store refused is refused before anything is changed.
    let found = ledger(&mut conn).await?;
    let migrating = pending(migrator, &found)?;
    into_wal(&mut conn).await?;
    if migrating {
        run_to(&mut conn, Some(path), migrator, latest(&build(migrator))).await?;
    }
    Ok(SqliteStore::on(conn, Some(path.to_path_buf()), migrator))
}

/// The database at `path` opened read-only, or `None` if there is no file
/// there; migrated first, on a connection of its own, if it has
/// migrations pending: refused, untouched, if the port's `validate`
/// refuses its ledger.
pub(crate) async fn open_reading(
    path: &Path,
    migrator: &'static Migrator,
) -> Result<Option<SqliteStore>, StoreError> {
    if matches!(path.try_exists(), Ok(false)) {
        return Ok(None);
    }
    let mut conn = reading(path).await?;
    let found = ledger(&mut conn).await?;
    if pending(migrator, &found)? {
        conn.close().await.map_err(unavailable)?;
        let writer = open_writing(path, migrator, false).await?;
        writer.into_conn().close().await.map_err(unavailable)?;
        conn = reading(path).await?;
    }
    Ok(Some(SqliteStore::on(
        conn,
        Some(path.to_path_buf()),
        migrator,
    )))
}

impl Migrations for SqliteStore {
    fn migrations(&self) -> Vec<Migration> {
        build(self.migrator())
    }

    async fn applied(&self) -> Result<Vec<AppliedMigration>, StoreError> {
        ledger(&mut *self.connection().await).await
    }

    /// Planned first without a lock, so a run with nothing to do takes
    /// none (and runs on a store opened read-only); then on a connection
    /// of its own to write, for a store on a file, or on its own for one
    /// in memory.
    async fn run_to(&self, target: u64) -> Result<Vec<AppliedMigration>, StoreError> {
        let found = self.applied().await?;
        if plan_run(&self.migrations(), &found, target)?.is_empty() {
            return Ok(Vec::new());
        }
        match self.path() {
            Some(path) => {
                let mut conn = writing(path, false).await?;
                let ran = run_to(&mut conn, Some(path), self.migrator(), target).await;
                conn.close().await.map_err(unavailable)?;
                ran
            }
            None => run_to(&mut *self.connection().await, None, self.migrator(), target).await,
        }
    }

    /// As [`run_to`](Migrations::run_to), through [`undo_to`].
    async fn undo_to(&self, target: u64) -> Result<Vec<AppliedMigration>, StoreError> {
        let found = self.applied().await?;
        if plan_undo(&self.migrations(), &found, target)?.is_empty() {
            return Ok(Vec::new());
        }
        match self.path() {
            Some(path) => {
                let mut conn = writing(path, false).await?;
                let undone = undo_to(&mut conn, Some(path), self.migrator(), target).await;
                conn.close().await.map_err(unavailable)?;
                undone
            }
            None => undo_to(&mut *self.connection().await, None, self.migrator(), target).await,
        }
    }
}

/// A backup is `VACUUM INTO` a new file at the target, on the store's own
/// connection (one opened read-only included), which never overwrites.
impl Backup for SqliteStore {
    type Target = PathBuf;

    async fn backup(&self, target: &PathBuf) -> Result<(), StoreError> {
        vacuum_into(&mut *self.connection().await, target).await
    }
}

/// The in-memory database, migrated.
pub(crate) async fn open_in_memory(migrator: &'static Migrator) -> Result<SqliteStore, StoreError> {
    let options = SqliteConnectOptions::from_str("sqlite::memory:").map_err(unavailable)?;
    let mut conn = options.connect().await.map_err(unavailable)?;
    run_to(&mut conn, None, migrator, latest(&build(migrator))).await?;
    Ok(SqliteStore::on(conn, None, migrator))
}

#[cfg(test)]
#[path = "migrations.test.rs"]
mod tests;
