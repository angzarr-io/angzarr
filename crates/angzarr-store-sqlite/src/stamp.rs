//! The wire stamp (decision 14.113, docs/design/wire-versioning.md in
//! angzarr-project): the version of the wire package that wrote each page
//! and snapshot, and the store's `wire_stamp` row of the oldest and newest.
//!
//! Migration 0009 adds the columns, marking every row it finds unstamped;
//! [`backfill`] then runs once, under the migration's lock, and classifies
//! each row by the shape of its bytes ([`wire::shape_of_page`]), marks it,
//! and inserts the stamp. A store whose rows fit no shape is refused and the
//! migration, being one transaction, leaves it as it was. [`survey`] is the
//! same classification without the writes, for a caller that asks first.

use std::collections::BTreeMap;
use std::path::Path;

use angzarr_ports::angzarr_proto::v1::Cover;
use angzarr_ports::angzarr_proto::wire::{self, Shape};
use angzarr_ports::{StoreError, WireStamp, WireVersion};
use prost::Message;
use sqlx::sqlite::SqliteConnection;
use sqlx::{Connection, Row};

use crate::error::unavailable;
use crate::rows;
use crate::sqlite::SqliteDialect;

/// The migration that adds the stamp: once it is applied, a migration runs
/// the backfill (which runs once).
pub(crate) const STAMPED_FROM: u64 = 9;

/// The backfill's name in the `backfills` table.
const WIRE_BACKFILL: &str = "wire-stamp";

/// Rows of each wire shape found in one table.
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct ShapeCounts {
    pub v0: u64,
    pub v1: u64,
    pub unrecognised: u64,
}

/// What [`SqliteStore::detect_wire_at`](crate::SqliteStore::detect_wire_at)
/// found: the rows of each shape, the transient rows a legacy store holds,
/// and what the open would refuse, if anything. Nothing was written.
#[derive(Clone, PartialEq, Debug)]
pub struct WireDetected {
    pub pages: ShapeCounts,
    pub snapshots: ShapeCounts,
    pub outbox: u64,
    pub dead_letters: u64,
    pub refusal: Option<StoreError>,
}

/// What [`SqliteStore::rewrite_to_current`](crate::SqliteStore::rewrite_to_current)
/// rewrote.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct WireRewritten {
    pub pages: u64,
    pub snapshots: u64,
}

/// A row to mark `v0`: its table's key.
enum Legacy {
    Page(String, String, Vec<u8>, i64),
    Snapshot(String, String, Vec<u8>),
}

pub(crate) struct Surveyed {
    pub detected: WireDetected,
    legacy: Vec<Legacy>,
}

fn count(counts: &mut ShapeCounts, shape: Shape) {
    match shape {
        Shape::V0 => counts.v0 += 1,
        Shape::V1 => counts.v1 += 1,
        Shape::Unrecognised => counts.unrecognised += 1,
    }
}

async fn has_table(conn: &mut SqliteConnection, name: &str) -> Result<bool, StoreError> {
    let found: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1")
            .bind(name)
            .fetch_one(&mut *conn)
            .await
            .map_err(unavailable)?;
    Ok(found > 0)
}

async fn rows_of(conn: &mut SqliteConnection, table: &str) -> Result<u64, StoreError> {
    if !has_table(conn, table).await? {
        return Ok(0);
    }
    let rows: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(&mut *conn)
        .await
        .map_err(unavailable)?;
    Ok(rows.unsigned_abs())
}

/// Classify every page and snapshot by shape, writing nothing. The first
/// refusal is kept (every row is still counted).
pub(crate) async fn survey(conn: &mut SqliteConnection) -> Result<Surveyed, StoreError> {
    let mut detected = WireDetected {
        pages: ShapeCounts::default(),
        snapshots: ShapeCounts::default(),
        outbox: 0,
        dead_letters: 0,
        refusal: None,
    };
    let mut legacy = Vec::new();
    let pages = sqlx::query(
        "SELECT domain, edition, root, sequence, page FROM events \
         ORDER BY domain, edition, root, sequence",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(unavailable)?;
    // Whether a stream has shown a current-shape page: no older page may
    // follow one.
    let mut current: BTreeMap<(String, String, Vec<u8>), bool> = BTreeMap::new();
    for row in &pages {
        let domain: String = row.try_get(0).map_err(unavailable)?;
        let edition: String = row.try_get(1).map_err(unavailable)?;
        let root: Vec<u8> = row.try_get(2).map_err(unavailable)?;
        let sequence: i64 = row.try_get(3).map_err(unavailable)?;
        let bytes: Vec<u8> = row.try_get(4).map_err(unavailable)?;
        let mut shape = wire::shape_of_page(&bytes);
        // A page that says another sequence than its row is no page of
        // that shape, whatever its fields look like.
        if let Some(version) = shape.version() {
            let says = wire::decode_page(version.0, &bytes).map(|p| p.sequence);
            if says != Ok(sequence.unsigned_abs()) {
                shape = Shape::Unrecognised;
            }
        }
        let seen = current
            .entry((domain.clone(), edition.clone(), root.clone()))
            .or_default();
        match shape {
            Shape::V1 => *seen = true,
            Shape::V0 if *seen => shape = Shape::Unrecognised,
            _ => {}
        }
        count(&mut detected.pages, shape);
        match shape {
            Shape::V0 => legacy.push(Legacy::Page(domain, edition, root, sequence)),
            Shape::Unrecognised if detected.refusal.is_none() => {
                let cover = rows::cover::<SqliteDialect>(&domain, edition, &root)?;
                detected.refusal = Some(StoreError::WireUnrecognised {
                    table: "events".to_string(),
                    stream: Box::new(cover),
                    sequence: sequence.unsigned_abs(),
                });
            }
            _ => {}
        }
    }
    let snapshots = sqlx::query(
        "SELECT domain, edition, root, snapshot FROM snapshots ORDER BY domain, edition, root",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(unavailable)?;
    for row in &snapshots {
        let domain: String = row.try_get(0).map_err(unavailable)?;
        let edition: String = row.try_get(1).map_err(unavailable)?;
        let root: Vec<u8> = row.try_get(2).map_err(unavailable)?;
        let bytes: Vec<u8> = row.try_get(3).map_err(unavailable)?;
        let shape = wire::shape_of_snapshot(&bytes);
        count(&mut detected.snapshots, shape);
        match shape {
            Shape::V0 => legacy.push(Legacy::Snapshot(domain, edition, root)),
            Shape::Unrecognised if detected.refusal.is_none() => {
                let cover = rows::cover::<SqliteDialect>(&domain, edition, &root)?;
                detected.refusal = Some(StoreError::WireUnrecognised {
                    table: "snapshots".to_string(),
                    stream: Box::new(cover),
                    sequence: 0,
                });
            }
            _ => {}
        }
    }
    detected.outbox = rows_of(conn, "outbox").await?;
    detected.dead_letters = rows_of(conn, "dead_letters").await?;
    if detected.refusal.is_none()
        && !legacy.is_empty()
        && (detected.outbox > 0 || detected.dead_letters > 0)
    {
        detected.refusal = Some(StoreError::WireLegacyTransient {
            outbox: detected.outbox,
            dead_letters: detected.dead_letters,
        });
    }
    Ok(Surveyed { detected, legacy })
}

/// Stamp the store once: classify every row (refusing the store if one
/// fits no shape), mark each, and insert the stamp. Run under the lock of
/// the migration that added the columns, and marked done.
pub(crate) async fn backfill(conn: &mut SqliteConnection) -> Result<(), StoreError> {
    let done: i64 = sqlx::query_scalar("SELECT done FROM backfills WHERE name = ?1")
        .bind(WIRE_BACKFILL)
        .fetch_one(&mut *conn)
        .await
        .map_err(unavailable)?;
    if done != 0 {
        return Ok(());
    }
    let Surveyed { detected, legacy } = survey(conn).await?;
    if let Some(refusal) = detected.refusal {
        return Err(refusal);
    }
    let current = i64::from(WireVersion::CURRENT.0);
    sqlx::query("UPDATE events SET wire = ?1")
        .bind(current)
        .execute(&mut *conn)
        .await
        .map_err(unavailable)?;
    sqlx::query("UPDATE snapshots SET wire = ?1")
        .bind(current)
        .execute(&mut *conn)
        .await
        .map_err(unavailable)?;
    for row in &legacy {
        let old = i64::from(WireVersion::V0.0);
        match row {
            Legacy::Page(domain, edition, root, sequence) => {
                sqlx::query(
                    "UPDATE events SET wire = ?1 \
                     WHERE domain = ?2 AND edition = ?3 AND root = ?4 AND sequence = ?5",
                )
                .bind(old)
                .bind(domain)
                .bind(edition)
                .bind(root)
                .bind(sequence)
                .execute(&mut *conn)
                .await
                .map_err(unavailable)?;
            }
            Legacy::Snapshot(domain, edition, root) => {
                sqlx::query(
                    "UPDATE snapshots SET wire = ?1 \
                     WHERE domain = ?2 AND edition = ?3 AND root = ?4",
                )
                .bind(old)
                .bind(domain)
                .bind(edition)
                .bind(root)
                .execute(&mut *conn)
                .await
                .map_err(unavailable)?;
            }
        }
    }
    let (oldest, newest) = if legacy.is_empty() {
        (current, current)
    } else {
        let others = detected.pages.v1 + detected.snapshots.v1;
        (
            i64::from(WireVersion::V0.0),
            if others > 0 {
                current
            } else {
                i64::from(WireVersion::V0.0)
            },
        )
    };
    sqlx::query("INSERT INTO wire_stamp (id, oldest, newest) VALUES (1, ?1, ?2)")
        .bind(oldest)
        .bind(newest)
        .execute(&mut *conn)
        .await
        .map_err(unavailable)?;
    sqlx::query("UPDATE backfills SET done = 1 WHERE name = ?1")
        .bind(WIRE_BACKFILL)
        .execute(&mut *conn)
        .await
        .map_err(unavailable)?;
    Ok(())
}

/// The store's stamp, or none for a store with no stamp table.
pub(crate) async fn read(conn: &mut SqliteConnection) -> Result<Option<WireStamp>, StoreError> {
    if !has_table(conn, "wire_stamp").await? {
        return Ok(None);
    }
    let row = sqlx::query("SELECT oldest, newest FROM wire_stamp")
        .fetch_optional(&mut *conn)
        .await
        .map_err(unavailable)?;
    let Some(row) = row else { return Ok(None) };
    let oldest: i64 = row.try_get(0).map_err(unavailable)?;
    let newest: i64 = row.try_get(1).map_err(unavailable)?;
    let version = |n: i64| WireVersion(u8::try_from(n).unwrap_or(WireVersion::UNSTAMPED));
    Ok(Some(WireStamp {
        oldest: version(oldest),
        newest: version(newest),
    }))
}

/// Refuse a store a newer wire package wrote, before a row is read.
pub(crate) async fn check(conn: &mut SqliteConnection) -> Result<(), StoreError> {
    match read(conn).await? {
        Some(stamp) if stamp.newest > WireVersion::CURRENT => Err(StoreError::WireNewerThanBuild {
            stamped: stamp.newest.0,
            build: WireVersion::CURRENT.0,
        }),
        _ => Ok(()),
    }
}

/// The stamp of the store at `path`, read on a read-only connection of its
/// own, nothing migrated: none if there is no file, or the store predates
/// the stamp.
pub(crate) async fn read_at(path: &Path) -> Result<Option<WireStamp>, StoreError> {
    if matches!(path.try_exists(), Ok(false)) {
        return Ok(None);
    }
    let mut conn = crate::migrations::reading(path).await?;
    let stamp = read(&mut conn).await;
    conn.close().await.map_err(unavailable)?;
    stamp
}

/// What the open of the store at `path` would stamp or refuse, counted on a
/// read-only connection of its own and written nowhere.
pub(crate) async fn detect_at(path: &Path) -> Result<WireDetected, StoreError> {
    let mut conn = crate::migrations::reading(path).await?;
    let surveyed = survey(&mut conn).await;
    conn.close().await.map_err(unavailable)?;
    surveyed.map(|s| s.detected)
}

/// Re-encode every old page and snapshot as the current package, in one
/// transaction, and raise the stamp's `oldest`.
pub(crate) async fn rewrite(conn: &mut SqliteConnection) -> Result<WireRewritten, StoreError> {
    let current = i64::from(WireVersion::CURRENT.0);
    let mut tx = conn
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(unavailable)?;
    let mut done = WireRewritten {
        pages: 0,
        snapshots: 0,
    };
    let pages = sqlx::query(
        "SELECT domain, edition, root, sequence, page, wire FROM events WHERE wire <> ?1",
    )
    .bind(current)
    .fetch_all(&mut *tx)
    .await
    .map_err(unavailable)?;
    for row in &pages {
        let domain: String = row.try_get(0).map_err(unavailable)?;
        let edition: String = row.try_get(1).map_err(unavailable)?;
        let root: Vec<u8> = row.try_get(2).map_err(unavailable)?;
        let sequence: i64 = row.try_get(3).map_err(unavailable)?;
        let bytes: Vec<u8> = row.try_get(4).map_err(unavailable)?;
        let wire: i64 = row.try_get(5).map_err(unavailable)?;
        let cover: Cover = rows::cover::<SqliteDialect>(&domain, edition.clone(), &root)?;
        let page = rows::page(&cover, sequence, wire, &bytes)?;
        sqlx::query(
            "UPDATE events SET page = ?1, wire = ?2 \
             WHERE domain = ?3 AND edition = ?4 AND root = ?5 AND sequence = ?6",
        )
        .bind(page.encode_to_vec())
        .bind(current)
        .bind(domain)
        .bind(edition)
        .bind(root)
        .bind(sequence)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        done.pages += 1;
    }
    let snapshots =
        sqlx::query("SELECT domain, edition, root, snapshot, wire FROM snapshots WHERE wire <> ?1")
            .bind(current)
            .fetch_all(&mut *tx)
            .await
            .map_err(unavailable)?;
    for row in &snapshots {
        let domain: String = row.try_get(0).map_err(unavailable)?;
        let edition: String = row.try_get(1).map_err(unavailable)?;
        let root: Vec<u8> = row.try_get(2).map_err(unavailable)?;
        let bytes: Vec<u8> = row.try_get(3).map_err(unavailable)?;
        let wire: i64 = row.try_get(4).map_err(unavailable)?;
        let cover: Cover = rows::cover::<SqliteDialect>(&domain, edition.clone(), &root)?;
        let snapshot = rows::snapshot(&cover, wire, &bytes)?;
        sqlx::query(
            "UPDATE snapshots SET snapshot = ?1, wire = ?2 \
             WHERE domain = ?3 AND edition = ?4 AND root = ?5",
        )
        .bind(snapshot.encode_to_vec())
        .bind(current)
        .bind(domain)
        .bind(edition)
        .bind(root)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        done.snapshots += 1;
    }
    sqlx::query("UPDATE wire_stamp SET oldest = ?1, newest = max(newest, ?1)")
        .bind(current)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    tx.commit().await.map_err(unavailable)?;
    Ok(done)
}

#[cfg(test)]
#[path = "stamp.test.rs"]
mod tests;
