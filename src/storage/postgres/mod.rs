//! Postgres storage: the event and snapshot stores (feature `postgres`).
//!
//! The same schema as SQLite (`migrations/postgres/0001_baseline.sql`
//! mirrors `migrations/sqlite/0001_baseline.sql`): pages stored whole, the
//! stream key `(domain, edition, root)` with `edition` NULL for main, ids as
//! 16 bytes, `recorded_at` as fixed-width RFC 3339 text so text order is time
//! order at nanosecond precision (a `timestamptz` keeps only microseconds).
//! The edition predicate and value are SQLite's ([`super::sqlite`]), so the
//! two backends never split on how main is stored.

mod event_store;
mod snapshot_store;

pub use event_store::PostgresEventStore;
pub use snapshot_store::PostgresSnapshotStore;

use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

use super::StorageError;

/// A Postgres pool with the baseline schema migrated. A database left by the
/// schema from before the latest proto is refused, never migrated over.
pub async fn postgres_pool(uri: &str) -> Result<PgPool, StorageError> {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(uri)
        .await?;
    refuse_old_schema(&pool).await?;
    sqlx::migrate!("migrations/postgres")
        .run(&pool)
        .await
        .map_err(|e| StorageError::Database(e.into()))?;
    Ok(pool)
}

/// Refuse a database created by the old schema: an `events` table with the
/// old `created_at` column, or applied migrations past the single baseline
/// (the old schema had twelve). Its events cannot be decoded by this build,
/// and migrating over it would mix two schemas. The database is left
/// untouched; the operator recreates it.
async fn refuse_old_schema(pool: &PgPool) -> Result<(), StorageError> {
    let old_events: i64 = sqlx::query(
        "SELECT count(*) AS n FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name = 'events' \
         AND column_name = 'created_at'",
    )
    .fetch_one(pool)
    .await?
    .get("n");
    let has_ledger: i64 = sqlx::query(
        "SELECT count(*) AS n FROM information_schema.tables \
         WHERE table_schema = current_schema() AND table_name = '_sqlx_migrations'",
    )
    .fetch_one(pool)
    .await?
    .get("n");
    let old_migrations: i64 = if has_ledger > 0 {
        sqlx::query("SELECT count(*) AS n FROM _sqlx_migrations WHERE version > 1")
            .fetch_one(pool)
            .await?
            .get("n")
    } else {
        0
    };
    if old_events > 0 || old_migrations > 0 {
        return Err(StorageError::OldSchema(
            "the Postgres database uses the schema from before the latest angzarr proto; \
             its events cannot be read by this build. Stop every coordinator using it, \
             recreate the database (or point the configuration at a new one), and start \
             again: it will be created with the baseline schema"
                .to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod test_db;

#[cfg(test)]
#[path = "schema.test.rs"]
mod schema_tests;
