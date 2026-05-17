//! Unified SQL SnapshotStore implementation.
//!
//! Uses a macro to generate implementations for each SQL backend,
//! eliminating code duplication while maintaining type safety.
//!
//! ## Edition NULL polarity (C-15)
//!
//! Both Postgres (migration 0007) and SQLite (migration 0006) made the
//! `edition` column nullable and normalized pre-existing rows where
//! `edition IN ('angzarr', '')` to SQL NULL. The API surface keeps two
//! sentinel forms — `""` and `"angzarr"` — for "main timeline" per
//! `is_main_timeline`. To preserve that equivalence at the storage layer
//! we MUST:
//!
//!   * Insert SQL NULL whenever the caller passes either main-timeline form.
//!   * Match `IS NULL` (not `edition = ''`/`edition = 'angzarr'`) when reading.
//!
//! Both rules apply uniformly through `edition_to_db_value` and
//! `edition_predicate_expr` below; a bare `Expr::col(Edition).eq(edition)`
//! is a polarity-split bug.

use std::marker::PhantomData;

use super::SqlDatabase;

/// Translate the API-layer edition string to a sea-query `SimpleExpr` value
/// suitable for INSERT. Both main-timeline sentinels (`""`, `"angzarr"`)
/// become SQL NULL; named editions become a string literal.
pub(crate) fn edition_to_db_value(edition: &str) -> sea_query::SimpleExpr {
    if crate::storage::helpers::is_main_timeline(edition) {
        // `Option::<String>::None.into()` lowers to SQL NULL in both
        // PostgresQueryBuilder and SqliteQueryBuilder.
        let none: Option<String> = None;
        sea_query::SimpleExpr::Value(sea_query::Value::String(none.map(Box::new)))
    } else {
        sea_query::SimpleExpr::Value(sea_query::Value::String(Some(Box::new(
            edition.to_string(),
        ))))
    }
}

/// Build an `edition` column WHERE predicate. Either main-timeline sentinel
/// translates to `IS NULL`; named editions to `= <name>`.
pub(crate) fn edition_predicate_expr<T: sea_query::Iden + 'static>(
    col: T,
    edition: &str,
) -> sea_query::SimpleExpr {
    if crate::storage::helpers::is_main_timeline(edition) {
        sea_query::Expr::col(col).is_null()
    } else {
        sea_query::Expr::col(col).eq(edition)
    }
}

/// SQL-based implementation of SnapshotStore.
///
/// This generic implementation works with any SQL database that implements
/// the `SqlDatabase` trait (PostgreSQL, SQLite).
pub struct SqlSnapshotStore<DB: SqlDatabase> {
    pool: DB::Pool,
    _marker: PhantomData<DB>,
}

impl<DB: SqlDatabase> SqlSnapshotStore<DB> {
    /// Create a new SQL snapshot store with the given pool.
    pub fn new(pool: DB::Pool) -> Self {
        Self {
            pool,
            _marker: PhantomData,
        }
    }

    /// Get the underlying pool.
    pub fn pool(&self) -> &DB::Pool {
        &self.pool
    }
}

/// Macro to implement SnapshotStore for a specific SQL backend.
///
/// Both backends now support multiple snapshots per aggregate with retention
/// policies. The schema's primary key is (domain, edition, root, sequence).
///
/// Note: Feature gating is applied at the macro invocation site, not inside the macro.
macro_rules! impl_snapshot_store {
    ($db_type:ty) => {
        #[async_trait::async_trait]
        impl crate::storage::SnapshotStore for SqlSnapshotStore<$db_type> {
            async fn get(
                &self,
                domain: &str,
                edition: &str,
                root: uuid::Uuid,
            ) -> crate::storage::Result<Option<crate::proto::Snapshot>> {
                use prost::Message;
                use sea_query::{Expr, Query};
                use sqlx::Row;

                use crate::storage::schema::Snapshots;

                let root_str = root.to_string();

                // C-15: main-timeline sentinels map to SQL NULL via
                // `edition_predicate_expr`.
                let stmt = Query::select()
                    .column(Snapshots::StateData)
                    .column(Snapshots::Sequence)
                    .from(Snapshots::Table)
                    .and_where(
                        $crate::storage::sql::snapshot_store::edition_predicate_expr(
                            Snapshots::Edition,
                            edition,
                        ),
                    )
                    .and_where(Expr::col(Snapshots::Domain).eq(domain))
                    .and_where(Expr::col(Snapshots::Root).eq(&root_str))
                    .order_by(Snapshots::Sequence, sea_query::Order::Desc)
                    .limit(1)
                    .to_owned();

                let sql = <$db_type>::build_select(stmt);
                let row = sqlx::query(&sql).fetch_optional(&self.pool).await?;

                match row {
                    Some(row) => {
                        let state_data: Vec<u8> = row.get("state_data");
                        let snapshot = crate::proto::Snapshot::decode(state_data.as_slice())?;
                        Ok(Some(snapshot))
                    }
                    None => Ok(None),
                }
            }

            async fn get_at_seq(
                &self,
                domain: &str,
                edition: &str,
                root: uuid::Uuid,
                seq: u32,
            ) -> crate::storage::Result<Option<crate::proto::Snapshot>> {
                use prost::Message;
                use sea_query::{Expr, Query};
                use sqlx::Row;

                use crate::storage::schema::Snapshots;

                let root_str = root.to_string();

                // C-15: main-timeline sentinels map to SQL NULL.
                let stmt = Query::select()
                    .column(Snapshots::StateData)
                    .column(Snapshots::Sequence)
                    .from(Snapshots::Table)
                    .and_where(
                        $crate::storage::sql::snapshot_store::edition_predicate_expr(
                            Snapshots::Edition,
                            edition,
                        ),
                    )
                    .and_where(Expr::col(Snapshots::Domain).eq(domain))
                    .and_where(Expr::col(Snapshots::Root).eq(&root_str))
                    .and_where(Expr::col(Snapshots::Sequence).lte(seq))
                    .order_by(Snapshots::Sequence, sea_query::Order::Desc)
                    .limit(1)
                    .to_owned();

                let sql = <$db_type>::build_select(stmt);
                let row = sqlx::query(&sql).fetch_optional(&self.pool).await?;

                match row {
                    Some(row) => {
                        let state_data: Vec<u8> = row.get("state_data");
                        let snapshot = crate::proto::Snapshot::decode(state_data.as_slice())?;
                        Ok(Some(snapshot))
                    }
                    None => Ok(None),
                }
            }

            async fn put(
                &self,
                domain: &str,
                edition: &str,
                root: uuid::Uuid,
                snapshot: crate::proto::Snapshot,
            ) -> crate::storage::Result<()> {
                use prost::Message;
                use sea_query::{Expr, OnConflict, Query};

                use crate::proto::SnapshotRetention;
                use crate::storage::schema::Snapshots;

                let root_str = root.to_string();
                let state_data = snapshot.encode_to_vec();
                let sequence = snapshot.sequence;
                let retention = snapshot.retention;
                let created_at = chrono::Utc::now().to_rfc3339();

                // Step 1: Insert or update the snapshot at this sequence
                // PK is (domain, edition, root, sequence)
                // C-15: edition is stored as SQL NULL for main-timeline writes.
                let edition_value =
                    $crate::storage::sql::snapshot_store::edition_to_db_value(edition);
                let stmt = Query::insert()
                    .into_table(Snapshots::Table)
                    .columns([
                        Snapshots::Edition,
                        Snapshots::Domain,
                        Snapshots::Root,
                        Snapshots::Sequence,
                        Snapshots::StateData,
                        Snapshots::Retention,
                        Snapshots::CreatedAt,
                    ])
                    .values_panic([
                        edition_value.clone(),
                        domain.into(),
                        root_str.clone().into(),
                        sequence.into(),
                        state_data.into(),
                        retention.into(),
                        created_at.into(),
                    ])
                    .on_conflict(
                        OnConflict::columns([
                            Snapshots::Edition,
                            Snapshots::Domain,
                            Snapshots::Root,
                            Snapshots::Sequence,
                        ])
                        .update_columns([
                            Snapshots::StateData,
                            Snapshots::Retention,
                            Snapshots::CreatedAt,
                        ])
                        .to_owned(),
                    )
                    .to_owned();

                let sql = <$db_type>::build_insert(stmt);
                sqlx::query(&sql).execute(&self.pool).await?;

                // Step 2: Clean up old TRANSIENT snapshots (retention = 2)
                // Keep PERSIST (1) and DEFAULT (0) snapshots
                let cleanup_stmt = Query::delete()
                    .from_table(Snapshots::Table)
                    .and_where(
                        $crate::storage::sql::snapshot_store::edition_predicate_expr(
                            Snapshots::Edition,
                            edition,
                        ),
                    )
                    .and_where(Expr::col(Snapshots::Domain).eq(domain))
                    .and_where(Expr::col(Snapshots::Root).eq(&root_str))
                    .and_where(Expr::col(Snapshots::Sequence).lt(sequence))
                    .and_where(
                        Expr::col(Snapshots::Retention)
                            .eq(SnapshotRetention::RetentionTransient as i32),
                    )
                    .to_owned();

                let cleanup_sql = <$db_type>::build_delete(cleanup_stmt);
                sqlx::query(&cleanup_sql).execute(&self.pool).await?;

                Ok(())
            }

            async fn delete(
                &self,
                domain: &str,
                edition: &str,
                root: uuid::Uuid,
            ) -> crate::storage::Result<()> {
                use sea_query::{Expr, Query};

                use crate::storage::schema::Snapshots;

                let root_str = root.to_string();

                // C-15: main-timeline sentinels map to SQL NULL.
                let stmt = Query::delete()
                    .from_table(Snapshots::Table)
                    .and_where(
                        $crate::storage::sql::snapshot_store::edition_predicate_expr(
                            Snapshots::Edition,
                            edition,
                        ),
                    )
                    .and_where(Expr::col(Snapshots::Domain).eq(domain))
                    .and_where(Expr::col(Snapshots::Root).eq(&root_str))
                    .to_owned();

                let sql = <$db_type>::build_delete(stmt);
                sqlx::query(&sql).execute(&self.pool).await?;

                Ok(())
            }
        }
    };
}

// Generate implementations for each SQL backend
#[cfg(feature = "postgres")]
impl_snapshot_store!(super::postgres::Postgres);
// SQLite is always compiled
impl_snapshot_store!(super::sqlite::Sqlite);
