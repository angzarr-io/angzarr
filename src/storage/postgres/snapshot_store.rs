//! Postgres [`SnapshotStore`]: one row per stream, the newest, holding the
//! state type, the state bytes (offered unchanged), the sequence and the
//! full rules version.

use async_trait::async_trait;
use sea_query::{Expr, PostgresQueryBuilder, Query};
use sqlx::{PgPool, Row};

use crate::proto::Snapshot;
use crate::storage::schema::Snapshots;
use crate::storage::sqlite::{edition_predicate, edition_value};
use crate::storage::{Result, SnapshotStore, StreamRef};

/// Postgres implementation of [`SnapshotStore`].
pub struct PostgresSnapshotStore {
    pool: PgPool,
}

impl PostgresSnapshotStore {
    /// A snapshot store over a migrated pool (see [`super::postgres_pool`]).
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn stream_condition(stream: StreamRef<'_>) -> sea_query::Condition {
    sea_query::Cond::all()
        .add(Expr::col(Snapshots::Domain).eq(stream.domain))
        .add(edition_predicate(Snapshots::Edition, stream.edition))
        .add(Expr::col(Snapshots::Root).eq(stream.root.as_bytes().to_vec()))
}

#[async_trait]
impl SnapshotStore for PostgresSnapshotStore {
    async fn get(&self, stream: StreamRef<'_>) -> Result<Option<Snapshot>> {
        let sql = Query::select()
            .columns([
                Snapshots::Sequence,
                Snapshots::StateTypeUrl,
                Snapshots::StateValue,
                Snapshots::RulesVersion,
            ])
            .from(Snapshots::Table)
            .cond_where(stream_condition(stream))
            .to_string(PostgresQueryBuilder);
        let row = sqlx::query(&sql).fetch_optional(&self.pool).await?;
        Ok(row.map(|row| Snapshot {
            sequence: row.get::<i64, _>("sequence") as u64,
            state: Some(prost_types::Any {
                type_url: row.get("state_type_url"),
                value: row.get("state_value"),
            }),
            rules_version: row.get("rules_version"),
        }))
    }

    async fn put(&self, stream: StreamRef<'_>, snapshot: &Snapshot) -> Result<()> {
        // The coordinator always stores a state; a snapshot without one has
        // nothing to offer.
        let Some(state) = snapshot.state.as_ref() else {
            return Ok(());
        };
        let mut tx = self.pool.begin().await?;
        let delete = Query::delete()
            .from_table(Snapshots::Table)
            .cond_where(stream_condition(stream))
            .to_string(PostgresQueryBuilder);
        sqlx::query(&delete).execute(&mut *tx).await?;
        let insert = Query::insert()
            .into_table(Snapshots::Table)
            .columns([
                Snapshots::Domain,
                Snapshots::Edition,
                Snapshots::Root,
                Snapshots::Sequence,
                Snapshots::StateTypeUrl,
                Snapshots::StateValue,
                Snapshots::RulesVersion,
            ])
            .values_panic([
                stream.domain.into(),
                edition_value(stream.edition),
                stream.root.as_bytes().to_vec().into(),
                (snapshot.sequence as i64).into(),
                state.type_url.clone().into(),
                state.value.clone().into(),
                snapshot.rules_version.clone().into(),
            ])
            .to_string(PostgresQueryBuilder);
        sqlx::query(&insert).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "snapshot_store.test.rs"]
mod tests;
