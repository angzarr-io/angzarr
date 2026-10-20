//! SQLite [`SnapshotStore`]: one row per stream, the newest.

use async_trait::async_trait;
use prost::Message;
use sea_query::{Expr, Order, Query, SqliteQueryBuilder};
use sqlx::{Row, SqlitePool};

use super::{edition_predicate, edition_value};
use crate::proto::Snapshot;
use crate::storage::schema::Snapshots;
use crate::storage::{Result, SnapshotStore, StreamRef};

/// SQLite implementation of [`SnapshotStore`].
pub struct SqliteSnapshotStore {
    pool: SqlitePool,
}

impl SqliteSnapshotStore {
    /// A snapshot store over a migrated pool (see `factory::sqlite_pool`).
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

fn stream_condition(stream: StreamRef<'_>) -> sea_query::Condition {
    sea_query::Cond::all()
        .add(Expr::col(Snapshots::Domain).eq(stream.domain))
        .add(edition_predicate(Snapshots::Edition, stream.edition))
        .add(Expr::col(Snapshots::Root).eq(stream.root.to_string()))
}

#[async_trait]
impl SnapshotStore for SqliteSnapshotStore {
    async fn get(&self, stream: StreamRef<'_>) -> Result<Option<Snapshot>> {
        let sql = Query::select()
            .column(Snapshots::StateData)
            .from(Snapshots::Table)
            .cond_where(stream_condition(stream))
            .order_by(Snapshots::Sequence, Order::Desc)
            .limit(1)
            .to_string(SqliteQueryBuilder);
        let row = sqlx::query(&sql).fetch_optional(&self.pool).await?;
        row.map(|row| {
            let data: Vec<u8> = row.get("state_data");
            Ok(Snapshot::decode(data.as_slice())?)
        })
        .transpose()
    }

    async fn put(&self, stream: StreamRef<'_>, snapshot: &Snapshot) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let delete = Query::delete()
            .from_table(Snapshots::Table)
            .cond_where(stream_condition(stream))
            .to_string(SqliteQueryBuilder);
        sqlx::query(&delete).execute(&mut *tx).await?;
        let insert = Query::insert()
            .into_table(Snapshots::Table)
            .columns([
                Snapshots::Domain,
                Snapshots::Edition,
                Snapshots::Root,
                Snapshots::Sequence,
                Snapshots::StateData,
                Snapshots::CreatedAt,
            ])
            .values_panic([
                stream.domain.into(),
                edition_value(stream.edition),
                stream.root.to_string().into(),
                (snapshot.sequence as i64).into(),
                snapshot.encode_to_vec().into(),
                chrono::Utc::now().to_rfc3339().into(),
            ])
            .to_string(SqliteQueryBuilder);
        sqlx::query(&insert).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "snapshot_store.test.rs"]
mod tests;
