//! SQLite [`EventStore`].

use std::collections::BTreeMap;

use async_trait::async_trait;
use prost::Message;
use prost_types::Timestamp;
use sea_query::{Expr, Order, Query, SqliteQueryBuilder};
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use super::{edition_predicate, edition_value};
use crate::proto::{event_meta::Causation, Cover, EventBook, EventPage};
use crate::proto_ext::EventBookExt;
use crate::storage::schema::Events;
use crate::storage::{recorded_at_text, EventStore, Result, StorageError, StreamHead, StreamRef};

/// SQLite implementation of [`EventStore`].
pub struct SqliteEventStore {
    pool: SqlitePool,
}

impl SqliteEventStore {
    /// An event store over a migrated pool (see `factory::sqlite_pool`).
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    fn stream_select(stream: StreamRef<'_>) -> sea_query::SelectStatement {
        Query::select()
            .from(Events::Table)
            .and_where(Expr::col(Events::Domain).eq(stream.domain))
            .and_where(edition_predicate(Events::Edition, stream.edition))
            .and_where(Expr::col(Events::Root).eq(stream.root.as_bytes().to_vec()))
            .to_owned()
    }

    async fn fetch_pages(&self, query: sea_query::SelectStatement) -> Result<Vec<EventPage>> {
        let sql = query.to_string(SqliteQueryBuilder);
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;
        rows.iter()
            .map(|row| {
                let data: Vec<u8> = row.get("event_data");
                Ok(EventPage::decode(data.as_slice())?)
            })
            .collect()
    }

    async fn next_sequence_in(
        conn: &mut sqlx::SqliteConnection,
        stream: StreamRef<'_>,
    ) -> Result<u64> {
        let sql = Self::stream_select(stream)
            .expr(Expr::col(Events::Sequence).max())
            .to_string(SqliteQueryBuilder);
        let row = sqlx::query(&sql).fetch_one(&mut *conn).await?;
        let max: Option<i64> = row.get(0);
        Ok(max.map(|m| m as u64 + 1).unwrap_or(0))
    }

    async fn insert_pages(
        conn: &mut sqlx::SqliteConnection,
        stream: StreamRef<'_>,
        pages: &[EventPage],
    ) -> Result<()> {
        for page in pages {
            let meta = page.meta.clone().unwrap_or_default();
            let recorded_at = meta
                .recorded_at
                .as_ref()
                .map(recorded_at_text)
                .transpose()?
                .unwrap_or_default();
            let correlation = meta
                .correlation_id
                .as_ref()
                .and_then(|c| Uuid::try_from(c).ok())
                .unwrap_or_default();
            let command_id: Option<Vec<u8>> = page_command_id(page).map(|u| u.as_bytes().to_vec());
            let sql = Query::insert()
                .into_table(Events::Table)
                .columns([
                    Events::Domain,
                    Events::Edition,
                    Events::Root,
                    Events::Sequence,
                    Events::RecordedAt,
                    Events::CommandId,
                    Events::CorrelationId,
                    Events::EventData,
                ])
                .values_panic([
                    stream.domain.into(),
                    edition_value(stream.edition),
                    stream.root.as_bytes().to_vec().into(),
                    (page.sequence as i64).into(),
                    recorded_at.into(),
                    command_id.into(),
                    correlation.as_bytes().to_vec().into(),
                    page.encode_to_vec().into(),
                ])
                .to_string(SqliteQueryBuilder);
            sqlx::query(&sql).execute(&mut *conn).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl EventStore for SqliteEventStore {
    async fn append(&self, stream: StreamRef<'_>, pages: &[EventPage]) -> Result<()> {
        let Some(first) = pages.first() else {
            return Ok(());
        };
        let mut conn = self.pool.acquire().await?;
        // BEGIN IMMEDIATE takes the write lock up front, so two appends to
        // one stream serialize here instead of deadlocking on lock upgrade.
        sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;
        let result = async {
            let next = Self::next_sequence_in(&mut conn, stream).await?;
            if first.sequence != next {
                return Err(StorageError::SequenceConflict {
                    expected: first.sequence,
                    actual: next,
                });
            }
            Self::insert_pages(&mut conn, stream, pages).await
        }
        .await;
        match result {
            Ok(()) => {
                sqlx::query("COMMIT").execute(&mut *conn).await?;
                Ok(())
            }
            Err(e) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                Err(e)
            }
        }
    }

    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Vec<EventPage>> {
        let mut query = Self::stream_select(stream)
            .column(Events::EventData)
            .and_where(Expr::col(Events::Sequence).gte(from as i64))
            .order_by(Events::Sequence, Order::Asc)
            .to_owned();
        if let Some(to) = to {
            query.and_where(Expr::col(Events::Sequence).lt(to as i64));
        }
        self.fetch_pages(query).await
    }

    async fn read_until_time(
        &self,
        stream: StreamRef<'_>,
        until: &Timestamp,
    ) -> Result<Vec<EventPage>> {
        let query = Self::stream_select(stream)
            .column(Events::EventData)
            .and_where(Expr::col(Events::RecordedAt).lte(recorded_at_text(until)?))
            .order_by(Events::Sequence, Order::Asc)
            .to_owned();
        self.fetch_pages(query).await
    }

    async fn head(&self, stream: StreamRef<'_>) -> Result<StreamHead> {
        let query = Self::stream_select(stream)
            .column(Events::EventData)
            .order_by(Events::Sequence, Order::Desc)
            .limit(1)
            .to_owned();
        let last = self.fetch_pages(query).await?.pop();
        Ok(match last {
            Some(page) => StreamHead {
                next_sequence: page.sequence + 1,
                last_recorded_at: page.meta.and_then(|m| m.recorded_at),
            },
            None => StreamHead::default(),
        })
    }

    async fn find_by_command_id(
        &self,
        stream: StreamRef<'_>,
        command_id: &Uuid,
    ) -> Result<Vec<EventPage>> {
        // An index lookup on (stream, command_id): idx_events_command.
        let query = Self::stream_select(stream)
            .column(Events::EventData)
            .and_where(Expr::col(Events::CommandId).eq(command_id.as_bytes().to_vec()))
            .order_by(Events::Sequence, Order::Asc)
            .to_owned();
        self.fetch_pages(query).await
    }

    async fn list_streams(&self) -> Result<Vec<Cover>> {
        let sql = Query::select()
            .distinct()
            .columns([Events::Domain, Events::Edition, Events::Root])
            .from(Events::Table)
            .order_by(Events::Domain, Order::Asc)
            .order_by(Events::Root, Order::Asc)
            .to_string(SqliteQueryBuilder);
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;
        rows.iter()
            .map(|row| {
                let root: Vec<u8> = row.get("root");
                Ok(Cover {
                    domain: row.get("domain"),
                    edition: row.get("edition"),
                    root: Some(Uuid::from_slice(&root)?.into()),
                })
            })
            .collect()
    }

    async fn read_by_correlation(
        &self,
        domain: Option<&str>,
        edition: Option<&str>,
        correlation_id: &Uuid,
    ) -> Result<Vec<EventBook>> {
        let mut query = Query::select()
            .columns([
                Events::Domain,
                Events::Edition,
                Events::Root,
                Events::EventData,
            ])
            .from(Events::Table)
            .and_where(Expr::col(Events::CorrelationId).eq(correlation_id.as_bytes().to_vec()))
            .and_where(edition_predicate(Events::Edition, edition))
            .order_by(Events::Domain, Order::Asc)
            .order_by(Events::Root, Order::Asc)
            .order_by(Events::Sequence, Order::Asc)
            .to_owned();
        if let Some(domain) = domain {
            query.and_where(Expr::col(Events::Domain).eq(domain));
        }
        let sql = query.to_string(SqliteQueryBuilder);
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;
        let mut books: BTreeMap<(String, Vec<u8>), EventBook> = BTreeMap::new();
        for row in &rows {
            let domain: String = row.get("domain");
            let root: Vec<u8> = row.get("root");
            let data: Vec<u8> = row.get("event_data");
            let book = books
                .entry((domain.clone(), root.clone()))
                .or_insert_with(|| EventBook {
                    cover: Some(Cover {
                        domain,
                        edition: edition.map(str::to_string),
                        root: Uuid::from_slice(&root).ok().map(Into::into),
                    }),
                    ..Default::default()
                });
            book.pages.push(EventPage::decode(data.as_slice())?);
        }
        Ok(books
            .into_values()
            .map(|mut book| {
                book.next_sequence = book.computed_next_sequence();
                book
            })
            .collect())
    }
}

/// The command id a page records as its cause, when it has one.
pub(crate) fn page_command_id(page: &EventPage) -> Option<Uuid> {
    match page.meta.as_ref()?.causation.as_ref()? {
        Causation::CommandId(id) => Uuid::try_from(id).ok(),
        Causation::TriggeringEvent(_) => None,
    }
}

#[cfg(test)]
#[path = "event_store.test.rs"]
mod tests;
