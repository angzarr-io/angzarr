//! Postgres [`EventStore`].

use std::collections::BTreeMap;

use async_trait::async_trait;
use prost::Message;
use prost_types::Timestamp;
use sea_query::{Expr, Order, PostgresQueryBuilder, Query};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::proto::{Cover, EventBook, EventPage};
use crate::proto_ext::EventBookExt;
use crate::storage::schema::Events;
use crate::storage::sqlite::{edition_predicate, edition_value, page_command_id};
use crate::storage::{recorded_at_text, EventStore, Result, StorageError, StreamHead, StreamRef};

/// Postgres's unique-violation SQLSTATE.
const UNIQUE_VIOLATION: &str = "23505";

/// Postgres implementation of [`EventStore`].
pub struct PostgresEventStore {
    pool: PgPool,
}

impl PostgresEventStore {
    /// An event store over a migrated pool (see [`super::postgres_pool`]).
    pub fn new(pool: PgPool) -> Self {
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
        let sql = query.to_string(PostgresQueryBuilder);
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;
        rows.iter()
            .map(|row| {
                let data: Vec<u8> = row.get("event_data");
                Ok(EventPage::decode(data.as_slice())?)
            })
            .collect()
    }

    async fn next_sequence_in(conn: &mut sqlx::PgConnection, stream: StreamRef<'_>) -> Result<u64> {
        let sql = Self::stream_select(stream)
            .expr(Expr::col(Events::Sequence).max())
            .to_string(PostgresQueryBuilder);
        let row = sqlx::query(&sql).fetch_one(&mut *conn).await?;
        let max: Option<i64> = row.get(0);
        Ok(max.map(|m| m as u64 + 1).unwrap_or(0))
    }

    async fn insert_pages(
        conn: &mut sqlx::PgConnection,
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
                .to_string(PostgresQueryBuilder);
            sqlx::query(&sql).execute(&mut *conn).await?;
        }
        Ok(())
    }
}

/// The advisory-lock key of a stream: appends to one stream serialize on
/// it, so the head read and the insert see no interleaved writer. Fields are
/// joined by U+001F (unit separator): Postgres text cannot hold NUL.
fn stream_lock_key(stream: StreamRef<'_>) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}",
        stream.domain,
        stream.edition.unwrap_or(""),
        stream.root
    )
}

#[async_trait]
impl EventStore for PostgresEventStore {
    async fn append(&self, stream: StreamRef<'_>, pages: &[EventPage]) -> Result<()> {
        let Some(first) = pages.first() else {
            return Ok(());
        };
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(stream_lock_key(stream))
            .execute(&mut *tx)
            .await?;
        let next = Self::next_sequence_in(&mut tx, stream).await?;
        if first.sequence != next {
            return Err(StorageError::SequenceConflict {
                expected: first.sequence,
                actual: next,
            });
        }
        match Self::insert_pages(&mut tx, stream, pages).await {
            Ok(()) => {}
            // The unique stream key is the backstop if a writer ever skips
            // the advisory lock: the append lost the race.
            Err(StorageError::Database(sqlx::Error::Database(e)))
                if e.code().as_deref() == Some(UNIQUE_VIOLATION) =>
            {
                drop(tx);
                let actual = self.head(stream).await?.next_sequence;
                return Err(StorageError::SequenceConflict {
                    expected: first.sequence,
                    actual,
                });
            }
            Err(e) => return Err(e),
        }
        tx.commit().await?;
        Ok(())
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
            .to_string(PostgresQueryBuilder);
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
        let sql = query.to_string(PostgresQueryBuilder);
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

#[cfg(test)]
#[path = "event_store.test.rs"]
mod tests;
