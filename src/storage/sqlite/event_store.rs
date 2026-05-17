//! SQLite EventStore implementation.
//!
//! Implements composite reads for editions: query edition events first to derive
//! the implicit divergence point, then query main timeline up to that point,
//! then merge the results.
//!
//! ## Edition NULL polarity (C-15)
//!
//! Since migration 0006 the SQLite `events` table has a nullable `edition`
//! column with the literal `'angzarr'` / `''` rows backfilled to NULL. The
//! API surface keeps two sentinel forms for "main timeline" — `""` and
//! `"angzarr"` (per `is_main_timeline`). To keep both forms interchangeable
//! and not split data across `NULL` / `''` / `'angzarr'` rows, writes and
//! reads MUST normalize through `edition_to_db` (write side) and
//! `edition_predicate` (read side) below. A bare
//! `Expr::col(Events::Edition).eq(edition)` is a polarity-split bug — it
//! misses NULL rows that the migration backfilled, and creates fragmented
//! storage when callers alternate between sentinels.

use async_trait::async_trait;
use prost::Message;
use sea_query::{Expr, Iden, Order, Query, SimpleExpr, SqliteQueryBuilder};
use sqlx::{Row, SqliteConnection, SqlitePool};
use uuid::Uuid;

use crate::orchestration::aggregate::DEFAULT_EDITION;
use crate::proto::EventPage;
use crate::storage::helpers::{assemble_event_books, is_main_timeline};
use crate::storage::schema::Events;
use crate::storage::{
    AddOutcome, CascadeParticipant, EventStore, Result, SourceInfo, StorageError,
};

/// Build an `edition` column WHERE predicate. Both main-timeline sentinels
/// (`""` and `"angzarr"`) translate to `IS NULL`; named editions to `= <name>`.
fn edition_predicate<T: Iden + 'static>(col: T, edition: &str) -> SimpleExpr {
    if is_main_timeline(edition) {
        Expr::col(col).is_null()
    } else {
        Expr::col(col).eq(edition)
    }
}

/// Convert the API-layer edition to a sea-query `SimpleExpr` value.
///
/// BOTH `""` and `"angzarr"` normalize to SQL NULL so a caller-side polarity
/// flip does not silently split storage.
fn edition_to_db(edition: &str) -> SimpleExpr {
    if is_main_timeline(edition) {
        let none: Option<String> = None;
        SimpleExpr::Value(sea_query::Value::String(none.map(Box::new)))
    } else {
        SimpleExpr::Value(sea_query::Value::String(Some(Box::new(
            edition.to_string(),
        ))))
    }
}

/// Inverse of `edition_to_db`: SQL NULL surfaces as the empty-string sentinel
/// at the API boundary. (`""` is the more conventional form; `"angzarr"`
/// remains accepted on input.)
fn edition_from_db(value: Option<String>) -> String {
    value.unwrap_or_default()
}

/// SQLite implementation of EventStore.
pub struct SqliteEventStore {
    pool: SqlitePool,
}

impl SqliteEventStore {
    /// Create a new SQLite event store.
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Query events for a specific edition (internal helper).
    async fn query_edition_events(
        &self,
        domain: &str,
        edition: &str,
        root_str: &str,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        // C-15: main-timeline sentinels map to SQL NULL via `edition_predicate`.
        let query = Query::select()
            .column(Events::EventData)
            .from(Events::Table)
            .and_where(edition_predicate(Events::Edition, edition))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(root_str))
            .and_where(Expr::col(Events::Sequence).gte(from))
            .order_by(Events::Sequence, Order::Asc)
            .to_string(SqliteQueryBuilder);

        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let event_data: Vec<u8> = row.get("event_data");
            let event = EventPage::decode(event_data.as_slice())?;
            events.push(event);
        }

        Ok(events)
    }

    /// Get the minimum sequence number from edition events (implicit divergence point).
    async fn get_edition_min_sequence(
        &self,
        domain: &str,
        edition: &str,
        root_str: &str,
    ) -> Result<Option<u32>> {
        let query = Query::select()
            .expr(Expr::col(Events::Sequence).min())
            .from(Events::Table)
            .and_where(edition_predicate(Events::Edition, edition))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(root_str))
            .to_string(SqliteQueryBuilder);

        let row = sqlx::query(&query).fetch_optional(&self.pool).await?;

        match row {
            Some(row) => {
                let min_seq: Option<i32> = row.get(0);
                Ok(min_seq.map(|s| s as u32))
            }
            None => Ok(None),
        }
    }

    /// Query main timeline events up to (but not including) a sequence number.
    async fn query_main_events_until(
        &self,
        domain: &str,
        root_str: &str,
        until_seq: u32,
    ) -> Result<Vec<EventPage>> {
        // Main-timeline rows store edition as SQL NULL (C-15).
        let query = Query::select()
            .column(Events::EventData)
            .from(Events::Table)
            .and_where(edition_predicate(Events::Edition, DEFAULT_EDITION))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(root_str))
            .and_where(Expr::col(Events::Sequence).lt(until_seq))
            .order_by(Events::Sequence, Order::Asc)
            .to_string(SqliteQueryBuilder);

        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let event_data: Vec<u8> = row.get("event_data");
            let event = EventPage::decode(event_data.as_slice())?;
            events.push(event);
        }

        Ok(events)
    }

    /// Perform a composite read for an edition.
    ///
    /// 1. Query edition events to get implicit divergence point (min sequence)
    /// 2. Query main timeline events up to divergence point
    /// 3. Merge: main events + edition events
    async fn composite_read(
        &self,
        domain: &str,
        edition: &str,
        root_str: &str,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        self.composite_read_with_divergence(domain, edition, root_str, from, None)
            .await
    }

    /// Perform a composite read with optional explicit divergence point.
    ///
    /// - `explicit_divergence = Some(N)`: Branch starts at sequence N
    /// - `explicit_divergence = None`: Uses implicit divergence (first edition event)
    async fn composite_read_with_divergence(
        &self,
        domain: &str,
        edition: &str,
        root_str: &str,
        from: u32,
        explicit_divergence: Option<u32>,
    ) -> Result<Vec<EventPage>> {
        // Query edition events first
        let edition_events = self
            .query_edition_events(domain, edition, root_str, 0)
            .await?;

        // Determine divergence point: explicit > implicit (first edition event) > 0
        let divergence = if let Some(div) = explicit_divergence {
            div
        } else if edition_events.is_empty() {
            // No edition events and no explicit divergence - return main timeline only
            return self
                .query_edition_events(domain, DEFAULT_EDITION, root_str, from)
                .await;
        } else {
            // Implicit divergence from first edition event
            self.get_edition_min_sequence(domain, edition, root_str)
                .await?
                .unwrap_or(0)
        };

        // Query main timeline events up to divergence point
        let main_events = self
            .query_main_events_until(domain, root_str, divergence)
            .await?;

        // Merge: main events (filtered by from) + edition events (filtered by from)
        let mut result = Vec::new();

        // Add main events that are >= from and < divergence
        for event in main_events {
            let seq = crate::storage::helpers::event_sequence(&event);
            if seq >= from {
                result.push(event);
            }
        }

        // Add edition events that are >= from
        for event in edition_events {
            let seq = crate::storage::helpers::event_sequence(&event);
            if seq >= from {
                result.push(event);
            }
        }

        Ok(result)
    }

    /// Insert events within an already-started transaction.
    /// Returns (first_sequence, last_sequence) of inserted events.
    #[allow(clippy::too_many_arguments)]
    async fn insert_events(
        conn: &mut SqliteConnection,
        domain: &str,
        edition: &str,
        root_str: &str,
        events: Vec<EventPage>,
        correlation_id: &str,
        external_id: &str,
        source_info: Option<&SourceInfo>,
    ) -> Result<(u32, u32)> {
        let base_sequence = {
            let query = Query::select()
                .expr(Expr::col(Events::Sequence).max())
                .from(Events::Table)
                .and_where(edition_predicate(Events::Edition, edition))
                .and_where(Expr::col(Events::Domain).eq(domain))
                .and_where(Expr::col(Events::Root).eq(root_str))
                .to_string(SqliteQueryBuilder);

            let row = sqlx::query(&query).fetch_optional(&mut *conn).await?;

            match row {
                Some(row) => {
                    let max_seq: Option<i32> = row.get(0);
                    max_seq.map(|s| s as u32 + 1).unwrap_or(0)
                }
                None => 0,
            }
        };

        let mut auto_sequence = base_sequence;
        let mut first_sequence = None;
        let mut last_sequence = 0u32;

        // Prepare source info values (empty strings for None)
        let source_edition = source_info.map(|s| s.edition.as_str()).unwrap_or("");
        let source_domain = source_info.map(|s| s.domain.as_str()).unwrap_or("");
        let source_root = source_info.map(|s| s.root.to_string()).unwrap_or_default();
        let source_seq = source_info.map(|s| s.seq as i32);

        for event in events {
            let event_data = event.encode_to_vec();
            let sequence = crate::storage::helpers::resolve_sequence(
                &event,
                base_sequence,
                &mut auto_sequence,
            )?;
            let created_at = crate::storage::helpers::parse_timestamp(&event)?;

            // Extract cascade tracking fields from EventPage
            let committed = !event.no_commit;
            let cascade_id = event.cascade_id.clone();

            if first_sequence.is_none() {
                first_sequence = Some(sequence);
            }
            last_sequence = sequence;

            // C-15: edition + source_edition normalize to SQL NULL when the
            // caller passes a main-timeline sentinel. Splitting storage
            // between NULL/`""`/`"angzarr"` is the polarity bug this fix
            // closes; treat the value as a sea_query Value, not a `&str`.
            let edition_value = edition_to_db(edition);
            let source_edition_value = edition_to_db(source_edition);

            let query = if source_info.is_some() && !source_edition.is_empty() {
                Query::insert()
                    .into_table(Events::Table)
                    .columns([
                        Events::Edition,
                        Events::Domain,
                        Events::Root,
                        Events::Sequence,
                        Events::CreatedAt,
                        Events::EventData,
                        Events::CorrelationId,
                        Events::ExternalId,
                        Events::SourceEdition,
                        Events::SourceDomain,
                        Events::SourceRoot,
                        Events::SourceSeq,
                        Events::Committed,
                        Events::CascadeId,
                    ])
                    .values_panic([
                        edition_value.clone(),
                        domain.into(),
                        root_str.to_string().into(),
                        sequence.into(),
                        created_at.into(),
                        event_data.into(),
                        correlation_id.into(),
                        external_id.into(),
                        source_edition_value.clone(),
                        source_domain.into(),
                        source_root.clone().into(),
                        source_seq.into(),
                        committed.into(),
                        cascade_id.clone().into(),
                    ])
                    .to_string(SqliteQueryBuilder)
            } else {
                Query::insert()
                    .into_table(Events::Table)
                    .columns([
                        Events::Edition,
                        Events::Domain,
                        Events::Root,
                        Events::Sequence,
                        Events::CreatedAt,
                        Events::EventData,
                        Events::CorrelationId,
                        Events::ExternalId,
                        Events::Committed,
                        Events::CascadeId,
                    ])
                    .values_panic([
                        edition_value.clone(),
                        domain.into(),
                        root_str.to_string().into(),
                        sequence.into(),
                        created_at.into(),
                        event_data.into(),
                        correlation_id.into(),
                        external_id.into(),
                        committed.into(),
                        cascade_id.into(),
                    ])
                    .to_string(SqliteQueryBuilder)
            };

            sqlx::query(&query).execute(&mut *conn).await?;
        }

        Ok((first_sequence.unwrap_or(0), last_sequence))
    }

    /// Check if events with the given external_id already exist.
    /// Returns Some((first_sequence, last_sequence)) if found.
    async fn check_idempotency(
        conn: &mut SqliteConnection,
        domain: &str,
        edition: &str,
        root_str: &str,
        external_id: &str,
    ) -> Result<Option<(u32, u32)>> {
        let query = Query::select()
            .expr(Expr::col(Events::Sequence).min())
            .expr(Expr::col(Events::Sequence).max())
            .from(Events::Table)
            .and_where(edition_predicate(Events::Edition, edition))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(root_str))
            .and_where(Expr::col(Events::ExternalId).eq(external_id))
            .to_string(SqliteQueryBuilder);

        let row = sqlx::query(&query).fetch_optional(&mut *conn).await?;

        match row {
            Some(row) => {
                let min_seq: Option<i32> = row.get(0);
                let max_seq: Option<i32> = row.get(1);
                match (min_seq, max_seq) {
                    (Some(min), Some(max)) => Ok(Some((min as u32, max as u32))),
                    _ => Ok(None),
                }
            }
            None => Ok(None),
        }
    }
}

#[async_trait]
impl EventStore for SqliteEventStore {
    async fn add(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        events: Vec<EventPage>,
        correlation_id: &str,
        external_id: Option<&str>,
        source_info: Option<&SourceInfo>,
    ) -> Result<AddOutcome> {
        if events.is_empty() {
            return Ok(AddOutcome::Added {
                first_sequence: 0,
                last_sequence: 0,
            });
        }

        let root_str = root.to_string();
        let external_id = external_id.unwrap_or("");

        // BEGIN IMMEDIATE acquires the write lock upfront, preventing deadlocks
        // when concurrent DEFERRED transactions race to upgrade from shared to exclusive.
        let mut conn = self.pool.acquire().await?;
        sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;

        // Check for idempotency if external_id is provided
        if !external_id.is_empty() {
            if let Some((first, last)) =
                Self::check_idempotency(&mut conn, domain, edition, &root_str, external_id).await?
            {
                sqlx::query("COMMIT").execute(&mut *conn).await?;
                return Ok(AddOutcome::Duplicate {
                    first_sequence: first,
                    last_sequence: last,
                });
            }
        }

        let result = Self::insert_events(
            &mut conn,
            domain,
            edition,
            &root_str,
            events,
            correlation_id,
            external_id,
            source_info,
        )
        .await;

        match result {
            Ok((first, last)) => {
                sqlx::query("COMMIT").execute(&mut *conn).await?;
                Ok(AddOutcome::Added {
                    first_sequence: first,
                    last_sequence: last,
                })
            }
            Err(e) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                Err(e)
            }
        }
    }

    async fn get(&self, domain: &str, edition: &str, root: Uuid) -> Result<Vec<EventPage>> {
        self.get_from(domain, edition, root, 0).await
    }

    async fn get_with_divergence(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        explicit_divergence: Option<u32>,
    ) -> Result<Vec<EventPage>> {
        let root_str = root.to_string();

        // Main timeline: simple query, explicit divergence doesn't apply
        if is_main_timeline(edition) {
            return self
                .query_edition_events(domain, DEFAULT_EDITION, &root_str, 0)
                .await;
        }

        // Named edition: use composite read with explicit divergence
        self.composite_read_with_divergence(domain, edition, &root_str, 0, explicit_divergence)
            .await
    }

    async fn get_from(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        let root_str = root.to_string();

        // Main timeline: simple query
        if is_main_timeline(edition) {
            return self
                .query_edition_events(domain, DEFAULT_EDITION, &root_str, from)
                .await;
        }

        // Named edition: composite read (main timeline up to divergence + edition events)
        self.composite_read(domain, edition, &root_str, from).await
    }

    async fn get_from_to(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
        to: u32,
    ) -> Result<Vec<EventPage>> {
        let root_str = root.to_string();

        let query = Query::select()
            .column(Events::EventData)
            .from(Events::Table)
            .and_where(edition_predicate(Events::Edition, edition))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(&root_str))
            .and_where(Expr::col(Events::Sequence).gte(from))
            .and_where(Expr::col(Events::Sequence).lt(to))
            .order_by(Events::Sequence, Order::Asc)
            .to_string(SqliteQueryBuilder);

        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let event_data: Vec<u8> = row.get("event_data");
            let event = EventPage::decode(event_data.as_slice())?;
            events.push(event);
        }

        Ok(events)
    }

    async fn get_until_timestamp(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        until: &str,
    ) -> Result<Vec<EventPage>> {
        let root_str = root.to_string();

        let query = Query::select()
            .column(Events::EventData)
            .from(Events::Table)
            .and_where(edition_predicate(Events::Edition, edition))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(&root_str))
            .and_where(Expr::col(Events::CreatedAt).lte(until))
            .order_by(Events::Sequence, Order::Asc)
            .to_string(SqliteQueryBuilder);

        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let event_data: Vec<u8> = row.get("event_data");
            let event = EventPage::decode(event_data.as_slice())?;
            events.push(event);
        }

        Ok(events)
    }

    async fn list_roots(&self, domain: &str, edition: &str) -> Result<Vec<Uuid>> {
        let query = Query::select()
            .distinct()
            .column(Events::Root)
            .from(Events::Table)
            .and_where(edition_predicate(Events::Edition, edition))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .to_string(SqliteQueryBuilder);

        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        let mut roots = Vec::with_capacity(rows.len());
        for row in rows {
            let root_str: String = row.get("root");
            let root = Uuid::parse_str(&root_str)?;
            roots.push(root);
        }

        Ok(roots)
    }

    async fn list_domains(&self) -> Result<Vec<String>> {
        let query = Query::select()
            .distinct()
            .column(Events::Domain)
            .from(Events::Table)
            .to_string(SqliteQueryBuilder);

        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        let domains = rows.iter().map(|row| row.get("domain")).collect();

        Ok(domains)
    }

    async fn get_next_sequence(&self, domain: &str, edition: &str, root: Uuid) -> Result<u32> {
        let root_str = root.to_string();

        // For non-default editions with implicit divergence, we need composite logic:
        // If the edition has no events yet, use the main timeline's max sequence
        if !is_main_timeline(edition) {
            let edition_query = Query::select()
                .expr(Expr::col(Events::Sequence).max())
                .from(Events::Table)
                .and_where(edition_predicate(Events::Edition, edition))
                .and_where(Expr::col(Events::Domain).eq(domain))
                .and_where(Expr::col(Events::Root).eq(&root_str))
                .to_string(SqliteQueryBuilder);

            let edition_row = sqlx::query(&edition_query)
                .fetch_optional(&self.pool)
                .await?;

            if let Some(row) = edition_row {
                let max_seq: Option<i32> = row.get(0);
                if let Some(seq) = max_seq {
                    // Edition has events, use edition's max sequence
                    return Ok(seq as u32 + 1);
                }
            }

            // No edition events - fall through to check main timeline
        }

        // Query the target edition (or main timeline for fallback). The
        // main timeline is stored as SQL NULL (C-15) regardless of which
        // sentinel form the caller passed; `edition_predicate` handles the
        // mapping uniformly.
        let target_edition = if is_main_timeline(edition) {
            edition
        } else {
            DEFAULT_EDITION
        };

        let query = Query::select()
            .expr(Expr::col(Events::Sequence).max())
            .from(Events::Table)
            .and_where(edition_predicate(Events::Edition, target_edition))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(&root_str))
            .to_string(SqliteQueryBuilder);

        let row = sqlx::query(&query).fetch_optional(&self.pool).await?;

        match row {
            Some(row) => {
                let max_seq: Option<i32> = row.get(0);
                Ok(max_seq.map(|s| s as u32 + 1).unwrap_or(0))
            }
            None => Ok(0),
        }
    }

    async fn get_by_correlation(
        &self,
        correlation_id: &str,
    ) -> Result<Vec<crate::proto::EventBook>> {
        use std::collections::HashMap;

        if correlation_id.is_empty() {
            return Ok(vec![]);
        }

        let query = Query::select()
            .columns([
                Events::Domain,
                Events::Edition,
                Events::Root,
                Events::EventData,
                Events::Sequence,
            ])
            .from(Events::Table)
            .and_where(Expr::col(Events::CorrelationId).eq(correlation_id))
            .order_by(Events::Domain, Order::Asc)
            .order_by(Events::Root, Order::Asc)
            .order_by(Events::Sequence, Order::Asc)
            .to_string(SqliteQueryBuilder);

        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        let mut books_map: HashMap<(String, String, Uuid), Vec<EventPage>> = HashMap::new();

        for row in rows {
            let domain: String = row.get("domain");
            // C-15: the SQLite `edition` column is nullable since
            // migration 0006. Decode as `Option<String>` and normalize NULL
            // back to the empty-string sentinel; a bare `String` decode
            // would panic with `UnexpectedNullError` on any main-timeline
            // row written or migration-backfilled after 0006.
            let edition: String = edition_from_db(row.get("edition"));
            let root_str: String = row.get("root");
            let event_data: Vec<u8> = row.get("event_data");

            let root = Uuid::parse_str(&root_str)?;
            let event = EventPage::decode(event_data.as_slice())?;

            books_map
                .entry((domain, edition, root))
                .or_default()
                .push(event);
        }

        Ok(assemble_event_books(books_map, correlation_id))
    }

    async fn find_by_source(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        source_info: &SourceInfo,
    ) -> Result<Option<Vec<EventPage>>> {
        if source_info.is_empty() {
            return Ok(None);
        }

        let root_str = root.to_string();
        let source_root_str = source_info.root.to_string();

        let query = Query::select()
            .column(Events::EventData)
            .from(Events::Table)
            .and_where(edition_predicate(Events::Edition, edition))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(&root_str))
            .and_where(edition_predicate(
                Events::SourceEdition,
                &source_info.edition,
            ))
            .and_where(Expr::col(Events::SourceDomain).eq(&source_info.domain))
            .and_where(Expr::col(Events::SourceRoot).eq(&source_root_str))
            .and_where(Expr::col(Events::SourceSeq).eq(source_info.seq as i32))
            .order_by(Events::Sequence, Order::Asc)
            .to_string(SqliteQueryBuilder);

        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let event_data: Vec<u8> = row.get("event_data");
            let event = EventPage::decode(event_data.as_slice())?;
            events.push(event);
        }

        Ok(Some(events))
    }

    async fn find_by_external_id(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        external_id: &str,
    ) -> Result<Option<Vec<EventPage>>> {
        if external_id.is_empty() {
            return Ok(None);
        }

        let root_str = root.to_string();
        let query = Query::select()
            .column(Events::EventData)
            .from(Events::Table)
            .and_where(edition_predicate(Events::Edition, edition))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(&root_str))
            .and_where(Expr::col(Events::ExternalId).eq(external_id))
            .order_by(Events::Sequence, Order::Asc)
            .to_string(SqliteQueryBuilder);

        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;
        if rows.is_empty() {
            return Ok(None);
        }

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let event_data: Vec<u8> = row.get("event_data");
            events.push(EventPage::decode(event_data.as_slice())?);
        }
        Ok(Some(events))
    }

    async fn delete_edition_events(&self, domain: &str, edition: &str) -> Result<u32> {
        // C-15: main-timeline sentinels (`""` and `"angzarr"`) refer to the
        // append-only canonical timeline. SQLite has no equivalent of the
        // Postgres stored proc, so the guard lives in the Rust impl. Without
        // it a caller passing `"angzarr"` (or `""`) would silently delete
        // every main-timeline event for that domain.
        if is_main_timeline(edition) {
            return Err(StorageError::MainTimelineProtected(format!(
                "delete_edition_events(edition={:?}) refused; the main \
                 timeline is append-only",
                edition
            )));
        }

        let query = Query::delete()
            .from_table(Events::Table)
            .and_where(edition_predicate(Events::Edition, edition))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .to_string(SqliteQueryBuilder);

        let result = sqlx::query(&query).execute(&self.pool).await?;
        Ok(result.rows_affected() as u32)
    }

    async fn query_stale_cascades(&self, threshold: &str) -> Result<Vec<String>> {
        // Per-participant resolution (C-02): a cascade is stale iff it has
        // at least one (cascade_id, domain, edition, root) participant that
        // is past the threshold AND has no committed cascade row on that
        // SAME (domain, edition, root) for the same cascade_id.
        //
        // Pre-fix semantics filtered out the entire cascade when ANY
        // committed row existed for that cascade_id (globally) — once
        // participant 1 of N was revoked, participants 2..N were stranded
        // because the cascade was treated as "already resolved" even though
        // their no_commit rows were still live.
        //
        // SQL: NOT EXISTS (SELECT 1 FROM events c WHERE c.cascade_id = stale.cascade_id
        //                  AND c.domain = stale.domain AND c.edition = stale.edition
        //                  AND c.root = stale.root AND c.committed = true)
        //
        // sea-query lacks correlated-subquery sugar — fall back to raw SQL.
        //
        // C-15: `c.edition IS s.edition` (SQLite's NULL-aware equality) so
        // main-timeline rows (where both sides are NULL) match correctly.
        // A plain `=` would yield NULL there and exclude the row.
        let raw = "SELECT DISTINCT s.cascade_id \
             FROM events s \
             WHERE s.committed = false \
               AND s.cascade_id IS NOT NULL \
               AND s.created_at < ? \
               AND NOT EXISTS ( \
                 SELECT 1 FROM events c \
                 WHERE c.committed = true \
                   AND c.cascade_id = s.cascade_id \
                   AND c.domain = s.domain \
                   AND c.edition IS s.edition \
                   AND c.root = s.root \
               )";
        let rows = sqlx::query(raw)
            .bind(threshold)
            .fetch_all(&self.pool)
            .await?;

        let mut cascade_ids = Vec::with_capacity(rows.len());
        for row in rows {
            let cascade_id: String = row.get("cascade_id");
            cascade_ids.push(cascade_id);
        }

        Ok(cascade_ids)
    }

    async fn query_cascade_participants(
        &self,
        cascade_id: &str,
    ) -> Result<Vec<CascadeParticipant>> {
        use std::collections::HashMap;

        // Per-participant resolution (C-02): exclude (domain, edition, root)
        // participants that already have a committed cascade row (a
        // Revocation or Confirmation) for this cascade_id. Without this
        // filter, the reaper would re-write Revocations on every cycle for
        // already-resolved participants.
        //
        // C-15: `c.edition IS s.edition` for NULL-aware equality on the
        // main timeline (SQLite stores `""` / `"angzarr"` as SQL NULL).
        let raw = "SELECT s.domain, s.edition, s.root, s.sequence \
             FROM events s \
             WHERE s.cascade_id = ? \
               AND s.committed = false \
               AND NOT EXISTS ( \
                 SELECT 1 FROM events c \
                 WHERE c.committed = true \
                   AND c.cascade_id = s.cascade_id \
                   AND c.domain = s.domain \
                   AND c.edition IS s.edition \
                   AND c.root = s.root \
               ) \
             ORDER BY s.domain ASC, s.root ASC, s.sequence ASC";

        let rows = sqlx::query(raw)
            .bind(cascade_id)
            .fetch_all(&self.pool)
            .await?;

        // Group by (domain, edition, root)
        let mut participants_map: HashMap<(String, String, Uuid), Vec<u32>> = HashMap::new();

        for row in rows {
            let domain: String = row.get("domain");
            // C-15: SQLite stores main-timeline rows as SQL NULL since
            // migration 0006; decode as Option<String> and surface back
            // as the empty-string sentinel at the API boundary.
            let edition_raw: Option<String> = row.get("edition");
            let edition = edition_from_db(edition_raw);
            let root_str: String = row.get("root");
            let sequence: i32 = row.get("sequence");

            let root = Uuid::parse_str(&root_str)?;
            let key = (domain, edition, root);

            participants_map
                .entry(key)
                .or_default()
                .push(sequence as u32);
        }

        // Convert to CascadeParticipant list
        let participants: Vec<CascadeParticipant> = participants_map
            .into_iter()
            .map(|((domain, edition, root), sequences)| CascadeParticipant {
                domain,
                edition,
                root,
                sequences,
            })
            .collect();

        Ok(participants)
    }
}
