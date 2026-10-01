//! SQLite EventStore implementation.
//!
//! Implements composite reads for editions: query edition events first to derive
//! the implicit divergence point, then query main timeline up to that point,
//! then merge the results. The divergence resolution and main+edition merge
//! are shared logic — see `crate::storage::sql::event_store` (finding #28).
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
use sea_query::{Expr, Order, Query, SqliteQueryBuilder};
use sqlx::{Row, SqliteConnection, SqlitePool};
use uuid::Uuid;

use crate::orchestration::aggregate::DEFAULT_EDITION;
use crate::proto::EventPage;
use crate::storage::helpers::{assemble_event_books, event_sequence, is_main_timeline, BookParts};
use crate::storage::schema::Events;
use crate::storage::sql::event_store::{
    edition_from_db, edition_predicate_expr as edition_predicate,
    edition_to_db_value as edition_to_db, implicit_divergence, map_write_conflict,
    merge_composite_events, resolve_divergence,
};
use crate::storage::timeline::{validate_append, AppendWindow, MAIN_TIMELINE_STORAGE_EDITION};
use crate::storage::{AddMeta, AddOutcome, EventStore, Result, SourceInfo, StorageError};

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

    /// Query main timeline events up to (but not including) a sequence
    /// number, or the ENTIRE main timeline when `until_seq` is `None`.
    ///
    /// `None` is the #12 eventless-edition case: `resolve_divergence`
    /// returns `None` when there's no explicit divergence and the edition
    /// has no events of its own yet, meaning "not diverged — inherit the
    /// whole main timeline". Omitting the upper-bound clause (rather than
    /// substituting a sentinel like `u32::MAX`, which doesn't fit the
    /// column's `i32` representation) is what makes that inheritance work.
    async fn query_main_events_until(
        &self,
        domain: &str,
        root_str: &str,
        until_seq: Option<u32>,
    ) -> Result<Vec<EventPage>> {
        // Main-timeline rows store edition as SQL NULL (C-15).
        let mut stmt = Query::select()
            .column(Events::EventData)
            .from(Events::Table)
            .and_where(edition_predicate(Events::Edition, DEFAULT_EDITION))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(root_str))
            .order_by(Events::Sequence, Order::Asc)
            .to_owned();
        if let Some(seq) = until_seq {
            stmt.and_where(Expr::col(Events::Sequence).lt(seq));
        }
        let query = stmt.to_string(SqliteQueryBuilder);

        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let event_data: Vec<u8> = row.get("event_data");
            let event = EventPage::decode(event_data.as_slice())?;
            events.push(event);
        }

        Ok(events)
    }

    /// Fetch the raw ingredients of a composite (main + edition) read: the
    /// full edition-event set and the main-timeline events up to the
    /// resolved divergence point (or the whole main timeline when there is
    /// none — see `query_main_events_until`). Callers merge these with
    /// whatever `keep` predicate their read shape needs (`get`/`get_from`
    /// filter on a sequence lower bound; `get_from_to` on a range;
    /// `get_until_timestamp` on `created_at`) via
    /// `sql::event_store::merge_composite_events` — see that module for
    /// why this single extraction point closes finding #10.
    async fn composite_parts(
        &self,
        domain: &str,
        edition: &str,
        root_str: &str,
        explicit_divergence: Option<u32>,
    ) -> Result<(Vec<EventPage>, Vec<EventPage>)> {
        let edition_events = self
            .query_edition_events(domain, edition, root_str, 0)
            .await?;
        let divergence =
            resolve_divergence(explicit_divergence, implicit_divergence(&edition_events));
        let main_events = self
            .query_main_events_until(domain, root_str, divergence)
            .await?;
        Ok((main_events, edition_events))
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
        let (main_events, edition_events) = self
            .composite_parts(domain, edition, root_str, explicit_divergence)
            .await?;
        Ok(merge_composite_events(main_events, edition_events, |e| {
            event_sequence(e) >= from
        }))
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
        ext: Option<&prost_types::Any>,
    ) -> Result<(u32, u32)> {
        // Parent-routing cover, serialized once and replicated per row (mirrors
        // correlation_id). All pages of this write share the same value.
        let ext_bytes: Option<Vec<u8>> = ext.map(prost::Message::encode_to_vec);
        let stream_next = Self::max_sequence(conn, domain, edition, root_str)
            .await?
            .map(|max| max + 1);
        let main_next = if stream_next.is_none() && !is_main_timeline(edition) {
            Self::max_sequence(conn, domain, MAIN_TIMELINE_STORAGE_EDITION, root_str)
                .await?
                .map_or(0, |max| max + 1)
        } else {
            stream_next.unwrap_or(0)
        };
        let window = AppendWindow::for_edition(edition, stream_next, main_next);
        let (first_sequence, last_sequence) = validate_append(window, &events)?;

        // Prepare source info values (empty strings for None)
        let source_edition = source_info.map(|s| s.edition.as_str()).unwrap_or("");
        let source_domain = source_info.map(|s| s.domain.as_str()).unwrap_or("");
        let source_root = source_info.map(|s| s.root.to_string()).unwrap_or_default();
        let source_seq = source_info.map(|s| s.seq as i32);
        let source_component = source_info.map(|s| s.component.as_str()).unwrap_or("");
        let source_command_index = source_info.map(|s| s.command_index as i32).unwrap_or(0);

        for event in events {
            let event_data = event.encode_to_vec();
            let sequence = event_sequence(&event);
            let created_at = crate::storage::helpers::parse_timestamp(&event)?;

            // C-15: edition + source_edition normalize to SQL NULL when the
            // caller passes a main-timeline sentinel. Splitting storage
            // between NULL/`""`/`"angzarr"` is the polarity bug this fix
            // closes; treat the value as a sea_query Value, not a `&str`.
            let edition_value = edition_to_db(edition);
            let source_edition_value = edition_to_db(source_edition);

            // Persist the source claim whenever SourceInfo is non-empty —
            // gated on the same `is_empty()` the read side uses (and the
            // Postgres write side mirrors). The previous gate
            // (`!source_edition.is_empty()`) was C-15-class polarity on the
            // write side: main-timeline source covers spell the edition ""
            // so every main-timeline deferred-idempotency claim was silently
            // dropped, and saga-command redeliveries re-executed instead of
            // hitting the cache.
            let query = if source_info.is_some_and(|s| !s.is_empty()) {
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
                        Events::SourceComponent,
                        Events::SourceCommandIndex,
                        Events::Ext,
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
                        source_component.into(),
                        source_command_index.into(),
                        ext_bytes.clone().into(),
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
                        Events::Ext,
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
                        ext_bytes.clone().into(),
                    ])
                    .to_string(SqliteQueryBuilder)
            };

            // #20: a colliding INSERT (concurrent writer that claimed this
            // sequence first, or an in-batch duplicate sequence) trips the
            // PRIMARY KEY on (domain, edition, root, sequence). Classify it
            // as a `SequenceConflict` (retryable → `failed_precondition`)
            // via the shared classifier rather than letting it fall through
            // the blanket `From<sqlx::Error>` as `Database` (non-retryable →
            // `internal`). SQLite's `BEGIN IMMEDIATE` serializes writers so
            // this is rare across transactions, but in-batch duplicates and
            // the same classifier keep it consistent with Postgres/immudb.
            sqlx::query(&query)
                .execute(&mut *conn)
                .await
                .map_err(|e| map_write_conflict(e, window.max_first, sequence))?;
        }

        Ok((first_sequence, last_sequence))
    }

    /// Highest sequence stored for `edition` (`None` when it has no events),
    /// read inside the caller's transaction.
    async fn max_sequence(
        conn: &mut SqliteConnection,
        domain: &str,
        edition: &str,
        root_str: &str,
    ) -> Result<Option<u32>> {
        let query = Query::select()
            .expr(Expr::col(Events::Sequence).max())
            .from(Events::Table)
            .and_where(edition_predicate(Events::Edition, edition))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(root_str))
            .to_string(SqliteQueryBuilder);
        let row = sqlx::query(&query).fetch_optional(&mut *conn).await?;
        Ok(row
            .and_then(|row| row.get::<Option<i32>, _>(0))
            .map(|max| max as u32))
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
        meta: &AddMeta<'_>,
    ) -> Result<AddOutcome> {
        let correlation_id = meta.correlation_id;
        let external_id = meta.external_id;
        let source_info = meta.source_info;
        if events.is_empty() {
            return Ok(AddOutcome::Added {
                first_sequence: 0,
                last_sequence: 0,
            });
        }

        let root_str = root.to_string();
        let external_id = external_id.unwrap_or("");

        // BEGIN IMMEDIATE takes the write lock up front, so a concurrent
        // writer reads the committed max and loses with SequenceConflict
        // instead of racing a DEFERRED shared-to-exclusive upgrade. The
        // transaction guard rolls back on every early return (including `?`)
        // before the connection goes back to the pool.
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        if !external_id.is_empty() {
            if let Some((first, last)) =
                Self::check_idempotency(&mut tx, domain, edition, &root_str, external_id).await?
            {
                tx.commit().await?;
                return Ok(AddOutcome::Duplicate {
                    first_sequence: first,
                    last_sequence: last,
                });
            }
        }

        let (first, last) = Self::insert_events(
            &mut tx,
            domain,
            edition,
            &root_str,
            events,
            correlation_id,
            external_id,
            source_info,
            meta.ext,
        )
        .await?;
        tx.commit().await?;

        Ok(AddOutcome::Added {
            first_sequence: first,
            last_sequence: last,
        })
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

        // Main timeline: a single edition-scoped range query is exact —
        // there is no other timeline to merge in.
        if is_main_timeline(edition) {
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

            return Ok(events);
        }

        // Named edition: route through the SAME composite (main-prefix +
        // edition) logic as `get`/`get_from` (finding #10). The pre-fix
        // path filtered only on `edition_predicate`, so a ranged read on a
        // diverged edition silently dropped every pre-divergence
        // main-timeline event in the range.
        let (main_events, edition_events) = self
            .composite_parts(domain, edition, &root_str, None)
            .await?;
        Ok(merge_composite_events(main_events, edition_events, |e| {
            let seq = event_sequence(e);
            seq >= from && seq < to
        }))
    }

    async fn get_until_timestamp(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        until: &prost_types::Timestamp,
    ) -> Result<Vec<EventPage>> {
        let root_str = root.to_string();

        // Main timeline: filter the single timeline at the SQL layer.
        //
        // C10: `created_at` is stored as TEXT, compared lexically. The typed
        // `until` is canonicalized through the SAME function
        // (`timestamp_to_rfc3339`) that produces the stored `created_at`
        // value on write (`storage::helpers::parse_timestamp` delegates to
        // it) — one shared boundary, so the two can never drift on
        // suffix/precision and the lexical comparison stays equivalent to
        // chronological order (see the property tests on
        // `timestamp_to_rfc3339` in `storage/helpers/tests.rs`).
        if is_main_timeline(edition) {
            let until_str = crate::storage::helpers::timestamp_to_rfc3339(until)?;

            let query = Query::select()
                .column(Events::EventData)
                .from(Events::Table)
                .and_where(edition_predicate(Events::Edition, edition))
                .and_where(Expr::col(Events::Domain).eq(domain))
                .and_where(Expr::col(Events::Root).eq(&root_str))
                .and_where(Expr::col(Events::CreatedAt).lte(until_str))
                .order_by(Events::Sequence, Order::Asc)
                .to_string(SqliteQueryBuilder);

            let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

            let mut events = Vec::with_capacity(rows.len());
            for row in rows {
                let event_data: Vec<u8> = row.get("event_data");
                let event = EventPage::decode(event_data.as_slice())?;
                events.push(event);
            }

            return Ok(events);
        }

        // Named edition: composite (main-prefix + edition) read, then apply
        // the temporal cut to BOTH halves (finding #10). The pre-fix path
        // filtered only on `edition_predicate`, so a "state as of T" read on
        // a diverged edition — the exact path `get_temporal_by_sequence`'s
        // full-replay branch takes — dropped the pre-divergence main-timeline
        // prefix and reconstructed corrupt state.
        //
        // The temporal cut is applied in-memory against the decoded typed
        // `created_at` (nanosecond-exact chrono compare, matching the mock)
        // rather than re-deriving a comparable string per row; this cannot
        // reopen the C10 lexical footgun because there is no string form
        // involved.
        let until_dt = chrono::DateTime::from_timestamp(until.seconds, until.nanos as u32).ok_or(
            StorageError::InvalidTimestamp {
                seconds: until.seconds,
                nanos: until.nanos,
            },
        )?;
        let (main_events, edition_events) = self
            .composite_parts(domain, edition, &root_str, None)
            .await?;
        Ok(merge_composite_events(
            main_events,
            edition_events,
            |e| match &e.created_at {
                Some(ts) => chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                    .map(|dt| dt <= until_dt)
                    .unwrap_or(false),
                None => false,
            },
        ))
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
                Events::Ext,
            ])
            .from(Events::Table)
            .and_where(Expr::col(Events::CorrelationId).eq(correlation_id))
            .order_by(Events::Domain, Order::Asc)
            .order_by(Events::Root, Order::Asc)
            .order_by(Events::Sequence, Order::Asc)
            .to_string(SqliteQueryBuilder);

        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        let mut books_map: HashMap<(String, String, Uuid), BookParts> = HashMap::new();

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
            let ext_bytes: Option<Vec<u8>> = row.get("ext");

            let root = Uuid::parse_str(&root_str)?;
            let event = EventPage::decode(event_data.as_slice())?;

            let entry = books_map.entry((domain, edition, root)).or_default();
            entry.pages.push(event);
            if entry.ext.is_none() {
                if let Some(bytes) = ext_bytes {
                    entry.ext = Some(prost_types::Any::decode(bytes.as_slice())?);
                }
            }
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
            .and_where(Expr::col(Events::SourceComponent).eq(&source_info.component))
            .and_where(Expr::col(Events::SourceCommandIndex).eq(source_info.command_index as i32))
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
}
