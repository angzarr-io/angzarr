//! PostgreSQL EventStore implementation.
//!
//! Composite edition reads (main-timeline prefix + edition events) run
//! through the SHARED divergence/merge logic in
//! `crate::storage::sql::event_store` (finding #28) — the same code SQLite
//! and immudb use — rather than a Postgres-only stored procedure. This
//! removes the second, drift-prone copy of the divergence math the stored
//! procedure held (which computed the eventless-edition divergence point as
//! the literal `0`, silently returning zero rows — finding #12). The read
//! stored procedures remain defined by migrations for backward
//! compatibility but are no longer on the read path; `delete_edition_events`
//! still uses its proc for the DB-side main-timeline guard.

use async_trait::async_trait;
use prost::Message;
use sea_query::{Expr, Order, PostgresQueryBuilder, Query};
use sqlx::{Acquire, PgPool, Row};
use uuid::Uuid;

use crate::proto::EventPage;
use crate::storage::helpers::{assemble_event_books, event_sequence, is_main_timeline, BookParts};
use crate::storage::schema::Events;
use crate::storage::sql::event_store::{
    edition_from_db, edition_predicate_expr as edition_predicate, implicit_divergence,
    map_write_conflict, merge_composite_events, resolve_divergence,
};
use crate::storage::timeline::{validate_append, AppendWindow};
use crate::storage::{AddMeta, AddOutcome, EventStore, Result, SourceInfo, StorageError};

/// Convert the API-layer edition to the storage-layer `Option<String>`
/// (`None` = SQL NULL). Thin wrapper over the shared
/// [`edition_to_db_value`] so the Postgres write path — which binds
/// `Option<String>` for the source-edition columns and needs the value
/// twice — keeps a convenient owned form. Both main-timeline sentinels
/// (`""`, `"angzarr"`) normalize to `None` (C-15).
fn edition_to_db(edition: &str) -> Option<String> {
    if is_main_timeline(edition) {
        None
    } else {
        Some(edition.to_string())
    }
}

/// PostgreSQL implementation of EventStore.
pub struct PostgresEventStore {
    pool: PgPool,
}

impl PostgresEventStore {
    /// Create a new PostgreSQL event store.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Query the events of a single edition (`edition` matched via the
    /// C-15 NULL-polarity predicate), from `from` onward.
    async fn query_edition_events(
        &self,
        domain: &str,
        edition: &str,
        root: &str,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        let query = Query::select()
            .column(Events::EventData)
            .from(Events::Table)
            .and_where(edition_predicate(Events::Edition, edition))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(root))
            .and_where(Expr::col(Events::Sequence).gte(from))
            .order_by(Events::Sequence, Order::Asc)
            .to_string(PostgresQueryBuilder);

        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let event_data: Vec<u8> = row.get("event_data");
            let event = EventPage::decode(event_data.as_slice())?;
            events.push(event);
        }

        Ok(events)
    }

    /// Main-timeline events up to (exclusive) `until_seq`, or the entire
    /// main timeline when `until_seq` is `None` (the #12 eventless-edition
    /// "no cap — inherit whole main timeline" case).
    async fn query_main_events_until(
        &self,
        domain: &str,
        root: &str,
        until_seq: Option<u32>,
    ) -> Result<Vec<EventPage>> {
        let mut stmt = Query::select()
            .column(Events::EventData)
            .from(Events::Table)
            .and_where(edition_predicate(Events::Edition, ""))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(root))
            .order_by(Events::Sequence, Order::Asc)
            .to_owned();
        if let Some(seq) = until_seq {
            stmt.and_where(Expr::col(Events::Sequence).lt(seq));
        }
        let query = stmt.to_string(PostgresQueryBuilder);

        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let event_data: Vec<u8> = row.get("event_data");
            let event = EventPage::decode(event_data.as_slice())?;
            events.push(event);
        }

        Ok(events)
    }

    /// Fetch the raw halves of a composite read (main-timeline prefix +
    /// edition events) for a NAMED edition, using the shared divergence
    /// resolution. Callers merge with their own `keep` predicate via
    /// [`merge_composite_events`] — see the shared module for why one merge
    /// point per backend closes findings #10/#12.
    async fn composite_parts(
        &self,
        domain: &str,
        edition: &str,
        root: &str,
        explicit_divergence: Option<u32>,
    ) -> Result<(Vec<EventPage>, Vec<EventPage>)> {
        let edition_events = self.query_edition_events(domain, edition, root, 0).await?;
        let divergence =
            resolve_divergence(explicit_divergence, implicit_divergence(&edition_events));
        let main_events = self
            .query_main_events_until(domain, root, divergence)
            .await?;
        Ok((main_events, edition_events))
    }

    /// Composite read from `from` onward (implicit divergence).
    async fn composite_read(
        &self,
        domain: &str,
        edition: &str,
        root: &str,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        self.composite_read_with_divergence(domain, edition, root, from, None)
            .await
    }

    /// Composite read with an optional explicit divergence point.
    async fn composite_read_with_divergence(
        &self,
        domain: &str,
        edition: &str,
        root: &str,
        from: u32,
        explicit_divergence: Option<u32>,
    ) -> Result<Vec<EventPage>> {
        let (main_events, edition_events) = self
            .composite_parts(domain, edition, root, explicit_divergence)
            .await?;
        Ok(merge_composite_events(main_events, edition_events, |e| {
            event_sequence(e) >= from
        }))
    }

    /// Highest sequence stored for `edition` (`None` when it has no events),
    /// read inside the caller's transaction.
    async fn max_sequence(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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
            .to_string(PostgresQueryBuilder);
        let row = sqlx::query(&query).fetch_optional(&mut **tx).await?;
        Ok(row
            .and_then(|row| row.get::<Option<i32>, _>(0))
            .map(|max| max as u32))
    }

    /// Simple query for main timeline events (no composite logic needed).
    async fn query_main_timeline(
        &self,
        domain: &str,
        root: &str,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        self.query_edition_events(domain, "", root, from).await
    }
}

#[async_trait]
impl EventStore for PostgresEventStore {
    async fn add(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        events: Vec<EventPage>,
        meta: &AddMeta<'_>,
    ) -> Result<AddOutcome> {
        let mut events = events;
        crate::storage::helpers::stamp_created_at(&mut events);
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

        // Use a transaction to ensure atomicity
        let mut conn = self.pool.acquire().await?;
        let mut tx = conn.begin().await?;

        // Check for idempotency if external_id is provided
        if !external_id.is_empty() {
            let query = Query::select()
                .expr(Expr::col(Events::Sequence).min())
                .expr(Expr::col(Events::Sequence).max())
                .from(Events::Table)
                .and_where(edition_predicate(Events::Edition, edition))
                .and_where(Expr::col(Events::Domain).eq(domain))
                .and_where(Expr::col(Events::Root).eq(&root_str))
                .and_where(Expr::col(Events::ExternalId).eq(external_id))
                .to_string(PostgresQueryBuilder);

            let row = sqlx::query(&query).fetch_optional(&mut *tx).await?;

            if let Some(row) = row {
                let min_seq: Option<i32> = row.get(0);
                let max_seq: Option<i32> = row.get(1);
                if let (Some(min), Some(max)) = (min_seq, max_seq) {
                    tx.commit().await?;
                    return Ok(AddOutcome::Duplicate {
                        first_sequence: min as u32,
                        last_sequence: max as u32,
                    });
                }
            }
        }

        let stream_next = Self::max_sequence(&mut tx, domain, edition, &root_str)
            .await?
            .map(|max| max + 1);
        let main_next = if stream_next.is_none() && !is_main_timeline(edition) {
            Self::max_sequence(&mut tx, domain, "", &root_str)
                .await?
                .map_or(0, |max| max + 1)
        } else {
            stream_next.unwrap_or(0)
        };
        let window = AppendWindow::for_edition(edition, stream_next, main_next);
        let (first_sequence, last_sequence) = validate_append(window, &events)?;

        // Prepare source tracking values. source_edition stored as NULL
        // when the source was on the main timeline ("" at the API).
        let (source_edition, source_domain, source_root, source_seq) =
            if let Some(info) = source_info.filter(|s| !s.is_empty()) {
                (
                    edition_to_db(&info.edition),
                    Some(info.domain.clone()),
                    Some(info.root.to_string()),
                    Some(info.seq as i32),
                )
            } else {
                (None, None, None, None)
            };
        let source_component = source_info.map(|s| s.component.as_str()).unwrap_or("");
        let source_command_index = source_info.map(|s| s.command_index as i32).unwrap_or(0);
        let source_kind = source_info.map(|s| s.kind).unwrap_or_default().as_str();

        // Parent-routing cover, serialized once and replicated per row (mirrors
        // correlation_id). All pages of this write share the same value.
        let ext_bytes: Option<Vec<u8>> = meta.ext.map(prost::Message::encode_to_vec);

        for event in events {
            let event_data = event.encode_to_vec();
            let sequence = event_sequence(&event);
            let created_at = crate::storage::helpers::parse_timestamp(&event)?;

            let query = Query::insert()
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
                    Events::SourceKind,
                    Events::Ext,
                ])
                .values_panic([
                    edition_to_db(edition).into(),
                    domain.into(),
                    root_str.clone().into(),
                    sequence.into(),
                    created_at.into(),
                    event_data.into(),
                    correlation_id.into(),
                    external_id.into(),
                    source_edition.clone().into(),
                    source_domain.clone().into(),
                    source_root.clone().into(),
                    source_seq.into(),
                    source_component.into(),
                    source_command_index.into(),
                    source_kind.into(),
                    ext_bytes.clone().into(),
                ])
                .to_string(PostgresQueryBuilder);

            // `add` is read-max-then-insert under READ COMMITTED with no row
            // lock, so two writers can validate against the same max and the
            // loser's INSERT trips the `(domain, edition, root, sequence)`
            // unique key (SQLSTATE 23505). That is an optimistic-concurrency
            // loss, classified as a retryable `SequenceConflict` rather than
            // a `Database` error. The `tx` rolls back on this early return.
            sqlx::query(&query)
                .execute(&mut *tx)
                .await
                .map_err(|e| map_write_conflict(e, window.max_first, sequence))?;
        }

        // Commit the transaction
        tx.commit().await?;

        Ok(AddOutcome::Added {
            first_sequence,
            last_sequence,
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
            return self.query_main_timeline(domain, &root_str, 0).await;
        }

        // Named edition: use stored procedure with explicit divergence
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
            return self.query_main_timeline(domain, &root_str, from).await;
        }

        // Named edition: use stored procedure for composite read
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

        // Main timeline: a single edition-scoped range query is exact.
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
                .to_string(PostgresQueryBuilder);

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
        // query filtered only on `edition_predicate`, dropping the
        // pre-divergence main-timeline prefix in the range.
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
        // C10: same single-boundary canonicalization as SQLite — see the
        // comment on `SqliteEventStore::get_until_timestamp`.
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
                .to_string(PostgresQueryBuilder);

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
        // the temporal cut to BOTH halves (finding #10) — the corrupt
        // temporal-reconstruction path. Cut is in-memory against the typed
        // `created_at` (nanosecond-exact chrono compare, matching SQLite and
        // the mock); no string form is involved, so the C10 lexical footgun
        // cannot reopen.
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
            .to_string(PostgresQueryBuilder);

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
            .to_string(PostgresQueryBuilder);

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
                .to_string(PostgresQueryBuilder);

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

        // Query the target edition (or main timeline for fallback).
        // The main timeline is our `""` sentinel at the Rust API layer,
        // which `edition_predicate` translates to `IS NULL`.
        let target_edition = if is_main_timeline(edition) {
            edition
        } else {
            ""
        };

        let query = Query::select()
            .expr(Expr::col(Events::Sequence).max())
            .from(Events::Table)
            .and_where(edition_predicate(Events::Edition, target_edition))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(&root_str))
            .to_string(PostgresQueryBuilder);

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

        // Query all events with this correlation_id
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
            .to_string(PostgresQueryBuilder);

        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        // Group events by (domain, edition, root)
        let mut books_map: HashMap<(String, String, Uuid), BookParts> = HashMap::new();

        for row in rows {
            let domain: String = row.get("domain");
            // C-16: `edition` is nullable (migration 0007). Decode as Option
            // and normalize NULL back to the API-layer empty-string sentinel;
            // a bare `String` decode would panic with `UnexpectedNullError`
            // on any main-timeline row.
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

    async fn delete_edition_events(&self, domain: &str, edition: &str) -> Result<u32> {
        // C-15: client-side guard mirrors the stored-proc guard so both
        // forms of the main-timeline sentinel (`""` and `"angzarr"`) raise
        // BEFORE we round-trip to Postgres. The proc was hardened in
        // migration 0010 too (defense in depth), but failing fast here
        // surfaces a clean Rust-level error (no language-of-database
        // dialect mixed into the message).
        if is_main_timeline(edition) {
            return Err(StorageError::MainTimelineProtected(format!(
                "delete_edition_events(edition={:?}) refused; the main \
                 timeline is append-only",
                edition
            )));
        }

        // The stored procedure additionally rejects NULL/empty/"angzarr" at
        // the database boundary, so even a direct SQL caller can't bypass.
        let row = sqlx::query("SELECT delete_edition_events($1, $2)")
            .bind(edition)
            .bind(domain)
            .fetch_one(&self.pool)
            .await?;

        let count: i32 = row.get(0);
        Ok(count as u32)
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
            .and_where(Expr::col(Events::SourceKind).eq(source_info.kind.as_str()))
            .order_by(Events::Sequence, Order::Asc)
            .to_string(PostgresQueryBuilder);

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
            .to_string(PostgresQueryBuilder);

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
}
