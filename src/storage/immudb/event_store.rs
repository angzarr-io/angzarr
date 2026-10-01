//! ImmuDB EventStore implementation via PostgreSQL wire protocol.
//!
//! Uses sqlx with Postgres driver connecting to immudb's pgsql server.
//! Queries built with sea_query for type-safe SQL generation.
//!
//! # Simple Query Mode
//!
//! immudb's pgsql server only supports simple query mode - it does not support
//! the extended query protocol (prepared statements). All queries must be
//! executed using `raw_sql()` to avoid Parse/Bind/Execute messages.

use async_trait::async_trait;
use hex;
use prost::Message;
use sea_query::{Asterisk, Expr, Order, PostgresQueryBuilder, Query};
use sqlx::{Executor, PgPool, Row};
use uuid::Uuid;

use crate::proto::EventPage;
use crate::storage::helpers::{assemble_event_books, event_sequence, is_main_timeline, BookParts};
use crate::storage::schema::Events;
use crate::storage::sql::event_store::{
    implicit_divergence, map_write_conflict, merge_composite_events, resolve_divergence,
};
use crate::storage::timeline::{
    storage_edition, validate_append, AppendWindow, MAIN_TIMELINE_STORAGE_EDITION,
};
use crate::storage::{AddMeta, AddOutcome, EventStore, Result, SourceInfo, StorageError};

/// Format a typed timestamp as immudb's `TIMESTAMP` literal.
///
/// immudb's `created_at` column is a real `TIMESTAMP` (not TEXT like
/// SQLite/Postgres) holding whole UTC seconds. The write path (`add`, via
/// `CAST('...' AS TIMESTAMP)`) and the read bound (`get_until_timestamp`)
/// both format through this one function; exact sub-second ordering comes
/// from each page's own `created_at`.
fn immudb_timestamp_literal(ts: &prost_types::Timestamp) -> Result<String> {
    let dt = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32).ok_or(
        StorageError::InvalidTimestamp {
            seconds: ts.seconds,
            nanos: ts.nanos,
        },
    )?;
    Ok(dt.format("%Y-%m-%d %H:%M:%S").to_string())
}

/// Decode a BLOB column from immudb.
///
/// immudb returns BLOBs as hex-encoded ASCII strings through the pgsql wire
/// protocol, not as raw bytes. We need to decode the hex string.
fn decode_blob_column(row: &sqlx::postgres::PgRow, index: usize) -> Result<Vec<u8>> {
    use sqlx::Row as _;
    use sqlx::ValueRef;

    // Get the raw column value
    let value_ref = row.try_get_raw(index)?;

    // Check if it's null
    if value_ref.is_null() {
        return Ok(Vec::new());
    }

    // immudb returns BLOB as hex-encoded ASCII string bytes
    let hex_bytes = value_ref
        .as_bytes()
        .map_err(|e| StorageError::Backend(format!("immudb BLOB raw bytes: {}", e)))?;

    // Convert ASCII bytes to string and decode hex
    let hex_str = std::str::from_utf8(hex_bytes)
        .map_err(|e| StorageError::Backend(format!("immudb BLOB is not UTF-8 hex: {}", e)))?;

    // Decode the hex string to get the original binary data
    hex::decode(hex_str)
        .map_err(|e| StorageError::Backend(format!("immudb BLOB hex decode: {}", e)))
}

/// ImmuDB implementation of EventStore via pgsql wire protocol.
///
/// Connects to immudb using standard Postgres driver. immudb must be
/// started with `IMMUDB_PGSQL_SERVER=true`.
///
/// # Connection String
///
/// ```text
/// postgresql://immudb:immudb@localhost:5432/defaultdb?sslmode=disable
/// ```
///
/// # Advantages over other backends
///
/// - **Immutability guaranteed**: immudb prevents modification/deletion at storage level
/// - **Cryptographic proofs**: Data integrity verifiable via Merkle trees
/// - **Time-travel**: `SINCE TX` queries for temporal access
/// - **Audit trail**: `HISTORY OF events` for full revision history
pub struct ImmudbEventStore {
    pool: PgPool,
}

impl ImmudbEventStore {
    /// Create a new immudb event store.
    ///
    /// The pool should be configured to connect to immudb's pgsql port.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Initialize the schema (create tables and indexes).
    ///
    /// Safe to call multiple times - uses IF NOT EXISTS.
    /// Uses raw_sql for immudb simple query mode compatibility.
    pub async fn init_schema(&self) -> Result<()> {
        self.pool
            .execute(sqlx::raw_sql(super::schema::CREATE_EVENTS_TABLE))
            .await?;

        // Note: immudb requires indexes on empty tables, so these may fail
        // if table already has data. Using IF NOT EXISTS to handle gracefully.
        let _ = self
            .pool
            .execute(sqlx::raw_sql(super::schema::CREATE_CORRELATION_INDEX))
            .await;

        let _ = self
            .pool
            .execute(sqlx::raw_sql(super::schema::CREATE_DOMAIN_ROOT_INDEX))
            .await;

        Ok(())
    }

    /// Query events for a specific edition.
    /// Uses raw_sql for immudb simple query mode compatibility.
    async fn query_edition_events(
        &self,
        domain: &str,
        edition: &str,
        root_str: &str,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        let query = Query::select()
            .column(Events::EventData)
            .from(Events::Table)
            .and_where(Expr::col(Events::Edition).eq(edition))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(root_str))
            .and_where(Expr::col(Events::Sequence).gte(from))
            .order_by(Events::Sequence, Order::Asc)
            .to_string(PostgresQueryBuilder);

        let rows = sqlx::raw_sql(&query).fetch_all(&self.pool).await?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            // Use index 0 since raw_sql doesn't reliably support column names
            // immudb returns BLOBs as hex strings through pgsql wire protocol
            let event_data = decode_blob_column(&row, 0)?;
            let event = EventPage::decode(event_data.as_slice())?;
            events.push(event);
        }

        Ok(events)
    }

    /// Query main timeline events up to (but not including) `until_seq`, or
    /// the ENTIRE main timeline when `until_seq` is `None` (the #12
    /// eventless-edition "inherit whole main timeline" case).
    /// Uses raw_sql for immudb simple query mode compatibility.
    async fn query_main_events_until(
        &self,
        domain: &str,
        root_str: &str,
        until_seq: Option<u32>,
    ) -> Result<Vec<EventPage>> {
        let mut stmt = Query::select()
            .column(Events::EventData)
            .from(Events::Table)
            .and_where(Expr::col(Events::Edition).eq(MAIN_TIMELINE_STORAGE_EDITION))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(root_str))
            .order_by(Events::Sequence, Order::Asc)
            .to_owned();
        if let Some(seq) = until_seq {
            stmt.and_where(Expr::col(Events::Sequence).lt(seq));
        }
        let query = stmt.to_string(PostgresQueryBuilder);

        let rows = sqlx::raw_sql(&query).fetch_all(&self.pool).await?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            // Use index 0 since raw_sql doesn't reliably support column names
            let event_data = decode_blob_column(&row, 0)?;
            let event = EventPage::decode(event_data.as_slice())?;
            events.push(event);
        }

        Ok(events)
    }

    /// Fetch the raw halves of a composite read (main-timeline prefix +
    /// edition events) for a NAMED edition, using the SHARED divergence
    /// resolution (`crate::storage::sql::event_store`, finding #28) so
    /// immudb resolves divergence identically to SQLite/Postgres. Callers
    /// merge with their own `keep` predicate via [`merge_composite_events`].
    async fn composite_parts(
        &self,
        domain: &str,
        edition: &str,
        root_str: &str,
    ) -> Result<(Vec<EventPage>, Vec<EventPage>)> {
        self.composite_parts_with_divergence(domain, edition, root_str, None)
            .await
    }

    /// [`Self::composite_parts`] with an optional explicit divergence point.
    async fn composite_parts_with_divergence(
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

    /// Composite read for editions: main timeline (before divergence) +
    /// edition events, from `from` onward.
    async fn composite_read(
        &self,
        domain: &str,
        edition: &str,
        root_str: &str,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        let (main_events, edition_events) = self.composite_parts(domain, edition, root_str).await?;
        Ok(merge_composite_events(main_events, edition_events, |e| {
            event_sequence(e) >= from
        }))
    }

    /// C-18 helper: scan for an existing external_id claim on this aggregate.
    /// Returns `Some((first_sequence, last_sequence))` of the prior batch
    /// when found, `None` otherwise. Mirrors `SqliteEventStore::check_idempotency`.
    async fn find_external_id_sequences(
        &self,
        domain: &str,
        edition: &str,
        root_str: &str,
        external_id: &str,
    ) -> Result<Option<(u32, u32)>> {
        // Select the matching sequences rather than MIN/MAX: immudb answers
        // an aggregate over no rows with 0 instead of NULL, which would read
        // as a claim at sequence 0.
        let query = Query::select()
            .column(Events::Sequence)
            .from(Events::Table)
            .and_where(Expr::col(Events::Edition).eq(edition))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(root_str))
            .and_where(Expr::col(Events::ExternalId).eq(external_id))
            .to_string(PostgresQueryBuilder);

        let rows = sqlx::raw_sql(&query).fetch_all(&self.pool).await?;
        let sequences: Vec<u32> = rows
            .iter()
            .filter_map(|row| row.try_get::<i64, _>(0).ok())
            .map(|seq| seq as u32)
            .collect();
        Ok(sequences
            .iter()
            .min()
            .zip(sequences.iter().max())
            .map(|(min, max)| (*min, *max)))
    }

    /// Get max sequence number for an aggregate.
    /// Uses raw_sql for immudb simple query mode compatibility.
    async fn get_max_sequence(
        &self,
        domain: &str,
        edition: &str,
        root_str: &str,
    ) -> Result<Option<u32>> {
        let query = Query::select()
            .expr(Expr::col(Events::Sequence).max())
            .from(Events::Table)
            .and_where(Expr::col(Events::Edition).eq(edition))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(root_str))
            .to_string(PostgresQueryBuilder);

        let rows = sqlx::raw_sql(&query).fetch_all(&self.pool).await?;

        if rows.is_empty() {
            return Ok(None);
        }

        // immudb returns 0 instead of NULL for MAX() on empty result sets,
        // so we need to check if any events actually exist
        let max_seq: Option<i64> = rows[0].try_get(0).ok().flatten();

        // If we got a value, verify it's not a false 0 from empty result
        // by checking if the aggregate actually has events
        if max_seq == Some(0) {
            // Check if there's actually a sequence 0 event
            // Note: immudb only supports COUNT(*), not COUNT(column)
            let count_query = Query::select()
                .expr(Expr::col(Asterisk).count())
                .from(Events::Table)
                .and_where(Expr::col(Events::Edition).eq(edition))
                .and_where(Expr::col(Events::Domain).eq(domain))
                .and_where(Expr::col(Events::Root).eq(root_str))
                .to_string(PostgresQueryBuilder);

            let count_rows = sqlx::raw_sql(&count_query).fetch_all(&self.pool).await?;
            if !count_rows.is_empty() {
                let count: i64 = count_rows[0].try_get(0).unwrap_or(0);
                if count == 0 {
                    return Ok(None);
                }
            }
        }

        Ok(max_seq.map(|s| s as u32))
    }
}

#[async_trait]
impl EventStore for ImmudbEventStore {
    async fn add(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        events: Vec<EventPage>,
        meta: &AddMeta<'_>,
    ) -> Result<AddOutcome> {
        let edition = storage_edition(edition);
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

        // C-18: external_id idempotency check, parity with SQLite/Postgres.
        // If a prior call recorded this external_id on this aggregate, return
        // `AddOutcome::Duplicate` with the original sequence range instead
        // of re-persisting.
        if !external_id.is_empty() {
            if let Some((first, last)) = self
                .find_external_id_sequences(domain, edition, &root_str, external_id)
                .await?
            {
                return Ok(AddOutcome::Duplicate {
                    first_sequence: first,
                    last_sequence: last,
                });
            }
        }

        let stream_next = self
            .get_max_sequence(domain, edition, &root_str)
            .await?
            .map(|max| max + 1);
        let main_next = if stream_next.is_none() && !is_main_timeline(edition) {
            self.get_max_sequence(domain, MAIN_TIMELINE_STORAGE_EDITION, &root_str)
                .await?
                .map_or(0, |max| max + 1)
        } else {
            stream_next.unwrap_or(0)
        };
        let window = AppendWindow::for_edition(edition, stream_next, main_next);
        let (first_sequence, last_sequence) = validate_append(window, &events)?;

        // C-19: Per-row INSERTs must be wrapped in a transaction so a
        // partial failure (concurrent writer collided on the PRIMARY KEY
        // at any sequence in the batch) rolls back the whole batch
        // instead of leaving a partial stream behind. We issue
        // BEGIN/COMMIT/ROLLBACK by hand on a single pooled connection
        // rather than via `pool.begin()` because sqlx's transaction
        // wrapper sends extended-query bookkeeping that immudb's
        // pgsql-wire server rejects (immudb is simple-query-only — see
        // the module doc). Holding the connection ourselves keeps every
        // statement on the same session so BEGIN/INSERT/COMMIT actually
        // serialize. The PRIMARY KEY (domain, edition, root, sequence)
        // on the events table is the CAS fence: a losing writer hits a
        // UNIQUE-violation that we map to
        // `StorageError::SequenceConflict`; the aggregate pipeline
        // retries with a fresh sequence read.
        let mut conn = self.pool.acquire().await?;
        let conn_ref: &mut sqlx::PgConnection = &mut conn;
        conn_ref.execute(sqlx::raw_sql("BEGIN")).await?;

        // Insert events one by one (immudb may not support multi-row INSERT well)
        for event in events {
            let event_data = event.encode_to_vec();
            let sequence = event_sequence(&event);

            // Format event_data as hex for immudb BLOB type (x'...' format)
            let event_data_hex = format!("x'{}'", hex::encode(&event_data));

            // C10: format directly from the typed `created_at`, through the
            // SAME function `get_until_timestamp` reads through
            // (`immudb_timestamp_literal`) — no RFC3339-string round trip,
            // no ad hoc split/truncate. Falls back to "now" when the event
            // carries no timestamp, mirroring `storage::helpers::parse_timestamp`.
            let created_ts = event.created_at.unwrap_or_else(|| {
                let now = chrono::Utc::now();
                prost_types::Timestamp {
                    seconds: now.timestamp(),
                    nanos: now.timestamp_subsec_nanos() as i32,
                }
            });
            let timestamp_simple = immudb_timestamp_literal(&created_ts)?;

            // Build INSERT manually since sea-query doesn't handle immudb BLOB format
            // Note: immudb requires CAST for string timestamps
            // NOTE: this builds SQL via string concatenation with single-quote
            // doubling. That is a separate SQL-injection-class concern tracked
            // alongside C-19 (see `plans/deep-review-remediation.md` for the
            // C-19 NOTE pointing at this site).
            //
            // C-18 (this finding): always include the external_id and
            // source_info columns. NULL-equivalents (empty string for
            // external_id; NULL for source_*) are written so the
            // round-trip lookup contracts hold.
            let ext_lit = if external_id.is_empty() {
                "NULL".to_string()
            } else {
                format!("'{}'", external_id.replace('\'', "''"))
            };
            // Parent-routing cover (Cover.ext) → BLOB hex literal, replicated
            // per row to mirror correlation_id. NULL when the write carried none.
            let cover_ext_lit = match meta.ext {
                Some(any) => format!("x'{}'", hex::encode(prost::Message::encode_to_vec(any))),
                None => "NULL".to_string(),
            };
            let (
                source_edition_lit,
                source_domain_lit,
                source_root_lit,
                source_seq_lit,
                source_component_lit,
                source_command_index_lit,
            ) = if let Some(info) = source_info.filter(|s| !s.is_empty()) {
                (
                    format!("'{}'", storage_edition(&info.edition).replace('\'', "''")),
                    format!("'{}'", info.domain.replace('\'', "''")),
                    format!("'{}'", info.root),
                    info.seq.to_string(),
                    format!("'{}'", info.component.replace('\'', "''")),
                    info.command_index.to_string(),
                )
            } else {
                (
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                )
            };

            let query = format!(
                "INSERT INTO events (edition, domain, root, sequence, created_at, event_data, correlation_id, external_id, source_edition, source_domain, source_root, source_seq, source_component, source_command_index, ext) \
                 VALUES ('{}', '{}', '{}', {}, CAST('{}' AS TIMESTAMP), {}, '{}', {}, {}, {}, {}, {}, {}, {}, {})",
                edition.replace('\'', "''"),
                domain.replace('\'', "''"),
                root_str.replace('\'', "''"),
                sequence,
                timestamp_simple,
                event_data_hex,
                correlation_id.replace('\'', "''"),
                ext_lit,
                source_edition_lit,
                source_domain_lit,
                source_root_lit,
                source_seq_lit,
                source_component_lit,
                source_command_index_lit,
                cover_ext_lit,
            );

            // Use raw_sql for immudb simple query mode compatibility.
            let conn_ref: &mut sqlx::PgConnection = &mut conn;
            match conn_ref.execute(sqlx::raw_sql(&query)).await {
                Ok(_) => {}
                Err(err) => {
                    // Roll back the entire batch before propagating.
                    let conn_ref: &mut sqlx::PgConnection = &mut conn;
                    let _ = conn_ref.execute(sqlx::raw_sql("ROLLBACK")).await;
                    // #20/#28: classify a PRIMARY-KEY duplicate-key violation
                    // through the SHARED classifier instead of a bespoke
                    // substring match. immudb returns generic SQL errors
                    // without a SQLSTATE, so `map_write_conflict` falls back
                    // to matching the error Display (see
                    // `sql::event_store::is_unique_violation`) — the same
                    // "primary key"/"duplicate"/"unique"/"already exists" set
                    // this site used before, now owned in one place.
                    return Err(map_write_conflict(err, window.max_first, sequence));
                }
            }
        }

        let conn_ref: &mut sqlx::PgConnection = &mut conn;
        conn_ref.execute(sqlx::raw_sql("COMMIT")).await?;

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
        let edition = storage_edition(edition);
        let root_str = root.to_string();
        if is_main_timeline(edition) {
            return self
                .query_edition_events(domain, MAIN_TIMELINE_STORAGE_EDITION, &root_str, 0)
                .await;
        }
        let (main_events, edition_events) = self
            .composite_parts_with_divergence(domain, edition, &root_str, explicit_divergence)
            .await?;
        Ok(merge_composite_events(main_events, edition_events, |_| {
            true
        }))
    }

    async fn get_from(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        let edition = storage_edition(edition);
        let root_str = root.to_string();

        if is_main_timeline(edition) {
            self.query_edition_events(domain, MAIN_TIMELINE_STORAGE_EDITION, &root_str, from)
                .await
        } else {
            self.composite_read(domain, edition, &root_str, from).await
        }
    }

    async fn get_from_to(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
        to: u32,
    ) -> Result<Vec<EventPage>> {
        let edition = storage_edition(edition);
        let root_str = root.to_string();

        // Main timeline: a single edition-scoped range query is exact.
        if is_main_timeline(edition) {
            let query = Query::select()
                .column(Events::EventData)
                .from(Events::Table)
                .and_where(Expr::col(Events::Edition).eq(MAIN_TIMELINE_STORAGE_EDITION))
                .and_where(Expr::col(Events::Domain).eq(domain))
                .and_where(Expr::col(Events::Root).eq(&root_str))
                .and_where(Expr::col(Events::Sequence).gte(from))
                .and_where(Expr::col(Events::Sequence).lt(to)) // exclusive end [from, to)
                .order_by(Events::Sequence, Order::Asc)
                .to_string(PostgresQueryBuilder);

            let rows = sqlx::raw_sql(&query).fetch_all(&self.pool).await?;

            let mut events = Vec::with_capacity(rows.len());
            for row in rows {
                let event_data = decode_blob_column(&row, 0)?; // Use index for raw_sql compatibility
                let event = EventPage::decode(event_data.as_slice())?;
                events.push(event);
            }

            return Ok(events);
        }

        // Named edition: route through the SAME composite (main-prefix +
        // edition) logic as `get`/`get_from` (finding #10). The pre-fix
        // query filtered only on the literal edition column, dropping the
        // pre-divergence main-timeline prefix in the range.
        let (main_events, edition_events) =
            self.composite_parts(domain, edition, &root_str).await?;
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
        let edition = storage_edition(edition);
        let root_str = root.to_string();

        // The TIMESTAMP column holds whole seconds, so the column comparison
        // (`created_at <= CAST(until truncated to seconds)`) is a superset of
        // the answer; the exact cut is made on each page's own nanosecond
        // `created_at`. Pages without a timestamp keep the column's verdict.
        // The literal is inlined (not bound) because immudb queries run via
        // `sqlx::raw_sql`; `immudb_timestamp_literal` emits only digits,
        // dashes, colons and a space, so there is nothing to escape.
        let until_dt = chrono::DateTime::from_timestamp(until.seconds, until.nanos as u32).ok_or(
            StorageError::InvalidTimestamp {
                seconds: until.seconds,
                nanos: until.nanos,
            },
        )?;
        let at_or_before_until = |e: &EventPage| match &e.created_at {
            Some(ts) => chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                .is_some_and(|dt| dt <= until_dt),
            None => true,
        };

        if is_main_timeline(edition) {
            let until_str = immudb_timestamp_literal(until)?;
            let until_ts_expr = Expr::cust(format!("CAST('{until_str}' AS TIMESTAMP)"));

            let query = Query::select()
                .column(Events::EventData)
                .from(Events::Table)
                .and_where(Expr::col(Events::Edition).eq(MAIN_TIMELINE_STORAGE_EDITION))
                .and_where(Expr::col(Events::Domain).eq(domain))
                .and_where(Expr::col(Events::Root).eq(&root_str))
                .and_where(Expr::col(Events::CreatedAt).lte(until_ts_expr))
                .order_by(Events::Sequence, Order::Asc)
                .to_string(PostgresQueryBuilder);

            let rows = sqlx::raw_sql(&query).fetch_all(&self.pool).await?;

            let mut events = Vec::with_capacity(rows.len());
            for row in rows {
                let event_data = decode_blob_column(&row, 0)?; // Use index for raw_sql compatibility
                let event = EventPage::decode(event_data.as_slice())?;
                if at_or_before_until(&event) {
                    events.push(event);
                }
            }

            return Ok(events);
        }

        // Named edition: composite (main-prefix + edition) read, then the
        // same exact cut on both halves. Pages without a timestamp have no
        // column verdict here and are excluded.
        let (main_events, edition_events) =
            self.composite_parts(domain, edition, &root_str).await?;
        Ok(merge_composite_events(main_events, edition_events, |e| {
            e.created_at.is_some() && at_or_before_until(e)
        }))
    }

    async fn get_by_correlation(
        &self,
        correlation_id: &str,
    ) -> Result<Vec<crate::proto::EventBook>> {
        let query = Query::select()
            .columns([
                Events::Domain,
                Events::Edition,
                Events::Root,
                Events::EventData,
                Events::Ext,
            ])
            .from(Events::Table)
            .and_where(Expr::col(Events::CorrelationId).eq(correlation_id))
            .order_by(Events::Domain, Order::Asc)
            .order_by(Events::Root, Order::Asc)
            .order_by(Events::Sequence, Order::Asc)
            .to_string(PostgresQueryBuilder);

        let rows = sqlx::raw_sql(&query).fetch_all(&self.pool).await?;

        let mut books_map: std::collections::HashMap<(String, String, Uuid), BookParts> =
            std::collections::HashMap::new();

        for row in rows {
            // Columns: Domain(0), Edition(1), Root(2), EventData(3), Ext(4)
            let domain: String = row.get(0);
            let edition: String = row.get(1);
            let root_str: String = row.get(2);
            let event_data = decode_blob_column(&row, 3)?;
            // NULL ext decodes to an empty Vec (see decode_blob_column).
            let ext_bytes = decode_blob_column(&row, 4)?;

            let root = Uuid::parse_str(&root_str)?;
            let event = EventPage::decode(event_data.as_slice())?;

            let entry = books_map.entry((domain, edition, root)).or_default();
            entry.pages.push(event);
            if entry.ext.is_none() && !ext_bytes.is_empty() {
                entry.ext = Some(prost_types::Any::decode(ext_bytes.as_slice())?);
            }
        }

        Ok(assemble_event_books(books_map, correlation_id))
    }

    async fn get_next_sequence(&self, domain: &str, edition: &str, root: Uuid) -> Result<u32> {
        let edition = storage_edition(edition);
        let root_str = root.to_string();

        if let Some(max) = self.get_max_sequence(domain, edition, &root_str).await? {
            return Ok(max + 1);
        }
        if is_main_timeline(edition) {
            return Ok(0);
        }
        // An edition with no events of its own continues the main timeline.
        Ok(self
            .get_max_sequence(domain, MAIN_TIMELINE_STORAGE_EDITION, &root_str)
            .await?
            .map_or(0, |max| max + 1))
    }

    async fn list_roots(&self, domain: &str, edition: &str) -> Result<Vec<Uuid>> {
        let edition = storage_edition(edition);
        // immudb may not support DISTINCT well, use regular query
        let query = Query::select()
            .column(Events::Root)
            .distinct()
            .from(Events::Table)
            .and_where(Expr::col(Events::Edition).eq(edition))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .to_string(PostgresQueryBuilder);

        let rows = sqlx::raw_sql(&query).fetch_all(&self.pool).await?;

        let mut roots = Vec::with_capacity(rows.len());
        for row in rows {
            let root_str: String = row.get(0); // Root is the only column
            let root = Uuid::parse_str(&root_str)?;
            roots.push(root);
        }

        Ok(roots)
    }

    async fn list_domains(&self) -> Result<Vec<String>> {
        let query = Query::select()
            .column(Events::Domain)
            .distinct()
            .from(Events::Table)
            .to_string(PostgresQueryBuilder);

        let rows = sqlx::raw_sql(&query).fetch_all(&self.pool).await?;

        let mut domains = Vec::with_capacity(rows.len());
        for row in rows {
            let domain: String = row.get(0); // Domain is the only column
            domains.push(domain);
        }

        Ok(domains)
    }

    async fn delete_edition_events(&self, _domain: &str, _edition: &str) -> Result<u32> {
        // immudb is immutable - deletion is not supported by design
        // This is a feature, not a bug: events should never be deleted
        Err(StorageError::NotImplemented(
            "immudb does not support deletion - events are immutable".to_string(),
        ))
    }

    async fn find_by_source(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        source_info: &SourceInfo,
    ) -> Result<Option<Vec<EventPage>>> {
        let edition = storage_edition(edition);
        // C-18: Saga idempotency. Pre-fix this returned `Ok(None)`
        // unconditionally, violating the trait contract. Query the
        // events table on the C-18 source_* columns persisted by
        // `add()`. Uses raw_sql for immudb simple-query-mode compat.
        if source_info.is_empty() {
            return Ok(None);
        }
        let root_str = root.to_string();
        let query = Query::select()
            .column(Events::EventData)
            .from(Events::Table)
            .and_where(Expr::col(Events::Edition).eq(edition))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(&root_str))
            .and_where(Expr::col(Events::SourceEdition).eq(storage_edition(&source_info.edition)))
            .and_where(Expr::col(Events::SourceDomain).eq(source_info.domain.as_str()))
            .and_where(Expr::col(Events::SourceRoot).eq(source_info.root.to_string()))
            .and_where(Expr::col(Events::SourceSeq).eq(source_info.seq as i32))
            .and_where(Expr::col(Events::SourceComponent).eq(source_info.component.as_str()))
            .and_where(Expr::col(Events::SourceCommandIndex).eq(source_info.command_index as i32))
            .order_by(Events::Sequence, Order::Asc)
            .to_string(PostgresQueryBuilder);

        let rows = sqlx::raw_sql(&query).fetch_all(&self.pool).await?;
        if rows.is_empty() {
            return Ok(None);
        }
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let event_data = decode_blob_column(&row, 0)?;
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
        let edition = storage_edition(edition);
        // C-18: fact-injection idempotency. Pre-fix this returned
        // `Ok(None)` unconditionally, violating the trait contract.
        // Query on the C-18 `external_id` column persisted by `add()`.
        // Empty external_id returns None per contract.
        if external_id.is_empty() {
            return Ok(None);
        }
        let root_str = root.to_string();
        let query = Query::select()
            .column(Events::EventData)
            .from(Events::Table)
            .and_where(Expr::col(Events::Edition).eq(edition))
            .and_where(Expr::col(Events::Domain).eq(domain))
            .and_where(Expr::col(Events::Root).eq(&root_str))
            .and_where(Expr::col(Events::ExternalId).eq(external_id))
            .order_by(Events::Sequence, Order::Asc)
            .to_string(PostgresQueryBuilder);

        let rows = sqlx::raw_sql(&query).fetch_all(&self.pool).await?;
        if rows.is_empty() {
            return Ok(None);
        }
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let event_data = decode_blob_column(&row, 0)?;
            let event = EventPage::decode(event_data.as_slice())?;
            events.push(event);
        }
        Ok(Some(events))
    }

    // -------------------------------------------------------------------------
    // Cascade query methods.
    //
    // NOTE (out of scope for C-19): the immudb EventStore does not yet store
    // the cascade-tracking columns (`committed`, `cascade_id`) that the
    // `query_stale_cascades` / `query_cascade_participants` trait methods
    // depend on — the schema in `super::schema::CREATE_EVENTS_TABLE` predates
    // the Phase-5 cascade trait additions. These stub implementations exist
    // ONLY so the `immudb` feature compiles against the current trait shape;
    // they do not provide cascade reaper coverage on this backend. Proper
    // implementation belongs to whichever finding picks up immudb's missing
    // cascade-tracking columns (related to C-02 / C-18). C-19's responsibility
    // is the missing-transaction race in `add()`, which IS fixed above.
    async fn query_stale_cascades(&self, _threshold: &str) -> Result<Vec<String>> {
        Err(StorageError::NotImplemented(
            "immudb EventStore does not yet store cascade tracking columns; \
             see C-19 NOTE in plans/deep-review-remediation.md"
                .to_string(),
        ))
    }

    async fn query_cascade_participants(
        &self,
        _cascade_id: &str,
    ) -> Result<Vec<crate::storage::CascadeParticipant>> {
        Err(StorageError::NotImplemented(
            "immudb EventStore does not yet store cascade tracking columns; \
             see C-19 NOTE in plans/deep-review-remediation.md"
                .to_string(),
        ))
    }
}

#[cfg(test)]
#[path = "event_store.test.rs"]
mod tests;
