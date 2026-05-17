//! PostgreSQL EventStore implementation.
//!
//! Uses stored procedures for composite edition reads. The `get_edition_events`
//! stored procedure handles implicit divergence (deriving divergence point from
//! the first edition event).

use async_trait::async_trait;
use prost::Message;
use sea_query::{Expr, Iden, Order, PostgresQueryBuilder, Query, SimpleExpr};
use sqlx::{Acquire, PgPool, Row};
use uuid::Uuid;

use crate::proto::EventPage;
use crate::storage::helpers::{assemble_event_books, is_main_timeline};
use crate::storage::schema::Events;
use crate::storage::{
    AddOutcome, CascadeParticipant, EventStore, Result, SourceInfo, StorageError,
};

/// Build a WHERE predicate on an edition column that translates EITHER
/// main-timeline sentinel (`""` or `"angzarr"`) to SQL `IS NULL`.
///
/// C-15: both sentinels must round-trip to the same SQL NULL row. Migration
/// 0007 normalized pre-existing literal rows to NULL; new writes go through
/// `edition_to_db`. Reads MUST use this predicate (not a bare `Edition.eq`)
/// so a caller passing either form finds the row, and never accidentally
/// matches a literal `"angzarr"` left in the table by a legacy writer.
fn edition_predicate<T: Iden + 'static>(col: T, edition: &str) -> SimpleExpr {
    if is_main_timeline(edition) {
        Expr::col(col).is_null()
    } else {
        Expr::col(col).eq(edition)
    }
}

/// Convert the API-layer edition to the storage-layer value (`None` = SQL NULL).
///
/// C-15: BOTH `""` and `"angzarr"` are main-timeline sentinels per the
/// trait/`is_main_timeline` contract. Both MUST normalize to `None` so that
/// the SQL column holds a single canonical representation (NULL) for the
/// main timeline. Pre-fix this helper only handled `""` → None, which let
/// `"angzarr"` land as a literal table row that `edition_predicate` then
/// failed to match (it looks for `IS NULL`). Result: a write under one
/// sentinel was silently invisible to a read under the other.
fn edition_to_db(edition: &str) -> Option<String> {
    if is_main_timeline(edition) {
        None
    } else {
        Some(edition.to_string())
    }
}

/// Convert a storage-layer edition column value back to the API-layer
/// representation.
///
/// Migration 0007 made the `edition` column genuinely nullable and normalized
/// pre-existing main-timeline sentinels (`''`, `'angzarr'`) to SQL `NULL`. The
/// API surface uses the empty string as the canonical main-timeline sentinel,
/// so a `NULL` read must round-trip to `""`. Use this helper at every read site
/// — a bare `row.get::<String, _>("edition")` panics with `UnexpectedNullError`
/// on any post-migration main-timeline row (bug C-16).
fn edition_from_db(value: Option<String>) -> String {
    value.unwrap_or_default()
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

    /// Query events using the composite edition stored procedure.
    ///
    /// Calls `get_edition_events_from(domain, edition, root, from, explicit_divergence)`
    /// which handles implicit divergence (from first edition event) and main timeline
    /// merging.
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

    /// Query events with optional explicit divergence point.
    ///
    /// The explicit_divergence parameter specifies where the edition branches
    /// from the main timeline. When None, uses implicit divergence (first edition event).
    async fn composite_read_with_divergence(
        &self,
        domain: &str,
        edition: &str,
        root: &str,
        from: u32,
        explicit_divergence: Option<u32>,
    ) -> Result<Vec<EventPage>> {
        // Use stored procedure for composite read
        // The procedure handles: main timeline query if edition is 'angzarr',
        // or composite query (main + edition) with optional explicit divergence
        let query = "SELECT event_data FROM get_edition_events_from($1, $2, $3, $4, $5)";

        let rows = sqlx::query(query)
            .bind(domain)
            .bind(edition)
            .bind(root)
            .bind(from as i32)
            .bind(explicit_divergence.map(|d| d as i32))
            .fetch_all(&self.pool)
            .await?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let event_data: Vec<u8> = row.get("event_data");
            let event = EventPage::decode(event_data.as_slice())?;
            events.push(event);
        }

        Ok(events)
    }

    /// Simple query for main timeline events (no composite logic needed).
    async fn query_main_timeline(
        &self,
        domain: &str,
        root: &str,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        let query = Query::select()
            .column(Events::EventData)
            .from(Events::Table)
            .and_where(edition_predicate(Events::Edition, ""))
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
}

#[async_trait]
impl EventStore for PostgresEventStore {
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

        // Get the next sequence number once at the start of the transaction
        let base_sequence = {
            let query = Query::select()
                .expr(Expr::col(Events::Sequence).max())
                .from(Events::Table)
                .and_where(edition_predicate(Events::Edition, edition))
                .and_where(Expr::col(Events::Domain).eq(domain))
                .and_where(Expr::col(Events::Root).eq(&root_str))
                .to_string(PostgresQueryBuilder);

            let row = sqlx::query(&query).fetch_optional(&mut *tx).await?;

            match row {
                Some(row) => {
                    let max_seq: Option<i32> = row.get(0);
                    max_seq.map(|s| s as u32 + 1).unwrap_or(0)
                }
                None => 0,
            }
        };

        let mut first_sequence = None;
        let mut last_sequence = 0u32;

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

        for event in events {
            let event_data = event.encode_to_vec();
            let sequence = crate::storage::helpers::resolve_sequence(&event, base_sequence)?;
            let created_at = crate::storage::helpers::parse_timestamp(&event)?;

            // Extract cascade tracking fields from EventPage
            let committed = !event.no_commit;
            let cascade_id = event.cascade_id.clone();

            if first_sequence.is_none() {
                first_sequence = Some(sequence);
            }
            last_sequence = sequence;

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
                    Events::Committed,
                    Events::CascadeId,
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
                    committed.into(),
                    cascade_id.into(),
                ])
                .to_string(PostgresQueryBuilder);

            sqlx::query(&query).execute(&mut *tx).await?;
        }

        // Commit the transaction
        tx.commit().await?;

        Ok(AddOutcome::Added {
            first_sequence: first_sequence.unwrap_or(0),
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
            ])
            .from(Events::Table)
            .and_where(Expr::col(Events::CorrelationId).eq(correlation_id))
            .order_by(Events::Domain, Order::Asc)
            .order_by(Events::Root, Order::Asc)
            .order_by(Events::Sequence, Order::Asc)
            .to_string(PostgresQueryBuilder);

        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        // Group events by (domain, edition, root)
        let mut books_map: HashMap<(String, String, Uuid), Vec<EventPage>> = HashMap::new();

        for row in rows {
            let domain: String = row.get("domain");
            // C-16: `edition` is nullable (migration 0007). Decode as Option
            // and normalize NULL back to the API-layer empty-string sentinel;
            // a bare `String` decode would panic with `UnexpectedNullError`
            // on any main-timeline row.
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

    async fn query_stale_cascades(&self, threshold: &str) -> Result<Vec<String>> {
        // Per-participant resolution (C-02): a cascade is stale iff it has
        // at least one (cascade_id, domain, edition, root) participant that
        // is past the threshold AND has no committed cascade row on that
        // SAME (domain, edition, root) for the same cascade_id.
        //
        // Pre-fix semantics filtered out the entire cascade when ANY
        // committed row existed for that cascade_id (globally) — once
        // participant 1 of N was revoked, participants 2..N were stranded.
        //
        // Edition uses IS NOT DISTINCT FROM so SQL NULL (the postgres
        // representation of the main-timeline sentinel "") joins correctly
        // against itself.
        let raw = "SELECT DISTINCT s.cascade_id \
                   FROM events s \
                   WHERE s.committed = false \
                     AND s.cascade_id IS NOT NULL \
                     AND s.created_at < $1 \
                     AND NOT EXISTS ( \
                       SELECT 1 FROM events c \
                       WHERE c.committed = true \
                         AND c.cascade_id = s.cascade_id \
                         AND c.domain = s.domain \
                         AND c.edition IS NOT DISTINCT FROM s.edition \
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
        // participants that already have a committed cascade row for this
        // cascade_id. Without this filter, the reaper re-writes Revocations
        // on every cycle for participants already resolved by a prior pass.
        let raw = "SELECT s.domain, s.edition, s.root, s.sequence \
                   FROM events s \
                   WHERE s.cascade_id = $1 \
                     AND s.committed = false \
                     AND NOT EXISTS ( \
                       SELECT 1 FROM events c \
                       WHERE c.committed = true \
                         AND c.cascade_id = s.cascade_id \
                         AND c.domain = s.domain \
                         AND c.edition IS NOT DISTINCT FROM s.edition \
                         AND c.root = s.root \
                     ) \
                   ORDER BY s.domain ASC, s.root ASC, s.sequence ASC";

        let rows = sqlx::query(raw)
            .bind(cascade_id)
            .fetch_all(&self.pool)
            .await?;

        // Group by (domain, edition, root). Postgres stores `edition=""` as
        // SQL NULL; surface that back as the empty-string main-timeline
        // sentinel at the API boundary.
        let mut participants_map: HashMap<(String, String, Uuid), Vec<u32>> = HashMap::new();

        for row in rows {
            let domain: String = row.get("domain");
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
