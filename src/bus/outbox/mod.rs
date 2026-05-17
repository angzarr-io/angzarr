//! Outbox pattern wrapper for guaranteed event delivery.
//!
//! This module provides an `OutboxEventBus` that wraps any `EventBus` implementation
//! and ensures events are persisted before publishing. The flow:
//!
//! 1. Write event to SQL outbox table (within transaction)
//! 2. Publish to inner bus
//! 3. Delete from outbox on success
//!
//! If step 2 fails, the event remains in the outbox for retry by a background process.
//!
//! # When to Use (and When Not To)
//!
//! **The outbox pattern is often superfluous.** Many messaging systems already provide
//! durability guarantees:
//!
//! | Messaging Layer | Built-in Durability | Outbox Needed? |
//! |-----------------|---------------------|----------------|
//! | **Kafka** | Yes (replicated log) | Rarely - Kafka already guarantees delivery |
//! | **RabbitMQ** | Optional (persistent queues) | Maybe - if not using persistent queues |
//! | **In-memory** | No | Yes - if delivery matters |
//! | **Redis Streams** | Optional (AOF/RDB) | Depends on persistence config |
//!
//! **Use outbox when:**
//! - Network between app and message broker is unreliable
//! - Message broker lacks durability guarantees
//! - Regulatory/compliance requires local audit trail before transmission
//! - You need exactly-once semantics (combined with idempotent consumers)
//!
//! **Skip outbox when:**
//! - Using Kafka or other durable message brokers
//! - Best-effort delivery is acceptable (analytics, logging)
//! - Latency is critical
//! - You're already paying for managed messaging with SLAs
//!
//! # Performance & Cost Impact
//!
//! **Warning:** The outbox pattern has significant overhead:
//!
//! - **Latency:** 2 SQL round-trips per publish (INSERT + DELETE), typically 1-5ms added
//! - **Duplication:** Events stored twice (outbox table + message broker)
//! - **Storage cost:** Outbox table grows during outages; requires monitoring
//! - **Operational cost:** Background recovery process, table maintenance, monitoring
//! - **Complexity:** More failure modes to understand and debug
//!
//! **Understand what you're getting into.** The outbox pattern trades simplicity and
//! performance for delivery guarantees. If your messaging layer already provides those
//! guarantees, you're paying twice for the same thing.
//!
//! # Configuration
//!
//! Enable via config or environment variable:
//! ```yaml
//! messaging:
//!   outbox:
//!     enabled: true
//!     max_retries: 10
//!     recovery_interval_secs: 5
//! ```
//!
//! Or via environment: `ANGZARR_OUTBOX_ENABLED=true`

use std::sync::Arc;

use async_trait::async_trait;
use prost::Message;
#[cfg(feature = "postgres")]
use sea_query::PostgresQueryBuilder;
// SQLite is always compiled
use sea_query::SqliteQueryBuilder;
use sea_query::{ColumnDef, Expr, Iden, Index, Query, Table};
use serde::Deserialize;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::{BusError, EventBus, EventHandler, PublishResult, Result};
use crate::config::OUTBOX_ENABLED_ENV_VAR;
use crate::proto::EventBook;
use crate::proto_ext::pages::EventPageExt;
use crate::proto_ext::CoverExt;

// ============================================================================
// Schema
// ============================================================================

/// Outbox table schema.
#[derive(Iden)]
enum Outbox {
    Table,
    #[iden = "id"]
    Id,
    #[iden = "domain"]
    Domain,
    #[iden = "root"]
    Root,
    #[iden = "event_data"]
    EventData,
    #[iden = "created_at"]
    CreatedAt,
    #[iden = "retry_count"]
    RetryCount,
}

/// Per-root max-published-sequence watermark.
///
/// Each successful publish (normal path or recovery path) bumps the
/// watermark for `(domain, root)`. Recovery consults this table before
/// republishing an orphaned row: if the orphaned event's max page
/// sequence is `<=` the stored watermark, the row is superseded and is
/// deleted from the outbox without republishing.
///
/// This guards CQRS-ES per-root monotonicity across the
/// persist-then-publish boundary. Without it, an orphaned seq=N for
/// root X can be recovered AFTER seq=N+k for X has already gone out the
/// normal path, regressing downstream consumer state. See C-13 in
/// `plans/deep-review-remediation.md`.
#[derive(Iden)]
enum OutboxPublishedSeq {
    #[iden = "outbox_published_seq"]
    Table,
    #[iden = "domain"]
    Domain,
    #[iden = "root"]
    Root,
    #[iden = "max_sequence"]
    MaxSequence,
}

/// Extract `(domain, root_hex, max_page_sequence)` from an EventBook.
///
/// `max_page_sequence` is the last page's sequence (pages are ordered
/// by sequence). An empty book yields `max_page_sequence = 0`. A book
/// with no root yields `root_hex = ""` — root-less events all share a
/// single bucket per domain, which is acceptable for this transport
/// (root-less ordering is undefined; sister-transport policy lives in
/// C-12 / H-09 / H-10 / C-11).
fn extract_routing_key(book: &EventBook) -> (String, String, u32) {
    let domain = book.domain().to_string();
    let root = book.root_id_hex().unwrap_or_default();
    let max_seq = book.pages.last().map(|p| p.sequence_num()).unwrap_or(0);
    (domain, root, max_seq)
}

// ============================================================================
// Configuration
// ============================================================================

/// Outbox configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct OutboxConfig {
    /// Enable outbox pattern. Default: false.
    /// Can be overridden via ANGZARR_OUTBOX_ENABLED env var.
    pub enabled: bool,
    /// Maximum retry attempts before moving to dead letter. Default: 10.
    pub max_retries: u32,
    /// Interval in seconds for background recovery. Default: 5.
    pub recovery_interval_secs: u64,
}

impl Default for OutboxConfig {
    fn default() -> Self {
        Self {
            enabled: std::env::var(OUTBOX_ENABLED_ENV_VAR)
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false),
            max_retries: 10,
            recovery_interval_secs: 5,
        }
    }
}

impl OutboxConfig {
    /// Check if outbox is enabled (config or env var).
    pub fn is_enabled(&self) -> bool {
        self.enabled
            || std::env::var(OUTBOX_ENABLED_ENV_VAR)
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false)
    }
}

// ============================================================================
// PostgreSQL Implementation
// ============================================================================

/// Outbox wrapper for PostgreSQL.
#[cfg(feature = "postgres")]
pub struct PostgresOutboxEventBus {
    inner: Arc<dyn EventBus>,
    pool: sqlx::PgPool,
    config: OutboxConfig,
}

#[cfg(feature = "postgres")]
impl PostgresOutboxEventBus {
    /// Create a new outbox-wrapped event bus.
    pub fn new(inner: Arc<dyn EventBus>, pool: sqlx::PgPool, config: OutboxConfig) -> Self {
        Self {
            inner,
            pool,
            config,
        }
    }

    /// Initialize the outbox table schema.
    pub async fn init(&self) -> std::result::Result<(), sqlx::Error> {
        let create_table = Table::create()
            .table(Outbox::Table)
            .if_not_exists()
            .col(ColumnDef::new(Outbox::Id).uuid().primary_key())
            .col(ColumnDef::new(Outbox::Domain).text().not_null())
            .col(ColumnDef::new(Outbox::Root).text().not_null())
            .col(ColumnDef::new(Outbox::EventData).binary().not_null())
            .col(
                ColumnDef::new(Outbox::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Outbox::RetryCount)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .to_string(PostgresQueryBuilder);

        sqlx::query(&create_table).execute(&self.pool).await?;

        // Index for recovery queries
        let create_index = Index::create()
            .if_not_exists()
            .name("idx_outbox_created_at")
            .table(Outbox::Table)
            .col(Outbox::CreatedAt)
            .to_string(PostgresQueryBuilder);

        sqlx::query(&create_index).execute(&self.pool).await?;

        // Per-root max-published-sequence watermark (C-13). Composite PK on
        // (domain, root) so each aggregate root in each domain has at most
        // one watermark row. Root-less events collapse to a single bucket
        // per domain — acceptable since their relative ordering is
        // undefined on this transport.
        let create_published_seq = Table::create()
            .table(OutboxPublishedSeq::Table)
            .if_not_exists()
            .col(ColumnDef::new(OutboxPublishedSeq::Domain).text().not_null())
            .col(ColumnDef::new(OutboxPublishedSeq::Root).text().not_null())
            .col(
                ColumnDef::new(OutboxPublishedSeq::MaxSequence)
                    .big_integer()
                    .not_null(),
            )
            .primary_key(
                sea_query::Index::create()
                    .col(OutboxPublishedSeq::Domain)
                    .col(OutboxPublishedSeq::Root),
            )
            .to_string(PostgresQueryBuilder);

        sqlx::query(&create_published_seq)
            .execute(&self.pool)
            .await?;

        info!("Outbox table initialized (PostgreSQL)");
        Ok(())
    }

    /// Bump the per-root published-sequence watermark.
    ///
    /// Called after a successful publish (normal path or recovery path).
    /// The watermark moves monotonically upward — `GREATEST(existing, new)`
    /// in SQL — so a recovery republish of an older event can never reset
    /// it backwards. A failure to write the watermark is logged but does
    /// not fail the publish: at-least-once is preserved, the worst case is
    /// that a future recovery cycle re-emits a slightly stale event before
    /// the watermark catches up — still bounded by per-root monotonicity
    /// because the seq=N row will not be in the outbox anymore.
    async fn bump_published_watermark(&self, domain: &str, root: &str, max_seq: u32) {
        // Use raw SQL for the UPSERT — sea-query's expression builder
        // doesn't have a portable GREATEST(...) helper across PG/SQLite.
        let sql = "INSERT INTO outbox_published_seq (domain, root, max_sequence) \
                   VALUES ($1, $2, $3) \
                   ON CONFLICT (domain, root) DO UPDATE SET \
                   max_sequence = GREATEST(outbox_published_seq.max_sequence, EXCLUDED.max_sequence)";
        if let Err(e) = sqlx::query(sql)
            .bind(domain)
            .bind(root)
            .bind(max_seq as i64)
            .execute(&self.pool)
            .await
        {
            warn!(domain = %domain, root = %root, max_seq = %max_seq, error = %e,
                  "Failed to bump outbox published-sequence watermark; recovery may still emit a stale event for this root");
        }
    }

    /// Read the per-root published-sequence watermark.
    ///
    /// Returns `None` if no watermark has ever been recorded for this
    /// `(domain, root)` — i.e., no event for this root has ever been
    /// successfully published. Recovery treats that as "not superseded"
    /// and proceeds to republish.
    async fn read_published_watermark(
        &self,
        domain: &str,
        root: &str,
    ) -> std::result::Result<Option<u32>, sqlx::Error> {
        use sqlx::Row;
        let select =
            "SELECT max_sequence FROM outbox_published_seq WHERE domain = $1 AND root = $2";
        let row = sqlx::query(select)
            .bind(domain)
            .bind(root)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| {
            let v: i64 = r.get("max_sequence");
            v.max(0) as u32
        }))
    }

    /// Recover orphaned events (events that were written but not published).
    ///
    /// Call this periodically from a background task.
    pub async fn recover_orphaned(&self) -> std::result::Result<u32, sqlx::Error> {
        use sqlx::Row;

        // Find events older than 30 seconds (publish should be <1s normally).
        //
        // Why 30 seconds? This threshold balances two concerns:
        // 1. **Avoid interfering with in-flight publishes**: A publish might take a few
        //    seconds under load. We don't want recovery to grab an event that's still
        //    being processed by its original publisher.
        // 2. **Timely recovery**: We don't want failed events sitting for minutes. 30s
        //    is long enough for any reasonable publish to complete, short enough to
        //    recover quickly after transient failures.
        //
        // Why limit to 100 records? Prevents the recovery process from overwhelming
        // the system during outages. If thousands of events pile up, we process them
        // in batches. Each interval picks up the next batch.
        let select = Query::select()
            .columns([Outbox::Id, Outbox::EventData, Outbox::RetryCount])
            .from(Outbox::Table)
            .and_where(Expr::col(Outbox::CreatedAt).lt(Expr::cust("NOW() - INTERVAL '30 seconds'")))
            // Events at or exceeding max_retries are intentionally left in the outbox.
            // Why not delete them? They represent failed deliveries that need human
            // attention — a dead letter queue. Deleting them silently loses data.
            // Operators can query for events at max_retries to investigate and either
            // manually retry (after fixing the underlying issue) or archive.
            .and_where(Expr::col(Outbox::RetryCount).lt(self.config.max_retries as i32))
            .limit(100)
            .to_string(PostgresQueryBuilder);

        let rows = sqlx::query(&select).fetch_all(&self.pool).await?;

        let mut recovered = 0u32;
        for row in rows {
            let id: Uuid = row.get("id");
            let event_data: Vec<u8> = row.get("event_data");
            let retry_count: i32 = row.get("retry_count");

            match EventBook::decode(event_data.as_slice()) {
                Ok(book) => {
                    // C-13: per-root ordering guard. If this root has
                    // already seen a successful publish at seq >=
                    // max_page_seq, this orphaned event is superseded;
                    // emitting it now would regress the consumer past
                    // newer state. Drop the row from the outbox without
                    // republishing.
                    let (domain, root, max_seq) = extract_routing_key(&book);
                    let superseded = match self.read_published_watermark(&domain, &root).await {
                        Ok(Some(watermark)) => watermark >= max_seq,
                        Ok(None) => false,
                        Err(e) => {
                            // Fail-safe: on a watermark read error, treat
                            // as not-superseded and proceed to republish.
                            // Preserves at-least-once at the cost of a
                            // possible duplicate (consumer should be
                            // idempotent — sequence-based dedup).
                            warn!(id = %id, domain = %domain, root = %root, error = %e,
                                  "Failed to read outbox published-sequence watermark; proceeding with republish (at-least-once preserved)");
                            false
                        }
                    };

                    if superseded {
                        info!(id = %id, domain = %domain, root = %root, max_seq = %max_seq,
                              "Outbox recovery skipping superseded event (C-13): newer event for this root has already been published");
                        let delete = Query::delete()
                            .from_table(Outbox::Table)
                            .and_where(Expr::col(Outbox::Id).eq(id.to_string()))
                            .to_string(PostgresQueryBuilder);
                        let _ = sqlx::query(&delete).execute(&self.pool).await;
                        continue;
                    }

                    match self.inner.publish(Arc::new(book)).await {
                        Ok(_) => {
                            // Bump the watermark BEFORE deleting the row so
                            // a concurrent recovery pass (or restart between
                            // these statements) cannot later re-emit a
                            // superseded event past a fresh watermark.
                            self.bump_published_watermark(&domain, &root, max_seq).await;

                            // Delete from outbox
                            let delete = Query::delete()
                                .from_table(Outbox::Table)
                                .and_where(Expr::col(Outbox::Id).eq(id.to_string()))
                                .to_string(PostgresQueryBuilder);

                            if let Err(e) = sqlx::query(&delete).execute(&self.pool).await {
                                error!(id = %id, error = %e, "Failed to delete recovered event from outbox");
                            } else {
                                recovered += 1;
                                debug!(id = %id, "Recovered orphaned event");
                            }
                        }
                        Err(e) => {
                            // Increment retry count
                            warn!(id = %id, retry_count = retry_count + 1, error = %e, "Failed to recover event, incrementing retry count");
                            let update = Query::update()
                                .table(Outbox::Table)
                                .value(Outbox::RetryCount, retry_count + 1)
                                .and_where(Expr::col(Outbox::Id).eq(id.to_string()))
                                .to_string(PostgresQueryBuilder);

                            let _ = sqlx::query(&update).execute(&self.pool).await;
                        }
                    }
                }
                Err(e) => {
                    // Why delete corrupt events immediately (vs keeping them)?
                    // Corrupt data cannot be recovered by retry — it's fundamentally broken.
                    // Keeping it wastes storage and pollutes metrics. The error log provides
                    // an audit trail; operators can investigate from there if needed.
                    // Unlike max-retry events (which might succeed after fixing infra),
                    // corrupt events are definitively unrecoverable.
                    error!(id = %id, error = %e, "Failed to decode orphaned event, removing from outbox");
                    let delete = Query::delete()
                        .from_table(Outbox::Table)
                        .and_where(Expr::col(Outbox::Id).eq(id.to_string()))
                        .to_string(PostgresQueryBuilder);

                    let _ = sqlx::query(&delete).execute(&self.pool).await;
                }
            }
        }

        if recovered > 0 {
            info!(
                recovered = recovered,
                "Recovered orphaned events from outbox"
            );
        }

        Ok(recovered)
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl EventBus for PostgresOutboxEventBus {
    #[tracing::instrument(name = "bus.publish", skip_all, fields(domain = %book.domain()))]
    async fn publish(&self, book: Arc<EventBook>) -> Result<PublishResult> {
        let id = Uuid::new_v4();
        let event_data = book.encode_to_vec();

        // Extract routing key + max page sequence up front; the book
        // moves into `self.inner.publish` below.
        let (domain, root, max_seq) = extract_routing_key(&book);

        // Step 1: Write to outbox
        let insert = Query::insert()
            .into_table(Outbox::Table)
            .columns([Outbox::Id, Outbox::Domain, Outbox::Root, Outbox::EventData])
            .values_panic([
                id.to_string().into(),
                domain.clone().into(),
                root.clone().into(),
                event_data.into(),
            ])
            .to_string(PostgresQueryBuilder);

        sqlx::query(&insert)
            .execute(&self.pool)
            .await
            .map_err(|e| BusError::Publish(format!("Outbox insert failed: {}", e)))?;

        debug!(id = %id, domain = %domain, "Event written to outbox");

        // Step 2: Publish to inner bus
        let result = self.inner.publish(book).await;

        // Step 3: Delete from outbox on success
        if result.is_ok() {
            // Bump the per-root published-sequence watermark BEFORE
            // deleting the outbox row. If the process crashes between
            // these two statements, the row will appear orphaned to a
            // future recovery — but the watermark will also be at this
            // event's sequence, so recovery will see seq <= watermark
            // and drop the row (C-13 invariant). Order matters here:
            // bump first so the watermark is monotone-correct from the
            // moment the outbox row could re-surface.
            self.bump_published_watermark(&domain, &root, max_seq).await;

            let delete = Query::delete()
                .from_table(Outbox::Table)
                .and_where(Expr::col(Outbox::Id).eq(id.to_string()))
                .to_string(PostgresQueryBuilder);

            if let Err(e) = sqlx::query(&delete).execute(&self.pool).await {
                // Log but don't fail - event was published, recovery will clean up
                warn!(id = %id, error = %e, "Failed to delete from outbox after successful publish");
            } else {
                debug!(id = %id, "Event removed from outbox after successful publish");
            }
        } else {
            debug!(id = %id, "Publish failed, event remains in outbox for recovery");
        }

        result
    }

    async fn subscribe(&self, handler: Box<dyn EventHandler>) -> Result<()> {
        self.inner.subscribe(handler).await
    }

    async fn start_consuming(&self) -> Result<()> {
        self.inner.start_consuming().await
    }

    async fn create_subscriber(
        &self,
        name: &str,
        domain_filter: Option<&str>,
    ) -> Result<Arc<dyn EventBus>> {
        self.inner.create_subscriber(name, domain_filter).await
    }
}

// ============================================================================
// SQLite Implementation (always compiled)
// ============================================================================

/// Outbox wrapper for SQLite.
pub struct SqliteOutboxEventBus {
    inner: Arc<dyn EventBus>,
    pool: sqlx::SqlitePool,
    config: OutboxConfig,
}

impl SqliteOutboxEventBus {
    /// Create a new outbox-wrapped event bus.
    pub fn new(inner: Arc<dyn EventBus>, pool: sqlx::SqlitePool, config: OutboxConfig) -> Self {
        Self {
            inner,
            pool,
            config,
        }
    }

    /// Initialize the outbox table schema.
    pub async fn init(&self) -> std::result::Result<(), sqlx::Error> {
        let create_table = Table::create()
            .table(Outbox::Table)
            .if_not_exists()
            .col(ColumnDef::new(Outbox::Id).text().primary_key())
            .col(ColumnDef::new(Outbox::Domain).text().not_null())
            .col(ColumnDef::new(Outbox::Root).text().not_null())
            .col(ColumnDef::new(Outbox::EventData).blob().not_null())
            .col(
                ColumnDef::new(Outbox::CreatedAt)
                    .text()
                    .not_null()
                    .default(Expr::cust("(datetime('now'))")),
            )
            .col(
                ColumnDef::new(Outbox::RetryCount)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .to_string(SqliteQueryBuilder);

        sqlx::query(&create_table).execute(&self.pool).await?;

        // Index for recovery queries
        let create_index = Index::create()
            .if_not_exists()
            .name("idx_outbox_created_at")
            .table(Outbox::Table)
            .col(Outbox::CreatedAt)
            .to_string(SqliteQueryBuilder);

        sqlx::query(&create_index).execute(&self.pool).await?;

        // Per-root max-published-sequence watermark (C-13). Same shape
        // as the Postgres twin; SQLite supports `ON CONFLICT ... DO UPDATE`
        // and `MAX(a, b)` so the upsert is portable.
        let create_published_seq = Table::create()
            .table(OutboxPublishedSeq::Table)
            .if_not_exists()
            .col(ColumnDef::new(OutboxPublishedSeq::Domain).text().not_null())
            .col(ColumnDef::new(OutboxPublishedSeq::Root).text().not_null())
            .col(
                ColumnDef::new(OutboxPublishedSeq::MaxSequence)
                    .big_integer()
                    .not_null(),
            )
            .primary_key(
                sea_query::Index::create()
                    .col(OutboxPublishedSeq::Domain)
                    .col(OutboxPublishedSeq::Root),
            )
            .to_string(SqliteQueryBuilder);

        sqlx::query(&create_published_seq)
            .execute(&self.pool)
            .await?;

        info!("Outbox table initialized (SQLite)");
        Ok(())
    }

    /// Bump the per-root published-sequence watermark.
    ///
    /// See `PostgresOutboxEventBus::bump_published_watermark` for the
    /// monotonicity contract and at-least-once trade-off. SQLite uses
    /// `MAX(a, b)` rather than PG's `GREATEST(a, b)` — same semantics.
    async fn bump_published_watermark(&self, domain: &str, root: &str, max_seq: u32) {
        let sql = "INSERT INTO outbox_published_seq (domain, root, max_sequence) \
                   VALUES (?, ?, ?) \
                   ON CONFLICT (domain, root) DO UPDATE SET \
                   max_sequence = MAX(outbox_published_seq.max_sequence, excluded.max_sequence)";
        if let Err(e) = sqlx::query(sql)
            .bind(domain)
            .bind(root)
            .bind(max_seq as i64)
            .execute(&self.pool)
            .await
        {
            warn!(domain = %domain, root = %root, max_seq = %max_seq, error = %e,
                  "Failed to bump outbox published-sequence watermark; recovery may still emit a stale event for this root");
        }
    }

    /// Read the per-root published-sequence watermark, or None if no
    /// publish for this `(domain, root)` has ever been recorded.
    async fn read_published_watermark(
        &self,
        domain: &str,
        root: &str,
    ) -> std::result::Result<Option<u32>, sqlx::Error> {
        use sqlx::Row;
        let select = "SELECT max_sequence FROM outbox_published_seq WHERE domain = ? AND root = ?";
        let row = sqlx::query(select)
            .bind(domain)
            .bind(root)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| {
            let v: i64 = r.get("max_sequence");
            v.max(0) as u32
        }))
    }

    /// Recover orphaned events (events that were written but not published).
    pub async fn recover_orphaned(&self) -> std::result::Result<u32, sqlx::Error> {
        use sqlx::Row;

        // Find events older than 30 seconds.
        // See PostgreSQL version for detailed rationale on timing and batch size.
        let select = Query::select()
            .columns([Outbox::Id, Outbox::EventData, Outbox::RetryCount])
            .from(Outbox::Table)
            .and_where(
                Expr::col(Outbox::CreatedAt).lt(Expr::cust("datetime('now', '-30 seconds')")),
            )
            // Events at max_retries are kept as a dead letter queue (not deleted).
            .and_where(Expr::col(Outbox::RetryCount).lt(self.config.max_retries as i32))
            .limit(100)
            .to_string(SqliteQueryBuilder);

        let rows = sqlx::query(&select).fetch_all(&self.pool).await?;

        let mut recovered = 0u32;
        for row in rows {
            let id: String = row.get("id");
            let event_data: Vec<u8> = row.get("event_data");
            let retry_count: i32 = row.get("retry_count");

            if self.try_recover_event(&id, &event_data, retry_count).await {
                recovered += 1;
            }
        }

        if recovered > 0 {
            info!(
                recovered = recovered,
                "Recovered orphaned events from outbox"
            );
        }

        Ok(recovered)
    }

    /// Attempt to recover a single orphaned event.
    ///
    /// Returns true if the event was successfully recovered and deleted.
    ///
    /// C-13 invariant: before republishing, this checks the per-root
    /// published-sequence watermark. If a newer event for the same root
    /// has already been published, the orphaned event is **superseded**
    /// — re-emitting it would regress the consumer past newer state.
    /// Superseded rows are deleted from the outbox WITHOUT publishing.
    /// This intentionally trades the strict at-least-once contract for
    /// monotonic per-root ordering on the success-and-superseded
    /// branch; the consumer has already observed seq > max_page_seq, so
    /// the redundant delivery would be both useless and harmful.
    async fn try_recover_event(&self, id: &str, event_data: &[u8], retry_count: i32) -> bool {
        let book = match EventBook::decode(event_data) {
            Ok(b) => b,
            Err(e) => {
                // Corrupt events are deleted immediately — they can't be fixed by retry.
                error!(id = %id, error = %e, "Failed to decode orphaned event");
                self.delete_outbox_entry(id).await;
                return false;
            }
        };

        let (domain, root, max_seq) = extract_routing_key(&book);

        // C-13 ordering guard.
        let superseded = match self.read_published_watermark(&domain, &root).await {
            Ok(Some(watermark)) => watermark >= max_seq,
            Ok(None) => false,
            Err(e) => {
                // Fail-safe: on a watermark read error, treat as
                // not-superseded and proceed to republish — preserves
                // at-least-once at the cost of a possible duplicate.
                warn!(id = %id, domain = %domain, root = %root, error = %e,
                      "Failed to read outbox published-sequence watermark; proceeding with republish (at-least-once preserved)");
                false
            }
        };
        if superseded {
            info!(id = %id, domain = %domain, root = %root, max_seq = %max_seq,
                  "Outbox recovery skipping superseded event (C-13): newer event for this root has already been published");
            self.delete_outbox_entry(id).await;
            return false;
        }

        match self.inner.publish(Arc::new(book)).await {
            Ok(_) => {
                // Bump watermark BEFORE deleting so a crash between
                // these two writes leaves the row still subject to the
                // C-13 guard on the next recovery pass.
                self.bump_published_watermark(&domain, &root, max_seq).await;
                if self.delete_outbox_entry(id).await {
                    debug!(id = %id, "Recovered orphaned event");
                    true
                } else {
                    false
                }
            }
            Err(e) => {
                warn!(id = %id, retry_count = retry_count + 1, error = %e, "Failed to recover event");
                self.increment_retry_count(id, retry_count).await;
                false
            }
        }
    }

    /// Delete an outbox entry by ID.
    async fn delete_outbox_entry(&self, id: &str) -> bool {
        let delete = Query::delete()
            .from_table(Outbox::Table)
            .and_where(Expr::col(Outbox::Id).eq(id))
            .to_string(SqliteQueryBuilder);

        match sqlx::query(&delete).execute(&self.pool).await {
            Ok(_) => true,
            Err(e) => {
                error!(id = %id, error = %e, "Failed to delete from outbox");
                false
            }
        }
    }

    /// Increment the retry count for a failed recovery attempt.
    async fn increment_retry_count(&self, id: &str, current: i32) {
        let update = Query::update()
            .table(Outbox::Table)
            .value(Outbox::RetryCount, current + 1)
            .and_where(Expr::col(Outbox::Id).eq(id))
            .to_string(SqliteQueryBuilder);

        let _ = sqlx::query(&update).execute(&self.pool).await;
    }
}

#[async_trait]
impl EventBus for SqliteOutboxEventBus {
    #[tracing::instrument(name = "bus.publish", skip_all, fields(domain = %book.domain()))]
    async fn publish(&self, book: Arc<EventBook>) -> Result<PublishResult> {
        let id = Uuid::new_v4();
        let event_data = book.encode_to_vec();

        // Extract routing key + max page sequence up front; the book
        // moves into `self.inner.publish` below.
        let (domain, root, max_seq) = extract_routing_key(&book);

        // Step 1: Write to outbox
        let insert = Query::insert()
            .into_table(Outbox::Table)
            .columns([Outbox::Id, Outbox::Domain, Outbox::Root, Outbox::EventData])
            .values_panic([
                id.to_string().into(),
                domain.clone().into(),
                root.clone().into(),
                event_data.into(),
            ])
            .to_string(SqliteQueryBuilder);

        sqlx::query(&insert)
            .execute(&self.pool)
            .await
            .map_err(|e| BusError::Publish(format!("Outbox insert failed: {}", e)))?;

        debug!(id = %id, domain = %domain, "Event written to outbox");

        // Step 2: Publish to inner bus
        let result = self.inner.publish(book).await;

        // Step 3: Delete from outbox on success
        if result.is_ok() {
            // Bump the per-root published-sequence watermark BEFORE
            // deleting the outbox row. See the Postgres twin's comment
            // for the crash-safety rationale; the order matters for
            // C-13's per-root ordering invariant.
            self.bump_published_watermark(&domain, &root, max_seq).await;

            let delete = Query::delete()
                .from_table(Outbox::Table)
                .and_where(Expr::col(Outbox::Id).eq(id.to_string()))
                .to_string(SqliteQueryBuilder);

            if let Err(e) = sqlx::query(&delete).execute(&self.pool).await {
                warn!(id = %id, error = %e, "Failed to delete from outbox after successful publish");
            } else {
                debug!(id = %id, "Event removed from outbox after successful publish");
            }
        }

        result
    }

    async fn subscribe(&self, handler: Box<dyn EventHandler>) -> Result<()> {
        self.inner.subscribe(handler).await
    }

    async fn start_consuming(&self) -> Result<()> {
        self.inner.start_consuming().await
    }

    async fn create_subscriber(
        &self,
        name: &str,
        domain_filter: Option<&str>,
    ) -> Result<Arc<dyn EventBus>> {
        self.inner.create_subscriber(name, domain_filter).await
    }
}

// ============================================================================
// Background Recovery Task
// ============================================================================

/// Handle to a running recovery task.
pub struct RecoveryTaskHandle {
    cancel: tokio::sync::watch::Sender<bool>,
}

impl RecoveryTaskHandle {
    /// Signal the recovery task to stop.
    pub fn stop(&self) {
        let _ = self.cancel.send(true);
    }
}

/// Spawn a background task that periodically recovers orphaned events.
///
/// Returns a handle that can be used to stop the task.
#[cfg(feature = "postgres")]
pub fn spawn_postgres_recovery_task(
    outbox: Arc<PostgresOutboxEventBus>,
    interval_secs: u64,
) -> RecoveryTaskHandle {
    let (cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);

    tokio::spawn(async move {
        let interval = std::time::Duration::from_secs(interval_secs);
        info!(
            interval_secs = interval_secs,
            "Outbox recovery task started"
        );

        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    if let Err(e) = outbox.recover_orphaned().await {
                        error!(error = %e, "Outbox recovery failed");
                    }
                }
                _ = cancel_rx.changed() => {
                    if *cancel_rx.borrow() {
                        info!("Outbox recovery task stopped");
                        break;
                    }
                }
            }
        }
    });

    RecoveryTaskHandle { cancel: cancel_tx }
}

/// Spawn a background task that periodically recovers orphaned events.
pub fn spawn_sqlite_recovery_task(
    outbox: Arc<SqliteOutboxEventBus>,
    interval_secs: u64,
) -> RecoveryTaskHandle {
    let (cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);

    tokio::spawn(async move {
        let interval = std::time::Duration::from_secs(interval_secs);
        info!(
            interval_secs = interval_secs,
            "Outbox recovery task started"
        );

        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    if let Err(e) = outbox.recover_orphaned().await {
                        error!(error = %e, "Outbox recovery failed");
                    }
                }
                _ = cancel_rx.changed() => {
                    if *cancel_rx.borrow() {
                        info!("Outbox recovery task stopped");
                        break;
                    }
                }
            }
        }
    });

    RecoveryTaskHandle { cancel: cancel_tx }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
