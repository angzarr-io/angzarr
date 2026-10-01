//! EventStore trait definition.

use async_trait::async_trait;
use uuid::Uuid;

use prost_types::Any;

use super::Result;
use crate::proto::EventPage;

/// Source tracking info for saga-produced events.
///
/// Used for idempotency: if events exist with matching source info,
/// the saga command was already processed.
///
/// The full key is (edition, domain, root, seq, component, command_index).
/// The first four identify only the triggering event; component and
/// command_index identify which emission of that trigger this is — one
/// invocation emitting several commands at the same destination (or two
/// components reacting to the same event) must not share a key (O1).
#[derive(Debug, Clone, Default)]
pub struct SourceInfo {
    /// Source edition (usually "angzarr")
    pub edition: String,
    /// Source domain (e.g., "order")
    pub domain: String,
    /// Source aggregate root UUID
    pub root: Uuid,
    /// Source event sequence that triggered the saga
    pub seq: u32,
    /// Registered name of the producing component (saga/PM).
    /// Empty on pre-upgrade rows/messages.
    pub component: String,
    /// Position of the command within the invocation's emitted command list.
    pub command_index: u32,
}

impl SourceInfo {
    /// Create new source info from saga origin.
    pub fn new(
        edition: impl Into<String>,
        domain: impl Into<String>,
        root: Uuid,
        seq: u32,
        component: impl Into<String>,
        command_index: u32,
    ) -> Self {
        Self {
            edition: edition.into(),
            domain: domain.into(),
            root,
            seq,
            component: component.into(),
            command_index,
        }
    }

    /// Check if this source info is empty/unset.
    pub fn is_empty(&self) -> bool {
        self.edition.is_empty() && self.domain.is_empty()
    }
}

/// Metadata bundled with an [`EventStore::add`] call.
///
/// Groups the book-cover fields that travel with a write so `add` keeps a small
/// signature and new cover/metadata fields (e.g. `ext`) are additive rather than
/// signature-breaking. Borrowed; construct with `..Default::default()` for the
/// fields a caller doesn't set.
#[derive(Debug, Clone, Default)]
pub struct AddMeta<'a> {
    /// Workflow correlation id (propagates across commands/events).
    pub correlation_id: &'a str,
    /// External id for fact idempotency (exactly-once), if any.
    pub external_id: Option<&'a str>,
    /// Saga source-event tracking for idempotency, if saga-produced.
    pub source_info: Option<&'a SourceInfo>,
    /// Parent-aggregate routing cover (`Cover.ext`), if present. Persisted and
    /// reconstructed so it survives a storage round-trip.
    pub ext: Option<&'a Any>,
}

/// Outcome of an `add()` operation.
///
/// Distinguishes between newly added events and duplicates detected via external_id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddOutcome {
    /// Events were persisted. Returns the sequence range of added events.
    Added {
        first_sequence: u32,
        last_sequence: u32,
    },
    /// Duplicate external_id detected. Returns the sequence range of the original events.
    /// No new events were persisted.
    Duplicate {
        first_sequence: u32,
        last_sequence: u32,
    },
}

impl AddOutcome {
    /// Returns true if events were added (not a duplicate).
    pub fn is_added(&self) -> bool {
        matches!(self, AddOutcome::Added { .. })
    }

    /// Returns true if this was a duplicate request.
    pub fn is_duplicate(&self) -> bool {
        matches!(self, AddOutcome::Duplicate { .. })
    }

    /// Returns the first sequence number regardless of outcome.
    pub fn first_sequence(&self) -> u32 {
        match self {
            AddOutcome::Added { first_sequence, .. }
            | AddOutcome::Duplicate { first_sequence, .. } => *first_sequence,
        }
    }

    /// Returns the last sequence number regardless of outcome.
    pub fn last_sequence(&self) -> u32 {
        match self {
            AddOutcome::Added { last_sequence, .. }
            | AddOutcome::Duplicate { last_sequence, .. } => *last_sequence,
        }
    }
}

/// Interface for event persistence.
///
/// All domain-scoped operations take `domain` as their first parameter,
/// followed by `edition`. The edition identifies the timeline: `""` or
/// `"angzarr"` (interchangeable) for the main timeline, or a named edition
/// (e.g., `"v2"`) for diverged timelines. Every backend keys all spellings
/// of the main timeline identically and reports it in its wire form, `""`.
///
/// The `(domain, edition, root, sequence)` tuple forms the unique key
/// for stored events.
///
/// # Idempotency
///
/// The `external_id` parameter enables exactly-once delivery semantics.
/// When provided (non-empty), the store records a claim for that external_id.
/// Subsequent requests with the same (domain, edition, root, external_id)
/// return `AddOutcome::Duplicate` with the original sequence range instead
/// of persisting duplicate events.
///
/// Pass `None` or empty string for non-idempotent operations.
///
/// # Appends
///
/// Each `add` batch must continue its stream exactly (see
/// `storage::timeline::AppendWindow`): the main timeline and editions with
/// events continue at `max + 1`; an edition's first batch may start at any
/// divergence point up to the main timeline's next sequence. Pages within a
/// batch are consecutive. A violation, or losing a concurrent race for the
/// same sequence, is `StorageError::SequenceConflict`.
///
/// Implementations:
/// - `SqliteEventStore`: SQLite storage
/// - `PostgresEventStore`: PostgreSQL storage
/// - `BigtableEventStore`: Bigtable storage
/// - `DynamoEventStore`: DynamoDB storage
/// - `ImmudbEventStore`: ImmuDB storage
/// - `MockEventStore`: In-memory mock for testing
#[async_trait]
pub trait EventStore: Send + Sync {
    /// Store events for an aggregate root.
    ///
    /// Events are appended to the existing event stream for this root.
    /// Sequence numbers are validated for consistency.
    /// The correlation_id links related events across aggregates for tracing.
    ///
    /// # Idempotency
    ///
    /// If `external_id` is `Some(id)` where `id` is non-empty:
    /// - First call: persists events and returns `AddOutcome::Added`
    /// - Subsequent calls with same external_id: returns `AddOutcome::Duplicate`
    ///   with the original sequence range (no new events persisted)
    ///
    /// If `external_id` is `None` or `Some("")`: events are always persisted.
    ///
    /// # Source Tracking
    ///
    /// If `source_info` is `Some(info)` where info is non-empty:
    /// - Source info is stored with each event for saga provenance tracking
    /// - Enables idempotency checking for saga-produced commands
    async fn add(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        events: Vec<EventPage>,
        meta: &AddMeta<'_>,
    ) -> Result<AddOutcome>;

    /// Retrieve all events for an aggregate.
    async fn get(&self, domain: &str, edition: &str, root: Uuid) -> Result<Vec<EventPage>>;

    /// Retrieve all events for an aggregate with explicit divergence point.
    ///
    /// For edition branches, the divergence point determines where the edition
    /// splits from the main timeline:
    /// - `explicit_divergence = Some(N)`: Branch starts at sequence N.
    ///   Returns main timeline events with sequence < N, plus edition events.
    /// - `explicit_divergence = None`: Uses implicit divergence (first edition event).
    ///   This is the default behavior of `get()`.
    ///
    /// # Eventless-edition contract (finding #12 — inherit main timeline)
    ///
    /// A named edition with NO events of its own AND no explicit divergence
    /// is "not diverged yet": it **inherits the entire main timeline** until
    /// it explicitly diverges (by writing its first event, or by a
    /// `Some(N)` divergence here). Every backend resolves this through
    /// `storage::timeline::resolve_divergence`.
    ///
    /// # Example: Branch at sequence 3
    /// ```text
    /// Main:   [0, 1, 2, 3, 4]
    /// Branch with explicit_divergence=3: returns [0, 1, 2] from main
    /// After adding branch event at seq 3: returns [0, 1, 2, branch_3]
    /// ```
    async fn get_with_divergence(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        explicit_divergence: Option<u32>,
    ) -> Result<Vec<EventPage>> {
        // H-20: the pre-fix default silently delegated to `get()` and
        // ignored `explicit_divergence`, so every backend that didn't
        // override this method returned the wrong events for new-branch
        // reads at a non-implicit divergence. Make the missing
        // implementation loud — fail closed so callers can route around
        // it instead of consuming silently-wrong history.
        let _ = (domain, edition, root, explicit_divergence);
        Err(super::error::StorageError::NotImplemented(format!(
            "get_with_divergence is not implemented for this backend; \
             override it on the EventStore impl to support explicit-divergence \
             reads (called with edition={:?}, explicit_divergence={:?})",
            edition, explicit_divergence
        )))
    }

    /// Retrieve events from sequence N onwards.
    async fn get_from(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
    ) -> Result<Vec<EventPage>>;

    /// Retrieve events in range [from, to).
    async fn get_from_to(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
        to: u32,
    ) -> Result<Vec<EventPage>>;

    /// List all aggregate roots in a domain within an edition.
    async fn list_roots(&self, domain: &str, edition: &str) -> Result<Vec<Uuid>>;

    /// List all domains that have events stored.
    async fn list_domains(&self) -> Result<Vec<String>>;

    /// Get the next sequence number for an aggregate.
    async fn get_next_sequence(&self, domain: &str, edition: &str, root: Uuid) -> Result<u32>;

    /// Retrieve events up to (inclusive) a timestamp.
    ///
    /// Returns events ordered by sequence ASC where created_at <= until.
    /// Used for temporal queries to reconstruct historical state.
    ///
    /// # Typed boundary (C10)
    ///
    /// `until` is a typed `prost_types::Timestamp`, not a caller-formatted
    /// string. Prior to C10 this took `until: &str`, and every SQL backend
    /// compared it lexically against a TEXT `created_at` column — a caller
    /// that built the string with a `Z` suffix instead of the producer's
    /// uniform `+00:00` corrupted the comparison (`'Z' > '+'` in ASCII),
    /// silently returning the wrong event set. Accepting the typed value
    /// here removes the possibility entirely: every backend derives its own
    /// comparable form from `until` at exactly one point
    /// (`storage::helpers::timestamp_to_rfc3339` for the TEXT-column SQL
    /// backends; a direct `chrono::DateTime::from_timestamp` for the
    /// semantic backends), so no caller-supplied string ever reaches the
    /// comparison.
    async fn get_until_timestamp(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        until: &prost_types::Timestamp,
    ) -> Result<Vec<EventPage>>;

    /// Retrieve all events with a given correlation ID across all domains.
    ///
    /// Used for tracing related events across aggregates during saga workflows.
    /// Returns EventBooks grouped by domain/root.
    async fn get_by_correlation(
        &self,
        correlation_id: &str,
    ) -> Result<Vec<crate::proto::EventBook>>;

    /// Find events by source info (for saga idempotency checking).
    ///
    /// Queries for events within the target aggregate that were produced by
    /// the specified source (saga trigger). Used to detect duplicate saga commands.
    ///
    /// Returns `Some(events)` if matching events exist, `None` if not found.
    async fn find_by_source(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        source_info: &SourceInfo,
    ) -> Result<Option<Vec<EventPage>>>;

    /// Find events by external_id (for fact-injection idempotency checking).
    ///
    /// Queries for events within the target aggregate that were persisted
    /// with the given `external_id`. Used to short-circuit fact handlers
    /// on external-system retries (e.g. Stripe webhook redelivery) before
    /// the handler is invoked. Storage-level dedup at `add()` remains the
    /// authoritative safety net.
    ///
    /// Returns `Some(events)` if matching events exist, `None` if not.
    /// Returns `None` for empty external_id (non-idempotent operations
    /// never recorded a claim, so any match would be coincidental).
    async fn find_by_external_id(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        external_id: &str,
    ) -> Result<Option<Vec<EventPage>>>;

    /// Delete all events for an edition+domain combination.
    ///
    /// Returns the number of events deleted.
    /// Note: This is a destructive operation - events cannot be recovered.
    /// The main timeline (`""` or `"angzarr"`) is refused with
    /// `StorageError::MainTimelineProtected`.
    async fn delete_edition_events(&self, domain: &str, edition: &str) -> Result<u32>;

    // =========================================================================
    // Cascade (2PC) Query Methods - Phase 5
    // =========================================================================

    /// Query cascade IDs that have at least one unresolved participant past
    /// the threshold.
    ///
    /// Used by the CascadeReaper background job to find stale cascades that
    /// need timeout-based revocation. Resolution is **per-participant** (per
    /// `(cascade_id, domain, edition, root)` tuple) — a participant is
    /// resolved when there is a committed cascade row (Confirmation or
    /// Revocation) on the same `(domain, edition, root)` for that cascade.
    /// A cascade is therefore "stale" when:
    /// - It has at least one uncommitted (`committed=false`) row
    /// - That row's `created_at` is older than `threshold`
    /// - That participant's `(domain, edition, root)` has no committed
    ///   cascade row for the same cascade_id
    ///
    /// Pre-C-02 semantics excluded the cascade globally when ANY committed
    /// row existed for that cascade_id; that stranded participants 2..N
    /// after participant 1's Revocation succeeded.
    ///
    /// # Arguments
    /// * `threshold` - ISO 8601 timestamp string. Events older than this are considered stale.
    async fn query_stale_cascades(&self, threshold: &str) -> Result<Vec<String>>;

    /// Query unresolved participants (aggregates) in a cascade.
    ///
    /// Returns a list of `(domain, edition, root, sequences)` tuples for all
    /// aggregates that have uncommitted events for the given cascade_id AND
    /// have not yet been resolved (no committed cascade row on that
    /// `(domain, edition, root)` for the same cascade_id). Used by the
    /// CascadeReaper to write Revocation events without re-revoking
    /// participants that previous reaper passes already resolved.
    async fn query_cascade_participants(&self, cascade_id: &str)
        -> Result<Vec<CascadeParticipant>>;
}

/// Information about an aggregate participating in a cascade.
#[derive(Debug, Clone)]
pub struct CascadeParticipant {
    /// Domain name of the aggregate.
    pub domain: String,
    /// Edition (timeline) of the aggregate.
    pub edition: String,
    /// Root UUID of the aggregate.
    pub root: Uuid,
    /// Sequences of uncommitted events for this cascade.
    pub sequences: Vec<u32>,
}
