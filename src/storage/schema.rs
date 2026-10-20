//! Database schema identifiers for sea-query.
//!
//! Only the columns the current contract writes or reads are named here.
//! The SQLite schema (`migrations/sqlite`) still carries columns of removed
//! features (cascade, deferred sources, `Cover.ext`, the external-id fact
//! key); they keep their defaults and nothing reads them. Dropping them, and
//! adding a dedicated `command_id` index (backlog CI-1), is a schema change
//! left for an owner decision.

use sea_query::Iden;

/// Events table.
#[derive(Iden)]
pub enum Events {
    Table,
    #[iden = "domain"]
    Domain,
    #[iden = "edition"]
    Edition,
    #[iden = "root"]
    Root,
    #[iden = "sequence"]
    Sequence,
    /// `EventMeta.recorded_at`, as fixed-width RFC 3339 text (nanoseconds,
    /// `Z`), so text order is time order (see [`super::recorded_at_text`]).
    #[iden = "created_at"]
    CreatedAt,
    /// The whole `EventPage`, protobuf-encoded, meta included.
    #[iden = "event_data"]
    EventData,
    /// `EventMeta.correlation_id` as canonical UUID text.
    #[iden = "correlation_id"]
    CorrelationId,
}

/// Snapshots table. One row per stream is kept (the newest).
#[derive(Iden)]
pub enum Snapshots {
    Table,
    #[iden = "domain"]
    Domain,
    #[iden = "edition"]
    Edition,
    #[iden = "root"]
    Root,
    #[iden = "sequence"]
    Sequence,
    /// The whole `Snapshot`, protobuf-encoded (its state and rules_version).
    #[iden = "state_data"]
    StateData,
    #[iden = "created_at"]
    CreatedAt,
}
