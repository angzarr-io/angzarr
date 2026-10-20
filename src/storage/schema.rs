//! Database schema identifiers for sea-query: the SQLite baseline schema
//! (`migrations/sqlite/0001_baseline.sql`).

use sea_query::Iden;

/// Events table: one row per event, the page stored whole.
#[derive(Iden)]
pub enum Events {
    Table,
    #[iden = "domain"]
    Domain,
    #[iden = "edition"]
    Edition,
    /// The stream's root, 16 bytes.
    #[iden = "root"]
    Root,
    #[iden = "sequence"]
    Sequence,
    /// `EventMeta.recorded_at`, as fixed-width RFC 3339 text (nanoseconds,
    /// `Z`), so text order is time order (see [`super::recorded_at_text`]).
    #[iden = "recorded_at"]
    RecordedAt,
    /// `EventMeta.causation.command_id`, 16 bytes (NULL for process events).
    #[iden = "command_id"]
    CommandId,
    /// `EventMeta.correlation_id`, 16 bytes.
    #[iden = "correlation_id"]
    CorrelationId,
    /// The whole `EventPage`, protobuf-encoded, meta included.
    #[iden = "event_data"]
    EventData,
}

/// Snapshots table: the newest snapshot per stream.
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
    #[iden = "state_type_url"]
    StateTypeUrl,
    #[iden = "state_value"]
    StateValue,
    #[iden = "rules_version"]
    RulesVersion,
}
