//! Database schema definitions using sea-query.
//!
//! These define the table and column identifiers for type-safe query building.
//! Schema creation is handled via sqlx migrations (see `migrations/`).

use sea_query::Iden;

/// Events table schema.
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
    #[iden = "created_at"]
    CreatedAt,
    #[iden = "event_data"]
    EventData,
    #[iden = "correlation_id"]
    CorrelationId,
    #[iden = "external_id"]
    ExternalId,
    // Source tracking for saga-produced events (angzarr_deferred)
    #[iden = "source_edition"]
    SourceEdition,
    #[iden = "source_domain"]
    SourceDomain,
    #[iden = "source_root"]
    SourceRoot,
    #[iden = "source_seq"]
    SourceSeq,
    #[iden = "source_component"]
    SourceComponent,
    #[iden = "source_command_index"]
    SourceCommandIndex,
    // Cascade tracking for 2PC (Phase 5)
    #[iden = "committed"]
    Committed,
    #[iden = "cascade_id"]
    CascadeId,
    // Parent-aggregate routing cover (Cover.ext), serialized google.protobuf.Any.
    #[iden = "ext"]
    Ext,
}

/// Snapshots table schema.
///
/// Primary key is (domain, edition, root, sequence) to support multiple
/// snapshots per aggregate with different retention policies.
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
    #[iden = "state_data"]
    StateData,
    #[iden = "retention"]
    Retention,
    #[iden = "created_at"]
    CreatedAt,
}

/// Positions table schema.
///
/// Tracks last-processed event sequence per handler/domain/edition/root.
/// Used by projectors and sagas to resume from their last checkpoint.
#[derive(Iden)]
pub enum Positions {
    Table,
    #[iden = "handler"]
    Handler,
    #[iden = "domain"]
    Domain,
    #[iden = "edition"]
    Edition,
    #[iden = "root"]
    Root,
    #[iden = "sequence"]
    Sequence,
    #[iden = "updated_at"]
    UpdatedAt,
}
