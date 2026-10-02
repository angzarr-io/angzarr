//! SQL database abstraction trait.

use sea_query::OnConflict;

use crate::storage::schema::{Positions, Snapshots};

/// Trait for SQL database backends.
///
/// This trait abstracts over different SQL databases (PostgreSQL, SQLite)
/// by providing the pool type and query building method.
pub trait SqlDatabase: Send + Sync + 'static {
    /// The connection pool type for this database.
    type Pool: Clone + Send + Sync;

    /// Build a SQL query string from a sea-query SELECT statement.
    fn build_select(stmt: sea_query::SelectStatement) -> String;

    /// Build a SQL query string from a sea-query INSERT statement.
    fn build_insert(stmt: sea_query::InsertStatement) -> String;

    /// Build a SQL query string from a sea-query DELETE statement.
    fn build_delete(stmt: sea_query::DeleteStatement) -> String;

    /// Upsert conflict target for the `positions` table.
    ///
    /// Default: the column-list primary key — correct for Postgres, whose
    /// PK is `UNIQUE NULLS NOT DISTINCT`, so NULL (main-timeline, C-15)
    /// editions conflict like ordinary values. SQLite overrides this with
    /// `COALESCE(edition, '')` expressions matching the unique expression
    /// index from migration 0007: SQLite treats NULLs as DISTINCT in plain
    /// unique constraints, so a column-list target can NEVER fire for
    /// main-timeline rows — every checkpoint put inserted a duplicate row
    /// and `get` returned an arbitrary one (S1: frozen checkpoints).
    fn positions_conflict_target() -> OnConflict {
        OnConflict::columns([
            Positions::Handler,
            Positions::Edition,
            Positions::Domain,
            Positions::Root,
        ])
    }

    /// Upsert conflict target for the `snapshots` table. Same S1 rationale
    /// as [`Self::positions_conflict_target`].
    fn snapshots_conflict_target() -> OnConflict {
        OnConflict::columns([
            Snapshots::Edition,
            Snapshots::Domain,
            Snapshots::Root,
            Snapshots::Sequence,
        ])
    }
}
