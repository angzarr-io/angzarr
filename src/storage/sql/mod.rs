//! Unified SQL storage implementations.
//!
//! This module provides shared implementations for SQL-based storage backends
//! (PostgreSQL, SQLite). The implementations are parameterized by database type
//! using the `SqlDatabase` trait.

pub(crate) mod event_store;
mod position_store;
mod query;
mod snapshot_store;

pub use position_store::SqlPositionStore;
pub use query::SqlDatabase;
pub use snapshot_store::SqlSnapshotStore;

#[cfg(feature = "postgres")]
pub mod postgres {
    //! PostgreSQL database backend.

    use sea_query::PostgresQueryBuilder;
    use sqlx::PgPool;

    /// PostgreSQL database marker type.
    pub struct Postgres;

    impl super::SqlDatabase for Postgres {
        type Pool = PgPool;

        fn build_select(stmt: sea_query::SelectStatement) -> String {
            stmt.to_string(PostgresQueryBuilder)
        }

        fn build_insert(stmt: sea_query::InsertStatement) -> String {
            stmt.to_string(PostgresQueryBuilder)
        }

        fn build_delete(stmt: sea_query::DeleteStatement) -> String {
            stmt.to_string(PostgresQueryBuilder)
        }
    }

    /// PostgreSQL position store.
    pub type PostgresPositionStore = super::SqlPositionStore<Postgres>;

    /// PostgreSQL snapshot store.
    pub type PostgresSnapshotStore = super::SqlSnapshotStore<Postgres>;
}

// SQLite is always compiled
pub mod sqlite {
    //! SQLite database backend.

    use sea_query::SqliteQueryBuilder;
    use sqlx::SqlitePool;

    /// SQLite database marker type.
    pub struct Sqlite;

    impl super::SqlDatabase for Sqlite {
        type Pool = SqlitePool;

        fn build_select(stmt: sea_query::SelectStatement) -> String {
            stmt.to_string(SqliteQueryBuilder)
        }

        fn build_insert(stmt: sea_query::InsertStatement) -> String {
            stmt.to_string(SqliteQueryBuilder)
        }

        fn build_delete(stmt: sea_query::DeleteStatement) -> String {
            stmt.to_string(SqliteQueryBuilder)
        }

        /// S1: SQLite treats NULLs as DISTINCT in unique constraints, so
        /// the column-list PK target never fires for main-timeline rows
        /// (edition stored as SQL NULL per C-15). Target the
        /// `COALESCE(edition, '')` unique expression index from migration
        /// 0007 instead — expression order matches the index definition.
        fn positions_conflict_target() -> sea_query::OnConflict {
            use sea_query::{Expr, Func, OnConflict, SimpleExpr};

            use crate::storage::schema::Positions;

            let mut target = OnConflict::new();
            target.exprs([
                SimpleExpr::from(Expr::col(Positions::Handler)),
                SimpleExpr::from(Expr::col(Positions::Domain)),
                SimpleExpr::from(Func::coalesce([
                    Expr::col(Positions::Edition).into(),
                    Expr::val("").into(),
                ])),
                SimpleExpr::from(Expr::col(Positions::Root)),
            ]);
            target
        }

        /// S1: same NULL-distinctness rationale as
        /// [`Self::positions_conflict_target`], for the snapshots upsert.
        fn snapshots_conflict_target() -> sea_query::OnConflict {
            use sea_query::{Expr, Func, OnConflict, SimpleExpr};

            use crate::storage::schema::Snapshots;

            let mut target = OnConflict::new();
            target.exprs([
                SimpleExpr::from(Expr::col(Snapshots::Domain)),
                SimpleExpr::from(Func::coalesce([
                    Expr::col(Snapshots::Edition).into(),
                    Expr::val("").into(),
                ])),
                SimpleExpr::from(Expr::col(Snapshots::Root)),
                SimpleExpr::from(Expr::col(Snapshots::Sequence)),
            ]);
            target
        }
    }

    /// SQLite position store.
    pub type SqlitePositionStore = super::SqlPositionStore<Sqlite>;

    /// SQLite snapshot store.
    pub type SqliteSnapshotStore = super::SqlSnapshotStore<Sqlite>;
}
