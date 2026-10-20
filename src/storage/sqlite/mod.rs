//! SQLite storage: the event and snapshot stores.
//!
//! Main-timeline rows store `edition` as SQL NULL; a named edition stores its
//! name (migration 0006). Every read and write goes through
//! [`edition_predicate`] and [`edition_value`] so the two never split.

mod event_store;
mod snapshot_store;

pub(crate) use event_store::page_command_id;
pub use event_store::SqliteEventStore;
pub use snapshot_store::SqliteSnapshotStore;

use sea_query::{Expr, Iden, SimpleExpr};

/// `edition IS NULL` for main, `edition = <name>` for a named edition.
pub(crate) fn edition_predicate<T: Iden + 'static>(col: T, edition: Option<&str>) -> SimpleExpr {
    match edition {
        None => Expr::col(col).is_null(),
        Some(name) => Expr::col(col).eq(name),
    }
}

/// The stored value of an edition: NULL for main.
pub(crate) fn edition_value(edition: Option<&str>) -> SimpleExpr {
    SimpleExpr::Value(sea_query::Value::String(
        edition.map(|e| Box::new(e.to_string())),
    ))
}

#[cfg(test)]
#[path = "schema.test.rs"]
mod schema_tests;
