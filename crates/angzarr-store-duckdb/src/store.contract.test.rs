//! The ports' conformance suite, every section this store has the
//! capabilities for, each on a store in memory and on a file. The store's
//! handles are clones of one, which share everything, so the
//! `shared_handles*` sections run too, on `(clone, original)`. Not run:
//! `editions` and `editions_outbox` and `dead_letters_editions` (the
//! `Editions` administration is not implemented, as in the SQLite store),
//! and the migration and backup sections (this store has one format, not a
//! ledger).

use angzarr_test_dir::TestFile;

use crate::DuckStore;

/// A new, empty store in memory.
fn memory() -> DuckStore {
    DuckStore::open_in_memory().expect("a store in memory opens")
}

/// A scratch file for a store of its own: a temporary of the statement
/// that builds the store, so it is removed once the check has run.
fn scratch() -> TestFile {
    TestFile::new("angzarr-store-duckdb", "store.duckdb")
}

/// A new, empty store on `file`.
fn on(file: &TestFile) -> DuckStore {
    DuckStore::open(file).expect("a new database file opens")
}

/// Two handles on one store.
fn pair(store: DuckStore) -> (DuckStore, DuckStore) {
    (store.clone(), store)
}

/// Every section, each in a module of its own (its tests are named after
/// their checks), on stores from `$fresh` and pairs from `$pair`.
macro_rules! sections {
    ($fresh:expr, $pair:expr) => {
        mod base {
            use super::*;
            angzarr_ports::store_contract!(base, #[tokio::test], $fresh);
        }
        mod shared_handles {
            use super::*;
            angzarr_ports::store_contract!(shared_handles, #[tokio::test], $pair);
        }
        mod shared_handles_read {
            use super::*;
            angzarr_ports::store_contract!(shared_handles_read, #[tokio::test], $pair);
        }
        mod shared_handles_read_purge {
            use super::*;
            angzarr_ports::store_contract!(shared_handles_read_purge, #[tokio::test], $pair);
        }
        mod shared_handles_watch {
            use super::*;
            angzarr_ports::store_contract!(shared_handles_watch, #[tokio::test], $pair);
        }
        mod shared_handles_notify {
            use super::*;
            angzarr_ports::store_contract!(shared_handles_notify, #[tokio::test], $pair);
        }
        mod shared_handles_export {
            use super::*;
            angzarr_ports::store_contract!(shared_handles_export, #[tokio::test], $pair);
        }
        mod read_one_handle {
            use super::*;
            angzarr_ports::store_contract!(read_one_handle, #[tokio::test], $fresh);
        }
        mod read_one_handle_purge {
            use super::*;
            angzarr_ports::store_contract!(read_one_handle_purge, #[tokio::test], $fresh);
        }
        mod read_forks {
            use super::*;
            angzarr_ports::store_contract!(read_forks, #[tokio::test], $fresh);
        }
        mod watch {
            use super::*;
            angzarr_ports::store_contract!(watch, #[tokio::test], $fresh);
        }
        mod notify {
            use super::*;
            angzarr_ports::store_contract!(notify, #[tokio::test], $fresh);
        }
        mod until_time {
            use super::*;
            angzarr_ports::store_contract!(until_time, #[tokio::test], $fresh);
        }
        mod correlation {
            use super::*;
            angzarr_ports::store_contract!(correlation, #[tokio::test], $fresh);
        }
        mod list_streams {
            use super::*;
            angzarr_ports::store_contract!(list_streams, #[tokio::test], $fresh);
        }
        mod processed {
            use super::*;
            angzarr_ports::store_contract!(processed, #[tokio::test], $fresh);
        }
        mod purge {
            use super::*;
            angzarr_ports::store_contract!(purge, #[tokio::test], $fresh);
        }
        mod outbox {
            use super::*;
            angzarr_ports::store_contract!(outbox, #[tokio::test], $fresh);
        }
        mod outbox_watch {
            use super::*;
            angzarr_ports::store_contract!(outbox_watch, #[tokio::test], $fresh);
        }
        mod dead_letters {
            use super::*;
            angzarr_ports::store_contract!(dead_letters, #[tokio::test], $fresh);
        }
        mod dead_letters_watch {
            use super::*;
            angzarr_ports::store_contract!(dead_letters_watch, #[tokio::test], $fresh);
        }
        mod dead_letters_purge {
            use super::*;
            angzarr_ports::store_contract!(dead_letters_purge, #[tokio::test], $fresh);
        }
        mod export {
            use super::*;
            angzarr_ports::store_contract!(export, #[tokio::test], $fresh);
        }
        mod export_dead_letters {
            use super::*;
            angzarr_ports::store_contract!(export_dead_letters, #[tokio::test], $fresh);
        }
        mod export_every_kind {
            use super::*;
            angzarr_ports::store_contract!(export_every_kind, #[tokio::test], $fresh);
        }
    };
}

mod in_memory {
    use super::*;
    sections!(memory(), pair(memory()));
}

mod on_a_file {
    use super::*;
    sections!(on(&scratch()), pair(on(&scratch())));
}
