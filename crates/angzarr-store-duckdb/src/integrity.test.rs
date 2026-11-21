//! The check, and the stores it must find broken: each way the extent and
//! the pages can disagree is made by hand in a file, and the check must
//! report it.

use angzarr_ports::contract::{main_stream, page};
use angzarr_ports::{EventStore, Write};

use crate::fixture::populate;
use crate::{DuckStore, Integrity};

async fn populated() -> DuckStore {
    let store = DuckStore::open_in_memory().unwrap();
    populate(&store).await;
    store
}

fn tamper(store: &DuckStore, sql: &str) {
    store
        .with(|conn| conn.execute_batch(sql).map_err(crate::error::unavailable))
        .unwrap();
}

fn problems(store: &DuckStore) -> Vec<String> {
    match store.integrity_check().unwrap() {
        Integrity::Intact => Vec::new(),
        Integrity::Damaged(damage) => damage.problems().to_vec(),
    }
}

/// A store that was only written through its ports is intact, however it
/// was written: this is what the check must not cry wolf about.
#[tokio::test]
async fn a_store_written_through_its_ports_is_intact() {
    let store = populated().await;
    assert_eq!(store.integrity_check().unwrap(), Integrity::Intact);
    let stream = main_stream("tasks", 2);
    store
        .write(Write::append(stream, 1, &[page(1, 1), page(2, 1)]))
        .await
        .unwrap();
    assert_eq!(store.integrity_check().unwrap(), Integrity::Intact);
}

/// Each way an extent can be wrong is found: a next sequence too far, a
/// first sequence off, an extent with no pages, and pages with no extent.
#[tokio::test]
async fn every_way_the_extent_can_disagree_is_found() {
    for (what, sql) in [
        (
            "next too far",
            "UPDATE streams SET next_sequence = next_sequence + 1",
        ),
        (
            "next too short",
            "UPDATE streams SET next_sequence = next_sequence - 1 WHERE domain = 'tasks'",
        ),
        (
            "first off",
            "UPDATE streams SET first_sequence = first_sequence + 1 WHERE edition = 'what-if'",
        ),
        (
            "no pages",
            "DELETE FROM events WHERE domain = 'tasks' AND edition = ''",
        ),
        (
            "no extent",
            "DELETE FROM streams WHERE domain = 'tasks' AND edition = ''",
        ),
        (
            "a page lost",
            "DELETE FROM events WHERE sequence = 1 AND edition = ''",
        ),
    ] {
        let store = populated().await;
        tamper(&store, sql);
        assert!(!problems(&store).is_empty(), "{what} was not found");
    }
}

/// A damage report says its first problem and how many more, in the
/// catalog's args.
#[test]
fn a_damage_names_its_first_problem_and_the_rest() {
    let damage = crate::damage_sample();
    assert_eq!(damage.problems(), ["x", "y"]);
    assert_eq!(
        damage.to_string(),
        concat!(
            angzarr_ports::angzarr_diagnostic::code!("damaged"),
            " first=x more=1"
        )
    );
}
