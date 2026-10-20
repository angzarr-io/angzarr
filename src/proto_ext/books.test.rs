//! Tests for [`EventBookExt`].
//!
//! Why this matters: the coordinator stamps expected_sequence and sequences
//! from this value; an off-by-one either rejects every command or overwrites
//! history.

use super::*;
use crate::proto::{EventPage, Snapshot};

fn page(sequence: u64) -> EventPage {
    EventPage {
        sequence,
        ..Default::default()
    }
}

#[test]
fn test_empty_book_starts_at_zero() {
    assert_eq!(EventBook::default().computed_next_sequence(), 0);
}

#[test]
fn test_last_page_wins() {
    let book = EventBook {
        pages: vec![page(6), page(7)],
        snapshot: Some(Snapshot {
            sequence: 5,
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_eq!(book.computed_next_sequence(), 8);
}

#[test]
fn test_snapshot_only_book() {
    let book = EventBook {
        snapshot: Some(Snapshot {
            sequence: 50,
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_eq!(book.computed_next_sequence(), 51);
}
