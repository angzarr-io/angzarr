//! Book extension traits for EventBook and CommandBook.
//!
//! Provides convenience methods for working with pages and sequence numbers.

use crate::proto::{CommandBook, CommandPage, EventBook, EventPage, MergeStrategy, Snapshot};

use super::cover::CoverExt;
use super::pages::{CommandPageExt, EventPageExt};

/// Extension trait for EventBook proto type (beyond CoverExt).
///
/// Provides convenience methods for working with event pages.
pub trait EventBookExt: CoverExt {
    /// Get the next sequence number from the pre-computed field.
    ///
    /// The framework sets this on load. Returns 0 if not set.
    fn next_sequence(&self) -> u32;

    /// Check if the event book has no pages.
    fn is_empty(&self) -> bool;

    /// Get the last event page, if any.
    fn last_page(&self) -> Option<&EventPage>;

    /// Get the first event page, if any.
    fn first_page(&self) -> Option<&EventPage>;
}

/// Compute next sequence number from pages and optional snapshot.
///
/// Returns (last page sequence + 1) OR (snapshot sequence + 1) if no pages, OR 0 if neither.
/// Use this when manually constructing EventBooks or when the framework hasn't set next_sequence.
pub fn calculate_next_sequence(pages: &[EventPage], snapshot: Option<&Snapshot>) -> u32 {
    if let Some(last_page) = pages.last() {
        last_page.sequence_num() + 1
    } else {
        snapshot.map(|s| s.sequence + 1).unwrap_or(0)
    }
}

/// Calculate and set the next_sequence field on an EventBook.
pub fn calculate_set_next_seq(book: &mut EventBook) {
    book.next_sequence = calculate_next_sequence(&book.pages, book.snapshot.as_ref());
}

impl EventBookExt for EventBook {
    fn next_sequence(&self) -> u32 {
        self.next_sequence
    }

    fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    fn last_page(&self) -> Option<&EventPage> {
        self.pages.last()
    }

    fn first_page(&self) -> Option<&EventPage> {
        self.pages.first()
    }
}

/// Extension trait for CommandBook proto type (beyond CoverExt).
///
/// Provides convenience methods for working with command pages.
pub trait CommandBookExt: CoverExt {
    /// Get the sequence number from the first command page.
    fn command_sequence(&self) -> u32;

    /// Get the first command page, if any.
    fn first_command(&self) -> Option<&CommandPage>;

    /// Get the merge strategy from the first command page.
    ///
    /// Returns the MergeStrategy enum value. Defaults to Commutative if no
    /// pages; UNSPECIFIED and unknown wire values resolve to Commutative.
    fn effective_merge_strategy(&self) -> MergeStrategy;
}

impl CommandBookExt for CommandBook {
    fn command_sequence(&self) -> u32 {
        self.pages.first().map(|p| p.sequence_num()).unwrap_or(0)
    }

    fn first_command(&self) -> Option<&CommandPage> {
        self.pages.first()
    }

    fn effective_merge_strategy(&self) -> MergeStrategy {
        self.pages
            .first()
            .map(CommandPageExt::effective_merge_strategy)
            .unwrap_or(MergeStrategy::MergeCommutative)
    }
}
