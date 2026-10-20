//! Sequence arithmetic on [`EventBook`]s.

use crate::proto::EventBook;

/// Sequence helpers on a book of one stream's events.
pub trait EventBookExt {
    /// The stream's next sequence as this book describes it: the last
    /// page's sequence + 1, else the snapshot's sequence + 1, else 0
    /// (types.proto, `EventBook.next_sequence`).
    fn computed_next_sequence(&self) -> u64;
}

impl EventBookExt for EventBook {
    fn computed_next_sequence(&self) -> u64 {
        if let Some(last) = self.pages.last() {
            return last.sequence + 1;
        }
        self.snapshot.as_ref().map(|s| s.sequence + 1).unwrap_or(0)
    }
}

#[cfg(test)]
#[path = "books.test.rs"]
mod tests;
