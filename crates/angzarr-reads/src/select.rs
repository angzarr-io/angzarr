//! [`Select`]: which of a stream's pages a read answers.

use std::collections::BTreeSet;

use angzarr_proto::v1::EventPage;
use prost_types::Timestamp;

/// Which pages of each selected stream a read answers (the query's
/// `range`, `sequences` or `temporal`, without its transport).
#[derive(Clone, Debug, PartialEq)]
pub enum Select {
    /// Every page.
    All,
    /// The pages with `from <= sequence`, and `sequence < to` when `to` is
    /// set (a `SequenceRange`: `lower` inclusive, no upper: to the latest).
    Range { from: u64, to: Option<u64> },
    /// Exactly these sequences, those the stream holds.
    Sequences(BTreeSet<u64>),
    /// The pages with `sequence <= n` (a `TemporalQuery`'s `as_of_sequence`).
    AsOfSequence(u64),
    /// The pages recorded at or before the time (`as_of_time`): a prefix,
    /// `recorded_at` never falling along a stream.
    AsOfTime(Timestamp),
}

impl Select {
    /// The store read that covers this selection: `from <= sequence < to`.
    /// A read of exactly the selection's pages, but for `Sequences` (its
    /// span) and `AsOfTime` (everything): [`Select::keeps`] cuts the rest.
    pub fn bounds(&self) -> (u64, Option<u64>) {
        match self {
            Select::All | Select::AsOfTime(_) => (0, None),
            Select::Range { from, to } => (*from, *to),
            Select::Sequences(set) => match (set.first(), set.last()) {
                (Some(first), Some(last)) => (*first, last.checked_add(1)),
                _ => (0, Some(0)),
            },
            Select::AsOfSequence(n) => (0, n.checked_add(1)),
        }
    }

    /// Whether the selection keeps `page`.
    pub fn keeps(&self, page: &EventPage) -> bool {
        match self {
            Select::All => true,
            Select::Range { from, to } => {
                page.sequence >= *from && to.is_none_or(|to| page.sequence < to)
            }
            Select::Sequences(set) => set.contains(&page.sequence),
            Select::AsOfSequence(n) => page.sequence <= *n,
            Select::AsOfTime(time) => page
                .meta
                .as_ref()
                .and_then(|meta| meta.recorded_at)
                .is_some_and(|at| (at.seconds, at.nanos) <= (time.seconds, time.nanos)),
        }
    }
}

#[cfg(test)]
#[path = "select.test.rs"]
mod tests;
