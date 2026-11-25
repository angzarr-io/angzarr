//! What a bus served.

use angzarr_proto::v1::Cover;
use coded_diagnostic::Report;

/// What a bus served since it last asked: how many items its handlers
/// returned handled, and every failure.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Served {
    handled: u64,
    failures: Vec<Failed>,
}

/// What became of an item that failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Outcome {
    /// Held: it will be handed again, after its retry delay. A held item
    /// holds its stream: nothing after it is handed meanwhile.
    Held,
    /// Dead-lettered and passed: its letter is kept and the stream's
    /// position is past it; it is not handed again, and the stream goes on.
    Passed,
    /// Dead-lettered, and its stream stays held where it was: a delivery
    /// waiting on a sequence no source has, given up at the retry cap. It is
    /// not handed again until an operator repairs the stream.
    DeadLettered,
}

/// One item that failed, and what became of it: held and handed again, or
/// dead-lettered (passed, or its stream held).
#[derive(Clone, Debug, PartialEq)]
pub struct Failed {
    /// The subscription's name.
    pub subscription: String,
    /// The stream.
    pub stream: Cover,
    /// The event's sequence.
    pub sequence: Option<u64>,
    /// Why.
    pub cause: Report,
    /// What became of it.
    pub outcome: Outcome,
}

impl Served {
    /// Nothing served.
    pub fn new() -> Served {
        Served::default()
    }

    /// How many items were handled.
    pub fn handled(&self) -> u64 {
        self.handled
    }

    /// What failed, in the order it did.
    pub fn failures(&self) -> &[Failed] {
        &self.failures
    }

    /// Whether nothing was handled and nothing failed.
    pub fn is_empty(&self) -> bool {
        self.handled == 0 && self.failures.is_empty()
    }

    /// `count` more handled.
    pub fn add_handled(&mut self, count: u64) {
        self.handled += count;
    }

    /// `failed` too.
    pub fn push(&mut self, failed: Failed) {
        self.failures.push(failed);
    }
}

#[cfg(test)]
#[path = "served.test.rs"]
mod tests;
