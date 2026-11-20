//! What a bus served.

use angzarr_diagnostic::Report;
use angzarr_proto::v1::Cover;

/// What a bus served since it last asked: how many items its handlers
/// returned handled (events, and gone streams handed to a projector), and
/// every failure.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Served {
    handled: u64,
    failures: Vec<Failed>,
}

/// One item that failed: held and handed again, or, for a policy, what its
/// router's bug setting says.
#[derive(Clone, Debug, PartialEq)]
pub struct Failed {
    /// The subscription's name.
    pub subscription: String,
    /// The stream.
    pub stream: Cover,
    /// The event's sequence; `None` for a gone stream's.
    pub sequence: Option<u64>,
    /// Why.
    pub cause: Report,
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
