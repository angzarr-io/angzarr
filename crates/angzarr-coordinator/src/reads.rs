//! The read half's typed absence.

use crate::{Reach, Served};

/// A coordinator with no read half: what the edge is generic over where a
/// frontend serves commands alone. The reads engine's trait is implemented
/// for it where that engine is linked.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NoReads;

/// A coordinator with no read half serves no domain.
impl Served for NoReads {
    fn reach(&self, domain: &str) -> Reach {
        let _ = domain;
        Reach::None
    }
}

#[cfg(test)]
#[path = "reads.test.rs"]
mod tests;
