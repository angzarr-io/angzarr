//! Per-root ordering helpers shared by batch-delivering transports.

use std::collections::HashSet;

use super::error::{BusError, Result};
use crate::proto::EventBook;
use crate::proto_ext::CoverExt;

/// Per-root ordering key (hex aggregate root) for an ordered publish.
///
/// Ordered transports key delivery order on the root; a book without one
/// would be published unordered, silently breaking per-root ordering, so it
/// is rejected. `transport` names the backend in the error.
pub fn require_ordering_key(book: &EventBook, transport: &str) -> Result<String> {
    match book.root_id_hex() {
        Some(key) if !key.is_empty() => Ok(key),
        _ => Err(BusError::Publish(format!(
            "EventBook has no aggregate root: {} publishes are ordered per root and \
             require one on the cover",
            transport
        ))),
    }
}

/// Ordering groups (aggregate roots) that failed earlier in the current
/// receive batch.
///
/// Ordered transports that deliver in batches (SQS FIFO message groups,
/// Pub/Sub ordering keys) can hand a consumer several messages of one root
/// at once. When one fails it is redelivered later; handling and
/// acknowledging the root's later messages meanwhile would apply that
/// root's events out of order. Later messages of a failed group are
/// therefore not handled in this batch and come back behind the failed
/// one. A message whose group is unknown is treated as belonging to any
/// failed group.
#[derive(Debug, Default)]
pub struct FailedGroups {
    groups: HashSet<String>,
    unknown_group_failed: bool,
}

impl FailedGroups {
    /// Whether a message of `group` must be skipped for the rest of the batch.
    pub fn is_blocked(&self, group: Option<&str>) -> bool {
        if self.unknown_group_failed {
            return true;
        }
        match group {
            Some(g) => self.groups.contains(g),
            None => !self.groups.is_empty(),
        }
    }

    /// Record that a message of `group` failed.
    pub fn record_failure(&mut self, group: Option<&str>) {
        match group {
            Some(g) => {
                self.groups.insert(g.to_string());
            }
            None => self.unknown_group_failed = true,
        }
    }
}

#[cfg(test)]
#[path = "ordering.test.rs"]
mod tests;
