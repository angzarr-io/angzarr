//! How often the kernel stores the snapshot a domain's state offers: the
//! framework's storage policy, configured per domain
//! (docs/design/snapshots.md §1). Snapshots are never business data, so
//! business code never decides when one is taken: a state offers itself
//! with every answer ([`State::snapshot`](super::State::snapshot)), and
//! this decides which offers the kernel writes.
//!
//! - **Frequency**, per domain: a snapshot is written at a write's last
//!   event once at least `every` events have been written since the
//!   snapshot the write's load started from (since the stream's first
//!   event, for a load that started from none). Every write by default
//!   (`every` is 1); a domain may be given its own.
//! - **Retention** is the newest only, the default and the store's one
//!   policy: angzarr-ports' `SnapshotStore` keeps the snapshot written
//!   last, each write replacing the one before.
//!
//! A snapshot is a cache: the events are committed before it is written,
//! and one the load cannot use (its state type or rules version not the
//! registered ones: [`Catalog::snapshot_usable`](super::Catalog::snapshot_usable))
//! counts as none, so the next write that is due replaces it.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

/// The kernel's snapshot storage policy: how many events a domain's
/// stream gains between the snapshots written of it, by domain, with one
/// for every domain not named. The default writes one at every write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotPolicy {
    every: NonZeroU64,
    domains: BTreeMap<String, NonZeroU64>,
}

/// The default policy: a snapshot at every write, of every domain.
pub(crate) static EVERY_WRITE: SnapshotPolicy = SnapshotPolicy::every_write();

impl Default for SnapshotPolicy {
    fn default() -> Self {
        SnapshotPolicy::every_write()
    }
}

impl SnapshotPolicy {
    /// A snapshot at every write, of every domain: the default.
    pub const fn every_write() -> SnapshotPolicy {
        SnapshotPolicy {
            every: NonZeroU64::MIN,
            domains: BTreeMap::new(),
        }
    }

    /// A snapshot of every domain's stream once it has gained `events`
    /// events since its last.
    pub const fn every(events: NonZeroU64) -> SnapshotPolicy {
        SnapshotPolicy {
            every: events,
            domains: BTreeMap::new(),
        }
    }

    /// This policy, `domain`'s streams snapshotted once each has gained
    /// `events` events since its last, in place of the policy's own.
    pub fn domain(mut self, domain: impl Into<String>, events: NonZeroU64) -> SnapshotPolicy {
        self.domains.insert(domain.into(), events);
        self
    }

    /// How many events `domain`'s stream gains between its snapshots.
    pub fn events(&self, domain: &str) -> NonZeroU64 {
        self.domains.get(domain).copied().unwrap_or(self.every)
    }

    /// Whether a snapshot of a `domain` stream at `sequence`, a write's
    /// last event, is written, given the sequence of the snapshot the
    /// write's load started from (`None`: it started from none).
    pub(crate) fn due(&self, domain: &str, since: Option<u64>, sequence: u64) -> bool {
        let gained = match since {
            Some(since) => sequence.saturating_sub(since),
            None => sequence.saturating_add(1),
        };
        gained >= self.events(domain).get()
    }
}

#[cfg(test)]
#[path = "snapshot_policy.test.rs"]
mod tests;
