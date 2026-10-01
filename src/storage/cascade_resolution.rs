//! Per-participant cascade (2PC) resolution for backends that answer the
//! reaper's queries in application code.
//!
//! The SQL backends express these rules as correlated `NOT EXISTS`
//! subqueries; the key-addressed backends (Bigtable, DynamoDB, Mock) fetch
//! the cascade rows and apply the same rules here, so every backend answers
//! [`EventStore::query_stale_cascades`](crate::storage::EventStore::query_stale_cascades)
//! and
//! [`EventStore::query_cascade_participants`](crate::storage::EventStore::query_cascade_participants)
//! identically.
//!
//! A participant is one `(cascade_id, domain, edition, root)`. It is
//! RESOLVED once it holds a committed row for that cascade (a Confirmation
//! or Revocation). Resolution is per participant: one participant's
//! Revocation does not resolve the others.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::storage::timeline::{reported_edition, storage_edition};
use crate::storage::CascadeParticipant;

/// One event row that carries a cascade id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CascadeRow {
    /// Cascade the row belongs to.
    pub cascade_id: String,
    /// Aggregate domain.
    pub domain: String,
    /// Aggregate edition (any main-timeline spelling).
    pub edition: String,
    /// Aggregate root.
    pub root: Uuid,
    /// Event sequence.
    pub sequence: u32,
    /// `true` for committed rows (Confirmation/Revocation markers and
    /// ordinary committed events), `false` for provisional (`no_commit`)
    /// rows.
    pub committed: bool,
    /// Row creation time; `None` when the row carries no timestamp.
    pub created_at: Option<DateTime<Utc>>,
}

type ParticipantKey = (String, String, String, Uuid);

fn participant_key(row: &CascadeRow) -> ParticipantKey {
    (
        row.cascade_id.clone(),
        row.domain.clone(),
        storage_edition(&row.edition).to_string(),
        row.root,
    )
}

fn resolved_participants(rows: &[CascadeRow]) -> HashSet<ParticipantKey> {
    rows.iter()
        .filter(|row| row.committed)
        .map(participant_key)
        .collect()
}

/// Cascade ids with at least one unresolved participant whose provisional
/// row was created strictly before `threshold`. Sorted, without duplicates.
///
/// Rows without a timestamp are never stale (their age is unknown).
pub fn stale_cascade_ids(rows: &[CascadeRow], threshold: DateTime<Utc>) -> Vec<String> {
    let resolved = resolved_participants(rows);
    let stale: BTreeSet<String> = rows
        .iter()
        .filter(|row| !row.committed)
        .filter(|row| row.created_at.is_some_and(|created| created < threshold))
        .filter(|row| !resolved.contains(&participant_key(row)))
        .map(|row| row.cascade_id.clone())
        .collect();
    stale.into_iter().collect()
}

/// Unresolved participants of `cascade_id` with the sequences of their
/// provisional rows (ascending). Participants are ordered by
/// `(domain, edition, root)`; the main timeline is reported as `""`.
pub fn unresolved_participants(rows: &[CascadeRow], cascade_id: &str) -> Vec<CascadeParticipant> {
    let resolved = resolved_participants(rows);
    let mut grouped: BTreeMap<(String, String, Uuid), Vec<u32>> = BTreeMap::new();
    for row in rows
        .iter()
        .filter(|row| row.cascade_id == cascade_id && !row.committed)
        .filter(|row| !resolved.contains(&participant_key(row)))
    {
        grouped
            .entry((
                row.domain.clone(),
                reported_edition(&row.edition).to_string(),
                row.root,
            ))
            .or_default()
            .push(row.sequence);
    }
    grouped
        .into_iter()
        .map(|((domain, edition, root), mut sequences)| {
            sequences.sort_unstable();
            sequences.dedup();
            CascadeParticipant {
                domain,
                edition,
                root,
                sequences,
            }
        })
        .collect()
}

#[cfg(test)]
#[path = "cascade_resolution.test.rs"]
mod tests;
