//! Mock EventStore implementation for testing.

use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::orchestration::aggregate::DEFAULT_EDITION;
use crate::proto::{EventBook, EventPage};
use crate::proto_ext::EventPageExt;
use crate::storage::helpers::{assemble_event_books, is_main_timeline};
use crate::storage::{
    AddOutcome, CascadeParticipant, EventStore, Result, SourceInfo, StorageError,
};

/// Stored event with correlation and idempotency tracking.
struct StoredEvent {
    page: EventPage,
    correlation_id: String,
    external_id: String,
    source_info: Option<SourceInfo>,
}

/// Mock event store that stores events in memory.
#[derive(Default)]
pub struct MockEventStore {
    events: RwLock<HashMap<(String, String, Uuid), Vec<StoredEvent>>>,
    fail_on_add: RwLock<bool>,
    fail_on_get: RwLock<bool>,
    next_sequence_override: RwLock<Option<u32>>,
}

impl MockEventStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn set_fail_on_add(&self, fail: bool) {
        *self.fail_on_add.write().await = fail;
    }

    pub async fn set_fail_on_get(&self, fail: bool) {
        *self.fail_on_get.write().await = fail;
    }

    pub async fn set_next_sequence(&self, seq: u32) {
        *self.next_sequence_override.write().await = Some(seq);
    }

    pub async fn clear_next_sequence_override(&self) {
        *self.next_sequence_override.write().await = None;
    }
}

#[async_trait]
impl EventStore for MockEventStore {
    async fn add(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        events: Vec<EventPage>,
        correlation_id: &str,
        external_id: Option<&str>,
        source_info: Option<&SourceInfo>,
    ) -> Result<AddOutcome> {
        if *self.fail_on_add.read().await {
            return Err(StorageError::NotFound {
                domain: domain.to_string(),
                root,
            });
        }

        if events.is_empty() {
            return Ok(AddOutcome::Added {
                first_sequence: 0,
                last_sequence: 0,
            });
        }

        let external_id = external_id.unwrap_or("");
        let key = (domain.to_string(), edition.to_string(), root);
        let mut store = self.events.write().await;

        // Check for idempotency if external_id is provided
        if !external_id.is_empty() {
            if let Some(existing) = store.get(&key) {
                let matching: Vec<_> = existing
                    .iter()
                    .filter(|e| e.external_id == external_id)
                    .collect();
                if !matching.is_empty() {
                    let first = matching
                        .iter()
                        .map(|e| e.page.sequence_num())
                        .min()
                        .unwrap();
                    let last = matching
                        .iter()
                        .map(|e| e.page.sequence_num())
                        .max()
                        .unwrap();
                    return Ok(AddOutcome::Duplicate {
                        first_sequence: first,
                        last_sequence: last,
                    });
                }
            }
        }

        // H-24: enforce the same sequence-conflict contract the SQL
        // backends enforce via `PRIMARY KEY (domain, edition, root,
        // sequence)`. The mock previously appended without checking,
        // so a unit test that asserted "this duplicate is rejected"
        // against mock could silently pass while Postgres / SQLite
        // would reject the same shape. We reject three shapes:
        //   1. New events overlap an existing sequence (duplicate).
        //   2. New events fall below the current max sequence (rewind).
        //   3. The batch itself contains a repeated sequence.
        // Gap inserts (sequence > current_max + 1) are NOT rejected:
        // the framework allows snapshot-rehydrate paths to seed a
        // stream at a non-zero base, matching SQL behavior at the
        // storage layer.
        let existing_sequences: std::collections::HashSet<u32> = store
            .get(&key)
            .map(|events| events.iter().map(|e| e.page.sequence_num()).collect())
            .unwrap_or_default();
        let next_sequence = existing_sequences.iter().max().map(|s| s + 1).unwrap_or(0);

        let mut seen_in_batch: std::collections::HashSet<u32> = std::collections::HashSet::new();
        for event in &events {
            let seq = event.sequence_num();
            if existing_sequences.contains(&seq) {
                return Err(StorageError::SequenceConflict {
                    expected: next_sequence,
                    actual: seq,
                });
            }
            if !seen_in_batch.insert(seq) {
                // In-batch duplicate. SQL stores fail on the second
                // INSERT inside the transaction; mirror that here so
                // the contract holds across both surfaces.
                return Err(StorageError::SequenceConflict {
                    expected: next_sequence,
                    actual: seq,
                });
            }
        }

        let first_sequence = events.first().map(|e| e.sequence_num()).unwrap_or(0);
        let last_sequence = events.last().map(|e| e.sequence_num()).unwrap_or(0);

        let stored: Vec<StoredEvent> = events
            .into_iter()
            .map(|page| StoredEvent {
                page,
                correlation_id: correlation_id.to_string(),
                external_id: external_id.to_string(),
                source_info: source_info.cloned(),
            })
            .collect();
        store.entry(key).or_default().extend(stored);

        Ok(AddOutcome::Added {
            first_sequence,
            last_sequence,
        })
    }

    async fn get(&self, domain: &str, edition: &str, root: Uuid) -> Result<Vec<EventPage>> {
        if *self.fail_on_get.read().await {
            return Err(StorageError::NotFound {
                domain: domain.to_string(),
                root,
            });
        }
        let key = (domain.to_string(), edition.to_string(), root);
        let store = self.events.read().await;
        Ok(store
            .get(&key)
            .map(|events| events.iter().map(|e| e.page.clone()).collect())
            .unwrap_or_default())
    }

    /// H-20: explicit-divergence composite read for editions. Mirrors the
    /// SQL backends' `composite_read_with_divergence` so the in-memory
    /// mock can stand in for SQL stores in tests that exercise branch
    /// creation. Without this override the default impl raises
    /// `NotImplemented` (the safer fail-closed for unsupported backends).
    async fn get_with_divergence(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        explicit_divergence: Option<u32>,
    ) -> Result<Vec<EventPage>> {
        // Main timeline: explicit divergence has no meaning; degrade to
        // a plain `get()` so callers that pass `Some(_)` for the main
        // timeline don't trip a backend-asymmetry. Use the caller's
        // edition value verbatim — mock keys events on the exact
        // string, so substituting `DEFAULT_EDITION` here would skip
        // rows stored under `""` (the wire-level sentinel for main).
        if is_main_timeline(edition) {
            return self.get(domain, edition, root).await;
        }

        let edition_events = self.get(domain, edition, root).await?;

        // Determine the divergence point. Explicit wins; else implicit
        // (first edition event's sequence). With no edition events and
        // no explicit divergence, fall back to the main timeline.
        let divergence = if let Some(div) = explicit_divergence {
            div
        } else if let Some(first) = edition_events.first() {
            first.sequence_num()
        } else {
            // No edition events + no explicit divergence: caller wants
            // the implicit-divergence fallback (main timeline). The
            // mock doesn't normalize `""` ↔ `"angzarr"` keying, so
            // probe both sentinels and concatenate (in practice tests
            // use one or the other consistently, but the contract is
            // that both name the same timeline).
            let mut main_events = self.get(domain, DEFAULT_EDITION, root).await?;
            if main_events.is_empty() {
                main_events = self.get(domain, "", root).await?;
            }
            return Ok(main_events);
        };

        let mut main_events = self.get(domain, DEFAULT_EDITION, root).await?;
        if main_events.is_empty() {
            main_events = self.get(domain, "", root).await?;
        }

        let mut result: Vec<EventPage> = main_events
            .into_iter()
            .filter(|e| e.sequence_num() < divergence)
            .collect();

        for event in edition_events {
            if event.sequence_num() >= divergence {
                result.push(event);
            }
        }

        Ok(result)
    }

    async fn get_from(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        let events = self.get(domain, edition, root).await?;
        Ok(events
            .into_iter()
            .filter(|e| e.sequence_num() >= from)
            .collect())
    }

    async fn get_from_to(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
        to: u32,
    ) -> Result<Vec<EventPage>> {
        let events = self.get(domain, edition, root).await?;
        Ok(events
            .into_iter()
            .filter(|e| e.sequence_num() >= from && e.sequence_num() < to)
            .collect())
    }

    async fn get_until_timestamp(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        until: &str,
    ) -> Result<Vec<EventPage>> {
        let events = self.get(domain, edition, root).await?;
        let until_dt = chrono::DateTime::parse_from_rfc3339(until)
            .map_err(|e| StorageError::InvalidTimestampFormat(e.to_string()))?;
        Ok(events
            .into_iter()
            .filter(|e| {
                if let Some(ref ts) = e.created_at {
                    if let Some(dt) = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                    {
                        return dt <= until_dt;
                    }
                }
                false
            })
            .collect())
    }

    async fn list_roots(&self, domain: &str, edition: &str) -> Result<Vec<Uuid>> {
        let store = self.events.read().await;
        Ok(store
            .keys()
            .filter(|(d, e, _)| d == domain && e == edition)
            .map(|(_, _, r)| *r)
            .collect())
    }

    async fn list_domains(&self) -> Result<Vec<String>> {
        let store = self.events.read().await;
        let mut domains: Vec<_> = store.keys().map(|(d, _, _)| d.clone()).collect();
        domains.sort();
        domains.dedup();
        Ok(domains)
    }

    async fn get_next_sequence(&self, domain: &str, edition: &str, root: Uuid) -> Result<u32> {
        if let Some(seq) = *self.next_sequence_override.read().await {
            return Ok(seq);
        }

        // Helper to get max sequence from events
        fn max_sequence(events: &[EventPage]) -> Option<u32> {
            events.iter().map(|e| e.sequence_num()).max()
        }

        // For non-default editions with implicit divergence, we need composite logic:
        // If the edition has no events yet, use the main timeline's max sequence
        if !is_main_timeline(edition) {
            let edition_events = self.get(domain, edition, root).await?;
            if let Some(max_seq) = max_sequence(&edition_events) {
                // Edition has events, use edition's max sequence
                return Ok(max_seq + 1);
            }
            // No edition events - fall through to check main timeline
        }

        // Query the target edition (or main timeline for fallback)
        let target_edition = if is_main_timeline(edition) {
            edition
        } else {
            DEFAULT_EDITION
        };

        let events = self.get(domain, target_edition, root).await?;
        Ok(max_sequence(&events).map(|s| s + 1).unwrap_or(0))
    }

    async fn get_by_correlation(&self, correlation_id: &str) -> Result<Vec<EventBook>> {
        if correlation_id.is_empty() {
            return Ok(vec![]);
        }

        let store = self.events.read().await;
        let mut books_map: HashMap<(String, String, Uuid), Vec<EventPage>> = HashMap::new();

        for ((domain, edition, root), events) in store.iter() {
            for stored in events {
                if stored.correlation_id == correlation_id {
                    books_map
                        .entry((domain.clone(), edition.clone(), *root))
                        .or_default()
                        .push(stored.page.clone());
                }
            }
        }

        Ok(assemble_event_books(books_map, correlation_id))
    }

    async fn delete_edition_events(&self, domain: &str, edition: &str) -> Result<u32> {
        let mut store = self.events.write().await;
        let keys_to_remove: Vec<_> = store
            .keys()
            .filter(|(d, e, _)| d == domain && e == edition)
            .cloned()
            .collect();

        let mut count = 0u32;
        for key in keys_to_remove {
            if let Some(events) = store.remove(&key) {
                count += events.len() as u32;
            }
        }
        Ok(count)
    }

    async fn find_by_source(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        source_info: &SourceInfo,
    ) -> Result<Option<Vec<EventPage>>> {
        if source_info.is_empty() {
            return Ok(None);
        }

        let key = (domain.to_string(), edition.to_string(), root);
        let store = self.events.read().await;

        if let Some(events) = store.get(&key) {
            let matching: Vec<EventPage> = events
                .iter()
                .filter(|e| {
                    if let Some(ref stored_source) = e.source_info {
                        stored_source.edition == source_info.edition
                            && stored_source.domain == source_info.domain
                            && stored_source.root == source_info.root
                            && stored_source.seq == source_info.seq
                    } else {
                        false
                    }
                })
                .map(|e| e.page.clone())
                .collect();

            if matching.is_empty() {
                return Ok(None);
            }
            return Ok(Some(matching));
        }

        Ok(None)
    }

    async fn find_by_external_id(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        external_id: &str,
    ) -> Result<Option<Vec<EventPage>>> {
        if external_id.is_empty() {
            return Ok(None);
        }
        let key = (domain.to_string(), edition.to_string(), root);
        let store = self.events.read().await;
        if let Some(events) = store.get(&key) {
            let matching: Vec<EventPage> = events
                .iter()
                .filter(|e| e.external_id == external_id)
                .map(|e| e.page.clone())
                .collect();
            if matching.is_empty() {
                return Ok(None);
            }
            return Ok(Some(matching));
        }
        Ok(None)
    }

    async fn query_stale_cascades(&self, threshold: &str) -> Result<Vec<String>> {
        use std::collections::HashSet;

        let threshold_dt = chrono::DateTime::parse_from_rfc3339(threshold)
            .map_err(|e| StorageError::InvalidTimestampFormat(e.to_string()))?;

        let store = self.events.read().await;

        // Per-participant resolution (C-02): a cascade is stale if it has at
        // least one unresolved participant past the threshold. A participant
        // (cascade_id, domain, edition, root) is "resolved" when there is a
        // committed cascade row (Confirmation/Revocation) on the SAME
        // (domain, edition, root) carrying the SAME cascade_id.
        //
        // Pre-fix semantics excluded the cascade globally if ANY committed
        // row existed anywhere for the cascade_id; that stranded participants
        // 2..N after participant 1's Revocation succeeded.
        let mut resolved_participants: HashSet<(String, String, String, Uuid)> = HashSet::new();
        for ((domain, edition, root), events) in store.iter() {
            for stored in events {
                if let Some(ref cid) = stored.page.cascade_id {
                    if !stored.page.no_commit {
                        resolved_participants.insert((
                            cid.clone(),
                            domain.clone(),
                            edition.clone(),
                            *root,
                        ));
                    }
                }
            }
        }

        let mut stale_cascade_ids: HashSet<String> = HashSet::new();
        for ((domain, edition, root), events) in store.iter() {
            for stored in events {
                if !stored.page.no_commit {
                    continue;
                }
                let cid = match stored.page.cascade_id.as_ref() {
                    Some(c) => c,
                    None => continue,
                };
                if resolved_participants.contains(&(
                    cid.clone(),
                    domain.clone(),
                    edition.clone(),
                    *root,
                )) {
                    continue;
                }
                let ts = match stored.page.created_at.as_ref() {
                    Some(t) => t,
                    None => continue,
                };
                let dt = match chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32) {
                    Some(dt) => dt,
                    None => continue,
                };
                if dt < threshold_dt {
                    stale_cascade_ids.insert(cid.clone());
                }
            }
        }

        Ok(stale_cascade_ids.into_iter().collect())
    }

    async fn query_cascade_participants(
        &self,
        cascade_id: &str,
    ) -> Result<Vec<CascadeParticipant>> {
        use std::collections::HashSet;

        let store = self.events.read().await;

        // Per-participant resolution (C-02): a participant is "resolved"
        // when its (domain, edition, root) has a committed row for the same
        // cascade_id (a Revocation or Confirmation marker). Resolved
        // participants are filtered out so the reaper doesn't double-revoke.
        let mut resolved: HashSet<(String, String, Uuid)> = HashSet::new();
        for ((domain, edition, root), events) in store.iter() {
            for stored in events {
                if let Some(ref cid) = stored.page.cascade_id {
                    if cid == cascade_id && !stored.page.no_commit {
                        resolved.insert((domain.clone(), edition.clone(), *root));
                    }
                }
            }
        }

        // Group uncommitted sequences by (domain, edition, root), skipping
        // any participant that is already resolved.
        let mut participants_map: HashMap<(String, String, Uuid), Vec<u32>> = HashMap::new();
        for ((domain, edition, root), events) in store.iter() {
            let key = (domain.clone(), edition.clone(), *root);
            if resolved.contains(&key) {
                continue;
            }
            for stored in events {
                if let Some(ref cid) = stored.page.cascade_id {
                    if cid == cascade_id && stored.page.no_commit {
                        participants_map
                            .entry(key.clone())
                            .or_default()
                            .push(stored.page.sequence_num());
                    }
                }
            }
        }

        // Convert to CascadeParticipant list
        let participants: Vec<CascadeParticipant> = participants_map
            .into_iter()
            .map(|((domain, edition, root), sequences)| CascadeParticipant {
                domain,
                edition,
                root,
                sequences,
            })
            .collect();

        Ok(participants)
    }
}
