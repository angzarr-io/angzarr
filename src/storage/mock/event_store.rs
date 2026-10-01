//! Mock EventStore implementation for testing.
//!
//! An in-memory store that follows the same contract as the production
//! backends — one main-timeline spelling, composite edition reads, the
//! append window, the main-timeline delete guard and per-participant
//! cascade resolution — by using the shared rules in
//! [`crate::storage::timeline`] and [`crate::storage::cascade_resolution`].

use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::proto::{EventBook, EventPage};
use crate::proto_ext::EventPageExt;
use crate::storage::cascade_resolution::{stale_cascade_ids, unresolved_participants, CascadeRow};
use crate::storage::helpers::{assemble_event_books, is_main_timeline, BookParts};
use crate::storage::timeline::{
    guard_edition_delete, implicit_divergence, merge_composite_events, parse_rfc3339_utc,
    reported_edition, resolve_divergence, storage_edition, validate_append, AppendWindow,
    MAIN_TIMELINE_STORAGE_EDITION,
};
use crate::storage::{
    AddMeta, AddOutcome, CascadeParticipant, EventStore, Result, SourceInfo, StorageError,
};

/// Stored event with correlation and idempotency tracking.
struct StoredEvent {
    page: EventPage,
    correlation_id: String,
    external_id: String,
    source_info: Option<SourceInfo>,
    /// Parent-aggregate routing cover (`Cover.ext`), replicated per row to
    /// mirror the SQL backends' per-row storage model.
    ext: Option<prost_types::Any>,
}

type StreamKey = (String, String, Uuid);

fn stream_key(domain: &str, edition: &str, root: Uuid) -> StreamKey {
    (
        domain.to_string(),
        storage_edition(edition).to_string(),
        root,
    )
}

/// Mock event store that stores events in memory.
#[derive(Default)]
pub struct MockEventStore {
    events: RwLock<HashMap<StreamKey, Vec<StoredEvent>>>,
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

    fn stream_pages(
        store: &HashMap<StreamKey, Vec<StoredEvent>>,
        domain: &str,
        edition: &str,
        root: Uuid,
    ) -> Vec<EventPage> {
        let mut pages: Vec<EventPage> = store
            .get(&stream_key(domain, edition, root))
            .map(|events| events.iter().map(|e| e.page.clone()).collect())
            .unwrap_or_default();
        pages.sort_by_key(|p| p.sequence_num());
        pages
    }

    fn stream_max(
        store: &HashMap<StreamKey, Vec<StoredEvent>>,
        domain: &str,
        edition: &str,
        root: Uuid,
    ) -> Option<u32> {
        store
            .get(&stream_key(domain, edition, root))
            .and_then(|events| events.iter().map(|e| e.page.sequence_num()).max())
    }

    /// Composite read: the main timeline below the divergence point
    /// followed by the edition's own events, keeping the pages `keep`
    /// accepts.
    async fn read(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        explicit_divergence: Option<u32>,
        keep: impl FnMut(&EventPage) -> bool,
    ) -> Result<Vec<EventPage>> {
        if *self.fail_on_get.read().await {
            return Err(StorageError::NotFound {
                domain: domain.to_string(),
                root,
            });
        }
        let store = self.events.read().await;
        let main = Self::stream_pages(&store, domain, MAIN_TIMELINE_STORAGE_EDITION, root);
        if is_main_timeline(edition) {
            return Ok(merge_composite_events(main, Vec::new(), keep));
        }
        let edition_events = Self::stream_pages(&store, domain, edition, root);
        let divergence =
            resolve_divergence(explicit_divergence, implicit_divergence(&edition_events));
        let main_prefix: Vec<EventPage> = main
            .into_iter()
            .filter(|e| divergence.is_none_or(|d| e.sequence_num() < d))
            .collect();
        Ok(merge_composite_events(main_prefix, edition_events, keep))
    }

    fn cascade_rows(store: &HashMap<StreamKey, Vec<StoredEvent>>) -> Vec<CascadeRow> {
        store
            .iter()
            .flat_map(|((domain, edition, root), events)| {
                events.iter().filter_map(move |stored| {
                    let cascade_id = stored.page.cascade_id.clone()?;
                    Some(CascadeRow {
                        cascade_id,
                        domain: domain.clone(),
                        edition: edition.clone(),
                        root: *root,
                        sequence: stored.page.sequence_num(),
                        committed: !stored.page.no_commit,
                        created_at: stored.page.created_at.as_ref().and_then(|ts| {
                            chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                        }),
                    })
                })
            })
            .collect()
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
        meta: &AddMeta<'_>,
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

        let external_id = meta.external_id.unwrap_or("");
        let key = stream_key(domain, edition, root);
        let mut store = self.events.write().await;

        if !external_id.is_empty() {
            let matching: Vec<u32> = store
                .get(&key)
                .map(|existing| {
                    existing
                        .iter()
                        .filter(|e| e.external_id == external_id)
                        .map(|e| e.page.sequence_num())
                        .collect()
                })
                .unwrap_or_default();
            if let (Some(first), Some(last)) = (
                matching.iter().min().copied(),
                matching.iter().max().copied(),
            ) {
                return Ok(AddOutcome::Duplicate {
                    first_sequence: first,
                    last_sequence: last,
                });
            }
        }

        let stream_next = Self::stream_max(&store, domain, edition, root).map(|max| max + 1);
        let main_next = if stream_next.is_none() && !is_main_timeline(edition) {
            Self::stream_max(&store, domain, MAIN_TIMELINE_STORAGE_EDITION, root)
                .map_or(0, |max| max + 1)
        } else {
            stream_next.unwrap_or(0)
        };
        let window = AppendWindow::for_edition(edition, stream_next, main_next);
        let (first_sequence, last_sequence) = validate_append(window, &events)?;

        let stored: Vec<StoredEvent> = events
            .into_iter()
            .map(|page| StoredEvent {
                page,
                correlation_id: meta.correlation_id.to_string(),
                external_id: external_id.to_string(),
                source_info: meta.source_info.cloned(),
                ext: meta.ext.cloned(),
            })
            .collect();
        store.entry(key).or_default().extend(stored);

        Ok(AddOutcome::Added {
            first_sequence,
            last_sequence,
        })
    }

    async fn get(&self, domain: &str, edition: &str, root: Uuid) -> Result<Vec<EventPage>> {
        self.read(domain, edition, root, None, |_| true).await
    }

    async fn get_with_divergence(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        explicit_divergence: Option<u32>,
    ) -> Result<Vec<EventPage>> {
        self.read(domain, edition, root, explicit_divergence, |_| true)
            .await
    }

    async fn get_from(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        self.read(domain, edition, root, None, |e| e.sequence_num() >= from)
            .await
    }

    async fn get_from_to(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
        to: u32,
    ) -> Result<Vec<EventPage>> {
        self.read(domain, edition, root, None, |e| {
            (from..to).contains(&e.sequence_num())
        })
        .await
    }

    async fn get_until_timestamp(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        until: &prost_types::Timestamp,
    ) -> Result<Vec<EventPage>> {
        let until_dt = chrono::DateTime::from_timestamp(until.seconds, until.nanos as u32).ok_or(
            StorageError::InvalidTimestamp {
                seconds: until.seconds,
                nanos: until.nanos,
            },
        )?;
        self.read(domain, edition, root, None, |e| {
            e.created_at
                .as_ref()
                .and_then(|ts| chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32))
                .is_some_and(|dt| dt <= until_dt)
        })
        .await
    }

    async fn list_roots(&self, domain: &str, edition: &str) -> Result<Vec<Uuid>> {
        let edition = storage_edition(edition);
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
        let store = self.events.read().await;
        if let Some(max) = Self::stream_max(&store, domain, edition, root) {
            return Ok(max + 1);
        }
        // An edition with no events of its own continues the main timeline.
        Ok(
            Self::stream_max(&store, domain, MAIN_TIMELINE_STORAGE_EDITION, root)
                .map_or(0, |max| max + 1),
        )
    }

    async fn get_by_correlation(&self, correlation_id: &str) -> Result<Vec<EventBook>> {
        if correlation_id.is_empty() {
            return Ok(vec![]);
        }

        let store = self.events.read().await;
        let mut books_map: HashMap<(String, String, Uuid), BookParts> = HashMap::new();

        for ((domain, edition, root), events) in store.iter() {
            for stored in events {
                if stored.correlation_id == correlation_id {
                    let entry = books_map
                        .entry((domain.clone(), reported_edition(edition).to_string(), *root))
                        .or_default();
                    entry.pages.push(stored.page.clone());
                    if entry.ext.is_none() {
                        entry.ext = stored.ext.clone();
                    }
                }
            }
        }

        Ok(assemble_event_books(books_map, correlation_id))
    }

    async fn delete_edition_events(&self, domain: &str, edition: &str) -> Result<u32> {
        guard_edition_delete(edition)?;
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

        let store = self.events.read().await;
        let matching: Vec<EventPage> = store
            .get(&stream_key(domain, edition, root))
            .map(|events| {
                events
                    .iter()
                    .filter(|e| {
                        e.source_info.as_ref().is_some_and(|stored| {
                            storage_edition(&stored.edition)
                                == storage_edition(&source_info.edition)
                                && stored.domain == source_info.domain
                                && stored.root == source_info.root
                                && stored.seq == source_info.seq
                                && stored.component == source_info.component
                                && stored.command_index == source_info.command_index
                        })
                    })
                    .map(|e| e.page.clone())
                    .collect()
            })
            .unwrap_or_default();
        Ok((!matching.is_empty()).then_some(matching))
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
        let store = self.events.read().await;
        let matching: Vec<EventPage> = store
            .get(&stream_key(domain, edition, root))
            .map(|events| {
                events
                    .iter()
                    .filter(|e| e.external_id == external_id)
                    .map(|e| e.page.clone())
                    .collect()
            })
            .unwrap_or_default();
        Ok((!matching.is_empty()).then_some(matching))
    }

    async fn query_stale_cascades(&self, threshold: &str) -> Result<Vec<String>> {
        let threshold = parse_rfc3339_utc(threshold)?;
        let store = self.events.read().await;
        Ok(stale_cascade_ids(&Self::cascade_rows(&store), threshold))
    }

    async fn query_cascade_participants(
        &self,
        cascade_id: &str,
    ) -> Result<Vec<CascadeParticipant>> {
        let store = self.events.read().await;
        Ok(unresolved_participants(
            &Self::cascade_rows(&store),
            cascade_id,
        ))
    }
}
