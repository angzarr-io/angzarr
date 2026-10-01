//! Hybrid destination fetcher for process manager sidecars.
//!
//! # The Chicken-and-Egg Problem
//!
//! Process managers (PMs) need to fetch their own state to make decisions, but:
//! - PMs run as sidecars, not as aggregate services
//! - The normal destination fetcher routes to aggregate services via gRPC
//! - There IS no aggregate service for the PM domain — the PM sidecar IS the service
//!
//! The hybrid fetcher solves this by routing queries for the PM's own domain to
//! local storage, while delegating all other domain queries to the remote fetcher.
//!
//! # Why Not Just Use Local Storage for Everything?
//!
//! The PM sidecar only has storage for its own domain. Other domains (order,
//! inventory, player, etc.) are served by their respective aggregate sidecars.
//! Hybrid routing is the cleanest way to handle "local for self, remote for others".
//!
//! # Snapshot Optimization
//!
//! Uses `EventBookRepository` for local fetches to properly load snapshots
//! and only fetch events AFTER the snapshot sequence. Direct EventStore queries
//! would either miss snapshots or double-apply events.

use std::sync::Arc;

use async_trait::async_trait;
use tonic::Status;
use tracing::{debug, warn};

use crate::proto::{Cover, EventBook};
use crate::proto_ext::CoverExt;
use crate::repository::{EventBookRepository, SnapshotRepository};
use crate::storage::EventStore;

use super::DestinationFetcher;

/// Fetches from local storage for one domain, delegates others to a remote fetcher.
///
/// Used by the process manager sidecar to access its own state directly while
/// routing other domain queries to remote aggregate sidecars via gRPC.
pub struct HybridDestinationFetcher {
    local_domain: String,
    local_event_store: Arc<dyn EventStore>,
    local_snapshot_repo: Arc<SnapshotRepository>,
    remote: Arc<dyn DestinationFetcher>,
}

impl HybridDestinationFetcher {
    /// Create with the local domain name, repositories, and remote fetcher for other domains.
    pub fn new(
        local_domain: String,
        local_event_store: Arc<dyn EventStore>,
        local_snapshot_repo: Arc<SnapshotRepository>,
        remote: Arc<dyn DestinationFetcher>,
    ) -> Self {
        Self {
            local_domain,
            local_event_store,
            local_snapshot_repo,
            remote,
        }
    }
}

#[async_trait]
impl DestinationFetcher for HybridDestinationFetcher {
    async fn fetch(&self, cover: &Cover) -> Result<Option<EventBook>, Status> {
        if cover.domain == self.local_domain {
            // A malformed cover or a storage failure is an error, not "no
            // state": Ok(None) would make a PM restart a live workflow.
            let root = cover.root.as_ref().ok_or_else(|| {
                Status::invalid_argument(crate::orchestration::errmsg::COVER_MISSING_ROOT)
            })?;
            let root = uuid::Uuid::from_slice(&root.value).map_err(|e| {
                Status::invalid_argument(format!(
                    "{}{}",
                    crate::orchestration::errmsg::INVALID_UUID,
                    e
                ))
            })?;
            let edition = cover.edition().unwrap_or_default();

            // Use EventBookRepository to properly load snapshot + subsequent events
            let repo = EventBookRepository::new(
                self.local_event_store.clone(),
                self.local_snapshot_repo.clone(),
            );

            match repo.get(&self.local_domain, edition, root).await {
                Ok(mut book) => {
                    // Preserve the correlation_id from the input cover.
                    // Why: EventBookRepository.get() doesn't know the correlation_id
                    // (it queries by domain/edition/root). The caller passed us the
                    // correlation_id in the cover; we must preserve it for downstream
                    // PM logic that expects it.
                    if let Some(ref mut book_cover) = book.cover {
                        book_cover.correlation_id = cover.correlation_id.clone();
                    }
                    Ok(Some(book))
                }
                Err(e) => {
                    warn!(
                        domain = %self.local_domain,
                        error = %e,
                        "Failed to fetch local domain EventBook"
                    );
                    // Same storage-error → Status mapping the EventQuery
                    // service applies for remote reads.
                    Err(Status::internal(e.to_string()))
                }
            }
        } else {
            self.remote.fetch(cover).await
        }
    }

    /// The PM's state lives in the local store under the correlation-derived
    /// root (the same derivation the PM persist path uses) on the trigger's
    /// edition — no correlation scan, so no arbitrary pick between editions.
    /// An aggregate with neither events nor a snapshot is a new workflow.
    async fn fetch_pm_state(
        &self,
        pm_domain: &str,
        edition: &str,
        correlation_id: &str,
    ) -> Result<Option<EventBook>, Status> {
        use crate::orchestration::shared::CorrelationRootExt;
        if pm_domain != self.local_domain {
            return self
                .remote
                .fetch_pm_state(pm_domain, edition, correlation_id)
                .await;
        }
        let repo = EventBookRepository::new(
            self.local_event_store.clone(),
            self.local_snapshot_repo.clone(),
        );
        let mut book = repo
            .get(pm_domain, edition, correlation_id.correlation_root())
            .await
            .map_err(|e| {
                warn!(domain = %pm_domain, correlation_id = %correlation_id, error = %e, "Failed to fetch PM state");
                Status::internal(e.to_string())
            })?;
        if book.pages.is_empty() && book.snapshot.is_none() {
            return Ok(None);
        }
        if let Some(cover) = book.cover.as_mut() {
            cover.correlation_id = correlation_id.to_string();
        }
        Ok(Some(book))
    }

    async fn fetch_by_correlation(
        &self,
        domain: &str,
        correlation_id: &str,
    ) -> Result<Option<EventBook>, Status> {
        if domain == self.local_domain {
            debug!(
                domain = %domain,
                correlation_id = %correlation_id,
                "Fetching PM state from local store"
            );

            // First find the aggregate by correlation_id.
            //
            // A storage failure propagates as Err: as Ok(None) the PM would
            // treat a live workflow as brand new.
            let books = self
                .local_event_store
                .get_by_correlation(correlation_id)
                .await
                .map_err(|e| {
                    warn!(
                        domain = %domain,
                        correlation_id = %correlation_id,
                        error = %e,
                        "Failed to fetch local domain state by correlation"
                    );
                    Status::internal(e.to_string())
                })?;

            // Find the first book matching this domain.
            //
            // Why filter by domain? get_by_correlation returns ALL event books across
            // ALL domains that share this correlation_id. A single correlation_id ties
            // together events from order, inventory, fulfillment, AND the PM itself.
            // We only want the PM's own state here — other domains would be fetched
            // via the remote fetcher if needed.
            //
            // No matching book is the ONE case that genuinely means "no
            // state": the store answered and holds nothing for this
            // (domain, correlation) — a new workflow. Hence Ok(None).
            let Some(book) = books.into_iter().find(|b| {
                b.cover
                    .as_ref()
                    .is_some_and(|c| c.domain == self.local_domain)
            }) else {
                return Ok(None);
            };

            // Re-fetch using EventBookRepository to get snapshot-optimized version.
            //
            // Why re-fetch instead of using the book we just found?
            //
            // 1. **Snapshot loading**: get_by_correlation queries the event store directly,
            //    returning raw events without snapshot optimization. EventBookRepository
            //    loads the latest snapshot and only fetches events AFTER that snapshot's
            //    sequence, dramatically reducing data transfer for long-lived PMs.
            //
            // 2. **State consistency**: The book from get_by_correlation might have been
            //    built from a partial event set (depending on store implementation).
            //    EventBookRepository guarantees a complete, correctly-ordered view.
            //
            // The first lookup is just to find the root UUID — we need to know which
            // aggregate instance has this correlation_id before we can do a proper fetch.
            //
            // State for the workflow exists here; a book we cannot interpret
            // (missing cover/root, unparseable root) is corrupt data, not
            // absence, so it is an internal error.
            let cover = book.cover.as_ref().ok_or_else(|| {
                Status::internal(crate::orchestration::errmsg::EVENT_BOOK_MISSING_COVER)
            })?;
            let edition = cover.edition().unwrap_or_default();
            let root = cover.root.as_ref().ok_or_else(|| {
                Status::internal(crate::orchestration::errmsg::COVER_MISSING_ROOT)
            })?;
            let root_uuid = uuid::Uuid::from_slice(&root.value).map_err(|e| {
                Status::internal(format!(
                    "{}{}",
                    crate::orchestration::errmsg::INVALID_UUID,
                    e
                ))
            })?;

            let repo = EventBookRepository::new(
                self.local_event_store.clone(),
                self.local_snapshot_repo.clone(),
            );

            match repo.get(domain, edition, root_uuid).await {
                Ok(mut fetched_book) => {
                    // Preserve the correlation_id
                    if let Some(ref mut fetched_cover) = fetched_book.cover {
                        fetched_cover.correlation_id = correlation_id.to_string();
                    }
                    Ok(Some(fetched_book))
                }
                Err(e) => {
                    warn!(
                        domain = %domain,
                        correlation_id = %correlation_id,
                        error = %e,
                        "Failed to re-fetch local domain with snapshot"
                    );
                    Err(Status::internal(e.to_string()))
                }
            }
        } else {
            self.remote
                .fetch_by_correlation(domain, correlation_id)
                .await
        }
    }
}

#[cfg(test)]
#[path = "hybrid.test.rs"]
mod tests;
