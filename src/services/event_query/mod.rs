//! Event query service.

use std::collections::HashSet;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info};

use crate::orchestration::aggregate::{transform_for_two_phase, TwoPhaseContext};
use crate::proto::{
    event_query_service_server::EventQueryService as EventQueryTrait, query::Selection,
    temporal_query::PointInTime, AggregateRoot, EventBook, Query, Uuid as ProtoUuid,
};
use crate::proto_ext::{CoverExt, EventPageExt};
use crate::repository::{EventBookRepository, SnapshotRepository};
use crate::storage::{EventStore, SnapshotStore};
use crate::validation;

/// Resolve 2PC visibility for EVERY book a correlation-id query returned
/// (C01 #7).
///
/// `EventStore::get_by_correlation` filters at the STORAGE layer by the
/// `correlation_id` column, bypassing `EventBookRepository` entirely — so
/// its raw results carry unresolved `no_commit` pages, violating the
/// framework-wide invariant that "no reader outside cascade propagation
/// may see raw unresolved `no_commit` pages"
/// (`src/repository/event_book/mod.rs` module doc).
///
/// # Why resolve per-root against the FULL stream, not the correlation slice
///
/// Reaper-written Revocations (and any Confirmation written the same way)
/// carry an EMPTY `correlation_id` (`AddMeta::default()` in
/// `cascade::reaper::write_revocation`) — they never match the
/// correlation-id filter and so are ABSENT from `books`. Running
/// `transform_for_two_phase` on the correlation-filtered book alone would
/// never see the marker that resolves its own provisional pages. Each
/// book already tells us which `(domain, edition, root)` it came from
/// (its `cover`); fetch that root's FULL raw stream (which DOES contain
/// the marker), resolve, then filter back down to just the sequences the
/// correlation query originally matched — same shape as the sparse path
/// in `EventBookRepository::get_sequences`.
async fn resolve_correlation_books(
    event_store: &Arc<dyn EventStore>,
    books: Vec<EventBook>,
) -> Vec<EventBook> {
    let mut resolved = Vec::with_capacity(books.len());
    for book in books {
        resolved.push(resolve_one_correlation_book(event_store, book).await);
    }
    resolved
}

/// Resolve a single correlation-query book against its own full stream.
///
/// Fails CLOSED: if the full-stream read errors, the book is dropped to
/// an empty (but cover-preserving) book rather than falling back to the
/// unresolved raw pages — silently leaking raw `no_commit` pages on a
/// storage hiccup would defeat the whole point of this fix.
async fn resolve_one_correlation_book(
    event_store: &Arc<dyn EventStore>,
    book: EventBook,
) -> EventBook {
    let Some(cover) = book.cover.clone() else {
        return book;
    };
    let Some(root_proto) = cover.root.as_ref() else {
        return book;
    };
    let Ok(root_uuid) = uuid::Uuid::from_slice(&root_proto.value) else {
        return book;
    };
    if book.pages.is_empty() {
        return book;
    }

    let requested_sequences: HashSet<u32> = book.pages.iter().map(|p| p.sequence_num()).collect();
    let domain = cover.domain.clone();
    let edition = cover.edition().unwrap_or_default().to_string();

    match event_store.get(&domain, &edition, root_uuid).await {
        Ok(full_stream_pages) => {
            let raw = EventBook {
                cover: Some(cover.clone()),
                pages: full_stream_pages,
                ..Default::default()
            };
            let resolved = transform_for_two_phase(&raw, &TwoPhaseContext::standard()).events;
            let filtered_pages = resolved
                .pages
                .into_iter()
                .filter(|p| requested_sequences.contains(&p.sequence_num()))
                .collect();
            EventBook {
                cover: Some(cover),
                pages: filtered_pages,
                snapshot: None,
                next_sequence: book.next_sequence,
            }
        }
        Err(e) => {
            error!(
                domain = %domain,
                root = %root_uuid,
                error = %e,
                "Correlation query: full-stream 2PC resolution failed for this root; \
                 dropping its pages rather than leaking unresolved raw pages"
            );
            EventBook {
                cover: Some(cover),
                pages: vec![],
                snapshot: None,
                next_sequence: book.next_sequence,
            }
        }
    }
}

/// Event query service.
///
/// Provides query access to the event store.
pub struct EventQueryService {
    event_book_repo: Arc<EventBookRepository>,
    event_store: Arc<dyn EventStore>,
}

impl EventQueryService {
    /// Create a new event query service with snapshot optimization disabled.
    ///
    /// Snapshots are disabled because the EventQuery service returns event
    /// history — callers expect all events in `pages`, not a snapshot plus
    /// subsequent events. Snapshot optimization is for aggregate state
    /// reconstruction (AggregateCoordinator), not event queries.
    pub fn new(event_store: Arc<dyn EventStore>, snapshot_store: Arc<dyn SnapshotStore>) -> Self {
        Self::with_options(event_store, snapshot_store, false)
    }

    /// Create a new event query service with configurable snapshot reading.
    ///
    /// Use `enable_snapshots = true` (default) for saga workloads where snapshots
    /// improve efficiency. Use `false` for raw event queries (debugging, replay).
    pub fn with_options(
        event_store: Arc<dyn EventStore>,
        snapshot_store: Arc<dyn SnapshotStore>,
        enable_snapshots: bool,
    ) -> Self {
        // write_enabled=false because EventQueryService never persists
        // snapshots — it's a read-only surface. read_enabled mirrors the
        // caller's preference.
        let snapshot_repo = Arc::new(SnapshotRepository::with_flags(
            snapshot_store,
            enable_snapshots,
            false,
        ));
        Self {
            event_book_repo: Arc::new(EventBookRepository::new(event_store.clone(), snapshot_repo)),
            event_store,
        }
    }
}

/// Resolve and validate the `(domain, edition, root)` a root-addressed query
/// targets — the same checks for every query RPC.
fn query_target(
    cover: Option<&crate::proto::Cover>,
) -> Result<(String, String, uuid::Uuid), Status> {
    let cover = cover.ok_or_else(|| {
        Status::invalid_argument(crate::services::errmsg::QUERY_MISSING_COVER_OR_CORRELATION)
    })?;
    validation::validate_domain(&cover.domain)?;
    let root = cover.root.as_ref().ok_or_else(|| {
        Status::invalid_argument(crate::services::errmsg::QUERY_MISSING_ROOT_OR_CORRELATION)
    })?;
    let root = uuid::Uuid::from_slice(&root.value).map_err(|e| {
        Status::invalid_argument(format!("{}{}", crate::services::errmsg::INVALID_UUID, e))
    })?;
    let edition = cover.edition().unwrap_or_default();
    validation::validate_edition(edition)?;
    Ok((cover.domain.clone(), edition.to_string(), root))
}

/// Resolve a `Query::selection` against the repository.
///
/// `get_event_book` (unary), `get_events` (server-stream) and `synchronize`
/// (bidi-stream) MUST produce the same event set for the same
/// `(domain, edition, root, selection)`. This helper centralises the dispatch
/// so the call sites cannot drift.
///
/// Range upper bound: the proto `SequenceRange.upper` is inclusive (see the
/// `test_get_event_book_with_range` doc-comment in mod.test.rs); storage
/// `get_from_to(from, to)` is `[from, to)` half-open. The helper converts
/// inclusive→exclusive via `saturating_add(1)`. `upper: None` means "to
/// latest" → `u32::MAX`.
///
/// Errors:
/// - `InvalidArgument` if a temporal query is missing its `point_in_time`.
/// - `InvalidArgument` if an `as_of_time` timestamp is malformed.
/// - `Internal` for any storage / repository error.
pub(crate) async fn dispatch_selection(
    repo: &EventBookRepository,
    domain: &str,
    edition: &str,
    root: uuid::Uuid,
    selection: Option<Selection>,
) -> Result<EventBook, Status> {
    let result = match selection {
        Some(Selection::Range(range)) => {
            let lower = range.lower;
            // H-36: proto `SequenceRange.upper` is INCLUSIVE; storage
            // `get_from_to` is `[from, to)` half-open. Convert
            // inclusive→exclusive with saturating_add so the unary
            // `get_event_book` and the streamed `synchronize` produce
            // the same event set for the same Query.
            let upper = range.upper.map(|u| u.saturating_add(1)).unwrap_or(u32::MAX);
            repo.get_from_to(domain, edition, root, lower, upper).await
        }
        Some(Selection::Sequences(seq_set)) => {
            repo.get_sequences(domain, edition, root, &seq_set.values)
                .await
        }
        Some(Selection::Temporal(tq)) => match tq.point_in_time {
            Some(PointInTime::AsOfTime(ref ts)) => {
                // C10: forward the typed timestamp; normalization happens
                // once at the repository/storage boundary, not here.
                repo.get_temporal_by_time(domain, edition, root, ts).await
            }
            Some(PointInTime::AsOfSequence(seq)) => {
                repo.get_temporal_by_sequence(domain, edition, root, seq)
                    .await
            }
            None => {
                // H-35: emit the constant's VALUE, not its path.
                return Err(Status::invalid_argument(
                    crate::services::errmsg::TEMPORAL_QUERY_MISSING_POINT,
                ));
            }
        },
        None => repo.get(domain, edition, root).await,
    };
    result.map_err(|e| Status::internal(e.to_string()))
}

#[tonic::async_trait]
impl EventQueryTrait for EventQueryService {
    type GetEventsStream = ReceiverStream<Result<EventBook, Status>>;
    type SynchronizeStream = ReceiverStream<Result<EventBook, Status>>;
    type GetAggregateRootsStream = ReceiverStream<Result<AggregateRoot, Status>>;

    async fn get_event_book(&self, request: Request<Query>) -> Result<Response<EventBook>, Status> {
        let query = request.into_inner();
        let cover = query.cover.as_ref();

        // Extract and validate correlation_id from cover
        let correlation_id = cover.map(|c| c.correlation_id.as_str()).unwrap_or("");
        validation::validate_correlation_id(correlation_id)?;

        // Correlation ID query: returns first matching EventBook across all domains
        // Useful for sagas that need to find related events without knowing the root ID
        if !correlation_id.is_empty() {
            info!(correlation_id = %correlation_id, "GetEventBook by correlation_id");

            let books = self
                .event_store
                .get_by_correlation(correlation_id)
                .await
                .map_err(|e| {
                    error!(correlation_id = %correlation_id, error = %e, "GetEventBook correlation query failed");
                    Status::internal(e.to_string())
                })?;

            // C01 #7: resolve 2PC visibility per-root before returning —
            // `get_by_correlation` bypasses `EventBookRepository` and its
            // results carry raw unresolved `no_commit` pages otherwise.
            let books = resolve_correlation_books(&self.event_store, books).await;

            // Return first matching book, or empty book if none found
            let book = books.into_iter().next().unwrap_or_default();
            info!(correlation_id = %correlation_id, pages = book.pages.len(), "GetEventBook by correlation_id completed");
            return Ok(Response::new(book));
        }

        let (domain, edition, root_uuid) = query_target(cover)?;
        let edition = edition.as_str();

        info!(
            domain = %domain,
            root = %root_uuid,
            edition = %edition,
            selection = ?query.selection,
            "GetEventBook starting query"
        );

        // Selection dispatch goes through the shared `dispatch_selection`
        // helper so the unary RPC and the `synchronize` bidi-stream
        // produce the same event set for the same Query (H-35 / H-36).
        let book = dispatch_selection(
            &self.event_book_repo,
            &domain,
            edition,
            root_uuid,
            query.selection,
        )
        .await
        .map_err(|status| {
            error!(domain = %domain, root = %root_uuid, status = %status, "GetEventBook query failed");
            status
        })?;

        info!(domain = %domain, root = %root_uuid, pages = book.pages.len(), "GetEventBook completed");
        Ok(Response::new(book))
    }

    async fn get_events(
        &self,
        request: Request<Query>,
    ) -> Result<Response<Self::GetEventsStream>, Status> {
        let query = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let cover = query.cover.as_ref();

        // Extract and validate correlation_id from cover
        let correlation_id = cover.map(|c| c.correlation_id.clone()).unwrap_or_default();
        validation::validate_correlation_id(&correlation_id)?;

        // Correlation ID query: streams ALL matching EventBooks across all domains
        if !correlation_id.is_empty() {
            let event_store = self.event_store.clone();

            tokio::spawn(async move {
                match event_store.get_by_correlation(&correlation_id).await {
                    Ok(books) => {
                        // C01 #7: same per-root resolution as `get_event_book`
                        // — `get_by_correlation` results carry raw unresolved
                        // `no_commit` pages otherwise.
                        let books = resolve_correlation_books(&event_store, books).await;
                        for book in books {
                            if tx.send(Ok(book)).await.is_err() {
                                break; // Client disconnected
                            }
                        }
                    }
                    Err(e) => {
                        if tx.send(Err(Status::internal(e.to_string()))).await.is_err() {
                            debug!(correlation_id = %correlation_id, "Client disconnected before error could be sent");
                        }
                    }
                }
            });

            return Ok(Response::new(ReceiverStream::new(rx)));
        }

        let (domain, edition, root_uuid) = query_target(cover)?;
        let selection = query.selection;
        let event_book_repo = self.event_book_repo.clone();

        tokio::spawn(async move {
            let result =
                dispatch_selection(&event_book_repo, &domain, &edition, root_uuid, selection).await;
            if tx.send(result).await.is_err() {
                debug!(domain = %domain, root = %root_uuid, "Client disconnected before response");
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn synchronize(
        &self,
        request: Request<tonic::Streaming<Query>>,
    ) -> Result<Response<Self::SynchronizeStream>, Status> {
        let mut stream = request.into_inner();
        let event_book_repo = self.event_book_repo.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        tokio::spawn(async move {
            use tokio_stream::StreamExt;

            while let Some(query_result) = stream.next().await {
                match query_result {
                    Ok(query) => {
                        let (domain, edition, root) = match query_target(query.cover.as_ref()) {
                            Ok(target) => target,
                            Err(status) => {
                                if tx.send(Err(status)).await.is_err() {
                                    debug!("Client disconnected during synchronize");
                                    break;
                                }
                                continue;
                            }
                        };
                        let edition = edition.as_str();

                        // Selection dispatch goes through the shared
                        // `dispatch_selection` helper so this bidi-stream
                        // path and the unary `get_event_book` produce the
                        // same event set for the same Query
                        // (H-35 / H-36). `dispatch_selection` returns a
                        // pre-wrapped `Status` covering both the
                        // invalid_argument cases (missing temporal point,
                        // unparseable timestamp) and the internal-storage
                        // case, so the send path collapses to a single
                        // Ok/Err match.
                        let result = dispatch_selection(
                            &event_book_repo,
                            &domain,
                            edition,
                            root,
                            query.selection,
                        )
                        .await;

                        match result {
                            Ok(book) => {
                                info!(domain = %domain, root = %root, "Synchronize: sending event book");
                                if tx.send(Ok(book)).await.is_err() {
                                    break; // Client disconnected
                                }
                            }
                            Err(status) => {
                                error!(domain = %domain, root = %root, status = %status, "Synchronize: failed to get events");
                                if tx.send(Err(status)).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Synchronize: stream error");
                        if tx.send(Err(e)).await.is_err() {
                            debug!("Client disconnected during synchronize stream error");
                        }
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_aggregate_roots(
        &self,
        _request: Request<()>,
    ) -> Result<Response<Self::GetAggregateRootsStream>, Status> {
        let event_store = self.event_store.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        tokio::spawn(async move {
            // Get all domains from the event store
            let domains = match event_store.list_domains().await {
                Ok(d) => d,
                Err(e) => {
                    error!(error = %e, "Failed to list domains");
                    if tx.send(Err(Status::internal(e.to_string()))).await.is_err() {
                        debug!("Client disconnected before domain list error could be sent");
                    }
                    return;
                }
            };

            // Roots of the main timeline (AggregateRoot carries no edition).
            for domain in domains {
                match event_store.list_roots(&domain, "").await {
                    Ok(roots) => {
                        for root in roots {
                            let aggregate = AggregateRoot {
                                domain: domain.clone(),
                                root: Some(ProtoUuid {
                                    value: root.as_bytes().to_vec(),
                                }),
                            };
                            if tx.send(Ok(aggregate)).await.is_err() {
                                return; // Client disconnected
                            }
                        }
                    }
                    Err(e) => {
                        error!(domain = %domain, error = %e, "Failed to list roots");
                        let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                        return;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
