//! Event query service.

use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info};

use crate::proto::{
    event_query_service_server::EventQueryService as EventQueryTrait, query::Selection,
    temporal_query::PointInTime, AggregateRoot, EventBook, Query, Uuid as ProtoUuid,
};
use crate::proto_ext::CoverExt;
use crate::repository::EventBookRepository;
use crate::storage::EventStore;
use crate::storage::SnapshotStore;
use crate::validation;

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
        Self {
            event_book_repo: Arc::new(EventBookRepository::with_config(
                event_store.clone(),
                snapshot_store,
                enable_snapshots,
            )),
            event_store,
        }
    }
}

/// Resolve a `Query::selection` against the repository.
///
/// Both `get_event_book` (unary) and `synchronize` (bidi-stream) MUST produce
/// the same event set for the same `(domain, edition, root, selection)`. This
/// helper centralises the dispatch so the two call sites cannot drift.
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
///
/// Currently exercised only by the H-35/H-36 regression tests; production
/// `get_event_book` / `synchronize` still inline the same branch logic with
/// matching semantics. A future refactor will collapse both methods onto
/// this helper.
#[allow(dead_code)]
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
                let rfc3339 = crate::storage::helpers::timestamp_to_rfc3339(ts)
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
                repo.get_temporal_by_time(domain, edition, root, &rfc3339)
                    .await
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

            // Return first matching book, or empty book if none found
            let book = books.into_iter().next().unwrap_or_default();
            info!(correlation_id = %correlation_id, pages = book.pages.len(), "GetEventBook by correlation_id completed");
            return Ok(Response::new(book));
        }

        // Standard query by domain + root
        let cover = cover.ok_or_else(|| {
            Status::invalid_argument(crate::services::errmsg::QUERY_MISSING_COVER_OR_CORRELATION)
        })?;
        let domain = cover.domain.clone();
        validation::validate_domain(&domain)?;
        let root = cover.root.as_ref().ok_or_else(|| {
            Status::invalid_argument(crate::services::errmsg::QUERY_MISSING_ROOT_OR_CORRELATION)
        })?;

        let root_uuid = uuid::Uuid::from_slice(&root.value).map_err(|e| {
            Status::invalid_argument(format!("{}{}", crate::services::errmsg::INVALID_UUID, e))
        })?;

        let edition = cover.edition().unwrap_or_default();
        validation::validate_edition(edition)?;

        info!(
            domain = %domain,
            root = %root_uuid,
            edition = %edition,
            selection = ?query.selection,
            "GetEventBook starting query"
        );

        // Handle selection: range, specific sequences, temporal, or full query
        let book = match query.selection {
            Some(Selection::Range(ref range)) => {
                let lower = range.lower;
                // Proto uses inclusive upper bound, storage uses exclusive.
                // Convert: inclusive N → exclusive N+1 (saturating to avoid overflow)
                let upper = range
                    .upper
                    .map(|u| u.saturating_add(1))
                    .unwrap_or(u32::MAX);
                info!(domain = %domain, root = %root_uuid, lower = lower, upper = upper, "GetEventBook range query");
                self.event_book_repo
                    .get_from_to(&domain, edition, root_uuid, lower, upper)
                    .await
            }
            Some(Selection::Sequences(ref seq_set)) => {
                info!(domain = %domain, root = %root_uuid, sequences = ?seq_set.values, "GetEventBook sequences query");
                self.event_book_repo
                    .get_sequences(&domain, edition, root_uuid, &seq_set.values)
                    .await
            }
            Some(Selection::Temporal(ref tq)) => {
                match tq.point_in_time {
                    Some(PointInTime::AsOfTime(ref ts)) => {
                        let rfc3339 = crate::storage::helpers::timestamp_to_rfc3339(ts)
                            .map_err(|e| Status::invalid_argument(e.to_string()))?;
                        info!(domain = %domain, root = %root_uuid, as_of = %rfc3339, "GetEventBook temporal time query");
                        self.event_book_repo
                            .get_temporal_by_time(&domain, edition, root_uuid, &rfc3339)
                            .await
                    }
                    Some(PointInTime::AsOfSequence(seq)) => {
                        info!(domain = %domain, root = %root_uuid, as_of_sequence = seq, "GetEventBook temporal sequence query");
                        self.event_book_repo
                            .get_temporal_by_sequence(&domain, edition, root_uuid, seq)
                            .await
                    }
                    None => {
                        return Err(Status::invalid_argument(
                            crate::services::errmsg::TEMPORAL_QUERY_MISSING_POINT,
                        ));
                    }
                }
            }
            None => {
                info!(domain = %domain, root = %root_uuid, "GetEventBook full query");
                self.event_book_repo.get(&domain, edition, root_uuid).await
            }
        }
        .map_err(|e| {
            error!(domain = %domain, root = %root_uuid, error = %e, "GetEventBook query failed");
            Status::internal(e.to_string())
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

        // Standard query by domain + root
        let cover = cover.ok_or_else(|| {
            Status::invalid_argument(crate::services::errmsg::QUERY_MISSING_COVER_OR_CORRELATION)
        })?;
        let domain = cover.domain.clone();
        validation::validate_domain(&domain)?;
        let root = cover.root.as_ref().ok_or_else(|| {
            Status::invalid_argument(crate::services::errmsg::QUERY_MISSING_ROOT_OR_CORRELATION)
        })?;

        let root_uuid = uuid::Uuid::from_slice(&root.value).map_err(|e| {
            Status::invalid_argument(format!("{}{}", crate::services::errmsg::INVALID_UUID, e))
        })?;

        let edition = cover.edition().unwrap_or_default().to_string();
        let event_book_repo = self.event_book_repo.clone();

        tokio::spawn(async move {
            match event_book_repo.get(&domain, &edition, root_uuid).await {
                Ok(book) => {
                    if tx.send(Ok(book)).await.is_err() {
                        debug!(domain = %domain, root = %root_uuid, "Client disconnected before response");
                    }
                }
                Err(e) => {
                    if tx.send(Err(Status::internal(e.to_string()))).await.is_err() {
                        debug!(domain = %domain, root = %root_uuid, "Client disconnected before error could be sent");
                    }
                }
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
                        let cover = match query.cover.as_ref() {
                            Some(c) => c,
                            None => {
                                if tx
                                    .send(Err(Status::invalid_argument(
                                        crate::services::errmsg::QUERY_MISSING_COVER,
                                    )))
                                    .await
                                    .is_err()
                                {
                                    debug!("Client disconnected during synchronize");
                                    break;
                                }
                                continue;
                            }
                        };
                        let domain = cover.domain.clone();
                        let edition = cover.edition().unwrap_or_default();
                        let root = match cover.root.as_ref() {
                            Some(r) => match uuid::Uuid::from_slice(&r.value) {
                                Ok(uuid) => uuid,
                                Err(e) => {
                                    error!(error = %e, "Invalid UUID in synchronize query");
                                    if tx
                                        .send(Err(Status::invalid_argument(format!(
                                            "{}{e}",
                                            crate::services::errmsg::INVALID_UUID
                                        ))))
                                        .await
                                        .is_err()
                                    {
                                        debug!("Client disconnected during synchronize");
                                        break;
                                    }
                                    continue;
                                }
                            },
                            None => {
                                if tx
                                    .send(Err(Status::invalid_argument(
                                        crate::services::errmsg::QUERY_MISSING_ROOT,
                                    )))
                                    .await
                                    .is_err()
                                {
                                    debug!("Client disconnected during synchronize");
                                    break;
                                }
                                continue;
                            }
                        };

                        // Handle selection: range, specific sequences, temporal, or full query
                        // H-36: match get_event_book's inclusive→exclusive
                        // conversion so the same Query produces the same
                        // event set whether the caller goes through the
                        // unary or the bidi-stream RPC.
                        let result = match query.selection {
                            Some(Selection::Range(ref range)) => {
                                let lower = range.lower;
                                let upper =
                                    range.upper.map(|u| u.saturating_add(1)).unwrap_or(u32::MAX);
                                event_book_repo
                                    .get_from_to(&domain, edition, root, lower, upper)
                                    .await
                            }
                            Some(Selection::Sequences(ref seq_set)) => {
                                event_book_repo
                                    .get_sequences(&domain, edition, root, &seq_set.values)
                                    .await
                            }
                            Some(Selection::Temporal(ref tq)) => match tq.point_in_time {
                                Some(PointInTime::AsOfTime(ref ts)) => {
                                    match crate::storage::helpers::timestamp_to_rfc3339(ts) {
                                        Ok(rfc3339) => {
                                            event_book_repo
                                                .get_temporal_by_time(
                                                    &domain, edition, root, &rfc3339,
                                                )
                                                .await
                                        }
                                        Err(e) => {
                                            if tx
                                                .send(Err(Status::invalid_argument(e.to_string())))
                                                .await
                                                .is_err()
                                            {
                                                debug!("Client disconnected during synchronize");
                                                break;
                                            }
                                            continue;
                                        }
                                    }
                                }
                                Some(PointInTime::AsOfSequence(seq)) => {
                                    event_book_repo
                                        .get_temporal_by_sequence(&domain, edition, root, seq)
                                        .await
                                }
                                None => {
                                    if tx
                                        .send(Err(Status::invalid_argument(
                                            crate::services::errmsg::TEMPORAL_QUERY_MISSING_POINT,
                                        )))
                                        .await
                                        .is_err()
                                    {
                                        debug!("Client disconnected during synchronize");
                                        break;
                                    }
                                    continue;
                                }
                            },
                            None => event_book_repo.get(&domain, edition, root).await,
                        };

                        match result {
                            Ok(book) => {
                                info!(domain = %domain, root = %root, "Synchronize: sending event book");
                                if tx.send(Ok(book)).await.is_err() {
                                    break; // Client disconnected
                                }
                            }
                            Err(e) => {
                                error!(domain = %domain, root = %root, error = %e, "Synchronize: failed to get events");
                                if tx.send(Err(Status::internal(e.to_string()))).await.is_err() {
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
