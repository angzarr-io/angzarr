//! The event query service (`EventQueryService`, query.proto).
//!
//! Serves a domain's stored events. A returned book may start from a
//! snapshot only when the coordinator's own registration accepts it under
//! the snapshot rule ([`crate::rules_version`]); otherwise, and while no
//! component is registered, it carries every page from 0.

use std::sync::Arc;

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::debug;
use uuid::Uuid;

use super::RegistrationSource;
use crate::proto::{
    event_query_service_server::EventQueryService as EventQueryTrait, query::Selection,
    temporal_query::PointInTime, CorrelationQuery, Cover, EventBook, EventPage, Query, Snapshot,
    TemporalQuery, Uuid as ProtoUuid,
};
use crate::proto_ext::EventBookExt;
use crate::rules_version::SnapshotRules;
use crate::storage::{EventStore, SnapshotStore, StreamRef};
use crate::upcast::EventUpcaster;

/// Refusal messages.
pub mod errmsg {
    pub const MISSING_COVER: &str = "Query.cover is required";
    pub const MISSING_DOMAIN: &str = "Query.cover.domain is required";
    pub const MISSING_ROOT_OR_CORRELATION: &str =
        "a Query needs cover.root, correlation_id, or both";
    pub const MALFORMED_UUID: &str = " must be a 16-byte UUID";
    pub const EMPTY_EDITION: &str = "edition is set but empty: main is the unset edition";
    pub const UNKNOWN_EDITION: &str = "edition was never created: ";
    pub const MISSING_POINT: &str = "TemporalQuery must set as_of_time or as_of_sequence";
    pub const MISSING_CORRELATION: &str = "CorrelationQuery.correlation_id is required";
}

/// `EventQueryService` over the coordinator's stores.
pub struct EventQueryService {
    event_store: Arc<dyn EventStore>,
    snapshot_store: Arc<dyn SnapshotStore>,
    registration: Option<Arc<dyn RegistrationSource>>,
    rules: Arc<SnapshotRules>,
    upcaster: Option<Arc<dyn EventUpcaster>>,
}

impl EventQueryService {
    /// A query service over `event_store` and `snapshot_store`, judging
    /// snapshots under `rules`. It holds no registration until one is given
    /// ([`Self::with_registration`]), so it offers no snapshot and every book
    /// starts at sequence 0 (C-0556).
    pub fn new(
        event_store: Arc<dyn EventStore>,
        snapshot_store: Arc<dyn SnapshotStore>,
        rules: Arc<SnapshotRules>,
    ) -> Self {
        Self {
            event_store,
            snapshot_store,
            registration: None,
            rules,
            upcaster: None,
        }
    }

    /// Judge snapshots against `registration` (the mounting coordinator's
    /// own, decision 14.21).
    pub fn with_registration(mut self, registration: Arc<dyn RegistrationSource>) -> Self {
        self.registration = Some(registration);
        self
    }

    /// Upcast every returned page (readers see only current shapes).
    pub fn with_upcaster(mut self, upcaster: Arc<dyn EventUpcaster>) -> Self {
        self.upcaster = Some(upcaster);
        self
    }

    async fn upcast(&self, mut books: Vec<EventBook>) -> Result<Vec<EventBook>, Status> {
        let Some(upcaster) = &self.upcaster else {
            return Ok(books);
        };
        for book in &mut books {
            if book.pages.is_empty() {
                continue;
            }
            let domain = book
                .cover
                .as_ref()
                .map(|c| c.domain.clone())
                .unwrap_or_default();
            book.pages = upcaster
                .upcast(&domain, std::mem::take(&mut book.pages))
                .await?;
        }
        Ok(books)
    }

    /// The newest stored snapshot with sequence <= `limit` that this
    /// coordinator's own registration accepts; none while it holds no
    /// registration (decisions 14.4, 14.21).
    async fn offerable_snapshot(
        &self,
        stream: StreamRef<'_>,
        limit: Option<u64>,
    ) -> Result<Option<Snapshot>, Status> {
        let Some(registration) = self.registration.as_ref().and_then(|r| r.registration()) else {
            return Ok(None);
        };
        let snapshot = self
            .snapshot_store
            .get(stream)
            .await
            .map_err(storage_status)?;
        Ok(snapshot
            .filter(|s| limit.is_none_or(|l| s.sequence <= l))
            .filter(|s| self.rules.offerable(stream.domain, s, &registration)))
    }

    /// The books a Query selects: one for a stream query, one per matching
    /// stream for a correlation query over a domain.
    pub async fn select(&self, query: &Query) -> Result<Vec<EventBook>, Status> {
        let cover = query
            .cover
            .as_ref()
            .ok_or_else(|| Status::invalid_argument(errmsg::MISSING_COVER))?;
        if cover.domain.is_empty() {
            return Err(Status::invalid_argument(errmsg::MISSING_DOMAIN));
        }
        check_edition(cover.edition.as_deref())?;
        let correlation = uuid_field("Query.correlation_id", query.correlation_id.as_ref())?;
        let root = uuid_field("Query.cover.root", cover.root.as_ref())?;
        match (root, correlation) {
            (Some(root), correlation) => {
                let stream = StreamRef::main(&cover.domain, root);
                let mut book = self.stream_book(stream, query.selection.as_ref()).await?;
                if let Some(correlation) = correlation {
                    keep_correlation(&mut book, &correlation);
                }
                Ok(vec![book])
            }
            (None, Some(correlation)) => self
                .event_store
                .read_by_correlation(Some(&cover.domain), None, &correlation)
                .await
                .map_err(storage_status),
            (None, None) => Err(Status::invalid_argument(
                errmsg::MISSING_ROOT_OR_CORRELATION,
            )),
        }
    }

    /// One stream's book under a selection.
    async fn stream_book(
        &self,
        stream: StreamRef<'_>,
        selection: Option<&Selection>,
    ) -> Result<EventBook, Status> {
        let snapshot = match selection {
            None => self.offerable_snapshot(stream, None).await?,
            Some(Selection::Temporal(TemporalQuery {
                point_in_time: Some(PointInTime::AsOfSequence(n)),
            })) => self.offerable_snapshot(stream, Some(*n)).await?,
            _ => None,
        };
        let from = snapshot.as_ref().map(|s| s.sequence + 1).unwrap_or(0);
        let pages = match selection {
            None => self.event_store.read(stream, from, None).await,
            Some(Selection::Range(range)) => {
                // SequenceRange.upper is inclusive; reads are half-open.
                let to = range.upper.map(|u| u.saturating_add(1));
                self.event_store.read(stream, range.lower, to).await
            }
            Some(Selection::Sequences(set)) => {
                self.event_store.read(stream, 0, None).await.map(|pages| {
                    pages
                        .into_iter()
                        .filter(|p| set.values.contains(&p.sequence))
                        .collect()
                })
            }
            Some(Selection::Temporal(temporal)) => match &temporal.point_in_time {
                Some(PointInTime::AsOfSequence(n)) => {
                    self.event_store
                        .read(stream, from, Some(n.saturating_add(1)))
                        .await
                }
                Some(PointInTime::AsOfTime(t)) => self.event_store.read_until_time(stream, t).await,
                None => return Err(Status::invalid_argument(errmsg::MISSING_POINT)),
            },
        }
        .map_err(storage_status)?;
        let mut book = book(stream.to_cover(), pages);
        book.snapshot = snapshot;
        book.next_sequence = book.computed_next_sequence();
        Ok(book)
    }
}

fn book(cover: Cover, pages: Vec<EventPage>) -> EventBook {
    let mut book = EventBook {
        cover: Some(cover),
        pages,
        ..Default::default()
    };
    book.next_sequence = book.computed_next_sequence();
    book
}

fn keep_correlation(book: &mut EventBook, correlation: &Uuid) {
    let wanted: ProtoUuid = (*correlation).into();
    book.pages
        .retain(|p| p.meta.as_ref().and_then(|m| m.correlation_id.as_ref()) == Some(&wanted));
}

/// A set edition: empty is malformed; a name was never created (no edition
/// registry exists yet, backlog EA-1).
fn check_edition(edition: Option<&str>) -> Result<(), Status> {
    match edition {
        None => Ok(()),
        Some("") => Err(Status::invalid_argument(errmsg::EMPTY_EDITION)),
        Some(name) => Err(Status::not_found(format!(
            "{}{name}",
            errmsg::UNKNOWN_EDITION
        ))),
    }
}

fn uuid_field(name: &str, value: Option<&ProtoUuid>) -> Result<Option<Uuid>, Status> {
    value
        .map(|id| {
            Uuid::try_from(id)
                .map_err(|_| Status::invalid_argument(format!("{name}{}", errmsg::MALFORMED_UUID)))
        })
        .transpose()
}

fn storage_status(e: crate::storage::StorageError) -> Status {
    Status::unavailable(e.to_string())
}

fn stream_of<T: Send + 'static>(items: Vec<T>) -> ReceiverStream<Result<T, Status>> {
    let (tx, rx) = tokio::sync::mpsc::channel(32);
    tokio::spawn(async move {
        for item in items {
            if tx.send(Ok(item)).await.is_err() {
                debug!("query client disconnected");
                break;
            }
        }
    });
    ReceiverStream::new(rx)
}

#[tonic::async_trait]
impl EventQueryTrait for EventQueryService {
    type GetEventsStream = ReceiverStream<Result<EventBook, Status>>;
    type GetCorrelatedEventsStream = ReceiverStream<Result<EventBook, Status>>;
    type GetAggregateRootsStream = ReceiverStream<Result<Cover, Status>>;

    async fn get_event_book(&self, request: Request<Query>) -> Result<Response<EventBook>, Status> {
        let books = self
            .upcast(self.select(&request.into_inner()).await?)
            .await?;
        Ok(Response::new(books.into_iter().next().unwrap_or_default()))
    }

    async fn get_events(
        &self,
        request: Request<Query>,
    ) -> Result<Response<Self::GetEventsStream>, Status> {
        let books = self
            .upcast(self.select(&request.into_inner()).await?)
            .await?;
        Ok(Response::new(stream_of(books)))
    }

    async fn get_correlated_events(
        &self,
        request: Request<CorrelationQuery>,
    ) -> Result<Response<Self::GetCorrelatedEventsStream>, Status> {
        let query = request.into_inner();
        let correlation = uuid_field(
            "CorrelationQuery.correlation_id",
            query.correlation_id.as_ref(),
        )?
        .ok_or_else(|| Status::invalid_argument(errmsg::MISSING_CORRELATION))?;
        check_edition(query.edition.as_deref())?;
        let books = self
            .event_store
            .read_by_correlation(None, None, &correlation)
            .await
            .map_err(storage_status)?;
        let books = self.upcast(books).await?;
        Ok(Response::new(stream_of(books)))
    }

    async fn get_aggregate_roots(
        &self,
        _request: Request<()>,
    ) -> Result<Response<Self::GetAggregateRootsStream>, Status> {
        let covers = self
            .event_store
            .list_streams()
            .await
            .map_err(storage_status)?;
        Ok(Response::new(stream_of(covers)))
    }
}

#[cfg(test)]
#[path = "service.test.rs"]
mod tests;
