//! `EventQueryService`: `GetEvents` through the gate onto the reads engine;
//! the other RPCs are UNIMPLEMENTED with a typed status.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::slice::from_ref;

use angzarr_coordinator::{Gate, Reach, ReadOp, Served};
use angzarr_ports::{BoxFuture, MaybeSend, MaybeSync};
use angzarr_proto::v1::{Cover, EventBook};
use angzarr_proto_transport_distributed::v1::event_query_service_server::EventQueryService;
use angzarr_proto_transport_distributed::v1::temporal_query::PointInTime;
use angzarr_proto_transport_distributed::v1::{query, CorrelationQuery, Query};
use angzarr_reads::{Query as ReadQuery, ReadError, ReadPorts, Reads, Select};
use tonic::{Request, Response, Status};

use crate::edge::{unimplemented, Presented};
use crate::{status_of, Edge};

/// The read half a coordinator serves: the domains it reaches and the
/// engine that answers `GetEvents`.
pub trait Queries: Served + MaybeSend + MaybeSync {
    /// The books of `query`, chunked under the engine's limits.
    fn events<'a>(
        &'a self,
        query: &'a ReadQuery,
    ) -> BoxFuture<'a, Result<Vec<EventBook>, ReadError>>;
}

/// A [`Reads`] engine and the reach it serves each domain at.
pub struct ServedReads<S> {
    pub(crate) reads: Reads<S>,
    served: BTreeMap<String, Reach>,
}

impl<S: ReadPorts> ServedReads<S> {
    /// `reads`, serving the domains in `served`.
    pub fn new(reads: Reads<S>, served: BTreeMap<String, Reach>) -> ServedReads<S> {
        ServedReads { reads, served }
    }
}

impl<S> Served for ServedReads<S> {
    fn reach(&self, domain: &str) -> Reach {
        self.served.reach(domain)
    }
}

impl<S: ReadPorts + MaybeSend + MaybeSync> Queries for ServedReads<S> {
    fn events<'a>(
        &'a self,
        query: &'a ReadQuery,
    ) -> BoxFuture<'a, Result<Vec<EventBook>, ReadError>> {
        Box::pin(async move { self.reads.events(query).await })
    }
}

type Books =
    Pin<Box<dyn tonic::codegen::tokio_stream::Stream<Item = Result<EventBook, Status>> + Send>>;
type Covers =
    Pin<Box<dyn tonic::codegen::tokio_stream::Stream<Item = Result<Cover, Status>> + Send>>;

/// The wire query as the reads engine's: which pages of the stream, from
/// which correlation. An unset selection is every page.
fn read_query(query: Query) -> ReadQuery {
    let select = match query.selection {
        None => Select::All,
        Some(query::Selection::Range(range)) => Select::Range {
            from: range.lower,
            to: range.upper,
        },
        Some(query::Selection::Sequences(set)) => {
            Select::Sequences(set.values.into_iter().collect())
        }
        Some(query::Selection::Temporal(temporal)) => match temporal.point_in_time {
            Some(PointInTime::AsOfSequence(sequence)) => Select::AsOfSequence(sequence),
            Some(PointInTime::AsOfTime(time)) => Select::AsOfTime(time),
            None => Select::All,
        },
    };
    ReadQuery {
        cover: query.cover.unwrap_or_default(),
        select,
        correlation: query.correlation_id,
    }
}

#[tonic::async_trait]
impl<C, R, G> EventQueryService for Edge<C, R, G>
where
    C: Send + Sync + 'static,
    R: Queries + 'static,
    G: Gate + 'static,
{
    async fn get_event_book(
        &self,
        _request: Request<Query>,
    ) -> Result<Response<EventBook>, Status> {
        Err(unimplemented("get_event_book"))
    }

    type GetEventsStream = Books;

    async fn get_events(&self, request: Request<Query>) -> Result<Response<Books>, Status> {
        let presented = Presented::of(&request)?;
        let session = self.session(&presented).await?;
        // A read is asked of the gate as a write is: a revoked identity is
        // refused here too (UNAUTHENTICATED, shown).
        if !session.still_valid().await {
            return Err(status_of(&angzarr_coordinator::Unauthenticated.coded()));
        }
        let query = read_query(request.into_inner());
        let books = match session.authorize_streams(ReadOp::Events, from_ref(&query.cover)) {
            Ok(()) => self
                .coordinator
                .reads()
                .events(&query)
                .await
                .map_err(|failure| status_of(&failure))?,
            // A stream the caller may not read is an absent one: empty at 0.
            Err(_denied) => vec![EventBook {
                cover: Some(query.cover),
                ..EventBook::default()
            }],
        };
        Ok(Response::new(Box::pin(tokio_stream::iter(
            books.into_iter().map(Ok),
        ))))
    }

    type GetCorrelatedEventsStream = Books;

    async fn get_correlated_events(
        &self,
        _request: Request<CorrelationQuery>,
    ) -> Result<Response<Books>, Status> {
        Err(unimplemented("get_correlated_events"))
    }

    type ListStreamsStream = Covers;

    async fn list_streams(&self, _request: Request<()>) -> Result<Response<Covers>, Status> {
        Err(unimplemented("list_streams"))
    }
}
