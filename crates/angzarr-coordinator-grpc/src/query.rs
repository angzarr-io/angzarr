//! `EventQueryService`: `GetEvents` through the gate onto the reads engine;
//! the other RPCs are UNIMPLEMENTED with a typed status.

use std::collections::BTreeMap;
use std::pin::Pin;

use angzarr_coordinator::{Gate, Reach, Served};
use angzarr_ports::{BoxFuture, MaybeSend, MaybeSync};
use angzarr_proto::v1::{Cover, EventBook};
use angzarr_proto_transport_distributed::v1::event_query_service_server::EventQueryService;
use angzarr_proto_transport_distributed::v1::{CorrelationQuery, Query};
use angzarr_reads::{Query as ReadQuery, ReadError, ReadPorts, Reads};
use tonic::{Request, Response, Status};

use crate::Edge;

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
    reads: Reads<S>,
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
        Err(Status::unimplemented("stub"))
    }

    type GetEventsStream = Books;

    async fn get_events(&self, _request: Request<Query>) -> Result<Response<Books>, Status> {
        Err(Status::unimplemented("stub"))
    }

    type GetCorrelatedEventsStream = Books;

    async fn get_correlated_events(
        &self,
        _request: Request<CorrelationQuery>,
    ) -> Result<Response<Books>, Status> {
        Err(Status::unimplemented("stub"))
    }

    type ListStreamsStream = Covers;

    async fn list_streams(&self, _request: Request<()>) -> Result<Response<Covers>, Status> {
        Err(Status::unimplemented("stub"))
    }
}
