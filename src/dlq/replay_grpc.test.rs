//! Tests for the gRPC replay publisher.
//!
//! Replay is the operator's way to re-run a dead-lettered command: the
//! command must reach the aggregate of its own domain, carry its lineage
//! (source DLQ row, original correlation id), and — in FRESH_SEQUENCE mode —
//! be re-stamped from the aggregate's current sequence so it is not
//! rejected as stale.

use std::sync::{Arc, Mutex as StdMutex};

use tokio::net::TcpListener;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status};

use super::*;
use crate::proto::command_handler_coordinator_service_server::{
    CommandHandlerCoordinatorService, CommandHandlerCoordinatorServiceServer,
};
use crate::proto::event_query_service_server::{EventQueryService, EventQueryServiceServer};
use crate::proto::{
    AggregateRoot, BusinessResponse, CommandPage, CommandResponse, Cover, EventBook, EventRequest,
    ExternalDeferredSequence, FactInjectionResponse, PageHeader, SpeculateCommandHandlerRequest,
    Uuid as ProtoUuid,
};

/// What the fake aggregate observed.
#[derive(Default)]
struct Seen {
    commands: Vec<SeenCommand>,
    queries: Vec<Query>,
}

/// A command the fake aggregate received, with its request metadata.
struct SeenCommand {
    request: CommandRequest,
    dlq_id: Option<String>,
    original_correlation_id: Option<String>,
    correlation_id: Option<String>,
}

#[derive(Clone)]
struct FakeAggregate {
    seen: Arc<StdMutex<Seen>>,
    next_sequence: u32,
    reject: bool,
}

fn header(req: &tonic::metadata::MetadataMap, key: &str) -> Option<String> {
    req.get(key).and_then(|v| v.to_str().ok()).map(String::from)
}

#[tonic::async_trait]
impl CommandHandlerCoordinatorService for FakeAggregate {
    async fn handle_command(
        &self,
        request: Request<CommandRequest>,
    ) -> Result<Response<CommandResponse>, Status> {
        let md = request.metadata().clone();
        self.seen.lock().unwrap().commands.push(SeenCommand {
            request: request.into_inner(),
            dlq_id: header(&md, REPLAYED_FROM_DLQ_ID_HEADER),
            original_correlation_id: header(&md, ORIGINAL_CORRELATION_ID_HEADER),
            correlation_id: header(&md, "x-correlation-id"),
        });
        if self.reject {
            return Err(Status::failed_precondition("Sequence mismatch: stale"));
        }
        Ok(Response::new(CommandResponse::default()))
    }

    async fn handle_event(
        &self,
        _: Request<EventRequest>,
    ) -> Result<Response<FactInjectionResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn handle_sync_speculative(
        &self,
        _: Request<SpeculateCommandHandlerRequest>,
    ) -> Result<Response<CommandResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn handle_compensation(
        &self,
        _: Request<CommandRequest>,
    ) -> Result<Response<BusinessResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
}

#[tonic::async_trait]
impl EventQueryService for FakeAggregate {
    type GetEventsStream = ReceiverStream<Result<EventBook, Status>>;
    type SynchronizeStream = ReceiverStream<Result<EventBook, Status>>;
    type GetAggregateRootsStream = ReceiverStream<Result<AggregateRoot, Status>>;

    async fn get_event_book(&self, request: Request<Query>) -> Result<Response<EventBook>, Status> {
        self.seen.lock().unwrap().queries.push(request.into_inner());
        Ok(Response::new(EventBook {
            next_sequence: self.next_sequence,
            ..Default::default()
        }))
    }

    async fn get_events(
        &self,
        _: Request<Query>,
    ) -> Result<Response<Self::GetEventsStream>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn synchronize(
        &self,
        _: Request<tonic::Streaming<Query>>,
    ) -> Result<Response<Self::SynchronizeStream>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn get_aggregate_roots(
        &self,
        _: Request<()>,
    ) -> Result<Response<Self::GetAggregateRootsStream>, Status> {
        Err(Status::unimplemented("not used"))
    }
}

async fn start(aggregate: FakeAggregate) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(CommandHandlerCoordinatorServiceServer::new(
                aggregate.clone(),
            ))
            .add_service(EventQueryServiceServer::new(aggregate))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    addr
}

fn fake(next_sequence: u32, reject: bool) -> (FakeAggregate, Arc<StdMutex<Seen>>) {
    let seen = Arc::new(StdMutex::new(Seen::default()));
    (
        FakeAggregate {
            seen: Arc::clone(&seen),
            next_sequence,
            reject,
        },
        seen,
    )
}

fn page(sequence_type: SequenceType) -> CommandPage {
    CommandPage {
        header: Some(PageHeader {
            sequence_type: Some(sequence_type),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn command(domain: &str) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(ProtoUuid { value: vec![1; 16] }),
            correlation_id: "new-corr".to_string(),
            ..Default::default()
        }),
        pages: vec![page(SequenceType::Sequence(3))],
    }
}

fn metadata(mode: ReplayMode) -> ReplayMetadata {
    ReplayMetadata {
        replayed_from_dlq_id: 42,
        original_correlation_id: "orig-corr".to_string(),
        mode,
    }
}

fn sequence_of(page: &CommandPage) -> Option<u32> {
    match page.header.as_ref()?.sequence_type.as_ref()? {
        SequenceType::Sequence(s) => Some(*s),
        _ => None,
    }
}

/// AS_IS re-submits the command unchanged to its domain's aggregate, with
/// its lineage in request metadata and no sequence lookup.
#[tokio::test]
async fn as_is_submits_unchanged_command_with_lineage() {
    let (agg, seen) = fake(9, false);
    let addr = start(agg).await;
    let publisher = GrpcReplayPublisher::new([("order".to_string(), addr)]);

    publisher
        .replay(command("order"), metadata(ReplayMode::AsIs))
        .await
        .expect("replayed");

    let seen = seen.lock().unwrap();
    assert!(
        seen.queries.is_empty(),
        "AS_IS must not look up the sequence"
    );
    let seen_cmd = &seen.commands[0];
    let sent = seen_cmd.request.command.as_ref().unwrap();
    assert_eq!(sequence_of(&sent.pages[0]), Some(3));
    assert_eq!(seen_cmd.request.sync_mode(), SyncMode::Simple);
    assert_eq!(seen_cmd.dlq_id.as_deref(), Some("42"));
    assert_eq!(
        seen_cmd.original_correlation_id.as_deref(),
        Some("orig-corr")
    );
    assert_eq!(seen_cmd.correlation_id.as_deref(), Some("new-corr"));
}

/// FRESH_SEQUENCE re-stamps explicit sequences from the aggregate's
/// current next_sequence, looked up by domain + root (not correlation).
#[tokio::test]
async fn fresh_sequence_restamps_from_current_next_sequence() {
    let (agg, seen) = fake(9, false);
    let addr = start(agg).await;
    let publisher = GrpcReplayPublisher::new([("order".to_string(), addr)]);

    publisher
        .replay(command("order"), metadata(ReplayMode::FreshSequence))
        .await
        .expect("replayed");

    let seen = seen.lock().unwrap();
    let query_cover = seen.queries[0].cover.as_ref().unwrap();
    assert_eq!(query_cover.domain, "order");
    assert!(
        query_cover.correlation_id.is_empty(),
        "lookup must be by root"
    );
    let sent = seen.commands[0].request.command.as_ref().unwrap();
    assert_eq!(sequence_of(&sent.pages[0]), Some(9));
}

/// A command for a domain with no configured endpoint is refused without
/// contacting any aggregate.
#[tokio::test]
async fn unknown_domain_is_refused() {
    let publisher = GrpcReplayPublisher::new(Vec::<(String, String)>::new());
    let err = publisher
        .replay(command("order"), metadata(ReplayMode::AsIs))
        .await
        .unwrap_err();
    assert!(
        matches!(err, DlqError::InvalidArgument(ref m) if m.contains("order")),
        "{err}"
    );
}

/// An aggregate rejection surfaces as a publish failure (the handler then
/// records a failed replay and the row stays in the DLQ).
#[tokio::test]
async fn aggregate_rejection_is_publish_failure() {
    let (agg, _seen) = fake(9, true);
    let addr = start(agg).await;
    let publisher = GrpcReplayPublisher::new([("order".to_string(), addr)]);

    let err = publisher
        .replay(command("order"), metadata(ReplayMode::AsIs))
        .await
        .unwrap_err();
    assert!(
        matches!(err, DlqError::PublishFailed(ref m) if m.contains("stale")),
        "{err}"
    );
}

/// Only explicit sequences are re-stamped, consecutively; deferred pages
/// keep their framework-assigned provenance.
#[test]
fn restamp_only_touches_explicit_sequences() {
    let mut book = CommandBook {
        cover: None,
        pages: vec![
            page(SequenceType::Sequence(1)),
            page(SequenceType::ExternalDeferred(
                ExternalDeferredSequence::default(),
            )),
            page(SequenceType::Sequence(2)),
        ],
    };
    restamp_sequences(&mut book, 10);
    assert_eq!(sequence_of(&book.pages[0]), Some(10));
    assert!(matches!(
        book.pages[1].header.as_ref().unwrap().sequence_type,
        Some(SequenceType::ExternalDeferred(_))
    ));
    assert_eq!(sequence_of(&book.pages[2]), Some(11));
}
