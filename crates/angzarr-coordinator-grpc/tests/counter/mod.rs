//! The domain the end-to-end tests serve: a counter (one Fact type,
//! `Counted`) over an in-memory store, behind a gate of bearer tokens, on a
//! Unix socket. Nothing here is the crate's: it is what an embedder writes.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use angzarr_coordinator::{
    Allowlist, Arrival, AuditSink, BodyKind, Caller, Commands, Coordinator, Credential, Denied,
    Gate, Principal, Reach, ReadOp, Served, Trusted, Unauthenticated,
};
use angzarr_coordinator_base::{Answered, Code, Coded};
use angzarr_coordinator_grpc::{Edge, Listener, Running, ServedReads, Server};
use angzarr_ports::memory::MemoryStore;
use angzarr_ports::{BoxFuture, EventRead, EventStore, StoreError, StreamRef, Write};
use angzarr_proto::uuid::Uuid;
use angzarr_proto::v1::event_meta::Causation;
use angzarr_proto::v1::{
    command_page, event_page, Command, CommandBook, CommandPage, Cover, EventBook, EventMeta,
    EventPage, Fact, FactCause, FactOrigin,
};
use angzarr_proto_transport_distributed::v1::command_handler_coordinator_service_client::CommandHandlerCoordinatorServiceClient;
use angzarr_proto_transport_distributed::v1::event_query_service_client::EventQueryServiceClient;
use angzarr_proto_transport_distributed::v1::{query, Query, SequenceRange};
use angzarr_reads::Reads;
use prost_types::{Any, Timestamp};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Request, Status};

/// The one domain served.
pub const DOMAIN: &str = "counter";
/// The Fact type the writers may send.
pub const COUNTED: &str = "type.invalid/counter.v1.Counted";
/// A Fact type no principal may send.
pub const OTHER: &str = "type.invalid/counter.v1.Other";

/// The writer's token: may send `Counted` facts, read events, owns every stream.
pub const WRITER: &str = "writer-secret";
/// The reader's token: may read events only.
pub const READER: &str = "reader-secret";
/// A token that may send `Counted` facts and read events, but owns only
/// stream 7.
pub const SEVEN: &str = "seven-secret";

/// The counter's command half: every fact appends one event to its stream,
/// unless its origin was recorded already, which is a duplicate and writes
/// nothing (the endpoint's own rule: the kernel's, in a real runtime).
pub struct Counter {
    store: Arc<MemoryStore>,
    served: BTreeMap<String, Reach>,
}

impl Served for Counter {
    fn reach(&self, domain: &str) -> Reach {
        self.served.reach(domain)
    }
}

fn internal(e: StoreError) -> Coded {
    Coded::new(
        Code::Internal,
        "counter-store",
        [("cause".to_string(), format!("{e:?}"))],
    )
}

impl Commands for Counter {
    type Failure = Coded;

    fn unknown_domain(&self, domain: &str) -> Coded {
        Coded::new(
            Code::NotFound,
            "counter-unknown-domain",
            [("domain".to_string(), domain.to_string())],
        )
    }

    fn handle<'a>(
        &'a self,
        book: CommandBook,
        _caller: Caller,
    ) -> BoxFuture<'a, Result<Answered, Coded>> {
        Box::pin(async move {
            let cover = book.cover.clone().unwrap_or_default();
            let root = Uuid::try_from(&cover.root.clone().unwrap_or_default())
                .map_err(|_| Coded::new(Code::InvalidArgument, "counter-bad-root", []))?;
            let stream = StreamRef::main(&cover.domain, root);
            let Some(command_page::Body::Fact(fact)) = book.page.and_then(|p| p.body) else {
                return Err(Coded::new(Code::InvalidArgument, "counter-fact-only", []));
            };
            let read = EventRead::read(&*self.store, stream, 0, None)
                .await
                .map_err(internal)?;
            let head = read.next_sequence;
            let origin = fact.origin.as_ref().map(|o| o.external_id.clone());
            let recorded = read.pages.iter().any(|page| {
                matches!(
                    page.meta.as_ref().and_then(|m| m.causation.as_ref()),
                    Some(Causation::Fact(cause))
                        if cause.origin.as_ref().map(|o| o.external_id.clone()) == origin
                )
            });
            if recorded {
                return Ok(Answered::duplicate(EventBook {
                    cover: Some(cover),
                    next_sequence: head,
                    ..EventBook::default()
                }));
            }
            let page = EventPage {
                body: Some(event_page::Body::Event(fact.payload.unwrap_or_default())),
                meta: Some(EventMeta {
                    causation: Some(Causation::Fact(FactCause {
                        command_id: None,
                        origin: fact.origin,
                    })),
                    correlation_id: None,
                    recorded_at: Some(Timestamp {
                        seconds: head as i64 + 1,
                        nanos: 0,
                    }),
                    rules_version: String::new(),
                }),
                sequence: head,
            };
            let pages = [page.clone()];
            self.store
                .write(Write::append(stream, head, &pages))
                .await
                .map_err(internal)?;
            Ok(Answered::applied(EventBook {
                cover: Some(cover),
                pages: vec![page],
                next_sequence: head + 1,
                ..EventBook::default()
            }))
        })
    }
}

/// What the gate saw, and decided.
#[derive(Default)]
pub struct Record {
    /// Each `identify`: the arrival as text and the secret presented.
    pub identified: Vec<(String, Option<Vec<u8>>)>,
    /// Each denial the audit sink was given, as the real reason.
    pub denied: Vec<String>,
    /// Identities whose token is no longer good.
    pub revoked: BTreeSet<String>,
}

/// A gate of bearer tokens: three principals, a revocation list the test
/// flips, and a record of what it was shown.
#[derive(Clone, Default)]
pub struct Tokens {
    state: Arc<Mutex<Record>>,
}

impl Tokens {
    /// From now on `token`'s identity is no longer valid (`still_valid`).
    pub fn revoke(&self, identity: &str) {
        self.state
            .lock()
            .unwrap()
            .revoked
            .insert(identity.to_string());
    }

    /// What the gate has seen so far.
    pub fn identified(&self) -> Vec<(String, Option<Vec<u8>>)> {
        self.state.lock().unwrap().identified.clone()
    }

    /// The real reasons it was told of.
    pub fn denied(&self) -> Vec<String> {
        self.state.lock().unwrap().denied.clone()
    }
}

impl Gate for Tokens {
    type Identity = &'static str;

    fn identify<'a>(
        &'a self,
        arrival: Arrival,
        credential: Option<&'a Credential>,
    ) -> BoxFuture<'a, Result<Principal<&'static str>, Unauthenticated>> {
        self.state.lock().unwrap().identified.push((
            arrival.as_str().to_string(),
            credential.map(|c| c.expose_secret().to_vec()),
        ));
        let principal = credential.and_then(|credential| {
            let sends = Allowlist::none()
                .send(BodyKind::Fact, COUNTED)
                .read(ReadOp::Events);
            if credential.secret_matches(WRITER.as_bytes()) {
                Some(Principal::new("writer", sends))
            } else if credential.secret_matches(READER.as_bytes()) {
                Some(Principal::new(
                    "reader",
                    Allowlist::none().read(ReadOp::Events),
                ))
            } else if credential.secret_matches(SEVEN.as_bytes()) {
                Some(Principal::new("seven", sends))
            } else {
                None
            }
        });
        Box::pin(async move { principal.ok_or(Unauthenticated) })
    }

    fn owns(&self, identity: &&'static str, cover: &Cover) -> bool {
        *identity != "seven" || cover.root.as_ref().map(|r| r.value.to_vec()) == Some(vec![7; 16])
    }

    fn still_valid<'a>(&'a self, identity: &'a &'static str) -> BoxFuture<'a, bool> {
        let valid = !self.state.lock().unwrap().revoked.contains(*identity);
        Box::pin(async move { valid })
    }
}

impl AuditSink<&'static str> for Tokens {
    fn denied(&self, _principal: &&'static str, _arrival: Arrival, denied: &Denied) {
        self.state
            .lock()
            .unwrap()
            .denied
            .push(format!("{denied:?}"));
    }
}

/// A served counter: the store behind it, the gate's handle, the running
/// server and the socket's path.
pub struct Harness {
    pub store: Arc<MemoryStore>,
    pub tokens: Tokens,
    pub running: Running,
    pub path: PathBuf,
}

/// Serve the counter, behind the token gate, on a Unix socket at `path`.
pub async fn serve(path: PathBuf) -> Harness {
    serve_declared(path, Reach::Loopback, None).await
}

/// Serve the counter at `reach`, on a Unix socket at `path` whose listener
/// declares `arrival` (the socket's own default, loopback, when `None`).
pub async fn serve_declared(path: PathBuf, reach: Reach, arrival: Option<Arrival>) -> Harness {
    let store = Arc::new(MemoryStore::new());
    let served: BTreeMap<String, Reach> = [(DOMAIN.to_string(), reach)].into();
    let commands = Counter {
        store: Arc::clone(&store),
        served: served.clone(),
    };
    let reads = ServedReads::new(Reads::new(Arc::clone(&store)), served);
    let tokens = Tokens::default();
    let coordinator =
        Coordinator::new(commands, reads, tokens.clone()).with_audit(Arc::new(tokens.clone()));
    let edge = Edge::new(Arc::new(coordinator));
    let running = Server::builder()
        .add_edge(edge)
        .listen(match arrival {
            Some(arrival) => Listener::uds(&path).declare(arrival),
            None => Listener::uds(&path),
        })
        .bind()
        .await
        .expect("binds");
    Harness {
        store,
        tokens,
        running,
        path,
    }
}

/// Serve the counter behind the permissive `Trusted` gate (no credential
/// is asked for).
pub async fn serve_trusted(path: PathBuf) -> Running {
    let store = Arc::new(MemoryStore::new());
    let served: BTreeMap<String, Reach> = [(DOMAIN.to_string(), Reach::Loopback)].into();
    let commands = Counter {
        store: Arc::clone(&store),
        served: served.clone(),
    };
    let reads = ServedReads::new(Reads::new(store), served);
    let edge = Edge::new(Arc::new(Coordinator::new(commands, reads, Trusted)));
    Server::builder()
        .add_edge(edge)
        .listen(Listener::uds(&path))
        .bind()
        .await
        .expect("binds")
}

/// Stream `n` of the counter domain.
pub fn cover(n: u8) -> Cover {
    Cover {
        domain: DOMAIN.to_string(),
        root: Some(angzarr_proto::v1::Uuid {
            value: vec![n; 16].into(),
        }),
        edition: None,
    }
}

/// A Fact for stream `n`: `type_url`, `payload`, recorded under `origin`.
pub fn fact_of(n: u8, type_url: &str, payload: &[u8], origin: &str) -> CommandBook {
    CommandBook {
        cover: Some(cover(n)),
        page: Some(CommandPage {
            body: Some(command_page::Body::Fact(Fact {
                payload: Some(Any {
                    type_url: type_url.to_string(),
                    value: payload.to_vec(),
                }),
                origin: Some(FactOrigin {
                    external_id: origin.to_string(),
                    description: "a test".to_string(),
                }),
            })),
            ..CommandPage::default()
        }),
    }
}

/// A `Counted` fact for stream `n`.
pub fn counted(n: u8, origin: &str) -> CommandBook {
    fact_of(n, COUNTED, origin.as_bytes(), origin)
}

/// A Command (not a Fact) for stream `n`.
pub fn command(n: u8) -> CommandBook {
    CommandBook {
        cover: Some(cover(n)),
        page: Some(CommandPage {
            body: Some(command_page::Body::Command(Command {
                payload: Some(Any {
                    type_url: COUNTED.to_string(),
                    value: Vec::new(),
                }),
                ..Command::default()
            })),
            ..CommandPage::default()
        }),
    }
}

/// `message` as a request carrying `authorization: Bearer <token>`.
pub fn bearing<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).expect("ascii"),
    );
    request
}

/// `message` as a request with the `authorization` header's raw text.
pub fn presenting<T>(message: T, header: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(header).expect("ascii"),
    );
    request
}

/// The query for stream `n` from sequence `from`.
pub fn from_sequence(n: u8, from: u64) -> Query {
    Query {
        cover: Some(cover(n)),
        selection: Some(query::Selection::Range(SequenceRange {
            lower: from,
            upper: None,
        })),
        correlation_id: None,
    }
}

/// The events of `query`, as `token` reads them.
pub async fn events(
    channel: &Channel,
    token: &str,
    query: Query,
) -> Result<Vec<EventBook>, Status> {
    let mut client = EventQueryServiceClient::new(channel.clone());
    let mut stream = client.get_events(bearing(query, token)).await?.into_inner();
    let mut books = Vec::new();
    while let Some(book) = stream.message().await? {
        books.push(book);
    }
    Ok(books)
}

/// A commands client over `channel`.
pub fn commands(channel: &Channel) -> CommandHandlerCoordinatorServiceClient<Channel> {
    CommandHandlerCoordinatorServiceClient::new(channel.clone())
}
