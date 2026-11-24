//! The conformance suite run against the generic bus over the store's
//! transport, on angzarr-ports' in-memory store: the suite's own reference
//! run, so the suite is proven runnable (and its checks able to fail) in
//! this crate. Its dispatch is a reference one: a projector is the suite's
//! sink; a policy's outputs are appended to their targets, each
//! deduplicated by its derived id, its position written with the last.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use angzarr_bus::coded_diagnostic::{Arg, Diagnostic, Report};
use angzarr_bus::contract::transport::TransportHarness;
use angzarr_bus::contract::{Delivery, Domains, Failing, Harness, React, Scripted, Sink, Wake};
use angzarr_bus::{
    Admitted, Answer, Dispatch, Following, Handler, Hub, Item, Pumped, RetryPolicy, Scope,
    StoreRead, StoreTransport, Timer,
};
use angzarr_ports::faulty::{Fault, FaultyStore};
use angzarr_ports::memory::MemoryStore;
use angzarr_ports::{
    Append, BoxFuture, EventStore, PositionKey, PositionStore, Processed, Purge, StoreError,
    StreamRef, Write,
};
use angzarr_proto::ids::policy_command_id;
use angzarr_proto::v1::event_meta::Causation;
use angzarr_proto::v1::{event_page, CommandCause, Cover, EventMeta, EventPage, EventRef};
use prost_types::Any;

/// A subscription a bus refuses: a second of a name, or a policy whose
/// positions cannot be committed with its outputs.
#[derive(Debug)]
struct Taken(String, &'static str);

impl Taken {
    fn name(name: &str) -> Taken {
        Taken(name.to_string(), "angzarr-bus-test-taken")
    }

    fn not_atomic(name: &str) -> Taken {
        Taken(name.to_string(), "angzarr-bus-test-not-atomic")
    }
}

impl fmt::Display for Taken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_bus::coded_diagnostic::display(self, f)
    }
}

impl Diagnostic for Taken {
    fn code(&self) -> &str {
        self.1
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![("name", Arg::Str(self.0.clone()))]
    }
}

/// The reference dispatch's policy.
struct Policy {
    name: String,
    source: String,
    react: Arc<dyn React>,
}

type Store = FaultyStore<MemoryStore>;

/// The reference dispatch. See the module.
struct Reference<'s> {
    store: &'s Store,
    letters: &'s AtomicUsize,
}

impl Dispatch for Reference<'_> {
    type Projector = (String, Arc<dyn Sink>);
    type Policy = Policy;
    type Refused = Taken;
    type Error = StoreError;

    fn admit(&self, handler: &Handler<Self::Projector, Policy>) -> Result<Admitted, Taken> {
        Ok(match handler {
            Handler::Projector { scope, projector } => Admitted {
                name: projector.0.clone(),
                scope: scope.clone(),
            },
            Handler::Policy(policy) => Admitted {
                name: policy.name.clone(),
                scope: Scope::domain(&policy.source),
            },
        })
    }

    fn taken(&self, name: &str) -> Taken {
        Taken::name(name)
    }

    fn non_atomic_policy(&self, name: &str) -> Taken {
        Taken::not_atomic(name)
    }

    async fn missing<'a>(
        &'a self,
        _: &'a Handler<Self::Projector, Policy>,
        _: &'a str,
        _: &'a Cover,
        _: u64,
        _: u64,
    ) -> Result<(), StoreError> {
        self.lettered();
        Ok(())
    }

    async fn project<'a>(
        &'a self,
        projector: &'a Self::Projector,
        _: &'a str,
        item: Item<'a>,
    ) -> Answer {
        let stream = item.stream.clone();
        let delivery = match item.page {
            Some(_) => Delivery::Event {
                stream,
                sequence: item.sequence,
            },
            None => Delivery::Gone { stream },
        };
        match projector.1.handle(delivery).await {
            Ok(()) => Answer::Handled,
            Err(Failing::Retryable(report)) if !item.last => Answer::Held(report),
            Err(Failing::Retryable(report) | Failing::Bug(report)) => {
                self.letters.fetch_add(1, Ordering::SeqCst);
                Answer::Passed(report)
            }
        }
    }

    async fn react<'a>(&'a self, policy: &'a Policy, name: &'a str, item: Item<'a>) -> Answer {
        let trigger = EventRef {
            stream: Some(item.stream.clone()),
            sequence: item.sequence,
        };
        let position = [(item.stream, item.sequence + 1)];
        let roots = match policy.react.react(&trigger).await {
            Ok(roots) => roots,
            Err(report) => {
                self.letters.fetch_add(1, Ordering::SeqCst);
                return match self.output(name, &position, None).await {
                    Ok(()) => Answer::Passed(report),
                    Err(error) => Answer::Held(Report::of(&error)),
                };
            }
        };
        if roots.is_empty() {
            return match self.output(name, &position, None).await {
                Ok(()) => Answer::Handled,
                Err(error) => Answer::Held(Report::of(&error)),
            };
        }
        let last = roots.len() - 1;
        for (index, root) in roots.into_iter().enumerate() {
            let target = Cover {
                domain: "target".to_string(),
                root: Some(root),
                edition: item.stream.edition.clone(),
            };
            let id = policy_command_id(&trigger, name, index as u32).unwrap_or_default();
            let positions = match index == last {
                true => &position[..],
                false => &[],
            };
            if let Err(error) = self.output(name, positions, Some((&target, id))).await {
                return Answer::Held(Report::of(&error));
            }
        }
        Answer::Handled
    }
}

impl Reference<'_> {
    /// A dead letter kept.
    fn lettered(&self) {
        self.letters.fetch_add(1, Ordering::SeqCst);
    }

    /// One output to `target` under `id`, unless it landed already, with
    /// `positions` (each the policy's next sequence on a stream), as one
    /// write; with no output, the positions alone.
    async fn output(
        &self,
        name: &str,
        positions: &[(&Cover, u64)],
        target: Option<(&Cover, angzarr_proto::v1::Uuid)>,
    ) -> Result<(), StoreError> {
        let keyed: Vec<(PositionKey<'_>, u64)> = (positions.iter())
            .map(|(stream, next)| {
                let stream = StreamRef::try_from(*stream)?;
                Ok((
                    PositionKey {
                        handler: name,
                        stream,
                    },
                    *next,
                ))
            })
            .collect::<Result<_, StoreError>>()?;
        let Some((target, id)) = target else {
            return self.store.write(Write::positions(&keyed)).await.map(drop);
        };
        let sref = StreamRef::try_from(target)?;
        let uuid = angzarr_proto::uuid::Uuid::from_bytes(id.to_bytes().unwrap_or_default());
        if !self.store.find_command(sref, uuid).await?.is_empty() {
            if keyed.is_empty() {
                return Ok(());
            }
            return self.store.write(Write::positions(&keyed)).await.map(drop);
        }
        let head = self.store.read(sref, 0, None).await?.next_sequence;
        let page = [page(head, Some(id))];
        let write = Write {
            events: Some(Append {
                stream: sref,
                expected: head,
                pages: &page,
            }),
            positions: &keyed,
            ..Write::default()
        };
        self.store.write(write).await.map(drop)
    }
}

/// An event at `sequence`, caused by command `id` if any.
fn page(sequence: u64, id: Option<angzarr_proto::v1::Uuid>) -> EventPage {
    EventPage {
        sequence,
        body: Some(event_page::Body::Event(Any {
            type_url: "type.googleapis.com/angzarr.bus.test.Happened".to_string(),
            value: Vec::new(),
        })),
        meta: Some(EventMeta {
            causation: id.map(|id| {
                Causation::Command(CommandCause {
                    command_id: Some(id),
                })
            }),
            ..EventMeta::default()
        }),
    }
}

/// A timer that fires as soon as the runtime has run everything else.
struct Soon;

impl Timer for Soon {
    fn after(&self, _: Duration) -> BoxFuture<'_, ()> {
        Box::pin(tokio::task::yield_now())
    }
}

/// A timer that never fires.
struct Never;

impl Timer for Never {
    fn after(&self, _: Duration) -> BoxFuture<'_, ()> {
        Box::pin(std::future::pending())
    }
}

/// The reference run: a faulty in-memory store, its commits pushed.
#[derive(Default)]
struct Run {
    store: Store,
    letters: AtomicUsize,
    commands: AtomicU64,
    /// The store transport is told which streams a commit touched.
    incremental: bool,
}

const EVERY: Duration = Duration::from_secs(1);

/// How long a check may wait: a check takes milliseconds, so a hang is a
/// quick failure.
const DEADLINE: Duration = Duration::from_secs(3);

/// The bus's retry cap in the reference run.
const CAP: u32 = 2;

/// The reference run's retry policy: two retries, at once.
fn retry() -> RetryPolicy {
    RetryPolicy {
        cap: Some(CAP),
        ..RetryPolicy::default()
    }
}

impl Run {
    /// A run whose store transport reads only the streams a commit touched.
    fn incremental() -> Run {
        Run {
            incremental: true,
            ..Run::default()
        }
    }

    fn memory(&self) -> &MemoryStore {
        self.store.inner()
    }

    /// A hub over `transport`, pumped: its positions in the faulty store,
    /// its recovery and resolution reading the memory store beneath, a
    /// reader apart from the position store.
    fn hub<T>(&self, transport: T) -> ReferenceBus<'_, T> {
        let reads = StoreRead::new(self.memory());
        let hub = Hub::reading(Pumped::new(transport), &self.store, self.reference(), reads);
        hub.retrying(retry(), || 0.0)
    }

    fn reference(&self) -> Reference<'_> {
        Reference {
            store: &self.store,
            letters: &self.letters,
        }
    }

    fn transport(&self, timer: &'static dyn Timer) -> StoreTransport<'_, Store> {
        let following = match self.incremental {
            true => Following::Changes(self.memory()),
            false => Following::FullScan,
        };
        StoreTransport::new(&self.store, timer, EVERY, following).pushed_by(self.memory())
    }

    async fn pages(&self, stream: &Cover) -> Vec<EventPage> {
        let sref = StreamRef::try_from(stream).unwrap();
        self.memory().read(sref, 0, None).await.unwrap().pages
    }
}

type ReferenceBus<'h, T> = Hub<'h, Pumped<T>, Store, Reference<'h>, StoreRead<'h, MemoryStore>>;

impl Harness for Run {
    type Projector = (String, Arc<dyn Sink>);
    type Policy = Policy;
    type Bus<'h> = ReferenceBus<'h, StoreTransport<'h, Store>>;
    type Scripted<'h> = ReferenceBus<'h, Scripted>;

    fn domains(&self) -> Domains {
        Domains {
            first: "first".to_string(),
            second: "second".to_string(),
            target: "target".to_string(),
        }
    }

    fn bus(&self, wake: Wake) -> Self::Bus<'_> {
        let timer: &'static dyn Timer = match wake {
            Wake::Poll => &Soon,
            Wake::CommitOnly => &Never,
        };
        self.hub(self.transport(timer))
    }

    fn bus_over(&self, transport: Scripted) -> Self::Scripted<'_> {
        self.hub(transport)
    }

    fn projector(&self, name: &str, sink: Arc<dyn Sink>) -> Self::Projector {
        (name.to_string(), sink)
    }

    fn policy(&self, name: &str, source: &str, react: Arc<dyn React>) -> Policy {
        Policy {
            name: name.to_string(),
            source: source.to_string(),
            react,
        }
    }

    async fn append(&self, stream: &Cover) {
        let head = self.pages(stream).await.len() as u64;
        let n = self.commands.fetch_add(1, Ordering::SeqCst);
        let id = angzarr_proto::v1::Uuid::from_bytes(u128::from(n + 1).to_be_bytes());
        let page = [page(head, Some(id))];
        let sref = StreamRef::try_from(stream).unwrap();
        self.memory()
            .write(Write::append(sref, head, &page))
            .await
            .unwrap();
    }

    async fn purge(&self, stream: &Cover) {
        let head = self.pages(stream).await.len() as u64;
        let sref = StreamRef::try_from(stream).unwrap();
        self.memory().purge(sref, head).await.unwrap();
    }

    async fn landed(&self, stream: &Cover) -> Vec<EventPage> {
        self.pages(stream).await
    }

    async fn position_rows(&self, name: &str) -> usize {
        self.memory().processed(name).await.unwrap().len()
    }

    async fn position(&self, name: &str, stream: &Cover) -> u64 {
        let stream = StreamRef::try_from(stream).unwrap();
        let at = PositionKey {
            handler: name,
            stream,
        };
        self.memory().position(at).await.unwrap()
    }

    async fn rewind(&self, name: &str, stream: &Cover, to: u64) {
        record(self.memory(), name, stream, to).await;
    }

    fn fail_position_commit(&self, outputs: usize) {
        let sound = std::iter::repeat_n(Fault::Sound, outputs.saturating_sub(1));
        self.store.fail(sound.chain([Fault::Unavailable]));
    }

    fn retry_cap(&self) -> u32 {
        CAP
    }

    async fn dead_letters(&self) -> usize {
        self.letters.load(Ordering::SeqCst)
    }

    fn deadline(&self) -> Pin<Box<dyn Future<Output = ()> + '_>> {
        Box::pin(tokio::time::sleep(DEADLINE))
    }
}

/// Subscription `name`'s position on `stream` written at `mark`.
async fn record(store: &MemoryStore, name: &str, stream: &Cover, mark: u64) {
    let sref = StreamRef::try_from(stream).unwrap();
    let positions = [(
        PositionKey {
            handler: name,
            stream: sref,
        },
        mark,
    )];
    store.write(Write::positions(&positions)).await.unwrap();
}

impl TransportHarness for Run {
    type Transport<'h> = StoreTransport<'h, Store>;

    fn domain(&self) -> String {
        "first".to_string()
    }

    fn transport(&self) -> Self::Transport<'_> {
        Run::transport(self, &Soon)
    }

    async fn append(&self, stream: &Cover) {
        Harness::append(self, stream).await;
    }

    async fn purge(&self, stream: &Cover) {
        Harness::purge(self, stream).await;
    }

    async fn stored(&self, stream: &Cover, sequence: u64) -> Option<EventPage> {
        self.pages(stream).await.into_iter().nth(sequence as usize)
    }

    async fn record(&self, name: &str, stream: &Cover, mark: u64) {
        record(self.memory(), name, stream, mark).await;
    }

    fn deadline(&self) -> Pin<Box<dyn Future<Output = ()> + '_>> {
        Box::pin(tokio::time::sleep(DEADLINE))
    }
}

mod bus {
    use super::Run;
    angzarr_bus::bus_contract!(#[tokio::test], Run::default());
}

mod transport {
    use super::Run;
    angzarr_bus::transport_contract!(#[tokio::test], Run::default());
}

/// The same suites with the store transport told which streams a commit
/// touched, not listing and reading every stream.
mod bus_incremental {
    use super::Run;
    angzarr_bus::bus_contract!(#[tokio::test], Run::incremental());
}

mod transport_incremental {
    use super::Run;
    angzarr_bus::transport_contract!(#[tokio::test], Run::incremental());
}

#[path = "contract/flawed.rs"]
mod flawed;

#[path = "contract/hub.rs"]
mod hub;
