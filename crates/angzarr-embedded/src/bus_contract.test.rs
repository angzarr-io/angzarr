//! angzarr core's bus conformance suite (`angzarr_bus::contract`), run
//! against the bus hosted on a kernel over the in-memory store: projector
//! subscriptions of the suite's sink, and policy subscriptions of its
//! policy, whose outputs are ledger records the kernel routes.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use angzarr_bus::contract::{Delivery, Domains, Failing, Harness, React, Scripted, Sink, Wake};
use angzarr_bus::{Pumped, RetryPolicy};
use angzarr_ports::faulty::{Fault, FaultyStore};
use angzarr_router::error::{CodedError, RetryableStatus};
use angzarr_router::hpb::{self, ProjectRequest, ProjectResponse};
use angzarr_router::router::{DynProjectorHandler, PolicyComponent, ProjectorHandler};
use coded_diagnostic::Diagnostic as _;

use crate::toys::prelude::*;
use crate::toys::{at, command, instant, ledger, no_retries, Account, AccountCmd, Edit, Shelf};
use crate::v1::{Cover, EventPage, EventRef};
use crate::{
    pack, Bus, BusPolicy, Host, Kernel, MemoryDeadLetters, Payload as _, PositionKey, Processed,
    Registry, Timestamp, Wait,
};

/// The suite's sink, as the router's projector port.
struct Sinking {
    name: String,
    sink: Arc<dyn Sink>,
}

impl ProjectorHandler for Sinking {
    fn name(&self) -> &str {
        &self.name
    }

    async fn dispatch(&self, request: &ProjectRequest) -> Result<ProjectResponse, CodedError> {
        let book = request.events.clone().unwrap_or_default();
        let stream = book.cover.clone().unwrap_or_default();
        let Some(page) = book.pages.first() else {
            return Ok(ProjectResponse::default());
        };
        let delivery = Delivery::Event {
            stream,
            sequence: page.sequence,
        };
        match self.sink.handle(delivery).await {
            Ok(()) => Ok(ProjectResponse::default()),
            Err(Failing::Bug(report)) => Err(CodedError::bug(report.code(), [])),
            Err(Failing::Retryable(report)) => Err(CodedError::retryable_bug(
                RetryableStatus::Unavailable,
                report.code(),
                [],
            )),
        }
    }
}

/// The suite's policy, as the router's policy component: each root it
/// answers a ledger record there.
struct Reacting {
    name: String,
    source: String,
    react: Arc<dyn React>,
}

impl PolicyComponent for Reacting {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_domain(&self) -> &str {
        &self.source
    }

    async fn react(&self, request: &hpb::ReactRequest) -> Result<hpb::ReactResponse, CodedError> {
        let book = request.trigger.clone().unwrap_or_default();
        let trigger = EventRef {
            stream: book.cover.clone(),
            sequence: book.pages.first().map_or(0, |page| page.sequence),
        };
        let roots = (self.react.react(&trigger).await)
            .map_err(|report| CodedError::bug(report.code(), []))?;
        let record = pack(&ledger::Record {
            source_sequence: trigger.sequence,
            cents: 0,
        });
        let outputs = roots.into_iter().map(|root| hpb::Output {
            root: Some(root),
            body: Some(hpb::output::Body::Command(record.clone())),
        });
        Ok(hpb::ReactResponse {
            outputs: outputs.collect(),
        })
    }

    async fn translate(
        &self,
        _: &hpb::TranslateRequest,
    ) -> Result<hpb::TranslateResponse, CodedError> {
        Err(CodedError::bug("toy-relay-translates-nothing", []))
    }
}

/// The poll the suite's buses wake on: at once, as soon as the runtime has
/// run everything else.
struct Soon;

impl Wait for Soon {
    fn wait(&self, _: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::task::yield_now())
    }

    fn draw(&self) -> f64 {
        0.0
    }
}

/// A poll that never fires.
struct Never;

impl Wait for Never {
    fn wait(&self, _: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(std::future::pending())
    }

    fn draw(&self) -> f64 {
        0.0
    }
}

/// The bus's retry cap in this run: two retries, at once.
const CAP: u32 = 2;

fn retry() -> RetryPolicy {
    RetryPolicy {
        cap: Some(CAP),
        ..RetryPolicy::default()
    }
}

fn clock() -> Timestamp {
    instant(100)
}

/// The kernel over the toy domains, its retries off (a failed commit is
/// handed back at once) and its dead letters logged, the store faulty on
/// demand: the bus's commits go through the faults, the suite's appends
/// and reads straight to the store.
struct Embedded {
    store: FaultyStore<MemoryStore>,
    kernel: Kernel<'static>,
    letters: &'static MemoryDeadLetters,
    commands: AtomicU64,
    incremental: bool,
}

impl Embedded {
    fn new() -> Embedded {
        let registry: &'static Registry = Box::leak(Box::new(crate::toys::registry()));
        let letters: &'static MemoryDeadLetters = Box::leak(Box::new(MemoryDeadLetters::new()));
        let kernel = Kernel::new(registry)
            .log_with(letters)
            .on_retryable(no_retries());
        Embedded {
            store: FaultyStore::new(MemoryStore::new()),
            kernel,
            letters,
            commands: AtomicU64::new(1),
            incremental: false,
        }
    }

    /// The same, its bus told which streams a commit touched.
    fn incremental() -> Embedded {
        Embedded {
            incremental: true,
            ..Embedded::new()
        }
    }

    fn memory(&self) -> &MemoryStore {
        self.store.inner()
    }
}

impl Harness for Embedded {
    type Projector = Box<dyn DynProjectorHandler>;
    type Policy = BusPolicy;
    type Bus<'h> = crate::Bus<'h, FaultyStore<MemoryStore>>;
    type Scripted<'h> = crate::Bus<'h, FaultyStore<MemoryStore>, Pumped<Scripted>>;

    fn domains(&self) -> Domains {
        Domains {
            first: "account".to_string(),
            second: "shelf".to_string(),
            target: "ledger".to_string(),
        }
    }

    fn bus(&self, wake: Wake) -> Self::Bus<'_> {
        let wait: &dyn Wait = match wake {
            Wake::Poll => &Soon,
            Wake::CommitOnly => &Never,
        };
        let following = match self.incremental {
            true => angzarr_bus::Following::Changes(self.memory()),
            false => angzarr_bus::Following::FullScan,
        };
        let host = Host::new(
            &self.store,
            &self.kernel,
            &clock,
            wait,
            Duration::from_secs(1),
            following,
        );
        Bus::new(host.pushed_by(self.memory()).retrying(retry()))
    }

    fn bus_over(&self, transport: Scripted) -> Self::Scripted<'_> {
        let host = Host::new(
            &self.store,
            &self.kernel,
            &clock,
            &Never,
            Duration::from_secs(1),
            angzarr_bus::Following::FullScan,
        );
        Bus::over(Pumped::new(transport), host.retrying(retry()))
    }

    fn projector(&self, name: &str, sink: Arc<dyn Sink>) -> Self::Projector {
        Box::new(Sinking {
            name: name.to_string(),
            sink,
        })
    }

    fn policy(&self, name: &str, source: &str, react: Arc<dyn React>) -> Self::Policy {
        let policy = Reacting {
            name: name.to_string(),
            source: source.to_string(),
            react,
        };
        BusPolicy::component(policy, "ledger")
    }

    async fn append(&self, stream: &Cover) {
        let id = u128::from(self.commands.fetch_add(1, Ordering::Relaxed));
        // A new or purged stream (its last event a tombstone) is opened.
        let stored = pages(self.memory(), stream);
        let fresh = stored.last().is_none_or(angzarr_ports::tombstone::is);
        let sampled = at(10 + id as i64);
        let handled = match (stream.domain.as_str(), fresh) {
            ("account", true) => {
                let book = command(stream.clone(), AccountCmd::Open.to_any()).id(id);
                self.kernel
                    .handle::<Account, _>(self.memory(), book, &sampled)
                    .await
            }
            ("account", _) => {
                let book = command(stream.clone(), AccountCmd::Deposit(1).to_any()).id(id);
                self.kernel
                    .handle::<Account, _>(self.memory(), book, &sampled)
                    .await
            }
            _ => {
                let book = command(stream.clone(), crate::toys::set("tea", id as i64)).id(id);
                self.kernel
                    .handle::<Shelf, _>(self.memory(), book, &sampled)
                    .await
            }
        };
        assert!(handled.is_ok(), "the suite's append is handled");
    }

    #[cfg(feature = "purge")]
    async fn purge(&self, stream: &Cover) {
        let head = self
            .memory()
            .read(sref(stream), 0, None)
            .await
            .unwrap()
            .next_sequence;
        self.memory().purge(sref(stream), head).await.unwrap();
    }

    async fn landed(&self, stream: &Cover) -> Vec<EventPage> {
        pages(self.memory(), stream)
    }

    async fn position_rows(&self, name: &str) -> usize {
        self.memory().processed(name).ready().unwrap().len()
    }

    async fn position(&self, name: &str, stream: &Cover) -> u64 {
        position(self.memory(), name, stream)
    }

    async fn rewind(&self, name: &str, stream: &Cover, to: u64) {
        let positions = [(
            PositionKey {
                handler: name,
                stream: sref(stream),
            },
            to,
        )];
        self.memory()
            .write(Write::positions(&positions))
            .await
            .unwrap();
    }

    fn retry_cap(&self) -> u32 {
        CAP
    }

    fn fail_position_commit(&self, _: usize) {
        self.store.fail([Fault::Unavailable]);
    }

    async fn dead_letters(&self) -> usize {
        self.letters.letters().len()
    }

    fn deadline(&self) -> Pin<Box<dyn Future<Output = ()> + '_>> {
        Box::pin(tokio::time::sleep(Duration::from_secs(20)))
    }
}

mod durable_scan {
    use super::Embedded;
    angzarr_bus::bus_contract!(#[tokio::test], Embedded::new());
    #[cfg(feature = "purge")]
    angzarr_bus::bus_contract!(purge, #[tokio::test], Embedded::new());
}

/// The suite again with the bus told which streams a commit touched.
mod incremental_scan {
    use super::Embedded;
    angzarr_bus::bus_contract!(#[tokio::test], Embedded::incremental());
    #[cfg(feature = "purge")]
    angzarr_bus::bus_contract!(purge, #[tokio::test], Embedded::incremental());
}
