//! The application: an [`App`] built from its parts ([`App::builder`]: a
//! registry, where its streams are kept ([`Stores`]), and the kernel's
//! settings), or [`run`] from its config, what the CLI's generated
//! `main.rs` calls.
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> Result<(), angzarr_embedded_host::HostError> {
//!     let app = std::sync::Arc::new(angzarr_embedded_host::run("angzarr.yaml", register).await?);
//!     // app.handle(book, &sampled).await from any task, concurrently;
//!     // app.events(&cover).await, app.kernel(), ...
//!     app.shutdown().await; // or drop it: the children stop
//!     Ok(())
//! }
//!
//! // From its parts: a store per project, a log of the dead letters' lines.
//! let app = App::builder(projects, registry)   // projects: impl Stores<Key = Project>
//!     .dead_letter_log(log)
//!     .on_retryable(retry)
//!     .start()
//!     .await;                                   // each store at_start names, resumed
//! app.handle_in(&project, book, &sampled).await;
//! ```
//!
//! The [`App`] is `Send + Sync` and its methods are async, on the caller's
//! runtime: commands run concurrently, and nothing orders streams against
//! each other (ordering is per stream only). Two commands to one stream
//! race optimistically: both decide, the store takes one write, and the
//! loser is decided again on the new head, unless it set an expected
//! sequence, which makes the loss its business failure. [`run`]'s store is
//! ONE SQLite connection, which serializes the I/O and nothing else.
//!
//! Dead letters are durable (owner ruling Q5, 2026-10-31): a store that
//! keeps them (an angzarr-ports `DeadLetterStore`, as SQLite's is) holds
//! every letter a step with no caller left wrote, in that step's write;
//! every call answers the letters it wrote; and a log the application
//! supplies ([`AppBuilder::dead_letter_log`]) also gets each as its line.
//! The host writes to no terminal: what it would say goes back to its
//! caller ([`App::resumed`], [`Unserved`]).
//!
//! The host's children are started and stopped off the caller's workers
//! (`tokio::task::spawn_blocking`): the App has no runtime of its own, and
//! the caller's runtime is tokio's, with its timer.

// The kernel's own Failure, as Kernel::handle_any returns it, is what
// every call of the App answers.
#![allow(clippy::result_large_err)]

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use angzarr_embedded::v1::{CommandBook, Cover, EventBook, EventPage};
use angzarr_embedded::{Answered, Catalog, Misconfigured};
use angzarr_embedded::{
    DeadLetterLog, EventStore, Failure, Handled, Kernel, KernelError, Malformed, OnPolicyBug,
    OnRetryable, Recorded, Registry, Rejected, Sampled, SnapshotPolicy, StoreError, StreamRef,
    Wait,
};
use angzarr_proto::v1::ComponentKind;
use angzarr_store_sqlite::SqliteStore;
use coded_diagnostic::Report;

use crate::config::{on_policy_bug, on_retryable, snapshot_policy, Expose, HostConfig, Languages};
use crate::error::HostError;
use crate::exposure::{Arrival, NotExposed, Unserved};
use crate::host::{ChildExit, Host};
use crate::stores::{OneStore, Stores};

/// Runs the application the config at `config` describes: reads it
/// (`angzarr.yaml`, strictly: [`crate::config::parse`]), starts its remote
/// components ([`Host::start`]), registers them by kind and calls
/// `register` to register the in-process ones (a registration it refuses
/// is [`HostError::Register`]), holding what it registers
/// to the in-process `component` entries, by name and kind (one listed and
/// not registered, [`HostError::NotRegistered`]; one registered and not
/// listed, [`HostError::NotListed`]), opens
/// its store (SQLite, `store`) on the caller's runtime, and starts the
/// [`App`] on it ([`AppBuilder::start`]: resuming the policy work the store
/// still owes) before it returns. The [`App`] is
/// how the application sends commands and reads its streams; dropping it
/// stops the children. Its kernel's settings are the config's: `policy`,
/// `retry` and `store.snapshots`. A component exposed (`expose`) that is registered
/// as a process manager is refused ([`HostError::NotExposable`]): only an
/// aggregate is served through the host's listener. On any refusal the
/// children started are stopped again. Its components' languages are the
/// built-in ones ([`Languages::builtin`]); [`run_with_languages`] loads
/// more, and [`run_with`] runs a config already in hand.
pub async fn run(
    config: impl AsRef<Path>,
    register: impl FnOnce(&mut Registry) -> Result<(), Misconfigured>,
) -> Result<App, HostError> {
    run_with_languages(config, Languages::builtin(), register).await
}

/// [`run`], its components' `language` one of `languages`: the built-in
/// ones and those the application loads before it starts
/// (`Languages::builtin().with(Language::child("go"))`). A component in a
/// language not among them is refused before anything starts
/// ([`HostError::UnknownLanguage`], its key path and the languages
/// loaded).
pub async fn run_with_languages(
    config: impl AsRef<Path>,
    languages: Languages,
    register: impl FnOnce(&mut Registry) -> Result<(), Misconfigured>,
) -> Result<App, HostError> {
    let path = config.as_ref().to_path_buf();
    let config = blocking(move || crate::config::read(&path)).await?;
    run_with(config, languages, register).await
}

/// [`run_with_languages`] on `config`, a config already in hand rather
/// than a file: built in code, or read by the caller
/// ([`crate::config::parse`]).
pub async fn run_with(
    config: HostConfig,
    languages: Languages,
    register: impl FnOnce(&mut Registry) -> Result<(), Misconfigured>,
) -> Result<App, HostError> {
    let (config, host, languages) = blocking(move || {
        let host = Host::start_with_languages(config.clone(), &languages)?;
        Ok((config, host, languages))
    })
    .await?;
    let mut registry = Registry::new();
    let refused = |e: Misconfigured| HostError::Register {
        cause: Report::of(&e),
    };
    if let Err(error) = host.register(&mut registry).map_err(refused) {
        return Err(stopped(host, error).await);
    }
    let hosted = Registered::of(registry.catalog());
    if let Err(error) = register(&mut registry).map_err(refused) {
        return Err(stopped(host, error).await);
    }
    let local = Registered::of(registry.catalog()).since(&hosted);
    if let Err(error) = listed(&config, &languages, &local, registry.catalog()) {
        return Err(stopped(host, error).await);
    }
    let exposed: BTreeMap<String, Expose> = config
        .components
        .iter()
        .filter(|c| c.expose != Expose::None)
        .map(|c| (c.name.clone(), c.expose))
        .collect();
    if let Some(name) = exposed
        .keys()
        .find(|name| process_manager(registry.catalog(), name))
    {
        let error = HostError::NotExposable {
            domain: name.clone(),
            kind: PROCESS_MANAGER.to_string(),
        };
        return Err(stopped(host, error).await);
    }
    let opened = match config.store.in_memory {
        true => SqliteStore::open_in_memory().await,
        false => SqliteStore::open(&config.store.path).await,
    };
    let store = match opened {
        Ok(store) => store,
        Err(e) => {
            let error = HostError::Store {
                cause: Report::of(&KernelError::Store(e)),
            };
            return Err(stopped(host, error).await);
        }
    };
    let app = App::builder(OneStore::new(store), registry)
        .host(host)
        .exposed(exposed)
        .on_policy_bug(on_policy_bug(config.policy.on_bug))
        .on_retryable(on_retryable(&config.retry))
        .snapshot_policy(snapshot_policy(&config.store.snapshots))
        .start()
        .await;
    Ok(app)
}

/// What a resume at start is sampled at: the wall clock, and a UUIDv7 of
/// it (RFC 9562, its random bits zero), which a resume never uses: its
/// steps' command ids are derived from their triggers.
fn started_at() -> Sampled {
    let since = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let ms = since.as_millis() as u64;
    let mut id = [0u8; 16];
    id[..6].copy_from_slice(&ms.to_be_bytes()[2..]);
    id[6] = 0x70;
    id[8] = 0x80;
    Sampled {
        now: angzarr_embedded::Timestamp {
            seconds: since.as_secs() as i64,
            nanos: since.subsec_nanos() as i32,
        },
        command_id: angzarr_embedded::v1::Uuid::from_bytes(id),
    }
}

/// The `kind` a process manager is refused exposure under: not a
/// `component` entry kind (a domain is an aggregate or a process manager),
/// its descriptor's kind.
const PROCESS_MANAGER: &str = "process_manager";

/// Whether the component `name` is registered here as a process manager:
/// a domain whose component name it is (decision 14.50), or a remote
/// domain the host registered under its `component` entry name.
fn process_manager(catalog: &Catalog, name: &str) -> bool {
    let is = |domain: &str| catalog.kind(domain) == ComponentKind::ProcessManager;
    is(name)
        || catalog
            .domains()
            .any(|domain| catalog.component_name(domain).as_deref() == Some(name) && is(domain))
}

/// What a registry holds by name: its domains (by component name,
/// decision 14.50), policies and projectors.
#[derive(Default)]
struct Registered {
    domains: BTreeSet<String>,
    policies: BTreeSet<String>,
    projectors: BTreeSet<String>,
}

impl Registered {
    /// What `catalog` lists now.
    fn of(catalog: &Catalog) -> Registered {
        let names = |names: &mut dyn Iterator<Item = &str>| names.map(str::to_string).collect();
        Registered {
            domains: catalog
                .domains()
                .filter_map(|domain| catalog.component_name(domain))
                .collect(),
            policies: names(&mut catalog.policy_names()),
            projectors: names(&mut catalog.projector_names()),
        }
    }

    /// What `self` holds that `before` did not.
    fn since(self, before: &Registered) -> Registered {
        let new = |now: BTreeSet<String>, then: &BTreeSet<String>| {
            now.into_iter()
                .filter(|name| !then.contains(name))
                .collect()
        };
        Registered {
            domains: new(self.domains, &before.domains),
            policies: new(self.policies, &before.policies),
            projectors: new(self.projectors, &before.projectors),
        }
    }
}

/// The config's in-process components held to what `register`
/// registered in process, `local` (the registry's domains, policies and
/// projectors the host did not register), each by name and kind, a
/// domain by its component name (an aggregate's domain, a process
/// manager's `pmg-` and its domain: decision 14.50): a listed
/// one registered of no such kind and name is [`HostError::NotRegistered`]
/// (an upcaster's `upcasts` no domain registered at all), in the config's
/// order; then one registered that no in-process `component` entry of its
/// kind lists is [`HostError::NotListed`], domains first, then policies,
/// then projectors.
fn listed(
    config: &HostConfig,
    languages: &Languages,
    local: &Registered,
    catalog: &Catalog,
) -> Result<(), HostError> {
    use crate::config::{ComponentKindConfig as Kind, Placement};
    let in_process: Vec<_> = config
        .components
        .iter()
        .filter(|c| {
            languages
                .get(&c.language)
                .is_some_and(|language| matches!(language.placement(c), Ok(Placement::InProcess)))
        })
        .collect();
    for component in &in_process {
        let registered = match component.kind {
            Kind::Domain => local.domains.contains(&component.name),
            Kind::Policy => local.policies.contains(&component.name),
            Kind::Projector => local.projectors.contains(&component.name),
            Kind::Upcaster => component
                .upcasts
                .as_deref()
                .is_some_and(|domain| catalog.domains().any(|d| d == domain)),
        };
        if !registered {
            return Err(HostError::NotRegistered {
                domain: component.name.clone(),
                kind: component.kind.as_str().to_string(),
            });
        }
    }
    let unlisted = |names: &BTreeSet<String>, kind: Kind| {
        names
            .iter()
            .find(|name| {
                !in_process
                    .iter()
                    .any(|c| c.kind == kind && &c.name == *name)
            })
            .map(|name| HostError::NotListed {
                domain: name.clone(),
                kind: kind.as_str().to_string(),
            })
    };
    match unlisted(&local.domains, Kind::Domain)
        .or_else(|| unlisted(&local.policies, Kind::Policy))
        .or_else(|| unlisted(&local.projectors, Kind::Projector))
    {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// `work`, which blocks (starting or stopping the host's children), run
/// off the caller's async workers. A panic in it is the caller's.
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    match tokio::task::spawn_blocking(work).await {
        Ok(done) => done,
        Err(error) => std::panic::resume_unwind(error.into_panic()),
    }
}

/// `work` awaited, each poll of it on a multi-threaded runtime's worker
/// made with the worker's other tasks handed on
/// (`tokio::task::block_in_place`). The kernel runs business code (a
/// domain's decide, a policy) synchronously as it is polled, and business
/// code may block; a task the worker held next (in its slot that no other
/// worker takes) would wait out the block, so commands for other streams,
/// which share nothing, would not run at once. On a current-thread
/// runtime, which has no other worker to hand on to, it is polled as is.
async fn off_worker<F: Future>(work: F) -> F::Output {
    let threaded = tokio::runtime::Handle::try_current()
        .is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread);
    let mut work = std::pin::pin!(work);
    std::future::poll_fn(|cx| match threaded {
        true => tokio::task::block_in_place(|| work.as_mut().poll(cx)),
        false => work.as_mut().poll(cx),
    })
    .await
}

/// `error`, once `host`'s children are stopped.
async fn stopped(host: Host, error: HostError) -> HostError {
    blocking(move || host.shutdown()).await;
    error
}

/// An [`App`] being built from its parts: its registry, its [`Stores`],
/// and the kernel's settings, each the kernel's default until set, as
/// [`Kernel`]'s are. [`AppBuilder::start`] starts it.
pub struct AppBuilder<R: Stores> {
    host: Host,
    registry: Registry,
    stores: R,
    log: Option<Arc<dyn DeadLetterLog>>,
    wait: Option<Arc<dyn Wait>>,
    policy_bug: OnPolicyBug,
    retry: OnRetryable,
    snapshots: SnapshotPolicy,
    exposed: BTreeMap<String, Expose>,
}

impl<R: Stores> AppBuilder<R> {
    /// The remote components' host: none until given ([`Host::none`]).
    pub fn host(mut self, host: Host) -> AppBuilder<R> {
        self.host = host;
        self
    }

    /// Where each dead letter's line goes, beside the store, which keeps
    /// the letters when it can: nowhere until given.
    pub fn dead_letter_log(mut self, log: Arc<dyn DeadLetterLog>) -> AppBuilder<R> {
        self.log = Some(log);
        self
    }

    /// How the kernel waits out a backoff and draws its jitter
    /// ([`Kernel::wait_with`]): the kernel's own until given.
    pub fn wait_with(mut self, wait: Arc<dyn Wait>) -> AppBuilder<R> {
        self.wait = Some(wait);
        self
    }

    /// What a policy step's bug does with the policy's position
    /// ([`Kernel::on_policy_bug`]).
    pub fn on_policy_bug(mut self, policy_bug: OnPolicyBug) -> AppBuilder<R> {
        self.policy_bug = policy_bug;
        self
    }

    /// How a step's retryable failure is retried ([`Kernel::on_retryable`]).
    pub fn on_retryable(mut self, retry: OnRetryable) -> AppBuilder<R> {
        self.retry = retry;
        self
    }

    /// Which of the snapshots each domain's state offers are stored, per
    /// domain ([`Kernel::snapshot_policy`]): one at every write until given.
    pub fn snapshot_policy(mut self, snapshots: SnapshotPolicy) -> AppBuilder<R> {
        self.snapshots = snapshots;
        self
    }

    /// The domains served to the host's listener, by `component` name,
    /// each as exposed ([`App::serve_in`]); one not here is exposed
    /// `"none"`.
    pub fn exposed(mut self, exposed: BTreeMap<String, Expose>) -> AppBuilder<R> {
        self.exposed = exposed;
        self
    }

    /// The application, started: the policy work each store
    /// [`Stores::at_start`] names still owes is resumed
    /// ([`Kernel::resume`]: what a crash left between a commit and its
    /// policies' steps) at [`started_at`]. A resume that fails does not
    /// stop the start: the work stays the store's to owe, its step
    /// dead-lettered and its position where `on_policy_bug` leaves it, and
    /// the failure is the application's to read ([`App::resumed`]).
    pub async fn start(self) -> App<R> {
        let mut app = App {
            host: self.host,
            registry: self.registry,
            log: self.log,
            wait: self.wait,
            policy_bug: self.policy_bug,
            retry: self.retry,
            snapshots: self.snapshots,
            stores: self.stores,
            exposed: self.exposed,
            resumed: Vec::new(),
        };
        let sampled = started_at();
        let mut failed = Vec::new();
        for key in app.stores.at_start() {
            if let Err(failure) = app.resume_in(&key, &sampled).await {
                failed.push((key, failure));
            }
        }
        app.resumed = failed;
        app
    }
}

/// A running application: its registry, its stores, its kernel's settings
/// and its remote components. `Send + Sync`: shared (an `Arc`) by every
/// task that sends it commands, and by the host's listener. `App` alone
/// is [`run`]'s: one SQLite store.
pub struct App<R: Stores = OneStore<SqliteStore>> {
    // Dropped in this order: the children stopped first.
    host: Host,
    registry: Registry,
    log: Option<Arc<dyn DeadLetterLog>>,
    wait: Option<Arc<dyn Wait>>,
    policy_bug: OnPolicyBug,
    retry: OnRetryable,
    snapshots: SnapshotPolicy,
    stores: R,
    /// Each `component` entry exposed to the host's listener, by name: one
    /// not here is exposed `"none"`.
    exposed: BTreeMap<String, Expose>,
    /// The resumes at start that failed, by store.
    resumed: Vec<(R::Key, Failure<Infallible>)>,
}

impl<R: Stores> App<R> {
    /// An application from its parts: `registry` and `stores`, the kernel's
    /// settings its defaults, no remote components, no
    /// dead-letter log (the store keeps the letters), until the builder is
    /// told otherwise.
    pub fn builder(stores: R, registry: Registry) -> AppBuilder<R> {
        AppBuilder {
            host: Host::none(),
            registry,
            stores,
            log: None,
            wait: None,
            policy_bug: OnPolicyBug::default(),
            retry: OnRetryable::default(),
            snapshots: SnapshotPolicy::default(),
            exposed: BTreeMap::new(),
        }
    }

    /// The kernel over this application's registry and dead-letter log,
    /// with its settings.
    pub fn kernel(&self) -> Kernel<'_> {
        let kernel = Kernel::new(&self.registry)
            .on_policy_bug(self.policy_bug)
            .on_retryable(self.retry)
            .snapshot_policy(&self.snapshots);
        let kernel = match &self.log {
            Some(log) => kernel.log_with(&**log),
            None => kernel,
        };
        match &self.wait {
            Some(wait) => kernel.wait_with(&**wait),
            None => kernel,
        }
    }

    /// The store of `key`, as [`Stores::store`] opens it; refused, the
    /// kernel's store failure.
    async fn store_of(&self, key: &R::Key) -> Result<R::Store, KernelError> {
        self.stores.store(key).await.map_err(KernelError::Store)
    }

    /// Runs the policy work the store of `key` still owes, by position
    /// ([`Kernel::resume`]), at `sampled`: what the application runs for
    /// a store it opens after start. What the steps recorded, and the dead
    /// letters they wrote.
    pub async fn resume_in(
        &self,
        key: &R::Key,
        sampled: &Sampled,
    ) -> Result<Recorded, Failure<Infallible>> {
        let store = self.store_of(key).await.map_err(Failure::Kernel)?;
        let kernel = self.kernel();
        off_worker(kernel.resume(&store, sampled)).await
    }

    /// The resumes at start that failed, by the key of their store
    /// ([`AppBuilder::start`]).
    pub fn resumed(&self) -> &[(R::Key, Failure<Infallible>)] {
        &self.resumed
    }

    /// Where the application's streams are kept.
    pub fn stores(&self) -> &R {
        &self.stores
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// The host of the remote components.
    pub fn host(&self) -> &Host {
        &self.host
    }

    /// Handles one command book in the store of `key`, for any domain,
    /// and every policy step it sets off ([`Kernel::handle_any`]), then the
    /// registry's projectors over what it recorded. Calls run concurrently; two on one stream race
    /// optimistically, as the kernel's do.
    pub async fn handle_in(
        &self,
        key: &R::Key,
        book: CommandBook,
        sampled: &Sampled,
    ) -> Result<Handled, Failure<Rejected>> {
        let store = self.store_of(key).await.map_err(Failure::Kernel)?;
        let kernel = self.kernel();
        off_worker(kernel.handle_any(&store, book, sampled)).await
    }

    /// Every event of the stream `cover` names in the store of `key`, in
    /// sequence order.
    pub async fn events_in(
        &self,
        key: &R::Key,
        cover: &Cover,
    ) -> Result<Vec<EventPage>, StoreError> {
        let stream = StreamRef::try_from(cover)?;
        let store = self.stores.store(key).await?;
        Ok(store.read(stream, 0, None).await?.pages)
    }

    /// The exposure seam ([`crate::exposure`]): one request to the host's
    /// listener, which arrived from `arrived`, for the store of `key`.
    /// Handled as [`App::handle_in`] handles a book when its domain's
    /// `component` entry is exposed to the arrival, and answered as
    /// `CommandHandlerCoordinatorService.Handle` answers, in core's messages
    /// ([`Answered`]; its listener builds the distributed transport's
    /// `CommandResponse` from it): the addressed stream's events and the
    /// delivery outcome. A domain not exposed to
    /// the arrival is refused as an unknown domain is
    /// (`Malformed::UnknownDomain`), the real reason beside it for the
    /// listener's own log ([`Unserved::Hidden`]).
    pub async fn serve_in(
        &self,
        key: &R::Key,
        book: CommandBook,
        arrived: Arrival,
        sampled: &Sampled,
    ) -> Result<Answered, Unserved> {
        let cover = book.cover.clone();
        if let Some(cover) = &cover {
            let expose = self.exposed.get(&cover.domain).copied().unwrap_or_default();
            if !arrived.admitted_by(expose) {
                return Err(Unserved::Hidden {
                    answer: Failure::Kernel(KernelError::Malformed(Malformed::UnknownDomain {
                        domain: cover.domain.clone(),
                    })),
                    hidden: NotExposed {
                        domain: cover.domain.clone(),
                        arrival: arrived,
                        expose,
                    },
                });
            }
        }
        let handled = self
            .handle_in(key, book, sampled)
            .await
            .map_err(Unserved::Failed)?;
        let events = handled
            .books()
            .into_iter()
            .find(|book| book.cover == cover)
            .unwrap_or(EventBook {
                cover,
                ..EventBook::default()
            });
        Ok(Answered {
            events: Some(events),
            delivery: handled.delivery(),
        })
    }

    /// Stops the remote components' children ([`Host::shutdown`], off the
    /// caller's workers) and closes the stores.
    pub async fn shutdown(self) -> Vec<(String, ChildExit)> {
        let App { host, stores, .. } = self;
        let exits = blocking(move || host.shutdown()).await;
        drop(stores);
        exits
    }
}

/// An application of one store: every call is the store's, by key `()`.
impl<R: Stores<Key = ()>> App<R> {
    /// [`App::handle_in`] the one store.
    pub async fn handle(
        &self,
        book: CommandBook,
        sampled: &Sampled,
    ) -> Result<Handled, Failure<Rejected>> {
        self.handle_in(&(), book, sampled).await
    }

    /// [`App::events_in`] the one store.
    pub async fn events(&self, cover: &Cover) -> Result<Vec<EventPage>, StoreError> {
        self.events_in(&(), cover).await
    }

    /// [`App::serve_in`] the one store.
    pub async fn serve(
        &self,
        book: CommandBook,
        arrived: Arrival,
        sampled: &Sampled,
    ) -> Result<Answered, Unserved> {
        self.serve_in(&(), book, arrived, sampled).await
    }
}

impl<S> App<OneStore<S>>
where
    S: angzarr_embedded::Store
        + angzarr_embedded::Processed
        + angzarr_embedded::ListStreams
        + Send
        + Sync,
{
    /// The application's one store.
    pub fn store(&self) -> &S {
        self.stores.get()
    }
}

#[cfg(test)]
#[path = "app.test.rs"]
mod tests;
