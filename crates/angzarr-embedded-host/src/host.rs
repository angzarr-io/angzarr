//! The host: launches every remote component of a [`HostConfig`], waits for
//! it to report SERVING on the socket the host chose, describes it, and
//! hands back a handler of its
//! kind (a [`RemoteCommandHandler`] per domain, a [`RemotePolicyHandler`]
//! per policy, a [`RemoteProjectorHandler`] per projector) to register with
//! the application's registry beside its in-process ones. Then it
//! supervises: a child that
//! exits is restarted, after a backoff that grows while restarts fail, and
//! one unhealthy past its grace is restarted at once, up to its limit;
//! while it is down its handler's dispatch is retryable
//! ([`crate::is_retryable`]). A restarted child is described again, and its
//! component registered by its new descriptor before it is published
//! (C-0563): a registered domain's whole descriptor is replaced, and a
//! child describing another component (another name or kind, or a policy
//! or projector of other inputs or another output domain) is not published.
//!
//! Every child's gRPC health (`grpc.health.v1`, its server's, service
//! `""`) is watched from when it is first SERVING on, as the child protocol has every
//! child serve it, and checked every `health_check_interval_ms` with a
//! `Check` given `health_check_deadline_ms` (the protocol's supervision,
//! decision 14.63; [`crate::vitals`]): a child is up while it is running
//! and its health, as last seen by either, is SERVING, and down (its calls
//! retryable, cause `not-serving`) while it is not, a check unanswered
//! within its deadline included; a watch that ends reports nothing more.
//! A child is ready once its health reports SERVING on the socket path the
//! host chose (`ANGZARR_SOCKET`), within the start limit, and only then is
//! it described (decision 14.80: there is no handshake line); an upcaster
//! sidecar, which is not described, is ready once it answers its health at
//! all, and may come up down. A child down for `unhealthy_grace_ms`, unbroken, is stopped
//! at the first check (or watch report) past it and launched again at
//! once, up to `unhealthy_restart_limit` times for its component, a count
//! that clears once its child has been healthy for `unhealthy_reset_ms`,
//! unbroken (decision 14.67); past the limit, it is stopped and none
//! launched, its calls retryable (cause `not-running`) until the host ends. A call in flight on a child that
//! is stopped fails retryably, and is never sent again.
//!
//! The generated `main.rs` calls [`crate::run`], which holds the
//! in-process registrations to the config's listing; by hand, in
//! outline:
//!
//! ```ignore
//! let config = config::parse(include_str!("angzarr.yaml"), "angzarr.yaml")?;
//! let host = Host::start(config)?;
//! let mut registry = Registry::new();
//! registry.aggregate::<table::Table>(Default::default)?; // in-process
//! host.register(&mut registry)?; // every remote component, by its kind
//! let kernel = Kernel::new(&registry);
//! // ... kernel.handle_any(&store, book, &sampled) for any domain;
//! // dropping `host` stops the children.
//! ```
//!
//! The host supervises its children on a tokio runtime of its own, on its
//! own threads, so starting and stopping it works the same under any
//! caller; an async caller does both off its workers
//! (`tokio::task::spawn_blocking`), as [`crate::run`] does. A remote
//! component's calls are not the host's: each is awaited on the caller's
//! runtime, which must be tokio's, with its timer.

use std::collections::BTreeSet;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use angzarr_embedded::{Misconfigured, Registry};
use angzarr_proto::v1 as pb;
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::oneshot;

use crate::config::{
    ComponentKindConfig, Expose, HostConfig, Languages, Launch, Placement, Supervision,
};
use crate::error::HostError;
use crate::remote::{
    causes, Link, RemoteCommandHandler, RemotePolicyHandler, RemoteProjectorHandler, RemoteUpcaster,
};
#[cfg(unix)]
use crate::rpc::connect_unix;
use crate::vitals::{check, Clock, Verdict, Vitals};
#[cfg(not(unix))]
use crate::without_unix::{connect_unix, spawn, terminate};
use angzarr_proto_component::v1::component_service_client::ComponentServiceClient;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

/// The environment variable the host sets to the socket path it chose, the
/// spec's (angzarr-proto-transport-embedded's `child_protocol`; decision
/// 14.64): the child serves gRPC there.
pub const SOCKET_ENV: &str = angzarr_proto_transport_embedded::child_protocol::SOCKET_ENV;

/// How a child ended at shutdown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChildExit {
    /// It exited by itself once its stdin was closed: its exit code, `None`
    /// when a signal ended it.
    Exited(Option<i32>),
    /// It outlived its stdin's grace, and exited once sent SIGTERM.
    Terminated,
    /// It outlived SIGTERM's grace too, and was killed (SIGKILL).
    Killed,
    /// No child was running (it had exited and was waiting to be
    /// restarted).
    NotRunning,
}

impl ChildExit {
    /// How it ended, as an arg: `exited` (with a status), `signalled`
    /// (exited by itself, a signal its end), `terminated`, `killed` or
    /// `not-running`.
    pub fn ended(self) -> &'static str {
        match self {
            ChildExit::Exited(Some(_)) => "exited",
            ChildExit::Exited(None) => "signalled",
            ChildExit::Terminated => "terminated",
            ChildExit::Killed => "killed",
            ChildExit::NotRunning => "not-running",
        }
    }
}

/// The running host: its children and their handlers. Dropping it shuts
/// them down as [`Host::shutdown`] does.
pub struct Host {
    runtime: Option<tokio::runtime::Runtime>,
    domains: Vec<Supervised>,
}

/// A component the host launches: its `component` entry's name and kind,
/// its launch command, and the domain it upcasts (an upcaster's; empty
/// for any other kind).
struct Remote {
    domain: String,
    kind: ComponentKindConfig,
    launch: Launch,
    upcasts: String,
}

/// The components of `config` the host launches, in its order, each
/// held to the config's rules first: its languages known
/// ([`Languages::check`]), names unique, only a domain exposed, an upcaster
/// naming its domain, and each placed by its language.
fn remote_components(config: &HostConfig, languages: &Languages) -> Result<Vec<Remote>, HostError> {
    languages.check(config)?;
    let mut names = BTreeSet::new();
    let mut remote = Vec::new();
    for (index, component) in config.components.iter().enumerate() {
        if !names.insert(component.name.as_str()) {
            return Err(HostError::DuplicateDomain {
                domain: component.name.clone(),
            });
        }
        if component.expose != Expose::None && component.kind != ComponentKindConfig::Domain {
            return Err(HostError::NotExposable {
                domain: component.name.clone(),
                kind: component.kind.as_str().to_string(),
            });
        }
        let upcasts = component.upcasts.clone().unwrap_or_default();
        if component.kind == ComponentKindConfig::Upcaster && upcasts.is_empty() {
            return Err(HostError::UpcasterNoDomain {
                domain: component.name.clone(),
            });
        }
        let language = languages.of(config, index)?;
        if let Placement::Remote(launch) = language.placement(component)? {
            remote.push(Remote {
                domain: component.name.clone(),
                kind: component.kind,
                launch,
                upcasts,
            });
        }
    }
    Ok(remote)
}

/// What a remote component is handed back as, by its kind.
#[derive(Clone)]
enum Hosted {
    Domain(RemoteCommandHandler),
    Policy(RemotePolicyHandler),
    Projector(RemoteProjectorHandler),
    Upcaster(RemoteUpcaster),
}

/// One remote component under supervision.
struct Supervised {
    /// Its `component` entry name.
    name: String,
    hosted: Hosted,
    /// Tells the supervisor to stop the child; taken when sent.
    stop: Option<oneshot::Sender<()>>,
    /// How the child ended, once the supervisor has stopped it. In a
    /// `Mutex` only so the host is `Sync`: it is read once, at shutdown.
    ended: Mutex<mpsc::Receiver<ChildExit>>,
}

impl Host {
    /// Starts every remote component of `config` and waits for each to come
    /// up: launched, its gRPC health watch reporting SERVING on the socket
    /// the host chose within the start limit ([`HostError::NotServing`],
    /// whose `status` is the last it reported, or `none`, a child serving
    /// no health or exiting first included), and described, its descriptor of the kind
    /// its `kind` says ([`HostError::KindMismatch`]; one that fails
    /// validation, a policy or projector naming no input domain among
    /// them, [`HostError::Descriptor`]; one that does not answer within
    /// the call timeout, [`HostError::Describe`]). An upcaster sidecar is
    /// not described, and may come up not SERVING:
    /// it is handed back down, its upcasts retryable, until it reports
    /// SERVING. One naming no domain to upcast is refused before
    /// any child starts ([`HostError::UpcasterNoDomain`]), as is a
    /// component other than a domain given an `expose`
    /// ([`HostError::NotExposable`]). In-process domains are left to the
    /// caller. Refused, with every child already started stopped again,
    /// when any domain's config is refused or any child fails to come up
    /// the first time. It blocks until then: an async caller calls it off
    /// its workers.
    pub fn start(config: HostConfig) -> Result<Host, HostError> {
        Host::start_with_languages(config, &Languages::builtin())
    }

    /// A host of no remote components: an application whose components are
    /// all in process.
    pub fn none() -> Host {
        Host {
            runtime: None,
            domains: Vec::new(),
        }
    }

    /// [`Host::start`] knowing `languages`: the built-in ones and those
    /// the application loads. A component in a language not among them is
    /// refused before any child starts ([`HostError::UnknownLanguage`]).
    pub fn start_with_languages(
        config: HostConfig,
        languages: &Languages,
    ) -> Result<Host, HostError> {
        Host::started(config, languages, Hooks::default())
    }

    /// [`Host::start`], its supervisors held at `hooks`' points.
    #[cfg(test)]
    fn start_with(config: HostConfig, hooks: Hooks) -> Result<Host, HostError> {
        Host::started(config, &Languages::builtin(), hooks)
    }

    /// [`Host::start_with_languages`], its supervisors held at `hooks`'
    /// points.
    fn started(config: HostConfig, languages: &Languages, hooks: Hooks) -> Result<Host, HostError> {
        let remote = remote_components(&config, languages)?;
        let socket_dir = socket_dir(
            config.supervision.socket_dir.as_deref(),
            std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from),
        )?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("angzarr-host")
            .enable_all()
            .build()
            .map_err(|e| HostError::Runtime {
                kind: format!("{:?}", e.kind()),
            })?;
        let mut host = Host {
            runtime: None,
            domains: Vec::new(),
        };
        for Remote {
            domain,
            kind,
            launch,
            upcasts,
        } in remote
        {
            let link = Arc::new(Link::default());
            let (stop, stopped) = oneshot::channel();
            let (up, came_up) = mpsc::channel();
            let (end, ended) = mpsc::channel();
            let supervisor = Supervisor {
                domain: domain.clone(),
                kind,
                launch,
                supervision: config.supervision.clone(),
                socket_dir: socket_dir.clone(),
                link: link.clone(),
                hooks: hooks.clone(),
            };
            runtime.spawn(async move {
                let _ = end.send(supervisor.run(stopped, up).await);
            });
            let error = match came_up.recv() {
                Ok(Ok(descriptor)) => {
                    let parts = (
                        domain.clone(),
                        descriptor,
                        link,
                        Duration::from_millis(config.supervision.call_timeout_ms),
                    );
                    let hosted = match kind {
                        ComponentKindConfig::Policy => Hosted::Policy(RemotePolicyHandler::new(
                            parts.0, parts.1, parts.2, parts.3,
                        )),
                        ComponentKindConfig::Projector => Hosted::Projector(
                            RemoteProjectorHandler::new(parts.0, parts.1, parts.2, parts.3),
                        ),
                        ComponentKindConfig::Upcaster => Hosted::Upcaster(RemoteUpcaster::new(
                            parts.0, upcasts, parts.2, parts.3,
                        )),
                        _ => Hosted::Domain(RemoteCommandHandler::new(
                            parts.0, parts.1, parts.2, parts.3,
                        )),
                    };
                    host.domains.push(Supervised {
                        name: domain,
                        hosted,
                        stop: Some(stop),
                        ended: Mutex::new(ended),
                    });
                    continue;
                }
                Ok(Err(error)) => error,
                // The supervisor ended without a word: only the runtime
                // going away does that.
                Err(_) => HostError::Runtime {
                    kind: "Interrupted".to_string(),
                },
            };
            // Its supervisor has stopped its child already; stop the rest.
            let _ = ended.recv();
            host.runtime = Some(runtime);
            host.stop_all();
            return Err(error);
        }
        host.runtime = Some(runtime);
        Ok(host)
    }

    /// One handler per remote domain, in the config's order: not its
    /// policies or projectors.
    pub fn handlers(&self) -> Vec<RemoteCommandHandler> {
        self.domains
            .iter()
            .filter_map(|d| match &d.hosted {
                Hosted::Domain(handler) => Some(handler.clone()),
                _ => None,
            })
            .collect()
    }

    /// The handler of the remote domain named `domain`; `None` for a
    /// policy or a projector.
    pub fn handler(&self, domain: &str) -> Option<RemoteCommandHandler> {
        self.hosted(domain).and_then(|hosted| match hosted {
            Hosted::Domain(handler) => Some(handler.clone()),
            _ => None,
        })
    }

    /// The handler of the remote policy configured as `name` (a
    /// `component` entry of `kind: policy`).
    pub fn policy(&self, name: &str) -> Option<RemotePolicyHandler> {
        self.hosted(name).and_then(|hosted| match hosted {
            Hosted::Policy(handler) => Some(handler.clone()),
            _ => None,
        })
    }

    /// The handler of the remote projector configured as `name` (a
    /// `component` entry of `kind: projector`).
    pub fn projector(&self, name: &str) -> Option<RemoteProjectorHandler> {
        self.hosted(name).and_then(|hosted| match hosted {
            Hosted::Projector(handler) => Some(handler.clone()),
            _ => None,
        })
    }

    /// The remote upcaster sidecar configured as `name` (a `component` entry
    /// of `kind = "upcaster"`).
    pub fn upcaster(&self, name: &str) -> Option<RemoteUpcaster> {
        self.hosted(name).and_then(|hosted| match hosted {
            Hosted::Upcaster(upcaster) => Some(upcaster.clone()),
            _ => None,
        })
    }

    fn hosted(&self, name: &str) -> Option<&Hosted> {
        self.domains
            .iter()
            .find(|d| d.name == name)
            .map(|d| &d.hosted)
    }

    /// Registers every remote component into `registry`: a domain's
    /// handler as the domain it is configured for (the kernel then
    /// dispatches that domain's commands, and its policies' steps into it,
    /// through the child: `Kernel::handle_any`); a policy beside every
    /// other policy of its source; a projector of its sources; and, last, an
    /// upcaster sidecar as the port its domain's inputs are upcast through
    /// (decision 14.39), which domain the caller registers first if it is
    /// in-process. In-process components are registered by the caller as
    /// ever. Refused as the registry refuses a component: a domain
    /// registered already, a descriptor with no state or rules version, a
    /// policy or projector of a name registered already, an upcaster of a
    /// domain not registered.
    pub fn register(&self, registry: &mut Registry) -> Result<(), Misconfigured> {
        for component in &self.domains {
            match &component.hosted {
                Hosted::Domain(handler) => {
                    // Under the lock the supervisor describes a restarted
                    // child under: registered by the current child's
                    // descriptor, and every later one's replaces it.
                    let mut registered = handler.link().registered();
                    let description = registry.described_handler(
                        handler.domain(),
                        handler.clone(),
                        registered.descriptor.clone(),
                    )?;
                    registered.description = Some(description);
                }
                Hosted::Policy(handler) => {
                    let output_domain = match handler.descriptor().kind {
                        Some(pb::component_descriptor::Kind::Policy(policy)) => {
                            policy.output_domain
                        }
                        _ => String::new(),
                    };
                    registry.policy_component(handler.clone(), &output_domain)?;
                }
                Hosted::Projector(handler) => {
                    registry.projector_handler(&handler.sources(), handler.clone())?;
                }
                Hosted::Upcaster(_) => {}
            }
        }
        // Each sidecar upcasts a domain registered by now: a remote one,
        // above, or an in-process one, by the caller before.
        for component in &self.domains {
            if let Hosted::Upcaster(upcaster) = &component.hosted {
                registry.upcast_with(upcaster.upcasts(), upcaster.clone())?;
            }
        }
        Ok(())
    }

    /// Stops supervising and stops every child: closes its stdin, waits up
    /// to the shutdown grace for it to exit, and kills it if it has not.
    /// How each ended, by domain, in the config's order.
    pub fn shutdown(mut self) -> Vec<(String, ChildExit)> {
        self.stop_all()
    }

    /// Stops every child, all at once, and then the runtime.
    fn stop_all(&mut self) -> Vec<(String, ChildExit)> {
        for domain in &mut self.domains {
            if let Some(stop) = domain.stop.take() {
                let _ = stop.send(());
            }
        }
        let exits = self
            .domains
            .drain(..)
            .map(|d| {
                let ended = d.ended.into_inner().unwrap_or_else(|e| e.into_inner());
                let exit = ended.recv().unwrap_or(ChildExit::NotRunning);
                (d.name, exit)
            })
            .collect();
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
        exits
    }
}

impl Drop for Host {
    /// Stops the children, as [`Host::shutdown`] does, waiting for them.
    /// Dropped on a worker of a multi-threaded tokio runtime, it hands the
    /// worker's other tasks on while it waits
    /// (`tokio::task::block_in_place`).
    fn drop(&mut self) {
        let worker = tokio::runtime::Handle::try_current()
            .is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread);
        match worker {
            true => tokio::task::block_in_place(|| self.stop_all()),
            false => self.stop_all(),
        };
    }
}

/// Refuses a child whose descriptor is of another kind than its `kind`
/// says ([`HostError::KindMismatch`]: a domain is an aggregate or a process
/// manager). A policy or projector naming no input domain is the kernel's
/// validation's ([`HostError::Descriptor`]).
fn check_kind(
    domain: &str,
    kind: ComponentKindConfig,
    descriptor: &pb::ComponentDescriptor,
) -> Result<(), HostError> {
    let described = angzarr_embedded::kind_of(descriptor);
    let agrees = match kind {
        ComponentKindConfig::Domain => matches!(
            described,
            pb::ComponentKind::Aggregate | pb::ComponentKind::ProcessManager
        ),
        ComponentKindConfig::Policy => described == pb::ComponentKind::Policy,
        ComponentKindConfig::Projector => described == pb::ComponentKind::Projector,
        ComponentKindConfig::Upcaster => false,
    };
    match agrees {
        true => Ok(()),
        false => Err(HostError::KindMismatch {
            domain: domain.to_string(),
            configured: kind.as_str().to_string(),
            described: described
                .as_str_name()
                .trim_start_matches("COMPONENT_KIND_")
                .to_string(),
        }),
    }
}

/// Whether `described`, a restarted child's descriptor, is of the same
/// component as `registered`, the one its component was registered by: the
/// same name and kind, and for a policy or a projector (`kind`) the same
/// input domains and output domain, which its registration routes by.
fn same_component(
    kind: ComponentKindConfig,
    registered: &pb::ComponentDescriptor,
    described: &pb::ComponentDescriptor,
) -> bool {
    let routed = match kind {
        // A reader's whole kind is its routing: its inputs and output.
        ComponentKindConfig::Policy | ComponentKindConfig::Projector => {
            registered.kind == described.kind
        }
        ComponentKindConfig::Domain | ComponentKindConfig::Upcaster => true,
    };
    routed
        && registered.name == described.name
        && angzarr_embedded::kind_of(registered) == angzarr_embedded::kind_of(described)
}

/// Sends SIGTERM to `child`, unless it has been reaped.
#[cfg(unix)]
fn terminate(child: &mut Child) {
    let Some(pid) = child.id() else {
        return;
    };
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return;
    };
    // SAFETY: kill(2) takes no pointers. The pid is a child this host has
    // not yet reaped (tokio's Child gives its id only until then), so it
    // names that child and no other process.
    unsafe {
        libc::kill(pid, libc::SIGTERM);
    }
}

/// Starts the child `command` runs. A child is reached on a Unix socket
/// (the child protocol's transport, [`connect_unix`]), so a platform with
/// none starts no child (`crate::without_unix`'s `spawn`).
#[cfg(unix)]
fn spawn(command: &mut Command) -> std::io::Result<Child> {
    command.spawn()
}

/// The wait after `backoff` when a restart fails again: twice as long, up
/// to `max`.
fn next_backoff(backoff: Duration, max: Duration) -> Duration {
    (backoff * 2).min(max)
}

/// The longest Unix socket path, in bytes: the portable minimum of
/// `sun_path` (macOS and the BSDs' 104), less the NUL (the spec's 103, from
/// its `(child_protocol)` option; decision 14.108).
pub const MAX_SOCKET_PATH: usize =
    angzarr_proto_transport_embedded::child_protocol::MAX_SOCKET_PATH_BYTES;

/// The longest name [`SocketFile::fresh`] gives, with its leading `/`: a pid
/// (at most 7 digits on Linux) and a counter (at most 20).
const MAX_SOCKET_NAME: usize = "/angzarr--.sock".len() + 7 + 20;

/// Where the host puts its sockets: `configured`, refused when too long to
/// hold the socket names; else `xdg_runtime_dir` when it is absolute and
/// short enough; else `/tmp`.
pub(crate) fn socket_dir(
    configured: Option<&Path>,
    xdg_runtime_dir: Option<PathBuf>,
) -> Result<PathBuf, HostError> {
    let max = MAX_SOCKET_PATH - MAX_SOCKET_NAME;
    let fits = |dir: &Path| dir.as_os_str().len() <= max;
    match configured {
        Some(dir) if fits(dir) => Ok(dir.to_path_buf()),
        Some(dir) => Err(HostError::SocketDirTooLong {
            dir: dir.display().to_string(),
            max_bytes: max as u64,
        }),
        None => Ok(xdg_runtime_dir
            .filter(|dir| dir.is_absolute() && fits(dir))
            .unwrap_or_else(|| PathBuf::from("/tmp"))),
    }
}

/// The socket a child is told to serve on, its file removed when this
/// drops: however the child's supervision ends, stopped, restarted, given
/// up, or cancelled with the runtime it ran on, it leaves no socket file
/// behind (only a host killed outright does).
struct SocketFile(PathBuf);

impl SocketFile {
    /// A fresh path in `dir`, the host's pid and a counter, kept short (at
    /// most [`MAX_SOCKET_NAME`] bytes after `dir`); a leftover file there
    /// is removed.
    fn fresh(dir: &Path) -> SocketFile {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!("angzarr-{}-{n}.sock", std::process::id()));
        let _ = std::fs::remove_file(&path);
        SocketFile(path)
    }
}

impl Deref for SocketFile {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for SocketFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// A child that came up: SERVING on its socket, its health watched, and
/// described (a sidecar is not).
pub(crate) struct Up {
    child: Child,
    stdin: Option<ChildStdin>,
    pub(crate) channel: tonic::transport::Channel,
    pub(crate) descriptor: pb::ComponentDescriptor,
    /// The socket it was told to serve on, held for its removal when this
    /// drops.
    _socket: SocketFile,
    /// Whether it serves, as its health first said: a described child
    /// does, or it is refused.
    serving: bool,
    /// Its health, each status it changes to; `None` once the watch has
    /// ended.
    health: Option<tonic::Streaming<HealthCheckResponse>>,
}

/// The next status `health` reports, `true` for SERVING; never, when it is
/// `None`. A watch that ends (or fails) is not serving, and is not watched
/// again: the child stays down until it is restarted.
async fn next_health(health: &mut Option<tonic::Streaming<HealthCheckResponse>>) -> bool {
    let Some(watch) = health else {
        return std::future::pending().await;
    };
    match watch.message().await {
        Ok(Some(response)) => response.status == ServingStatus::Serving as i32,
        _ => {
            *health = None;
            false
        }
    }
}

/// How often a starting child's socket is tried, beside its check
/// interval, while nothing listens there.
const START_POLL: Duration = Duration::from_millis(20);

/// A child's health watch: its channel, and its statuses.
type Watched = (
    tonic::transport::Channel,
    tonic::Streaming<HealthCheckResponse>,
);

/// A health watch opened on the child's `socket`, given `deadline` on
/// `clock`: `None` while nothing listens there or answers a watch.
async fn open_watch(socket: &Path, deadline: Duration, clock: &Clock) -> Option<Watched> {
    let opened = async {
        let channel = connect_unix(socket.to_path_buf()).await.ok()?;
        let watch = HealthClient::new(channel.clone())
            .watch(HealthCheckRequest::default())
            .await
            .ok()?
            .into_inner();
        Some((channel, watch))
    };
    tokio::select! {
        opened = opened => opened,
        _ = clock.sleep(deadline) => None,
    }
}

/// `component`'s child brought to readiness (docs/design/child-protocol.md
/// §2; decision 14.80: there is no handshake line): its health watched on
/// the socket the host chose, `socket`, until it reports SERVING (for a
/// sidecar, `ready_at_any`, until it reports at all), within the start
/// limit, as the clock tells it a check interval at a time (and, while
/// nothing listens there, every [`START_POLL`] meanwhile). Refused
/// ([`HostError::NotServing`]) when the limit passes first, or `child`
/// exits first: `status` the last status it reported, `none` for none. The
/// watch, kept as the supervisor's, and whether it is SERVING.
async fn ready(
    component: &str,
    child: &mut Child,
    socket: &Path,
    supervision: &Supervision,
    (clock, hooks): (&Clock, &Hooks),
    ready_at_any: bool,
) -> Result<(Watched, bool), HostError> {
    let limit = Duration::from_millis(supervision.handshake_timeout_ms);
    let interval = Duration::from_millis(supervision.health_check_interval_ms);
    let deadline = Duration::from_millis(supervision.health_check_deadline_ms);
    let started = clock.now();
    let mut seen: Option<ServingStatus> = None;
    let not_serving = |seen: Option<ServingStatus>| HostError::NotServing {
        component: component.to_string(),
        status: seen.map_or("none", |s| s.as_str_name()).to_string(),
        timeout_ms: supervision.handshake_timeout_ms,
    };
    let mut watched: Option<Watched> = None;
    loop {
        if watched.is_none() {
            watched = tokio::select! {
                _ = child.wait() => return Err(not_serving(seen)),
                opened = open_watch(socket, deadline, clock) => opened,
            };
        }
        let waited = clock.now().saturating_sub(started);
        if waited >= limit {
            return Err(not_serving(seen));
        }
        let pause = interval.min(limit - waited);
        match watched.as_mut() {
            Some((_, watch)) => {
                let reported = tokio::select! {
                    _ = child.wait() => return Err(not_serving(seen)),
                    reported = watch.message() => Some(reported),
                    _ = clock.sleep(pause) => None,
                };
                match reported {
                    Some(Ok(Some(response))) => {
                        let status = ServingStatus::try_from(response.status)
                            .unwrap_or(ServingStatus::Unknown);
                        let serving = status == ServingStatus::Serving;
                        if serving || ready_at_any {
                            let watched = watched.take().expect("a watch is open");
                            return Ok((watched, serving));
                        }
                        seen = Some(status);
                        hooks.starting(status);
                    }
                    // The watch ended: opened again at the next turn.
                    Some(_) => watched = None,
                    None => {}
                }
            }
            None => {
                tokio::select! {
                    _ = child.wait() => return Err(not_serving(seen)),
                    _ = clock.sleep(pause) => {}
                    _ = tokio::time::sleep(START_POLL) => {}
                }
            }
        }
    }
}

/// Where a test holds every supervisor, to force an interleaving rather
/// than wait for it: none outside the tests.
#[derive(Clone, Default)]
pub(crate) struct Hooks {
    /// Awaited by a supervisor once it has told `start` its first child is
    /// up, before it does anything else.
    #[cfg(test)]
    reported_up: Option<Arc<tokio::sync::Notify>>,
    /// Told, by a supervisor, each health status it has acted on: `true`
    /// once it has published its child's connection, `false` once it has
    /// withdrawn it.
    #[cfg(test)]
    applied: Option<mpsc::Sender<bool>>,
    /// The clock a supervisor's health checks run on, which the test
    /// moves; tokio's when unset.
    #[cfg(test)]
    clock: Option<Arc<crate::clock::ManualClock>>,
    /// Told, by a supervisor, each verdict it reaches on its child's health
    /// once it is up (a check's, or its health watch's), once it has acted
    /// on it: published or withdrawn the child, or stopped it.
    #[cfg(test)]
    judged: Option<mpsc::Sender<crate::vitals::Verdict>>,
    /// Told, by a starting child's readiness, each health status its
    /// watch reports before it is ready.
    #[cfg(test)]
    starting: Option<mpsc::Sender<ServingStatus>>,
}

impl Hooks {
    /// The clock the supervisor's health checks run on.
    fn clock(&self) -> Clock {
        #[cfg(test)]
        if let Some(clock) = &self.clock {
            return Clock::manual(clock.clone());
        }
        Clock::tokio()
    }

    #[cfg_attr(not(test), allow(unused_variables))]
    fn judged(&self, verdict: Verdict) {
        #[cfg(test)]
        if let Some(judged) = &self.judged {
            let _ = judged.send(verdict);
        }
    }

    async fn reported_up(&self) {
        #[cfg(test)]
        if let Some(hold) = &self.reported_up {
            hold.notified().await;
        }
    }

    #[cfg_attr(not(test), allow(unused_variables))]
    fn starting(&self, status: ServingStatus) {
        #[cfg(test)]
        if let Some(starting) = &self.starting {
            let _ = starting.send(status);
        }
    }

    #[cfg_attr(not(test), allow(unused_variables))]
    fn applied(&self, serving: bool) {
        #[cfg(test)]
        if let Some(applied) = &self.applied {
            let _ = applied.send(serving);
        }
    }
}

/// How a running child's supervision ended ([`Supervisor::watch`]).
enum Ended {
    /// It exited by itself: it is restarted after the backoff.
    Exited,
    /// The host stopped it, ending supervision: how it ended.
    Stopped(ChildExit),
    /// It was unhealthy past its grace, and is stopped: it is restarted.
    Unhealthy,
    /// It was unhealthy past its grace with its component's restart limit
    /// spent, and is stopped: none is launched again.
    GivenUp,
}

/// One remote component's supervisor: it starts the child, restarts it when it
/// exits or is unhealthy past its grace, and stops it when told.
pub(crate) struct Supervisor {
    pub(crate) domain: String,
    pub(crate) kind: ComponentKindConfig,
    pub(crate) launch: Launch,
    pub(crate) supervision: Supervision,
    pub(crate) socket_dir: PathBuf,
    pub(crate) link: Arc<Link>,
    pub(crate) hooks: Hooks,
}

impl Supervisor {
    /// Supervises until `stop`; how the child ended. The first start's
    /// outcome goes to `up`; a first start that fails ends supervision.
    async fn run(
        self,
        mut stop: oneshot::Receiver<()>,
        up: mpsc::Sender<Result<pb::ComponentDescriptor, HostError>>,
    ) -> ChildExit {
        let mut up = Some(up);
        let initial = Duration::from_millis(self.supervision.initial_backoff_ms);
        let max = Duration::from_millis(self.supervision.max_backoff_ms);
        let mut backoff = initial;
        let clock = self.hooks.clock();
        let mut vitals = Vitals::new(&self.supervision);
        loop {
            let started = tokio::select! {
                started = self.come_up() => started,
                _ = &mut stop => return ChildExit::NotRunning,
            };
            let started = match started {
                // Every child is registered by its descriptor before it is
                // published: a restarted one is described again (C-0563).
                Ok(mut child) => match self.registered_by(&child.descriptor, up.is_some()) {
                    Ok(()) => Ok(child),
                    Err(error) => {
                        let _ = child.child.kill().await;
                        Err(error)
                    }
                },
                Err(error) => Err(error),
            };
            match started {
                Ok(mut child) => {
                    backoff = initial;
                    // Published before `start` hears the child is up, so
                    // the handler it hands back reaches the child at once.
                    self.serve(child.serving, &child.channel);
                    if let Some(up) = up.take() {
                        let _ = up.send(Ok(child.descriptor.clone()));
                        self.hooks.reported_up().await;
                    }
                    vitals.launched();
                    match self.watch(&mut child, &mut stop, &mut vitals, &clock).await {
                        Ended::Exited => {}
                        Ended::Stopped(exit) => return exit,
                        // Restarted at once: the backoff is for a child
                        // that fails to come up.
                        Ended::Unhealthy => continue,
                        Ended::GivenUp => {
                            drop(child);
                            let _ = (&mut stop).await;
                            return ChildExit::NotRunning;
                        }
                    }
                }
                Err(error) => {
                    if let Some(up) = up.take() {
                        let _ = up.send(Err(error));
                        return ChildExit::NotRunning;
                    }
                    backoff = next_backoff(backoff, max);
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = &mut stop => return ChildExit::NotRunning,
            }
        }
    }

    /// Supervises the running `child` until it ends: it exits, `stop` is
    /// told (the child is stopped), or its health, from its watch and from
    /// a check every interval, is unhealthy past its grace (the child is
    /// stopped, to be restarted or, past the restart limit, given up).
    /// Meanwhile it is published while it is healthy and withdrawn while it
    /// is not, each time that changes.
    async fn watch(
        &self,
        child: &mut Up,
        stop: &mut oneshot::Receiver<()>,
        vitals: &mut Vitals,
        clock: &Clock,
    ) -> Ended {
        let mut published = child.serving;
        let mut checking = Box::pin(self.checked(child.channel.clone(), clock.clone()));
        loop {
            let serving = tokio::select! {
                _ = child.child.wait() => {
                    self.set_channel(Err(causes::NOT_RUNNING));
                    return Ended::Exited;
                }
                _ = &mut *stop => return Ended::Stopped(self.stopped(child).await),
                serving = next_health(&mut child.health) => serving,
                serving = &mut checking => {
                    checking = Box::pin(self.checked(child.channel.clone(), clock.clone()));
                    serving
                }
            };
            let verdict = vitals.seen(serving, clock.now());
            let ended = match verdict {
                Verdict::Up | Verdict::Down => {
                    if serving != published {
                        self.serve(serving, &child.channel);
                        published = serving;
                    }
                    None
                }
                Verdict::Restart => Some(Ended::Unhealthy),
                Verdict::GiveUp => Some(Ended::GivenUp),
            };
            if ended.is_some() {
                self.stopped(child).await;
            }
            self.hooks.judged(verdict);
            if let Some(ended) = ended {
                return ended;
            }
        }
    }

    /// The child's health, by a `Check` once the check interval has passed
    /// on `clock`, given its deadline: whether it answered SERVING.
    async fn checked(&self, channel: tonic::transport::Channel, clock: Clock) -> bool {
        let interval = Duration::from_millis(self.supervision.health_check_interval_ms);
        let deadline = Duration::from_millis(self.supervision.health_check_deadline_ms);
        clock.sleep(interval).await;
        check(&channel, deadline, &clock).await
    }

    /// The child withdrawn and stopped ([`Supervisor::stop_child`]): how
    /// it ended. A call in flight on it fails retryably, and is never sent
    /// again. Its socket file goes when it is dropped.
    async fn stopped(&self, child: &mut Up) -> ChildExit {
        self.set_channel(Err(causes::NOT_RUNNING));
        self.stop_child(child).await
    }

    /// Registers a child by `descriptor`, its answer to `Describe` (an
    /// upcaster sidecar, which is not described, is left as it is): the
    /// `first` child's is the one its component is registered by; a
    /// restarted child's replaces it, its registered domain's descriptor
    /// replaced whole
    /// ([`Description::replace`](angzarr_embedded::Description::replace)),
    /// under the lock [`Host::register`] registers under, before the child
    /// is published. Refused, the old registration kept, when the registry
    /// refuses it ([`HostError::Register`]) or it describes another
    /// component than the first child did ([`HostError::DescriptorChanged`]):
    /// another name or kind, or a policy or projector of other inputs or
    /// another output domain, which the kernel routes by for good.
    fn registered_by(
        &self,
        descriptor: &pb::ComponentDescriptor,
        first: bool,
    ) -> Result<(), HostError> {
        if self.kind == ComponentKindConfig::Upcaster {
            return Ok(());
        }
        let mut registered = self.link.registered();
        if !first && !same_component(self.kind, &registered.descriptor, descriptor) {
            return Err(HostError::DescriptorChanged {
                domain: self.domain.clone(),
            });
        }
        if let Some(description) = &registered.description {
            description
                .replace(descriptor.clone())
                .map_err(|cause| HostError::Register {
                    cause: angzarr_diagnostic::Report::of(&cause),
                })?;
        }
        registered.descriptor = descriptor.clone();
        Ok(())
    }

    /// Publishes `channel`, the running child `serving`, counting it; or,
    /// not serving, withdraws the child's connection, the child down.
    fn serve(&self, serving: bool, channel: &tonic::transport::Channel) {
        match serving {
            true => {
                self.set_channel(Ok(channel.clone()));
                self.link.generation.send_modify(|n| *n += 1);
            }
            false => self.set_channel(Err(causes::NOT_SERVING)),
        }
        self.hooks.applied(serving);
    }

    fn set_channel(&self, channel: Result<tonic::transport::Channel, &'static str>) {
        *self
            .link
            .channel
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = channel;
    }

    /// Ends the host's watch of the child's health (a call in flight the
    /// child would otherwise wait out its grace for), closes the child's
    /// stdin and gives it the grace to exit; then sends it SIGTERM (where
    /// there is none, starts its kill) and gives it the terminate grace;
    /// then kills it.
    pub(crate) async fn stop_child(&self, up: &mut Up) -> ChildExit {
        drop(up.health.take());
        drop(up.stdin.take());
        let grace = Duration::from_millis(self.supervision.shutdown_grace_ms);
        if let Ok(Ok(status)) = tokio::time::timeout(grace, up.child.wait()).await {
            return ChildExit::Exited(status.code());
        }
        terminate(&mut up.child);
        let grace = Duration::from_millis(self.supervision.terminate_grace_ms);
        if let Ok(Ok(_)) = tokio::time::timeout(grace, up.child.wait()).await {
            return ChildExit::Terminated;
        }
        let _ = up.child.kill().await;
        ChildExit::Killed
    }

    /// Launches the child and brings it up: [`ready`] on the socket the
    /// host chose, then its health watched and, but for a sidecar, the
    /// child described and its descriptor checked. Its stdout is its own
    /// output, never parsed: passed to the host's stderr from its start,
    /// and drained so the child never blocks on a full pipe. On a failure
    /// the child, if one started, is killed and reaped, and its socket file
    /// removed, before the error is returned.
    pub(crate) async fn come_up(&self) -> Result<Up, HostError> {
        let domain = || self.domain.clone();
        let socket = SocketFile::fresh(&self.socket_dir);
        let mut command = Command::new(&self.launch.program);
        command
            .args(&self.launch.args)
            .envs(&self.launch.env)
            .env(SOCKET_ENV, &*socket)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        if let Some(dir) = &self.launch.dir {
            command.current_dir(dir);
        }
        let mut child = spawn(&mut command).map_err(|e| HostError::Spawn {
            domain: domain(),
            program: self.launch.program.clone(),
            kind: format!("{:?}", e.kind()),
        })?;
        let stdin = child.stdin.take();
        let mut stdout = child.stdout.take().expect("stdout is piped");
        tokio::spawn(async move {
            let _ = tokio::io::copy(&mut stdout, &mut tokio::io::stderr()).await;
        });
        let call_timeout = Duration::from_millis(self.supervision.call_timeout_ms);
        let clock = self.hooks.clock();
        let sidecar = self.kind == ComponentKindConfig::Upcaster;
        let reached = async {
            let ((channel, watch), serving) = ready(
                &self.domain,
                &mut child,
                &socket,
                &self.supervision,
                (&clock, &self.hooks),
                sidecar,
            )
            .await?;
            // An upcaster sidecar has no descriptor (decision 14.39): it
            // is up while its health reports SERVING.
            if sidecar {
                let descriptor = pb::ComponentDescriptor::default();
                return Ok((channel, descriptor, serving, Some(watch)));
            }
            let mut describe =
                tonic::Request::new(angzarr_proto_component::v1::DescribeRequest::default());
            describe.set_timeout(call_timeout);
            let descriptor = ComponentServiceClient::new(channel.clone())
                .describe(describe)
                .await
                .map_err(|status| HostError::Describe {
                    domain: domain(),
                    grpc_code: status.code() as i32,
                })?
                .into_inner();
            check_kind(&self.domain, self.kind, &descriptor)?;
            angzarr_embedded::validate_descriptor(&descriptor).map_err(|cause| {
                HostError::Descriptor {
                    domain: domain(),
                    cause,
                }
            })?;
            Ok((channel, descriptor, true, Some(watch)))
        }
        .await;
        match reached {
            Ok((channel, descriptor, serving, health)) => Ok(Up {
                child,
                stdin,
                channel,
                descriptor,
                _socket: socket,
                serving,
                health,
            }),
            Err(error) => {
                let _ = child.kill().await;
                Err(error)
            }
        }
    }
}

#[cfg(test)]
#[path = "host.test.rs"]
mod tests;

#[cfg(test)]
#[path = "kernel.test.rs"]
mod kernel_tests;

#[cfg(test)]
#[path = "components.test.rs"]
mod component_tests;
