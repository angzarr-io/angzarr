//! A fake remote domain for the host's tests: this very test binary, run
//! again as the child. The host launches it as
//! `<test binary> --exact fake_child::entry --nocapture` with
//! `ANGZARR_FAKE_CHILD` set to a mode; [`entry`] is a no-op in every other
//! run. libtest's own "running 1 test" line is on its stdout, as a chatty
//! child's would be: the host never reads it.
//!
//! The child serves stub `CommandHandlerService` and `ComponentService`s
//! on `ANGZARR_SOCKET`, describing itself as `ANGZARR_FAKE_NAME` (an
//! aggregate whose rules version is `pid-<its pid>`, so a test can tell one
//! child from its restart), and handles a command by its type URL's last
//! segment:
//!
//! - `...Refuse`: FAILED_PRECONDITION, with an ErrorInfo (reason
//!   `TABLE_FULL`, metadata `seats = 6`) and the typed refusal
//!   [`refusal`] in its details;
//! - `...Taken`: ALREADY_EXISTS, with an ErrorInfo (reason `SEAT_TAKEN`,
//!   metadata `seat = 3`) and the typed refusal [`refusal`]: a business
//!   refusal under a status angzarr-router's `GrpcCode` has no variant for;
//! - `...Break`: INTERNAL, with an ErrorInfo (reason `PLAYER_BROKE`, a
//!   code the router does not know, metadata `seat = 3`): a bug;
//! - `...Die`: the process exits with code 7, mid-call;
//! - `...Hang`: never answers;
//! - anything else: one event, [`noted`], on the command's cover, rules
//!   version `pid-<its pid>`.
//!
//! `ANGZARR_FAKE_KIND` makes it another kind (unset: an aggregate):
//!
//! - `process_manager`: a process manager whose starting types are
//!   `ANGZARR_FAKE_STARTING`'s, comma-separated, handling as above;
//! - `policy`: a `PolicyService` from `ANGZARR_FAKE_INPUT` to
//!   `ANGZARR_FAKE_TARGET`, reacting to a `...Seated` event with
//!   [`HOLD`] and a `...SeatedBig` one with [`HOLD_TOO_MUCH`], each to the
//!   event's root; and translating a refusal into the fact
//!   [`SEAT_RELEASED`], its value the refused command's type URL;
//! - `projector`: a `ProjectorService` of `ANGZARR_FAKE_INPUT`'s events,
//!   failing an event by its type URL's last segment: `...Break` INTERNAL
//!   (`PROJECTOR_BROKE`), `...Refuse` FAILED_PRECONDITION
//!   (`VIEW_REFUSED`), `...Down` UNAVAILABLE (`VIEW_DB_DOWN`) every time,
//!   and the first time only, `...Flaky` UNAVAILABLE (`VIEW_DB_DOWN`) and
//!   `...Stale` ABORTED (`VIEW_STALE`).
//!
//! A policy or projector appends a line to `ANGZARR_FAKE_LOG` for each
//! call: `handle <type>` per event, `refused <typed refusal type URL>`.
//!
//! - `upcaster`: an upcaster sidecar (decision 14.39), serving
//!   `UpcasterService` and gRPC health (no descriptor), answering each
//!   page keyed by its sequence: a page whose event is
//!   `/test.table.v0.Sit` comes back `/test.table.v1.Seat`;
//!   `...v0.Die` exits with code 7 mid-call; `...v0.Bad` is INTERNAL
//!   (`UPCAST_RULE_BROKE`); any other passes through.
//!
//! Every kind serves gRPC health on `ANGZARR_SOCKET`, as the child protocol
//! says a child must (its readiness: there is no handshake line): SERVING,
//! or `ANGZARR_FAKE_HEALTH`'s ([`health`]: `none` serves no health at
//! all); given `ANGZARR_FAKE_CONTROL`, a Unix socket's path, it
//! connects there as it starts, and each byte read sets its status: `1`
//! SERVING, any other NOT_SERVING; and it writes `h` there when a
//! `...Hang` command reaches it, so a test knows the call is in flight.
//! Given `ANGZARR_FAKE_CHECK_HANG`, a path, a health `Check` made while a
//! file is there never answers (a wedged child, its health watch still
//! answering); one made once it is gone answers as ever.
//!
//! An aggregate appends `handle <type URL> pid-<its pid>` to
//! `ANGZARR_FAKE_LOG` for each command, before it acts on it.
//!
//! It exits with code 0 when its stdin reaches end of file.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;

use crate::rpc::google_rpc::{ErrorInfo, Status as RpcStatus, ERROR_INFO_TYPE_URL};
use angzarr_proto::v1 as pb;
use prost::Message;
use prost_types::Any;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};

use crate::config::{ComponentConfig, HostConfig, Supervision};
use crate::host::SOCKET_ENV;
use angzarr_proto_component::v1::command_handler_service_server::{
    CommandHandlerService, CommandHandlerServiceServer,
};
use angzarr_proto_component::v1::component_service_server::{
    ComponentService, ComponentServiceServer,
};
use angzarr_proto_component::v1::policy_service_server::{PolicyService, PolicyServiceServer};
use angzarr_proto_component::v1::projector_service_server::{
    ProjectorService, ProjectorServiceServer,
};
use angzarr_proto_component::v1::upcaster_service_server::{
    UpcasterService, UpcasterServiceServer,
};
use angzarr_proto_component::v1::DescribeRequest;
use angzarr_router::hpb;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_server::{Health, HealthServer};
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

/// The variable naming the fake's mode.
pub(crate) const MODE_ENV: &str = "ANGZARR_FAKE_CHILD";
/// The variable naming a file the fake writes when it exits on stdin's end,
/// and only then: a kill leaves no mark.
pub(crate) const EOF_MARK_ENV: &str = "ANGZARR_FAKE_EOF_MARK";
/// The variable naming the component it describes.
pub(crate) const NAME_ENV: &str = "ANGZARR_FAKE_NAME";
/// The variable naming its kind: [`kind`]'s; an aggregate when unset.
pub(crate) const KIND_ENV: &str = "ANGZARR_FAKE_KIND";
/// A process manager's starting types, comma-separated, as its descriptor
/// lists them.
pub(crate) const STARTING_ENV: &str = "ANGZARR_FAKE_STARTING";
/// A policy's or projector's input domain.
pub(crate) const INPUT_ENV: &str = "ANGZARR_FAKE_INPUT";
/// A policy's target domain.
pub(crate) const TARGET_ENV: &str = "ANGZARR_FAKE_TARGET";
/// The file a policy or projector logs its calls to.
pub(crate) const LOG_ENV: &str = "ANGZARR_FAKE_LOG";
/// Its health as it starts: [`health`]'s; SERVING when unset.
pub(crate) const HEALTH_ENV: &str = "ANGZARR_FAKE_HEALTH";
/// The Unix socket a child reads its health from, byte by byte.
pub(crate) const CONTROL_ENV: &str = "ANGZARR_FAKE_CONTROL";
/// The domain an aggregate or process manager describes; its described
/// name when unset.
pub(crate) const DOMAIN_ENV: &str = "ANGZARR_FAKE_DOMAIN";
/// A path: while a file is there, a health `Check` never answers.
pub(crate) const CHECK_HANG_ENV: &str = "ANGZARR_FAKE_CHECK_HANG";

/// What the fake writes on its control socket when a `...Hang` command
/// reaches it.
pub(crate) const HUNG: u8 = b'h';

/// The control socket's writing half, once the fake has connected: where
/// a `...Hang` command says it arrived.
static CONTROL: std::sync::OnceLock<tokio::sync::Mutex<tokio::net::unix::OwnedWriteHalf>> =
    std::sync::OnceLock::new();

/// A child's health as it starts, besides SERVING.
pub(crate) mod health {
    /// NOT_SERVING.
    pub(crate) const NOT_SERVING: &str = "not-serving";
    /// UNKNOWN.
    pub(crate) const UNKNOWN: &str = "unknown";
    /// SERVICE_UNKNOWN (a Watch's answer for a service it does not know).
    pub(crate) const SERVICE_UNKNOWN: &str = "service-unknown";
    /// No health service at all.
    pub(crate) const NONE: &str = "none";
}

/// The kinds besides an aggregate.
pub(crate) mod kind {
    pub(crate) const PROCESS_MANAGER: &str = "process_manager";
    pub(crate) const POLICY: &str = "policy";
    pub(crate) const PROJECTOR: &str = "projector";
    pub(crate) const UPCASTER: &str = "upcaster";
}

/// The command a policy answers `...Seated` with.
pub(crate) const HOLD: &str = "/test.wallet.v1.Hold";
/// The command a policy answers `...SeatedBig` with.
pub(crate) const HOLD_TOO_MUCH: &str = "/test.wallet.v1.HoldTooMuch";
/// The fact a policy answers a refusal with.
pub(crate) const SEAT_RELEASED: &str = "/test.table.v1.SeatReleased";

/// The fake's behaviours.
pub(crate) mod mode {
    /// Serve on the socket the host chose.
    pub(crate) const SERVE: &str = "serve";
    /// Lines of chatter on stdout, then serve.
    pub(crate) const NOISY: &str = "noisy";
    /// Never listen on the path; wait for stdin's end.
    pub(crate) const SILENT: &str = "silent";
    /// Exit with status 3 at once, before any health Check is answered.
    pub(crate) const EXITS: &str = "exits";
    /// Serve, and ignore stdin's end: SIGTERM (its default action) stops
    /// it.
    pub(crate) const STUBBORN: &str = "stubborn";
    /// Serve, and ignore stdin's end and SIGTERM: only SIGKILL stops it.
    pub(crate) const DEAF: &str = "deaf";
    /// Close stdout, then serve: the host sees stdout's end while the
    /// child lives on.
    pub(crate) const CLOSES_STDOUT: &str = "closes-stdout";
    /// Serve; on stdin's end, exit only once no health watch is open, as a
    /// server letting its calls in flight finish does.
    pub(crate) const GRACEFUL: &str = "graceful";
    /// Write more on stdout than a pipe holds
    /// ([`FLOOD_BYTES`](super::FLOOD_BYTES)), then serve: it serves only
    /// once the host drains its stdout.
    pub(crate) const FLOOD: &str = "flood";
}

/// What the flooding fake writes on stdout before it listens: four times
/// Linux's default pipe capacity.
pub(crate) const FLOOD_BYTES: usize = 4 * 65_536;

/// The typed refusal a `...Refuse` command is answered with.
pub(crate) fn refusal() -> Any {
    Any {
        type_url: "/examples.blackjack.TableFull".to_string(),
        value: vec![8, 6],
    }
}

/// The launch command that runs this binary as the fake child.
pub(crate) fn launch() -> Vec<String> {
    vec![
        std::env::current_exe()
            .unwrap()
            .to_string_lossy()
            .into_owned(),
        "--exact".to_string(),
        "fake_child::entry".to_string(),
        "--nocapture".to_string(),
    ]
}

/// A domain whose child is the fake in `mode`, describing itself as
/// `name`. Its language is Python, whose default launch (uv) the config's
/// `launch` overrides: the fake is what runs.
pub(crate) fn domain(name: &str, described: &str, mode: &str) -> ComponentConfig {
    ComponentConfig {
        name: name.to_string(),
        language: "python".to_string(),
        kind: crate::config::ComponentKindConfig::Domain,
        upcasts: None,
        launch: Some(launch()),
        entry: vec!["-m".to_string(), "never.used".to_string()],
        dir: None,
        env: BTreeMap::from([
            (MODE_ENV.to_string(), mode.to_string()),
            (NAME_ENV.to_string(), described.to_string()),
            (DOMAIN_ENV.to_string(), name.to_string()),
        ]),
        expose: Default::default(),
        scaffold: Default::default(),
    }
}

/// A config of `domains`, with short waits so the tests are quick.
pub(crate) fn config(domains: Vec<ComponentConfig>) -> HostConfig {
    HostConfig {
        store: Default::default(),
        policy: Default::default(),
        retry: Default::default(),
        components: domains,
        supervision: Supervision {
            handshake_timeout_ms: 20_000,
            initial_backoff_ms: 50,
            max_backoff_ms: 200,
            shutdown_grace_ms: 3_000,
            terminate_grace_ms: 3_000,
            call_timeout_ms: 20_000,
            // No test waits for a check: one that is about checks holds
            // the supervisor's clock (`Hooks::clock`) and moves it itself.
            health_check_interval_ms: 3_600_000,
            health_check_deadline_ms: 3_600_000,
            unhealthy_grace_ms: 3_600_000,
            unhealthy_restart_limit: 3,
            unhealthy_reset_ms: 3_600_000,
            socket_dir: None,
        },
    }
}

/// The child's entry: a no-op unless this run is the fake.
#[test]
fn entry() {
    let Ok(mode) = std::env::var(MODE_ENV) else {
        return;
    };
    let name = std::env::var(NAME_ENV).unwrap_or_default();
    if mode == mode::DEAF {
        // SAFETY: setting a signal's disposition to SIG_IGN installs no
        // handler code; nothing else here touches SIGTERM.
        unsafe {
            libc::signal(libc::SIGTERM, libc::SIG_IGN);
        }
    }
    if mode != mode::STUBBORN && mode != mode::DEAF {
        let graceful = mode == mode::GRACEFUL;
        std::thread::spawn(move || {
            let _ = std::io::copy(&mut std::io::stdin(), &mut std::io::sink());
            // A graceful server lets its calls in flight finish: a health
            // watch the host keeps open holds it up, for good.
            while graceful && WATCHES.load(std::sync::atomic::Ordering::SeqCst) > 0 {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            if let Some(mark) = std::env::var_os(EOF_MARK_ENV) {
                std::fs::write(mark, b"eof").unwrap();
            }
            std::process::exit(0);
        });
    }
    match mode.as_str() {
        mode::SILENT => park(),
        mode::EXITS => std::process::exit(3),
        mode::NOISY => {
            say("Loading player...");
            say("");
            serve(name, false, false)
        }
        mode::CLOSES_STDOUT => serve(name, true, false),
        mode::FLOOD => serve(name, false, true),
        _ => serve(name, false, false),
    }
}

fn say(line: &str) {
    let mut out = std::io::stdout().lock();
    writeln!(out, "{line}").unwrap();
    out.flush().unwrap();
}

fn park() -> ! {
    loop {
        std::thread::park();
    }
}

/// The health a child starts with (`started`, [`HEALTH_ENV`]'s), changed by
/// each byte read from the control socket [`CONTROL_ENV`] names, if any.
async fn health_status(started: &str) -> tokio::sync::watch::Receiver<i32> {
    let initial = match started {
        health::NOT_SERVING => ServingStatus::NotServing,
        health::UNKNOWN => ServingStatus::Unknown,
        health::SERVICE_UNKNOWN => ServingStatus::ServiceUnknown,
        _ => ServingStatus::Serving,
    };
    let (set, status) = tokio::sync::watch::channel(initial as i32);
    let Ok(control) = std::env::var(CONTROL_ENV) else {
        std::mem::forget(set);
        return status;
    };
    let (mut control, writer) = tokio::net::UnixStream::connect(control)
        .await
        .unwrap()
        .into_split();
    let _ = CONTROL.set(tokio::sync::Mutex::new(writer));
    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut byte = [0u8];
        while control.read_exact(&mut byte).await.is_ok() {
            let now = match byte[0] {
                b'1' => ServingStatus::Serving,
                _ => ServingStatus::NotServing,
            };
            let _ = set.send(now as i32);
        }
        std::future::pending::<()>().await;
    });
    status
}

fn serve(name: String, close_stdout: bool, flood: bool) -> ! {
    let socket = PathBuf::from(std::env::var(SOCKET_ENV).expect("the host sets ANGZARR_SOCKET"));
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async move {
        let _ = std::fs::remove_file(&socket);
        if close_stdout {
            close_stdout_for_good();
        }
        if flood {
            // More than a pipe holds, before anything is served: a child
            // whose stdout is not drained blocks here.
            let mut out = std::io::stdout().lock();
            out.write_all(&[b'.'; FLOOD_BYTES]).unwrap();
            writeln!(out).unwrap();
            out.flush().unwrap();
        }
        let listener = UnixListener::bind(&socket).unwrap();
        let fake = Fake { name };
        let kind = std::env::var(KIND_ENV).unwrap_or_default();
        let started = std::env::var(HEALTH_ENV).unwrap_or_default();
        let health = match started.as_str() {
            health::NONE => None,
            _ => Some(HealthServer::new(FakeHealth {
                status: health_status(&started).await,
            })),
        };
        let server = tonic::transport::Server::builder().add_optional_service(health);
        let server = match kind.as_str() {
            kind::UPCASTER => server.add_service(UpcasterServiceServer::new(fake)),
            kind::POLICY => server
                .add_service(ComponentServiceServer::new(fake.clone()))
                .add_service(PolicyServiceServer::new(fake)),
            kind::PROJECTOR => server
                .add_service(ComponentServiceServer::new(fake.clone()))
                .add_service(ProjectorServiceServer::new(fake)),
            _ => server
                .add_service(ComponentServiceServer::new(fake.clone()))
                .add_service(CommandHandlerServiceServer::new(fake)),
        };
        server
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await
            .unwrap();
    });
    // Reached only if the server stopped on its own.
    std::process::exit(3)
}

/// Closes the stdout pipe, so its reader sees end of file, and parks
/// /dev/null on descriptor 1 (the lowest free one, just closed), so a later
/// write to stdout goes nowhere rather than into a socket.
fn close_stdout_for_good() {
    use std::os::fd::{FromRawFd, OwnedFd};
    // SAFETY: descriptor 1 is this process's stdout, which nothing else
    // here owns; it is flushed, and closed exactly once.
    drop(unsafe { OwnedFd::from_raw_fd(1) });
    let null = std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/null")
        .unwrap();
    std::mem::forget(null);
}

#[derive(Clone)]
struct Fake {
    name: String,
}

/// How many health watches are open on the fake: the graceful fake waits
/// for none.
static WATCHES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// A child's gRPC health: the status it holds, for the server as a whole
/// and for every service.
struct FakeHealth {
    status: tokio::sync::watch::Receiver<i32>,
}

#[tonic::async_trait]
impl Health for FakeHealth {
    type WatchStream = tokio_stream::wrappers::ReceiverStream<Result<HealthCheckResponse, Status>>;

    async fn check(
        &self,
        _: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        if std::env::var_os(CHECK_HANG_ENV).is_some_and(|path| PathBuf::from(path).exists()) {
            std::future::pending::<()>().await;
        }
        Ok(Response::new(HealthCheckResponse {
            status: *self.status.borrow(),
        }))
    }

    /// The status now, then each status it changes to.
    async fn watch(
        &self,
        _: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let mut status = self.status.clone();
        let (send, sent) = tokio::sync::mpsc::channel(4);
        WATCHES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        tokio::spawn(async move {
            loop {
                let now = *status.borrow_and_update();
                if send
                    .send(Ok(HealthCheckResponse { status: now }))
                    .await
                    .is_err()
                {
                    break;
                }
                tokio::select! {
                    changed = status.changed() => if changed.is_err() { break },
                    _ = send.closed() => break,
                }
            }
            WATCHES.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        });
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            sent,
        )))
    }
}

#[tonic::async_trait]
impl UpcasterService for Fake {
    async fn upcast(
        &self,
        request: Request<hpb::UpcastRequest>,
    ) -> Result<Response<hpb::UpcastResponse>, Status> {
        let pages: Vec<(Option<u64>, Any)> = match request.into_inner().input {
            Some(hpb::upcast_request::Input::LonePayload(payload)) => vec![(None, payload)],
            Some(hpb::upcast_request::Input::Pages(pages)) => pages
                .events
                .iter()
                .filter_map(|page| {
                    angzarr_router::page_event(page).map(|e| (Some(page.sequence), e.clone()))
                })
                .collect(),
            None => Vec::new(),
        };
        let mut answered = Vec::new();
        for (sequence, mut payload) in pages {
            match payload.type_url.as_str() {
                "/test.table.v0.Sit" => payload.type_url = "/test.table.v1.Seat".to_string(),
                "/test.table.v0.Die" => std::process::exit(7),
                "/test.table.v0.Bad" => {
                    return Err(status(
                        tonic::Code::Internal,
                        "the upcast rule broke",
                        "UPCAST_RULE_BROKE",
                        ("type_url", "/test.table.v0.Bad"),
                        None,
                    ))
                }
                _ => {}
            }
            let key = match sequence {
                Some(sequence) => hpb::upcasted::Key::Sequence(sequence),
                None => hpb::upcasted::Key::NoSequence(hpb::NoSequence {}),
            };
            answered.push(hpb::Upcasted {
                key: Some(key),
                payload: Some(payload),
            });
        }
        Ok(Response::new(hpb::UpcastResponse { pages: answered }))
    }
}

fn rules_version() -> String {
    format!("pid-{}", std::process::id())
}

#[tonic::async_trait]
impl ComponentService for Fake {
    async fn describe(
        &self,
        _: Request<DescribeRequest>,
    ) -> Result<Response<pb::ComponentDescriptor>, Status> {
        use pb::component_descriptor::Kind;
        let env = |name| std::env::var(name).unwrap_or_default();
        // The domain it serves: its component entry's name
        // ([`DOMAIN_ENV`]), else its described name.
        let domain = match env(DOMAIN_ENV) {
            named if !named.is_empty() => named,
            _ => self.name.clone(),
        };
        let state = Some(pb::StateDescriptor {
            type_name: "test.player.v1.State".to_string(),
        });
        let kind = match env(KIND_ENV).as_str() {
            kind::POLICY => Kind::Policy(pb::PolicyDescriptor {
                input_domain: env(INPUT_ENV),
                output_domain: env(TARGET_ENV),
            }),
            kind::PROJECTOR => Kind::Projector(pb::ProjectorDescriptor {
                input_domains: [env(INPUT_ENV)]
                    .into_iter()
                    .filter(|d| !d.is_empty())
                    .collect(),
            }),
            kind::PROCESS_MANAGER => Kind::ProcessManager(pb::ProcessManagerDescriptor {
                domain: domain.clone(),
                state,
                starting_types: env(STARTING_ENV)
                    .split(',')
                    .filter(|t| !t.is_empty())
                    .map(str::to_string)
                    .collect(),
            }),
            _ => Kind::Aggregate(pb::AggregateDescriptor {
                domain: domain.clone(),
                state,
            }),
        };
        Ok(Response::new(pb::ComponentDescriptor {
            name: self.name.clone(),
            rules_version: rules_version(),
            kind: Some(kind),
        }))
    }
}

#[tonic::async_trait]
impl CommandHandlerService for Fake {
    async fn handle(
        &self,
        request: Request<hpb::HandleRequest>,
    ) -> Result<Response<hpb::HandleResponse>, Status> {
        let request = request.into_inner();
        let next = request.history.as_ref().map_or(0, |b| b.next_sequence);
        let command = request.input.unwrap_or_default();
        let type_url = command
            .page
            .as_ref()
            .and_then(|page| {
                angzarr_router::command_payload(page).or_else(|| angzarr_router::fact_payload(page))
            })
            .map(|any| any.type_url.clone())
            .unwrap_or_default();
        log(&format!("handle {type_url} {}", rules_version()));
        if type_url.ends_with("Refuse") {
            return Err(refused());
        }
        if type_url.ends_with("Taken") {
            return Err(taken());
        }
        if type_url.ends_with("Break") {
            return Err(broke());
        }
        if type_url.ends_with("Hang") {
            if let Some(control) = CONTROL.get() {
                use tokio::io::AsyncWriteExt;
                let _ = control.lock().await.write_all(&[HUNG]).await;
            }
            std::future::pending::<()>().await;
        }
        if type_url.ends_with("Die") {
            std::process::exit(7);
        }
        Ok(Response::new(hpb::HandleResponse {
            events: vec![noted(next)],
            state: None,
            rules_version: rules_version(),
        }))
    }
}

/// Appends `line` to the fake's call log, if it has one.
fn log(line: &str) {
    if let Some(path) = std::env::var_os(LOG_ENV) {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        writeln!(file, "{line}").unwrap();
    }
}

/// How many lines of the call log are `line`.
fn logged(line: &str) -> usize {
    std::env::var_os(LOG_ENV)
        .and_then(|path| std::fs::read_to_string(path).ok())
        .map_or(0, |text| text.lines().filter(|l| *l == line).count())
}

/// An `Any` of `type_url` carrying `value`.
fn packed(type_url: &str, value: Vec<u8>) -> Any {
    Any {
        type_url: type_url.to_string(),
        value,
    }
}

#[tonic::async_trait]
impl PolicyService for Fake {
    async fn react(
        &self,
        request: Request<hpb::ReactRequest>,
    ) -> Result<Response<hpb::ReactResponse>, Status> {
        let book = request.into_inner().trigger.unwrap_or_default();
        let root = book.cover.as_ref().and_then(|c| c.root.clone());
        let mut outputs = Vec::new();
        for page in &book.pages {
            let type_url = angzarr_router::page_event(page).map_or("", |a| a.type_url.as_str());
            log(&format!("handle {type_url}"));
            let command = if type_url.ends_with("SeatedBig") {
                HOLD_TOO_MUCH
            } else if type_url.ends_with("Seated") {
                HOLD
            } else {
                continue;
            };
            outputs.push(angzarr_router::policy::command(
                root.clone(),
                packed(command, Vec::new()),
            ));
        }
        Ok(Response::new(hpb::ReactResponse { outputs }))
    }

    async fn translate(
        &self,
        request: Request<hpb::TranslateRequest>,
    ) -> Result<Response<hpb::TranslateResponse>, Status> {
        let request = request.into_inner();
        let typed = (request.refusal.as_ref())
            .and_then(|r| r.typed.as_ref())
            .map_or(String::new(), |any| any.type_url.clone());
        log(&format!("refused {typed}"));
        let refused = match request.command.and_then(|c| c.body) {
            Some(
                pb::reaction_delivery::Body::Command(any) | pb::reaction_delivery::Body::Fact(any),
            ) => any.type_url,
            None => String::new(),
        };
        Ok(Response::new(hpb::TranslateResponse {
            fact: Some(packed(SEAT_RELEASED, refused.into_bytes())),
        }))
    }
}

#[tonic::async_trait]
impl ProjectorService for Fake {
    async fn project(
        &self,
        request: Request<hpb::ProjectRequest>,
    ) -> Result<Response<hpb::ProjectResponse>, Status> {
        let book = request.into_inner().events.unwrap_or_default();
        for page in &book.pages {
            let type_url = angzarr_router::page_event(page).map_or("", |a| a.type_url.as_str());
            let line = format!("handle {type_url}");
            let first = logged(&line) == 0;
            log(&line);
            let fail =
                |code, reason: &str| status(code, reason, reason, ("type_url", type_url), None);
            if type_url.ends_with("Break") {
                return Err(fail(tonic::Code::Internal, "PROJECTOR_BROKE"));
            }
            if type_url.ends_with("Refuse") {
                return Err(fail(tonic::Code::FailedPrecondition, "VIEW_REFUSED"));
            }
            if type_url.ends_with("Down") {
                return Err(fail(tonic::Code::Unavailable, "VIEW_DB_DOWN"));
            }
            if first && type_url.ends_with("Flaky") {
                return Err(fail(tonic::Code::Unavailable, "VIEW_DB_DOWN"));
            }
            if first && type_url.ends_with("Stale") {
                return Err(fail(tonic::Code::Aborted, "VIEW_STALE"));
            }
        }
        Ok(Response::new(hpb::ProjectResponse { projection: None }))
    }
}

/// The one event a command is answered with: `Noted`, its value the
/// stream's next sequence when the command arrived.
pub(crate) fn noted(next: u64) -> Any {
    Any {
        type_url: "/test.player.v1.Noted".to_string(),
        value: next.to_le_bytes().to_vec(),
    }
}

fn refused() -> Status {
    status(
        tonic::Code::FailedPrecondition,
        "table is full",
        "TABLE_FULL",
        ("seats", "6"),
        Some(refusal()),
    )
}

fn taken() -> Status {
    status(
        tonic::Code::AlreadyExists,
        "the seat is taken",
        "SEAT_TAKEN",
        ("seat", "3"),
        Some(refusal()),
    )
}

fn broke() -> Status {
    status(
        tonic::Code::Internal,
        "the player's handler broke",
        "PLAYER_BROKE",
        ("seat", "3"),
        None,
    )
}

/// A status as angzarr's client libraries answer one: `code` and
/// `message`, and in its details an ErrorInfo of `reason` and the one
/// `metadata` pair, then `typed`, if any.
fn status(
    code: tonic::Code,
    message: &str,
    reason: &str,
    metadata: (&str, &str),
    typed: Option<Any>,
) -> Status {
    let info = ErrorInfo {
        reason: reason.to_string(),
        domain: angzarr_router::error::ERROR_INFO_DOMAIN.to_string(),
        metadata: [(metadata.0.to_string(), metadata.1.to_string())].into(),
    };
    let info = Any {
        type_url: ERROR_INFO_TYPE_URL.to_string(),
        value: info.encode_to_vec(),
    };
    let rpc = RpcStatus {
        code: code as i32,
        message: message.to_string(),
        details: std::iter::once(info).chain(typed).collect(),
    };
    Status::with_details(code, message, rpc.encode_to_vec().into())
}
