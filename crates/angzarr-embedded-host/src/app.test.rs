use std::path::{Path, PathBuf};

use angzarr_embedded::v1::{command_page, Command, CommandBook, CommandPage, Cover, Uuid};
use angzarr_embedded::{Any, Handled, Misconfigured, Registry, Sampled, Timestamp};
use coded_diagnostic::Report;

use super::*;
use crate::config::HostConfig;
use crate::fake_child::{config, domain, mode, noted};
use test_dir::TestDir;

/// `body` stamped at the current version, written as `angzarr.yaml` into
/// `dir`.
fn written(dir: &Path, body: &str) -> PathBuf {
    let path = dir.join(crate::config::FILE_NAME);
    std::fs::write(&path, crate::config::stamped(body)).unwrap();
    path
}

/// [`run_with`] on `config`, in the built-in languages.
async fn run_config(
    config: &HostConfig,
    register: impl FnOnce(&mut Registry) -> Result<(), Misconfigured>,
) -> Result<App, HostError> {
    run_with(config.clone(), Languages::builtin(), register).await
}

/// A player domain served by the fake child.
fn player_config() -> HostConfig {
    config(vec![domain("player", "PlayerAggregate", mode::SERVE)])
}

fn player() -> Cover {
    Cover {
        domain: "player".to_string(),
        root: Some(Uuid::from_bytes([0x0a; 16])),
        edition: None,
    }
}

fn sit_down(n: u8) -> CommandBook {
    CommandBook {
        cover: Some(player()),
        page: Some(CommandPage {
            body: Some(command_page::Body::Command(Command {
                payload: Some(Any {
                    type_url: "/test.player.v1.SitDown".to_string(),
                    value: Vec::new(),
                }),
                handling: None,
            })),
            command_id: Some(Uuid::from_bytes([n; 16])),
            ..CommandPage::default()
        }),
    }
}

fn sampled(n: u8) -> Sampled {
    Sampled {
        now: Timestamp {
            seconds: i64::from(n),
            nanos: 0,
        },
        command_id: Uuid::from_bytes([0x70 | n; 16]),
    }
}

fn nothing(_: &mut Registry) -> Result<(), Misconfigured> {
    Ok(())
}

/// The whole application from its config: a command for the remote domain
/// goes through the kernel and its event is in the store.
#[tokio::test(flavor = "multi_thread")]
async fn run_handles_a_command_for_a_remote_domain() {
    let mut config = player_config();
    config.store.in_memory = true;
    let app = run_config(&config, nothing).await.expect("the app runs");
    assert!(matches!(
        app.handle(sit_down(1), &sampled(1)).await,
        Ok(Handled::Applied(_))
    ));
    let events: Vec<_> = app
        .events(&player())
        .await
        .unwrap()
        .into_iter()
        .map(|p| angzarr_embedded::angzarr_router::page_event(&p).cloned())
        .collect();
    assert_eq!(events, [Some(noted(0))]);
    assert_eq!(
        app.shutdown().await,
        vec![("player".to_string(), ChildExit::Exited(Some(0)))]
    );
}

/// `register` is called with the remote domains already registered, and
/// what it registers is the kernel's too.
#[tokio::test(flavor = "multi_thread")]
async fn run_calls_register_for_the_in_process_domains() {
    let mut config = player_config();
    config.store.in_memory = true;
    let app = run_config(&config, |registry: &mut Registry| {
        assert_eq!(
            registry
                .command_handler("player", NoHandler, fixtures::descriptor("player"))
                .err(),
            Some(Misconfigured::AlreadyRegistered {
                domain: "player".to_string(),
            }),
            "the remote domain is registered before register runs"
        );
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(
        app.registry().catalog().domains().collect::<Vec<_>>(),
        ["player"],
        "the app's registry is the one run filled"
    );
    drop(app);
}

/// The SQLite store is at the config's path: what one run wrote, the next
/// reads, and the next command reaches the child with it.
#[tokio::test(flavor = "multi_thread")]
async fn the_store_is_the_configured_sqlite_file() {
    let dir = TestDir::new("angzarr-host-app");
    let mut config = player_config();
    config.store.path = dir.join("app.db");
    let app = run_config(&config, nothing).await.unwrap();
    app.handle(sit_down(1), &sampled(1)).await.unwrap();
    drop(app);
    assert!(dir.join("app.db").exists());
    let app = run_config(&config, nothing).await.unwrap();
    app.handle(sit_down(2), &sampled(2)).await.unwrap();
    let events: Vec<_> = app
        .events(&player())
        .await
        .unwrap()
        .into_iter()
        .map(|p| angzarr_embedded::angzarr_router::page_event(&p).cloned())
        .collect();
    assert_eq!(events, [Some(noted(0)), Some(noted(1))]);
    drop(app);
}

/// The kernel the app hands out does with a policy's bug what the
/// config's `policy.on_bug` says; the default when it is left out.
#[tokio::test(flavor = "multi_thread")]
async fn the_kernel_follows_the_configs_policy_setting() {
    use angzarr_embedded::OnPolicyBug;
    let dir = TestDir::new("angzarr-host-app");
    let path = written(&dir, "store:\n  in_memory: true\n");
    let app = run(&path, nothing).await.unwrap();
    assert_eq!(app.kernel().policy_bug(), OnPolicyBug::MovePast);
    drop(app);
    let path = written(&dir, "store:\n  in_memory: true\npolicy:\n  on_bug: stay\n");
    let app = run(&path, nothing).await.unwrap();
    assert_eq!(app.kernel().policy_bug(), OnPolicyBug::Stay);
    drop(app);
}

/// The kernel the app hands out retries a step's retryable failure as the
/// config's `retry` says, its defaults (the kernel's) when it is left
/// out: retrying is on with no config.
#[tokio::test(flavor = "multi_thread")]
async fn the_kernel_follows_the_configs_retry_setting() {
    use angzarr_embedded::OnRetryable;
    use std::time::Duration;
    let dir = TestDir::new("angzarr-host-app");
    let path = written(&dir, "store:\n  in_memory: true\n");
    let app = run(&path, nothing).await.unwrap();
    assert_eq!(app.kernel().retryable(), OnRetryable::default());
    drop(app);
    let path = written(
        &dir,
        "store:\n  in_memory: true\nretry:\n  max_retries: 5\n  max_race_runs: 4\n  \
         initial_backoff_ms: 50\n  multiplier: 3.0\n  max_backoff_ms: 1000\n  jitter: 0.5\n",
    );
    let app = run(&path, nothing).await.unwrap();
    assert_eq!(
        app.kernel().retryable(),
        OnRetryable {
            max_retries: 5,
            max_race_runs: std::num::NonZeroU32::new(4).unwrap(),
            initial_backoff: Duration::from_millis(50),
            multiplier: 3.0,
            max_backoff: Duration::from_millis(1_000),
            jitter: 0.5,
        }
    );
    drop(app);
}

/// The kernel the app hands out stores snapshots as the config's
/// `store.snapshots` says: one at every write when it is left out.
#[tokio::test(flavor = "multi_thread")]
async fn the_kernel_follows_the_configs_snapshot_setting() {
    use angzarr_embedded::SnapshotPolicy;
    use std::num::NonZeroU64;
    let dir = TestDir::new("angzarr-host-app");
    let path = written(&dir, "store:\n  in_memory: true\n");
    let app = run(&path, nothing).await.unwrap();
    assert_eq!(app.kernel().snapshots(), &SnapshotPolicy::default());
    drop(app);
    let path = written(
        &dir,
        "store:\n  in_memory: true\n  snapshots:\n    every: 3\n    domain:\n      \
         table:\n        every: 7\n",
    );
    let app = run(&path, nothing).await.unwrap();
    let n = |events: u64| NonZeroU64::new(events).unwrap();
    assert_eq!(
        app.kernel().snapshots(),
        &SnapshotPolicy::every(n(3)).domain("table", n(7))
    );
    drop(app);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_config_is_unreadable() {
    let dir = TestDir::new("angzarr-host-app");
    let path = dir.join("angzarr.yaml");
    assert_eq!(
        run(&path, nothing).await.err(),
        Some(HostError::ConfigUnreadable {
            path: path.display().to_string(),
            kind: "NotFound".to_string(),
        })
    );
}

/// A config that does not load is invalid, its cause the reader's error:
/// an unknown key at its key path, YAML the reader does not take (the old
/// TOML among it) and a file of a newer version.
#[tokio::test(flavor = "multi_thread")]
async fn an_invalid_config_is_refused_where_it_goes_wrong() {
    let dir = TestDir::new("angzarr-host-app");
    let path = written(&dir, "store:\n  in_memory: true\n  lanuch: 1\n");
    let refused = run(&path, nothing).await.err();
    assert!(
        matches!(
            &refused,
            Some(HostError::ConfigInvalid { path: at, cause })
                if *at == path.display().to_string()
                    && cause.to_string().starts_with("angzarr-yaml-load-failed-at ")
                    && cause.to_string().contains("problem.key_path=store.lanuch")
        ),
        "{refused:?}"
    );
    for text in ["[[domain]\n", "schema_version: 5\n"] {
        std::fs::write(&path, text).unwrap();
        assert!(
            matches!(
                run(&path, nothing).await.err(),
                Some(HostError::ConfigInvalid { .. })
            ),
            "{text:?}"
        );
    }
}

/// A config in hand runs as the same config read from its file does.
#[tokio::test(flavor = "multi_thread")]
async fn a_config_in_hand_runs_as_its_file_does() {
    use angzarr_embedded::OnPolicyBug;
    let dir = TestDir::new("angzarr-host-app");
    let path = written(&dir, "store:\n  in_memory: true\npolicy:\n  on_bug: stay\n");
    let text = std::fs::read_to_string(&path).unwrap();
    let config = crate::config::parse(&text, "angzarr.yaml").unwrap();
    let from_file = run(&path, nothing).await.unwrap();
    let in_hand = run_config(&config, nothing).await.unwrap();
    assert_eq!(in_hand.kernel().policy_bug(), OnPolicyBug::Stay);
    assert_eq!(
        in_hand.kernel().policy_bug(),
        from_file.kernel().policy_bug()
    );
    assert_eq!(in_hand.kernel().retryable(), from_file.kernel().retryable());
}

/// A component in a language the application did not load is refused
/// before anything starts, with its key path and the languages loaded; the
/// application that loads the language runs (owner ruling, 2026-10-31:
/// `language` is a closed set, populated by the languages loaded).
#[tokio::test(flavor = "multi_thread")]
async fn a_language_not_loaded_is_refused_and_one_loaded_runs() {
    use crate::config::{Language, Languages};
    let dir = TestDir::new("angzarr-host-app");
    let path = written(
        &dir,
        "component:\n  - name: table\n    kind: domain\n    language: rustacean\n\
         store:\n  in_memory: true\n",
    );
    let refused = run(&path, nothing).await.err();
    assert_eq!(
        refused,
        Some(HostError::UnknownLanguage {
            key: "component[0].language".to_string(),
            language: "rustacean".to_string(),
            loaded: "python,rust".to_string(),
        })
    );
    let languages = Languages::builtin().with(Language::in_process("rustacean"));
    let app = run_with_languages(&path, languages, |registry: &mut Registry| {
        registry.command_handler("table", NoHandler, fixtures::descriptor("table"))?;
        Ok(())
    })
    .await;
    assert!(app.is_ok(), "{:?}", app.err());
}

/// A store that cannot be opened fails the run, its cause the kernel's
/// store error; the children started are stopped.
#[tokio::test(flavor = "multi_thread")]
async fn a_store_that_cannot_open_fails_the_run() {
    let dir = TestDir::new("angzarr-host-app");
    let mut config = player_config();
    config.store.path = dir.join("no/such/dir/app.db");
    let failed = run_config(&config, nothing).await.err();
    assert!(
        matches!(&failed, Some(HostError::Store { cause }) if cause != &Report::new("")),
        "{failed:?}"
    );
}

/// A handler registering nothing useful: only its name is asked for.
struct NoHandler;

impl angzarr_embedded::angzarr_router::router::CommandHandler for NoHandler {
    fn name(&self) -> &str {
        "none"
    }

    fn command_types(&self) -> Vec<String> {
        Vec::new()
    }

    async fn dispatch(
        &self,
        _: &angzarr_embedded::angzarr_router::hpb::HandleRequest,
    ) -> Result<
        angzarr_embedded::angzarr_router::hpb::HandleResponse,
        angzarr_embedded::angzarr_router::error::CodedError,
    > {
        unreachable!("never registered")
    }
}

/// The App is shared by every task that sends it commands, and by the
/// host's listener: `Send + Sync`, and `'static`.
#[test]
fn the_app_is_shareable() {
    fn shareable<T: Send + Sync + 'static>() {}
    shareable::<App>();
}

/// In-process aggregates for the concurrency and exposure tests,
/// hand-written as the router's `CommandHandler`, and the books they take.
mod fixtures {
    use std::sync::{Condvar, Mutex};
    use std::time::Duration;

    use angzarr_embedded::angzarr_router::error::CodedError;
    use angzarr_embedded::angzarr_router::hpb::{HandleRequest, HandleResponse};
    use angzarr_embedded::angzarr_router::router::CommandHandler;
    use angzarr_embedded::v1::{
        command_page, component_descriptor, AggregateDescriptor, Command, CommandBook, CommandPage,
        ComponentDescriptor, ComponentKind, Cover, ProcessManagerDescriptor, StateDescriptor, Uuid,
    };
    use angzarr_embedded::Any;

    /// How long a dispatch waits for what a test is forcing: past it, the
    /// test fails rather than hangs.
    pub const WITHIN: Duration = Duration::from_secs(300);

    pub fn any(type_url: &str) -> Any {
        Any {
            type_url: type_url.to_string(),
            value: Vec::new(),
        }
    }

    pub fn cover(domain: &str, root: u8) -> Cover {
        Cover {
            domain: domain.to_string(),
            root: Some(Uuid::from_bytes([root; 16])),
            edition: None,
        }
    }

    /// Command `n`, of type `type_url`, to `domain`'s stream `root`.
    pub fn book(domain: &str, root: u8, n: u8, type_url: &str) -> CommandBook {
        CommandBook {
            cover: Some(cover(domain, root)),
            page: Some(CommandPage {
                body: Some(command_page::Body::Command(Command {
                    payload: Some(any(type_url)),
                    handling: None,
                })),
                command_id: Some(Uuid::from_bytes([n; 16])),
                ..CommandPage::default()
            }),
        }
    }

    /// The build version every fixture aggregate is described at and
    /// answers with.
    pub const VERSION: &str = "1.0.0";

    /// An aggregate's descriptor, as angzarr-build generates it.
    pub fn descriptor(name: &str) -> ComponentDescriptor {
        descriptor_of(name, ComponentKind::Aggregate, &[])
    }

    /// A domain's descriptor of `kind` (an aggregate, or a process manager
    /// started by `starting`), as angzarr-build generates it.
    pub fn descriptor_of(
        name: &str,
        kind: ComponentKind,
        starting: &[&str],
    ) -> ComponentDescriptor {
        let state = Some(StateDescriptor {
            type_name: format!("test.{name}.v1.State"),
        });
        let domain = name.to_string();
        let kind = match kind {
            ComponentKind::ProcessManager => {
                component_descriptor::Kind::ProcessManager(ProcessManagerDescriptor {
                    domain,
                    state,
                    starting_types: starting.iter().map(|t| t.to_string()).collect(),
                })
            }
            _ => component_descriptor::Kind::Aggregate(AggregateDescriptor { domain, state }),
        };
        ComponentDescriptor {
            name: name.to_string(),
            rules_version: VERSION.to_string(),
            kind: Some(kind),
        }
    }

    /// One event, `type_url`.
    fn decided(type_url: &str, rules_version: &str) -> HandleResponse {
        HandleResponse {
            events: vec![any(type_url)],
            state: None,
            rules_version: rules_version.to_string(),
        }
    }

    pub const MET: &str = "/test.meet.v1.Met";
    pub const ALONE: &str = "/test.meet.v1.Alone";

    /// Where `parties` dispatches meet: each waits, up to [`WITHIN`], for
    /// all of them to be inside their handlers at once.
    pub struct Rendezvous {
        inside: Mutex<usize>,
        all: Condvar,
        parties: usize,
    }

    impl Rendezvous {
        pub fn of(parties: usize) -> Rendezvous {
            Rendezvous {
                inside: Mutex::new(0),
                all: Condvar::new(),
                parties,
            }
        }

        /// Whether every party arrived within [`WITHIN`].
        fn meet(&self) -> bool {
            let mut inside = self.inside.lock().unwrap();
            *inside += 1;
            self.all.notify_all();
            let (inside, waited) = self
                .all
                .wait_timeout_while(inside, WITHIN, |n| *n < self.parties)
                .unwrap();
            drop(inside);
            !waited.timed_out()
        }
    }

    /// An aggregate whose every command waits at the rendezvous: it
    /// decides [`MET`] when every party arrived while it waited, [`ALONE`]
    /// when it waited out [`WITHIN`] alone.
    pub struct Meeting(pub std::sync::Arc<Rendezvous>);

    impl CommandHandler for Meeting {
        fn name(&self) -> &str {
            "Meeting"
        }

        fn command_types(&self) -> Vec<String> {
            vec!["test.meet.v1.Meet".to_string()]
        }

        async fn dispatch(&self, _: &HandleRequest) -> Result<HandleResponse, CodedError> {
            let met = self.0.meet();
            Ok(decided(if met { MET } else { ALONE }, VERSION))
        }
    }
}

/// The App's async API, shared (red, design-by-test, 2026-10-30): commands
/// for different streams run at once, forced by handlers that can only
/// finish together. One stream's commands race optimistically, as the
/// kernel's tests show (`src/concurrency.test.rs` in angzarr-embedded).
mod concurrency {
    use std::sync::Arc;

    use super::fixtures::*;
    use super::*;

    /// A config listing the in-process aggregates `domains`, and an
    /// in-memory store.
    fn in_memory(dir: &Path, domains: &[&str]) -> PathBuf {
        let mut text = String::from("component:\n");
        for domain in domains {
            text.push_str(&format!(
                "  - name: {domain}\n    kind: domain\n    language: rust\n"
            ));
        }
        text.push_str("store:\n  in_memory: true\n");
        written(dir, &text)
    }

    /// The payloads of `cover`'s stream.
    async fn payloads(app: &App, cover: &Cover) -> Vec<String> {
        app.events(cover)
            .await
            .unwrap()
            .into_iter()
            .map(|p| {
                angzarr_embedded::angzarr_router::page_event(&p)
                    .map(|a| a.type_url.clone())
                    .unwrap_or_default()
            })
            .collect()
    }

    /// Two aggregates, two domains: a command to each, sent from two tasks
    /// at once. Their handlers can finish only once both are inside at
    /// the same time, so both deciding [`MET`] is the overlap; a host that
    /// handled one after the other would leave the first [`ALONE`].
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_aggregates_handle_commands_concurrently() {
        let dir = TestDir::new("angzarr-host-app");
        let rendezvous = Arc::new(Rendezvous::of(2));
        let meeting = rendezvous.clone();
        let app = run(
            in_memory(&dir, &["left", "right"]),
            move |registry: &mut Registry| {
                for domain in ["left", "right"] {
                    registry.command_handler(
                        domain,
                        Meeting(meeting.clone()),
                        descriptor(domain),
                    )?;
                }
                Ok(())
            },
        )
        .await
        .expect("the app runs");
        let app = Arc::new(app);
        let send = |domain: &'static str, n: u8| {
            let app = app.clone();
            tokio::spawn(async move {
                app.handle(book(domain, 1, n, "/test.meet.v1.Meet"), &sampled(n))
                    .await
            })
        };
        let (left, right) = (send("left", 1), send("right", 2));
        let (left, right) = (left.await.unwrap(), right.await.unwrap());
        assert!(matches!(left, Ok(Handled::Applied(_))), "{:?}", left.err());
        assert!(
            matches!(right, Ok(Handled::Applied(_))),
            "{:?}",
            right.err()
        );
        assert_eq!(payloads(&app, &fixtures::cover("left", 1)).await, [MET]);
        assert_eq!(payloads(&app, &fixtures::cover("right", 1)).await, [MET]);
    }

    /// Two streams of ONE aggregate are as independent as two aggregates'
    /// streams: nothing orders streams against each other, a domain's
    /// included.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_streams_of_one_aggregate_handle_commands_concurrently() {
        let dir = TestDir::new("angzarr-host-app");
        let rendezvous = Arc::new(Rendezvous::of(2));
        let meeting = rendezvous.clone();
        let app = run(
            in_memory(&dir, &["left"]),
            move |registry: &mut Registry| {
                registry.command_handler("left", Meeting(meeting), descriptor("left"))?;
                Ok(())
            },
        )
        .await
        .expect("the app runs");
        let app = Arc::new(app);
        let send = |root: u8| {
            let app = app.clone();
            tokio::spawn(async move {
                app.handle(
                    book("left", root, root, "/test.meet.v1.Meet"),
                    &sampled(root),
                )
                .await
            })
        };
        let (one, two) = (send(1), send(2));
        let (one, two) = (one.await.unwrap(), two.await.unwrap());
        assert!(matches!(one, Ok(Handled::Applied(_))), "{:?}", one.err());
        assert!(matches!(two, Ok(Handled::Applied(_))), "{:?}", two.err());
        assert_eq!(payloads(&app, &fixtures::cover("left", 1)).await, [MET]);
        assert_eq!(payloads(&app, &fixtures::cover("left", 2)).await, [MET]);
    }
}

/// The exposure seam (red, design-by-test, 2026-10-30): what the host's
/// listener calls for each request, [`App::serve`]. A domain is served
/// only to an arrival its `component` entry's `expose` admits; the answer is
/// `CommandHandlerCoordinatorService.Handle`'s.
mod exposure {
    use angzarr_embedded::v1::DeliveryOutcome;
    use angzarr_embedded::{KernelError, Malformed};

    use super::fixtures::*;
    use super::*;
    use crate::config::{ComponentConfig, Expose};
    use crate::exposure::Arrival;

    /// An in-process (Rust) aggregate's `component` entry, exposed `expose`.
    fn aggregate(name: &str, expose: Expose) -> ComponentConfig {
        ComponentConfig {
            name: name.to_string(),
            language: "rust".to_string(),
            kind: crate::config::ComponentKindConfig::Domain,
            upcasts: None,
            launch: None,
            entry: Vec::new(),
            dir: None,
            env: Default::default(),
            expose,
            scaffold: Default::default(),
        }
    }

    /// `hidden` (exposed none), `local` (loopback) and `open` (ip), all
    /// in-process; each answers a command with one `Gated` event.
    async fn app() -> App {
        let mut config = HostConfig {
            components: vec![
                aggregate("hidden", Expose::None),
                aggregate("local", Expose::Loopback),
                aggregate("open", Expose::Ip),
            ],
            ..HostConfig::default()
        };
        config.store.in_memory = true;
        run_config(&config, |registry: &mut Registry| {
            for domain in ["hidden", "local", "open"] {
                registry.command_handler(
                    domain,
                    Meeting(std::sync::Arc::new(Rendezvous::of(1))),
                    descriptor(domain),
                )?;
            }
            Ok(())
        })
        .await
        .expect("the app runs")
    }

    fn meet(domain: &str) -> CommandBook {
        book(domain, 1, 1, "/test.meet.v1.Meet")
    }

    /// The kernel's refusal of `domain` as unknown: what a caller gets for
    /// a domain that is not registered, and for one not exposed to it.
    fn unknown(domain: &str) -> Failure<Rejected> {
        Failure::Kernel(KernelError::Malformed(Malformed::UnknownDomain {
            domain: domain.to_string(),
        }))
    }

    /// A request is served when its domain is exposed to where it arrived
    /// from: loopback reaches `"loopback"` and `"ip"`, the network only
    /// `"ip"`. The answer is the stream's events, APPLIED.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_exposed_domain_is_served_to_the_arrivals_its_exposure_admits() {
        let app = app().await;
        for (domain, arrived) in [
            ("local", Arrival::Loopback),
            ("open", Arrival::Loopback),
            ("open", Arrival::Ip),
        ] {
            let mut book = meet(domain);
            if arrived == Arrival::Ip {
                book.cover = Some(fixtures::cover(domain, 2));
            }
            let response = app
                .serve(book.clone(), arrived, &sampled(1))
                .await
                .unwrap_or_else(|e| panic!("{domain} from {arrived:?}: {e:?}"));
            let events = response.events.expect("the stream's events");
            assert_eq!(events.cover, book.cover, "{domain} from {arrived:?}");
            let payloads: Vec<_> = events
                .pages
                .iter()
                .map(|p| {
                    angzarr_embedded::angzarr_router::page_event(p)
                        .map(|a| a.type_url.clone())
                        .unwrap_or_default()
                })
                .collect();
            assert_eq!(payloads, [MET], "{domain} from {arrived:?}");
            assert_eq!(
                response.delivery,
                DeliveryOutcome::Applied,
                "{domain} from {arrived:?}"
            );
        }
    }

    /// A request for a domain not exposed to where it arrived from is
    /// refused exactly as an unknown domain is: the caller cannot tell a
    /// hidden domain from a missing one (the owner's ruling, 2026-10-30).
    /// The real reason goes back beside the answer, for the listener's own
    /// log: the host writes to no terminal. It reaches no handler: nothing
    /// is written.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_domain_not_exposed_to_the_arrival_is_refused_as_unknown() {
        let app = app().await;
        for (domain, arrived, expose) in [
            ("hidden", Arrival::Loopback, Expose::None),
            ("hidden", Arrival::Ip, Expose::None),
            ("local", Arrival::Ip, Expose::Loopback),
        ] {
            assert_eq!(
                app.serve(meet(domain), arrived, &sampled(1)).await.err(),
                Some(Unserved::Hidden {
                    answer: unknown(domain),
                    hidden: NotExposed {
                        domain: domain.to_string(),
                        arrival: arrived,
                        expose,
                    },
                }),
                "{domain} from {arrived:?}"
            );
            assert!(
                app.events(&fixtures::cover(domain, 1))
                    .await
                    .unwrap()
                    .is_empty(),
                "{domain}: nothing written"
            );
        }
    }

    /// A domain that is not registered at all is refused as the kernel
    /// refuses it for a local caller: the unknown domain, whatever the
    /// arrival.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unknown_domain_is_the_kernels_unknown_domain() {
        let app = app().await;
        for arrived in [Arrival::Loopback, Arrival::Ip] {
            let unserved = app.serve(meet("nowhere"), arrived, &sampled(1)).await.err();
            assert_eq!(
                unserved.as_ref().map(Unserved::answer),
                Some(&unknown("nowhere")),
                "from {arrived:?}"
            );
            assert_eq!(
                unserved.as_ref().and_then(Unserved::hidden),
                Some(&NotExposed {
                    domain: "nowhere".to_string(),
                    arrival: arrived,
                    expose: Expose::None,
                }),
                "the reason, for the listener's own log"
            );
        }
    }
}

/// What the exposure seam answers beyond the owner's cases: an exposed
/// process manager is refused at run, and a decision with no events is
/// answered with the addressed stream, empty.
mod exposure_edges {
    use angzarr_embedded::angzarr_router::error::CodedError;
    use angzarr_embedded::angzarr_router::hpb::{HandleRequest, HandleResponse};
    use angzarr_embedded::angzarr_router::router::CommandHandler;
    use angzarr_embedded::v1::{ComponentKind, DeliveryOutcome, EventBook};

    use super::fixtures::*;
    use super::*;
    use crate::config::{ComponentConfig, Expose};
    use crate::exposure::Arrival;

    /// An in-process component's entry, exposed `expose`.
    fn in_process(name: &str, expose: Expose) -> ComponentConfig {
        ComponentConfig {
            name: name.to_string(),
            language: "rust".to_string(),
            kind: crate::config::ComponentKindConfig::Domain,
            upcasts: None,
            launch: None,
            entry: Vec::new(),
            dir: None,
            env: Default::default(),
            expose,
            scaffold: Default::default(),
        }
    }

    /// A domain answering every input with no events, of `kind`.
    struct Quiet(ComponentKind);

    impl CommandHandler for Quiet {
        fn name(&self) -> &str {
            "Quiet"
        }

        fn kind(&self) -> ComponentKind {
            self.0
        }

        fn command_types(&self) -> Vec<String> {
            vec!["test.meet.v1.Meet".to_string()]
        }

        async fn dispatch(&self, _: &HandleRequest) -> Result<HandleResponse, CodedError> {
            Ok(HandleResponse {
                rules_version: VERSION.to_string(),
                ..HandleResponse::default()
            })
        }
    }

    /// `quiet`, registered as `kind`, its component (named as decision
    /// 14.50 names it by default) exposed `expose`.
    async fn quiet(kind: ComponentKind, expose: Expose) -> Result<App, HostError> {
        let name = match kind {
            ComponentKind::ProcessManager => "pmg-quiet",
            _ => "quiet",
        };
        quiet_as(name, kind, expose).await
    }

    /// `quiet`, registered as `kind`, listed as component `name`, exposed
    /// `expose`.
    async fn quiet_as(name: &str, kind: ComponentKind, expose: Expose) -> Result<App, HostError> {
        let mut config = HostConfig {
            components: vec![in_process(name, expose)],
            ..HostConfig::default()
        };
        config.store.in_memory = true;
        run_config(&config, move |registry: &mut Registry| {
            let described = descriptor_of("quiet", kind, &["test.meet.v1.Meet"]);
            registry.command_handler("quiet", Quiet(kind), described)?;
            Ok(())
        })
        .await
    }

    /// Only an aggregate is served through the listener: a domain exposed
    /// but registered as a process manager is refused at run, by its
    /// descriptor's kind; one not exposed runs.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_exposed_process_manager_is_refused_at_run() {
        assert_eq!(
            quiet(ComponentKind::ProcessManager, Expose::Loopback)
                .await
                .err(),
            Some(HostError::NotExposable {
                domain: "pmg-quiet".to_string(),
                kind: "process_manager".to_string(),
            })
        );
        let hidden = quiet(ComponentKind::ProcessManager, Expose::None).await;
        assert!(hidden.is_ok(), "{:?}", hidden.err());
        let aggregate = quiet(ComponentKind::Aggregate, Expose::Loopback).await;
        assert!(aggregate.is_ok(), "{:?}", aggregate.err());
    }

    /// An in-process process manager's `component` entry names it by its
    /// component name (decision 14.50: `pmg-` and its domain), not by its
    /// domain: listed by its domain it is refused as listed and not
    /// registered.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_process_manager_is_listed_by_its_component_name() {
        let named = quiet_as("pmg-quiet", ComponentKind::ProcessManager, Expose::None).await;
        assert!(named.is_ok(), "{:?}", named.err());
        assert_eq!(
            quiet_as("quiet", ComponentKind::ProcessManager, Expose::None)
                .await
                .err(),
            Some(HostError::NotRegistered {
                domain: "quiet".to_string(),
                kind: "domain".to_string(),
            })
        );
        assert_eq!(
            quiet_as("pmg-quiet", ComponentKind::Aggregate, Expose::None)
                .await
                .err(),
            Some(HostError::NotRegistered {
                domain: "pmg-quiet".to_string(),
                kind: "domain".to_string(),
            })
        );
    }

    /// A decision with no events is answered APPLIED, with the addressed
    /// stream's book, empty.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_decision_with_no_events_answers_the_addressed_stream_empty() {
        let app = quiet(ComponentKind::Aggregate, Expose::Ip)
            .await
            .expect("the app runs");
        let book = book("quiet", 1, 1, "/test.meet.v1.Meet");
        let response = app
            .serve(book.clone(), Arrival::Ip, &sampled(1))
            .await
            .expect("served");
        assert_eq!(
            response.events,
            Some(EventBook {
                cover: book.cover,
                ..EventBook::default()
            })
        );
        assert_eq!(response.delivery, DeliveryOutcome::Applied);
    }
}

/// Every component is listed (owner ruling, 2026-10-31): an in-process
/// component's entry names what the application's `register`
/// registers, and `run` holds the two to each other, by name and kind. A
/// listed one nobody registered is refused, and so is one registered and
/// listed nowhere; the children started are stopped.
mod listing {
    use angzarr_embedded::angzarr_router::error::CodedError;
    use angzarr_embedded::angzarr_router::hpb::{ProjectRequest, ProjectResponse};
    use angzarr_embedded::angzarr_router::policy::PolicyDispatch;
    use angzarr_embedded::angzarr_router::router::ProjectorHandler;

    use super::fixtures::*;
    use super::*;
    use crate::config::{ComponentConfig, ComponentKindConfig, Expose};

    /// An in-process `component` entry of `kind`.
    fn listed(name: &str, kind: ComponentKindConfig) -> ComponentConfig {
        ComponentConfig {
            name: name.to_string(),
            language: "rust".to_string(),
            kind,
            upcasts: None,
            launch: None,
            entry: Vec::new(),
            dir: None,
            env: Default::default(),
            expose: Expose::None,
            scaffold: Default::default(),
        }
    }

    /// A projector that projects nothing.
    struct Still;

    impl ProjectorHandler for Still {
        fn name(&self) -> &str {
            "left-view"
        }

        async fn dispatch(&self, _: &ProjectRequest) -> Result<ProjectResponse, CodedError> {
            Ok(ProjectResponse::default())
        }
    }

    /// What `register` registers: each of `left` and `right` as a domain,
    /// `left-right` as a policy, `left-view` as a projector, as `with`
    /// says.
    #[derive(Clone, Copy, Default)]
    struct With {
        right: bool,
        policy: bool,
        projector: bool,
    }

    /// `components` and an in-memory store, run with `left` registered
    /// and what `with` adds.
    async fn ran(components: Vec<ComponentConfig>, with: With) -> Result<App, HostError> {
        let mut config = HostConfig {
            components,
            ..HostConfig::default()
        };
        config.store.in_memory = true;
        run_config(&config, move |registry: &mut Registry| {
            let rendezvous = || Meeting(std::sync::Arc::new(Rendezvous::of(1)));
            registry.command_handler("left", rendezvous(), descriptor("left"))?;
            if with.right {
                registry.command_handler("right", rendezvous(), descriptor("right"))?;
            }
            if with.policy {
                registry.policy_dispatch(PolicyDispatch::new("left-right", "left", "right"))?;
            }
            if with.projector {
                registry.projector_handler(&["left"], Still)?;
            }
            Ok(())
        })
        .await
    }

    /// Every kind listed and registered in process runs.
    #[tokio::test(flavor = "multi_thread")]
    async fn every_listed_in_process_component_registered_runs() {
        let mut upcaster = listed("left-upcaster", ComponentKindConfig::Upcaster);
        upcaster.upcasts = Some("left".to_string());
        let app = ran(
            vec![
                listed("left", ComponentKindConfig::Domain),
                listed("right", ComponentKindConfig::Domain),
                listed("left-right", ComponentKindConfig::Policy),
                listed("left-view", ComponentKindConfig::Projector),
                upcaster,
            ],
            With {
                right: true,
                policy: true,
                projector: true,
            },
        )
        .await;
        assert!(app.is_ok(), "{:?}", app.err());
    }

    /// A listed in-process component nobody registered, of each kind, is
    /// refused by its name and kind.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_listed_component_nobody_registered_is_refused() {
        let left = || listed("left", ComponentKindConfig::Domain);
        for (component, kind) in [
            (listed("right", ComponentKindConfig::Domain), "domain"),
            (listed("left-right", ComponentKindConfig::Policy), "policy"),
            (
                listed("left-view", ComponentKindConfig::Projector),
                "projector",
            ),
        ] {
            let name = component.name.clone();
            assert_eq!(
                ran(vec![left(), component], With::default()).await.err(),
                Some(HostError::NotRegistered {
                    domain: name,
                    kind: kind.to_string(),
                }),
                "{kind}"
            );
        }
    }

    /// An in-process upcaster's domain is one registered here; one naming
    /// a domain nobody registered is refused, and one naming none is
    /// refused as an upcaster with no domain.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_in_process_upcaster_names_a_registered_domain() {
        let left = || listed("left", ComponentKindConfig::Domain);
        let mut nowhere = listed("upcaster", ComponentKindConfig::Upcaster);
        nowhere.upcasts = Some("nowhere".to_string());
        assert_eq!(
            ran(vec![left(), nowhere], With::default()).await.err(),
            Some(HostError::NotRegistered {
                domain: "upcaster".to_string(),
                kind: "upcaster".to_string(),
            })
        );
        let none = listed("upcaster", ComponentKindConfig::Upcaster);
        assert_eq!(
            ran(vec![left(), none], With::default()).await.err(),
            Some(HostError::UpcasterNoDomain {
                domain: "upcaster".to_string(),
            })
        );
    }

    /// A component registered in process that no `component` entry lists,
    /// of each kind, is refused by its name and kind.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_registered_component_listed_nowhere_is_refused() {
        assert_eq!(
            ran(Vec::new(), With::default()).await.err(),
            Some(HostError::NotListed {
                domain: "left".to_string(),
                kind: "domain".to_string(),
            })
        );
        let domains = || {
            vec![
                listed("left", ComponentKindConfig::Domain),
                listed("right", ComponentKindConfig::Domain),
            ]
        };
        assert_eq!(
            ran(
                domains(),
                With {
                    right: true,
                    policy: true,
                    ..With::default()
                }
            )
            .await
            .err(),
            Some(HostError::NotListed {
                domain: "left-right".to_string(),
                kind: "policy".to_string(),
            })
        );
        assert_eq!(
            ran(
                domains(),
                With {
                    right: true,
                    projector: true,
                    ..With::default()
                }
            )
            .await
            .err(),
            Some(HostError::NotListed {
                domain: "left-view".to_string(),
                kind: "projector".to_string(),
            })
        );
    }

    /// A policy whose name a listed domain has is listed nowhere as a
    /// policy: names are held to their kind.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_listing_of_another_kind_does_not_list_a_component() {
        let mut config = HostConfig {
            components: vec![
                listed("left", ComponentKindConfig::Domain),
                listed("right", ComponentKindConfig::Domain),
            ],
            ..HostConfig::default()
        };
        config.store.in_memory = true;
        let refused = run_config(&config, |registry: &mut Registry| {
            for domain in ["left", "right"] {
                registry.command_handler(
                    domain,
                    Meeting(std::sync::Arc::new(Rendezvous::of(1))),
                    descriptor(domain),
                )?;
            }
            registry.policy_dispatch(PolicyDispatch::new("right", "left", "right"))?;
            Ok(())
        })
        .await
        .err();
        assert_eq!(
            refused,
            Some(HostError::NotListed {
                domain: "right".to_string(),
                kind: "policy".to_string(),
            })
        );
    }

    /// A component listed as one kind and registered as another is both:
    /// refused as the listed kind nobody registered.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_component_registered_as_another_kind_is_refused() {
        assert_eq!(
            ran(
                vec![listed("left", ComponentKindConfig::Policy)],
                With::default()
            )
            .await
            .err(),
            Some(HostError::NotRegistered {
                domain: "left".to_string(),
                kind: "policy".to_string(),
            })
        );
    }
}

/// A4: the application resumes, at start, the policy work owed when it
/// last stopped. The crash is forced, not waited for: a first process
/// commits `left`'s event and stops before its policy step runs (its
/// registry has no policy); the next starts with the policy registered and,
/// before any command, has run the step `left`'s event owes into `right`.
mod resume {
    use angzarr_embedded::angzarr_router::policy::PolicyDispatch;
    use angzarr_embedded::{EventStore, MemoryDeadLetters, StreamRef};

    use super::fixtures::*;
    use super::*;
    use crate::config::{ComponentConfig, ComponentKindConfig, Expose};

    fn listed(name: &str, kind: ComponentKindConfig) -> ComponentConfig {
        ComponentConfig {
            name: name.to_string(),
            language: "rust".to_string(),
            kind,
            upcasts: None,
            launch: None,
            entry: Vec::new(),
            dir: None,
            env: Default::default(),
            expose: Expose::None,
            scaffold: Default::default(),
        }
    }

    /// `left` as a domain whose every command decides [`MET`].
    fn left(registry: &mut Registry) {
        registry
            .command_handler(
                "left",
                Meeting(std::sync::Arc::new(Rendezvous::of(1))),
                descriptor("left"),
            )
            .unwrap();
    }

    /// `left-right`: every [`MET`] of `left` sends a Meet to `right`'s
    /// stream of the same root.
    fn left_right() -> PolicyDispatch {
        PolicyDispatch::new("left-right", "left", "right").on_event(
            MET.trim_start_matches('/'),
            |_, source| {
                Ok(vec![angzarr_embedded::angzarr_router::policy::command(
                    source.cover.and_then(|c| c.root.clone()),
                    any("/test.meet.v1.Meet"),
                )])
            },
        )
    }

    /// A resume is sampled at the wall clock, its id a UUIDv7 of the same
    /// instant (version 7, the RFC 9562 variant, the instant's
    /// milliseconds first).
    #[test]
    fn a_resume_is_sampled_at_the_wall_clock() {
        let before = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        let sampled = super::super::started_at();
        let after = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        let at = std::time::Duration::new(sampled.now.seconds as u64, sampled.now.nanos as u32);
        assert!(before <= at && at <= after, "{before:?} {at:?} {after:?}");
        let id = sampled.command_id.value.to_vec();
        assert_eq!(id.len(), 16);
        let mut ms = [0u8; 8];
        ms[2..].copy_from_slice(&id[..6]);
        assert_eq!(u64::from_be_bytes(ms), at.as_millis() as u64);
        assert_eq!(id[6], 0x70, "version 7");
        assert_eq!(id[8], 0x80, "the RFC 9562 variant");
        assert!(id[7] == 0 && id[9..].iter().all(|b| *b == 0), "{id:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn policy_work_owed_at_a_crash_is_resumed_at_start() {
        let dir = TestDir::new("angzarr-host-app");
        let db = dir.join("app.db");

        // The first process: `left`'s event committed, its step never run.
        {
            let store = SqliteStore::open(&db).await.unwrap();
            let mut registry = Registry::new();
            left(&mut registry);
            let log = MemoryDeadLetters::new();
            Kernel::new(&registry)
                .log_with(&log)
                .handle_any(
                    &store,
                    book("left", 1, 1, "/test.meet.v1.Meet"),
                    &sampled(1),
                )
                .await
                .expect("left's event committed");
            let right_cover = cover("right", 1);
            let right = StreamRef::try_from(&right_cover).unwrap();
            assert!(
                store.read(right, 0, None).await.unwrap().pages.is_empty(),
                "the step is owed"
            );
        }

        let mut config = HostConfig {
            components: vec![
                listed("left", ComponentKindConfig::Domain),
                listed("right", ComponentKindConfig::Domain),
                listed("left-right", ComponentKindConfig::Policy),
            ],
            ..HostConfig::default()
        };
        config.store.path = db.clone();
        let app = run_config(&config, |registry: &mut Registry| {
            left(registry);
            registry.command_handler(
                "right",
                Meeting(std::sync::Arc::new(Rendezvous::of(1))),
                descriptor("right"),
            )?;
            registry.policy_dispatch(left_right())?;
            Ok(())
        })
        .await
        .expect("the app runs");
        let right: Vec<_> = app
            .events(&cover("right", 1))
            .await
            .unwrap()
            .into_iter()
            .map(|p| angzarr_embedded::angzarr_router::page_event(&p).cloned())
            .collect();
        assert_eq!(right, [Some(any(MET))], "resumed before any command");
        drop(app);
    }
}

/// D1 (owner Q5, review K6): an [`App`] from its parts, on any store or a
/// store per key, its dead letters kept by the store and answered, its
/// projectors run on every call, and its stores resumed at start.
mod parts {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use angzarr_embedded::angzarr_ports::memory::MemoryStore;
    use angzarr_embedded::angzarr_router::error::CodedError;
    use angzarr_embedded::angzarr_router::policy::PolicyDispatch;
    use angzarr_embedded::{DeadLetterStore, MemoryDeadLetters, ProjectorHandler, StreamRef};

    use super::fixtures::*;
    use super::*;
    use crate::stores::{OneStore, Stores};

    /// `name` as a domain whose every command decides [`MET`].
    fn meeting(registry: &mut Registry, name: &str) {
        registry
            .command_handler(name, Meeting(Arc::new(Rendezvous::of(1))), descriptor(name))
            .unwrap();
    }

    fn meet(domain: &str, n: u8) -> CommandBook {
        book(domain, 1, n, "/test.meet.v1.Meet")
    }

    /// The payloads of `cover`'s stream in `store`.
    async fn payloads(store: &MemoryStore, cover: &Cover) -> Vec<String> {
        let stream = StreamRef::try_from(cover).unwrap();
        store
            .read(stream, 0, None)
            .await
            .unwrap()
            .pages
            .into_iter()
            .map(|p| {
                angzarr_embedded::angzarr_router::page_event(&p)
                    .map(|a| a.type_url.clone())
                    .unwrap_or_default()
            })
            .collect()
    }

    /// An app from its parts runs on the store it is given, a MemoryStore
    /// here: no SQLite, no config.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_app_from_parts_runs_on_the_store_it_is_given() {
        let mut registry = Registry::new();
        meeting(&mut registry, "left");
        let app = App::builder(OneStore::new(MemoryStore::new()), registry)
            .start()
            .await;
        let handled = app.handle(meet("left", 1), &sampled(1)).await;
        assert!(
            matches!(handled, Ok(Handled::Applied(_))),
            "{:?}",
            handled.err()
        );
        assert_eq!(payloads(app.store(), &cover("left", 1)).await, [MET]);
        assert!(app.resumed().is_empty());
    }

    /// A domain answering every command with [`MET`] and its state, the
    /// state its descriptor registers.
    struct Stated;

    impl angzarr_embedded::angzarr_router::router::CommandHandler for Stated {
        fn name(&self) -> &str {
            "stated"
        }

        fn command_types(&self) -> Vec<String> {
            Vec::new()
        }

        async fn dispatch(
            &self,
            _: &angzarr_embedded::angzarr_router::hpb::HandleRequest,
        ) -> Result<angzarr_embedded::angzarr_router::hpb::HandleResponse, CodedError> {
            Ok(angzarr_embedded::angzarr_router::hpb::HandleResponse {
                events: vec![any(MET)],
                state: Some(any("/test.stated.v1.State")),
                rules_version: VERSION.to_string(),
            })
        }
    }

    /// The app's kernel stores the snapshots its domains offer as the
    /// policy it is given says: here every second event of `stated`.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_app_stores_snapshots_as_its_snapshot_policy_says() {
        let mut registry = Registry::new();
        registry
            .command_handler("stated", Stated, descriptor("stated"))
            .unwrap();
        let every_second = std::num::NonZeroU64::new(2).unwrap();
        let app = App::builder(OneStore::new(MemoryStore::new()), registry)
            .snapshot_policy(angzarr_embedded::SnapshotPolicy::every(every_second))
            .start()
            .await;
        let stream = cover("stated", 1);
        let mut stored = Vec::new();
        for n in 1..=3 {
            app.handle(meet("stated", n), &sampled(n)).await.unwrap();
            let snapshot = angzarr_embedded::angzarr_ports::SnapshotStore::read_snapshot(
                app.store(),
                StreamRef::try_from(&stream).unwrap(),
            )
            .await
            .unwrap();
            stored.push(snapshot.map(|s| s.sequence));
        }
        assert_eq!(stored, [None, Some(1), Some(1)]);
    }

    /// One MemoryStore per project, opened by name.
    #[derive(Default)]
    struct Projects(Mutex<BTreeMap<String, Arc<MemoryStore>>>);

    impl Stores for Projects {
        type Key = String;
        type Store = Arc<MemoryStore>;

        async fn store(&self, key: &String) -> Result<Arc<MemoryStore>, StoreError> {
            let mut held = self.0.lock().unwrap();
            Ok(Arc::clone(held.entry(key.clone()).or_default()))
        }

        fn at_start(&self) -> Vec<String> {
            self.0.lock().unwrap().keys().cloned().collect()
        }
    }

    /// An app over a store per project handles each call in the store its
    /// key names, and no other.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_app_keyed_by_project_handles_each_call_in_its_projects_store() {
        let mut registry = Registry::new();
        meeting(&mut registry, "left");
        let app = App::builder(Projects::default(), registry).start().await;
        let (a, b) = ("a".to_string(), "b".to_string());
        app.handle_in(&a, meet("left", 1), &sampled(1))
            .await
            .expect("a's command");
        let in_a = app.stores().store(&a).await.unwrap();
        let in_b = app.stores().store(&b).await.unwrap();
        assert_eq!(payloads(&in_a, &cover("left", 1)).await, [MET]);
        assert!(
            payloads(&in_b, &cover("left", 1)).await.is_empty(),
            "b's untouched"
        );
        assert_eq!(app.events_in(&a, &cover("left", 1)).await.unwrap().len(), 1);
        assert!(app
            .events_in(&b, &cover("left", 1))
            .await
            .unwrap()
            .is_empty());
    }

    /// `left-right`, a policy of `left` whose every answer is a bug.
    fn buggy() -> PolicyDispatch {
        PolicyDispatch::new("left-right", "left", "right")
            .on_event(MET.trim_start_matches('/'), |_, _| {
                Err(CodedError::bug("toy-left-right-broken", []).into())
            })
    }

    /// The store keeps a policy's dead letter (it is a DeadLetterStore),
    /// the call answers it, and the log the application supplied gets its
    /// line.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_store_keeps_a_policys_dead_letter_and_the_call_answers_it() {
        let mut registry = Registry::new();
        meeting(&mut registry, "left");
        registry.policy_dispatch(buggy()).unwrap();
        let log = Arc::new(MemoryDeadLetters::new());
        let app = App::builder(OneStore::new(MemoryStore::new()), registry)
            .dead_letter_log(log.clone())
            .start()
            .await;
        let failure = app
            .handle(meet("left", 1), &sampled(1))
            .await
            .err()
            .expect("the policy's bug fails the call");
        assert_eq!(failure.dead_letters().len(), 1, "{failure:?}");
        let left = cover("left", 1);
        let kept = app
            .store()
            .dead_letters(StreamRef::try_from(&left).unwrap())
            .await
            .unwrap();
        assert_eq!(kept, failure.dead_letters(), "kept as answered");
        assert_eq!(log.lines().len(), 1, "and logged");
    }

    /// Counts the books it is handed.
    #[derive(Clone, Default)]
    struct Counted(Arc<Mutex<usize>>);

    impl ProjectorHandler for Counted {
        fn name(&self) -> &str {
            "counted"
        }

        async fn dispatch(
            &self,
            _: &angzarr_embedded::angzarr_router::hpb::ProjectRequest,
        ) -> Result<angzarr_embedded::angzarr_router::hpb::ProjectResponse, CodedError> {
            *self.0.lock().unwrap() += 1;
            Ok(Default::default())
        }
    }

    /// The registry's projectors run on every call's recorded chain.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_projectors_run_on_every_call() {
        let mut registry = Registry::new();
        meeting(&mut registry, "left");
        let counted = Counted::default();
        registry
            .projector_handler(&["left"], counted.clone())
            .unwrap();
        let app = App::builder(OneStore::new(MemoryStore::new()), registry)
            .start()
            .await;
        for n in 1..=2 {
            app.handle(meet("left", n), &sampled(n)).await.unwrap();
        }
        assert_eq!(*counted.0.lock().unwrap(), 2, "each call's step");
    }

    /// At start, each store the resolver names is resumed: the policy work
    /// it owes is run, and a resume that fails is the application's to
    /// read, by its store's key, the start going on. The crash is forced:
    /// each store's `left` event is committed by a kernel with no policy.
    #[tokio::test(flavor = "multi_thread")]
    async fn each_store_named_at_start_is_resumed_and_its_failure_kept() {
        let projects = Projects::default();
        for (key, root) in [("ok", 1), ("broken", 2)] {
            let store = projects.store(&key.to_string()).await.unwrap();
            let mut registry = Registry::new();
            meeting(&mut registry, "left");
            let log = MemoryDeadLetters::new();
            Kernel::new(&registry)
                .log_with(&log)
                .handle_any(
                    &*store,
                    book("left", root, 1, "/test.meet.v1.Meet"),
                    &sampled(1),
                )
                .await
                .expect("left's event committed, its step owed");
        }
        let mut registry = Registry::new();
        meeting(&mut registry, "left");
        meeting(&mut registry, "right");
        // Root 2's step is a bug; root 1's sends right a Meet.
        registry
            .policy_dispatch(PolicyDispatch::new("left-right", "left", "right").on_event(
                MET.trim_start_matches('/'),
                |_, source| {
                    let root = source.cover.and_then(|c| c.root.clone());
                    if root == cover("left", 2).root {
                        return Err(CodedError::bug("toy-left-right-broken", []).into());
                    }
                    Ok(vec![angzarr_embedded::angzarr_router::policy::command(
                        cover("right", 1).root,
                        any("/test.meet.v1.Meet"),
                    )])
                },
            ))
            .unwrap();
        let app = App::builder(projects, registry).start().await;
        let ok = app.events_in(&"ok".to_string(), &cover("right", 1)).await;
        assert_eq!(ok.unwrap().len(), 1, "resumed before any command");
        let resumed: Vec<&String> = app.resumed().iter().map(|(key, _)| key).collect();
        assert_eq!(resumed, ["broken"], "{:?}", app.resumed());
        assert_eq!(
            app.resumed()[0].1.dead_letters().len(),
            1,
            "its letter answered"
        );
    }
}
