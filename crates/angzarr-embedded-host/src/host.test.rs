use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use angzarr_proto::v1 as pb;
use angzarr_router::error::GrpcCode;
use angzarr_router::hpb;
use angzarr_router::router::CommandHandler;
use prost_types::Any;

use super::*;
use crate::config::ComponentConfig;
use crate::fake_child::{self, config, domain, mode};
use crate::remote::is_retryable;
use test_dir::{OnPanic, TestDir, TestFile};

/// A [`TestDir`] for a host's sockets: under the host's default socket
/// directory (`$XDG_RUNTIME_DIR`, else `/tmp`), not the temporary one, so it
/// is short enough to hold the host's socket names whatever `$TMPDIR` is.
fn for_sockets(prefix: &str) -> TestDir {
    let base = socket_dir(None, std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from))
        .expect("the default socket directory is short");
    TestDir::under(&base, prefix, OnPanic::from_env())
}

fn command(domain: &str, type_url: &str) -> hpb::HandleRequest {
    hpb::HandleRequest {
        input: Some(pb::CommandBook {
            cover: Some(pb::Cover {
                domain: domain.to_string(),
                ..Default::default()
            }),
            page: Some(pb::CommandPage {
                body: Some(pb::command_page::Body::Command(pb::Command {
                    payload: Some(Any {
                        type_url: type_url.to_string(),
                        value: Vec::new(),
                    }),
                    handling: None,
                })),
                ..Default::default()
            }),
        }),
        ..Default::default()
    }
}

/// A future run to its end on a runtime of its own (with its timer), as
/// the kernel's caller runs it: `handler.dispatch(&c).awaited()`.
trait Awaited: std::future::Future + Sized {
    fn awaited(self) -> Self::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(self)
    }
}

impl<F: std::future::Future> Awaited for F {}

fn player(mode: &str) -> ComponentConfig {
    domain("player", "player", mode)
}

fn started(domains: Vec<ComponentConfig>) -> Host {
    Host::start(config(domains)).expect("the host starts")
}

/// The fake's `pid-<n>` rules version, from its descriptor.
fn pid_of(handler: &RemoteCommandHandler) -> String {
    handler.descriptor().rules_version.clone()
}

/// Whether the process `pid-<n>` names is running (a zombie is not).
fn alive(pid: &str) -> bool {
    let pid = pid.strip_prefix("pid-").unwrap();
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
        stat.rsplit_once(") ")
            .is_some_and(|(_, rest)| !rest.starts_with('Z'))
    })
}

/// Describe's descriptor is the handler's: its name, its kind.
#[test]
fn start_describes_each_remote_domain() {
    let host = started(vec![player(mode::SERVE)]);
    let handlers = host.handlers();
    assert_eq!(handlers.len(), 1);
    let handler = &handlers[0];
    assert_eq!(handler.name(), "player");
    assert_eq!(handler.kind(), pb::ComponentKind::Aggregate);
    let descriptor = handler.descriptor();
    assert_eq!(descriptor.name, "player");
    assert_eq!(
        angzarr_embedded::kind_of(&descriptor),
        pb::ComponentKind::Aggregate
    );
    assert!(descriptor.rules_version.starts_with("pid-"));
    assert_eq!(handler.command_types(), Vec::<String>::new());
    assert_eq!(
        format!("{handler:?}"),
        "RemoteCommandHandler { domain: \"player\", .. }"
    );
    assert_eq!(host.handler("player").unwrap().name(), "player");
    assert!(host.handler("table").is_none());
    host.shutdown();
}

/// Every domain the config names, each with its own child, in order; the
/// in-process ones are not launched and get no handler.
#[test]
fn in_process_domains_are_left_to_the_caller() {
    let table = ComponentConfig {
        name: "table".to_string(),
        language: "rust".to_string(),
        kind: crate::config::ComponentKindConfig::Domain,
        upcasts: None,
        launch: None,
        entry: Vec::new(),
        dir: None,
        env: Default::default(),
        expose: Default::default(),
        scaffold: Default::default(),
    };
    let host = started(vec![
        table,
        player(mode::SERVE),
        domain("dealer", "dealer", mode::SERVE),
    ]);
    let names: Vec<String> = host
        .handlers()
        .iter()
        .map(|h| h.name().to_string())
        .collect();
    assert_eq!(names, ["player", "dealer"]);
    let (player, dealer) = (
        host.handler("player").unwrap(),
        host.handler("dealer").unwrap(),
    );
    assert_ne!(pid_of(&player), pid_of(&dealer));
    host.shutdown();
}

/// `Handle` on the wire: the command reaches the child and its answer comes
/// back whole.
#[test]
fn a_command_round_trips_through_the_child() {
    let host = started(vec![player(mode::SERVE)]);
    let handler = host.handler("player").unwrap();
    let response = handler
        .dispatch(&command("player", "/examples.blackjack.SitDown"))
        .awaited()
        .expect("dispatched");
    assert_eq!(response.events, [fake_child::noted(0)]);
    assert_eq!(response.rules_version, pid_of(&handler));
    host.shutdown();
}

/// The kernel may dispatch from inside its own tokio runtime: the call must
/// neither panic nor deadlock there.
#[test]
fn dispatch_works_inside_a_tokio_runtime() {
    let host = started(vec![player(mode::SERVE)]);
    let handler = host.handler("player").unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let response = runtime.block_on(async {
        handler
            .dispatch(&command("player", "/examples.blackjack.SitDown"))
            .await
    });
    assert!(response.is_ok(), "{response:?}");
    host.shutdown();
}

/// A child's FAILED_PRECONDITION, with its typed refusal in the details,
/// is the router's refusal.
#[test]
fn a_refusal_comes_back_as_the_routers_refusal() {
    let host = started(vec![player(mode::SERVE)]);
    let refused = host
        .handler("player")
        .unwrap()
        .dispatch(&command("player", "/examples.blackjack.Refuse"))
        .awaited()
        .expect_err("refused");
    assert_eq!(refused.grpc, GrpcCode::FailedPrecondition);
    assert_eq!(refused.code, "TABLE_FULL");
    assert_eq!(refused.extras.get("seats").map(String::as_str), Some("6"));
    assert_eq!(refused.refusal.as_deref(), Some(&fake_child::refusal()));
    assert!(!is_retryable(&refused));
    host.shutdown();
}

/// The handler start hands back reaches its child the moment start
/// returns: the supervisor, held right after telling start the child is up,
/// has published the child's connection already.
#[test]
fn a_handler_reaches_its_child_as_soon_as_start_returns() {
    let hold = Arc::new(tokio::sync::Notify::new());
    let hooks = Hooks {
        reported_up: Some(hold.clone()),
        ..Hooks::default()
    };
    let host = Host::start_with(config(vec![player(mode::SERVE)]), hooks).expect("the host starts");
    let answered = host
        .handler("player")
        .unwrap()
        .dispatch(&command("player", "/examples.blackjack.SitDown"))
        .awaited();
    hold.notify_one();
    assert!(answered.is_ok(), "{answered:?}");
    host.shutdown();
}

/// A child that dies is unreachable, which is retryable; it is restarted,
/// as a new process, and the same handler reaches it.
#[test]
fn a_dead_child_is_retryable_then_restarted() {
    let host = started(vec![player(mode::SERVE)]);
    let handler = host.handler("player").unwrap();
    let first = pid_of(&handler);
    let died = handler
        .dispatch(&command("player", "/examples.blackjack.Die"))
        .awaited()
        .expect_err("the child died mid-call");
    assert!(is_retryable(&died), "{died:?}");
    // The call reached the child and the child went away under it: not a
    // call that found no child, which would leave this one alive.
    assert_eq!(
        died.extras
            .get(angzarr_proto::v1::reason_args::CAUSE)
            .map(String::as_str),
        Some(crate::remote::causes::TRANSPORT),
        "{died:?}"
    );

    // The second child coming up is awaited, not polled for: the
    // supervisor counts each child once its client is set. The timeout is
    // a safety net for a broken restart, never the way the test passes.
    let mut generations = handler.generations();
    let restarted = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(20), generations.wait_for(|n| *n >= 2))
                .await
                .map(|seen| seen.is_ok())
        });
    assert_eq!(restarted, Ok(true), "the child was restarted");
    let answered = handler
        .dispatch(&command("player", "/examples.blackjack.SitDown"))
        .awaited()
        .expect("the restarted child answers");
    assert!(answered.rules_version.starts_with("pid-"));
    assert_ne!(answered.rules_version, first, "a new child answered");
    assert!(!alive(&first));
    host.shutdown();
}

/// A child that never answers fails the call retryably once the call
/// timeout passes, rather than blocking the caller; the child is left to
/// its supervisor.
#[test]
fn a_hung_child_times_out_retryably() {
    let mut config = config(vec![player(mode::SERVE)]);
    config.supervision.call_timeout_ms = 300;
    let host = Host::start(config).unwrap();
    let handler = host.handler("player").unwrap();
    let started = Instant::now();
    let hung = handler
        .dispatch(&command("player", "/examples.blackjack.Hang"))
        .awaited()
        .expect_err("timed out");
    assert!(started.elapsed() >= Duration::from_millis(300));
    assert!(is_retryable(&hung), "{hung:?}");
    assert_eq!(
        hung.extras
            .get(angzarr_proto::v1::reason_args::CAUSE)
            .map(String::as_str),
        Some(crate::remote::causes::TIMEOUT)
    );
    assert!(handler
        .dispatch(&command("player", "/examples.blackjack.SitDown"))
        .awaited()
        .is_ok());
    host.shutdown();
}

/// A restart that fails waits twice as long before the next, up to the
/// configured most.
#[test]
fn the_backoff_doubles_up_to_its_most() {
    let ms = Duration::from_millis;
    assert_eq!(next_backoff(ms(100), ms(5_000)), ms(200));
    assert_eq!(next_backoff(ms(3_000), ms(5_000)), ms(5_000));
    assert_eq!(next_backoff(ms(5_000), ms(5_000)), ms(5_000));
}

/// Stdout ending is not the child ending: it keeps serving, the same
/// process. The fake closes stdout before it serves its first call, so the
/// end is already there when the call is made.
#[test]
fn stdout_ending_is_not_the_child_ending() {
    let host = started(vec![player(mode::CLOSES_STDOUT)]);
    let handler = host.handler("player").unwrap();
    for _ in 0..3 {
        let response = handler
            .dispatch(&command("player", "/examples.blackjack.SitDown"))
            .awaited()
            .expect("still serving");
        assert_eq!(response.rules_version, pid_of(&handler));
    }
    assert_eq!(
        host.shutdown(),
        vec![("player".to_string(), ChildExit::Exited(Some(0)))]
    );
}

/// The default socket directory is short: `$XDG_RUNTIME_DIR` when it is
/// absolute and fits, else `/tmp`; never a deep `$TMPDIR`.
#[test]
fn the_default_socket_dir_is_short() {
    let xdg = PathBuf::from("/run/user/1000");
    assert_eq!(socket_dir(None, Some(xdg.clone())), Ok(xdg));
    assert_eq!(socket_dir(None, None), Ok(PathBuf::from("/tmp")));
    assert_eq!(
        socket_dir(None, Some(PathBuf::from("run/user/1000"))),
        Ok(PathBuf::from("/tmp"))
    );
    let deep = PathBuf::from(format!("/{}", "d".repeat(80)));
    assert_eq!(socket_dir(None, Some(deep)), Ok(PathBuf::from("/tmp")));
    let path = SocketFile::fresh(Path::new("/tmp"));
    assert!(path.as_os_str().len() <= MAX_SOCKET_PATH);
}

/// A configured directory is used as given, up to the longest that holds
/// the host's socket names, and refused past it.
#[test]
fn a_configured_socket_dir_too_long_is_refused() {
    let max = MAX_SOCKET_PATH - MAX_SOCKET_NAME;
    let fits = PathBuf::from(format!("/{}", "d".repeat(max - 1)));
    assert_eq!(socket_dir(Some(&fits), None), Ok(fits.clone()));
    let worst = fits.join(format!("angzarr-{}-{}.sock", 4_194_304, u64::MAX));
    assert!(worst.as_os_str().len() <= MAX_SOCKET_PATH);
    let long = PathBuf::from(format!("/{}", "d".repeat(max)));
    assert_eq!(
        socket_dir(Some(&long), None),
        Err(HostError::SocketDirTooLong {
            dir: long.display().to_string(),
            max_bytes: max as u64,
        })
    );
}

/// The refusal comes at start, before any child is launched.
#[test]
fn start_refuses_a_socket_dir_too_long() {
    let mut config = config(vec![player(mode::SERVE)]);
    let long = PathBuf::from(format!("/{}", "d".repeat(100)));
    config.supervision.socket_dir = Some(long.clone());
    assert_eq!(
        Host::start(config).err(),
        Some(HostError::SocketDirTooLong {
            dir: long.display().to_string(),
            max_bytes: (MAX_SOCKET_PATH - MAX_SOCKET_NAME) as u64,
        })
    );
}

/// The real Python player (client-python's harness, examples-python's
/// player bot), launched by its default: `uv run --locked angzarr-player`.
/// Ignored by default: it needs uv, the player's checkout, and its locked
/// dependencies. `ANGZARR_PLAYER_DIR` names the checkout's `player`
/// directory; `~/workspace/worktrees/examples-python--player-bot/player`
/// otherwise.
#[test]
#[ignore = "needs uv and the Python player's checkout"]
fn the_python_player_comes_up_by_its_default_launch() {
    let dir = std::env::var_os("ANGZARR_PLAYER_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var_os("HOME").unwrap())
                .join("workspace/worktrees/examples-python--player-bot/player")
        });
    let player = ComponentConfig {
        name: "player".to_string(),
        language: "python".to_string(),
        kind: crate::config::ComponentKindConfig::Domain,
        upcasts: None,
        launch: None,
        entry: vec!["angzarr-player".to_string()],
        dir: Some(dir),
        env: Default::default(),
        expose: Default::default(),
        scaffold: Default::default(),
    };
    let mut config = config(vec![player]);
    config.supervision.handshake_timeout_ms = 120_000;
    let host = Host::start(config).expect("the player comes up");
    let handler = host.handler("player").unwrap();
    assert_eq!(handler.kind(), pb::ComponentKind::Aggregate);
    assert!(!handler.name().is_empty());
    assert_eq!(
        host.shutdown(),
        vec![("player".to_string(), ChildExit::Exited(Some(0)))]
    );
}

/// A child's stdout is its own output, never read: chatter there, before
/// it serves or after, changes nothing.
#[test]
fn stdout_is_never_read() {
    let host = started(vec![player(mode::NOISY)]);
    assert!(host
        .handler("player")
        .unwrap()
        .dispatch(&command("player", "/examples.blackjack.SitDown"))
        .awaited()
        .is_ok());
    host.shutdown();
}

/// The config's launch is what runs: a program that does not exist fails
/// start by name, where the default (uv) would have been tried otherwise.
#[test]
fn the_configured_launch_is_what_runs() {
    let mut player = player(mode::SERVE);
    player.launch = Some(vec!["/nonexistent/angzarr-player".to_string()]);
    assert_eq!(
        Host::start(config(vec![player])).err(),
        Some(HostError::Spawn {
            domain: "player".to_string(),
            program: "/nonexistent/angzarr-player".to_string(),
            kind: "NotFound".to_string(),
        })
    );
}

/// A handler is named by the component its child describes, and found by
/// the domain it is configured for: the two differ (`PlayerAggregate` in
/// the domain `player`).
#[test]
fn a_handler_is_named_by_its_component_and_found_by_its_domain() {
    let host = started(vec![domain("player", "PlayerAggregate", mode::SERVE)]);
    let handler = host.handler("player").expect("found by its domain");
    assert_eq!(handler.name(), "PlayerAggregate");
    assert_eq!(handler.domain(), "player");
    assert!(host.handler("PlayerAggregate").is_none());
    assert_eq!(
        host.shutdown(),
        vec![("player".to_string(), ChildExit::Exited(Some(0)))]
    );
}

#[test]
fn a_domain_configured_twice_is_refused() {
    assert_eq!(
        Host::start(config(vec![player(mode::SERVE), player(mode::SERVE)])).err(),
        Some(HostError::DuplicateDomain {
            domain: "player".to_string(),
        })
    );
}

/// A start that fails leaves no child behind: the domains that came up
/// before the failing one are stopped.
#[test]
fn a_failed_start_stops_the_children_it_started() {
    let tag = format!("failed-start-{}", std::process::id());
    let mut config = config(vec![
        domain("dealer", "dealer", mode::SERVE),
        player(mode::EXITS),
    ]);
    for domain in &mut config.components {
        domain.env.insert(TAG_ENV.to_string(), tag.clone());
    }
    assert!(Host::start(config).is_err());
    assert_eq!(
        running_with_tag(&tag),
        0,
        "a child outlived the failed start"
    );
}

/// Shutdown closes the child's stdin, and the child exits by itself.
#[test]
fn shutdown_closes_stdin_and_the_child_exits() {
    let host = started(vec![player(mode::SERVE)]);
    let pid = pid_of(&host.handler("player").unwrap());
    let started = Instant::now();
    assert_eq!(
        host.shutdown(),
        vec![("player".to_string(), ChildExit::Exited(Some(0)))]
    );
    assert!(
        started.elapsed() < Duration::from_millis(3_000),
        "exited before the grace ran out"
    );
    assert!(!alive(&pid));
}

/// A child that ignores its stdin's end is sent SIGTERM once the grace
/// runs out, and exits on it.
#[test]
fn a_child_ignoring_stdin_eof_is_terminated_after_the_grace() {
    let mut config = config(vec![player(mode::STUBBORN)]);
    config.supervision.shutdown_grace_ms = 300;
    let host = Host::start(config).unwrap();
    let pid = pid_of(&host.handler("player").unwrap());
    assert_eq!(
        host.shutdown(),
        vec![("player".to_string(), ChildExit::Terminated)]
    );
    assert!(!alive(&pid));
}

/// A child that ignores SIGTERM too is killed once its grace runs out.
#[test]
fn a_child_ignoring_sigterm_is_killed_after_its_grace() {
    let mut config = config(vec![player(mode::DEAF)]);
    config.supervision.shutdown_grace_ms = 200;
    config.supervision.terminate_grace_ms = 300;
    let host = Host::start(config).unwrap();
    let pid = pid_of(&host.handler("player").unwrap());
    let started = Instant::now();
    assert_eq!(
        host.shutdown(),
        vec![("player".to_string(), ChildExit::Killed)]
    );
    assert!(started.elapsed() >= Duration::from_millis(500));
    assert!(!alive(&pid));
}

/// Dropping the host stops its children as shutdown does.
#[test]
fn dropping_the_host_stops_its_children() {
    let mark = TestFile::new("angzarr-host-drop", "eof");
    let mut player = player(mode::SERVE);
    player.env.insert(
        fake_child::EOF_MARK_ENV.to_string(),
        mark.display().to_string(),
    );
    let host = started(vec![player]);
    let pid = pid_of(&host.handler("player").unwrap());
    drop(host);
    assert!(!alive(&pid));
    // Stopped by its stdin's end, and waited for: not killed, as a
    // dropped runtime would have it (kill_on_drop).
    assert!(mark.exists(), "the child exited on stdin's end");
}

/// How many files `dir` holds.
fn files_in(dir: &Path) -> usize {
    std::fs::read_dir(dir).unwrap().count()
}

/// A supervisor of the fake child serving as "player", its sockets in
/// `sockets`.
fn fake_supervisor(sockets: &Path) -> Supervisor {
    let mut launch = fake_child::launch();
    Supervisor {
        domain: "player".to_string(),
        kind: ComponentKindConfig::Domain,
        launch: Launch {
            program: launch.remove(0),
            args: launch,
            env: std::collections::BTreeMap::from([
                (fake_child::MODE_ENV.to_string(), mode::SERVE.to_string()),
                (fake_child::NAME_ENV.to_string(), "player".to_string()),
            ]),
            dir: None,
        },
        supervision: config(Vec::new()).supervision,
        socket_dir: sockets.to_path_buf(),
        link: Arc::new(crate::remote::Link::default()),
        hooks: Hooks::default(),
    }
}

/// A child's socket file goes with it however its supervision ends: a
/// child brought up and dropped (its supervision cancelled, or its
/// runtime dropped, mid-way) leaves none behind.
#[test]
fn a_dropped_child_leaves_no_socket_file() {
    let sockets = for_sockets("angzarr-sockets");
    let up = fake_supervisor(&sockets)
        .come_up()
        .awaited()
        .expect("the child comes up");
    assert_eq!(files_in(&sockets), 1, "its socket");
    drop(up);
    assert_eq!(files_in(&sockets), 0);
}

/// A variable the tests set on a child to find it again.
const TAG_ENV: &str = "ANGZARR_FAKE_TAG";

/// How many running (not zombie) processes have `ANGZARR_FAKE_TAG=tag` in
/// their environment.
fn running_with_tag(tag: &str) -> usize {
    let wanted = format!("{TAG_ENV}={tag}");
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return 0;
    };
    entries
        .filter_map(|e| e.ok())
        .filter(|e| {
            let Some(pid) = e.file_name().to_str().map(str::to_string) else {
                return false;
            };
            pid.bytes().all(|b| b.is_ascii_digit())
                && alive(&format!("pid-{pid}"))
                && std::fs::read(e.path().join("environ"))
                    .is_ok_and(|env| env.split(|&b| b == 0).any(|var| var == wanted.as_bytes()))
        })
        .count()
}

/// A valid descriptor of `kind` named "order": what angzarr-cli generates.
fn described(kind: pb::ComponentKind) -> pb::ComponentDescriptor {
    use pb::component_descriptor::Kind;
    let state = || {
        Some(pb::StateDescriptor {
            type_name: "orders.v1.OrderState".to_string(),
        })
    };
    let kind = match kind {
        pb::ComponentKind::ProcessManager => Kind::ProcessManager(pb::ProcessManagerDescriptor {
            domain: "order".to_string(),
            state: state(),
            starting_types: vec!["orders.v1.Place".to_string()],
        }),
        pb::ComponentKind::Policy => Kind::Policy(pb::PolicyDescriptor {
            input_domain: "order".to_string(),
            output_domain: "inventory".to_string(),
        }),
        pb::ComponentKind::Projector => Kind::Projector(pb::ProjectorDescriptor {
            input_domains: vec!["order".to_string()],
        }),
        _ => Kind::Aggregate(pb::AggregateDescriptor {
            domain: "order".to_string(),
            state: state(),
        }),
    };
    pb::ComponentDescriptor {
        name: "order".to_string(),
        rules_version: "1.4.2+build.7".to_string(),
        kind: Some(kind),
    }
}

/// `descriptor`'s policy part, for a test to change.
fn policy_of(descriptor: &mut pb::ComponentDescriptor) -> &mut pb::PolicyDescriptor {
    match descriptor.kind.as_mut() {
        Some(pb::component_descriptor::Kind::Policy(policy)) => policy,
        _ => panic!("a policy's descriptor"),
    }
}

/// C-0687: the host refuses a policy's descriptor with no output domain,
/// naming the component, under ANGZARR_REASON_POLICY_NO_OUTPUT_DOMAIN.
#[test]
fn a_policy_descriptor_without_an_output_domain_is_refused() {
    use angzarr_diagnostic::Diagnostic;
    let mut descriptor = described(pb::ComponentKind::Policy);
    assert_eq!(angzarr_embedded::validate_descriptor(&descriptor), Ok(()));
    policy_of(&mut descriptor).output_domain = String::new();
    let refused = angzarr_embedded::validate_descriptor(&descriptor)
        .err()
        .unwrap();
    assert_eq!(refused.code(), "ANGZARR_REASON_POLICY_NO_OUTPUT_DOMAIN");
    assert_eq!(
        refused,
        angzarr_embedded::Misconfigured::PolicyNoOutputDomain {
            component: "order".to_string()
        }
    );
}

/// C-0687, every row: each fault refused under its own reason, a valid
/// descriptor of every kind accepted.
#[test]
fn a_descriptor_that_fails_its_validation_is_refused_for_its_reason() {
    use pb::component_descriptor::Kind;
    use pb::ComponentKind::{Aggregate, Policy, ProcessManager, Projector};
    for kind in [Aggregate, Policy, ProcessManager, Projector] {
        assert_eq!(
            angzarr_embedded::validate_descriptor(&described(kind)),
            Ok(()),
            "{kind:?}"
        );
    }
    let reason = |kind, fault: Fault| {
        let mut descriptor = described(kind);
        fault(&mut descriptor);
        angzarr_embedded::validate_descriptor(&descriptor)
            .err()
            .map(|r| angzarr_diagnostic::Diagnostic::code(&r).to_string())
    };
    type Fault = fn(&mut pb::ComponentDescriptor);
    let cases: [(pb::ComponentKind, Fault, &str); 9] = [
        (
            Aggregate,
            |d| d.rules_version.clear(),
            "ANGZARR_REASON_MISSING_RULES_VERSION",
        ),
        (
            Policy,
            |d| d.rules_version.clear(),
            "ANGZARR_REASON_MISSING_RULES_VERSION",
        ),
        (
            Aggregate,
            |d| d.kind = None,
            "ANGZARR_REASON_DESCRIPTOR_NO_KIND",
        ),
        (
            Aggregate,
            |d| {
                if let Some(Kind::Aggregate(a)) = d.kind.as_mut() {
                    a.state = None;
                }
            },
            "ANGZARR_REASON_MISSING_STATE_TYPE",
        ),
        (
            ProcessManager,
            |d| {
                if let Some(Kind::ProcessManager(pm)) = d.kind.as_mut() {
                    pm.state = None;
                }
            },
            "ANGZARR_REASON_MISSING_STATE_TYPE",
        ),
        (
            ProcessManager,
            |d| {
                if let Some(Kind::ProcessManager(pm)) = d.kind.as_mut() {
                    pm.domain.clear();
                }
            },
            "ANGZARR_REASON_DESCRIPTOR_NO_DOMAIN",
        ),
        (
            Policy,
            |d| policy_of(d).output_domain.clear(),
            "ANGZARR_REASON_POLICY_NO_OUTPUT_DOMAIN",
        ),
        (
            Policy,
            |d| policy_of(d).input_domain.clear(),
            "ANGZARR_REASON_NO_INPUT_DOMAIN",
        ),
        (
            Projector,
            |d| {
                if let Some(Kind::Projector(p)) = d.kind.as_mut() {
                    p.input_domains.clear();
                }
            },
            "ANGZARR_REASON_NO_INPUT_DOMAIN",
        ),
    ];
    for (kind, fault, expected) in cases {
        assert_eq!(reason(kind, fault).as_deref(), Some(expected), "{kind:?}");
    }
}

/// A host dropped inside a current-thread runtime stops its children
/// there, as it does anywhere: that runtime has no worker to hand off, so
/// nothing is asked of it.
#[test]
fn a_host_dropped_on_a_current_thread_runtime_stops_its_children() {
    let host = started(vec![player(mode::SERVE)]);
    let pid = pid_of(&host.handler("player").unwrap());
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move { drop(host) });
    assert!(!alive(&pid));
}

/// A restarted child is registered by its new descriptor only if it is
/// the same component: its name and kind, and for a policy or projector
/// its inputs and output domain, which its registration routes by. A
/// domain may change anything else (its rules version, its state, its
/// starting types).
#[test]
fn a_restarted_child_must_describe_the_same_component() {
    use pb::component_descriptor::Kind;
    use pb::ComponentKind::{Aggregate, Policy, ProcessManager, Projector};
    use ComponentKindConfig::{Domain, Policy as PolicyKind, Projector as ProjectorKind};
    let aggregate = described(Aggregate);
    let rebuilt = pb::ComponentDescriptor {
        rules_version: "1.5.0".to_string(),
        kind: Some(Kind::Aggregate(pb::AggregateDescriptor {
            domain: "order".to_string(),
            state: None,
        })),
        ..aggregate.clone()
    };
    assert!(same_component(Domain, &aggregate, &rebuilt));
    let renamed = pb::ComponentDescriptor {
        name: "other".to_string(),
        ..aggregate.clone()
    };
    assert!(!same_component(Domain, &aggregate, &renamed));
    let rekinded = described(ProcessManager);
    assert!(!same_component(Domain, &aggregate, &rekinded));

    let policy = described(Policy);
    let rebuilt = pb::ComponentDescriptor {
        rules_version: "1.5.0".to_string(),
        ..policy.clone()
    };
    assert!(same_component(PolicyKind, &policy, &rebuilt));
    let mut rerouted = policy.clone();
    policy_of(&mut rerouted).output_domain = "billing".to_string();
    assert!(!same_component(PolicyKind, &policy, &rerouted));
    let mut resourced = policy.clone();
    policy_of(&mut resourced).input_domain = "cart".to_string();
    assert!(!same_component(PolicyKind, &policy, &resourced));

    let projector = described(Projector);
    let more = pb::ComponentDescriptor {
        kind: Some(Kind::Projector(pb::ProjectorDescriptor {
            input_domains: vec!["order".to_string(), "cart".to_string()],
        })),
        ..projector.clone()
    };
    assert!(same_component(
        ProjectorKind,
        &projector,
        &projector.clone()
    ));
    assert!(!same_component(ProjectorKind, &projector, &more));
}

/// A restarted child describing another component is not registered, and
/// leaves its component's registration as it was: the host refuses it
/// ([`HostError::DescriptorChanged`]); one the registry refuses is
/// [`HostError::Register`]. The first child's is taken as it is.
#[test]
fn a_child_described_again_is_refused_unless_the_registry_takes_it() {
    let supervisor = |kind| Supervisor {
        domain: "order".to_string(),
        kind,
        launch: Launch {
            program: "true".to_string(),
            args: Vec::new(),
            env: Default::default(),
            dir: None,
        },
        supervision: Supervision::default(),
        socket_dir: PathBuf::from("/tmp"),
        link: Arc::new(crate::remote::Link::default()),
        hooks: Hooks::default(),
    };
    let order = supervisor(ComponentKindConfig::Domain);
    let first = described(pb::ComponentKind::Aggregate);
    assert_eq!(order.registered_by(&first, true), Ok(()));
    assert_eq!(order.link.registered().descriptor, first);
    let renamed = pb::ComponentDescriptor {
        name: "other".to_string(),
        ..first.clone()
    };
    assert_eq!(
        order.registered_by(&renamed, false),
        Err(HostError::DescriptorChanged {
            domain: "order".to_string()
        })
    );
    assert_eq!(order.link.registered().descriptor, first, "the old kept");

    let mut registry = Registry::new();
    let description = registry
        .described_handler(
            "order",
            RemoteCommandHandler::new(
                "order".to_string(),
                first.clone(),
                order.link.clone(),
                Duration::from_millis(1),
            ),
            first.clone(),
        )
        .unwrap();
    order.link.registered().description = Some(description);
    let unversioned = pb::ComponentDescriptor {
        rules_version: String::new(),
        ..first.clone()
    };
    let refused = order.registered_by(&unversioned, false);
    assert!(
        matches!(&refused, Err(HostError::Register { cause }) if angzarr_diagnostic::Diagnostic::code(cause) == "ANGZARR_REASON_MISSING_RULES_VERSION"),
        "{refused:?}"
    );
    assert_eq!(order.link.registered().descriptor, first, "the old kept");
    let rebuilt = pb::ComponentDescriptor {
        rules_version: "1.5.0".to_string(),
        ..first.clone()
    };
    assert_eq!(order.registered_by(&rebuilt, false), Ok(()));
    assert_eq!(order.link.registered().descriptor, rebuilt);
    assert_eq!(
        registry.catalog().rules_version("order").as_deref(),
        Some("1.5.0")
    );

    let sidecar = supervisor(ComponentKindConfig::Upcaster);
    assert_eq!(sidecar.registered_by(&renamed, false), Ok(()));
    assert_eq!(
        sidecar.link.registered().descriptor,
        pb::ComponentDescriptor::default(),
        "a sidecar is not described"
    );
}

/// A child given `env` beside its mode's.
fn with(mut component: ComponentConfig, env: &[(&str, &str)]) -> ComponentConfig {
    for (name, value) in env {
        component.env.insert(name.to_string(), value.to_string());
    }
    component
}

/// The child's stdout is drained from its launch, before it is ready,
/// described and registered: a child writing more there than a pipe holds
/// before it serves (the fake's flood) comes up, where one left undrained
/// would block on its write and never answer. Bounded: a host that never
/// starts fails the test, never hangs it.
#[test]
fn stdout_is_drained_from_the_launch() {
    let mut config = config(vec![player(mode::FLOOD)]);
    config.supervision.call_timeout_ms = 5_000;
    let (started, start) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = started.send(Host::start(config).map(|host| host.shutdown()));
    });
    let exits = start
        .recv_timeout(Duration::from_secs(30))
        .expect("start returns")
        .expect("the flooding child comes up");
    assert_eq!(exits, [("player".to_string(), ChildExit::Exited(Some(0)))]);
}

/// Every child's health is watched, not a sidecar's alone: a described
/// child that turns NOT_SERVING (a readiness probe failing, a child
/// wedged enough to say so) is down, its calls failing retryably, keyed
/// `not-serving`, without reaching it; once it turns SERVING again it is
/// up. It is the same process throughout: health is not its exit. Each
/// turn is forced through the fake's control socket, and the supervisor's
/// acting on it awaited, never polled for.
#[test]
fn a_described_child_is_up_only_while_its_health_reports_serving() {
    use fake_child::CONTROL_ENV;
    let socket = TestFile::new("angzarr-dctl", "c.sock");
    let control = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    let player = with(
        player(mode::SERVE),
        &[(CONTROL_ENV, socket.to_str().unwrap())],
    );
    let (applied, acted) = mpsc::channel();
    let hooks = Hooks {
        applied: Some(applied),
        ..Hooks::default()
    };
    let host = Host::start_with(config(vec![player]), hooks).expect("the host starts");
    let (mut fake, _) = control.accept().unwrap();
    let _ = std::fs::remove_file(&socket);
    let handler = host.handler("player").unwrap();
    let pid = pid_of(&handler);
    let sit = || {
        handler
            .dispatch(&command("player", "/examples.blackjack.SitDown"))
            .awaited()
    };
    let mut turned = |status: &[u8]| {
        use std::io::Write;
        fake.write_all(status).unwrap();
        acted.recv_timeout(Duration::from_secs(20))
    };

    assert_eq!(
        acted.recv_timeout(Duration::from_secs(20)),
        Ok(true),
        "came up"
    );
    assert!(sit().is_ok());
    assert_eq!(turned(b"0"), Ok(false), "turned NOT_SERVING");
    let down = sit().expect_err("not delivered");
    assert!(is_retryable(&down), "{down:?}");
    assert_eq!(
        down.extras
            .get(angzarr_proto::v1::reason_args::CAUSE)
            .map(String::as_str),
        Some(crate::remote::causes::NOT_SERVING)
    );
    assert_eq!(turned(b"1"), Ok(true), "turned SERVING");
    assert!(sit().is_ok());
    assert_eq!(pid_of(&handler), pid, "the same child");
    host.shutdown();
}

/// Stopping a child ends the host's own health watch first: a child that
/// lets its calls in flight finish before it exits (the fake, gracefully)
/// is not held up by the watch, and exits on its stdin's end within the
/// grace.
#[test]
fn a_graceful_child_is_not_held_up_by_the_hosts_health_watch() {
    let host = started(vec![player(mode::GRACEFUL)]);
    assert_eq!(
        host.shutdown(),
        [("player".to_string(), ChildExit::Exited(Some(0)))]
    );
}

// ---------------------------------------------------------------------
// Supervision: the periodic health check, the grace and the restart limit
// (the child protocol's section 8, decision 14.63;
// parity/host/supervision.feature). The supervisor's clock is the test's:
// each check is the test moving it on by the interval (and, for a child
// that answers no check, by the deadline), never a wait.

const INTERVAL: Duration = Duration::from_millis(1_000);
const DEADLINE: Duration = Duration::from_millis(100);
const GRACE_MS: u64 = 1_500;

/// A host of `player` whose supervisor runs on a clock the test moves,
/// its check interval [`INTERVAL`], its deadline [`DEADLINE`], its grace
/// [`GRACE_MS`] and its restart limit `limit`, telling the test each
/// verdict it reaches. Its sockets are in a directory of its own.
struct OnTheClock {
    host: Host,
    clock: Arc<crate::clock::ManualClock>,
    judged: mpsc::Receiver<crate::vitals::Verdict>,
    sockets: TestDir,
}

impl OnTheClock {
    fn start(player: ComponentConfig, limit: u32) -> OnTheClock {
        OnTheClock::resetting(player, limit, Duration::from_secs(3_600))
    }

    /// [`OnTheClock::start`], its restart count clearing after `reset`
    /// healthy.
    fn resetting(player: ComponentConfig, limit: u32, reset: Duration) -> OnTheClock {
        let sockets = for_sockets("angzarr-sockets");
        let mut config = config(vec![player]);
        config.supervision.socket_dir = Some(sockets.to_path_buf());
        config.supervision.unhealthy_reset_ms = reset.as_millis() as u64;
        config.supervision.health_check_interval_ms = INTERVAL.as_millis() as u64;
        config.supervision.health_check_deadline_ms = DEADLINE.as_millis() as u64;
        config.supervision.unhealthy_grace_ms = GRACE_MS;
        config.supervision.unhealthy_restart_limit = limit;
        let clock = crate::clock::ManualClock::new();
        let (judge, judged) = mpsc::channel();
        let hooks = Hooks {
            clock: Some(clock.clone()),
            judged: Some(judge),
            ..Hooks::default()
        };
        let host = Host::start_with(config, hooks).expect("the host starts");
        OnTheClock {
            host,
            clock,
            judged,
            sockets,
        }
    }

    /// One check: the interval passes, and, for a child that does not
    /// `answer`, the check's deadline; the verdict on what it found.
    fn check(&self, answer: bool) -> crate::vitals::Verdict {
        self.clock.advance(INTERVAL);
        if !answer {
            self.clock.advance(DEADLINE);
        }
        self.verdict()
    }

    /// The next verdict the supervisor reaches.
    fn verdict(&self) -> crate::vitals::Verdict {
        self.judged
            .recv_timeout(Duration::from_secs(20))
            .expect("the supervisor judged the child's health")
    }

    fn handler(&self) -> RemoteCommandHandler {
        self.host.handler("player").unwrap()
    }

    /// Whether its socket directory is empty within five seconds.
    fn no_sockets(&self) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while files_in(&self.sockets) > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        files_in(&self.sockets) == 0
    }

    /// Awaits the `n`th child coming up.
    fn came_up(&self, n: u64) {
        let mut generations = self.handler().generations();
        let up = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(20), generations.wait_for(|g| *g >= n))
                    .await
                    .map(|seen| seen.is_ok())
            });
        assert_eq!(up, Ok(true), "child {n} came up");
    }

    fn sit(&self) -> Result<hpb::HandleResponse, angzarr_router::error::CodedError> {
        self.handler()
            .dispatch(&command("player", "/examples.blackjack.SitDown"))
            .awaited()
    }
}

/// A path for a test's file, `tag`, in a directory of its own, removed
/// with it when the guard drops.
fn scratch(tag: &str) -> TestFile {
    TestFile::new("angzarr-host", tag)
}

/// `error`'s `cause`.
fn cause(error: &angzarr_router::error::CodedError) -> Option<&str> {
    (error.extras)
        .get(angzarr_proto::v1::reason_args::CAUSE)
        .map(String::as_str)
}

/// A healthy child is checked every interval, and answers; it is the same
/// child throughout (C-0763).
#[test]
fn a_healthy_child_is_checked_every_interval() {
    use crate::vitals::Verdict;
    let supervised = OnTheClock::start(player(mode::SERVE), 3);
    let pid = pid_of(&supervised.handler());
    for _ in 0..3 {
        assert_eq!(supervised.check(true), Verdict::Up);
    }
    let answered = supervised.sit().expect("the child answers");
    assert_eq!(answered.rules_version, pid, "the same child");
    supervised.host.shutdown();
}

/// A child that answers no check, its health watch still SERVING, is down
/// from its first unanswered check, its calls failing retryably without
/// reaching it, and restarted at the first check past its grace; the new
/// child answers (C-0764).
#[test]
fn a_wedged_child_is_restarted_once_its_grace_has_passed() {
    use crate::vitals::Verdict;
    let wedge = scratch("wedge");
    std::fs::write(&wedge, "").unwrap();
    let supervised = OnTheClock::start(
        with(
            player(mode::SERVE),
            &[(fake_child::CHECK_HANG_ENV, wedge.to_str().unwrap())],
        ),
        3,
    );
    let first = pid_of(&supervised.handler());
    assert_eq!(supervised.check(false), Verdict::Down, "1100 ms");
    let down = supervised.sit().expect_err("not sent");
    assert!(is_retryable(&down), "{down:?}");
    assert_eq!(cause(&down), Some(crate::remote::causes::NOT_SERVING));
    assert_eq!(supervised.check(false), Verdict::Down, "2200 ms");
    assert_eq!(supervised.check(false), Verdict::Restart, "3300 ms");
    std::fs::remove_file(&wedge).unwrap();
    supervised.came_up(2);
    let answered = supervised.sit().expect("the new child answers");
    assert_ne!(answered.rules_version, first, "a new child answered");
    assert!(!alive(&first));
    supervised.host.shutdown();
}

/// A child whose health reports NOT_SERVING past its grace is restarted,
/// its grace from when its watch first said so (C-0765).
#[test]
fn a_child_not_serving_past_its_grace_is_restarted() {
    use crate::vitals::Verdict;
    use fake_child::CONTROL_ENV;
    let socket = scratch("nsctl.sock");
    let control = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    let supervised = OnTheClock::start(
        with(
            player(mode::SERVE),
            &[(CONTROL_ENV, socket.to_str().unwrap())],
        ),
        3,
    );
    let (mut fake, _) = control.accept().unwrap();
    let first = pid_of(&supervised.handler());
    std::io::Write::write_all(&mut fake, b"0").unwrap();
    assert_eq!(supervised.verdict(), Verdict::Down, "its watch, at 0 ms");
    assert_eq!(supervised.check(true), Verdict::Down, "1000 ms");
    assert_eq!(supervised.check(true), Verdict::Restart, "2000 ms");
    supervised.came_up(2);
    let _ = std::fs::remove_file(&socket);
    assert_ne!(pid_of(&supervised.handler()), first);
    assert!(!alive(&first));
    supervised.host.shutdown();
}

/// A child that answers its checks again within its grace is up again,
/// and not restarted however long it then stays healthy (C-0766).
#[test]
fn a_child_healthy_again_within_its_grace_is_not_restarted() {
    use crate::vitals::Verdict;
    let wedge = scratch("recover");
    std::fs::write(&wedge, "").unwrap();
    let supervised = OnTheClock::start(
        with(
            player(mode::SERVE),
            &[(fake_child::CHECK_HANG_ENV, wedge.to_str().unwrap())],
        ),
        3,
    );
    let pid = pid_of(&supervised.handler());
    assert_eq!(supervised.check(false), Verdict::Down, "1100 ms");
    std::fs::remove_file(&wedge).unwrap();
    for _ in 0..3 {
        assert_eq!(supervised.check(true), Verdict::Up);
    }
    let answered = supervised.sit().expect("the child answers again");
    assert_eq!(answered.rules_version, pid, "the same child");
    supervised.host.shutdown();
}

/// A component restarted for its health as often as its limit stays down
/// the next time: its child stopped and none launched, its calls failing
/// retryably as a child not running does, until the host ends (C-0767).
#[test]
fn past_its_restart_limit_a_child_stays_down() {
    use crate::vitals::Verdict;
    let wedge = scratch("limit");
    std::fs::write(&wedge, "").unwrap();
    let supervised = OnTheClock::start(
        with(
            player(mode::SERVE),
            &[(fake_child::CHECK_HANG_ENV, wedge.to_str().unwrap())],
        ),
        1,
    );
    let first = pid_of(&supervised.handler());
    assert_eq!(supervised.check(false), Verdict::Down);
    assert_eq!(supervised.check(false), Verdict::Down);
    assert_eq!(supervised.check(false), Verdict::Restart);
    supervised.came_up(2);
    let second = pid_of(&supervised.handler());
    assert_ne!(second, first);
    assert_eq!(supervised.check(false), Verdict::Down);
    assert_eq!(supervised.check(false), Verdict::Down);
    assert_eq!(supervised.check(false), Verdict::GiveUp);
    assert!(
        supervised.no_sockets(),
        "the given-up child's socket is gone"
    );
    let down = supervised.sit().expect_err("given up");
    assert!(is_retryable(&down), "{down:?}");
    assert_eq!(cause(&down), Some(crate::remote::causes::NOT_RUNNING));
    std::fs::remove_file(&wedge).unwrap();
    assert!(!alive(&first) && !alive(&second));
    assert_eq!(
        supervised.host.shutdown(),
        [("player".to_string(), ChildExit::NotRunning)]
    );
}

/// A child healthy for the whole reset window, unbroken, clears its
/// component's restart count: wedged again past its grace, it is restarted
/// once more, not given up (C-0769; decision 14.67). The supervisor's clock
/// is the test's.
#[test]
fn a_child_healthy_for_the_reset_window_clears_the_restart_count() {
    use crate::vitals::Verdict;
    let wedge = scratch("reset");
    std::fs::write(&wedge, "").unwrap();
    let supervised = OnTheClock::resetting(
        with(
            player(mode::SERVE),
            &[(fake_child::CHECK_HANG_ENV, wedge.to_str().unwrap())],
        ),
        1,
        INTERVAL * 3,
    );
    assert_eq!(supervised.check(false), Verdict::Down);
    assert_eq!(supervised.check(false), Verdict::Down);
    assert_eq!(supervised.check(false), Verdict::Restart);
    supervised.came_up(2);
    std::fs::remove_file(&wedge).unwrap();
    for _ in 0..4 {
        assert_eq!(supervised.check(true), Verdict::Up);
    }
    std::fs::write(&wedge, "").unwrap();
    assert_eq!(supervised.check(false), Verdict::Down);
    assert_eq!(supervised.check(false), Verdict::Down);
    assert_eq!(supervised.check(false), Verdict::Restart);
    std::fs::remove_file(&wedge).unwrap();
    drop(supervised.host.shutdown());
}

/// A command in flight on a child that is restarted fails retryably, and
/// is never sent again: not to the new child, nor to the old one twice
/// (C-0768). The fake says on its control socket when the command has
/// reached it.
#[test]
fn a_command_in_flight_on_a_restarted_child_fails_retryably_once() {
    use crate::vitals::Verdict;
    use fake_child::{CONTROL_ENV, LOG_ENV};
    let socket = scratch("ifctl.sock");
    let log = scratch("inflight.log");
    let wedge = scratch("inflight");
    let control = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    let supervised = OnTheClock::start(
        with(
            player(mode::SERVE),
            &[
                (CONTROL_ENV, socket.to_str().unwrap()),
                (LOG_ENV, log.to_str().unwrap()),
                (fake_child::CHECK_HANG_ENV, wedge.to_str().unwrap()),
            ],
        ),
        3,
    );
    let (mut fake, _) = control.accept().unwrap();
    let first = pid_of(&supervised.handler());
    let handler = supervised.handler();
    let in_flight = std::thread::spawn(move || {
        handler
            .dispatch(&command("player", "/examples.blackjack.Hang"))
            .awaited()
    });
    fake.set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    let mut reached = [0u8];
    std::io::Read::read_exact(&mut fake, &mut reached).unwrap();
    assert_eq!(reached[0], fake_child::HUNG, "the command is in flight");
    std::fs::write(&wedge, "").unwrap();
    assert_eq!(supervised.check(false), Verdict::Down);
    assert_eq!(supervised.check(false), Verdict::Down);
    assert_eq!(supervised.check(false), Verdict::Restart);
    let failed = in_flight
        .join()
        .unwrap()
        .expect_err("its child was stopped");
    assert!(is_retryable(&failed), "{failed:?}");
    std::fs::remove_file(&wedge).unwrap();
    supervised.came_up(2);
    let second = pid_of(&supervised.handler());
    assert!(supervised.sit().is_ok());
    let _ = std::fs::remove_file(&socket);
    let logged = std::fs::read_to_string(&log).unwrap();
    let hung: Vec<&str> = logged.lines().filter(|l| l.contains("Hang")).collect();
    assert_eq!(
        hung,
        [format!("handle /examples.blackjack.Hang {first}")],
        "sent once, to the first child: {logged}"
    );
    assert!(logged.contains(&format!("handle /examples.blackjack.SitDown {second}")));
    supervised.host.shutdown();
}

// ---------------------------------------------------------------------
// Readiness: health SERVING on the host's path within the start limit
// (the child protocol's section 2; decision 14.80; C-0869 and C-0870).
// The start limit runs on the supervisor's clock, which the test moves a
// check interval at a time: never a wait.

/// The start limit the scenarios' fixture gives.
const START_LIMIT_MS: u64 = 5_000;

/// `player` started on a clock the test moves, its start limit
/// [`START_LIMIT_MS`] and its check interval [`INTERVAL`], in a socket
/// directory of its own: the start's answer, once the test has moved the
/// clock on `checks` check intervals (a start that answers before is
/// answered at once).
fn start_on_the_clock(player: ComponentConfig, checks: u64) -> (Result<Host, HostError>, TestDir) {
    start_seeing(player, checks, None)
}

/// [`start_on_the_clock`], the clock moved on only once the starting
/// child's watch has reported `first` (when given): forced, never raced.
fn start_seeing(
    player: ComponentConfig,
    checks: u64,
    first: Option<ServingStatus>,
) -> (Result<Host, HostError>, TestDir) {
    let sockets = for_sockets("angzarr-start");
    let mut config = config(vec![player]);
    config.supervision.socket_dir = Some(sockets.to_path_buf());
    config.supervision.handshake_timeout_ms = START_LIMIT_MS;
    config.supervision.health_check_interval_ms = INTERVAL.as_millis() as u64;
    config.supervision.health_check_deadline_ms = DEADLINE.as_millis() as u64;
    let clock = crate::clock::ManualClock::new();
    let (saw, seen) = mpsc::channel();
    let hooks = Hooks {
        clock: Some(clock.clone()),
        starting: Some(saw),
        ..Hooks::default()
    };
    let (answer, answered) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = answer.send(Host::start_with(config, hooks));
    });
    if let Some(first) = first {
        assert_eq!(seen.recv_timeout(Duration::from_secs(20)), Ok(first));
    }
    for _ in 0..checks {
        if let Ok(started) = answered.try_recv() {
            return (started, sockets);
        }
        clock.advance(INTERVAL);
    }
    let started = answered
        .recv_timeout(Duration::from_secs(20))
        .expect("start answers");
    (started, sockets)
}

/// The check intervals the start limit holds: the clock moved on by each,
/// the limit has passed.
const START_CHECKS: u64 = START_LIMIT_MS / INTERVAL.as_millis() as u64;

/// C-0869, both rows: a child not SERVING on the host's path within the
/// start limit fails its start, `EMBEDDED_REASON_CHILD_NOT_SERVING` naming
/// the component, the last status it reported (`NOT_SERVING`, or `none` for
/// a child that never listens) and the limit; the child is stopped and
/// the socket file at the path the host chose removed.
#[test]
fn a_child_not_serving_within_the_start_limit_fails_its_start() {
    use angzarr_diagnostic::Diagnostic;
    use fake_child::{health, HEALTH_ENV};
    let tag = format!("not-serving-{}", std::process::id());
    let rows = [
        (
            with(player(mode::SERVE), &[(HEALTH_ENV, health::NOT_SERVING)]),
            "NOT_SERVING",
            Some(ServingStatus::NotServing),
        ),
        (player(mode::SILENT), "none", None),
    ];
    for (child, status, first) in rows {
        let child = with(child, &[(TAG_ENV, &tag)]);
        let (started, sockets) = start_seeing(child, START_CHECKS, first);
        let refused = started.err().expect("the start fails");
        assert_eq!(
            refused,
            HostError::NotServing {
                component: "player".to_string(),
                status: status.to_string(),
                timeout_ms: START_LIMIT_MS,
            }
        );
        assert_eq!(refused.code(), "EMBEDDED_REASON_CHILD_NOT_SERVING");
        assert_eq!(running_with_tag(&tag), 0, "the child is stopped: {status}");
        assert_eq!(
            files_in(&sockets),
            0,
            "the socket file is removed: {status}"
        );
    }
}

/// C-0870: a child that exits before it reports SERVING fails its start at
/// once, its status `none`, and leaves no socket file.
#[test]
fn a_child_exiting_before_it_serves_fails_its_start() {
    let (started, sockets) = start_on_the_clock(player(mode::EXITS), 0);
    assert_eq!(
        started.err(),
        Some(HostError::NotServing {
            component: "player".to_string(),
            status: "none".to_string(),
            timeout_ms: START_LIMIT_MS,
        })
    );
    assert_eq!(files_in(&sockets), 0);
}

/// Readiness waits for SERVING: a child NOT_SERVING when it starts, turned
/// SERVING within the start limit, comes up then, the same child.
#[test]
fn a_child_serving_within_the_start_limit_comes_up() {
    use fake_child::{health, CONTROL_ENV, HEALTH_ENV};
    let socket = scratch("rdyctl.sock");
    let control = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    let child = with(
        player(mode::SERVE),
        &[
            (HEALTH_ENV, health::NOT_SERVING),
            (CONTROL_ENV, socket.to_str().unwrap()),
        ],
    );
    let accepting = std::thread::spawn(move || {
        let (mut fake, _) = control.accept().unwrap();
        std::io::Write::write_all(&mut fake, b"1").unwrap();
        fake
    });
    let (started, _sockets) = start_on_the_clock(child, 0);
    let _fake = accepting.join().unwrap();
    let host = started.expect("the child came up");
    assert!(host.handler("player").is_some());
    host.shutdown();
}
