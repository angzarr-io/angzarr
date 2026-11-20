//! Red tests (feat/host-admit-policies, the owner's rulings of 2026-10-29):
//! the host's remote process managers apply their start rule, and the host
//! carries policy and projector children beside its domains.
//!
//! - A remote process manager's start rule is the kernel's, from the
//!   child's descriptor (`ComponentDescriptor.starting_types`), with no
//!   call to the child: admit is coordinator work (decisions 14.39, 14.41,
//!   14.42). There is no admit RPC.
//! - A `component` entry of `kind: policy` is a `PolicyService` child, run
//!   by the kernel as any policy of its source; one of `kind: projector`
//!   a `ProjectorService` child, run after each commit of its source's
//!   events, its failures following the four classes (decision 14.37). A
//!   child that describes itself as another kind is refused at start.

use std::path::Path;
use std::time::Duration;

use angzarr_diagnostic::Diagnostic;
use angzarr_embedded::angzarr_router::error::{CodedError, GrpcCode};
use angzarr_embedded::angzarr_router::hpb;
use angzarr_embedded::angzarr_router::router::{CommandHandler, PolicyComponent, ProjectorHandler};
use angzarr_embedded::v1::{
    command_page, component_descriptor, AggregateDescriptor, Command, CommandBook, CommandPage,
    ComponentDescriptor, Cover, Fact, FactOrigin, StateDescriptor, Uuid,
};
use angzarr_embedded::{
    Any, EventStore, Failure, Kernel, MemoryDeadLetters, OnRetryable, Registry, Sampled, StreamRef,
    Timestamp,
};
use angzarr_ports::memory::MemoryStore;

use super::{Hooks, Host};
use crate::config::{ComponentConfig, ComponentKindConfig};
use crate::error::HostError;
use crate::fake_child::{
    config, domain, health, kind, mode, noted, CONTROL_ENV, HEALTH_ENV, HOLD, HOLD_TOO_MUCH,
    INPUT_ENV, KIND_ENV, LOG_ENV, SEAT_RELEASED, STARTING_ENV, TARGET_ENV,
};
use angzarr_test_dir::TestFile;

/// The kernel's key for a process manager with no instance for an input.
const PM_NO_INSTANCE: &str = "ANGZARR_REASON_PM_NO_INSTANCE";

fn root() -> Uuid {
    Uuid::from_bytes([0x0b; 16])
}

fn cover(domain: &str) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(root()),
        edition: None,
    }
}

fn any(type_url: &str) -> Any {
    Any {
        type_url: type_url.to_string(),
        value: Vec::new(),
    }
}

/// Command `n` to `domain`'s stream, as a client sends it.
fn send(domain: &str, n: u8, payload: Any) -> CommandBook {
    CommandBook {
        cover: Some(cover(domain)),
        page: Some(CommandPage {
            body: Some(command_page::Body::Command(Command {
                payload: Some(payload),
                handling: None,
            })),
            command_id: Some(Uuid::from_bytes([n; 16])),
            ..CommandPage::default()
        }),
    }
}

/// Fact `n` to `domain`'s stream, from an outside source.
fn tell(domain: &str, n: u8, payload: Any) -> CommandBook {
    CommandBook {
        cover: Some(cover(domain)),
        page: Some(CommandPage {
            body: Some(command_page::Body::Fact(Fact {
                payload: Some(payload),
                origin: Some(FactOrigin {
                    external_id: format!("outside-{n}"),
                    description: "a test's fact".to_string(),
                }),
            })),
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
        command_id: Uuid::from_bytes([0x60 | n; 16]),
    }
}

fn payloads(store: &MemoryStore, stream: &Cover) -> Vec<Option<Any>> {
    let stream = StreamRef::try_from(stream).unwrap();
    block_on(store.read(stream, 0, None))
        .unwrap()
        .pages
        .iter()
        .map(|p| angzarr_embedded::angzarr_router::page_event(p).cloned())
        .collect()
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

/// An aggregate's descriptor, `name` in the domain `name` lowercased, its
/// state `state_type`.
fn descriptor(name: &str, state_type: &str) -> ComponentDescriptor {
    ComponentDescriptor {
        name: name.to_string(),
        rules_version: format!("{name}-1"),
        kind: Some(component_descriptor::Kind::Aggregate(AggregateDescriptor {
            domain: name.to_lowercase(),
            state: Some(StateDescriptor {
                type_name: state_type.to_string(),
            }),
        })),
    }
}

/// The payload of a command or a fact.
fn input(req: &hpb::HandleRequest) -> Any {
    let page = req.input.as_ref().and_then(|c| c.page.as_ref());
    match page.and_then(|p| p.body.as_ref()) {
        Some(command_page::Body::Command(c)) => c.payload.clone().unwrap_or_default(),
        Some(command_page::Body::Fact(f)) => f.payload.clone().unwrap_or_default(),
        None => Any::default(),
    }
}

fn one_event(payload: Any, rules_version: &str) -> hpb::HandleResponse {
    hpb::HandleResponse {
        events: vec![payload],
        state: None,
        rules_version: rules_version.to_string(),
    }
}

/// An in-process `table`: a command `/test.table.v1.Seat<X>` records
/// `/test.table.v1.Seated<X>`; a fact is recorded as it arrives.
struct Table;

impl CommandHandler for Table {
    fn name(&self) -> &str {
        "Table"
    }

    fn command_types(&self) -> Vec<String> {
        vec!["test.table.v1.Seat".to_string()]
    }

    async fn dispatch(&self, req: &hpb::HandleRequest) -> Result<hpb::HandleResponse, CodedError> {
        let sent = input(req);
        let is_fact = matches!(
            req.input
                .as_ref()
                .and_then(|c| c.page.as_ref())
                .and_then(|p| p.body.as_ref()),
            Some(command_page::Body::Fact(_))
        );
        let event = match is_fact {
            true => sent,
            false => any(&sent.type_url.replacen("Seat", "Seated", 1)),
        };
        Ok(one_event(event, "Table-1"))
    }
}

/// An in-process `wallet`: `Hold` records `Held`; `HoldTooMuch` is
/// refused FAILED_PRECONDITION, `INSUFFICIENT_FUNDS`, with the typed
/// refusal [`INSUFFICIENT`].
struct Wallet;

const HELD: &str = "/test.wallet.v1.Held";
const INSUFFICIENT: &str = "/test.wallet.v1.Insufficient";

impl CommandHandler for Wallet {
    fn name(&self) -> &str {
        "Wallet"
    }

    fn command_types(&self) -> Vec<String> {
        vec![HOLD[1..].to_string(), HOLD_TOO_MUCH[1..].to_string()]
    }

    async fn dispatch(&self, req: &hpb::HandleRequest) -> Result<hpb::HandleResponse, CodedError> {
        if input(req).type_url == HOLD_TOO_MUCH {
            return Err(CodedError {
                code: "INSUFFICIENT_FUNDS".to_string(),
                extras: Default::default(),
                grpc: GrpcCode::FailedPrecondition,
                refusal: Some(Box::new(any(INSUFFICIENT))),
            });
        }
        Ok(one_event(any(HELD), "Wallet-1"))
    }
}

/// A fresh call log for a fake policy or projector, by test, removed with
/// its directory when the guard drops.
fn log_file(test: &str) -> TestFile {
    TestFile::new("angzarr-fake", &format!("{test}.log"))
}

fn log_lines(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

fn with_env(mut domain: ComponentConfig, env: &[(&str, &str)]) -> ComponentConfig {
    for (k, v) in env {
        domain.env.insert(k.to_string(), v.to_string());
    }
    domain
}

// ---------------------------------------------------------------------
// A remote process manager's start rule.
// ---------------------------------------------------------------------

/// A remote process manager `audit` whose child lists `starting` as its
/// starting types.
fn process_manager(starting: &str) -> Host {
    let pm = with_env(
        domain("audit", "AuditProcess", mode::SERVE),
        &[(KIND_ENV, kind::PROCESS_MANAGER), (STARTING_ENV, starting)],
    );
    Host::start(config(vec![pm])).expect("the host starts")
}

/// A non-starting COMMAND for a root with no instance is refused before
/// the child decides: FAILED_PRECONDITION, ANGZARR_REASON_PM_NO_INSTANCE,
/// the framework's refusal (never a typed one), nothing recorded (C-0595).
/// The child would have answered `Noted`.
#[test]
fn a_remote_process_managers_non_starting_command_with_no_instance_is_refused() {
    let host = process_manager("test.pm.v1.Open");
    let mut registry = Registry::new();
    host.register(&mut registry).unwrap();
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);

    let refused = block_on(kernel.handle_any(
        &store,
        send("audit", 1, any("/test.pm.v1.Close")),
        &sampled(1),
    ));

    let written = payloads(&store, &cover("audit"));
    assert!(
        written.is_empty(),
        "no instance is started by a non-starting command: {written:?}"
    );
    let Err(Failure::Kernel(angzarr_embedded::KernelError::Router(rejected))) = &refused else {
        panic!("refused ANGZARR_REASON_PM_NO_INSTANCE: {:?}", refused.err());
    };
    assert_eq!(rejected.code, PM_NO_INSTANCE);
    assert_eq!(rejected.grpc, GrpcCode::FailedPrecondition);
    assert!(log.lines().is_empty(), "{:?}", log.lines());
    host.shutdown();
}

/// A non-starting FACT for a root with no instance is dead-lettered, its
/// letter keyed ANGZARR_REASON_PM_NO_INSTANCE, and its sender answered
/// ANGZARR_REASON_FACT_DEAD_LETTERED, no events (C-0593).
#[test]
fn a_remote_process_managers_non_starting_fact_with_no_instance_is_dead_lettered() {
    let host = process_manager("test.pm.v1.Open");
    let mut registry = Registry::new();
    host.register(&mut registry).unwrap();
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);

    let answered = block_on(kernel.handle_any(
        &store,
        tell("audit", 1, any("/test.pm.v1.Closed")),
        &sampled(1),
    ));

    let written = payloads(&store, &cover("audit"));
    assert!(
        written.is_empty(),
        "a non-starting fact starts no instance: {written:?}"
    );
    assert!(
        matches!(
            &answered,
            Err(Failure::Kernel(
                angzarr_embedded::KernelError::FactDeadLettered { .. }
            ))
        ),
        "{:?}",
        answered.err()
    );
    let reasons: Vec<_> = log
        .letters()
        .into_iter()
        .filter_map(|l| match l.why {
            Some(angzarr_embedded::v1::angzarr_dead_letter::Why::Reason(r)) => Some(r.code),
            _ => None,
        })
        .collect();
    assert_eq!(reasons, [PM_NO_INSTANCE]);
    host.shutdown();
}

/// A starting command starts the instance (the child decides), and once it
/// exists any input reaches the child.
#[test]
fn a_remote_process_managers_starting_command_starts_an_instance_that_then_takes_any_input() {
    let host = process_manager("test.pm.v1.Open");
    let mut registry = Registry::new();
    host.register(&mut registry).unwrap();
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);

    block_on(kernel.handle_any(
        &store,
        send("audit", 1, any("/test.pm.v1.Open")),
        &sampled(1),
    ))
    .expect("a starting command is delivered");
    block_on(kernel.handle_any(
        &store,
        send("audit", 2, any("/test.pm.v1.Close")),
        &sampled(2),
    ))
    .expect("the instance exists: any command is delivered");

    assert_eq!(
        payloads(&store, &cover("audit")),
        [Some(noted(0)), Some(noted(1))]
    );
    host.shutdown();
}

/// The start rule matches by type NAME: a descriptor may list a starting
/// type as a full type URL (`type.googleapis.com/...`, as
/// component.proto calls them) or as a bare name (as angzarr-router's
/// `DomainDispatch` advertises them), and a payload under `/...` (angzarr's
/// canonical prefix) matches either. Nothing else starts an instance.
#[test]
fn a_remote_process_managers_start_rule_matches_by_type_name_whatever_the_prefix() {
    let host = process_manager("type.googleapis.com/test.pm.v1.Open,test.pm.v1.Reopen");
    let mut registry = Registry::new();
    host.register(&mut registry).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);

    let closed = MemoryStore::new();
    let refused = block_on(kernel.handle_any(
        &closed,
        send("audit", 1, any("/test.pm.v1.Close")),
        &sampled(1),
    ));
    assert!(
        matches!(&refused, Err(Failure::Kernel(angzarr_embedded::KernelError::Router(r))) if r.code == PM_NO_INSTANCE),
        "an unlisted type does not start one: {:?}",
        refused.err()
    );

    for (n, starting) in [(2, "/test.pm.v1.Open"), (3, "/test.pm.v1.Reopen")] {
        let store = MemoryStore::new();
        block_on(kernel.handle_any(&store, send("audit", n, any(starting)), &sampled(n)))
            .unwrap_or_else(|e| panic!("{starting} starts an instance: {e:?}"));
        assert_eq!(payloads(&store, &cover("audit")), [Some(noted(0))]);
    }
    host.shutdown();
}

// ---------------------------------------------------------------------
// A remote policy.
// ---------------------------------------------------------------------

/// A remote policy `table-wallet`, `kind: policy`, from `table` to
/// `wallet`, logging its calls to `log`.
fn policy_domain(log: &Path) -> ComponentConfig {
    let mut policy = with_env(
        domain("table-wallet", "TableWalletPolicy", mode::SERVE),
        &[
            (KIND_ENV, kind::POLICY),
            (INPUT_ENV, "table"),
            (TARGET_ENV, "wallet"),
            (LOG_ENV, log.to_str().unwrap()),
        ],
    );
    policy.kind = ComponentKindConfig::Policy;
    policy
}

/// `kind: policy` launches a `PolicyService` child, handed back as a
/// policy (named by its descriptor, its source its descriptor's one input),
/// never as a domain.
#[test]
fn a_policy_child_comes_up_as_a_policy_not_a_domain() {
    let log = log_file("policy-comes-up");
    let host = Host::start(config(vec![policy_domain(&log)])).expect("the host starts");

    let policy = host
        .policy("table-wallet")
        .expect("handed back as a policy");
    assert_eq!(policy.name(), "TableWalletPolicy");
    assert_eq!(policy.input_domain(), "table");
    assert_eq!(policy.configured(), "table-wallet");
    assert_eq!(policy.descriptor().name, "TableWalletPolicy");
    assert!(format!("{policy:?}").contains("table-wallet"), "{policy:?}");
    assert!(
        host.handler("table-wallet").is_none(),
        "a policy is not a domain"
    );
    host.shutdown();
}

/// The kernel runs the remote policy on its source's events: the table's
/// `Seated` reaches the child, whose `Hold` the wallet records.
#[test]
fn a_remote_policy_turns_its_sources_events_into_commands_through_the_kernel() {
    let log_path = log_file("policy-commands");
    let host = Host::start(config(vec![policy_domain(&log_path)])).expect("the host starts");
    let mut registry = Registry::new();
    registry
        .command_handler("table", Table, descriptor("Table", "test.table.v1.State"))
        .unwrap();
    registry
        .command_handler(
            "wallet",
            Wallet,
            descriptor("Wallet", "test.wallet.v1.State"),
        )
        .unwrap();
    host.register(&mut registry)
        .expect("a policy child registers as a policy");
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);

    block_on(kernel.handle_any(
        &store,
        send("table", 1, any("/test.table.v1.Seat")),
        &sampled(1),
    ))
    .expect("handled");

    assert_eq!(
        log_lines(&log_path),
        ["handle /test.table.v1.Seated"],
        "the child saw the event"
    );
    assert_eq!(payloads(&store, &cover("wallet")), [Some(any(HELD))]);
    assert!(log.lines().is_empty(), "{:?}", log.lines());
    host.shutdown();
}

/// The wallet refuses the remote policy's command: the kernel calls the
/// child's OnRefused with the refusal (the typed refusal included), and
/// the fact it answers lands on the table's stream, the trigger's. The
/// wallet records nothing.
#[test]
fn a_refusal_of_a_remote_policys_command_is_answered_by_its_on_refused() {
    let log_path = log_file("policy-refused");
    let host = Host::start(config(vec![policy_domain(&log_path)])).expect("the host starts");
    let mut registry = Registry::new();
    registry
        .command_handler("table", Table, descriptor("Table", "test.table.v1.State"))
        .unwrap();
    registry
        .command_handler(
            "wallet",
            Wallet,
            descriptor("Wallet", "test.wallet.v1.State"),
        )
        .unwrap();
    host.register(&mut registry)
        .expect("a policy child registers as a policy");
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);

    let handled = block_on(kernel.handle_any(
        &store,
        send("table", 1, any("/test.table.v1.SeatBig")),
        &sampled(1),
    ));

    assert!(
        log_lines(&log_path).contains(&format!("refused {INSUFFICIENT}")),
        "OnRefused got the typed refusal: {:?}",
        log_lines(&log_path)
    );
    let released = Any {
        type_url: SEAT_RELEASED.to_string(),
        value: HOLD_TOO_MUCH.as_bytes().to_vec(),
    };
    assert_eq!(
        payloads(&store, &cover("table")),
        [Some(any("/test.table.v1.SeatedBig")), Some(released)]
    );
    assert!(payloads(&store, &cover("wallet")).is_empty());
    assert!(
        handled.is_ok(),
        "the refusal was answered: {:?}",
        handled.err()
    );
    host.shutdown();
}

// ---------------------------------------------------------------------
// A remote projector.
// ---------------------------------------------------------------------

/// A remote projector `table-view`, `kind: projector`, of `table`,
/// logging its calls to `log`.
fn projector_domain(log: &Path) -> ComponentConfig {
    let mut projector = with_env(
        domain("table-view", "TableView", mode::SERVE),
        &[
            (KIND_ENV, kind::PROJECTOR),
            (INPUT_ENV, "table"),
            (LOG_ENV, log.to_str().unwrap()),
        ],
    );
    projector.kind = ComponentKindConfig::Projector;
    projector
}

/// The host, the in-process table and the remote projector, registered.
fn projected(log_path: &Path) -> (Host, Registry) {
    let host = Host::start(config(vec![projector_domain(log_path)])).expect("the host starts");
    let mut registry = Registry::new();
    registry
        .command_handler("table", Table, descriptor("Table", "test.table.v1.State"))
        .unwrap();
    host.register(&mut registry)
        .expect("a projector child registers as a projector");
    (host, registry)
}

/// Retrying quickly, so the tests are.
fn quick() -> OnRetryable {
    OnRetryable {
        max_retries: 3,
        max_race_runs: angzarr_embedded::DEFAULT_RACE_RUNS,
        initial_backoff: Duration::from_millis(5),
        multiplier: 1.0,
        max_backoff: Duration::from_millis(5),
        jitter: 0.0,
    }
}

/// `kind: projector` launches a `ProjectorService` child, handed back as
/// a projector of its descriptor's inputs, never as a domain.
#[test]
fn a_projector_child_comes_up_as_a_projector_not_a_domain() {
    let log = log_file("projector-comes-up");
    let host = Host::start(config(vec![projector_domain(&log)])).expect("the host starts");

    let projector = host
        .projector("table-view")
        .expect("handed back as a projector");
    assert_eq!(projector.name(), "TableView");
    assert_eq!(projector.sources(), ["table"]);
    assert_eq!(projector.configured(), "table-view");
    assert!(
        format!("{projector:?}").contains("table-view"),
        "{projector:?}"
    );
    assert!(
        host.handler("table-view").is_none(),
        "a projector is not a domain"
    );
    host.shutdown();
}

/// The kernel runs the remote projector after the commit, on its source's
/// events.
#[test]
fn a_remote_projector_receives_its_sources_committed_events() {
    let log_path = log_file("projector-receives");
    let (host, registry) = projected(&log_path);
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);

    block_on(kernel.handle_any(
        &store,
        send("table", 1, any("/test.table.v1.Seat")),
        &sampled(1),
    ))
    .expect("handled");

    assert_eq!(log_lines(&log_path), ["handle /test.table.v1.Seated"]);
    host.shutdown();
}

/// A projector's bug (INTERNAL: bug, fatal) is never retried: the call
/// fails after the commit (the events stay), naming the projector and the
/// child's code, and the bug is dead-lettered.
#[test]
fn a_remote_projectors_bug_is_fatal_not_retried_and_dead_lettered() {
    let log_path = log_file("projector-bug");
    let (host, registry) = projected(&log_path);
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log).on_retryable(quick());

    let failed = block_on(kernel.handle_any(
        &store,
        send("table", 1, any("/test.table.v1.SeatBreak")),
        &sampled(1),
    ));

    assert_eq!(
        log_lines(&log_path),
        ["handle /test.table.v1.SeatedBreak"],
        "called once"
    );
    let Err(Failure::Projection {
        committed,
        failures,
        ..
    }) = &failed
    else {
        panic!("a projection failure: {:?}", failed.err());
    };
    assert!(!committed.is_empty(), "the events stay committed");
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert_eq!(failures[0].0, "TableView");
    assert!(
        failures[0].1.to_string().contains("PROJECTOR_BROKE"),
        "{}",
        failures[0].1
    );
    let lines = log.lines();
    assert_eq!(lines.len(), 1, "dead-lettered: {lines:?}");
    assert!(lines[0].contains("PROJECTOR_BROKE"), "{}", lines[0]);
    processing_failed(&log, "PROJECTOR_BROKE", 0);
    host.shutdown();
}

/// A projector has no business refusal (decision 14.41): a business
/// status from one (FAILED_PRECONDITION) is its bug, dead-lettered as a
/// fatal one, never retried.
#[test]
fn a_remote_projectors_business_status_is_its_bug() {
    let log_path = log_file("projector-business");
    let (host, registry) = projected(&log_path);
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log).on_retryable(quick());

    let failed = block_on(kernel.handle_any(
        &store,
        send("table", 1, any("/test.table.v1.SeatRefuse")),
        &sampled(1),
    ));

    assert_eq!(
        log_lines(&log_path),
        ["handle /test.table.v1.SeatedRefuse"],
        "called once"
    );
    let Err(Failure::Projection { failures, .. }) = &failed else {
        panic!("a projection failure: {:?}", failed.err());
    };
    assert!(
        failures[0].1.to_string().contains("VIEW_REFUSED"),
        "{}",
        failures[0].1
    );
    let lines = log.lines();
    assert_eq!(lines.len(), 1, "dead-lettered: {lines:?}");
    assert!(lines[0].contains("VIEW_REFUSED"), "{}", lines[0]);
    processing_failed(&log, "VIEW_REFUSED", 0);
    host.shutdown();
}

/// A retryable failure that keeps failing is retried up to the cap, then
/// dead-lettered with its retry count.
#[test]
fn a_remote_projectors_retryable_failure_is_dead_lettered_at_the_cap() {
    let log_path = log_file("projector-cap");
    let (host, registry) = projected(&log_path);
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log).on_retryable(quick());

    let failed = block_on(kernel.handle_any(
        &store,
        send("table", 1, any("/test.table.v1.SeatDown")),
        &sampled(1),
    ));

    assert_eq!(
        log_lines(&log_path).len(),
        4,
        "the first call and three retries"
    );
    assert!(
        matches!(&failed, Err(Failure::Projection { .. })),
        "{:?}",
        failed.err()
    );
    let lines = log.lines();
    assert_eq!(lines.len(), 1, "dead-lettered: {lines:?}");
    assert!(lines[0].contains("VIEW_DB_DOWN"), "{}", lines[0]);
    assert!(lines[0].contains("retry_count=3"), "{}", lines[0]);
    processing_failed(&log, "VIEW_DB_DOWN", 3);
    host.shutdown();
}

/// `log`'s one letter is the projector's failure under the child's own
/// key as answered, its events the payload, with its `retry_count`
/// (decision 14.48).
fn processing_failed(log: &MemoryDeadLetters, key: &str, retry_count: u32) {
    use angzarr_embedded::v1::angzarr_dead_letter::{Payload, Why};
    let letters = log.letters();
    assert_eq!(letters.len(), 1, "{letters:?}");
    let letter = &letters[0];
    let Some(Why::Reason(reason)) = letter.why.clone() else {
        panic!("a reason: {letter:?}");
    };
    assert_eq!(reason.code, key, "{letter:?}");
    assert_eq!(
        letter.source_component_kind(),
        angzarr_embedded::v1::ComponentKind::Projector,
        "{letter:?}"
    );
    assert_eq!(letter.retry_count, retry_count);
    assert!(
        matches!(letter.payload, Some(Payload::RejectedEvents(_))),
        "{letter:?}"
    );
}

/// With retrying off, a retryable failure is returned at once and not
/// dead-lettered, as a step's is.
#[test]
fn a_remote_projectors_retryable_failure_with_retrying_off_is_returned() {
    let log_path = log_file("projector-off");
    let (host, registry) = projected(&log_path);
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry)
        .log_with(&log)
        .on_retryable(OnRetryable {
            max_retries: 0,
            ..quick()
        });

    let failed = block_on(kernel.handle_any(
        &store,
        send("table", 1, any("/test.table.v1.SeatFlaky")),
        &sampled(1),
    ));

    assert_eq!(log_lines(&log_path).len(), 1, "called once");
    assert!(
        matches!(&failed, Err(Failure::Projection { .. })),
        "{:?}",
        failed.err()
    );
    assert!(log.lines().is_empty(), "{:?}", log.lines());
    host.shutdown();
}

// ---------------------------------------------------------------------
// The kind a child is configured as.
// ---------------------------------------------------------------------

/// A child that describes itself as another kind than its `kind` says is
/// refused at start: here a `kind: policy` child that is an aggregate.
#[test]
fn a_child_of_another_kind_than_configured_is_refused() {
    let mut policy = domain("table-wallet", "TableWalletPolicy", mode::SERVE);
    policy.kind = ComponentKindConfig::Policy;
    let refused = Host::start(config(vec![policy])).err();
    assert_eq!(
        refused,
        Some(HostError::KindMismatch {
            domain: "table-wallet".to_string(),
            configured: "policy".to_string(),
            described: "AGGREGATE".to_string(),
        })
    );
}

/// A policy or projector child that names no input domain is refused at
/// start by the kernel's validation of its descriptor, under the spec's key
/// (C-0747; decision 14.55): there is no wildcard domain.
#[test]
fn a_reader_child_with_no_input_is_refused_under_its_key() {
    use angzarr_diagnostic::Diagnostic;
    let policy_log = log_file("policy-no-input");
    let projector_log = log_file("projector-no-input");
    let mut policy = policy_domain(&policy_log);
    policy.env.remove(INPUT_ENV);
    let mut projector = projector_domain(&projector_log);
    projector.env.remove(INPUT_ENV);
    for (child, name, kind) in [
        (policy, "table-wallet", "COMPONENT_KIND_POLICY"),
        (projector, "table-view", "COMPONENT_KIND_PROJECTOR"),
    ] {
        let refused = Host::start(config(vec![child])).err();
        let Some(HostError::Descriptor { domain, cause }) = &refused else {
            panic!("refused for its descriptor: {refused:?}");
        };
        assert_eq!(domain, name);
        assert_eq!(cause.code(), "ANGZARR_REASON_NO_INPUT_DOMAIN");
        assert_eq!(
            cause,
            &angzarr_embedded::Misconfigured::NoInputDomain {
                component: if name == "table-wallet" {
                    "TableWalletPolicy"
                } else {
                    "TableView"
                }
                .to_string(),
                kind: kind.to_string(),
            }
        );
    }
}

/// A projector's retryable bug (UNAVAILABLE) and its immediate-return
/// status (ABORTED, re-read and re-decide, the runtime being the caller)
/// are both retried under the kernel's backoff: the second call succeeds,
/// and the call is answered.
#[test]
fn a_remote_projectors_retryable_and_aborted_statuses_are_retried() {
    for (n, event) in [(1, "Flaky"), (2, "Stale")] {
        let log_path = log_file(&format!("projector-retry-{event}"));
        let (host, registry) = projected(&log_path);
        let store = MemoryStore::new();
        let log = MemoryDeadLetters::new();
        let kernel = Kernel::new(&registry).log_with(&log).on_retryable(quick());

        let handled = block_on(kernel.handle_any(
            &store,
            send("table", n, any(&format!("/test.table.v1.Seat{event}"))),
            &sampled(n),
        ));

        let line = format!("handle /test.table.v1.Seated{event}");
        assert_eq!(
            log_lines(&log_path),
            [line.clone(), line],
            "{event}: retried once"
        );
        assert!(handled.is_ok(), "{event}: {:?}", handled.err());
        assert!(log.lines().is_empty(), "{event}: {:?}", log.lines());
        host.shutdown();
    }
}

/// The host validates a child's descriptor at start (decision 14.42,
/// C-0687): a policy child whose descriptor names no output domain is
/// refused, naming the component and the reason, and no child is left
/// running.
#[test]
fn a_policy_child_with_no_output_domain_is_refused_at_start() {
    let log = log_file("policy-no-output");
    let mut policy = policy_domain(&log);
    policy.env.remove(TARGET_ENV);
    let refused = Host::start(config(vec![policy])).err();
    assert_eq!(
        refused,
        Some(HostError::Descriptor {
            domain: "table-wallet".to_string(),
            cause: angzarr_embedded::Misconfigured::PolicyNoOutputDomain {
                component: "TableWalletPolicy".to_string(),
            },
        })
    );
}

/// A `kind = "upcaster"` sidecar child for `table` (decision 14.39),
/// launched by the host and serving `UpcasterService`.
fn upcaster_domain() -> ComponentConfig {
    let mut upcaster = with_env(
        domain("table-upcaster", "TableUpcaster", mode::SERVE),
        &[(KIND_ENV, kind::UPCASTER)],
    );
    upcaster.kind = ComponentKindConfig::Upcaster;
    upcaster.upcasts = Some("table".to_string());
    upcaster
}

/// The host, the in-process table and the table's remote upcaster,
/// registered.
fn upcast() -> (Host, Registry) {
    let host = Host::start(config(vec![upcaster_domain()])).expect("the host starts");
    let mut registry = Registry::new();
    registry
        .command_handler("table", Table, descriptor("Table", "test.table.v1.State"))
        .unwrap();
    host.register(&mut registry)
        .expect("an upcaster registers as its domain's sidecar");
    (host, registry)
}

/// `kind = "upcaster"` launches an `UpcasterService` child, handed back as
/// its domain's sidecar, never as a domain.
#[test]
fn an_upcaster_child_comes_up_as_its_domains_sidecar() {
    let host = Host::start(config(vec![upcaster_domain()])).expect("the host starts");
    let upcaster = host
        .upcaster("table-upcaster")
        .expect("handed back as an upcaster");
    assert_eq!(upcaster.configured(), "table-upcaster");
    assert_eq!(upcaster.upcasts(), "table");
    let shown = format!("{upcaster:?}");
    assert!(
        shown.contains("table-upcaster") && shown.contains("\"table\""),
        "{shown}"
    );
    assert!(host.handler("table-upcaster").is_none(), "not a domain");
    assert!(host.upcaster("table").is_none());
}

/// A remote upcaster sidecar upcasts its domain's inputs before the kernel
/// dispatches them (decision 14.39): an old `Sit` reaches the in-process
/// table as the current `Seat`, which it records as `Seated`.
#[test]
fn a_remote_upcaster_upcasts_its_domains_inputs_before_dispatch() {
    let (_host, registry) = upcast();
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    block_on(kernel.handle_any(
        &store,
        send("table", 1, any("/test.table.v0.Sit")),
        &sampled(1),
    ))
    .expect("handled");
    assert_eq!(
        payloads(&store, &cover("table")),
        [Some(any("/test.table.v1.Seated"))]
    );
}

/// A sidecar that dies mid-call is not serving (C-0681): the input is not
/// delivered, and its caller is answered to retry.
#[test]
fn a_remote_upcaster_that_is_down_is_retryable() {
    let (_host, registry) = upcast();
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let kernel = Kernel::new(&registry)
        .log_with(&log)
        .on_retryable(OnRetryable {
            max_retries: 0,
            ..OnRetryable::default()
        });
    let failure = block_on(kernel.handle_any(
        &store,
        send("table", 1, any("/test.table.v0.Die")),
        &sampled(1),
    ))
    .err()
    .expect("not delivered");
    assert!(
        matches!(
            &failure,
            Failure::Kernel(angzarr_embedded::KernelError::Upcast {
                error: angzarr_embedded::UpcastError::Unavailable { .. },
                ..
            })
        ),
        "{failure:?}"
    );
    assert_eq!(failure.recovery(), angzarr_embedded::Recovery::Retry);
    let Failure::Kernel(angzarr_embedded::KernelError::Upcast {
        error: angzarr_embedded::UpcastError::Unavailable { cause },
        ..
    }) = &failure
    else {
        unreachable!()
    };
    assert!(
        cause
            .args()
            .iter()
            .any(|(name, arg)| *name == "domain" && format!("{arg:?}") == "Str(\"table\")"),
        "the sidecar's domain: {cause:?}"
    );
    assert!(payloads(&store, &cover("table")).is_empty());
}

/// A rule the sidecar fails on is a bug (decision 14.39, 5.8): the
/// upcast's failure, naming the rule's reason, never retried.
#[test]
fn a_remote_upcasters_failing_rule_is_a_bug() {
    let (_host, registry) = upcast();
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let failure = block_on(Kernel::new(&registry).log_with(&log).handle_any(
        &store,
        send("table", 1, any("/test.table.v0.Bad")),
        &sampled(1),
    ))
    .err()
    .expect("not delivered");
    let Failure::Kernel(angzarr_embedded::KernelError::Upcast {
        error: angzarr_embedded::UpcastError::Rule { source, error },
        ..
    }) = &failure
    else {
        panic!("the rule's failure: {failure:?}");
    };
    assert_eq!(source, "test.table.v0.Bad");
    // The child's own code, verbatim (decision 14.60), with its args.
    assert_eq!(
        error.code(),
        "UPCAST_RULE_BROKE",
        "the child's reason: {error:?}"
    );
    assert_eq!(failure.recovery(), angzarr_embedded::Recovery::Bug);
}

/// An upcaster that names no domain to upcast is refused at start.
#[test]
fn an_upcaster_naming_no_domain_is_refused() {
    let mut upcaster = upcaster_domain();
    upcaster.upcasts = None;
    assert_eq!(
        Host::start(config(vec![upcaster])).err(),
        Some(HostError::UpcasterNoDomain {
            domain: "table-upcaster".to_string(),
        })
    );
}

/// Whether `failure` is the upcast's `Unavailable`: the sidecar is not
/// serving (C-0681).
fn not_serving<R>(failure: &Failure<R>) -> bool {
    matches!(
        failure,
        Failure::Kernel(angzarr_embedded::KernelError::Upcast {
            error: angzarr_embedded::UpcastError::Unavailable { .. },
            ..
        })
    )
}

/// A sidecar is up only while its gRPC health reports SERVING (decision
/// 14.39), as a domain is up only once described: one that connects but
/// reports NOT_SERVING is handed back, but nothing that needs it is
/// delivered (C-0681); once it turns SERVING it is used; once it turns
/// NOT_SERVING again it is not. Each turn is forced through the fake's
/// control socket, and the supervisor's acting on it awaited, never
/// polled for: the timeout is a safety net, never the way the test passes.
#[test]
fn a_sidecar_is_up_only_while_its_health_reports_serving() {
    let socket = TestFile::new("angzarr-ctl", "c.sock");
    let control = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    let sidecar = with_env(
        upcaster_domain(),
        &[
            (HEALTH_ENV, health::NOT_SERVING),
            (CONTROL_ENV, socket.to_str().unwrap()),
        ],
    );
    let (applied, acted) = std::sync::mpsc::channel();
    let hooks = Hooks {
        applied: Some(applied),
        ..Hooks::default()
    };
    let host = Host::start_with(config(vec![sidecar]), hooks).expect("the host starts");
    let mut registry = Registry::new();
    registry
        .command_handler("table", Table, descriptor("Table", "test.table.v1.State"))
        .unwrap();
    host.register(&mut registry).unwrap();
    let (mut fake, _) = control.accept().unwrap();
    let _ = std::fs::remove_file(&socket);
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let kernel = Kernel::new(&registry)
        .log_with(&log)
        .on_retryable(OnRetryable {
            max_retries: 0,
            ..OnRetryable::default()
        });
    // Command `n`, an old `Sit`, to the table: how it failed, if it did.
    let sit = |n| {
        block_on(kernel.handle_any(
            &store,
            send("table", n, any("/test.table.v0.Sit")),
            &sampled(n),
        ))
        .err()
    };
    let mut turned = |status: &[u8]| {
        use std::io::Write;
        if !status.is_empty() {
            fake.write_all(status).unwrap();
        }
        acted.recv_timeout(Duration::from_secs(20))
    };

    assert_eq!(turned(b""), Ok(false), "came up NOT_SERVING");
    let refused = sit(1).expect("not delivered");
    assert!(not_serving(&refused), "{refused:?}");
    assert_eq!(refused.recovery(), angzarr_embedded::Recovery::Retry);
    assert!(payloads(&store, &cover("table")).is_empty());

    assert_eq!(turned(b"1"), Ok(true), "turned SERVING");
    let delivered = sit(2);
    assert!(delivered.is_none(), "delivered, upcast: {delivered:?}");
    assert_eq!(
        payloads(&store, &cover("table")),
        [Some(any("/test.table.v1.Seated"))]
    );

    assert_eq!(turned(b"0"), Ok(false), "turned NOT_SERVING");
    let refused = sit(3).expect("not delivered");
    assert!(not_serving(&refused), "{refused:?}");
    assert_eq!(payloads(&store, &cover("table")).len(), 1);
}

/// A sidecar is up only while its health reports SERVING, not merely while
/// it does not report NOT_SERVING: one that reports UNKNOWN or
/// SERVICE_UNKNOWN is handed back down, and nothing that needs it is
/// delivered. The supervisor's acting on the status is awaited, never
/// polled for.
#[test]
fn a_sidecar_whose_health_is_unknown_is_not_up() {
    for status in [health::UNKNOWN, health::SERVICE_UNKNOWN] {
        let sidecar = with_env(upcaster_domain(), &[(HEALTH_ENV, status)]);
        let (applied, acted) = std::sync::mpsc::channel();
        let hooks = Hooks {
            applied: Some(applied),
            ..Hooks::default()
        };
        let host = Host::start_with(config(vec![sidecar]), hooks).expect("the host starts");
        let mut registry = Registry::new();
        registry
            .command_handler("table", Table, descriptor("Table", "test.table.v1.State"))
            .unwrap();
        host.register(&mut registry).unwrap();
        let log = MemoryDeadLetters::new();
        let store = MemoryStore::new();
        let kernel = Kernel::new(&registry)
            .log_with(&log)
            .on_retryable(OnRetryable {
                max_retries: 0,
                ..OnRetryable::default()
            });

        assert_eq!(
            acted.recv_timeout(Duration::from_secs(20)),
            Ok(false),
            "came up {status}"
        );
        let refused = block_on(kernel.handle_any(
            &store,
            send("table", 1, any("/test.table.v0.Sit")),
            &sampled(1),
        ))
        .err()
        .expect("not delivered");
        assert!(not_serving(&refused), "{status}: {refused:?}");
        assert!(payloads(&store, &cover("table")).is_empty(), "{status}");
    }
}

/// A sidecar that serves no gRPC health is never ready: refused at start
/// once its start limit has passed, `EMBEDDED_REASON_CHILD_NOT_SERVING`
/// with no status (decision 14.80). Its limit is short: it is waited out.
#[test]
fn a_sidecar_serving_no_health_is_refused_at_start() {
    let sidecar = with_env(upcaster_domain(), &[(HEALTH_ENV, health::NONE)]);
    let mut config = config(vec![sidecar]);
    config.supervision.handshake_timeout_ms = 300;
    assert_eq!(
        Host::start(config).err(),
        Some(HostError::NotServing {
            component: "table-upcaster".to_string(),
            status: "none".to_string(),
            timeout_ms: 300,
        })
    );
}
