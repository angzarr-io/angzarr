//! The host's remote domains through angzarr-embedded's kernel: a command
//! for the remote domain handled by `Kernel::handle_any`, its events
//! written to the store; and a policy step from an in-process domain into
//! the remote one.

use angzarr_diagnostic::{Arg, Diagnostic};
use angzarr_embedded::angzarr_router::error::CodedError;
use angzarr_embedded::angzarr_router::hpb;
use angzarr_embedded::angzarr_router::policy::PolicyDispatch;
use angzarr_embedded::angzarr_router::router::CommandHandler;
use angzarr_embedded::v1::{
    command_page, component_descriptor, AggregateDescriptor, Command, CommandBook, CommandPage,
    ComponentDescriptor, Cover, EventPage, StateDescriptor, Uuid,
};
use angzarr_embedded::{
    Any, EventStore, Failure, Handled, Kernel, KernelError, MemoryDeadLetters, Misconfigured,
    Recovery, Registry, Sampled, StreamRef, Timestamp, DEAD_LETTER_PREFIX,
};
use angzarr_ports::memory::MemoryStore;

use super::Host;
use crate::fake_child::{config, domain, mode, noted};

fn root() -> Uuid {
    Uuid::from_bytes([0x0a; 16])
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

fn sampled(n: u8) -> Sampled {
    Sampled {
        now: Timestamp {
            seconds: i64::from(n),
            nanos: 0,
        },
        command_id: Uuid::from_bytes([0x70 | n; 16]),
    }
}

fn pages(store: &MemoryStore, stream: &Cover) -> Vec<EventPage> {
    let stream = StreamRef::try_from(stream).unwrap();
    block_on(store.read(stream, 0, None)).unwrap().pages
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

fn state(type_name: &str) -> Option<StateDescriptor> {
    Some(StateDescriptor {
        type_name: type_name.to_string(),
    })
}

/// The domain events of `pages`, in order.
fn events(pages: &[EventPage]) -> Vec<Option<Any>> {
    (pages.iter())
        .map(|p| angzarr_embedded::angzarr_router::page_event(p).cloned())
        .collect()
}

/// An in-process Rust domain, `table`, hand-written as the router's
/// `CommandHandler`: every command is answered with one `Seated` event.
struct Table;

const SEATED: &str = "test.table.v1.Seated";

impl CommandHandler for Table {
    fn name(&self) -> &str {
        "Table"
    }

    fn command_types(&self) -> Vec<String> {
        vec!["test.table.v1.Seat".to_string()]
    }

    async fn dispatch(&self, _: &hpb::HandleRequest) -> Result<hpb::HandleResponse, CodedError> {
        Ok(hpb::HandleResponse {
            events: vec![any(&format!("/{SEATED}"))],
            state: None,
            rules_version: "table-1".to_string(),
        })
    }
}

/// [`Table`]'s descriptor, as angzarr-build generates it.
fn table_descriptor() -> ComponentDescriptor {
    descriptor_in("table")
}

/// [`Table`]'s descriptor, its domain `domain`.
fn descriptor_in(domain: &str) -> ComponentDescriptor {
    ComponentDescriptor {
        name: "Table".to_string(),
        rules_version: "table-1".to_string(),
        kind: Some(component_descriptor::Kind::Aggregate(AggregateDescriptor {
            domain: domain.to_string(),
            state: state("test.table.v1.State"),
        })),
    }
}

fn host() -> Host {
    Host::start(config(vec![domain(
        "player",
        "PlayerAggregate",
        mode::SERVE,
    )]))
    .expect("the host starts")
}

/// A command for the remote domain, handled through the kernel: the
/// child decides, and its events land on the domain's stream, stamped with
/// its rules version; the next command reaches the child with them.
#[test]
fn a_remote_command_round_trips_through_the_kernel_into_the_store() {
    let host = host();
    let mut registry = Registry::new();
    host.register(&mut registry).unwrap();
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let rules_version = host
        .handler("player")
        .unwrap()
        .descriptor()
        .rules_version
        .clone();

    let handled = block_on(kernel.handle_any(
        &store,
        send("player", 1, any("/test.player.v1.SitDown")),
        &sampled(1),
    ))
    .expect("handled");
    assert!(matches!(handled, Handled::Applied(_)));
    block_on(kernel.handle_any(
        &store,
        send("player", 2, any("/test.player.v1.Bet")),
        &sampled(2),
    ))
    .expect("handled");

    let written = pages(&store, &cover("player"));
    assert_eq!(events(&written), [Some(noted(0)), Some(noted(1))]);
    assert_eq!(
        written[0].meta.as_ref().map(|m| m.rules_version.as_str()),
        Some(rules_version.as_str())
    );
    host.shutdown();
}

/// A policy from the in-process `table` into the remote `player`: the
/// table's event sets off a step the kernel dispatches through the child,
/// and both streams are written.
#[test]
fn a_policy_step_from_an_in_process_domain_reaches_the_remote_one() {
    let host = host();
    let mut registry = Registry::new();
    registry
        .command_handler("table", Table, table_descriptor())
        .unwrap();
    host.register(&mut registry).unwrap();
    registry
        .policy_dispatch(
            PolicyDispatch::new("table-player", "table", "player").on_event(SEATED, |_, page| {
                Ok(vec![angzarr_embedded::angzarr_router::policy::command(
                    page.cover.and_then(|c| c.root.clone()),
                    any("/test.player.v1.SitDown"),
                )])
            }),
        )
        .unwrap();
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);

    let handled = block_on(kernel.handle_any(
        &store,
        send("table", 1, any("/test.table.v1.Seat")),
        &sampled(1),
    ))
    .expect("handled");
    assert_eq!(
        handled.books().len(),
        2,
        "the table's step and the player's"
    );

    assert_eq!(
        events(&pages(&store, &cover("table"))),
        [Some(any(&format!("/{SEATED}")))]
    );
    assert_eq!(events(&pages(&store, &cover("player"))), [Some(noted(0))]);
    host.shutdown();
}

/// A child's FAILED_PRECONDITION is the domain's refusal, through the
/// kernel: a business rejection under its own code and args.
#[test]
fn a_remote_refusal_is_a_business_rejection() {
    let host = host();
    let mut registry = Registry::new();
    host.register(&mut registry).unwrap();
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let refused = block_on(kernel.handle_any(
        &store,
        send("player", 1, any("/test.player.v1.Refuse")),
        &sampled(1),
    ))
    .err()
    .expect("refused");
    let Failure::Rejected(rejected) = &refused else {
        panic!("a business rejection: {refused:?}");
    };
    assert_eq!(rejected.code(), "TABLE_FULL");
    assert!(
        matches!(rejected.args().as_slice(), [("seats", Arg::Str(v))] if v == "6"),
        "its args: {rejected:?}"
    );
    assert_eq!(refused.recovery(), Recovery::Business);
    assert!(log.lines().is_empty(), "{:?}", log.lines());
    host.shutdown();
}

/// A child's ALREADY_EXISTS under a code of its own is a business refusal
/// through the kernel (the owner's ruling, 2026-10-29), though
/// angzarr-router's `GrpcCode` has no variant for it: a rejection under
/// its code and args, with its typed refusal, nothing dead-lettered.
#[test]
fn a_remote_already_exists_is_a_business_rejection() {
    let host = host();
    let mut registry = Registry::new();
    host.register(&mut registry).unwrap();
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let refused = block_on(kernel.handle_any(
        &store,
        send("player", 1, any("/test.player.v1.Taken")),
        &sampled(1),
    ))
    .err()
    .expect("refused");
    let Failure::Rejected(rejected) = &refused else {
        panic!("a business rejection: {refused:?}");
    };
    assert_eq!(rejected.code(), "SEAT_TAKEN");
    assert!(
        matches!(rejected.args().as_slice(), [("seat", Arg::Str(v))] if v == "3"),
        "its args: {rejected:?}"
    );
    assert_eq!(
        rejected.refusal_object(),
        &crate::fake_child::refusal(),
        "its typed refusal"
    );
    assert_eq!(refused.recovery(), Recovery::Business);
    assert!(log.lines().is_empty(), "{:?}", log.lines());
    host.shutdown();
}

/// A child's INTERNAL under a code of its own, one the router does not
/// know, is a bug through the kernel, never a rejection: the router's
/// error, `Recovery::Bug`, dead-lettered by its code and args.
#[test]
fn a_remote_internal_failure_under_its_own_code_is_a_dead_lettered_bug() {
    let host = host();
    let mut registry = Registry::new();
    host.register(&mut registry).unwrap();
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let failed = block_on(kernel.handle_any(
        &store,
        send("player", 1, any("/test.player.v1.Break")),
        &sampled(1),
    ))
    .err()
    .expect("failed");
    assert!(
        matches!(&failed, Failure::Kernel(KernelError::Router(e)) if e.code == "PLAYER_BROKE"),
        "{failed:?}"
    );
    assert_eq!(failed.recovery(), Recovery::Bug);
    let lines = log.lines();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(lines[0].starts_with(DEAD_LETTER_PREFIX), "{}", lines[0]);
    assert!(
        lines[0].contains("player: PLAYER_BROKE seat=3"),
        "the child's own key and args, as answered (decision 14.48): {}",
        lines[0]
    );
    let letters = log.letters();
    let Some(angzarr_embedded::v1::angzarr_dead_letter::Why::Reason(reason)) =
        letters[0].why.clone()
    else {
        panic!("a reason: {letters:?}");
    };
    assert_eq!(reason.code, "PLAYER_BROKE");
    assert_eq!(letters[0].source_component, "player");
    assert_eq!(
        letters[0].source_component_kind(),
        angzarr_embedded::v1::ComponentKind::Aggregate,
        "the child's kind, as its descriptor says"
    );
    assert_eq!(letters[0].retry_count, 0, "a fatal bug: never retried");
    assert!(
        pages(&store, &cover("player")).is_empty(),
        "nothing written"
    );
    assert!(
        lines[0].contains(&Uuid::from_bytes([1; 16]).to_string()),
        "the command: {}",
        lines[0]
    );
    host.shutdown();
}

/// Registering is by domain, refused as the registry refuses: a domain
/// registered already.
#[test]
fn register_is_refused_for_a_domain_registered_already() {
    let host = host();
    let mut registry = Registry::new();
    registry
        .command_handler("player", Table, descriptor_in("player"))
        .unwrap();
    assert_eq!(
        host.register(&mut registry),
        Err(Misconfigured::AlreadyRegistered {
            domain: "player".to_string(),
        })
    );
    host.shutdown();
}

/// C-0563: a child that restarts is described again, and its new
/// descriptor is what the domain is registered by before the new child
/// gets any work. The fake reports `pid-<n>`, so every child is a new
/// build. The restart is forced (the child dies mid-call) and awaited by
/// the supervisor's count of children published, never polled for: at the
/// moment the second child is published, the registry already holds its
/// version, and the next command's events are stamped with it.
#[test]
fn a_restarted_child_is_registered_at_its_new_version_before_it_gets_work() {
    let host = host();
    let mut registry = Registry::new();
    host.register(&mut registry).unwrap();
    let handler = host.handler("player").unwrap();
    let first = registry
        .catalog()
        .rules_version("player")
        .map(|v| v.to_string())
        .expect("registered");
    let mut generations = handler.generations();
    let died = block_on(handler.dispatch(&hpb::HandleRequest {
        input: Some(send("player", 9, any("/test.player.v1.Die"))),
        ..Default::default()
    }));
    assert!(died.is_err(), "the child died mid-call: {died:?}");
    // The timeout is a safety net for a broken restart, never the way the
    // test passes.
    let published = block_on(async {
        tokio::time::timeout(
            std::time::Duration::from_secs(20),
            generations.wait_for(|n| *n >= 2),
        )
        .await
        .map(|seen| seen.is_ok())
    });
    assert_eq!(published, Ok(true), "the child was restarted");
    let registered = registry
        .catalog()
        .rules_version("player")
        .map(|v| v.to_string())
        .expect("registered");
    assert_ne!(
        registered, first,
        "the new child's descriptor is registered before it is published"
    );

    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    block_on(kernel.handle_any(
        &store,
        send("player", 1, any("/test.player.v1.SitDown")),
        &sampled(1),
    ))
    .expect("handled");
    let written = pages(&store, &cover("player"));
    assert_eq!(
        written[0].meta.as_ref().map(|m| m.rules_version.as_str()),
        Some(registered.as_str()),
        "stamped with the build that decided"
    );
    assert_eq!(
        host.handler("player").unwrap().descriptor().rules_version,
        registered,
        "the handler's descriptor is the new child's"
    );
    host.shutdown();
}
