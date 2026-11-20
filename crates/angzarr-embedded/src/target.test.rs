//! The routing seam, red (design-by-test, 2026-10-30): a domain the
//! registry routes to a coordinator elsewhere ([`Registry::remote`]) is
//! handled there, for the application's own command and for a policy's
//! step alike, and its answer comes back as a local domain's would.
//!
//! The coordinator here is a fake [`CoordinatorPort`]: it records every
//! book (`Handle`) and reaction (`Deliver`) it is handed and answers with
//! what the test gave it.

use std::sync::{Arc, Mutex};

use angzarr_router::error::{BusinessStatus, CodedError};
use angzarr_router::hpb::{HandleRequest, HandleResponse, ProjectRequest, ProjectResponse};
use angzarr_router::policy::PolicyDispatch;
use angzarr_router::router::CommandHandler;

use super::*;
use crate::toys::prelude::*;
use crate::toys::{at, command, command_id, cover, Edit};
use crate::v1::{
    command_page, component_descriptor, reaction_delivery, AggregateDescriptor, Command,
    CommandBook, ComponentDescriptor, ComponentKind, DeliveryOutcome, EventBook, EventPage,
    ReactionDelivery, StateDescriptor,
};
use crate::{
    pack, Any, Failure, Handled, Kernel, KernelError, Malformed, MemoryDeadLetters, Misconfigured,
    Registry,
};
use angzarr_diagnostic::Diagnostic;

fn any(type_url: &str) -> Any {
    Any {
        type_url: type_url.to_string(),
        value: Vec::new(),
    }
}

/// What the fake coordinator answers.
#[derive(Clone)]
enum Answer {
    Events(DeliveryOutcome, Vec<Any>),
    Refused(CodedError),
}

/// A coordinator elsewhere, owning the `bank` domain: every book and
/// reaction it is handed, in order, and one answer for all of them.
#[derive(Clone)]
struct FakeCoordinator {
    handed: Arc<Mutex<Vec<CommandBook>>>,
    delivered: Arc<Mutex<Vec<ReactionDelivery>>>,
    answer: Answer,
}

impl FakeCoordinator {
    fn answering(answer: Answer) -> FakeCoordinator {
        FakeCoordinator {
            handed: Arc::default(),
            delivered: Arc::default(),
            answer,
        }
    }

    /// The books handed by `Handle`.
    fn handed(&self) -> Vec<CommandBook> {
        self.handed.lock().unwrap().clone()
    }

    /// The reactions handed by `Deliver`.
    fn delivered(&self) -> Vec<ReactionDelivery> {
        self.delivered.lock().unwrap().clone()
    }

    /// The test's answer, for a call on `cover`.
    fn answer(&self, cover: Option<crate::v1::Cover>) -> Coordinated<'_> {
        let answer = self.answer.clone();
        Box::pin(async move {
            match answer {
                Answer::Refused(error) => Err(error),
                Answer::Events(delivery, payloads) => Ok(Answered {
                    events: Some(EventBook {
                        cover,
                        pages: payloads
                            .into_iter()
                            .enumerate()
                            .map(|(n, payload)| EventPage {
                                sequence: n as u64,
                                body: Some(crate::v1::event_page::Body::Event(payload)),
                                ..EventPage::default()
                            })
                            .collect(),
                        ..EventBook::default()
                    }),
                    delivery,
                }),
            }
        })
    }
}

impl CoordinatorPort for FakeCoordinator {
    fn handle(&self, book: CommandBook) -> Coordinated<'_> {
        let cover = book.cover.clone();
        self.handed.lock().unwrap().push(book);
        self.answer(cover)
    }

    fn deliver(&self, delivery: ReactionDelivery) -> Coordinated<'_> {
        let cover = delivery.cover.clone();
        self.delivered.lock().unwrap().push(delivery);
        self.answer(cover)
    }
}

/// The remote domain's events, as its coordinator persisted them.
fn deposited() -> Vec<Any> {
    vec![any("/toy.bank.v1.Deposited")]
}

/// An in-process domain, `till`, hand-written as the router's
/// `CommandHandler`: every command is answered with one `Rung` event.
struct Till;

const RUNG: &str = "toy.till.v1.Rung";

impl CommandHandler for Till {
    fn name(&self) -> &str {
        "Till"
    }

    fn command_types(&self) -> Vec<String> {
        vec!["toy.till.v1.Ring".to_string()]
    }

    async fn dispatch(&self, _: &HandleRequest) -> Result<HandleResponse, CodedError> {
        Ok(HandleResponse {
            events: vec![any(&format!("/{RUNG}"))],
            state: None,
            rules_version: "till-1".to_string(),
        })
    }
}

fn till_descriptor() -> ComponentDescriptor {
    descriptor_of("till")
}

/// The till's descriptor, as a domain named `domain`.
fn descriptor_of(domain: &str) -> ComponentDescriptor {
    ComponentDescriptor {
        name: "Till".to_string(),
        rules_version: "till-1".to_string(),
        kind: Some(component_descriptor::Kind::Aggregate(AggregateDescriptor {
            domain: domain.to_string(),
            state: Some(StateDescriptor {
                type_name: "toy.till.v1.State".to_string(),
            }),
        })),
    }
}

/// `bank` routed to `coordinator`, `till` here.
fn registry_with(coordinator: &FakeCoordinator) -> Registry {
    let mut registry = Registry::new();
    registry
        .command_handler("till", Till, till_descriptor())
        .unwrap();
    registry.remote("bank", coordinator.clone()).unwrap();
    registry
}

fn deposit(n: u128) -> CommandBook {
    let mut book = command(cover("bank", 1), any("/toy.bank.v1.Deposit"));
    if let Some(page) = book.page.as_mut() {
        page.command_id = Some(command_id(n));
    }
    book
}

/// The registry resolves each name to where it is handled: a local
/// handler, a remote coordinator, or nothing.
#[test]
fn a_domain_resolves_to_a_local_handler_or_a_remote_coordinator() {
    let coordinator = FakeCoordinator::answering(Answer::Events(DeliveryOutcome::Applied, vec![]));
    let registry = registry_with(&coordinator);
    assert!(
        matches!(registry.target("till"), Some(Target::Local(h)) if h.name() == "Till"),
        "{:?}",
        registry.target("till")
    );
    assert!(
        matches!(registry.target("bank"), Some(Target::Remote(_))),
        "{:?}",
        registry.target("bank")
    );
    assert!(registry.target("nowhere").is_none());
}

/// One domain is handled in one place: routing a domain registered here
/// to a coordinator elsewhere, or routing it twice, is refused.
#[test]
fn a_domain_is_either_local_or_remote_never_both() {
    let coordinator = FakeCoordinator::answering(Answer::Events(DeliveryOutcome::Applied, vec![]));
    let mut registry = registry_with(&coordinator);
    let taken = |domain: &str| {
        Some(Misconfigured::AlreadyRegistered {
            domain: domain.to_string(),
        })
    };
    assert_eq!(
        registry.remote("till", coordinator.clone()).err(),
        taken("till")
    );
    assert_eq!(
        registry.remote("bank", coordinator.clone()).err(),
        taken("bank")
    );
    assert_eq!(
        registry
            .command_handler("bank", Till, descriptor_of("bank"))
            .err(),
        taken("bank")
    );
}

/// The application's command for a remote domain reaches its coordinator,
/// untouched but for the ids the kernel fixed, and the coordinator's
/// events come back as a local domain's would: applied, one book, the
/// stream's. Nothing of it is written here.
#[test]
fn a_command_for_a_remote_domain_reaches_its_coordinator_and_answers_as_if_local() {
    let coordinator =
        FakeCoordinator::answering(Answer::Events(DeliveryOutcome::Applied, deposited()));
    let registry = registry_with(&coordinator);
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let handled = Kernel::new(&registry)
        .log_with(&log)
        .handle_any(&store, deposit(1), &at(1))
        .ready()
        .expect("handled remotely");

    let handed = coordinator.handed();
    assert_eq!(handed.len(), 1, "{handed:?}");
    assert_eq!(handed[0].cover, Some(cover("bank", 1)));
    let page = handed[0].page.as_ref().unwrap();
    assert_eq!(page.command_id, Some(command_id(1)));
    assert!(
        matches!(&page.body, Some(command_page::Body::Command(Command { payload: Some(p), handling: None }))
            if p.type_url == "/toy.bank.v1.Deposit"),
        "{page:?}"
    );

    let Handled::Applied(recorded) = &handled else {
        panic!("applied, as a local domain's command is");
    };
    let books = recorded.books();
    assert_eq!(books.len(), 1, "{books:?}");
    assert_eq!(books[0].cover, Some(cover("bank", 1)));
    let payloads: Vec<_> = books[0]
        .pages
        .iter()
        .map(|p| crate::angzarr_router::page_event(p).cloned())
        .collect();
    assert_eq!(
        payloads,
        deposited().into_iter().map(Some).collect::<Vec<_>>()
    );
    assert!(
        pages(&store, &cover("bank", 1)).is_empty(),
        "a remote domain's stream is its coordinator's, not written here"
    );
}

/// A command with no id is handed to the coordinator with the one the
/// application sampled: fixed here, before the network, so a retry of the
/// same call is deduplicated there.
#[test]
fn a_remote_command_is_sent_with_the_id_sampled_here() {
    let coordinator =
        FakeCoordinator::answering(Answer::Events(DeliveryOutcome::Applied, deposited()));
    let registry = registry_with(&coordinator);
    let log = MemoryDeadLetters::new();
    let sampled = at(7);
    Kernel::new(&registry)
        .log_with(&log)
        .handle_any(
            &MemoryStore::new(),
            command(cover("bank", 1), any("/toy.bank.v1.Deposit")),
            &sampled,
        )
        .ready()
        .expect("handled remotely");
    let handed = coordinator.handed();
    assert_eq!(
        handed[0].page.as_ref().and_then(|p| p.command_id.clone()),
        Some(sampled.command_id)
    );
}

/// The coordinator's DUPLICATE is the kernel's duplicate, with its events.
#[test]
fn a_remote_duplicate_is_a_duplicate() {
    let coordinator =
        FakeCoordinator::answering(Answer::Events(DeliveryOutcome::Duplicate, deposited()));
    let registry = registry_with(&coordinator);
    let log = MemoryDeadLetters::new();
    let handled = Kernel::new(&registry)
        .log_with(&log)
        .handle_any(&MemoryStore::new(), deposit(1), &at(1))
        .ready()
        .expect("handled remotely");
    let Handled::Duplicate(book, _) = handled else {
        panic!("a duplicate, as a local stream's is");
    };
    assert_eq!(book.pages.len(), 1);
}

/// The coordinator's typed refusal is the caller's rejection, its object
/// and status as sent, as a local handler's is; nothing dead-lettered.
#[test]
fn a_remote_refusal_is_the_callers_rejection() {
    let object = pack(&crate::toys::ToyRefusal {
        code: "BANK_CLOSED".to_string(),
    });
    let refusal = CodedError::refused(BusinessStatus::FailedPrecondition, object.clone());
    let coordinator = FakeCoordinator::answering(Answer::Refused(refusal));
    let registry = registry_with(&coordinator);
    let log = MemoryDeadLetters::new();
    let failed = Kernel::new(&registry)
        .log_with(&log)
        .handle_any(&MemoryStore::new(), deposit(1), &at(1))
        .ready()
        .err()
        .expect("refused");
    let Failure::Rejected(rejected) = &failed else {
        panic!("refused: {failed:?}");
    };
    assert_eq!(rejected.code(), "ANGZARR_REASON_COMMAND_REFUSED");
    assert_eq!(rejected.refusal_object(), &object);
    assert_eq!(rejected.status(), BusinessStatus::FailedPrecondition);
    assert!(
        rejected.rejection::<crate::toys::ToyRefusal>().is_none(),
        "a refusal from another process is its object alone"
    );
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

/// A framework key under a business status from the coordinator is no
/// refusal (decision 14.95: only a typed refusal is one): the caller is
/// answered it as the router's error, its status kept; nothing
/// dead-lettered.
#[test]
fn a_remote_framework_key_under_a_business_status_is_no_refusal() {
    let not_forked = crate::spec_coded(crate::v1::AngzarrReason::EditionNotForked, []);
    let coordinator = FakeCoordinator::answering(Answer::Refused(not_forked.clone()));
    let registry = registry_with(&coordinator);
    let log = MemoryDeadLetters::new();
    let failed = Kernel::new(&registry)
        .log_with(&log)
        .handle_any(&MemoryStore::new(), deposit(1), &at(1))
        .ready()
        .err()
        .expect("answered");
    assert_eq!(failed, Failure::Kernel(KernelError::Router(not_forked)));
    assert_eq!(failed.recovery(), crate::Recovery::Business);
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

/// The coordinator's bug is dead-lettered here, as a local handler's is,
/// from the remote domain; of no kind known here, its descriptor being
/// its coordinator's: `source_component_kind` UNSPECIFIED.
#[test]
fn a_remote_bug_is_dead_lettered_from_its_domain_of_no_known_kind() {
    let bug = CodedError::bug("BANK_BROKE", [("seat".to_string(), "3".to_string())]);
    let coordinator = FakeCoordinator::answering(Answer::Refused(bug));
    let registry = registry_with(&coordinator);
    let log = MemoryDeadLetters::new();
    let failed = Kernel::new(&registry)
        .log_with(&log)
        .handle_any(&MemoryStore::new(), deposit(1), &at(1))
        .ready()
        .err()
        .expect("a bug");
    assert!(
        matches!(&failed, Failure::Kernel(KernelError::Router(_))),
        "{failed:?}"
    );
    let letters = log.letters();
    assert_eq!(letters.len(), 1, "{letters:?}");
    assert_eq!(letters[0].source_component, "bank");
    assert_eq!(
        letters[0].source_component_kind(),
        ComponentKind::Unspecified
    );
}

/// A domain neither here nor remote is refused as before: the unknown
/// domain, and no coordinator is asked.
#[test]
fn a_domain_neither_here_nor_remote_is_unknown() {
    let coordinator = FakeCoordinator::answering(Answer::Events(DeliveryOutcome::Applied, vec![]));
    let registry = registry_with(&coordinator);
    let log = MemoryDeadLetters::new();
    let failed = Kernel::new(&registry)
        .log_with(&log)
        .handle_any(
            &MemoryStore::new(),
            command(cover("nowhere", 1), any("/toy.x.v1.X")),
            &at(1),
        )
        .ready()
        .err()
        .expect("refused");
    assert!(
        matches!(
            &failed,
            Failure::Kernel(KernelError::Malformed(Malformed::UnknownDomain { domain }))
                if domain == "nowhere"
        ),
        "{failed:?}"
    );
    assert!(coordinator.handed().is_empty());
}

/// A policy from the till into the bank: each Rung deposits into the bank
/// stream of the till's root.
fn till_to_bank() -> PolicyDispatch {
    PolicyDispatch::new("till-bank", "till", "bank").on_event(RUNG, |_, source| {
        Ok(vec![angzarr_router::policy::command(
            source.cover.and_then(|c| c.root.clone()),
            any("/toy.bank.v1.Deposit"),
        )])
    })
}

/// A policy from the local `till` into the remote `bank`: the till's event
/// sets off a step the kernel hands to the bank's coordinator as a
/// reaction (`Deliver`; decision 14.79), with the step's derived command
/// id, the trigger its causation and the policy its component; the chain
/// records both streams' events, and the policy's position on the till
/// moves past its trigger.
#[test]
fn a_policy_step_into_a_remote_domain_goes_to_its_coordinator() {
    let coordinator =
        FakeCoordinator::answering(Answer::Events(DeliveryOutcome::Applied, deposited()));
    let mut registry = registry_with(&coordinator);
    registry.policy_dispatch(till_to_bank()).unwrap();
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let ring = command(cover("till", 1), any("/toy.till.v1.Ring"));
    let handled = Kernel::new(&registry)
        .log_with(&log)
        .handle_any(&store, ring, &at(1))
        .ready()
        .expect("handled");

    assert!(
        coordinator.handed().is_empty(),
        "a reaction is no client's Handle"
    );
    let delivered = coordinator.delivered();
    assert_eq!(delivered.len(), 1, "{delivered:?}");
    assert_eq!(delivered[0].cover, Some(cover("bank", 1)));
    assert!(
        delivered[0].command_id.is_some(),
        "the step's derived command id is fixed here"
    );
    assert_eq!(delivered[0].component, "till-bank");
    assert_eq!(
        delivered[0].causation,
        Some(crate::v1::EventRef {
            stream: Some(cover("till", 1)),
            sequence: 0,
        })
    );
    assert!(matches!(
        &delivered[0].body,
        Some(reaction_delivery::Body::Command(p)) if p.type_url == "/toy.bank.v1.Deposit"
    ));
    let books = handled.books();
    let covers: Vec<_> = books.iter().map(|b| b.cover.clone()).collect();
    assert_eq!(covers, [Some(cover("till", 1)), Some(cover("bank", 1))]);
    assert_eq!(position(&store, "till-bank", &cover("till", 1)), 1);
    assert!(pages(&store, &cover("bank", 1)).is_empty());
}

/// A policy of the till's events, into nothing it ever reaches.
fn till_policy(name: &str, source: &str) -> PolicyDispatch {
    PolicyDispatch::new(name, source, "till").on_event(RUNG, |_, _| Ok(vec![]))
}

/// A projector, as the router's `ProjectorHandler`, that projects nothing.
struct Quiet;

impl angzarr_router::router::ProjectorHandler for Quiet {
    fn name(&self) -> &str {
        "quiet"
    }

    async fn dispatch(&self, _: &ProjectRequest) -> Result<ProjectResponse, CodedError> {
        Ok(ProjectResponse::default())
    }
}

/// Remote stays remote (owner ruling): a remote domain's events are its
/// coordinator's, never written here, so a policy here of them would
/// never run. Registering one after the domain is routed remote is
/// refused, naming both.
#[test]
fn a_policy_of_a_remote_domains_events_is_refused() {
    let coordinator = FakeCoordinator::answering(Answer::Events(DeliveryOutcome::Applied, vec![]));
    let mut registry = registry_with(&coordinator);
    let refused = registry
        .policy_dispatch(till_policy("bank-till", "bank"))
        .err()
        .map(|e| e.to_string());
    assert_eq!(
        refused.as_deref(),
        Some("ANGZARR_REASON_REMOTE_SOURCE component=bank-till domain=bank")
    );
}

/// The same, a projector of a remote domain's events, among its sources.
#[test]
fn a_projector_of_a_remote_domains_events_is_refused() {
    let coordinator = FakeCoordinator::answering(Answer::Events(DeliveryOutcome::Applied, vec![]));
    let mut registry = registry_with(&coordinator);
    let refused = registry
        .projector_handler(&["till", "bank"], Quiet)
        .err()
        .map(|e| e.to_string());
    assert_eq!(
        refused.as_deref(),
        Some("ANGZARR_REASON_REMOTE_SOURCE component=quiet domain=bank")
    );
}

/// In either order: routing a domain remote once a policy or projector
/// here handles its events is refused, naming the domain and the
/// component; a policy of a local domain is not.
#[test]
fn routing_remote_a_domain_whose_events_a_component_here_handles_is_refused() {
    let coordinator = FakeCoordinator::answering(Answer::Events(DeliveryOutcome::Applied, vec![]));
    let mut registry = Registry::new();
    registry
        .policy_dispatch(till_policy("bank-till", "bank"))
        .unwrap();
    registry
        .projector_handler(&["till", "ledger"], Quiet)
        .unwrap();
    registry
        .policy_dispatch(till_policy("till-till", "till"))
        .unwrap();
    assert_eq!(
        registry.remote("bank", coordinator.clone()).err(),
        Some(Misconfigured::RemoteSource {
            domain: "bank".to_string(),
            component: "bank-till".to_string(),
        })
    );
    assert_eq!(
        registry.remote("ledger", coordinator.clone()).err(),
        Some(Misconfigured::RemoteSource {
            domain: "ledger".to_string(),
            component: "quiet".to_string(),
        })
    );
    assert!(registry.remote("vault", coordinator.clone()).is_ok());
    assert!(registry.target("bank").is_none(), "not routed remote");
}

/// Remote defaults are the remote coordinator's (owner ruling): a command
/// the caller sent with no correlation is handed to the coordinator with
/// none, so it applies its own default (a process manager's root,
/// decision 14.31), never the command id filled in here. Its command id is
/// still fixed here.
#[test]
fn a_remote_command_without_a_correlation_is_sent_without_one() {
    let coordinator =
        FakeCoordinator::answering(Answer::Events(DeliveryOutcome::Applied, deposited()));
    let registry = registry_with(&coordinator);
    let log = MemoryDeadLetters::new();
    Kernel::new(&registry)
        .log_with(&log)
        .handle_any(&MemoryStore::new(), deposit(1), &at(1))
        .ready()
        .expect("handled remotely");
    let page = coordinator.handed()[0].page.clone().unwrap();
    assert_eq!(page.correlation_id, None, "{page:?}");
    assert_eq!(page.command_id, Some(command_id(1)));
}

/// A correlation the caller set is handed on as set.
#[test]
fn a_remote_command_keeps_the_correlation_its_caller_set() {
    let coordinator =
        FakeCoordinator::answering(Answer::Events(DeliveryOutcome::Applied, deposited()));
    let registry = registry_with(&coordinator);
    let log = MemoryDeadLetters::new();
    Kernel::new(&registry)
        .log_with(&log)
        .handle_any(&MemoryStore::new(), deposit(1).correlated(5), &at(1))
        .ready()
        .expect("handled remotely");
    let page = coordinator.handed()[0].page.clone().unwrap();
    assert_eq!(page.correlation_id, Some(crate::toys::correlation(5)));
}

/// A policy's step into a remote domain carries its trigger's
/// correlation, as every reaction does: the kernel's, set.
#[test]
fn a_remote_policy_step_carries_its_triggers_correlation() {
    let coordinator =
        FakeCoordinator::answering(Answer::Events(DeliveryOutcome::Applied, deposited()));
    let mut registry = registry_with(&coordinator);
    registry.policy_dispatch(till_to_bank()).unwrap();
    let log = MemoryDeadLetters::new();
    let ring = command(cover("till", 1), any("/toy.till.v1.Ring")).correlated(6);
    Kernel::new(&registry)
        .log_with(&log)
        .handle_any(&MemoryStore::new(), ring, &at(1))
        .ready()
        .expect("handled");
    let delivered = coordinator.delivered();
    assert_eq!(
        delivered[0].correlation_id,
        Some(crate::toys::correlation(6))
    );
}
