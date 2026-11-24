//! The chain as a sequence of committed steps: atomicity covers one
//! handling step, never a chain. A command commits on its own stream; each
//! policy step after it is one atomic write to one stream, committed with
//! the policy's position on its source; a step that fails keeps every commit before it, and a step left
//! undone (a crash, a failed store) is resumed by position, deduplicated by
//! its derived command id. The call waits for the chain to settle.

use angzarr_ports::faulty::{Fault, FaultyStore};
use angzarr_ports::memory::MemoryStore;

/// The in-memory store, failing what a test says.
type Recording = FaultyStore<MemoryStore>;
use crate::angzarr_router::error::CodedError;
use crate::angzarr_router::hpb;
use crate::toys::prelude::*;
use crate::v1::event_meta::Causation;
use crate::v1::{CommandBook, EventMeta, EventRef};
use crate::{
    ids, Failure, Kernel, KernelError, MemoryDeadLetters, OnPolicyBug, Payload, Recovery, Step,
    StepError,
};

use crate::toys::{
    at, command, cover, cover_in, ledger, ledger_root, opened, payloads, registry, Account,
    AccountCmd, Bookkeeping, Edit, Ledger, Unlucky, LEDGER_LIMIT,
};

fn deposit(n: u128, cents: i64) -> CommandBook {
    command(cover("account", 1), AccountCmd::Deposit(cents).to_any()).id(n)
}

fn ledger_cover() -> crate::v1::Cover {
    crate::v1::Cover {
        domain: "ledger".to_string(),
        root: Some(ledger_root()),
        edition: None,
    }
}

#[test]
fn a_policys_events_carry_the_derived_command_id_as_causation_and_no_trigger() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);

    let handled = kernel
        .handle::<Account, _>(&store, deposit(3, 25).correlated(8), &at(30))
        .ready()
        .unwrap();
    let books = handled.books();
    assert_eq!(books.len(), 2, "the account, then the ledger");
    assert_eq!(books[1].cover, Some(ledger_cover()));
    assert_eq!(
        payloads::<ledger::Recorded>(&books[1..]),
        [ledger::Recorded {
            source_sequence: 2,
            cents: 25
        }]
    );

    let trigger = EventRef {
        stream: Some(cover("account", 1)),
        sequence: 2,
    };
    let meta: EventMeta = books[1].pages[0].meta.clone().unwrap();
    assert_eq!(
        meta.causation,
        Some(Causation::Command(crate::v1::CommandCause {
            command_id: Some(ids::policy_command_id(&trigger, "bookkeeping", 0).unwrap())
        })),
        "the direct cause is the command, UUIDv5 of (trigger, component, \
         index) as angzarr derives it; never the trigger (decision 13.3)"
    );
    assert_eq!(
        meta.correlation_id,
        Some(crate::toys::correlation(8)),
        "the trigger's"
    );
    assert_eq!(
        meta.recorded_at.unwrap().seconds,
        30,
        "one instant per chain"
    );
}

/// A policy step its target refuses with a typed refusal reaches the
/// policy's ONE Translate callback (decision 14.89); a policy that does not
/// translate answers a bug, ANGZARR_REASON_NO_REFUSAL_HANDLER: the call
/// fails with the path to the step, but rolls nothing back, the command's
/// commit stands. The refused reaction is dead-lettered, the policy's
/// position moves past the trigger (its bug, by default), and nothing runs
/// it again: a retry of the command is a duplicate, and owes nothing.
#[test]
fn a_refused_policy_step_that_is_not_translated_keeps_the_commands_commit_and_is_dead_lettered() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let before = slice(&store, &cover("account", 1)).next_sequence;

    let failure = kernel
        .handle::<Account, _>(&store, deposit(3, LEDGER_LIMIT + 1), &at(30))
        .ready()
        .err()
        .unwrap();
    let Failure::Downstream { path, error, .. } = &failure else {
        panic!("a downstream failure: {failure:?}");
    };
    assert_eq!(
        path,
        &[Step {
            stream: ledger_cover(),
            cause: EventRef {
                stream: Some(cover("account", 1)),
                sequence: 2,
            },
        }]
    );
    assert!(
        matches!(error, StepError::PolicyFailed(r)
            if coded_diagnostic::Diagnostic::code(r) == "ANGZARR_REASON_NO_REFUSAL_HANDLER"),
        "{error:?}"
    );
    assert_eq!(failure.recovery(), Recovery::Bug);
    assert_eq!(
        slice(&store, &cover("account", 1)).next_sequence,
        before + 1,
        "the command's deposit committed, and stays"
    );
    assert_eq!(
        pages(&store, &ledger_cover()),
        [],
        "the step landed nothing"
    );
    let lines = log.lines();
    assert_eq!(lines.len(), 1, "the refused step, dead-lettered");
    assert!(lines[0].starts_with(crate::DEAD_LETTER_PREFIX));
    let letter = crate::toys::one_letter(log.letters());
    let why = crate::toys::reason(
        "ANGZARR_REASON_NO_REFUSAL_HANDLER",
        &[("command_type", "/toy.ledger.cmd.v1.Record")],
    );
    crate::toys::assert_letter(&letter, crate::v1::ComponentKind::Policy, &why, 0);
    // The reaction as delivered, its stamp whole.
    let Some(crate::v1::angzarr_dead_letter::Payload::RejectedReaction(delivery)) = &letter.payload
    else {
        panic!("the refused reaction: {letter:?}");
    };
    assert!(delivery.command_id.is_some(), "its id: {delivery:?}");
    assert!(
        delivery.correlation_id.is_some(),
        "its correlation: {delivery:?}"
    );
    assert!(delivery.causation.is_some(), "its trigger: {delivery:?}");
    assert_eq!(delivery.component, "bookkeeping");
    assert_eq!(delivery.cover.as_ref(), Some(&ledger_cover()));
    assert_eq!(letter.source_component, "bookkeeping");
    assert_eq!(
        position(&store, "bookkeeping", &cover("account", 1)),
        before + 1,
        "past the refused trigger"
    );

    let retried = kernel
        .handle::<Account, _>(&store, deposit(3, LEDGER_LIMIT + 1), &at(31))
        .ready()
        .unwrap();
    assert!(matches!(retried, crate::Handled::Duplicate(..)));
    assert_eq!(pages(&store, &ledger_cover()), [], "nothing run again");
    assert_eq!(log.lines().len(), 1);
}

#[test]
fn a_chain_deeper_than_its_limit_is_refused() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log).limit(0);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let failure = kernel
        .handle::<Account, _>(&store, deposit(3, 1), &at(30))
        .ready()
        .err()
        .unwrap();
    assert!(matches!(
        failure,
        Failure::Downstream {
            error: StepError::Kernel(KernelError::ChainTooDeep { limit: 0 }),
            ..
        }
    ));
    assert_eq!(failure.recovery(), Recovery::Bug);
    assert_eq!(
        pages(&store, &cover("account", 1)).len(),
        3,
        "the command's commit stands"
    );
}

/// Editions are explicit per-stream forks (decision 14.107): a command to
/// a stream not forked into its edition is refused before its handler runs
/// (`ANGZARR_REASON_EDITION_NOT_FORKED`, FAILED_PRECONDITION; C-0633), the
/// framework's key, never a typed refusal, and nothing is written.
/// angzarr-embedded creates no edition.
#[test]
fn a_command_to_an_edition_not_forked_is_refused_and_writes_nothing() {
    let mut registry = registry();
    registry.policy(Bookkeeping).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    let stream = cover_in("account", 1, "what-if");
    let open = command(stream.clone(), AccountCmd::Open.to_any());
    let failure = kernel
        .handle::<Account, _>(&store, open, &at(30))
        .ready()
        .err()
        .unwrap();
    let Failure::Kernel(KernelError::Router(coded)) = &failure else {
        panic!("refused before its handler: {failure:?}")
    };
    assert_eq!(coded.code, "ANGZARR_REASON_EDITION_NOT_FORKED");
    assert_eq!(
        coded.grpc,
        crate::angzarr_router::error::GrpcCode::FailedPrecondition
    );
    assert_eq!(
        coded.extras.get("stream").map(String::as_str),
        Some(crate::error::cover_text(&stream).as_str())
    );
    assert_eq!(
        coded.extras.get("edition").map(String::as_str),
        Some("what-if")
    );
    assert_eq!(failure.recovery(), Recovery::Business);
    assert_eq!(streams(&store), [], "nothing written");
    assert!(
        log.lines().is_empty(),
        "the caller's: nothing dead-lettered"
    );
}

/// Once forked, an edition stream takes commands, on its own timeline.
#[test]
fn a_command_to_a_forked_edition_is_decided_there() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 10);
    let stream = cover_in("account", 1, "what-if");
    crate::toys::fork(&store, &stream);
    let handled = crate::toys::handle::<Account>(
        &store,
        &registry,
        command(stream.clone(), AccountCmd::Deposit(5).to_any()),
        &at(30),
    )
    .expect("forked: decided");
    assert_eq!(handled.books()[0].cover, Some(stream));
    assert_eq!(
        slice(&store, &cover("account", 1)).next_sequence,
        2,
        "main untouched"
    );
}

#[test]
fn a_policys_target_is_in_its_sources_edition() {
    let target =
        crate::target_cover::<Bookkeeping>(&cover_in("account", 1, "what-if"), &ledger_root());
    assert_eq!(
        target.edition.as_deref(),
        Some("what-if"),
        "a policy never crosses timelines"
    );
}

/// A hand-written policy translates nothing by default (decision 14.89):
/// its refusal callback answers ANGZARR_REASON_NO_REFUSAL_HANDLER, a bug,
/// naming the refused command's type; there is no pass-through.
#[test]
fn a_hand_written_policy_translates_no_refusal() {
    use crate::angzarr_router::error::BusinessStatus;
    let delivery = crate::v1::ReactionDelivery {
        cover: Some(ledger_cover()),
        body: Some(crate::v1::reaction_delivery::Body::Command(crate::pack(
            &ledger::Record {
                source_sequence: 2,
                cents: 1,
            },
        ))),
        ..Default::default()
    };
    let refused = crate::Refused::new(
        delivery,
        BusinessStatus::OutOfRange,
        crate::pack(&crate::toys::ToyRefusal {
            code: "toy-ledger-too-large".to_string(),
        }),
        EventRef {
            stream: Some(cover("account", 1)),
            sequence: 2,
        },
        EventMeta::default(),
    );
    let answer = crate::Policy::on_refused(&Bookkeeping, &refused);
    assert_eq!(answer, Err(crate::no_refusal_handler(&refused)));
}

#[test]
fn a_book_for_another_domain_is_refused() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    let failure = kernel
        .handle::<Ledger, _>(
            &store,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
        )
        .ready()
        .err()
        .unwrap();
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::WrongDomain {
            stream: cover("account", 1),
            expected: "ledger"
        })
    );
}

/// Each step loads what every step before it committed: the second
/// deposit's ledger step finds the first's record. What a chain answers is
/// every stream it committed to, in the order first touched.
#[test]
fn each_step_loads_what_the_steps_before_it_committed() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    kernel
        .handle::<Account, _>(&store, deposit(3, 25), &at(30))
        .ready()
        .unwrap();
    let handled = kernel
        .handle::<Account, _>(&store, deposit(4, 5), &at(31))
        .ready()
        .unwrap();
    let crate::Handled::Applied(recorded) = handled else {
        panic!("applied");
    };
    assert_eq!(
        recorded
            .books()
            .iter()
            .map(|b| (b.cover.clone().unwrap(), b.pages.len(), b.next_sequence))
            .collect::<Vec<_>>(),
        [(cover("account", 1), 1, 4), (ledger_cover(), 1, 2)],
        "each stream once, in the order first touched"
    );
    let ledger = recorded
        .events::<Ledger>(&ledger_cover(), &registry)
        .unwrap();
    assert_eq!(ledger[0].sequence(), 1, "after the first deposit's record");
    assert!(recorded
        .events::<Ledger>(&cover("account", 1), &registry)
        .is_err());
    assert!(recorded
        .events::<Account>(&cover("account", 9), &registry)
        .unwrap()
        .is_empty());
}

/// Every deposit's amount, projected after commit: the projector port,
/// each book's account events decoded by type.
#[derive(Clone)]
struct Deposits(std::sync::Arc<std::sync::Mutex<Vec<i64>>>, &'static str);

impl Default for Deposits {
    fn default() -> Self {
        Deposits::named("deposits")
    }
}

impl Deposits {
    fn named(name: &'static str) -> Self {
        Deposits(std::sync::Arc::default(), name)
    }

    fn seen(&self) -> Vec<i64> {
        self.0.lock().unwrap().clone()
    }
}

impl angzarr_router::router::ProjectorHandler for Deposits {
    fn name(&self) -> &str {
        self.1
    }

    async fn dispatch(
        &self,
        request: &angzarr_router::hpb::ProjectRequest,
    ) -> Result<angzarr_router::hpb::ProjectResponse, angzarr_router::error::CodedError> {
        for page in request.events.iter().flat_map(|events| &events.pages) {
            let any = angzarr_router::page_event(page)
                .cloned()
                .unwrap_or_default();
            if let Ok(crate::toys::AccountEvent::Deposited(cents)) =
                crate::toys::AccountEvent::from_any(&any)
            {
                self.0.lock().unwrap().push(cents);
            }
        }
        Ok(angzarr_router::hpb::ProjectResponse::default())
    }
}

/// Projectors run inline on what each step committed, of their sources'
/// streams only: the command's events, not the ledger's the policy wrote.
/// What the chain recorded is typed only when asked, from its pages.
#[test]
fn projectors_run_after_commit_over_their_sources_streams() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping).unwrap();
    let deposits = Deposits::default();
    registry
        .projector_handler(&["account"], deposits.clone())
        .unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let handled = kernel
        .handle::<Account, _>(&store, deposit(3, 25), &at(30))
        .ready()
        .unwrap();
    assert_eq!(
        deposits.seen(),
        [25],
        "only the new events, only the account's"
    );

    let crate::Handled::Applied(recorded) = handled else {
        panic!("applied");
    };
    let ledger = recorded
        .events::<Ledger>(&ledger_cover(), &registry)
        .unwrap();
    assert_eq!(ledger.len(), 1);
    assert_eq!(
        recorded
            .events::<Ledger>(&cover("account", 1), &registry)
            .err(),
        Some(KernelError::Replay(crate::ReplayError::WrongDomain {
            found: "account".to_string(),
            expected: "ledger"
        }))
    );
    assert_eq!(
        recorded
            .events::<Ledger>(&cover("ledger", 9), &registry)
            .unwrap(),
        [],
        "a stream the chain did not touch: nothing recorded"
    );
}

#[test]
fn a_duplicate_runs_no_projector_and_commits_nothing() {
    let mut registry = registry();
    let deposits = Deposits::default();
    registry
        .projector_handler(&["account"], deposits.clone())
        .unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    opened(&store, &crate::toys::registry(), 1, 0);
    let open = command(cover("account", 1), AccountCmd::Open.to_any()).id(1);
    let handled = kernel
        .handle::<Account, _>(&store, open, &at(30))
        .ready()
        .unwrap();
    assert!(matches!(handled, crate::Handled::Duplicate(..)));
    assert!(deposits.seen().is_empty());
}

/// The handler's future is `Send` over a store whose futures are, so a
/// server spawns it on a multi-threaded runtime.
#[test]
fn handle_is_a_send_future_over_a_send_store() {
    fn send<T: Send>(t: T) -> T {
        t
    }
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let handled = send(kernel.handle::<Account, _>(&store, deposit(3, 25), &at(30)))
        .ready()
        .unwrap();
    assert_eq!(handled.books().len(), 2);
    let resumed = send(kernel.resume(&store, &at(31))).ready().unwrap();
    assert!(resumed.books().is_empty(), "nothing owed");
}

/// The account stream as stored.
fn account_stream(store: &MemoryStore, registry: &crate::Registry) -> crate::Stream<Account> {
    let pages = pages(store, &cover("account", 1));
    crate::Stream::replay(cover("account", 1), &pages, registry).unwrap()
}

/// Account deposits → the ledger, and nothing else: a policy that targets
/// from the event, so the ledger is owed only some of the account's events.
struct DepositsOnly;

impl crate::Policy for DepositsOnly {
    const NAME: &'static str = "deposits-only";
    type Source = Account;
    type Target = Ledger;

    fn on_event(
        &self,
        event: &crate::Any,
        ctx: &crate::markup::PolicyContext,
    ) -> Result<Option<crate::markup::Commands<Ledger>>, crate::markup::Report> {
        let Ok(crate::toys::AccountEvent::Deposited(cents)) =
            crate::toys::AccountEvent::from_any(event)
        else {
            return Ok(None);
        };
        let record = ledger::Record {
            source_sequence: ctx.trigger().sequence,
            cents,
        };
        Ok(Some(
            crate::markup::Commands::none().send(ledger_root(), &record),
        ))
    }
}

/// A policy's error is a bug its step cannot run past: the step is
/// dead-lettered with the error's code as its reason and its args, never
/// its English, the call fails downstream with it as
/// [`StepError::PolicyFailed`], a defect (not a business rejection), and
/// the policy's position moves past the trigger, as for a refused
/// command's step, so nothing runs it again. The command's commit stands,
/// and the next event is handled as usual.
#[test]
fn a_policys_error_dead_letters_its_step_and_moves_its_position_past_it() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Unlucky).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let before = slice(&store, &cover("account", 1)).next_sequence;

    let failure = kernel
        .handle::<Account, _>(&store, deposit(3, 13), &at(30))
        .ready()
        .err()
        .expect("the policy's error fails the call");
    let Failure::Downstream { error, .. } = &failure else {
        panic!("a downstream failure: {failure:?}");
    };
    let StepError::PolicyFailed(report) = error else {
        panic!("the policy's error, its own kind: {error:?}");
    };
    // As the wire carries a handler's bug: the router's key, the policy's
    // own code and args its cause.
    assert_eq!(
        report.to_string(),
        "ANGZARR_REASON_UNHANDLED_HANDLER_ERROR cause=toy-unlucky-deposit cause.cents=13"
    );
    assert_eq!(
        failure.recovery(),
        crate::Recovery::Bug,
        "a defect, alertable, not a business refusal"
    );
    assert_eq!(
        slice(&store, &cover("account", 1)).next_sequence,
        before + 1,
        "the command's deposit committed, and stays"
    );
    assert_eq!(pages(&store, &ledger_cover()), [], "the step sent nothing");
    let lines = log.lines();
    assert_eq!(lines.len(), 1, "the failed step, dead-lettered: {lines:?}");
    assert!(lines[0].starts_with(crate::DEAD_LETTER_PREFIX));
    assert!(
        lines[0].contains("unlucky: ANGZARR_REASON_UNHANDLED_HANDLER_ERROR cause=toy-unlucky-deposit cause.cents=13"),
        "the policy, its bug's key, its error's code as the cause, and its args: {}",
        lines[0]
    );
    // A policy's error is a handler's bug, keyed as the router keys one;
    // the trigger is the payload, processing_failed, never retried, not
    // transient (decision 14.48).
    let letter = crate::toys::one_letter(log.letters());
    let reason = crate::toys::reason(
        "ANGZARR_REASON_UNHANDLED_HANDLER_ERROR",
        &[("cause", "toy-unlucky-deposit"), ("cause.cents", "13")],
    );
    crate::toys::assert_letter(&letter, crate::v1::ComponentKind::Policy, &reason, 0);
    assert!(
        matches!(
            &letter.payload,
            Some(crate::v1::angzarr_dead_letter::Payload::RejectedEvents(book))
                if book.cover == Some(cover("account", 1))
        ),
        "{letter:?}"
    );
    assert!(
        !lines[0].contains("cannot happen"),
        "no English: {}",
        lines[0]
    );
    assert_eq!(
        position(&store, "unlucky", &cover("account", 1)),
        before + 1,
        "past the trigger it failed on"
    );

    kernel
        .handle::<Account, _>(&store, deposit(4, 5), &at(31))
        .ready()
        .expect("the next event is handled as usual");
    let book = crate::v1::EventBook {
        pages: pages(&store, &ledger_cover()),
        ..Default::default()
    };
    assert_eq!(
        payloads::<ledger::Recorded>(&[book])
            .iter()
            .map(|r| (r.source_sequence, r.cents))
            .collect::<Vec<_>>(),
        [(before + 1, 5)],
        "the next deposit, and only it"
    );
    assert_eq!(
        position(&store, "unlucky", &cover("account", 1)),
        before + 2
    );
    assert_eq!(log.lines().len(), 1, "nothing more dead-lettered");
}

/// What a kernel does with a policy's bug unless told otherwise: it moves
/// the policy's position past the trigger.
#[test]
fn a_kernel_moves_a_policys_position_past_its_bug_by_default() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    assert_eq!(OnPolicyBug::default(), OnPolicyBug::MovePast);
    assert_eq!(
        Kernel::new(&registry).log_with(&log).policy_bug(),
        OnPolicyBug::MovePast
    );
    assert_eq!(
        Kernel::new(&registry)
            .log_with(&log)
            .on_policy_bug(OnPolicyBug::Stay)
            .policy_bug(),
        OnPolicyBug::Stay
    );
}

/// Under [`OnPolicyBug::Stay`], a policy's error is still dead-lettered
/// and fails the call as [`StepError::PolicyFailed`], but the policy's
/// position stays at the trigger, so a resume runs the step again (and
/// dead-letters it again).
#[test]
fn under_stay_a_policys_error_holds_its_position() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Unlucky).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry)
        .log_with(&log)
        .on_policy_bug(OnPolicyBug::Stay);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let held = position(&store, "unlucky", &cover("account", 1));

    let failure = kernel
        .handle::<Account, _>(&store, deposit(3, 13), &at(30))
        .ready()
        .err()
        .expect("the policy's error fails the call");
    assert!(
        matches!(
            &failure,
            Failure::Downstream {
                error: StepError::PolicyFailed(_),
                ..
            }
        ),
        "{failure:?}"
    );
    assert_eq!(log.lines().len(), 1, "dead-lettered: {:?}", log.lines());
    assert_eq!(
        position(&store, "unlucky", &cover("account", 1)),
        held,
        "the position stays"
    );

    let resumed = kernel.resume(&store, &at(31)).ready();
    assert!(
        matches!(
            &resumed,
            Err(Failure::Downstream {
                error: StepError::PolicyFailed(_),
                ..
            })
        ),
        "the step runs again: {:?}",
        resumed.as_ref().err()
    );
    assert_eq!(log.lines().len(), 2, "dead-lettered again");
}

/// Under [`OnPolicyBug::Stay`], a policy's refusal answer that fails is
/// still dead-lettered as [`StepError::PolicyFailed`], but the policy's
/// position stays at the trigger.
#[test]
fn under_stay_a_policys_refusal_answer_error_holds_its_position() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(UnansweringBookkeeping).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry)
        .log_with(&log)
        .on_policy_bug(OnPolicyBug::Stay);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let held = position(&store, "unanswering-bookkeeping", &cover("account", 1));
    let failure = kernel
        .handle::<Account, _>(&store, deposit(3, LEDGER_LIMIT + 1), &at(30))
        .ready()
        .err()
        .expect("the answer's error fails the call");
    assert!(
        matches!(
            &failure,
            Failure::Downstream {
                error: StepError::PolicyFailed(_),
                ..
            }
        ),
        "{failure:?}"
    );
    assert_eq!(log.lines().len(), 1, "dead-lettered: {:?}", log.lines());
    assert_eq!(
        position(&store, "unanswering-bookkeeping", &cover("account", 1)),
        held,
        "the position stays"
    );
}

#[test]
fn a_filtered_policy_reaches_its_target_only_for_the_events_it_targets() {
    let mut registry = registry();
    registry.policy(DepositsOnly).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    for book in [
        command(cover("account", 1), AccountCmd::Open.to_any()).id(1),
        deposit(2, 5),
        command(cover("account", 1), AccountCmd::Withdraw(2).to_any()).id(3),
        deposit(4, 7),
    ] {
        kernel
            .handle::<Account, _>(&store, book, &at(30))
            .ready()
            .unwrap();
    }
    let book = crate::v1::EventBook {
        pages: pages(&store, &ledger_cover()),
        ..Default::default()
    };
    assert_eq!(
        payloads::<ledger::Recorded>(&[book])
            .iter()
            .map(|r| (r.source_sequence, r.cents))
            .collect::<Vec<_>>(),
        [(1, 5), (3, 7)],
        "the deposits, and only the deposits, in source order"
    );
    assert_eq!(
        position(&store, "deposits-only", &cover("account", 1)),
        4,
        "past every event, the untargeted included"
    );
}

/// A policy's command for a domain the kernel cannot route is the policy's
/// bug: never delivered, its triggering event dead-lettered
/// (`rejected_events`, C-0736) under
/// `ANGZARR_REASON_POLICY_COMMAND_UNKNOWN_DOMAIN`, naming the command.
#[test]
fn a_policy_book_for_an_unregistered_target_dead_letters_its_trigger() {
    let mut registry = crate::Registry::new();
    registry.policy(Bookkeeping).unwrap();
    registry
        .register::<Account>(
            crate::Upcaster::new("u", "account"),
            crate::toys::handlers::<crate::toys::Account>(),
        )
        .unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    let failure = kernel
        .handle::<Account, _>(
            &store,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
        )
        .ready()
        .err()
        .unwrap();
    let Failure::Downstream { path, error, .. } = &failure else {
        panic!("downstream: {failure:?}");
    };
    assert_eq!(path.len(), 1);
    assert_eq!(
        error,
        &StepError::Kernel(KernelError::Malformed(crate::Malformed::UnknownDomain {
            domain: "ledger".to_string()
        }))
    );
    assert_eq!(
        failure.recovery(),
        Recovery::Bug,
        "the kernel's book: a bug"
    );
    let lines = log.lines();
    assert_eq!(lines.len(), 1);
    assert!(lines[0].starts_with(crate::DEAD_LETTER_PREFIX));
    assert!(
        lines[0]
            .contains("bookkeeping: ANGZARR_REASON_POLICY_COMMAND_UNKNOWN_DOMAIN command_type=toy.ledger.cmd.v1.Record domain=ledger sequence=0 stream=account/"),
        "{}",
        lines[0]
    );
    let letters = log.letters();
    let Some(crate::v1::angzarr_dead_letter::Payload::RejectedEvents(trigger)) =
        &letters[0].payload
    else {
        panic!("the trigger, not the command: {:?}", letters[0].payload);
    };
    assert_eq!(trigger.cover.as_ref(), Some(&cover("account", 1)));
    assert_eq!(
        trigger.pages.iter().map(|p| p.sequence).collect::<Vec<_>>(),
        [0]
    );
    assert_eq!(
        streams(&store),
        [cover("account", 1)],
        "the command's commit stands"
    );
    assert_eq!(
        position(&store, "bookkeeping", &cover("account", 1)),
        0,
        "a bug holds the position, so the step runs again once it is fixed"
    );
}

/// A command's policy steps are owed for its own events only: the second
/// deposit's chain runs one ledger step, for its own event, not the first's
/// again.
#[test]
fn a_command_owes_policy_steps_for_its_own_events_only() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = Recording::default();
    opened(&store, &plain, 1, 0);
    for n in [3, 4] {
        kernel
            .handle::<Account, _>(&store, deposit(n, 1), &at(30))
            .ready()
            .unwrap();
    }
    let ledger = slice(store.inner(), &ledger_cover()).pages;
    let book = crate::v1::EventBook {
        pages: ledger,
        ..Default::default()
    };
    assert_eq!(
        payloads::<ledger::Recorded>(&[book])
            .iter()
            .map(|r| r.source_sequence)
            .collect::<Vec<_>>(),
        [2, 3],
        "each deposit's own event, once"
    );
    assert_eq!(
        store.landed()[2..],
        [
            Some(cover("account", 1)),
            Some(ledger_cover()),
            Some(cover("account", 1)),
            Some(ledger_cover()),
        ],
        "after the two opening commands: per deposit, its command, then one ledger step"
    );
}

#[test]
fn a_chain_on_a_stream_loaded_from_a_snapshot_records_only_its_events() {
    let mut registry = registry();
    let (one, two) = (Deposits::named("first"), Deposits::named("second"));
    registry
        .projector_handler(&["account"], one.clone())
        .unwrap()
        .projector_handler(&["account"], two.clone())
        .unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    opened(&store, &crate::toys::registry(), 1, 7);
    let source = account_stream(&store, &registry);
    store
        .write_snapshot(
            sref(&cover("account", 1)),
            &source.snapshot(&registry).unwrap(),
        )
        .ready()
        .unwrap();
    let handled = kernel
        .handle::<Account, _>(&store, deposit(3, 25), &at(30))
        .ready()
        .unwrap();
    let crate::Handled::Applied(recorded) = handled else {
        panic!("applied");
    };
    let events = recorded
        .events::<Account>(&cover("account", 1), &registry)
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence(), 2);
    let seen = [one.seen(), two.seen()];
    assert_eq!(seen, [vec![25], vec![25]], "both projectors ran");
}

/// Two ledgers per account event: orders are indexed in target order.
struct TwoLedgers;

impl crate::Policy for TwoLedgers {
    const NAME: &'static str = "two-ledgers";
    type Source = Account;
    type Target = Ledger;

    fn on_event(
        &self,
        _: &crate::Any,
        ctx: &crate::markup::PolicyContext,
    ) -> Result<Option<crate::markup::Commands<Ledger>>, crate::markup::Report> {
        let record = ledger::Record {
            source_sequence: ctx.trigger().sequence,
            cents: 0,
        };
        let commands = crate::markup::Commands::none()
            .send(two_ledgers_root(0), &record)
            .send(two_ledgers_root(1), &record);
        Ok(Some(commands))
    }
}

/// The root of the `n`th of [`TwoLedgers`]' ledgers.
fn two_ledgers_root(n: u8) -> crate::v1::Uuid {
    ids::compute_root("ledger", &n.to_string()).unwrap()
}

#[test]
fn a_triggers_commands_are_indexed_in_target_order() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(TwoLedgers).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    kernel
        .handle::<Account, _>(&store, deposit(3, 5), &at(30))
        .ready()
        .unwrap();
    let trigger = EventRef {
        stream: Some(cover("account", 1)),
        sequence: 2,
    };
    for index in [0u8, 1] {
        let pages = pages(&store, &two_ledgers(index));
        assert_eq!(
            pages[0].meta.as_ref().unwrap().causation,
            Some(Causation::Command(crate::v1::CommandCause {
                command_id: Some(
                    ids::policy_command_id(&trigger, "two-ledgers", u32::from(index)).unwrap()
                )
            })),
            "target {index}'s command is the trigger's {index}th"
        );
    }
}

/// The `n`th of [`TwoLedgers`]' ledgers.
fn two_ledgers(n: u8) -> crate::v1::Cover {
    crate::v1::Cover {
        domain: "ledger".to_string(),
        root: Some(ids::compute_root("ledger", &n.to_string()).unwrap()),
        edition: None,
    }
}

/// Reaction fan-out (the owner's addition): several policies on one source
/// domain, through the builder, each run for every event of that domain, in
/// the order they were registered (per event, every policy; per policy,
/// every target), each as its own committed step, after the command's own.
#[test]
fn every_policy_on_a_source_runs_for_each_of_its_events_in_registration_order() {
    let mut registry = registry();
    registry.policy(TwoLedgers).unwrap();
    registry.policy(Bookkeeping).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = Recording::default();
    let open = command(cover("account", 1), AccountCmd::Open.to_any()).id(1);
    for (book, second) in [(open, 30), (deposit(2, 5), 31)] {
        kernel
            .handle::<Account, _>(&store, book, &at(second))
            .ready()
            .unwrap();
    }
    let per_event = [
        Some(cover("account", 1)),
        Some(two_ledgers(0)),
        Some(two_ledgers(1)),
        Some(ledger_cover()),
    ];
    assert_eq!(
        store.landed(),
        [per_event.clone(), per_event].concat(),
        "per event: the command, TwoLedgers' targets in order, then Bookkeeping's"
    );
    for target in [two_ledgers(0), two_ledgers(1), ledger_cover()] {
        let book = crate::v1::EventBook {
            pages: slice(store.inner(), &target).pages,
            ..Default::default()
        };
        assert_eq!(
            payloads::<ledger::Recorded>(&[book])
                .iter()
                .map(|r| r.source_sequence)
                .collect::<Vec<_>>(),
            [0, 1],
            "{target:?} reached for each account event"
        );
    }
}

/// A policy step's events land with the policy's position on its source:
/// one write, and the command's own write puts no position. A policy with
/// several targets puts it once, with the trigger's last target, so a
/// crash between its targets leaves the trigger owed (the targets that
/// landed are deduplicated when it runs again).
#[test]
fn a_policy_step_commits_its_position_with_its_events() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(TwoLedgers).unwrap();
    registry.policy(Bookkeeping).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = Recording::default();
    opened(&store, &plain, 1, 0);
    kernel
        .handle::<Account, _>(&store, deposit(3, 5), &at(30))
        .ready()
        .unwrap();
    let writes = store.writes();
    let account = cover("account", 1);
    let [command, first, last, bookkeeping] = &writes[writes.len() - 4..] else {
        panic!("four writes for the deposit: {writes:?}");
    };
    assert_eq!(command.wrote, Some((account.clone(), 1)));
    assert_eq!(command.positions, [], "a command's write puts no position");
    assert_eq!(first.wrote, Some((two_ledgers(0), 1)));
    assert_eq!(first.positions, [], "not the trigger's last target");
    assert_eq!(last.wrote, Some((two_ledgers(1), 1)));
    assert_eq!(
        last.positions,
        [("two-ledgers".to_string(), account.clone(), 3)]
    );
    assert_eq!(bookkeeping.wrote, Some((ledger_cover(), 1)));
    assert_eq!(
        bookkeeping.positions,
        [("bookkeeping".to_string(), account.clone(), 3)]
    );
    assert_eq!(position(store.inner(), "bookkeeping", &account), 3);
}

/// A step the store fails (here, its write) keeps every write before it,
/// and writes nothing more: its policy's position stays where the last
/// committed step put it. The account is opened with the policy registered,
/// so the position past the opening is an ordinary step's. The call fails
/// with the store's error, to be retried; `Kernel::resume` runs what is
/// owed from each policy's position, and running it again finds nothing
/// more.
#[test]
fn a_step_the_store_failed_is_resumed_by_position() {
    let mut registry = registry();
    registry.policy(Bookkeeping).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry)
        .log_with(&log)
        .on_retryable(crate::toys::no_retries());
    let store = Recording::default();
    opened(&store, &registry, 1, 0);
    let account = cover("account", 1);
    assert_eq!(
        position(store.inner(), "bookkeeping", &account),
        2,
        "the opening's own ledger steps put it"
    );
    let before = store.writes().len();
    store.fail(failing(1, Fault::Unavailable));
    let failure = kernel
        .handle::<Account, _>(&store, deposit(3, 25), &at(30))
        .ready()
        .err()
        .unwrap();
    assert!(
        matches!(
            &failure,
            Failure::Downstream {
                error: StepError::Kernel(KernelError::Store(crate::StoreError::Unavailable(_))),
                ..
            }
        ),
        "{failure:?}"
    );
    assert_eq!(failure.recovery(), Recovery::Retry);
    assert_eq!(
        pages(store.inner(), &account).len(),
        3,
        "the deposit stands"
    );
    assert_eq!(
        pages(store.inner(), &ledger_cover()).len(),
        2,
        "the opening's two records; the deposit's step landed nothing"
    );
    let writes = store.writes();
    assert_eq!(
        writes[before..]
            .iter()
            .map(|w| (w.wrote.clone(), w.landed))
            .collect::<Vec<_>>(),
        [
            (Some((account.clone(), 1)), true),
            (Some((ledger_cover(), 1)), false),
        ],
        "the command's write, then the step's failed one, and nothing after it"
    );
    assert_eq!(position(store.inner(), "bookkeeping", &account), 2);

    let resumed = kernel.resume(&store, &at(31)).ready().unwrap();
    assert_eq!(
        payloads::<ledger::Recorded>(resumed.books()),
        [ledger::Recorded {
            source_sequence: 2,
            cents: 25
        }]
    );
    assert_eq!(position(store.inner(), "bookkeeping", &account), 3);
    let again = kernel.resume(&store, &at(32)).ready().unwrap();
    assert!(again.books().is_empty(), "nothing more owed");
    assert_eq!(pages(store.inner(), &ledger_cover()).len(), 3);
}

/// Account events → a sweep of account 9, which holds nothing: a policy
/// whose target decides no event.
struct SweepNine;

impl crate::Policy for SweepNine {
    const NAME: &'static str = "sweep-nine";
    type Source = Account;
    type Target = Account;

    fn on_event(
        &self,
        _: &crate::Any,
        _: &crate::markup::PolicyContext,
    ) -> Result<Option<crate::markup::Commands<Account>>, crate::markup::Report> {
        let root = cover("account", 9).root.unwrap();
        Ok(Some(
            crate::markup::Commands::none().send(root, &AccountCmd::Sweep),
        ))
    }
}

/// A policy step whose target decides no event writes no event, but still
/// puts its policy's position past the trigger, alone: the trigger is
/// handled, and nothing is owed.
#[test]
fn a_policy_step_that_decides_no_event_still_moves_its_position() {
    let mut registry = registry();
    registry.policy(SweepNine).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = Recording::default();
    let open = command(cover("account", 1), AccountCmd::Open.to_any()).id(1);
    kernel
        .handle::<Account, _>(&store, open, &at(30))
        .ready()
        .unwrap();
    let account = cover("account", 1);
    assert_eq!(
        streams(store.inner()),
        [cover("account", 1)],
        "the sweep wrote no event"
    );
    assert_eq!(
        store.writes(),
        [
            angzarr_ports::faulty::WriteRecord {
                wrote: Some((account.clone(), 1)),
                positions: vec![],
                letters: vec![],
                landed: true,
            },
            angzarr_ports::faulty::WriteRecord {
                wrote: None,
                positions: vec![("sweep-nine".to_string(), account.clone(), 1)],
                letters: vec![],
                landed: true,
            },
        ],
        "the command's write, then the step's position alone"
    );
    assert_eq!(position(store.inner(), "sweep-nine", &account), 1);
    let resumed = kernel.resume(&store, &at(31)).ready().unwrap();
    assert!(resumed.books().is_empty(), "nothing owed");
    assert_eq!(store.writes().len(), 2, "and nothing run again");
}

/// The refusals a policy was handed, each as its Translate callback reads
/// it.
type Answered = std::sync::Arc<std::sync::Mutex<Vec<crate::Refused>>>;

/// Account deposits → the ledger, translating its refusals: every refusal
/// it is handed, kept, and answered with a payment of 0 cents, a fact of
/// the account.
struct AnsweringBookkeeping(Answered);

impl crate::Policy for AnsweringBookkeeping {
    const NAME: &'static str = "answering-bookkeeping";
    type Source = Account;
    type Target = Ledger;

    fn on_event(
        &self,
        event: &crate::Any,
        ctx: &crate::markup::PolicyContext,
    ) -> Result<Option<crate::markup::Commands<Ledger>>, crate::markup::Report> {
        crate::Policy::on_event(&Bookkeeping, event, ctx)
    }

    fn on_refused(&self, refused: &crate::Refused) -> Result<crate::Any, crate::markup::Report> {
        self.0.lock().unwrap().push(refused.clone());
        Ok(crate::toys::AccountFact::PaymentReceived(0).to_any())
    }
}

/// As [`Bookkeeping`], translating refusals; but a refusal of a record for
/// more than its limit cannot happen, so its answer is its error, a bug.
struct UnansweringBookkeeping;

impl crate::Policy for UnansweringBookkeeping {
    const NAME: &'static str = "unanswering-bookkeeping";
    type Source = Account;
    type Target = Ledger;

    fn on_event(
        &self,
        event: &crate::Any,
        ctx: &crate::markup::PolicyContext,
    ) -> Result<Option<crate::markup::Commands<Ledger>>, crate::markup::Report> {
        crate::Policy::on_event(&Bookkeeping, event, ctx)
    }

    fn on_refused(&self, refused: &crate::Refused) -> Result<crate::Any, crate::markup::Report> {
        Err(crate::markup::Report::new("toy-unanswerable-refusal")
            .arg("trigger", refused.trigger().sequence))
    }
}

/// A policy's Translate callback reads the refused command whole, as it
/// was delivered (its target's stream, its derived id, the trigger's
/// correlation, the trigger as its causation, the policy as its
/// component), its target's typed refusal and status, and the trigger and
/// its metadata.
#[test]
fn a_policys_translate_reads_the_refused_reaction_and_its_refusal() {
    use crate::angzarr_router::error::BusinessStatus;
    let plain = registry();
    let mut registry = registry();
    let refusals = Answered::default();
    registry
        .policy(AnsweringBookkeeping(std::sync::Arc::clone(&refusals)))
        .unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    kernel
        .handle::<Account, _>(&store, deposit(3, 5), &at(30))
        .ready()
        .unwrap();
    kernel
        .handle::<Account, _>(&store, deposit(4, LEDGER_LIMIT + 1), &at(31))
        .ready()
        .unwrap();
    let refusals = refusals.lock().unwrap();
    let [refused] = refusals.as_slice() else {
        panic!("one refusal: {refusals:?}");
    };
    let trigger = EventRef {
        stream: Some(cover("account", 1)),
        sequence: 3,
    };
    let delivery = refused.delivery();
    assert_eq!(
        delivery.cover,
        Some(ledger_cover()),
        "the refused command's target"
    );
    assert_eq!(
        delivery.command_id,
        Some(ids::policy_command_id(&trigger, "answering-bookkeeping", 0).unwrap()),
        "the refused command's derived id"
    );
    let stored = pages(&store, &cover("account", 1))[3].meta.clone().unwrap();
    assert!(stored.correlation_id.is_some());
    assert_eq!(
        delivery.correlation_id, stored.correlation_id,
        "the trigger's correlation"
    );
    assert_eq!(delivery.causation, Some(trigger.clone()));
    assert_eq!(delivery.component, "answering-bookkeeping");
    let record: ledger::Record =
        prost::Message::decode(&refused.command_payload().value[..]).unwrap();
    assert_eq!(record.cents, LEDGER_LIMIT + 1);
    assert_eq!(refused.status(), Some(BusinessStatus::FailedPrecondition));
    assert_eq!(
        refused.refusal_object(),
        Some(&crate::toys::toy_refusal(&crate::toys::TooLarge))
    );
    assert_eq!(refused.trigger(), &trigger);
    assert_eq!(refused.meta(), &stored, "the trigger's metadata, whole");
}

/// The fact a Translate callback answers goes to the trigger's exact stream
/// as a step of its own (decision 14.89), identified by the refused
/// command's id, its origin derived from it; the step is done and the call
/// succeeds.
#[test]
fn a_translated_refusal_is_recorded_on_the_triggers_stream() {
    let plain = registry();
    let mut registry = registry();
    registry
        .policy(AnsweringBookkeeping(Answered::default()))
        .unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let handled = kernel
        .handle::<Account, _>(&store, deposit(3, LEDGER_LIMIT + 1), &at(30))
        .ready()
        .expect("translated: the chain goes on");
    let account = cover("account", 1);
    let stored = pages(&store, &account);
    assert_eq!(
        stored.len(),
        4,
        "the deposit, then the refusal fact's event"
    );
    let trigger = EventRef {
        stream: Some(account.clone()),
        sequence: 2,
    };
    let refused_id = ids::policy_command_id(&trigger, "answering-bookkeeping", 0).unwrap();
    let Some(Causation::Fact(cause)) = stored[3].meta.clone().unwrap().causation else {
        panic!("a fact's event: {:?}", stored[3]);
    };
    assert_eq!(
        cause.command_id,
        Some(refused_id.clone()),
        "the refused command's id"
    );
    assert_eq!(
        cause.origin.map(|o| o.external_id),
        Some(refused_id.to_string())
    );
    // The refused step landed nothing; the refusal fact's own event (at 3)
    // is one the policy reacts to, as to any.
    let recorded: Vec<_> = payloads::<ledger::Recorded>(&[crate::v1::EventBook {
        pages: pages(&store, &ledger_cover()),
        ..Default::default()
    }])
    .iter()
    .map(|r| r.source_sequence)
    .collect();
    assert_eq!(recorded, [3]);
    assert!(log.lines().is_empty(), "{:?}", log.lines());
    let answered = handled.books();
    let book = answered
        .iter()
        .find(|b| b.cover == Some(account.clone()))
        .expect("the account's book");
    assert_eq!(
        book.pages.iter().map(|p| p.sequence).collect::<Vec<_>>(),
        [2, 3],
        "the deposit's event and the refusal fact's, answered to the caller"
    );
}

/// A policy's refusal answer that fails is a bug: the step fails as
/// [`StepError::PolicyFailed`], a defect, is dead-lettered by the error's
/// code and args with no English, writes no refusal fact, and the
/// policy's position moves past the trigger, so the next event is handled
/// as usual.
#[test]
fn a_policys_refusal_answer_error_dead_letters_as_policy_failed() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(UnansweringBookkeeping).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let before = slice(&store, &cover("account", 1)).next_sequence;
    let failure = kernel
        .handle::<Account, _>(&store, deposit(3, LEDGER_LIMIT + 1), &at(30))
        .ready()
        .err()
        .expect("the answer's error fails the call");
    let Failure::Downstream { error, .. } = &failure else {
        panic!("a downstream failure: {failure:?}");
    };
    let StepError::PolicyFailed(report) = error else {
        panic!("the policy's failure: {error:?}");
    };
    assert_eq!(
        report.to_string(),
        format!(
            "ANGZARR_REASON_UNHANDLED_HANDLER_ERROR cause=toy-unanswerable-refusal cause.trigger={before}"
        )
    );
    assert_eq!(failure.recovery(), Recovery::Bug);
    assert_eq!(
        slice(&store, &cover("account", 1)).next_sequence,
        before + 1,
        "the deposit stays, and no refusal fact was recorded"
    );
    assert_eq!(
        pages(&store, &ledger_cover()),
        [],
        "the refused step landed nothing"
    );
    let lines = log.lines();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        lines[0].contains(&format!(
            "unanswering-bookkeeping: ANGZARR_REASON_UNHANDLED_HANDLER_ERROR cause=toy-unanswerable-refusal cause.trigger={before}"
        )),
        "{}",
        lines[0]
    );
    let reason = crate::toys::reason(
        "ANGZARR_REASON_UNHANDLED_HANDLER_ERROR",
        &[
            ("cause", "toy-unanswerable-refusal"),
            ("cause.trigger", &before.to_string()),
        ],
    );
    crate::toys::assert_letter(
        &crate::toys::one_letter(log.letters()),
        crate::v1::ComponentKind::Policy,
        &reason,
        0,
    );
    assert!(!lines[0].contains("cannot happen"), "{}", lines[0]);
    assert_eq!(
        position(&store, "unanswering-bookkeeping", &cover("account", 1)),
        before + 1,
        "past the trigger"
    );

    kernel
        .handle::<Account, _>(&store, deposit(4, 5), &at(31))
        .ready()
        .expect("the next event is handled as usual");
    assert_eq!(
        position(&store, "unanswering-bookkeeping", &cover("account", 1)),
        before + 2
    );
    assert_eq!(log.lines().len(), 1, "nothing more dead-lettered");
}

/// A crash between a command's commit and its policy steps leaves them
/// owed. A client's retry of the command is answered as a duplicate, and
/// settles what its stream still owes first, so the retry finishes the
/// chain.
#[test]
fn a_retry_after_a_crash_settles_what_its_stream_still_owes() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry)
        .log_with(&log)
        .on_retryable(crate::toys::no_retries());
    let store = Recording::default();
    opened(&store, &plain, 1, 0);
    store.fail(failing(1, Fault::Unavailable));
    assert!(kernel
        .handle::<Account, _>(&store, deposit(3, 25), &at(30))
        .ready()
        .is_err());
    let retried = kernel
        .handle::<Account, _>(&store, deposit(3, 25), &at(31))
        .ready()
        .unwrap();
    assert!(matches!(retried, crate::Handled::Duplicate(..)));
    assert_eq!(
        pages(store.inner(), &ledger_cover()).len(),
        1,
        "the owed step ran"
    );
    assert_eq!(
        position(store.inner(), "bookkeeping", &cover("account", 1)),
        3
    );
}

/// A policy added to a domain with history owes it everything: `resume`
/// runs it from the start (position 0), one step per event.
#[test]
fn resume_runs_a_new_policy_over_its_sources_history() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 7);
    opened(&store, &plain, 2, 9);
    let resumed = kernel.resume(&store, &at(40)).ready().unwrap();
    assert_eq!(
        payloads::<ledger::Recorded>(resumed.books())
            .iter()
            .map(|r| (r.source_sequence, r.cents))
            .collect::<Vec<_>>(),
        [(0, 0), (1, 7), (0, 0), (1, 9)],
        "every account's events, account by account, in order"
    );
    for n in [1, 2] {
        assert_eq!(position(&store, "bookkeeping", &cover("account", n)), 2);
    }
}

/// A step that landed before its position was put (or a position put back)
/// is run again from the position and deduplicated by its derived command
/// id: nothing lands twice.
#[test]
fn a_resumed_step_that_already_landed_is_a_duplicate() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    kernel.resume(&store, &at(30)).ready().unwrap();
    let account = cover("account", 1);
    let key = crate::PositionKey {
        handler: "bookkeeping",
        stream: sref(&account),
    };
    store.write(Write::positions(&[(key, 0)])).ready().unwrap();
    let resumed = kernel.resume(&store, &at(31)).ready().unwrap();
    assert!(resumed.books().iter().all(|b| b.pages.is_empty()));
    assert_eq!(pages(&store, &ledger_cover()).len(), 2, "nothing twice");
    assert_eq!(position(&store, "bookkeeping", &account), 2);
}

/// A command whose write the store fails is the store's error, and nothing
/// is written (or run after it).
#[test]
fn a_store_that_cannot_write_fails_the_command_with_nothing_written() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry)
        .log_with(&log)
        .on_retryable(crate::toys::no_retries());
    let store = Recording::default();
    store.fail(failing(0, Fault::Unavailable));
    let failure = kernel
        .handle::<Account, _>(
            &store,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
        )
        .ready()
        .err()
        .unwrap();
    assert!(matches!(
        failure,
        Failure::Kernel(KernelError::Store(crate::StoreError::Unavailable(_)))
    ));
    assert_eq!(failure.recovery(), Recovery::Retry);
    assert_eq!(streams(store.inner()), []);
}

/// Waits that complete at once, kept; every jitter draw 0.
#[derive(Default)]
struct Waits(std::sync::Mutex<Vec<std::time::Duration>>);

impl crate::Wait for Waits {
    fn wait(
        &self,
        backoff: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        self.0.lock().unwrap().push(backoff);
        Box::pin(std::future::ready(()))
    }

    fn draw(&self) -> f64 {
        0.0
    }
}

/// A policy's step whose write fails once with `error` is reloaded and
/// run again after one backoff: it lands, and nothing is dead-lettered.
fn a_policy_step_failing_once_lands_on_its_retry(error: crate::StoreError) {
    let mut registry = registry();
    registry.policy(Bookkeeping).unwrap();
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let kernel = Kernel::new(&registry).log_with(&log).wait_with(&waits);
    let store = Recording::default();
    opened(&store, &registry, 1, 0);
    store.fail(failing(1, Fault::Error(error)));
    kernel
        .handle::<Account, _>(&store, deposit(3, 25), &at(30))
        .ready()
        .unwrap();
    assert_eq!(
        pages(store.inner(), &ledger_cover()).len(),
        3,
        "the deposit's step landed on its retry"
    );
    assert_eq!(waits.0.lock().unwrap().len(), 1);
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

/// A policy's step whose write lost a race (a store conflict, ABORTED) has
/// the kernel for its caller: reloaded and run again.
#[test]
fn a_policy_steps_lost_race_is_reloaded_and_run_again() {
    a_policy_step_failing_once_lands_on_its_retry(crate::StoreError::Conflict {
        stream: Box::new(ledger_cover()),
        expected: 2,
        actual: 3,
    });
}

/// A policy's step whose store could not say is a retryable bug: run again.
#[test]
fn a_policy_steps_unavailable_store_is_run_again() {
    a_policy_step_failing_once_lands_on_its_retry(crate::StoreError::Unavailable(
        angzarr_ports::Cause::new(
            angzarr_store_sqlite::SqlError::Unavailable {
                errno: Some(5),
                detail: String::new(),
            }
            .code(),
            [],
        ),
    ));
}

/// The kernel's own jitter draws are uniform in `[0, 1)`: in range, spread
/// over it, and not repeating.
#[test]
fn the_kernels_own_draws_are_uniform_in_the_unit_interval() {
    use crate::Wait;
    let draws: Vec<f64> = (0..2000).map(|_| super::ThreadWait.draw()).collect();
    assert!(draws.iter().all(|d| (0.0..1.0).contains(d)), "{draws:?}");
    let mean = draws.iter().sum::<f64>() / draws.len() as f64;
    assert!((0.4..0.6).contains(&mean), "{mean}");
    let distinct: std::collections::BTreeSet<u64> = draws.iter().map(|d| d.to_bits()).collect();
    assert!(distinct.len() > 1900, "{}", distinct.len());
}

/// The kernel's own wait completes once its backoff has passed, waking the
/// task that waits.
#[test]
fn the_kernels_own_wait_waits_out_its_backoff() {
    use crate::Wait;
    use std::task::{Context, Poll, Wake, Waker};
    struct Unpark(std::thread::Thread);
    impl Wake for Unpark {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(std::sync::Arc::new(Unpark(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let backoff = std::time::Duration::from_millis(30);
    let started = std::time::Instant::now();
    let mut wait = super::ThreadWait.wait(backoff);
    let mut polls = 0;
    while wait.as_mut().poll(&mut cx) == Poll::Pending {
        polls += 1;
        std::thread::park_timeout(std::time::Duration::from_secs(5));
    }
    assert!(started.elapsed() >= backoff, "{:?}", started.elapsed());
    assert!(polls >= 1, "pending until the backoff passed");
}

/// Without threads (`wasm32-unknown-unknown`, which panics on a thread's
/// spawn), the kernel's own wait completes at once, whatever the backoff,
/// so a retry never panics there: a retry runs straight after the failure,
/// and an application that wants the backoff waited out injects its
/// timer ([`Kernel::wait_with`]). Its draws are the kernel's own, uniform
/// in `[0, 1)`. With threads, the wait is the thread's: still pending at
/// its first poll.
#[test]
fn without_threads_the_kernels_own_wait_completes_at_once() {
    use std::task::{Context, Poll, Waker};
    let mut cx = Context::from_waker(Waker::noop());
    let hour = std::time::Duration::from_secs(3600);

    let mut unthreaded = super::default_wait(false).wait(hour);
    assert_eq!(unthreaded.as_mut().poll(&mut cx), Poll::Ready(()));
    let draws: Vec<f64> = (0..64).map(|_| super::default_wait(false).draw()).collect();
    assert!(draws.iter().all(|d| (0.0..1.0).contains(d)), "{draws:?}");
    let distinct: std::collections::BTreeSet<u64> = draws.iter().map(|d| d.to_bits()).collect();
    assert!(distinct.len() > 1, "random, not one value: {draws:?}");

    let mut threaded = super::default_wait(true).wait(hour);
    assert_eq!(threaded.as_mut().poll(&mut cx), Poll::Pending);
}

/// A command with no expected sequence whose write lost a race (the
/// store's conflict: another write to its stream landed first) is the
/// kernel's own race: it is reloaded and decided again on the new head,
/// after one backoff under the retry policy, for an outside caller too
/// (decision 14.45). It lands, and nothing is dead-lettered.
#[test]
fn an_outside_callers_lost_race_without_an_expected_sequence_is_run_again() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let kernel = Kernel::new(&registry).log_with(&log).wait_with(&waits);
    let store = Recording::default();
    opened(&store, &registry, 1, 0);
    store.fail(failing(
        0,
        Fault::Error(crate::StoreError::Conflict {
            stream: Box::new(cover("account", 1)),
            expected: 2,
            actual: 3,
        }),
    ));
    let handled = kernel
        .handle::<Account, _>(&store, deposit(3, 25), &at(30))
        .ready();
    assert!(
        matches!(handled, Ok(crate::Handled::Applied(_))),
        "{:?}",
        handled.err()
    );
    assert_eq!(
        *waits.0.lock().unwrap(),
        [crate::OnRetryable::default().initial_backoff],
        "run again once, after the first backoff"
    );
    assert_eq!(pages(&store, &cover("account", 1)).len(), 3);
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

/// A command whose sender set its expected sequence, and whose write lost a
/// race (here the store's conflict, its head unmoved), is run again under
/// the race re-run cap (decision 14.84): its re-run meets the staleness
/// check against the head it loads again, which is its expected sequence
/// still, so it is decided and lands; nothing is dead-lettered.
#[test]
fn an_outside_callers_lost_race_with_an_expected_sequence_is_run_again_and_checked() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let kernel = Kernel::new(&registry).log_with(&log).wait_with(&waits);
    let store = Recording::default();
    opened(&store, &registry, 1, 0);
    store.fail(failing(
        0,
        Fault::Error(crate::StoreError::Conflict {
            stream: Box::new(cover("account", 1)),
            expected: 2,
            actual: 3,
        }),
    ));
    let handled = kernel
        .handle::<Account, _>(&store, deposit(3, 25).expecting(2), &at(30))
        .ready();
    assert!(
        matches!(handled, Ok(crate::Handled::Applied(_))),
        "{:?}",
        handled.err()
    );
    assert_eq!(
        *waits.0.lock().unwrap(),
        [crate::OnRetryable::default().initial_backoff],
        "run again once, after the first backoff"
    );
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

/// What an [`Unchecked`] policy's Translate answers.
type Translated = fn(&hpb::TranslateRequest) -> Result<hpb::TranslateResponse, CodedError>;

/// A policy of the account's events, behind a router that checks nothing:
/// it answers whatever `answer` builds for the triggering event, as a
/// remote policy's business code might (decision 14.42), and translates a
/// refusal as `translated` says.
struct Unchecked {
    answer: fn(&crate::v1::EventBook) -> Vec<hpb::Output>,
    translated: Translated,
}

impl Unchecked {
    /// Answering `answer`, translating nothing: the router's default.
    fn answering(answer: fn(&crate::v1::EventBook) -> Vec<hpb::Output>) -> Unchecked {
        Unchecked {
            answer,
            translated: |_| {
                Err(crate::spec_coded(
                    crate::v1::AngzarrReason::NoRefusalHandler,
                    [],
                ))
            },
        }
    }
}

impl crate::angzarr_router::router::PolicyComponent for Unchecked {
    fn name(&self) -> &str {
        "unchecked"
    }
    fn input_domain(&self) -> &str {
        "account"
    }
    async fn react(&self, request: &hpb::ReactRequest) -> Result<hpb::ReactResponse, CodedError> {
        let trigger = request.trigger.clone().unwrap_or_default();
        Ok(hpb::ReactResponse {
            outputs: (self.answer)(&trigger),
        })
    }
    async fn translate(
        &self,
        request: &hpb::TranslateRequest,
    ) -> Result<hpb::TranslateResponse, CodedError> {
        (self.translated)(request)
    }
}

/// A ledger `Record` for the stream rooted at `root`, as a policy's
/// business code returns it: a command Output, its root named.
fn record_at(root: crate::v1::Uuid) -> hpb::Output {
    crate::angzarr_router::policy::command(
        Some(root),
        crate::pack(&ledger::Record {
            source_sequence: 0,
            cents: 1,
        }),
    )
}

/// What the kernel refuses `answer`'s outputs with, as a policy into
/// `output_domain` of the account's opening, nothing delivered: the
/// trigger dead-lettered under the refusal's key and args, as answered,
/// and the position held at the trigger.
fn unchecked_refusal(
    output_domain: &str,
    answer: fn(&crate::v1::EventBook) -> Vec<hpb::Output>,
) -> CodedError {
    let mut registry = registry();
    registry
        .policy_component(Unchecked::answering(answer), output_domain)
        .unwrap();
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let failure = Kernel::new(&registry)
        .log_with(&log)
        .handle::<Account, _>(
            &store,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
        )
        .ready()
        .err()
        .expect("the policy's bug fails its step");
    assert_eq!(streams(&store), [cover("account", 1)], "nothing delivered");
    assert_eq!(failure.recovery(), Recovery::Bug);
    let error = match failure {
        Failure::Downstream {
            error: StepError::Kernel(KernelError::Router(error)),
            ..
        } => error,
        other => panic!("the policy's bug: {other:?}"),
    };
    let why = crate::v1::angzarr_dead_letter::Why::Reason(crate::v1::Diagnostic::with_text_args(
        error.code.clone(),
        error.extras.clone(),
    ));
    let letter = crate::toys::one_letter(log.letters());
    crate::toys::assert_letter(&letter, crate::v1::ComponentKind::Policy, &why, 0);
    assert!(
        matches!(
            &letter.payload,
            Some(crate::v1::angzarr_dead_letter::Payload::RejectedEvents(book))
                if book.cover == Some(cover("account", 1))
        ),
        "the trigger: {letter:?}"
    );
    assert_eq!(position(&store, "unchecked", &cover("account", 1)), 0);
    error
}

/// The kernel, not the router, checks a policy's answer (decision 14.42):
/// an output with no root (or one not 16 bytes) refuses the whole answer
/// as the policy's bug, ANGZARR_REASON_POLICY_COMMAND_NO_ROOT naming its
/// destination and type, INTERNAL; nothing is delivered, not even an
/// output that was well formed.
#[test]
fn a_policys_output_with_no_root_refuses_its_whole_answer() {
    for refused in [
        unchecked_refusal("ledger", |_| {
            vec![
                record_at(ledger_root()),
                hpb::Output {
                    root: None,
                    ..record_at(ledger_root())
                },
            ]
        }),
        unchecked_refusal("ledger", |_| {
            vec![hpb::Output {
                root: Some(crate::v1::Uuid {
                    value: Vec::new().into(),
                }),
                ..record_at(ledger_root())
            }]
        }),
    ] {
        assert_eq!(refused.code, "ANGZARR_REASON_POLICY_COMMAND_NO_ROOT");
        assert_eq!(
            refused.extras.get("domain").map(String::as_str),
            Some("ledger")
        );
        assert_eq!(
            refused.extras.get("command_type").map(String::as_str),
            Some("toy.ledger.cmd.v1.Record")
        );
        assert_eq!(
            refused.grpc,
            crate::angzarr_router::error::GrpcCode::Internal
        );
    }
}

/// An output addressing its trigger's own stream refuses the whole answer
/// (decision 14.71): ANGZARR_REASON_POLICY_TARGETS_ITS_TRIGGER, naming the
/// stream and the output's type.
#[test]
fn a_policys_output_to_its_triggers_own_stream_refuses_its_whole_answer() {
    let refused = unchecked_refusal("account", |source| {
        vec![crate::angzarr_router::policy::fact(
            source.cover.as_ref().and_then(|c| c.root.clone()),
            crate::toys::AccountFact::PaymentReceived(1).to_any(),
        )]
    });
    assert_eq!(refused.code, "ANGZARR_REASON_POLICY_TARGETS_ITS_TRIGGER");
    assert_eq!(
        refused.extras.get("stream").map(String::as_str),
        Some(crate::error::cover_text(&cover("account", 1)).as_str())
    );
}

/// Each output is delivered stamped by the kernel (decision 14.79): to the
/// destination at its root in the trigger's edition, under the id derived
/// from (trigger, policy, its index), the trigger's correlation, the
/// trigger as its causation; and the policy's position moves past the
/// trigger with the last.
#[test]
fn a_policys_outputs_are_delivered_stamped_each_by_its_index() {
    let mut registry = registry();
    registry
        .policy_component(
            Unchecked::answering(|_| {
                vec![
                    record_at(two_ledgers_root(0)),
                    record_at(two_ledgers_root(1)),
                ]
            }),
            "ledger",
        )
        .unwrap();
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let open = command(cover("account", 1), AccountCmd::Open.to_any()).correlated(5);
    Kernel::new(&registry)
        .log_with(&log)
        .handle::<Account, _>(&store, open, &at(30))
        .ready()
        .expect("delivered");
    assert!(log.lines().is_empty(), "{:?}", log.lines());
    let trigger = EventRef {
        stream: Some(cover("account", 1)),
        sequence: 0,
    };
    for index in 0..2 {
        let ledger = crate::v1::Cover {
            domain: "ledger".to_string(),
            root: Some(two_ledgers_root(index as u8)),
            edition: None,
        };
        let stored = pages(&store, &ledger);
        assert_eq!(stored.len(), 1, "ledger {index}");
        let meta = stored[0].meta.clone().unwrap();
        assert_eq!(
            meta.causation,
            Some(Causation::Command(crate::v1::CommandCause {
                command_id: Some(ids::policy_command_id(&trigger, "unchecked", index).unwrap())
            })),
            "ledger {index}"
        );
        assert_eq!(meta.correlation_id, Some(crate::toys::correlation(5)));
    }
    assert_eq!(position(&store, "unchecked", &cover("account", 1)), 1);
}

/// A policy's fact (decision 14.69) whose handler fails, a bug, is
/// dead-lettered at its destination, never refused: the policy's step is
/// done, its position past the trigger, and the chain goes on (C-0778).
#[test]
fn a_policys_fact_whose_handler_fails_is_dead_lettered_and_its_step_done() {
    use crate::v1::angzarr_dead_letter::Payload;
    let mut registry = registry();
    registry
        .policy_component(
            Unchecked::answering(|source| {
                let from_one = source.cover.as_ref() == Some(&cover("account", 1));
                // A negative payment: the toy's ingest panics, a bug.
                let fact = crate::angzarr_router::policy::fact(
                    cover("account", 2).root,
                    crate::toys::AccountFact::PaymentReceived(-1).to_any(),
                );
                match from_one {
                    true => vec![fact],
                    false => Vec::new(),
                }
            }),
            "account",
        )
        .unwrap();
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let handled = Kernel::new(&registry)
        .log_with(&log)
        .on_retryable(crate::toys::no_retries())
        .handle::<Account, _>(
            &store,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
        )
        .ready()
        .expect("a dead-lettered fact fails no step");
    assert_eq!(handled.books().len(), 1, "the account's own events alone");
    assert_eq!(
        streams(&store),
        [cover("account", 1)],
        "nothing recorded for it"
    );
    let rejected: Vec<_> = log
        .letters()
        .into_iter()
        .filter_map(|l| match l.payload {
            Some(Payload::RejectedReaction(delivery)) => delivery.body,
            _ => None,
        })
        .collect();
    assert!(
        matches!(&rejected[..], [crate::v1::reaction_delivery::Body::Fact(_)]),
        "the fact dead-lettered as rejected_reaction: {rejected:?}"
    );
    assert_eq!(position(&store, "unchecked", &cover("account", 1)), 1);
}

/// A ledger whose business process answers every input with a business
/// status and no typed refusal: what a remote one may answer a fact,
/// though a fact is never refused.
struct RefusesAll;

impl crate::angzarr_router::router::CommandHandler for RefusesAll {
    fn name(&self) -> &str {
        "ledger"
    }
    fn command_types(&self) -> Vec<String> {
        Vec::new()
    }
    async fn dispatch(&self, _: &hpb::HandleRequest) -> Result<hpb::HandleResponse, CodedError> {
        Err(CodedError {
            code: "TOY_REFUSED".to_string(),
            extras: Default::default(),
            grpc: crate::angzarr_router::error::GrpcCode::FailedPrecondition,
            refusal: None,
        })
    }
}

/// The account toy, and a ledger whose business process refuses every
/// input ([`RefusesAll`]).
fn refusing_ledger() -> crate::Registry {
    let mut registry = crate::Registry::new();
    crate::toys::register::<Account>(
        &mut registry,
        crate::Upcaster::new("account-upcaster", "account"),
    )
    .unwrap();
    registry
        .command_handler(
            "ledger",
            RefusesAll,
            crate::toys::descriptor_of::<Ledger>(crate::v1::ComponentKind::Aggregate, "v2", &[]),
        )
        .unwrap();
    registry
}

/// The one dead letter of `log`: a fact its business process answered as
/// it answers a command (`TOY_REFUSED`), which for a fact is that
/// process's bug (decision 14.72): ANGZARR_REASON_UNHANDLED_HANDLER_ERROR,
/// its answer the cause.
fn rejected_fact(log: &MemoryDeadLetters) -> crate::v1::AngzarrDeadLetter {
    let letter = crate::toys::one_letter(log.letters());
    assert_eq!(
        letter.why,
        Some(crate::toys::reason(
            "ANGZARR_REASON_UNHANDLED_HANDLER_ERROR",
            &[("cause", "TOY_REFUSED")]
        )),
        "{letter:?}"
    );
    letter
}

/// A policy's fact its destination's business process answers with a
/// business status, which a fact never is refused with (decision 14.72):
/// its process's bug, dead-lettered at the destination, so the policy's
/// step is done and its position past the trigger (C-0785). Not the
/// refusal path: no Translate, no refusal fact.
#[test]
fn a_policys_fact_answered_with_a_rejection_is_dead_lettered_and_its_step_done() {
    use crate::v1::angzarr_dead_letter::Payload;
    let mut registry = refusing_ledger();
    registry
        .policy_component(
            Unchecked::answering(|_| {
                vec![crate::angzarr_router::policy::fact(
                    Some(ledger_root()),
                    crate::pack(&ledger::Record {
                        source_sequence: 0,
                        cents: 1,
                    }),
                )]
            }),
            "ledger",
        )
        .unwrap();
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let handled = Kernel::new(&registry)
        .log_with(&log)
        .on_retryable(crate::toys::no_retries())
        .handle::<Account, _>(
            &store,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
        )
        .ready()
        .expect("a dead-lettered fact fails no step");
    assert_eq!(handled.books().len(), 1, "the account's own events alone");
    let letter = rejected_fact(&log);
    assert!(
        matches!(&letter.payload, Some(Payload::RejectedReaction(_))),
        "{letter:?}"
    );
    assert_eq!(streams(&store), [cover("account", 1)], "no refusal fact");
    assert_eq!(position(&store, "unchecked", &cover("account", 1)), 1);
}

/// A client's fact its business process answers with a business status
/// (decision 14.72): dead-lettered, even with its sender waiting, and its
/// sender answered FACT_DEAD_LETTERED, never refused; nothing is written
/// (C-0786).
#[test]
fn a_clients_fact_answered_with_a_rejection_is_dead_lettered_and_answered() {
    use crate::v1::angzarr_dead_letter::Payload;
    let registry = refusing_ledger();
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let fact = crate::toys::fact(
        ledger_cover(),
        crate::pack(&ledger::Record {
            source_sequence: 0,
            cents: 1,
        }),
        "rec-1",
    );
    let failure = Kernel::new(&registry)
        .log_with(&log)
        .on_retryable(crate::toys::no_retries())
        .handle::<Ledger, _>(&store, fact, &at(30))
        .ready()
        .err()
        .expect("its sender is answered");
    let Failure::Kernel(error @ KernelError::FactDeadLettered { .. }) = &failure else {
        panic!("dead-lettered: {failure:?}");
    };
    let coded = error.as_coded().expect("a spec key");
    assert_eq!(coded.code, "ANGZARR_REASON_FACT_DEAD_LETTERED");
    assert_eq!(coded.grpc, crate::angzarr_router::error::GrpcCode::Internal);
    assert_eq!(failure.recovery(), Recovery::Bug, "never retried");
    let letter = rejected_fact(&log);
    assert!(
        matches!(&letter.payload, Some(Payload::RejectedCommand(_))),
        "{letter:?}"
    );
    assert!(streams(&store).is_empty(), "nothing written");
}

/// An account whose business process records an `Opened` for every
/// command, and answers every fact, a refusal fact included, with a
/// business status.
struct RejectsFacts;

impl crate::angzarr_router::router::CommandHandler for RejectsFacts {
    fn name(&self) -> &str {
        "account"
    }
    fn command_types(&self) -> Vec<String> {
        Vec::new()
    }
    async fn dispatch(
        &self,
        delivered: &hpb::HandleRequest,
    ) -> Result<hpb::HandleResponse, CodedError> {
        use crate::v1::command_page::Body;
        let page = delivered.input.as_ref().and_then(|b| b.page.as_ref());
        if let Some(Body::Fact(_)) = page.and_then(|p| p.body.as_ref()) {
            return RefusesAll.dispatch(delivered).await;
        }
        Ok(hpb::HandleResponse {
            events: vec![crate::toys::AccountEvent::Opened.to_any()],
            state: None,
            rules_version: "v2".to_string(),
        })
    }
}

/// A refusal fact its source's business process answers with a business
/// status (decision 14.72): a bug of that process, as a failing handler
/// is, dead-lettered at the source; its letter is its record, the step is
/// done, and the policy's position moves past the trigger.
#[test]
fn a_refusal_fact_answered_with_a_rejection_is_its_sources_bug() {
    use crate::v1::angzarr_dead_letter::Payload;
    let mut registry = crate::Registry::new();
    registry
        .command_handler(
            "account",
            RejectsFacts,
            crate::toys::descriptor_of::<Account>(crate::v1::ComponentKind::Aggregate, "v2", &[]),
        )
        .unwrap();
    crate::toys::register::<Ledger>(
        &mut registry,
        crate::Upcaster::new("ledger-upcaster", "ledger"),
    )
    .unwrap();
    registry
        .policy_component(
            Unchecked {
                answer: |_| {
                    vec![crate::angzarr_router::policy::command(
                        Some(ledger_root()),
                        crate::pack(&ledger::Record {
                            source_sequence: 0,
                            cents: LEDGER_LIMIT + 1,
                        }),
                    )]
                },
                translated: |_| {
                    Ok(hpb::TranslateResponse {
                        fact: Some(crate::toys::AccountFact::PaymentReceived(0).to_any()),
                    })
                },
            },
            "ledger",
        )
        .unwrap();
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    Kernel::new(&registry)
        .log_with(&log)
        .on_retryable(crate::toys::no_retries())
        .handle::<Account, _>(
            &store,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
        )
        .ready()
        .expect("its letter is its record: the step is done");
    let letter = rejected_fact(&log);
    let Some(Payload::RejectedReaction(delivery)) = &letter.payload else {
        panic!("the refusal fact, as delivered: {letter:?}");
    };
    assert_eq!(
        delivery.cover,
        Some(cover("account", 1)),
        "the trigger's stream"
    );
    assert!(matches!(
        delivery.body,
        Some(crate::v1::reaction_delivery::Body::Fact(_))
    ));
    assert_eq!(
        position(&store, "unchecked", &cover("account", 1)),
        1,
        "past the trigger"
    );
}

/// A step folds its stream once, the router's fold that decides it: the
/// kernel decodes and folds nothing again to type what it recorded. That
/// is done when a caller asks (`Recorded::events`), and only for what
/// the chain recorded.
#[test]
fn a_step_folds_its_stream_once() {
    use crate::toys::APPLIED;
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let folds = |n: u128| {
        APPLIED.with(|applied| applied.set(0));
        crate::toys::handle::<Account>(&store, &registry, deposit(n, 1), &at(20)).unwrap();
        APPLIED.with(std::cell::Cell::get)
    };
    // Loaded from the snapshot the last step wrote, the router folds the
    // one event it decides onto it; nothing folds it again.
    assert_eq!((folds(3), folds(4)), (1, 1));
}

/// The write `n` writes from now (0: the next) failing with `fault`, the
/// ones before it sound.
fn failing(n: usize, fault: angzarr_ports::faulty::Fault) -> Vec<angzarr_ports::faulty::Fault> {
    let mut faults = vec![angzarr_ports::faulty::Fault::Sound; n];
    faults.push(fault);
    faults
}

// Decision 14.111: a typed refusal may declare ABORTED, and the status alone
// decides its class (14.37): "re-read and re-decide", run again for a step
// whose caller is the kernel, never a business-fatal refusal.
fn rejected_as(status: crate::angzarr_router::error::BusinessStatus) -> super::Rejected {
    let refusal = crate::pack(&crate::toys::ToyRefusal {
        code: "toy-moved".to_string(),
    });
    super::Rejected::of(&CodedError::refused(status, refusal)).expect("a typed refusal")
}

#[test]
fn an_aborted_refusal_is_of_the_immediate_class() {
    use crate::angzarr_router::error::BusinessStatus;
    let aborted = Failure::Rejected(rejected_as(BusinessStatus::Aborted));
    assert_eq!(
        super::failure_class(&aborted),
        Some(crate::error::Class::BusinessImmediate)
    );
}

#[test]
fn a_business_fatal_refusal_has_no_class_to_run_again() {
    use crate::angzarr_router::error::BusinessStatus;
    for status in [
        BusinessStatus::FailedPrecondition,
        BusinessStatus::InvalidArgument,
        BusinessStatus::NotFound,
        BusinessStatus::AlreadyExists,
        BusinessStatus::OutOfRange,
        BusinessStatus::PermissionDenied,
    ] {
        let refused = Failure::Rejected(rejected_as(status));
        assert_eq!(super::failure_class(&refused), None, "{status:?}");
    }
}
