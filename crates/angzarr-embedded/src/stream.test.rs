//! Replay is strict and checked: every page's sequence, metadata,
//! instant and rules; snapshots resume it; golden fixtures pin it; and every
//! failure says how to recover.
use crate::toys::prelude::*;
use crate::v1::event_meta::Causation;
use crate::v1::{EventMeta, EventPage, Snapshot};
use crate::{
    assert_replay_fixtures, check_replay_fixtures, pack, Failure, KernelError, Malformed, Recovery,
    ReplayError, ReplayFixture, StepError, StoreError, Stream,
};
use angzarr_diagnostic::{code, Report};
use angzarr_ports::Cause;
use angzarr_proto::v1::AngzarrReason;
use angzarr_router::error::{BusinessStatus, CodedError, GrpcCode};

use crate::toys::{
    account, at, command, command_id, cover, instant, registry, registry_with_upcasters, root,
    Account, AccountCmd, Edit, Ledger, Shelf,
};
use crate::Payload;

fn meta(sequence: u64, rules: &str) -> EventMeta {
    EventMeta {
        causation: Some(Causation::Command(crate::v1::CommandCause {
            command_id: Some(command_id(sequence as u128)),
        })),
        correlation_id: Some(command_id(sequence as u128)),
        recorded_at: Some(instant(sequence as i64)),
        rules_version: rules.to_string(),
    }
}

fn page(sequence: u64, payload: crate::Any) -> EventPage {
    EventPage {
        body: Some(crate::v1::event_page::Body::Event(payload)),
        meta: Some(meta(sequence, "v2")),
        sequence,
    }
}

/// An open account that deposited 100 then withdrew 10 (and its fee of 1),
/// the withdrawal decided by another build, `v1`.
fn log() -> Vec<EventPage> {
    let mut withdrew = page(2, pack(&account::Withdrew { cents: 10 }));
    withdrew.meta.as_mut().unwrap().rules_version = "v1".to_string();
    vec![
        page(0, pack(&account::Opened {})),
        page(1, pack(&account::Deposited { cents: 100 })),
        withdrew,
    ]
}

fn replay(pages: &[EventPage]) -> Result<Stream<Account>, KernelError> {
    Stream::replay(cover("account", 1), pages, &registry())
}

#[test]
fn replay_folds_each_event_and_keeps_the_version_that_decided_it() {
    let stream = replay(&log()).unwrap();
    assert_eq!(stream.state().cents, 89);
    assert_eq!(stream.len(), 3);
    assert_eq!(stream.last_recorded_at(), Some(instant(2)));
    assert_eq!(stream.events()[2].rules_version(), "v1");
    assert_eq!(stream.events()[1].command_id(), &command_id(1));
    assert_eq!(stream.events()[1].correlation_id(), &command_id(1));
    assert_eq!(stream.events()[1].fact(), None);
    assert_eq!(stream.events()[1].meta(), meta(1, "v2"));
    assert_eq!(
        stream.events()[1].page(),
        page(1, pack(&account::Deposited { cents: 100 }))
    );
    assert_eq!(stream.find_command(&command_id(1)).len(), 1);
    assert!(stream.find_command(&command_id(9)).is_empty());
    let (c, events, state) = stream.into_parts();
    assert_eq!((c, events.len(), state.cents), (cover("account", 1), 3, 89));
}

#[test]
fn replay_refuses_a_log_that_does_not_hold_together() {
    let mut pages = log();
    pages[1].sequence = 5;
    assert_eq!(
        replay(&pages).err(),
        Some(KernelError::Replay(ReplayError::OutOfSequence {
            index: 1,
            found: 5
        }))
    );

    let mut pages = log();
    pages[2].meta.as_mut().unwrap().recorded_at = Some(instant(0));
    assert_eq!(
        replay(&pages).err(),
        Some(KernelError::Replay(ReplayError::RecordedBackwards {
            sequence: 2
        }))
    );
    pages[2].meta.as_mut().unwrap().recorded_at = Some(instant(1));
    assert!(replay(&pages).is_ok(), "the same instant is not backwards");

    let mut pages = log();
    pages[2].meta.as_mut().unwrap().rules_version = "v0".to_string();
    assert!(
        replay(&pages).is_ok(),
        "an event another build decided folds as any: its version is never read to fold it"
    );

    for (field, edit) in [
        (
            "causation",
            (|m: &mut EventMeta| m.causation = None) as fn(&mut EventMeta),
        ),
        ("causation", |m| {
            m.causation = Some(Causation::Command(crate::v1::CommandCause {
                command_id: Some(crate::v1::Uuid {
                    value: vec![1; 15].into(),
                }),
            }))
        }),
        ("causation", |m| {
            m.causation = Some(Causation::Command(crate::v1::CommandCause {
                command_id: None,
            }))
        }),
        ("correlation_id", |m| m.correlation_id = None),
        ("correlation_id", |m| {
            m.correlation_id = Some(crate::v1::Uuid {
                value: vec![1; 17].into(),
            })
        }),
        ("recorded_at", |m| m.recorded_at = None),
    ] {
        let mut pages = log();
        edit(pages[1].meta.as_mut().unwrap());
        assert_eq!(
            replay(&pages).err(),
            Some(KernelError::Replay(ReplayError::IncompleteMeta {
                sequence: 1,
                field
            }))
        );
    }

    assert_eq!(
        Stream::<Account>::replay(cover("ledger", 1), &log(), &registry()).err(),
        Some(KernelError::Replay(ReplayError::WrongDomain {
            found: "ledger".to_string(),
            expected: "account"
        }))
    );
}

#[test]
fn a_stream_resumes_from_a_snapshot_and_holds_only_what_follows_it() {
    let registry = registry();
    let full = replay(&log()).unwrap();
    let pages = log();
    let snapshot = Snapshot {
        sequence: 1,
        state: Some(pack(&account::AccountState {
            open: true,
            cents: 100,
            ..Default::default()
        })),
        rules_version: "v2".to_string(),
    };
    let resumed =
        Stream::<Account>::resume(cover("account", 1), &snapshot, None, &pages[2..], &registry)
            .unwrap();
    assert_eq!(resumed.first(), 2);
    assert_eq!(resumed.len(), 3);
    assert_eq!(resumed.events().len(), 1);
    assert_eq!(resumed.state(), full.state());

    assert_eq!(resumed.events()[0].sequence(), 2, "the event after it");
    assert_eq!(
        resumed.state().cents,
        89,
        "the snapshot's state, then the event"
    );

    // The floor for the next instant is the one given.
    let floor = Stream::<Account>::resume(
        cover("account", 1),
        &snapshot,
        Some(instant(9)),
        &pages[2..],
        &registry,
    )
    .err();
    assert_eq!(
        floor,
        Some(KernelError::Replay(ReplayError::RecordedBackwards {
            sequence: 2
        }))
    );

    let foreign = Snapshot {
        state: Some(pack(&crate::toys::ShelfState::default())),
        ..snapshot.clone()
    };
    let error = Stream::<Account>::resume(cover("account", 1), &foreign, None, &[], &registry)
        .err()
        .unwrap();
    assert!(
        matches!(&error, KernelError::Unreadable { sequence: 1, reason, .. }
            if format!("{reason:?}").contains("toy.shelf.v1.ShelfState")),
        "{error:?}"
    );
    let garbled = Snapshot {
        state: Some(crate::Any {
            type_url: "/toy.account.v1.AccountState".to_string(),
            value: vec![0x08],
        }),
        ..snapshot
    };
    assert!(matches!(
        Stream::<Account>::resume(cover("account", 1), &garbled, None, &[], &registry),
        Err(KernelError::Unreadable { .. })
    ));
}

/// A book of a stream's pages (a projector's, or what a chain recorded)
/// decodes as replay decodes it, each page upcast and checked, folding
/// nothing; a book of another domain's stream is refused.
#[test]
fn a_book_decodes_as_its_domains_events() {
    let registry = registry_with_upcasters();
    let pages = log();
    let book = crate::v1::EventBook {
        cover: Some(cover("account", 1)),
        pages: pages[1..].to_vec(),
        ..crate::v1::EventBook::default()
    };
    let events = Stream::<Account>::decode_book(&book, &registry).unwrap();
    let replayed = replay(&pages).unwrap();
    assert_eq!(events, replayed.events()[1..]);
    let shelf = crate::v1::EventBook {
        cover: Some(cover("shelf", 1)),
        ..book
    };
    assert_eq!(
        Stream::<Account>::decode_book(&shelf, &registry).err(),
        Some(KernelError::Replay(ReplayError::WrongDomain {
            found: "shelf".to_string(),
            expected: "account",
        }))
    );
}

#[test]
fn a_decision_commits_only_to_the_stream_and_head_it_was_made_against() {
    let registry = registry();
    let log = crate::MemoryDeadLetters::new();
    let mut a = Stream::<Account>::new(root(1), None).unwrap();
    let mut b = Stream::<Account>::new(root(2), None).unwrap();
    let open = |n| command(cover("account", n), AccountCmd::Open.to_any());
    a.execute(open(1), &registry, &at(1), &log).ready().unwrap();
    // Another stream's book, as one at another head, is refused
    // ExpectedSequenceMismatch at this stream's head.
    assert_eq!(
        mismatch(a.execute(open(2), &registry, &at(2), &log).ready().err()),
        Some((1, 1))
    );
    assert_eq!(
        mismatch(b.execute(open(1), &registry, &at(2), &log).ready().err()),
        Some((0, 0))
    );
    assert_eq!(
        mismatch(
            a.execute(open(1).expecting(0), &registry, &at(2), &log)
                .ready()
                .err()
        ),
        Some((0, 1)),
        "behind the head"
    );
    assert_eq!(
        a.execute(
            command(cover("ledger", 1), AccountCmd::Open.to_any()),
            &registry,
            &at(2),
            &log
        )
        .ready()
        .err(),
        Some(Failure::Kernel(KernelError::WrongDomain {
            stream: cover("ledger", 1),
            expected: "account"
        }))
    );
    assert_eq!(
        Stream::<Account>::new(
            crate::v1::Uuid {
                value: vec![1].into()
            },
            None
        )
        .err(),
        Some(Malformed::BadRoot { len: 1 })
    );
    assert_eq!(
        Stream::<Account>::new(root(1), Some(String::new())).err(),
        Some(Malformed::EmptyEdition)
    );
    let deposit = || command(cover("account", 1), AccountCmd::Deposit(4).to_any()).id(9);
    let applied = a
        .execute(deposit(), &registry, &at(3), &log)
        .ready()
        .unwrap();
    assert!(matches!(applied, crate::Executed::Applied(e) if e.len() == 1));
    let duplicate = a
        .execute(deposit(), &registry, &at(4), &log)
        .ready()
        .unwrap();
    assert!(matches!(duplicate, crate::Executed::Duplicate(e)
        if e.len() == 1 && e[0].sequence() == 1));
    assert_eq!(a.state().cents, 4, "applied once");
}

#[test]
fn golden_fixtures_replay_through_the_upcaster_and_name_every_failure() {
    let registry = registry_with_upcasters();
    let state = |cents| account::AccountState {
        open: true,
        cents,
        ..Default::default()
    };
    let old = vec![
        page(0, pack(&account::Opened {})),
        page(1, pack(&account::v0::Credited { euros: 2 })),
    ];
    let good = ReplayFixture {
        name: "v0 credit",
        stream: cover("account", 1),
        log: old.clone(),
        states: vec![(0, state(0)), (1, state(200))],
    };
    assert_replay_fixtures::<Account>(&registry, std::slice::from_ref(&good));

    let drifted = ReplayFixture {
        name: "drifted",
        states: vec![(1, state(2)), (5, state(0))],
        ..good
    };
    let unreadable = ReplayFixture {
        name: "unreadable",
        stream: cover("account", 1),
        log: old,
        states: vec![],
    };
    let failures = check_replay_fixtures::<Account>(&registry, &[drifted]).unwrap_err();
    assert_eq!(
        failures.len(),
        2,
        "every failure of a fixture is listed: {failures:#?}"
    );
    assert!(
        failures[0].starts_with("drifted: after event 1"),
        "{}",
        failures[0]
    );
    assert_eq!(failures[1], "drifted: no event 5");
    let failures =
        check_replay_fixtures::<Account>(&crate::toys::registry(), &[unreadable]).unwrap_err();
    assert_eq!(failures.len(), 1);
    assert!(failures[0].starts_with("unreadable: "), "{}", failures[0]);
    assert!(
        failures[0].contains("toy.account.v0.Credited"),
        "{}",
        failures[0]
    );

    let missing = ReplayFixture {
        name: "missing",
        stream: cover("account", 1),
        log: vec![page(0, pack(&account::Opened {}))],
        states: vec![(3, state(0))],
    };
    assert_eq!(
        check_replay_fixtures::<Account>(&registry, &[missing]),
        Err(vec!["missing: no event 3".to_string()])
    );
}

#[test]
#[should_panic(expected = "account replay fixtures do not replay")]
fn assert_replay_fixtures_panics_with_every_failure() {
    let fixture = ReplayFixture {
        name: "empty",
        stream: cover("account", 1),
        log: vec![],
        states: vec![(0, account::AccountState::default())],
    };
    assert_replay_fixtures::<Account>(&registry(), &[fixture]);
}

/// Each error says how to recover, as itself and as the kernel's failure.
fn assert_recovers<const N: usize>(cases: [(Recovery, KernelError); N]) {
    for (recovery, error) in cases {
        let failure = Failure::<Malformed>::Kernel(error.clone());
        assert_eq!(failure.recovery(), recovery, "{error:?}");
        assert_eq!(error.recovery(), recovery, "{error:?}");
    }
}

/// A store that did not answer, retried.
fn unavailable() -> KernelError {
    KernelError::Store(StoreError::Unavailable(Cause::new("x-down", [])))
}

/// A refusal's expected sequence and head, if `failure` is the stale
/// refusal.
fn mismatch(failure: Option<Failure<crate::Rejected>>) -> Option<(u64, u64)> {
    let Some(Failure::Rejected(rejected)) = failure else {
        return None;
    };
    crate::toys::mismatch_of(&rejected)
}

/// The kernel's own errors say how to recover.
#[test]
fn the_kernels_errors_say_how_to_recover() {
    let stream = cover("account", 1);
    assert_recovers([
        (
            Recovery::Invalid,
            KernelError::Malformed(Malformed::NoCover),
        ),
        (
            Recovery::Invalid,
            KernelError::UnknownType {
                stream: stream.clone(),
                type_url: String::new(),
            },
        ),
        (Recovery::Retry, unavailable()),
        (
            Recovery::Bug,
            KernelError::RetryCapExhausted {
                stream: stream.clone(),
                attempts: 3,
                cap: 3,
            },
        ),
        (
            Recovery::Bug,
            KernelError::FactDeadLettered {
                stream: stream.clone(),
                cause: Report::new("x-bad"),
            },
        ),
        (
            Recovery::Bug,
            KernelError::Unreadable {
                stream: stream.clone(),
                sequence: 0,
                reason: Report::new("x-bad"),
            },
        ),
        (
            Recovery::Bug,
            KernelError::Upcast {
                stream: stream.clone(),
                error: crate::UpcastError::Rule {
                    source: String::new(),
                    error: Report::new("x-bad"),
                },
            },
        ),
        (
            Recovery::Bug,
            KernelError::WrongDomain {
                stream: stream.clone(),
                expected: "ledger",
            },
        ),
        (Recovery::Bug, KernelError::ChainTooDeep { limit: 1 }),
        (
            Recovery::Bug,
            KernelError::BeforeSnapshot {
                stream: stream.clone(),
                first: 1,
            },
        ),
        (
            Recovery::Bug,
            KernelError::Replay(ReplayError::RecordedBackwards { sequence: 1 }),
        ),
    ]);
}

/// The store's errors, forwarded, say how to recover.
#[test]
fn the_stores_errors_say_how_to_recover() {
    let stream = cover("account", 1);
    assert_recovers([
        (
            Recovery::Retry,
            KernelError::Store(StoreError::Conflict {
                stream: Box::new(stream.clone()),
                expected: 0,
                actual: 1,
            }),
        ),
        (
            Recovery::Retry,
            KernelError::Store(StoreError::Unavailable(Cause::new("x-down", []))),
        ),
        (
            Recovery::Invalid,
            KernelError::Store(StoreError::MalformedStream {
                stream: Box::new(stream.clone()),
            }),
        ),
        (
            Recovery::Bug,
            KernelError::Store(StoreError::Discontinuous {
                stream: Box::new(stream.clone()),
                expected: 0,
                found: 1,
            }),
        ),
        (
            Recovery::Bug,
            KernelError::Store(StoreError::Unreadable {
                stream: Box::new(stream.clone()),
                sequence: 0,
                reason: Cause::new("x-bad", []),
            }),
        ),
    ]);
}

/// A handler's code says how to recover by its status.
#[test]
fn a_handlers_code_says_how_to_recover_by_its_status() {
    assert_recovers([
        (
            Recovery::Business,
            KernelError::Router(CodedError::refused(
                BusinessStatus::FailedPrecondition,
                pack(&crate::toys::ToyRefusal {
                    code: "PLAYER_BROKE".to_string(),
                }),
            )),
        ),
        (
            Recovery::Bug,
            KernelError::Router(CodedError::bug("PLAYER_BROKE", [])),
        ),
        (
            Recovery::Retry,
            KernelError::Router(CodedError::immediate("PLAYER_BROKE", [])),
        ),
        // A handler's own code: under a business status a refusal, under a
        // bug's status a bug (the owner's ruling, 2026-10-29). Every status
        // of the business set arrives as itself (angzarr-router's GrpcCode
        // has each), so each is a refusal.
        (
            Recovery::Business,
            KernelError::Router(CodedError::refused(
                BusinessStatus::InvalidArgument,
                pack(&crate::toys::ToyRefusal {
                    code: "PLAYER_BROKE".to_string(),
                }),
            )),
        ),
        (
            Recovery::Business,
            KernelError::Router(own(GrpcCode::NotFound)),
        ),
        (
            Recovery::Business,
            KernelError::Router(own(GrpcCode::AlreadyExists)),
        ),
        (
            Recovery::Business,
            KernelError::Router(own(GrpcCode::OutOfRange)),
        ),
        (
            Recovery::Business,
            KernelError::Router(own(GrpcCode::PermissionDenied)),
        ),
        (Recovery::Bug, KernelError::Router(own(GrpcCode::Internal))),
        (Recovery::Bug, KernelError::Router(own(GrpcCode::Unknown))),
        (Recovery::Bug, KernelError::Router(own(GrpcCode::DataLoss))),
        // Under a retryable status, neither a refusal nor a bug: retried
        // (the owner's ruling, 2026-10-29).
        (
            Recovery::Retry,
            KernelError::Router(own(GrpcCode::Unimplemented)),
        ),
        (
            Recovery::Retry,
            KernelError::Router(own(GrpcCode::Unavailable)),
        ),
        (Recovery::Retry, KernelError::Router(own(GrpcCode::Aborted))),
    ]);
}

/// The router's own errors say how to recover: on malformed input, bugs.
#[test]
fn the_routers_codes_say_how_to_recover() {
    assert_recovers([
        // The router's own errors on malformed input are bugs, INTERNAL,
        // never the sender's to fix (the owner's ruling, 2026-10-29).
        (
            Recovery::Bug,
            KernelError::Router(router_own(AngzarrReason::NoRefusalHandler.as_str_name())),
        ),
        (
            Recovery::Bug,
            KernelError::Router(router_own(AngzarrReason::UnknownCommandType.as_str_name())),
        ),
        (
            Recovery::Bug,
            KernelError::Router(router_own(AngzarrReason::AnyDecodeFailed.as_str_name())),
        ),
        // A framework key under a business status is no refusal (decision
        // 14.95) but recovers as its status says.
        (
            Recovery::Business,
            KernelError::Router(crate::spec_coded(AngzarrReason::EditionNotForked, [])),
        ),
    ]);
}

/// A failure says how to recover as its error does: a refusal is the
/// caller's business, a chain's by its step's error.
#[test]
fn a_failure_says_how_to_recover_as_its_error() {
    assert_eq!(
        Failure::Rejected(Malformed::NoCover).recovery(),
        Recovery::Business
    );
    let downstream = |error| Failure::<Malformed>::Downstream {
        path: Vec::new(),
        error,
        dead_letters: Vec::new(),
    };
    assert_eq!(
        downstream(StepError::Rejected(Report::new("x-no"))).recovery(),
        Recovery::Business
    );
    assert_eq!(
        downstream(StepError::Kernel(KernelError::Malformed(
            Malformed::NoCover
        )))
        .recovery(),
        Recovery::Bug,
        "a malformed policy book is the kernel's, not the caller's"
    );
    assert_eq!(
        downstream(StepError::Kernel(unavailable())).recovery(),
        Recovery::Retry
    );
}

/// An error's `Display` is its code, then its args, ` name=value` each,
/// a wrapped cause flattened as on the wire (`name=code`, then
/// `name.arg=value`): never English (decision 14.37, the owner's
/// clarification of 2026-10-29). English comes only from a catalog keyed
/// by the code, where the error is shown.
#[test]
fn errors_display_their_code_and_args_and_wrappers_their_cause_flattened() {
    use angzarr_diagnostic::Diagnostic;
    let stale = KernelError::BeforeSnapshot {
        stream: crate::v1::Cover {
            edition: Some("what-if".to_string()),
            ..cover("account", 1)
        },
        first: 4,
    };
    let at = "account/0a000000-0000-0000-0000-000000000001@what-if";
    let text = stale.to_string();
    assert_eq!(
        text,
        format!(
            concat!(code!("before-snapshot"), " stream={at} first=4"),
            at = at
        )
    );
    let failure = Failure::<crate::toys::Refused>::Kernel(stale.clone());
    assert_eq!(failure.to_string(), text);
    assert_eq!(failure.code(), code!("before-snapshot"));
    assert_eq!(failure.args().len(), 2);
    // A condition the spec keys shows its key and the key's args alone
    // (decision 14.60).
    let keyed = KernelError::ChainTooDeep { limit: 2 };
    assert_eq!(keyed.to_string(), "ANGZARR_REASON_CHAIN_TOO_DEEP limit=2");
    let rejected = Failure::Rejected(crate::toys::Refused::Overdrawn { short: 3 });
    assert_eq!(rejected.code(), "toy-account-overdrawn");
    assert_eq!(rejected.to_string(), "toy-account-overdrawn short=3");
    let step = StepError::Rejected(Report::of(&crate::toys::Refused::NotOpen));
    assert_eq!(step.to_string(), "toy-account-not-open");
    let step = StepError::Kernel(stale.clone());
    assert_eq!(step.to_string(), text);
    let downstream = Failure::<crate::toys::Refused>::Downstream {
        path: vec![],
        error: step,
        dead_letters: Vec::new(),
    };
    assert_eq!(
        downstream.to_string(),
        format!(
            concat!(
                code!("downstream"),
                " depth=0 error=",
                code!("before-snapshot"),
                " \
             error.stream={at} error.first=4"
            ),
            at = at
        )
    );
    let upcast = crate::UpcastError::Payload(crate::PayloadError::UnknownType {
        type_url: "/a.B".to_string(),
    });
    assert_eq!(
        upcast.to_string(),
        concat!(code!("payload-unknown-type"), " type_url=/a.B")
    );
    let malformed = KernelError::Malformed(Malformed::NoCover);
    assert_eq!(malformed.to_string(), code!("malformed-no-cover"));
    let replay = KernelError::Replay(ReplayError::RecordedBackwards { sequence: 3 });
    assert_eq!(
        replay.to_string(),
        concat!(code!("replay-recorded-backwards"), " sequence=3")
    );
    // A store error is angzarr-ports' code, forwarded.
    let unavailable = StoreError::Unavailable(Cause::new("x-down", [])).code();
    let store = KernelError::Store(StoreError::Unavailable(Cause::new("x-down", [])));
    assert_eq!(store.code(), unavailable);
    assert_eq!(store.to_string(), format!("{unavailable} cause=x-down"));
    // A value that would not parse back is quoted, a cause's too.
    let locked = KernelError::Store(StoreError::Unavailable(Cause::new(
        "x-down",
        [(
            "ext_err_txt",
            angzarr_ports::Arg::Str("database is locked".to_string()),
        )],
    )));
    assert_eq!(
        locked.to_string(),
        format!("{unavailable} cause=x-down cause.ext_err_txt=\"database is locked\"")
    );
    let router = KernelError::Router(CodedError::bug(
        "PLAYER_BROKE",
        [("seat".to_string(), "2".to_string())],
    ));
    assert_eq!(
        router.to_string(),
        "ANGZARR_REASON_UNHANDLED_HANDLER_ERROR cause=PLAYER_BROKE cause.seat=2",
        "the kernel's code, never the router's English or its SCREAMING reason"
    );
}

/// A handler that could not be reached carries why as a key, `cause` (the
/// host's: `not-running`, `transport`, `timeout`), and says so as its code
/// and args, never in a sentence.
#[test]
fn an_unreachable_handler_carries_its_cause_as_a_key() {
    use angzarr_diagnostic::{Arg, Diagnostic};
    let unreachable = KernelError::HandlerUnavailable {
        stream: cover("account", 1),
        cause: "not-running".to_string(),
    };
    assert_eq!(unreachable.code(), "ANGZARR_REASON_HANDLER_UNAVAILABLE");
    assert!(unreachable
        .args()
        .iter()
        .any(|arg| matches!(arg, ("cause", Arg::Str(key)) if key == "not-running")));
    assert_eq!(
        unreachable.to_string(),
        "ANGZARR_REASON_HANDLER_UNAVAILABLE stream=account/0a000000-0000-0000-0000-000000000001 domain=account cause=not-running"
    );
}

/// The store's errors are angzarr-ports': the kernel forwards each one's
/// code and args unchanged (the codes ruled `angzarr-ports-*`), an arg a
/// `Uint` or `Str` as itself, and a wrapped `Cause` as a report nested
/// under the adapter's own code, with its args.
#[test]
fn a_store_error_keeps_its_angzarr_ports_code_and_args() {
    use angzarr_diagnostic::{Arg, Diagnostic};
    let stream = cover("account", 1);
    let inner = StoreError::Conflict {
        stream: Box::new(stream.clone()),
        expected: 1,
        actual: 2,
    };
    let conflict = KernelError::Store(inner.clone());
    assert_eq!(conflict.code(), inner.code());
    let args = conflict.args();
    let names: Vec<&str> = args.iter().map(|(name, _)| *name).collect();
    assert_eq!(names, ["stream", "expected", "actual"]);
    assert!(matches!(&args[0].1, Arg::Str(s) if *s == format!("account/{}", root(1))));
    assert!(matches!(args[1].1, Arg::Uint(1)));
    assert!(matches!(args[2].1, Arg::Uint(2)));

    let inner = StoreError::EditionNotForked {
        stream: Box::new(crate::toys::cover_in("account", 1, "what-if")),
    };
    let not_forked = KernelError::Store(inner.clone());
    assert_eq!(not_forked.code(), inner.code());
    assert_eq!(not_forked.args().len(), 1);

    // The adapter's cause carries its own args (the driver's numeric code,
    // never its text: decision 14.37), nested under the cause's code.
    let sqlite = angzarr_store_sqlite::SqlError::Unavailable {
        errno: Some(10),
        detail: String::new(),
    }
    .code();
    let inner = StoreError::Unavailable(Cause::new(
        sqlite,
        [("errno", angzarr_ports::Arg::Uint(10))],
    ));
    let down = KernelError::Store(inner.clone());
    let args = down.args();
    assert_eq!(args.len(), 1);
    let ("cause", Arg::Nested(cause)) = &args[0] else {
        panic!("a nested cause: {args:?}");
    };
    assert_eq!(cause.code(), sqlite);
    assert!(
        matches!(cause.args()[..], [("errno", Arg::Uint(10))]),
        "{:?}",
        cause.args()
    );
    assert_eq!(
        down.to_string(),
        format!("{} cause={sqlite} cause.errno=10", inner.code())
    );
}

/// A stored event the kernel cannot replay is the kernel's own error, not
/// the store's: angzarr-ports' `Unreadable` is bytes a store cannot give
/// back, never an upcasting or decoding failure.
#[test]
fn an_event_the_kernel_cannot_replay_is_its_own_error() {
    use angzarr_diagnostic::Diagnostic;
    let unreadable = KernelError::Unreadable {
        stream: cover("account", 1),
        sequence: 4,
        reason: Report::new("x-bad"),
    };
    assert_eq!(unreadable.code(), code!("unreadable"));
    assert_eq!(
        unreadable
            .args()
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>(),
        ["stream", "sequence", "reason"]
    );
    assert!(unreadable
        .to_string()
        .starts_with(concat!(code!("unreadable"), " stream=account/")));
}

#[test]
fn a_domain_the_registry_lacks_cannot_be_replayed_or_snapshotted() {
    let empty = crate::Registry::new();
    let unknown = Some(KernelError::UnregisteredDomain {
        domain: "account".to_string(),
    });
    assert_eq!(
        Stream::<Account>::replay(cover("account", 1), &log(), &empty).err(),
        unknown
    );
    let book = crate::v1::EventBook {
        cover: Some(cover("account", 1)),
        pages: log(),
        ..crate::v1::EventBook::default()
    };
    assert_eq!(Stream::<Account>::decode_book(&book, &empty).err(), unknown);
    let stream = replay(&log()).unwrap();
    assert_eq!(stream.snapshot(&empty), None);
    assert_eq!(
        KernelError::UnregisteredDomain {
            domain: "account".to_string()
        }
        .recovery(),
        Recovery::Bug,
        "the application's misconfiguration, not the caller's"
    );
    assert_eq!(
        Failure::<Malformed>::Kernel(KernelError::Malformed(Malformed::UnknownDomain {
            domain: "account".to_string()
        }))
        .recovery(),
        Recovery::Invalid,
        "an incoming book for it is the caller's"
    );
    let _ = (Ledger, Shelf);
}

#[test]
fn a_facts_events_read_back_with_its_origin() {
    let registry = registry();
    let store = MemoryStore::new();
    crate::toys::handle::<Account>(
        &store,
        &registry,
        crate::toys::fact(
            cover("account", 1),
            crate::toys::AccountFact::PaymentReceived(3).to_any(),
            "pi_r",
        ),
        &at(5),
    )
    .unwrap();
    let pages = pages(&store, &cover("account", 1));
    let stream = Stream::<Account>::replay(cover("account", 1), &pages, &registry).unwrap();
    assert_eq!(stream.events()[0].fact().unwrap().external_id, "pi_r");
    assert!(!stream.is_empty());
    assert!(Stream::<Account>::new(root(9), None).unwrap().is_empty());
}

/// A stream resumed from a snapshot hands the router the snapshot at the
/// sequence just before its first held event, so its state is the
/// snapshot's with every held event folded on: a withdrawal of all of it
/// is decided against the deposit after the snapshot.
#[test]
fn a_resumed_stream_decides_against_its_snapshot_and_the_events_after_it() {
    let registry = registry();
    let log = crate::MemoryDeadLetters::new();
    let snapshot = Snapshot {
        sequence: 1,
        state: Some(pack(&account::AccountState {
            open: true,
            cents: 100,
            ..Default::default()
        })),
        rules_version: "v2".to_string(),
    };
    let mut stream = Stream::<Account>::resume(
        cover("account", 1),
        &snapshot,
        None,
        &[page(2, pack(&account::Deposited { cents: 50 }))],
        &registry,
    )
    .unwrap();
    let withdraw = command(cover("account", 1), AccountCmd::Withdraw(149).to_any()).id(9);
    let executed = stream.execute(withdraw, &registry, &at(9), &log).ready();
    assert!(executed.is_ok(), "{:?}", executed.err());
    assert_eq!(stream.state().cents, 0, "150, less 149 and the fee");
}

/// A failure under a code of the handler's own (`PLAYER_BROKE`, one the
/// router does not know), with status `grpc`.
fn own(grpc: GrpcCode) -> CodedError {
    CodedError {
        code: "PLAYER_BROKE".to_string(),
        extras: Default::default(),
        grpc,
        refusal: None,
    }
}

/// One of the router's own errors on malformed input, as it answers them:
/// INTERNAL.
fn router_own(code: &str) -> CodedError {
    CodedError {
        code: code.to_string(),
        extras: Default::default(),
        grpc: GrpcCode::Internal,
        refusal: None,
    }
}
