//! angzarr-embedded's dead-letter queue is a logger whose every line warns that it
//! almost certainly indicates a problem.
use super::{Cause, Component};
use crate::toys::prelude::*;
use crate::v1::angzarr_dead_letter::Why;
use crate::v1::{angzarr_dead_letter, AngzarrDeadLetter, ComponentKind, Diagnostic};
use crate::{
    check, dead_letter_line, pack, DeadLetterLog, Failure, Kernel, KernelError, MemoryDeadLetters,
    DEAD_LETTER_PREFIX,
};

use crate::toys::{account, at, command, cover, fact, reason, registry, Account, Edit};
use angzarr_diagnostic::code;

/// The stream of account 1, as a reason names it.
const ACCOUNT_1: &str = "account/0a000000-0000-0000-0000-000000000001";

#[test]
fn a_dead_letter_line_starts_with_the_warning_and_names_the_command() {
    let book = check(
        command(
            cover("account", 1),
            pack(&account::cmd::Deposit { cents: 1 }),
        )
        .id(5),
        registry().catalog(),
    )
    .unwrap()
    .identify(&at(30));
    let letter = AngzarrDeadLetter {
        payload: Some(angzarr_dead_letter::Payload::RejectedCommand(
            book.to_book(),
        )),
        why: Some(reason(
            "ANGZARR_REASON_UNHANDLED_HANDLER_ERROR",
            &[("cause", "ANGZARR_REASON_HANDLER_PANICKED")],
        )),
        source_component: "account".to_string(),
        source_component_kind: ComponentKind::Aggregate as i32,
        ..AngzarrDeadLetter::default()
    };

    let line = dead_letter_line(&letter);
    assert!(
        line.starts_with(DEAD_LETTER_PREFIX),
        "an operator alerts on the prefix: {line}"
    );
    // A key to alert on, never English (decision 14.37).
    assert_eq!(DEAD_LETTER_PREFIX, "DEAD_LETTER");
    // The why is the letter's reason: its code, then its args (decision
    // 14.37).
    assert!(
        line.contains(
            "account: ANGZARR_REASON_UNHANDLED_HANDLER_ERROR cause=ANGZARR_REASON_HANDLER_PANICKED"
        ),
        "{line}"
    );
    assert!(
        line.contains(&crate::toys::command_id(5).to_string()),
        "the command id, as display text: {line}"
    );

    let log = MemoryDeadLetters::new();
    log.write(&line);
    assert_eq!(log.lines(), std::slice::from_ref(&line));
    assert!(log.letters().is_empty(), "a bare line is no letter");

    let log = MemoryDeadLetters::new();
    log.letter(&letter);
    assert_eq!(
        log.letters(),
        std::slice::from_ref(&letter),
        "the letter, whole"
    );
    assert_eq!(log.lines(), std::slice::from_ref(&line), "and its line");

    // A log that keeps lines only writes a letter as its line.
    #[derive(Default)]
    struct Lines(std::sync::Mutex<Vec<String>>);
    impl DeadLetterLog for Lines {
        fn write(&self, line: &str) {
            self.0.lock().unwrap().push(line.to_string());
        }
    }
    let lines = Lines::default();
    lines.letter(&letter);
    assert_eq!(*lines.0.lock().unwrap(), [line]);
}

/// A letter's why, each arm (decision 14.101): a reason by its code and
/// args; a typed refusal by the key it implies, ANGZARR_REASON_COMMAND_REFUSED,
/// and its type; a transport failure by its type.
#[test]
fn a_line_shows_each_arm_of_its_why() {
    let of = |why| {
        dead_letter_line(&AngzarrDeadLetter {
            why: Some(why),
            source_component: "account".to_string(),
            ..AngzarrDeadLetter::default()
        })
    };
    assert_eq!(
        of(reason("toy-down", &[("seat", "3")])),
        "DEAD_LETTER account: toy-down seat=3"
    );
    let refusal = pack(&crate::v1::ExpectedSequenceMismatch {
        expected: 1,
        head: 3,
    });
    assert_eq!(
        of(Why::Refusal(refusal)),
        "DEAD_LETTER account: ANGZARR_REASON_COMMAND_REFUSED \
         refusal_type=io.angzarr.v1.ExpectedSequenceMismatch"
    );
    let transport = crate::Any {
        type_url: "type.googleapis.com/toy.transport.v1.ClaimLost".to_string(),
        value: Vec::new(),
    };
    assert_eq!(
        of(Why::Transport(transport)),
        "DEAD_LETTER account: toy.transport.v1.ClaimLost"
    );
}

/// An edition removal's dead letter names its stream.
#[test]
fn an_edition_removals_line_names_its_stream() {
    let letter = AngzarrDeadLetter {
        payload: Some(angzarr_dead_letter::Payload::FailedEditionRemoval(
            crate::toys::cover_in("account", 1, "what-if"),
        )),
        source_component: "account".to_string(),
        ..AngzarrDeadLetter::default()
    };
    let line = dead_letter_line(&letter);
    assert!(
        line.ends_with(&format!(" stream={ACCOUNT_1}@what-if")),
        "{line}"
    );
}

/// A reaction's letter names the reaction's command id and stream.
#[test]
fn a_rejected_reactions_line_names_its_command_and_stream() {
    let letter = AngzarrDeadLetter {
        payload: Some(angzarr_dead_letter::Payload::RejectedReaction(
            crate::v1::ReactionDelivery {
                cover: Some(cover("account", 1)),
                command_id: Some(crate::toys::command_id(7)),
                ..crate::v1::ReactionDelivery::default()
            },
        )),
        why: Some(reason("toy-down", &[])),
        source_component: "bookkeeping".to_string(),
        ..AngzarrDeadLetter::default()
    };
    let line = dead_letter_line(&letter);
    assert!(
        line.ends_with(&format!(
            " command_id={} stream={ACCOUNT_1}",
            crate::toys::command_id(7)
        )),
        "{line}"
    );
}

/// A fact whose handler panics is dead-lettered (C-0599; decision
/// 14.109): the router catches the panic, a handler's bug (cause
/// ANGZARR_REASON_HANDLER_PANICKED; decision 14.37), the kernel
/// dead-letters the fact, its sender is answered FACT_DEAD_LETTERED, and
/// nothing is written.
#[test]
fn a_panicking_ingest_is_dead_lettered_and_answered() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();

    let bad = fact(
        cover("account", 1),
        pack(&account::fact::PaymentReceived { cents: -1 }),
        "pi_bad",
    );
    let failure = kernel
        .handle::<Account, _>(&store, bad, &at(30))
        .ready()
        .err()
        .expect("its sender is answered");
    assert!(
        matches!(
            failure,
            Failure::Kernel(KernelError::FactDeadLettered { .. })
        ),
        "{failure:?}"
    );
    assert!(streams(&store).is_empty(), "nothing written");
    let lines = log.lines();
    assert_eq!(lines.len(), 1);
    assert!(lines[0].starts_with(DEAD_LETTER_PREFIX));
    assert!(
        lines[0].contains("cause=ANGZARR_REASON_HANDLER_PANICKED"),
        "{}",
        lines[0]
    );
    assert!(lines[0].contains(&crate::ids::fact_command_id("pi_bad").to_string()));
}

/// A dead letter carries its error's code and args, never rendered
/// English: the operator's text is produced where the line is displayed.
#[test]
fn a_panicking_ingests_dead_letter_carries_its_code_not_english() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    let bad = fact(
        cover("account", 1),
        pack(&account::fact::PaymentReceived { cents: -1 }),
        "pi_bad",
    );
    let _ = kernel.handle::<Account, _>(&store, bad, &at(30)).ready();
    let lines = log.lines();
    assert_eq!(lines.len(), 1);
    assert!(
        lines[0].contains(
            "account: ANGZARR_REASON_UNHANDLED_HANDLER_ERROR cause=ANGZARR_REASON_HANDLER_PANICKED"
        ),
        "the spec's key, never the kernel's code: {}",
        lines[0]
    );
    let why = reason(
        "ANGZARR_REASON_UNHANDLED_HANDLER_ERROR",
        &[("cause", "ANGZARR_REASON_HANDLER_PANICKED")],
    );
    crate::toys::assert_letter(
        &crate::toys::one_letter(log.letters()),
        crate::v1::ComponentKind::Aggregate,
        &why,
        0,
    );
    assert!(!lines[0].contains("panicked:"), "{}", lines[0]);
    assert!(
        !lines[0].contains("negative"),
        "the panic's text stays out: {}",
        lines[0]
    );
}

/// A dead letter's value that would not parse back is quoted (decision
/// 14.37's display rule), each of its reason's args: one empty, or
/// holding whitespace, `=`, `"` or `\`, double-quoted with its
/// `"` and `\` escaped; any other value bare.
#[test]
fn a_dead_letters_ambiguous_value_is_quoted() {
    let pairs = |pairs: &[(&str, &str)]| {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    };
    let letter = AngzarrDeadLetter {
        why: Some(Why::Reason(Diagnostic {
            code: "toy-store-down".to_string(),
            args: pairs(&[
                ("ext_err_txt", "database is locked"),
                ("empty", ""),
                ("path", "C:\\dir"),
                ("said", "say \"hi\""),
                ("seat", "3"),
                ("note", "k=v"),
            ]),
        })),
        source_component: "account".to_string(),
        ..AngzarrDeadLetter::default()
    };
    assert_eq!(
        dead_letter_line(&letter),
        "DEAD_LETTER account: toy-store-down empty=\"\" ext_err_txt=\"database is locked\" \
         note=\"k=v\" path=\"C:\\\\dir\" said=\"say \\\"hi\\\"\" seat=3"
    );
}

/// A step whose retryable failure outlasted its retries is dead-lettered
/// with its command, its failure's own key and args as its why, with how
/// many retries it had; its line shows the count.
#[test]
fn a_retried_steps_dead_letter_holds_its_command_failure_and_retry_count() {
    use crate::angzarr_router::error::{CodedError, RetryableStatus};
    let book = deposit_book();
    let error = KernelError::Router(CodedError::retryable_bug(
        RetryableStatus::Unavailable,
        "toy-down",
        [("seat".to_string(), "3".to_string())],
    ));
    let letter = super::at_cap(
        angzarr_dead_letter::Payload::RejectedCommand(book.clone()),
        ACCOUNT,
        Cause::Kernel(&error),
        3,
    );
    assert_eq!(
        letter.payload,
        Some(angzarr_dead_letter::Payload::RejectedCommand(book))
    );
    assert_eq!(
        letter.why,
        Some(reason(
            "ANGZARR_REASON_UNHANDLED_HANDLER_ERROR",
            &[("cause", "toy-down"), ("cause.seat", "3")],
        ))
    );
    assert_eq!(letter.retry_count, 3);
    assert_eq!(letter.source_component, "account");
    let line = dead_letter_line(&letter);
    assert!(
        line.contains(
            "account: ANGZARR_REASON_UNHANDLED_HANDLER_ERROR cause=toy-down cause.seat=3 retry_count=3 command_id="
        ),
        "{line}"
    );
}

/// A command given up at the race re-run cap counts its retries, the runs
/// after the first (decision 14.48).
#[test]
fn a_letter_at_the_race_cap_counts_the_runs_after_the_first() {
    let error = KernelError::RetryCapExhausted {
        stream: cover("account", 1),
        attempts: 3,
        cap: 3,
    };
    let letter = letter_for(Cause::Kernel(&error));
    assert_eq!(letter.retry_count, 2);
    assert_eq!(
        letter.why,
        Some(reason(
            "ANGZARR_REASON_RETRY_CAP_EXHAUSTED",
            &[("attempts", "3"), ("cap", "3"), ("stream", ACCOUNT_1)],
        ))
    );
}

/// The account aggregate, as a letter names it.
const ACCOUNT: Component<'static> = Component {
    name: "account",
    kind: ComponentKind::Aggregate,
};

/// A letter names the component that could not take its payload, and its
/// kind (`source_component`, `source_component_kind`), retried or not: a
/// policy, a projector, or a domain of the kind the registry holds it as.
#[test]
fn a_letter_names_its_component_and_its_kind() {
    let error = KernelError::ChainTooDeep { limit: 4 };
    let payload = || angzarr_dead_letter::Payload::RejectedCommand(deposit_book());
    for component in [
        ACCOUNT,
        Component::policy("bookkeeping"),
        Component::projector("balances"),
        Component::domain(registry().catalog(), "account"),
    ] {
        for letter in [
            super::letter(payload(), component, Cause::Kernel(&error)),
            super::at_cap(payload(), component, Cause::Kernel(&error), 2),
        ] {
            assert_eq!(letter.source_component, component.name);
            assert_eq!(letter.source_component_kind(), component.kind);
        }
    }
    assert_eq!(Component::policy("bookkeeping").kind, ComponentKind::Policy);
    assert_eq!(
        Component::projector("balances").kind,
        ComponentKind::Projector
    );
    assert_eq!(
        Component::domain(registry().catalog(), "account").kind,
        ComponentKind::Aggregate
    );
    assert_eq!(
        Component::domain(registry().catalog(), "nowhere").kind,
        ComponentKind::Unspecified,
        "not registered here: no kind known"
    );
}

/// A command (5) to account 1, its ids assigned.
fn deposit_book() -> crate::v1::CommandBook {
    check(
        command(
            cover("account", 1),
            pack(&account::cmd::Deposit { cents: 1 }),
        )
        .id(5),
        registry().catalog(),
    )
    .unwrap()
    .identify(&at(30))
    .to_book()
}

/// The letter for `cause`, about the deposit book.
fn letter_for(cause: Cause) -> AngzarrDeadLetter {
    super::letter(
        angzarr_dead_letter::Payload::RejectedCommand(deposit_book()),
        ACCOUNT,
        cause,
    )
}

/// A kernel error the spec has no key for keeps angzarr-embedded's code.
#[test]
fn a_kernel_error_with_no_spec_key_keeps_its_own_code() {
    let error = KernelError::UnregisteredDomain {
        domain: "account".to_string(),
    };
    let letter = letter_for(Cause::Kernel(&error));
    let diagnostic = crate::route::diagnostic(&error);
    assert_eq!(diagnostic.code, code!("unregistered-domain"));
    assert_eq!(letter.why, Some(Why::Reason(diagnostic)));
    assert_eq!(letter.retry_count, 0);
}

/// The kernel's spec-keyed errors are keyed as the spec spells them
/// (decisions 14.49, 14.51): the chain's limit, an unreachable handler,
/// a store that could not answer and an upcaster sidecar that could not be
/// reached, each with its key's args; never angzarr-ports' or
/// angzarr-embedded's code.
#[test]
fn a_spec_keyed_errors_letter_carries_its_key_and_args() {
    use angzarr_diagnostic::Report;
    let sqlite = angzarr_store_sqlite::SqlError::Unavailable {
        errno: Some(5),
        detail: String::new(),
    }
    .code();
    for (error, why) in [
        (
            KernelError::ChainTooDeep { limit: 4 },
            reason("ANGZARR_REASON_CHAIN_TOO_DEEP", &[("limit", "4")]),
        ),
        (
            KernelError::HandlerUnavailable {
                stream: cover("account", 1),
                cause: "not-running".to_string(),
            },
            reason(
                "ANGZARR_REASON_HANDLER_UNAVAILABLE",
                &[
                    ("cause", "not-running"),
                    ("domain", "account"),
                    ("stream", ACCOUNT_1),
                ],
            ),
        ),
        (
            KernelError::Store(crate::StoreError::Unavailable(angzarr_ports::Cause::new(
                sqlite,
                [("errno", angzarr_ports::Arg::Uint(5))],
            ))),
            reason(
                "ANGZARR_REASON_STORE_UNAVAILABLE",
                &[("cause", sqlite), ("cause.errno", "5")],
            ),
        ),
        (
            KernelError::Upcast {
                stream: cover("account", 1),
                error: crate::UpcastError::Unavailable {
                    cause: Report::new("x-sidecar-down"),
                },
            },
            reason(
                "ANGZARR_REASON_UPCASTER_UNAVAILABLE",
                &[("cause", "x-sidecar-down"), ("domain", "account")],
            ),
        ),
    ] {
        let letter = letter_for(Cause::Kernel(&error));
        assert_eq!(letter.why, Some(why), "{error:?}");
    }
}

/// A reaction the kernel built that its check refused is keyed by the
/// coordinator's key where the spec has one, naming the reaction's domain
/// and its payload's type; otherwise by angzarr-embedded's code. Wrapped
/// as a kernel error, the same.
#[test]
fn a_malformed_kernel_books_letter_is_keyed_where_the_spec_has_a_key() {
    use crate::Malformed;
    let naming = [
        ("command_type", "toy.account.cmd.v1.Deposit"),
        ("domain", "account"),
    ];
    for (malformed, why) in [
        (
            Malformed::NoRoot,
            reason("ANGZARR_REASON_POLICY_COMMAND_NO_ROOT", &naming),
        ),
        (
            Malformed::BadRoot { len: 3 },
            reason("ANGZARR_REASON_POLICY_COMMAND_NO_ROOT", &naming),
        ),
        (
            Malformed::UnknownDomain {
                domain: "account".to_string(),
            },
            reason("ANGZARR_REASON_POLICY_COMMAND_UNKNOWN_DOMAIN", &naming),
        ),
        (Malformed::NoBody, reason(code!("malformed-no-body"), &[])),
    ] {
        let letter = letter_for(Cause::Malformed(&malformed));
        assert_eq!(letter.why, Some(why.clone()), "{malformed:?}");
        let wrapped = KernelError::Malformed(malformed.clone());
        assert_eq!(
            letter_for(Cause::Kernel(&wrapped)).why,
            Some(why),
            "{malformed:?}"
        );
    }
}

/// A malformed book's letter names its payload's type, a fact's too; a
/// reaction's by its body; events name no command, and leave it empty,
/// unless the answer refused names the output.
#[test]
fn a_malformed_books_letter_names_its_payloads_type() {
    use crate::Malformed;
    let fact = check(
        crate::toys::fact(
            cover("account", 1),
            pack(&account::fact::PaymentReceived { cents: 1 }),
            "pi_1",
        ),
        registry().catalog(),
    )
    .unwrap()
    .identify(&at(30))
    .to_book();
    let command_type = |letter: AngzarrDeadLetter| match letter.why {
        Some(Why::Reason(reason)) => reason.args.get("command_type").cloned(),
        _ => None,
    };
    let letter = super::letter(
        angzarr_dead_letter::Payload::RejectedCommand(fact),
        ACCOUNT,
        Cause::Malformed(&Malformed::NoRoot),
    );
    assert_eq!(
        command_type(letter).as_deref(),
        Some("toy.account.fact.v1.PaymentReceived")
    );
    let reaction = super::letter(
        angzarr_dead_letter::Payload::RejectedReaction(crate::v1::ReactionDelivery {
            cover: Some(cover("ledger", 1)),
            body: Some(crate::v1::reaction_delivery::Body::Fact(pack(
                &account::fact::PaymentReceived { cents: 1 },
            ))),
            ..crate::v1::ReactionDelivery::default()
        }),
        Component::policy("bookkeeping"),
        Cause::Malformed(&Malformed::NoRoot),
    );
    assert_eq!(
        reaction.why,
        Some(reason(
            "ANGZARR_REASON_POLICY_COMMAND_NO_ROOT",
            &[
                ("command_type", "toy.account.fact.v1.PaymentReceived"),
                ("domain", "ledger")
            ]
        ))
    );
    let events = || {
        angzarr_dead_letter::Payload::RejectedEvents(crate::v1::EventBook {
            cover: Some(cover("account", 1)),
            ..crate::v1::EventBook::default()
        })
    };
    let letter = super::letter(events(), ACCOUNT, Cause::Malformed(&Malformed::NoRoot));
    assert_eq!(
        letter.why,
        Some(reason(
            "ANGZARR_REASON_POLICY_COMMAND_NO_ROOT",
            &[("command_type", ""), ("domain", "")]
        ))
    );
    let answered = super::letter(
        events(),
        Component::policy("bookkeeping"),
        Cause::MalformedReaction {
            malformed: &Malformed::NoRoot,
            named: ("ledger", "toy.ledger.cmd.v1.Record"),
        },
    );
    assert_eq!(
        answered.why,
        Some(reason(
            "ANGZARR_REASON_POLICY_COMMAND_NO_ROOT",
            &[
                ("command_type", "toy.ledger.cmd.v1.Record"),
                ("domain", "ledger")
            ]
        ))
    );
}

/// A line shows a letter's retry count only where it was retried.
#[test]
fn a_line_shows_no_retry_count_for_a_letter_never_retried() {
    let error = KernelError::ChainTooDeep { limit: 4 };
    let line = dead_letter_line(&letter_for(Cause::Kernel(&error)));
    assert!(!line.contains("retry_count"), "{line}");
}
