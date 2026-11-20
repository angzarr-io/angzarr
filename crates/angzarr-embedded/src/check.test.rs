//! Steps 1 and 2: a client's book checked once and identified; a reaction
//! the kernel's own chain stamped, checked and identified from its trigger.

use angzarr_proto::v1::event_meta::Causation;

use super::*;
use crate::pack;
use crate::toys::{
    account, at, command, command_id, correlation, cover, fact, instant, minted, registry, Edit,
};
use crate::v1::{CommandCause, EventMeta, FactCause};

/// What the kernel stamps `id`'s events with when its handler is handed it
/// at `recorded_at`, under `rules_version`: the router's
/// `CommandContext::event_meta` of the `HandleRequest` holding its book.
fn stamped(id: &Identified, recorded_at: Timestamp, rules_version: &str) -> EventMeta {
    let request = angzarr_router::hpb::HandleRequest {
        input: Some(id.to_book()),
        recorded_at: Some(recorded_at),
        causation: id.causation().cloned(),
        ..Default::default()
    };
    angzarr_router::domain::command_context(&request).event_meta(rules_version)
}

fn deposit(cents: i64) -> Any {
    pack(&account::cmd::Deposit { cents })
}

fn payment(cents: i64) -> Any {
    pack(&account::fact::PaymentReceived { cents })
}

fn ok() -> CommandBook {
    command(cover("account", 1), deposit(1))
}

fn ok_fact() -> CommandBook {
    fact(cover("account", 1), payment(1), "pi_1")
}

fn short(n: usize) -> Uuid {
    Uuid {
        value: vec![1; n].into(),
    }
}

fn edit(mut book: CommandBook, f: &dyn Fn(&mut CommandBook)) -> CommandBook {
    f(&mut book);
    book
}

/// Each case's book is refused, naming its first malformation.
fn assert_refused(cases: Vec<(&str, CommandBook, Malformed)>) {
    let registry = registry();
    for (case, book, malformed) in cases {
        assert_eq!(check(book, registry.catalog()), Err(malformed), "{case}");
    }
}

fn identified(book: CommandBook, seconds: i64) -> Identified {
    check(book, registry().catalog())
        .unwrap()
        .identify(&at(seconds))
}

/// The cover: present, its domain known, its root 16 bytes, its edition
/// unset or named.
#[test]
fn a_malformed_cover_is_refused() {
    assert_refused(vec![
        (
            "no cover",
            edit(ok(), &|b| b.cover = None),
            Malformed::NoCover,
        ),
        (
            "a domain nobody registered",
            command(cover("nowhere", 1), deposit(1)),
            Malformed::UnknownDomain {
                domain: "nowhere".to_string(),
            },
        ),
        (
            "no root",
            edit(ok(), &|b| b.cover.as_mut().unwrap().root = None),
            Malformed::NoRoot,
        ),
        (
            "a root of 15 bytes",
            edit(ok(), &|b| b.cover.as_mut().unwrap().root = Some(short(15))),
            Malformed::BadRoot { len: 15 },
        ),
        (
            "a set empty edition: main is unset, never \"\"",
            command(
                Cover {
                    edition: Some(String::new()),
                    ..cover("account", 1)
                },
                deposit(1),
            ),
            Malformed::EmptyEdition,
        ),
        (
            "no root and an empty edition: the first is the answer",
            command(
                Cover {
                    root: None,
                    edition: Some(String::new()),
                    ..cover("account", 1)
                },
                deposit(1),
            ),
            Malformed::NoRoot,
        ),
    ]);
}

/// The page: present, with a body and a payload, its ids 16 bytes.
#[test]
fn a_malformed_page_is_refused() {
    let page = |body| {
        Some(CommandPage {
            body,
            ..CommandPage::default()
        })
    };
    assert_refused(vec![
        ("no page", edit(ok(), &|b| b.page = None), Malformed::NoPage),
        (
            "no body",
            edit(ok(), &|b| b.page = page(None)),
            Malformed::NoBody,
        ),
        (
            "a command with no payload",
            edit(ok(), &|b| {
                b.page = page(Some(command_page::Body::Command(Default::default())))
            }),
            Malformed::NoPayload,
        ),
        (
            "a fact with no payload",
            edit(ok_fact(), &|b| {
                if let Some(command_page::Body::Fact(fact)) = b.page_mut().body.as_mut() {
                    fact.payload = None;
                }
            }),
            Malformed::NoPayload,
        ),
        (
            "a command id of 3 bytes",
            edit(ok(), &|b| b.page_mut().command_id = Some(short(3))),
            Malformed::BadCommandId { len: 3 },
        ),
        (
            "an empty correlation id",
            edit(ok(), &|b| b.page_mut().correlation_id = Some(short(0))),
            Malformed::BadCorrelationId { len: 0 },
        ),
    ]);
}

/// A fact has its origin, a named external id, and no command id but its
/// derived one.
#[test]
fn a_malformed_fact_is_refused() {
    assert_refused(vec![
        (
            "a fact without its origin",
            edit(ok_fact(), &|b| {
                if let Some(command_page::Body::Fact(fact)) = b.page_mut().body.as_mut() {
                    fact.origin = None;
                }
            }),
            Malformed::FactWithoutOrigin,
        ),
        (
            "a fact with an empty external id",
            fact(cover("account", 1), payment(1), ""),
            Malformed::EmptyExternalId,
        ),
        (
            "a fact naming a command id not its derived one",
            ok_fact().id(9),
            Malformed::FactCommandIdNotDerived,
        ),
    ]);
    let derived = edit(ok_fact(), &|b| {
        b.page_mut().command_id = Some(ids::fact_command_id("pi_1"))
    });
    assert!(check(derived, registry().catalog()).is_ok());
}

#[test]
fn a_checked_command_has_non_optional_accessors() {
    let checked = check(ok().expecting(4), registry().catalog()).unwrap();
    assert_eq!(checked.cover(), &cover("account", 1));
    assert_eq!(checked.domain(), "account");
    assert_eq!(Some(checked.root()), cover("account", 1).root.as_ref());
    assert_eq!(checked.edition(), None);
    assert_eq!(checked.payload(), &deposit(1));
    assert_eq!(checked.body().arm(), Arm::Command);
    assert_eq!(checked.body().source(), None);
    assert_eq!(checked.expected_sequence(), Some(4), "as sent");
    assert_eq!(checked.command_id(), None, "assigned at identify");
    assert_eq!(checked.correlation_id(), None);
    assert!(!checked.is_reaction());
    let unexpecting = check(ok(), registry().catalog()).unwrap();
    assert_eq!(unexpecting.expected_sequence(), None);
}

#[test]
fn a_checked_fact_carries_its_source_in_its_body_and_no_expected_sequence() {
    let checked = check(ok_fact().expecting(4), registry().catalog()).unwrap();
    assert_eq!(checked.body().arm(), Arm::Fact);
    assert_eq!(
        checked.body().source().map(|s| s.external_id.as_str()),
        Some("pi_1")
    );
    assert_eq!(checked.expected_sequence(), None, "a fact has none");
}

#[test]
fn a_client_command_without_ids_gets_the_minted_one_as_both() {
    let id = identified(command(cover("account", 1), deposit(5)), 30);
    assert_eq!(
        id.command_id(),
        &minted(30),
        "the application's UUIDv7, from Sampled"
    );
    assert_eq!(
        id.correlation_id(),
        &minted(30),
        "an absent correlation is the command id"
    );
    assert_eq!(id.causation(), None);
    assert_eq!(id.component(), None);
    assert!(!id.is_reaction());
    assert_eq!(id.to_delivery(), None, "a client's input is no reaction");
}

#[test]
fn a_client_command_keeps_the_ids_it_sent() {
    let id = identified(
        command(cover("account", 1), deposit(5)).id(7).correlated(8),
        30,
    );
    assert_eq!(id.command_id(), &command_id(7), "reused verbatim on retry");
    assert_eq!(id.correlation_id(), &correlation(8));

    let id = identified(command(cover("account", 1), deposit(5)).id(7), 31);
    assert_eq!(
        id.correlation_id(),
        &command_id(7),
        "every retry of a client-keyed command lands in one correlation"
    );
}

#[test]
fn a_fact_is_identified_by_its_external_id() {
    let id = identified(fact(cover("account", 1), payment(70), "pi_1234"), 30);
    let derived = "917f26ae-ea2e-58af-86de-77013a69617e"
        .parse()
        .expect("types.proto's worked example");
    assert_eq!(id.command_id(), &derived, "UUIDv5 of the external id");
    assert_eq!(id.command_id(), &ids::fact_command_id("pi_1234"));
    assert_eq!(
        id.correlation_id(),
        &ids::fact_correlation("pi_1234"),
        "and, unsupplied, its correlation"
    );
    assert_ne!(id.command_id(), &minted(30), "nothing minted for a fact");

    let id = identified(
        fact(cover("account", 1), payment(70), "pi_1234").correlated(3),
        30,
    );
    assert_eq!(id.command_id(), &derived);
    assert_eq!(id.correlation_id(), &correlation(3), "a sent one is kept");
}

#[test]
fn every_event_of_an_input_carries_it_as_causation_by_its_arm() {
    let id = identified(command(cover("account", 1), deposit(5)).id(7), 30);
    assert_eq!(
        stamped(&id, instant(31), "v2"),
        EventMeta {
            causation: Some(Causation::Command(CommandCause {
                command_id: Some(command_id(7)),
            })),
            correlation_id: Some(command_id(7)),
            recorded_at: Some(instant(31)),
            rules_version: "v2".to_string(),
        }
    );

    let id = identified(fact(cover("account", 1), payment(70), "pi_9"), 30);
    assert_eq!(
        stamped(&id, instant(31), "v2").causation,
        Some(Causation::Fact(FactCause {
            command_id: Some(ids::fact_command_id("pi_9")),
            origin: Some(FactOrigin {
                external_id: "pi_9".to_string(),
                description: "toy payments webhook".to_string(),
            }),
        })),
        "a fact's events carry its id and its origin"
    );
}

#[test]
fn an_identified_book_checks_again_and_identifies_the_same() {
    let registry = registry();
    for book in [
        command(cover("account", 1), deposit(5)),
        command(cover("account", 1), deposit(5)).id(2).correlated(3),
        command(cover("account", 1), deposit(5)).expecting(4),
        fact(cover("account", 1), payment(1), "pi_2"),
    ] {
        let first = check(book, registry.catalog()).unwrap().identify(&at(40));
        let again = check(first.to_book(), registry.catalog())
            .expect("its ids are well formed, a fact's the derived one")
            .identify(&at(41));
        // Its correlation, filled in when unset, is set in the book: so
        // the re-read's is the sender's, which only a remote coordinator's
        // book tells apart (`to_remote_book`).
        assert_eq!(
            again.to_book(),
            first.to_book(),
            "a DLQ entry or an audit line re-reads as sent"
        );
        assert_eq!(again.expected_sequence(), first.expected_sequence());
    }
}

#[test]
fn a_remote_book_leaves_an_unsent_correlation_unset() {
    let id = identified(command(cover("account", 1), deposit(5)).id(2), 40);
    let page = id.to_remote_book().page.unwrap();
    assert_eq!(page.command_id, Some(command_id(2)), "fixed here");
    assert_eq!(page.correlation_id, None, "the remote's default");
    let sent = identified(
        command(cover("account", 1), deposit(5)).id(2).correlated(3),
        40,
    );
    assert_eq!(
        sent.to_remote_book().page.unwrap().correlation_id,
        Some(correlation(3))
    );
}

#[test]
fn with_payload_keeps_the_arm_identity_and_handling() {
    let id = identified(
        command(cover("account", 1), deposit(5)).id(2).expecting(6),
        40,
    );
    let upcast = id.with_payload(deposit(9));
    assert_eq!(upcast.payload(), &deposit(9));
    assert_eq!(upcast.command_id(), id.command_id());
    assert_eq!(upcast.expected_sequence(), Some(6));
    let fact = identified(fact(cover("account", 1), payment(1), "pi_3"), 40);
    let upcast = fact.with_payload(payment(2));
    assert_eq!(upcast.arm(), Arm::Fact);
    assert_eq!(upcast.body().source(), fact.body().source());
}

#[test]
fn recorded_at_is_raised_to_the_streams_last_never_lowered() {
    use crate::{not_before, Timestamp};
    let later = Timestamp {
        seconds: 40,
        nanos: 5,
    };
    assert_eq!(not_before(None, instant(30)), instant(30));
    assert_eq!(
        not_before(Some(&later), instant(40)),
        later,
        "a clock step backwards never makes a later event look older"
    );
    assert_eq!(not_before(Some(&instant(20)), instant(30)), instant(30));
}

// ---------------------------------------------------------------- reactions

/// The trigger every reaction here answers: account 1's event 4.
fn trigger() -> EventRef {
    EventRef {
        stream: Some(cover("account", 1)),
        sequence: 4,
    }
}

/// A reaction of the bookkeeping policy to [`trigger`], to `cover`,
/// carrying `body`, stamped as the chain stamps it.
fn reaction(cover: Cover, body: reaction_delivery::Body) -> ReactionDelivery {
    ReactionDelivery {
        cover: Some(cover),
        body: Some(body),
        command_id: ids::policy_command_id(&trigger(), "bookkeeping", 0).ok(),
        correlation_id: Some(correlation(8)),
        causation: Some(trigger()),
        component: "bookkeeping".to_string(),
    }
}

fn record() -> Any {
    pack(&crate::toys::ledger::Record {
        source_sequence: 4,
        cents: 1,
    })
}

fn ledger() -> Cover {
    Cover {
        domain: "ledger".to_string(),
        root: Some(crate::toys::ledger_root()),
        edition: None,
    }
}

#[test]
fn a_reaction_is_identified_by_its_stamp() {
    let delivery = reaction(ledger(), reaction_delivery::Body::Command(record()));
    let checked = check_reaction(delivery.clone(), registry().catalog()).unwrap();
    assert!(checked.is_reaction());
    assert_eq!(checked.expected_sequence(), None, "a reaction names none");
    let id = checked.identify(&at(50));
    assert_eq!(Some(id.command_id()), delivery.command_id.as_ref());
    assert_eq!(id.correlation_id(), &correlation(8));
    assert_eq!(id.causation(), Some(&trigger()));
    assert_eq!(id.component(), Some("bookkeeping"));
    assert_eq!(id.arm(), Arm::Command);
    assert_eq!(id.to_delivery(), Some(delivery), "delivered as stamped");
    assert_eq!(
        id.to_remote_book().page.unwrap().correlation_id,
        Some(correlation(8)),
        "a reaction's correlation is its trigger's, always sent"
    );
}

#[test]
fn a_reactions_fact_is_given_an_origin_derived_from_its_stamp() {
    let delivery = reaction(ledger(), reaction_delivery::Body::Fact(record()));
    let id = check_reaction(delivery.clone(), registry().catalog())
        .unwrap()
        .identify(&at(50));
    assert_eq!(id.arm(), Arm::Fact);
    let command_id = delivery.command_id.clone().unwrap();
    assert_eq!(
        id.body().source(),
        Some(&FactOrigin {
            external_id: command_id.to_string(),
            description: "toy.ledger.cmd.v1.Record from bookkeeping".to_string(),
        })
    );
    assert_eq!(id.command_id(), &command_id, "its stamp, not a derived one");
    assert_eq!(
        stamped(&id, instant(51), "v1").causation,
        Some(Causation::Fact(FactCause {
            command_id: Some(command_id),
            origin: id.body().source().cloned(),
        }))
    );
}

#[test]
fn a_malformed_reaction_is_refused() {
    let registry = registry();
    let refused = |delivery: ReactionDelivery| check_reaction(delivery, registry.catalog()).err();
    let good = || reaction(ledger(), reaction_delivery::Body::Command(record()));
    assert_eq!(
        refused(ReactionDelivery {
            cover: None,
            ..good()
        }),
        Some(Malformed::NoCover)
    );
    assert_eq!(
        refused(ReactionDelivery {
            cover: Some(Cover {
                root: None,
                ..ledger()
            }),
            ..good()
        }),
        Some(Malformed::NoRoot)
    );
    assert_eq!(
        refused(ReactionDelivery {
            cover: Some(cover("nowhere", 1)),
            ..good()
        }),
        Some(Malformed::UnknownDomain {
            domain: "nowhere".to_string()
        })
    );
    assert_eq!(
        refused(ReactionDelivery {
            causation: None,
            ..good()
        }),
        Some(Malformed::BadTrigger(ids::UuidError::MissingTrigger))
    );
    assert_eq!(
        refused(ReactionDelivery {
            command_id: None,
            ..good()
        }),
        Some(Malformed::BadCommandId { len: 0 })
    );
    assert_eq!(
        refused(ReactionDelivery {
            command_id: Some(short(4)),
            ..good()
        }),
        Some(Malformed::BadCommandId { len: 4 })
    );
    assert_eq!(
        refused(ReactionDelivery {
            correlation_id: None,
            ..good()
        }),
        Some(Malformed::BadCorrelationId { len: 0 })
    );
    assert_eq!(
        refused(ReactionDelivery {
            body: None,
            ..good()
        }),
        Some(Malformed::NoBody)
    );
}

#[test]
fn a_malformations_code_is_its_own_and_names_its_args() {
    use angzarr_diagnostic::Diagnostic;
    assert_eq!(
        Malformed::BadRoot { len: 3 }.to_string(),
        concat!(code!("malformed-bad-root"), " len=3")
    );
    assert_eq!(
        Malformed::BadTrigger(ids::UuidError::MissingTrigger)
            .args()
            .len(),
        1
    );
}
