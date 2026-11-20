//! Upcasting by type URL, for events and (discouraged) commands, in
//! registration order, with strict decoding.
use crate::angzarr_router::hpb::{
    self, upcast_request, upcasted, UpcastRequest, UpcastResponse, Upcasted,
};
use crate::toys::prelude::*;
use crate::v1::{event_page, EventMeta, EventPage};
use crate::{
    pack, ChainProblem, EventStore, Failure, KernelError, Registry, Stream, UpcastChainError,
    Upcaster,
};

use crate::toys::{
    account, account_upcaster, at, command, command_id, cover, handle, instant, opened, payloads,
    ready, registry, registry_with_upcasters, Account, AccountEvent, Edit,
};

fn page(sequence: u64, payload: crate::Any) -> EventPage {
    EventPage {
        body: Some(event_page::Body::Event(payload)),
        meta: Some(EventMeta {
            causation: Some(crate::v1::event_meta::Causation::Command(
                crate::v1::CommandCause {
                    command_id: Some(command_id(sequence as u128)),
                },
            )),
            correlation_id: Some(command_id(sequence as u128)),
            recorded_at: Some(instant(sequence as i64)),
            rules_version: "v2".to_string(),
        }),
        sequence,
    }
}

#[test]
fn upcasting_by_type_url_runs_the_chain_in_registration_order() {
    let upcaster = account_upcaster();
    assert_eq!(
        upcaster.source_types(),
        ["toy.account.cmd.v0.Credit", "toy.account.v0.Credited"]
    );
    let current = pack(&account::Deposited { cents: 200 });
    assert_eq!(
        upcaster.upcast_event(&pack(&account::v0::Credited { euros: 2 })),
        Ok(current.clone())
    );
    assert_eq!(upcaster.upcast_event(&current), Ok(current.clone()));
    let unknown = pack(&account::v9::Bonus { cents: 1 });
    assert_eq!(
        upcaster.upcast_event(&unknown),
        Ok(unknown.clone()),
        "other types pass through; strictness is at decode"
    );

    let old = page(
        u64::from(u32::MAX) + 1,
        pack(&account::v0::Credited { euros: 2 }),
    );
    let upcast = upcaster.upcast_page(&old).unwrap();
    assert_eq!(upcast.sequence, old.sequence, "sequence kept");
    assert_eq!(upcast.meta, old.meta, "meta kept");
    assert_eq!(upcast.body, Some(event_page::Body::Event(current)));

    let elsewhere = UpcastRequest {
        domain: "ledger".to_string(),
        input: Some(pages_of(vec![old.clone()])),
    };
    assert_eq!(
        upcaster.dispatch(&elsewhere).unwrap().pages,
        [keyed(
            old.sequence,
            pack(&account::v0::Credited { euros: 2 })
        )],
        "another domain's pages answered unchanged, keyed by sequence"
    );
}

/// A request's pages.
fn pages_of(events: Vec<EventPage>) -> upcast_request::Input {
    upcast_request::Input::Pages(hpb::UpcastPages { events })
}

/// One answer keyed by `sequence`.
fn keyed(sequence: u64, payload: crate::Any) -> Upcasted {
    Upcasted {
        key: Some(upcasted::Key::Sequence(sequence)),
        payload: Some(payload),
    }
}

/// A port answering `answer` whatever it is asked.
struct Answering(Vec<Upcasted>);

impl crate::UpcastPort for Answering {
    fn upcast<'a>(
        &'a self,
        _: &'a UpcastRequest,
    ) -> crate::angzarr_router::send::BoxFuture<'a, Result<UpcastResponse, crate::UpcastError>>
    {
        Box::pin(std::future::ready(Ok(UpcastResponse {
            pages: self.0.clone(),
        })))
    }
}

#[test]
fn pages_are_upcast_by_their_answers_keys_never_their_position() {
    let pages = [
        page(3, pack(&account::v0::Credited { euros: 1 })),
        page(4, pack(&account::v0::Credited { euros: 2 })),
    ];
    // Answered out of order: each page takes the payload its sequence is
    // answered with.
    let port = Answering(vec![
        keyed(4, pack(&account::Deposited { cents: 200 })),
        keyed(3, pack(&account::Deposited { cents: 100 })),
    ]);
    let upcast = ready(crate::upcast::upcast_pages(&port, "account", &pages)).unwrap();
    assert_eq!(
        upcast,
        [
            page(3, pack(&account::Deposited { cents: 100 })),
            page(4, pack(&account::Deposited { cents: 200 })),
        ]
    );
}

#[test]
fn an_answer_whose_keys_are_not_the_pages_sequences_is_the_rules_bug() {
    let pages = [
        page(3, pack(&account::v0::Credited { euros: 1 })),
        page(4, pack(&account::v0::Credited { euros: 2 })),
    ];
    let deposited = || pack(&account::Deposited { cents: 1 });
    for answer in [
        vec![keyed(3, deposited())],
        vec![keyed(3, deposited()), keyed(3, deposited())],
        vec![keyed(3, deposited()), keyed(5, deposited())],
        vec![
            keyed(3, deposited()),
            Upcasted {
                key: Some(upcasted::Key::NoSequence(hpb::NoSequence {})),
                payload: Some(deposited()),
            },
        ],
        vec![
            keyed(3, deposited()),
            keyed(4, deposited()),
            keyed(5, deposited()),
        ],
    ] {
        assert_eq!(
            ready(crate::upcast::upcast_pages(
                &Answering(answer.clone()),
                "account",
                &pages
            )),
            Err(crate::UpcastError::AnswerKeys),
            "{answer:?}"
        );
    }
}

#[test]
fn a_lone_payload_is_answered_by_one_payload_keyed_no_sequence() {
    let old = pack(&account::cmd::v0::Credit { euros: 2 });
    let lone = Upcasted {
        key: Some(upcasted::Key::NoSequence(hpb::NoSequence {})),
        payload: Some(pack(&account::cmd::Deposit { cents: 200 })),
    };
    assert_eq!(
        account_upcaster()
            .dispatch(&UpcastRequest {
                domain: "account".to_string(),
                input: Some(upcast_request::Input::LonePayload(old.clone())),
            })
            .unwrap()
            .pages,
        std::slice::from_ref(&lone)
    );
    assert_eq!(
        ready(crate::upcast::upcast_input(
            &Answering(vec![lone.clone()]),
            "account",
            &old
        )),
        Ok(pack(&account::cmd::Deposit { cents: 200 }))
    );
    for answer in [
        vec![],
        vec![keyed(0, old.clone())],
        vec![lone.clone(), lone],
    ] {
        assert_eq!(
            ready(crate::upcast::upcast_input(
                &Answering(answer.clone()),
                "account",
                &old
            )),
            Err(crate::UpcastError::AnswerKeys),
            "{answer:?}"
        );
    }
}

#[test]
fn a_meta_event_page_is_not_sent_and_kept() {
    let fork = EventPage {
        sequence: 0,
        body: Some(event_page::Body::MetaEvent(crate::v1::MetaEvent {
            kind: Some(crate::v1::meta_event::Kind::Created(
                crate::v1::EditionCreated {},
            )),
        })),
        meta: None,
    };
    let pages = [
        fork.clone(),
        page(1, pack(&account::v0::Credited { euros: 1 })),
    ];
    let port = Answering(vec![keyed(1, pack(&account::Deposited { cents: 100 }))]);
    assert_eq!(
        ready(crate::upcast::upcast_pages(&port, "account", &pages)).unwrap(),
        [
            fork.clone(),
            page(1, pack(&account::Deposited { cents: 100 }))
        ]
    );
    // Nothing to send: no request, the pages as read.
    assert_eq!(
        ready(crate::upcast::upcast_pages(
            &Answering(vec![]),
            "account",
            std::slice::from_ref(&fork)
        ))
        .unwrap(),
        [fork]
    );
}

#[test]
fn old_events_replay_through_the_upcaster() {
    let registry = registry_with_upcasters();
    let log = [
        page(0, pack(&account::Opened {})),
        page(1, pack(&account::v0::Credited { euros: 3 })),
    ];
    let stream = Stream::<Account>::replay(cover("account", 1), &log, &registry).unwrap();
    assert_eq!(stream.state().cents, 300);
    assert_eq!(stream.events()[1].event(), &AccountEvent::Deposited(300));

    let store = MemoryStore::new();
    store
        .write(Write::append(sref(&cover("account", 1)), 0, &log))
        .ready()
        .unwrap();
    assert_eq!(
        pages(&store, &cover("account", 1)),
        log,
        "the store keeps what was written"
    );
}

#[test]
fn an_unknown_event_type_fails_replay_naming_it() {
    let log = [page(0, pack(&account::v9::Bonus { cents: 1 }))];
    let error = Stream::<Account>::replay(cover("account", 1), &log, &registry())
        .err()
        .unwrap();
    let KernelError::Unreadable {
        stream,
        sequence,
        reason,
    } = error
    else {
        panic!("never skipped: {error:?}");
    };
    assert_eq!((stream, sequence), (cover("account", 1), 0));
    assert!(format!("{reason:?}").contains("toy.account.v9.Bonus"));
}

#[test]
fn an_old_command_is_upcast_then_decided() {
    let registry = registry_with_upcasters();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 0);

    let old = command(
        cover("account", 1),
        pack(&account::cmd::v0::Credit { euros: 2 }),
    )
    .id(5);
    let handled = handle::<Account>(&store, &registry, old, &at(20)).unwrap();
    assert_eq!(
        payloads::<AccountEvent>(&handled.books()),
        [AccountEvent::Deposited(200)]
    );

    let without = crate::toys::registry();
    let old = command(
        cover("account", 1),
        pack(&account::cmd::v0::Credit { euros: 2 }),
    )
    .id(6);
    let refused = handle::<Account>(&store, &without, old, &at(21))
        .err()
        .unwrap();
    assert!(
        matches!(
            &refused,
            Failure::Kernel(KernelError::Router(e))
                if e.code == "ANGZARR_REASON_UNKNOWN_COMMAND_TYPE"
                    && e.extras.get("type_url").map(String::as_str)
                        == Some("/toy.account.cmd.v0.Credit")
        ),
        "no upcaster path: ANGZARR_REASON_UNKNOWN_COMMAND_TYPE, naming the type URL: {refused:?}"
    );
}

#[test]
fn an_upcaster_is_checked_whole() {
    let problems = |upcaster: Upcaster| -> Vec<ChainProblem> {
        let mut registry = Registry::new();
        registry
            .register::<Account>(upcaster, crate::toys::handlers::<crate::toys::Account>())
            .unwrap();
        registry
            .check()
            .err()
            .unwrap_or_default()
            .into_iter()
            .map(|UpcastChainError { problem, .. }| problem)
            .collect()
    };

    assert_eq!(problems(account_upcaster()), []);
    let dead_end =
        Upcaster::new("u", "account").rule::<account::v0::Credited, account::v9::Bonus>(|c| {
            Ok(account::v9::Bonus { cents: c.euros })
        });
    assert_eq!(
        problems(dead_end),
        [ChainProblem::DeadEnd {
            source: "toy.account.v0.Credited".to_string(),
            ends_at: "toy.account.v9.Bonus".to_string()
        }],
        "v9 is not a type the account decodes"
    );
    let twice = account_upcaster().rule::<account::v0::Credited, account::Deposited>(|c| {
        Ok(account::Deposited {
            cents: c.euros * 100,
        })
    });
    assert_eq!(
        problems(twice),
        [ChainProblem::Duplicate {
            source: "toy.account.v0.Credited".to_string()
        }]
    );
}

#[test]
fn an_upcaster_is_checked_for_order_and_untyped_rules_are_not_followed() {
    let problems = |upcaster: Upcaster| -> Vec<ChainProblem> {
        let mut registry = Registry::new();
        registry
            .register::<Account>(upcaster, crate::toys::handlers::<crate::toys::Account>())
            .unwrap();
        registry
            .check()
            .err()
            .unwrap_or_default()
            .into_iter()
            .map(|e| {
                assert_eq!(e.domain, "account");
                e.problem
            })
            .collect()
    };
    // v1 -> current registered before v0 -> v1: a v0 payload stops at v1.
    let out_of_order = Upcaster::new("u", "account")
        .rule::<account::v9::Bonus, account::Deposited>(|b| {
            Ok(account::Deposited { cents: b.cents })
        })
        .rule::<account::v0::Credited, account::v9::Bonus>(|c| {
            Ok(account::v9::Bonus { cents: c.euros })
        });
    assert_eq!(
        problems(out_of_order),
        [
            ChainProblem::OutOfOrder {
                source: "toy.account.v9.Bonus".to_string(),
                produced_by: "toy.account.v0.Credited".to_string()
            },
            ChainProblem::DeadEnd {
                source: "toy.account.v0.Credited".to_string(),
                ends_at: "toy.account.v9.Bonus".to_string()
            }
        ]
    );
    let chained = Upcaster::new("u", "account")
        .rule::<account::v0::Credited, account::v9::Bonus>(|c| {
            Ok(account::v9::Bonus { cents: c.euros })
        })
        .rule::<account::v9::Bonus, account::Deposited>(|b| {
            Ok(account::Deposited { cents: b.cents })
        });
    assert_eq!(problems(chained), [], "v0 -> v9 -> current finishes");
    let untyped = Upcaster::new("u", "account")
        .rule::<account::v0::Credited, account::v9::Bonus>(|c| {
            Ok(account::v9::Bonus { cents: c.euros })
        })
        .on_event("toy.account.v9.Bonus", |any| Ok(any.clone()));
    assert_eq!(problems(untyped), [], "an untyped rule cannot be followed");
}

#[test]
fn an_upcast_rule_reports_its_own_failure_and_bytes_it_cannot_read() {
    let refusing =
        Upcaster::new("u", "account").rule::<account::v0::Credited, account::Deposited>(|_| {
            Err(angzarr_diagnostic::Report::new("toy-refused"))
        });
    let old = pack(&account::v0::Credited { euros: 2 });
    assert_eq!(
        refusing.upcast_event(&old),
        Err(crate::UpcastError::Rule {
            source: "toy.account.v0.Credited".to_string(),
            error: angzarr_diagnostic::Report::new("toy-refused")
        })
    );
    let garbled = crate::Any {
        type_url: old.type_url.clone(),
        value: vec![0x08],
    };
    assert!(matches!(
        account_upcaster().upcast_event(&garbled),
        Err(crate::UpcastError::Payload(
            crate::PayloadError::Decode { .. }
        ))
    ));

    let registry = {
        let mut registry = Registry::new();
        registry
            .register::<Account>(refusing, crate::toys::handlers::<crate::toys::Account>())
            .unwrap();
        registry
    };
    let store = MemoryStore::new();
    let failure = handle::<Account>(
        &store,
        &registry,
        command(cover("account", 1), old.clone()),
        &at(1),
    )
    .err()
    .unwrap();
    assert!(matches!(
        failure,
        Failure::Kernel(KernelError::Upcast { ref stream, .. }) if *stream == cover("account", 1)
    ));
    assert_eq!(failure.recovery(), crate::Recovery::Bug);

    let mut request = UpcastRequest {
        domain: "account".to_string(),
        input: Some(pages_of(vec![page(0, old.clone()), EventPage::default()])),
    };
    assert_eq!(
        account_upcaster().dispatch(&request).unwrap().pages,
        [keyed(0, pack(&account::Deposited { cents: 200 }))],
        "its own domain's pages upcast; a page with no event not answered"
    );
    request.input = Some(pages_of(vec![page(0, old.clone()), page(1, old)]));
    assert!(registry
        .upcaster("account")
        .unwrap()
        .dispatch(&request)
        .is_err());
}

#[test]
fn an_upcaster_names_itself_and_each_problem_once() {
    assert_eq!(account_upcaster().name(), "account-upcaster");
    let problems = |upcaster: Upcaster| -> Vec<ChainProblem> {
        let mut registry = Registry::new();
        registry
            .register::<Account>(upcaster, crate::toys::handlers::<crate::toys::Account>())
            .unwrap();
        registry
            .check()
            .err()
            .unwrap_or_default()
            .into_iter()
            .map(|e| e.problem)
            .collect()
    };
    let same = Upcaster::new("u", "account").rule::<account::Deposited, account::Deposited>(Ok);
    assert_eq!(
        problems(same),
        [],
        "a rule onto its own type is not out of order"
    );
    let rule = |c: account::v0::Credited| Ok(account::v9::Bonus { cents: c.euros });
    let twice = Upcaster::new("u", "account")
        .rule::<account::v0::Credited, account::v9::Bonus>(rule)
        .rule::<account::v0::Credited, account::v9::Bonus>(rule);
    assert_eq!(
        problems(twice),
        [
            ChainProblem::Duplicate {
                source: "toy.account.v0.Credited".to_string()
            },
            ChainProblem::DeadEnd {
                source: "toy.account.v0.Credited".to_string(),
                ends_at: "toy.account.v9.Bonus".to_string()
            }
        ],
        "one chain, one dead end"
    );
}
