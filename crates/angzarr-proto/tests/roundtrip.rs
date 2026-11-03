//! Core's messages as the merged spec shapes them (decisions 14.78 to
//! 14.109): each round-trips, and the field numbers the rewrite assigned
//! (contiguous from 1, in declaration order, oneof members included;
//! decision 14.100) are the ones on the wire.

use std::collections::HashSet;
use std::hash::Hash;

use angzarr_proto::ids::{fact_command_id, policy_command_id};
use angzarr_proto::v1::{
    angzarr_dead_letter, command_page, component_descriptor, event_meta, event_page, meta_event,
    reaction_delivery, AggregateDescriptor, AngzarrDeadLetter, Command, CommandBook, CommandCause,
    CommandOptions, CommandPage, ComponentDescriptor, ComponentKind, Cover, DeliveryOutcome,
    Diagnostic, EditionCreated, EventMeta, EventPage, EventRef, ExpectedSequenceMismatch, Fact,
    FactCause, FactOptions, FactOrigin, Handling, MetaEvent, PolicyDescriptor,
    ProcessManagerDescriptor, ProjectorDescriptor, ReactionDelivery, Snapshot, StateDescriptor,
    Uuid,
};
use prost::{Message, Name};

fn uuid(byte: u8) -> Uuid {
    Uuid {
        value: vec![byte; 16].into(),
    }
}

fn any(type_url: &str, value: &[u8]) -> prost_types::Any {
    prost_types::Any {
        type_url: type_url.into(),
        value: value.to_vec(),
    }
}

/// An event in a non-main edition, so the nested Cover's edition is on the wire.
fn trigger() -> EventRef {
    EventRef {
        stream: Some(Cover {
            domain: "lists".into(),
            root: Some(uuid(0xab)),
            edition: Some("what-if".into()),
        }),
        sequence: u64::MAX,
    }
}

fn fact_origin() -> FactOrigin {
    FactOrigin {
        external_id: "pi_1234".into(),
        description: "Stripe webhook".into(),
    }
}

fn round_trip<M: Message + Default>(m: &M) -> M {
    M::decode(m.encode_to_vec().as_slice()).expect("decode")
}

/// The tags of a message's top-level fields, in wire order: each value
/// here is length-delimited and under 128 bytes, so a field is its tag,
/// one length byte and the value.
fn tags<M: Message>(m: &M) -> Vec<u8> {
    let bytes = m.encode_to_vec();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        out.push(bytes[i]);
        i += 2 + usize::from(bytes[i + 1]);
    }
    out
}

/// Length-delimited tag of field `n`.
const fn ld(n: u8) -> u8 {
    (n << 3) | 2
}

fn handling() -> Handling {
    Handling {
        expected_sequence: Some(u64::from(u32::MAX) + 1),
    }
}

fn client_command() -> CommandPage {
    CommandPage {
        body: Some(command_page::Body::Command(Command {
            payload: Some(any("/draws.v1.Draw", &[9])),
            handling: Some(handling()),
        })),
        command_id: Some(uuid(0x5e)),
        correlation_id: Some(uuid(0xc0)),
    }
}

fn client_fact() -> CommandPage {
    CommandPage {
        body: Some(command_page::Body::Fact(Fact {
            payload: Some(any("/payments.v1.PaymentReceived", &[7])),
            origin: Some(fact_origin()),
        })),
        command_id: Some(fact_command_id("pi_1234")),
        correlation_id: Some(uuid(0xc0)),
    }
}

fn book(page: CommandPage) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: "draws".into(),
            root: Some(uuid(0xcd)),
            edition: None, // main timeline (unset)
        }),
        page: Some(page),
    }
}

// Decision 14.79: a CommandPage is identity plus a body; the Command arm
// carries its Handling, the Fact arm its origin.
#[test]
fn a_command_page_carries_handling_in_the_command_arm_and_origin_in_the_fact_arm() {
    for page in [client_command(), client_fact()] {
        let original = book(page);
        assert_eq!(round_trip(&original), original);
    }
    match round_trip(&client_command()).body {
        Some(command_page::Body::Command(c)) => assert_eq!(c.handling, Some(handling())),
        other => panic!("expected the command arm, got {other:?}"),
    }
    match round_trip(&client_fact()).body {
        Some(command_page::Body::Fact(f)) => assert_eq!(f.origin, Some(fact_origin())),
        other => panic!("expected the fact arm, got {other:?}"),
    }
    // The fact's command id is derived from its external id.
    assert_eq!(
        client_fact().command_id.unwrap().to_string(),
        "917f26ae-ea2e-58af-86de-77013a69617e"
    );
}

#[test]
fn command_page_fields_are_numbered_from_one_in_declaration_order() {
    // body: command 1, fact 2; command_id 3, correlation_id 4.
    assert_eq!(tags(&client_command()), [ld(1), ld(3), ld(4)]);
    assert_eq!(tags(&client_fact()), [ld(2), ld(3), ld(4)]);
    // Command: payload 1, handling 2; Fact: payload 1, origin 2.
    let command = Command {
        payload: Some(any("/a", &[])),
        handling: Some(Handling::default()),
    };
    assert_eq!(tags(&command), [ld(1), ld(2)]);
    let fact = Fact {
        payload: Some(any("/a", &[])),
        origin: Some(FactOrigin::default()),
    };
    assert_eq!(tags(&fact), [ld(1), ld(2)]);
    // CommandBook: cover 1, page 2, and nothing else.
    assert_eq!(tags(&book(CommandPage::default())), [ld(1), ld(2)]);
}

#[test]
fn an_absent_expected_sequence_is_distinct_from_zero() {
    assert_eq!(round_trip(&Handling::default()).expected_sequence, None);
    let zero = Handling {
        expected_sequence: Some(0),
    };
    assert_eq!(round_trip(&zero).expected_sequence, Some(0));
    assert_eq!(zero.encode_to_vec(), [1 << 3, 0], "field 1, present at 0");
}

// Decision 14.102: an event's causation mirrors its input, the command arm
// or the fact arm with its origin.
#[test]
fn event_meta_causation_is_a_oneof_of_command_and_fact() {
    let command = EventMeta {
        causation: Some(event_meta::Causation::Command(CommandCause {
            command_id: Some(uuid(1)),
        })),
        correlation_id: Some(uuid(2)),
        recorded_at: Some(prost_types::Timestamp {
            seconds: 1_700_000_001,
            nanos: 0,
        }),
        rules_version: "1.4.2".into(),
    };
    let fact = EventMeta {
        causation: Some(event_meta::Causation::Fact(FactCause {
            command_id: Some(fact_command_id("pi_1234")),
            origin: Some(fact_origin()),
        })),
        ..command.clone()
    };
    for meta in [&command, &fact] {
        assert_eq!(&round_trip(meta), meta);
    }
    // command 1, fact 2, correlation_id 3, recorded_at 4, rules_version 5.
    assert_eq!(tags(&command), [ld(1), ld(3), ld(4), ld(5)]);
    assert_eq!(tags(&fact), [ld(2), ld(3), ld(4), ld(5)]);
}

// Decision 14.106: a page's body is a domain's event or a framework
// meta-event, by type, never by type URL.
#[test]
fn an_event_page_body_is_an_event_or_a_meta_event() {
    let event = EventPage {
        body: Some(event_page::Body::Event(any("/orders.v1.OrderPlaced", &[1]))),
        meta: Some(EventMeta::default()),
        // Above u32::MAX: the widened sequence survives the wire.
        sequence: u64::from(u32::MAX) + 7,
    };
    let fork = EventPage {
        body: Some(event_page::Body::MetaEvent(MetaEvent {
            kind: Some(meta_event::Kind::Created(EditionCreated {})),
        })),
        ..event.clone()
    };
    for page in [&event, &fork] {
        assert_eq!(&round_trip(page), page);
    }
    // event 1, meta 2, sequence 3 (a varint), meta_event 4.
    let bare = |body| EventPage {
        body: Some(body),
        meta: None,
        sequence: 0,
    };
    assert_eq!(
        tags(&bare(event_page::Body::Event(any("/a", &[])))),
        [ld(1)]
    );
    assert_eq!(
        tags(&bare(event_page::Body::MetaEvent(MetaEvent::default()))),
        [ld(4)]
    );
    assert_eq!(
        EventPage {
            meta: Some(EventMeta::default()),
            ..EventPage::default()
        }
        .encode_to_vec(),
        [ld(2), 0]
    );
    // EditionCreated is empty (decision 14.107): the page's sequence is the
    // fork point and its cover names the edition.
    assert!(EditionCreated {}.encode_to_vec().is_empty());
}

#[test]
fn a_snapshot_records_its_state_and_rules_version_in_fields_one_to_three() {
    let original = Snapshot {
        sequence: u64::from(u32::MAX) + 9,
        state: Some(any("/draws.v1.DrawState", &[1, 1])),
        rules_version: "1.4.2".into(),
    };
    assert_eq!(round_trip(&original), original);
    assert_eq!(
        Snapshot {
            rules_version: "r".into(),
            ..Snapshot::default()
        }
        .encode_to_vec(),
        [ld(3), 1, b'r']
    );
    assert_eq!(
        Snapshot {
            state: Some(prost_types::Any::default()),
            ..Snapshot::default()
        }
        .encode_to_vec(),
        [ld(2), 0]
    );
}

// Decision 14.79: a policy's output or refusal fact is a ReactionDelivery,
// its body a oneof of the two arms (decision 14.91), never core's Command
// or Fact.
#[test]
fn a_reaction_delivery_carries_its_arm_identity_and_trigger() {
    let output = ReactionDelivery {
        cover: Some(Cover {
            domain: "draws".into(),
            root: Some(uuid(0xcd)),
            edition: Some("what-if".into()),
        }),
        body: Some(reaction_delivery::Body::Command(any(
            "/draws.v1.Draw",
            &[9],
        ))),
        command_id: Some(policy_command_id(&trigger(), "list-to-draw", 2).unwrap()),
        correlation_id: Some(uuid(0xc0)),
        causation: Some(trigger()),
        component: "list-to-draw".into(),
    };
    let fact = ReactionDelivery {
        body: Some(reaction_delivery::Body::Fact(any("/draws.v1.Drawn", &[]))),
        ..output.clone()
    };
    for delivery in [&output, &fact] {
        assert_eq!(&round_trip(delivery), delivery);
    }
    // cover 1, command 2 or fact 3, command_id 4, correlation_id 5,
    // causation 6, component 7.
    assert_eq!(tags(&output), [ld(1), ld(2), ld(4), ld(5), ld(6), ld(7)]);
    assert_eq!(tags(&fact), [ld(1), ld(3), ld(4), ld(5), ld(6), ld(7)]);
}

#[test]
fn the_staleness_refusal_carries_the_expected_sequence_and_the_head() {
    // Decision 14.94: core's one refusal, raised by the coordinator.
    let refusal = ExpectedSequenceMismatch {
        expected: 3,
        head: 5,
    };
    assert_eq!(round_trip(&refusal), refusal);
    assert_eq!(refusal.encode_to_vec(), [1 << 3, 3, 2 << 3, 5]);
    assert_eq!(
        ExpectedSequenceMismatch::full_name(),
        "io.angzarr.v1.ExpectedSequenceMismatch"
    );
}

#[test]
fn a_delivery_outcome_is_applied_or_duplicate() {
    // Decision 14.100: PARKED is gone; a fact that could not be recorded is
    // dead-lettered, never an outcome.
    for (value, number, name) in [
        (
            DeliveryOutcome::Unspecified,
            0,
            "DELIVERY_OUTCOME_UNSPECIFIED",
        ),
        (DeliveryOutcome::Applied, 1, "DELIVERY_OUTCOME_APPLIED"),
        (DeliveryOutcome::Duplicate, 2, "DELIVERY_OUTCOME_DUPLICATE"),
    ] {
        assert_eq!(value as i32, number);
        assert_eq!(value.as_str_name(), name);
    }
    assert_eq!(DeliveryOutcome::try_from(3).ok(), None);
}

// Decisions 14.78 and 14.101: a descriptor's kind is a oneof, each arm
// carrying what that kind has; type identities are names.
#[test]
fn a_component_descriptor_names_its_kind_by_arm() {
    let state = StateDescriptor {
        type_name: "hatpick.v1.ListState".into(),
    };
    let kinds = [
        component_descriptor::Kind::Aggregate(AggregateDescriptor {
            domain: "lists".into(),
            state: Some(state.clone()),
        }),
        component_descriptor::Kind::ProcessManager(ProcessManagerDescriptor {
            domain: "buy-in".into(),
            state: Some(state),
            starting_types: vec!["io.angzarr.examples.v1.StartBuyIn".into()],
        }),
        component_descriptor::Kind::Policy(PolicyDescriptor {
            input_domain: "order".into(),
            output_domain: "inventory".into(),
        }),
        component_descriptor::Kind::Projector(ProjectorDescriptor {
            input_domains: vec!["wallet".into()],
        }),
    ];
    for (kind, tag) in kinds.into_iter().zip([ld(3), ld(4), ld(5), ld(6)]) {
        let original = ComponentDescriptor {
            name: "c".into(),
            rules_version: "1.4.2+build.7".into(),
            kind: Some(kind),
        };
        assert_eq!(round_trip(&original), original);
        // name 1, rules_version 2, then the kind's arm.
        assert_eq!(tags(&original), [ld(1), ld(2), tag]);
    }
    assert_eq!(
        StateDescriptor {
            type_name: "a".into()
        }
        .encode_to_vec(),
        [ld(1), 1, b'a']
    );
    // ComponentKind stays the bare tag dead letters store.
    for (kind, number) in [
        (ComponentKind::Unspecified, 0),
        (ComponentKind::Aggregate, 1),
        (ComponentKind::Policy, 2),
        (ComponentKind::ProcessManager, 3),
        (ComponentKind::Projector, 4),
    ] {
        assert_eq!(kind as i32, number);
    }
}

// Decisions 14.101 and 14.107: a dead letter's why is exactly one of a
// typed refusal, a reason or a transport's own why.
#[test]
fn a_dead_letter_has_one_payload_and_one_why() {
    let reason = Diagnostic {
        code: "ANGZARR_REASON_HANDLER_UNAVAILABLE".into(),
        args: [("domain".to_string(), "draws".to_string())].into(),
    };
    let whys = [
        angzarr_dead_letter::Why::Refusal(any(
            "/io.angzarr.v1.ExpectedSequenceMismatch",
            &[8, 3, 16, 5],
        )),
        angzarr_dead_letter::Why::Reason(reason),
        angzarr_dead_letter::Why::Transport(any(
            "/io.angzarr.transport.distributed.v1.PayloadRetrievalFailed",
            &[],
        )),
    ];
    for (why, tag) in whys.into_iter().zip([ld(5), ld(6), ld(7)]) {
        let original = AngzarrDeadLetter {
            payload: Some(angzarr_dead_letter::Payload::RejectedCommand(book(
                client_command(),
            ))),
            why: Some(why),
            retry_count: 0,
            occurred_at: None,
            source_component: "list-to-draw".into(),
            source_component_kind: ComponentKind::Policy as i32,
        };
        assert_eq!(round_trip(&original), original);
        assert_eq!(original.encode_to_vec()[0], ld(1));
        assert!(original.encode_to_vec().contains(&tag));
    }
    // The payloads: rejected_command 1, rejected_reaction 2, rejected_events
    // 3, failed_edition_removal 4 (the stream's Cover, decision 14.107).
    let removal = AngzarrDeadLetter {
        payload: Some(angzarr_dead_letter::Payload::FailedEditionRemoval(
            Cover::default(),
        )),
        ..AngzarrDeadLetter::default()
    };
    assert_eq!(removal.encode_to_vec(), [ld(4), 0]);
    let reaction = AngzarrDeadLetter {
        payload: Some(angzarr_dead_letter::Payload::RejectedReaction(
            ReactionDelivery::default(),
        )),
        ..AngzarrDeadLetter::default()
    };
    assert_eq!(reaction.encode_to_vec(), [ld(2), 0]);
    // retry_count 8 (a varint), source_component 10, its kind 11.
    let counted = AngzarrDeadLetter {
        retry_count: 2,
        ..AngzarrDeadLetter::default()
    };
    assert_eq!(counted.encode_to_vec(), [8 << 3, 2]);
    let named = AngzarrDeadLetter {
        source_component: "c".into(),
        source_component_kind: ComponentKind::Projector as i32,
        ..AngzarrDeadLetter::default()
    };
    assert_eq!(named.encode_to_vec(), [ld(10), 1, b'c', 11 << 3, 4]);
}

/// Main is unset, not a magic name: an unset edition is absent from the wire
/// and decodes as None (main); a set empty string is a different value on the
/// wire (explicit presence), so a server can refuse it as malformed rather
/// than read it as main; a named edition round-trips.
#[test]
fn edition_unset_is_main_and_a_set_empty_string_is_distinguishable() {
    let cover = |edition: Option<&str>| Cover {
        domain: "draws".into(),
        root: Some(uuid(0xcd)),
        edition: edition.map(Into::into),
    };
    let main = cover(None);
    assert_eq!(round_trip(&main).edition, None);
    assert_eq!(Cover::default().edition, None, "the default cover is main");
    let empty = cover(Some(""));
    // Field 4, wire type 2, length 0: present on the wire though empty.
    assert!(empty.encode_to_vec().ends_with(&[ld(4), 0x00]));
    assert_ne!(empty.encode_to_vec(), main.encode_to_vec());
    assert_eq!(round_trip(&empty).edition.as_deref(), Some(""));
    let named = cover(Some("what-if"));
    assert_eq!(round_trip(&named), named);
}

#[test]
fn key_types_are_eq_and_hash_and_bytes_are_shared() {
    // Compile-time: each key type is Eq + Hash (prost derives both for
    // float-free messages). A float added to one fails this build.
    fn key<T: Eq + Hash + Clone>(value: T) -> usize {
        HashSet::from([value.clone(), value]).len()
    }
    assert_eq!(key(uuid(1)), 1);
    assert_eq!(key(trigger()), 1);
    assert_eq!(key(trigger().stream.unwrap()), 1);
    assert_eq!(key(fact_origin()), 1);
    assert_eq!(key(StateDescriptor::default()), 1);

    // `bytes` fields are bytes::Bytes: a clone shares the buffer.
    let id = uuid(7);
    let copy = id.clone();
    let _: &bytes::Bytes = &id.value;
    assert_eq!(id.value.as_ptr(), copy.value.as_ptr());
}

#[test]
fn type_names_are_io_angzarr_v1() {
    assert_eq!(EventPage::full_name(), "io.angzarr.v1.EventPage");
    assert_eq!(EventMeta::full_name(), "io.angzarr.v1.EventMeta");
    assert_eq!(MetaEvent::full_name(), "io.angzarr.v1.MetaEvent");
    assert_eq!(EditionCreated::full_name(), "io.angzarr.v1.EditionCreated");
    assert_eq!(CommandPage::full_name(), "io.angzarr.v1.CommandPage");
    assert_eq!(
        ReactionDelivery::full_name(),
        "io.angzarr.v1.ReactionDelivery"
    );
    // angzarr's type URL prefix is "/" (identity-and-streams.md §1), which
    // is prost's default when no type-name domain is configured.
    assert_eq!(EventPage::type_url(), "/io.angzarr.v1.EventPage");
    let b = book(client_fact());
    let packed = prost_types::Any::from_msg(&b).expect("pack");
    assert_eq!(packed.type_url, "/io.angzarr.v1.CommandBook");
    assert_eq!(packed.to_msg::<CommandBook>().expect("unpack"), b);
}

// Decision 14.91: a fact is declared by its own option, never by a flag on
// a command's.
#[test]
fn a_command_and_a_fact_are_declared_by_their_own_options() {
    let command = CommandOptions {
        component: "io.angzarr.examples.v1.TableState".into(),
        emits: vec!["io.angzarr.examples.v1.BetPlaced".into()],
        refuses: vec!["io.angzarr.examples.v1.AlreadyBet".into()],
    };
    assert_eq!(round_trip(&command), command);
    assert_eq!(tags(&command)[..], [ld(1), ld(2), ld(3)]);
    let fact = FactOptions {
        component: "io.angzarr.examples.v1.BuyInState".into(),
        emits: vec!["io.angzarr.examples.v1.FundsHeld".into()],
    };
    assert_eq!(round_trip(&fact), fact);
    assert_eq!(tags(&fact)[..], [ld(1), ld(2)]);
    // The two share component and emits by number, so converting a message
    // between (command) and (fact) keeps them (decision 14.98).
    let converted = FactOptions::decode(&*command.encode_to_vec()).unwrap();
    assert_eq!(converted.component, command.component);
    assert_eq!(converted.emits, command.emits);
}
