use std::collections::HashSet;
use std::hash::Hash;

use angzarr_proto::ids::{fact_command_id, policy_command_id};
use angzarr_proto::v1::{
    angzarr_dead_letter, command_page, event_meta, refusal_response, AngzarrDeadLetter, Command,
    CommandBook, CommandPage, CommandRefused, ComponentDescriptor, ComponentKind,
    ConflictResolution, ConflictUnit, ContextualCommand, CorrelationQuery, Cover, DeliveryOutcome,
    Diagnostic, EventBook, EventMeta, EventPage, EventRef, Fact, FactOrigin, Handling, Outcome,
    Projection, ProjectorFailure, RefusalResponse, Snapshot, StateDescriptor, StateOptions, Target,
    UpcastRequest, Uuid,
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
fn cause() -> EventRef {
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

/// The three kinds of command, as the page carries them once the
/// coordinator has assigned their ids.
#[derive(Clone, Copy, Debug)]
enum Kind {
    /// A client Command: a client-supplied (or minted) id, no causation.
    Client,
    /// A reaction Command: a derived id and the triggering event.
    Policy,
    /// A Fact: a derived id and its source.
    Fact,
}

const KINDS: [Kind; 3] = [Kind::Client, Kind::Policy, Kind::Fact];

fn command_id(kind: Kind) -> Uuid {
    match kind {
        Kind::Client => uuid(0x5e),
        Kind::Policy => policy_command_id(&cause(), "list-to-draw", 2).unwrap(),
        Kind::Fact => fact_command_id("pi_1234"),
    }
}

fn body(kind: Kind) -> command_page::Body {
    match kind {
        Kind::Fact => command_page::Body::Fact(Fact {
            payload: Some(any("/payments.v1.PaymentReceived", &[7])),
        }),
        _ => command_page::Body::Command(Command {
            payload: Some(any("/draws.v1.Draw", &[9])),
        }),
    }
}

fn command_page(kind: Kind) -> CommandPage {
    CommandPage {
        body: Some(body(kind)),
        command_id: Some(command_id(kind)),
        correlation_id: Some(uuid(0xc0)),
        causation_id: matches!(kind, Kind::Policy).then(cause),
        fact_origin: matches!(kind, Kind::Fact).then(fact_origin),
    }
}

fn recorded_at() -> Option<prost_types::Timestamp> {
    Some(prost_types::Timestamp {
        seconds: 1_700_000_001,
        nanos: 0,
    })
}

/// The meta the coordinator stamps from a command page: the command as the
/// event's causation, its correlation and fact source, plus recorded_at and
/// the reported rules version. The page's causation_id (a reaction's
/// trigger) is not recorded.
fn meta(kind: Kind) -> EventMeta {
    let page = command_page(kind);
    EventMeta {
        causation: page.command_id.map(event_meta::Causation::CommandId),
        fact: page.fact_origin,
        correlation_id: page.correlation_id,
        recorded_at: recorded_at(),
        rules_version: "v3".into(),
    }
}

fn page(kind: Kind) -> EventPage {
    EventPage {
        // Above u32::MAX: the widened sequence survives the wire.
        sequence: u64::from(u32::MAX) + 7,
        payload: Some(any("/orders.v1.OrderPlaced", &[1, 2, 3])),
        meta: Some(meta(kind)),
    }
}

fn book(kind: Kind, handling: Option<Handling>) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: "draws".into(),
            root: Some(uuid(0xcd)),
            edition: None, // main timeline (unset)
        }),
        page: Some(command_page(kind)),
        handling,
    }
}

fn handling() -> Option<Handling> {
    Some(Handling {
        expected_sequence: Some(u64::from(u32::MAX) + 1),
        conflict_resolution: ConflictResolution::AggregateHandles as i32,
    })
}

fn request_book(handling: Option<Handling>) -> CommandBook {
    book(Kind::Client, handling)
}

fn round_trip<M: Message + Default>(m: &M) -> M {
    M::decode(m.encode_to_vec().as_slice()).expect("decode")
}

#[test]
fn command_page_round_trips_each_kind_with_its_identity() {
    for kind in KINDS {
        let original = book(kind, handling());
        let decoded = round_trip(&original);
        assert_eq!(decoded, original);
        let page = decoded.page.expect("single page");
        assert_eq!(page.body, Some(body(kind)), "{kind:?}");
        assert_eq!(page.command_id, Some(command_id(kind)), "{kind:?}");
        assert_eq!(page.correlation_id, Some(uuid(0xc0)), "{kind:?}");
        // causation_id only on reactions, fact_origin only on facts.
        assert_eq!(page.causation_id.is_some(), matches!(kind, Kind::Policy));
        assert_eq!(page.fact_origin.is_some(), matches!(kind, Kind::Fact));
    }
}

#[test]
fn fact_origin_rides_the_page_not_the_fact_arm() {
    let page = round_trip(&book(Kind::Fact, None)).page.unwrap();
    match (page.body, page.fact_origin) {
        (Some(command_page::Body::Fact(f)), Some(o)) => {
            assert_eq!(o, fact_origin());
            assert_eq!(f.payload.unwrap().type_url, "/payments.v1.PaymentReceived");
        }
        other => panic!("expected fact arm with fact origin, got {other:?}"),
    }
    // The fact's command id is derived from its external id, and is the same
    // value as its default correlation.
    assert_eq!(
        page.command_id.unwrap().to_string(),
        "917f26ae-ea2e-58af-86de-77013a69617e"
    );
}

#[test]
fn page_identity_fields_have_their_numbers() {
    // command_id 8, correlation_id 9, causation_id 10, fact_origin 11; the
    // body arms keep 3 and 4. Each is length-delimited: tag = num << 3 | 2.
    let tags = |page: &CommandPage| -> Vec<u8> {
        let bytes = page.encode_to_vec();
        let mut out = Vec::new();
        let mut i = 0;
        while i < bytes.len() {
            out.push(bytes[i]);
            let len = usize::from(bytes[i + 1]); // every value here is < 128 bytes
            i += 2 + len;
        }
        out
    };
    assert_eq!(tags(&command_page(Kind::Client)), [0x1a, 0x42, 0x4a]);
    assert_eq!(tags(&command_page(Kind::Policy)), [0x1a, 0x42, 0x4a, 0x52]);
    assert_eq!(tags(&command_page(Kind::Fact)), [0x22, 0x42, 0x4a, 0x5a]);
}

#[test]
fn event_meta_records_the_command_as_causation_not_its_trigger() {
    // An event's causation is its direct cause, the producing command. The
    // command's own causation (a reaction's trigger) is not on the event.
    for kind in KINDS {
        let page = command_page(kind);
        let meta = round_trip(&meta(kind));
        let id = page.command_id.clone().expect("assigned");
        assert_eq!(meta.causation, Some(event_meta::Causation::CommandId(id)));
        assert_eq!(meta.correlation_id, page.correlation_id);
        assert_eq!(meta.fact, page.fact_origin);
        assert_eq!(meta.rules_version, "v3");
        assert!(meta.recorded_at.is_some());
    }
    // The reaction page does carry a trigger; its event meta does not.
    assert!(command_page(Kind::Policy).causation_id.is_some());
}

#[test]
fn a_process_managers_events_name_their_fact() {
    // Decision 14.31: a PM is a domain; its events for a fact carry the
    // fact's id as causation, its origin and its correlation.
    let page = command_page(Kind::Fact);
    let meta = round_trip(&meta(Kind::Fact));
    assert_eq!(
        meta.causation,
        Some(event_meta::Causation::CommandId(
            page.command_id.clone().expect("assigned")
        ))
    );
    assert_eq!(meta.fact, page.fact_origin);
    assert_eq!(meta.correlation_id, page.correlation_id);
}

#[test]
fn deleted_triggering_event_bytes_decode_to_no_causation() {
    // triggering_event (18) is deleted, unreserved (decision 14.31): its
    // bytes are an unknown field, never read as a cause.
    let old = [0x92, 0x01, 0];
    assert_eq!(EventMeta::decode(&old[..]).expect("decode").causation, None);
}

#[test]
fn event_meta_field_numbers() {
    // causation: command_id 17 (triggering_event 18 is deleted); correlation_id 14,
    // fact 16, recorded_at 5, rules_version 7. No actor, no origin, no
    // base_sequence, and no causation_id: 13 and 15 are gone.
    let first_two = |meta: EventMeta| meta.encode_to_vec()[..2].to_vec();
    let id = Some(uuid(1));
    // Field 17 needs a two-byte tag: 17 << 3 | 2 = 138 = 0x8a 0x01.
    assert_eq!(
        EventMeta {
            causation: id.clone().map(event_meta::Causation::CommandId),
            ..Default::default()
        }
        .encode_to_vec()[..3],
        [0x8a, 0x01, 18]
    );
    assert_eq!(
        first_two(EventMeta {
            correlation_id: id,
            ..Default::default()
        }),
        [0x72, 18]
    );
    // Pass-12 bytes for command_id (13) and causation_id (15) decode to
    // nothing: unknown fields, not misread as the new causation.
    let old = [&[0x6a, 16][..], &[1; 16][..], &[0x7a, 0][..]].concat();
    assert_eq!(
        EventMeta::decode(old.as_slice()).expect("decode"),
        EventMeta::default()
    );
    // Field 16 needs a two-byte tag: 16 << 3 | 2 = 130 = 0x82 0x01.
    assert_eq!(
        EventMeta {
            fact: Some(FactOrigin::default()),
            ..Default::default()
        }
        .encode_to_vec(),
        [0x82, 0x01, 0]
    );
    assert_eq!(
        first_two(EventMeta {
            rules_version: "v".into(),
            ..Default::default()
        }),
        [0x3a, 1]
    );
}

#[test]
fn absent_expected_sequence_is_distinct_from_zero() {
    let absent = round_trip(&request_book(Some(Handling::default())));
    assert_eq!(absent.handling.unwrap().expected_sequence, None);

    let zero = Some(Handling {
        expected_sequence: Some(0),
        ..Handling::default()
    });
    let decoded = round_trip(&request_book(zero));
    assert_eq!(decoded.handling, zero);
    assert_eq!(decoded.handling.unwrap().expected_sequence, Some(0));
}

#[test]
fn handling_rides_the_command_book_not_the_page() {
    let decoded = round_trip(&request_book(handling()));
    let h = decoded.handling.expect("handling");
    assert_eq!(h.expected_sequence, Some(u64::from(u32::MAX) + 1));
    assert_eq!(
        h.conflict_resolution(),
        ConflictResolution::AggregateHandles
    );

    // The page is identity + body only: encoding it with and
    // without the book's handling yields the same page bytes.
    let with = request_book(handling());
    let without = request_book(None);
    assert_eq!(
        with.page.unwrap().encode_to_vec(),
        without.page.unwrap().encode_to_vec()
    );
}

#[test]
fn unset_handling_reads_as_unspecified_defaults() {
    // Unspecified conflict resolution means "use configuration", not STRICT.
    let h = round_trip(&request_book(None)).handling.unwrap_or_default();
    assert_eq!(h.expected_sequence, None);
    assert_eq!(h.conflict_resolution(), ConflictResolution::Unspecified);
}

#[test]
fn conflict_resolution_values_are_lint_conformant_and_keep_their_numbers() {
    // Renamed from MergeStrategy; the numbers are unchanged, and COMMUTATIVE
    // is restored (opt-in, resolved in the client library) at its old 1.
    for (value, number, name) in [
        (
            ConflictResolution::Unspecified,
            0,
            "CONFLICT_RESOLUTION_UNSPECIFIED",
        ),
        (
            ConflictResolution::Commutative,
            1,
            "CONFLICT_RESOLUTION_COMMUTATIVE",
        ),
        (ConflictResolution::Strict, 2, "CONFLICT_RESOLUTION_STRICT"),
        (
            ConflictResolution::AggregateHandles,
            3,
            "CONFLICT_RESOLUTION_AGGREGATE_HANDLES",
        ),
    ] {
        assert_eq!(value as i32, number);
        assert_eq!(value.as_str_name(), name);
        assert_eq!(ConflictResolution::from_str_name(name), Some(value));
        let h = Handling {
            expected_sequence: Some(4),
            conflict_resolution: value as i32,
        };
        assert_eq!(round_trip(&h).conflict_resolution(), value);
    }
}

#[test]
fn contextual_command_carries_the_conflict_nonce() {
    let nonce = bytes::Bytes::from(vec![0x9e; 16]);
    let original = ContextualCommand {
        events: Some(EventBook {
            next_sequence: 12,
            ..EventBook::default()
        }),
        command: Some(request_book(handling())),
        conflict_nonce: nonce.clone(),
        ..ContextualCommand::default()
    };
    let decoded = round_trip(&original);
    assert_eq!(decoded, original);
    assert_eq!(decoded.conflict_nonce, nonce);

    // Under STRICT there is no nonce: empty, and absent from the wire. The
    // nonce is field 3 (tag 0x1a), written after the book and the command.
    let strict = ContextualCommand {
        conflict_nonce: bytes::Bytes::new(),
        ..original.clone()
    };
    let with = original.encode_to_vec();
    let without = strict.encode_to_vec();
    assert!(with.starts_with(&without));
    assert_eq!(
        &with[without.len()..],
        &[&[0x1a, 16][..], &nonce[..]].concat()[..]
    );
    assert!(round_trip(&strict).conflict_nonce.is_empty());
}

#[test]
fn snapshot_records_state_type_and_rules_version() {
    let original = Snapshot {
        sequence: u64::from(u32::MAX) + 9,
        state: Some(any("/draws.v1.DrawState", &[1, 1])),
        rules_version: "v3".into(),
    };
    let decoded = round_trip(&original);
    assert_eq!(decoded, original);
    // Both halves of the load-time validity check survive the wire: the
    // state type (the Any's type URL) and the rules version.
    assert_eq!(decoded.state.unwrap().type_url, "/draws.v1.DrawState");
    assert_eq!(decoded.rules_version, "v3");
    // Field 5: a fresh number (4 was the pass-2 retention field).
    let bare = Snapshot {
        rules_version: "r".into(),
        ..Snapshot::default()
    };
    assert_eq!(bare.encode_to_vec(), vec![0x2a, 1, b'r']);
}

#[test]
fn handling_carries_no_sync_mode() {
    // Sync is a call-level choice (which coordinator RPC), not handling. A
    // pre-pass-4 sender's field 3 (sync = DECISION) is not part of Handling:
    // it decodes to nothing and does not survive re-encoding.
    let current = Handling {
        expected_sequence: Some(5),
        conflict_resolution: ConflictResolution::Strict as i32,
    };
    let mut legacy = current.encode_to_vec();
    legacy.extend_from_slice(&[0x18, 0x04]); // field 3, varint 4
    let decoded = Handling::decode(legacy.as_slice()).expect("decode");
    assert_eq!(decoded, current);
    assert_eq!(decoded.encode_to_vec(), current.encode_to_vec());
}

#[test]
fn event_meta_round_trips_each_kind() {
    for kind in KINDS {
        let original = page(kind);
        let decoded = round_trip(&original);
        assert_eq!(decoded, original);
        assert_eq!(decoded.meta, Some(meta(kind)));
    }
}

#[test]
fn event_refs_nest_a_cover_with_its_edition() {
    let decoded = round_trip(&cause());
    let stream = decoded.stream.expect("stream");
    assert_eq!(stream.domain, "lists");
    assert_eq!(stream.edition.as_deref(), Some("what-if"));
    assert_eq!(stream.root, Some(uuid(0xab)));
    assert_eq!(decoded.sequence, u64::MAX);

    // Nested deeper: command page -> causation_id -> cover.
    let page = round_trip(&book(Kind::Policy, None)).page.unwrap();
    let causation = page.causation_id.unwrap();
    assert_eq!(
        causation.stream.unwrap().edition.as_deref(),
        Some("what-if")
    );
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
    let main_bytes = main.encode_to_vec();
    assert_eq!(round_trip(&main).edition, None);
    assert_eq!(Cover::default().edition, None, "the default cover is main");

    let empty = cover(Some(""));
    let empty_bytes = empty.encode_to_vec();
    // Field 4, wire type 2, length 0: present on the wire though empty.
    assert!(empty_bytes.ends_with(&[0x22, 0x00]));
    assert_ne!(empty_bytes, main_bytes);
    assert_eq!(round_trip(&empty).edition.as_deref(), Some(""));

    let named = cover(Some("what-if"));
    assert_eq!(round_trip(&named), named);
    assert_eq!(round_trip(&named).edition.as_deref(), Some("what-if"));

    // CorrelationQuery.edition follows the same convention.
    for edition in [None, Some(String::new()), Some("what-if".to_string())] {
        let q = CorrelationQuery {
            correlation_id: Some(uuid(0xc0)),
            edition: edition.clone(),
        };
        assert_eq!(round_trip(&q).edition, edition);
    }
    assert_ne!(
        CorrelationQuery {
            correlation_id: Some(uuid(0xc0)),
            edition: None,
        }
        .encode_to_vec(),
        CorrelationQuery {
            correlation_id: Some(uuid(0xc0)),
            edition: Some(String::new()),
        }
        .encode_to_vec()
    );
}

#[test]
fn event_meta_is_field_8_and_appended_after_the_page_body() {
    let mut p = page(Kind::Client);
    p.sequence = 0; // default: not encoded, so meta is the last field written
    let with = p.encode_to_vec();
    p.meta = None;
    let without = p.encode_to_vec();
    assert!(with.starts_with(&without));
    // Field 8, wire type 2 (length-delimited): tag byte 0x42.
    assert_eq!(with[without.len()], 0x42);
}

#[test]
fn outcome_round_trips_each_delivery_with_projections_and_failures() {
    let projection = Projection {
        cover: Some(Cover {
            domain: "draws".into(),
            root: Some(uuid(0xcd)),
            edition: None,
        }),
        projector: "draw-history".into(),
        // The stream's next_sequence after the projected events.
        next_sequence: u64::from(u32::MAX) + 2,
        projection: Some(any("/draws.v1.DrawHistory", &[4])),
    };
    // HandleSync answers OK with the successful projections beside the
    // failed ones; a failure never replaces the events.
    let failure = ProjectorFailure {
        projector: "draw-stats".into(),
        code: 14, // UNAVAILABLE
        diagnostic: Some(Diagnostic {
            code: "STATS_STORE_UNAVAILABLE".into(),
            args: [("store".to_string(), "draw-stats".to_string())].into(),
        }),
    };
    for delivery in [
        DeliveryOutcome::Unspecified,
        DeliveryOutcome::Applied,
        DeliveryOutcome::Duplicate,
        DeliveryOutcome::Parked,
    ] {
        let original = Outcome {
            delivery: delivery as i32,
            projections: vec![projection.clone()],
            projector_failures: vec![failure.clone()],
        };
        let decoded = round_trip(&original);
        assert_eq!(decoded, original);
        assert_eq!(decoded.delivery(), delivery);
        assert_eq!(decoded.projections, vec![projection.clone()]);
        assert_eq!(decoded.projector_failures, vec![failure.clone()]);
    }
    assert_eq!(
        DeliveryOutcome::Duplicate.as_str_name(),
        "DELIVERY_OUTCOME_DUPLICATE"
    );
    // A failed fact ingest is "accepted, parked": its own outcome, neither a
    // rejection nor APPLIED.
    assert_eq!(DeliveryOutcome::Parked as i32, 3);
    assert_eq!(
        DeliveryOutcome::Parked.as_str_name(),
        "DELIVERY_OUTCOME_PARKED"
    );
}

#[test]
fn component_descriptor_carries_its_state_descriptor() {
    let state = StateDescriptor {
        type_url: "/hatpick.v1.ListState".into(),
        // The full resolved list: the base plus COMMUTATIVE, in number order.
        options: Some(StateOptions {
            conflict_resolutions: vec![
                ConflictResolution::Commutative as i32,
                ConflictResolution::Strict as i32,
                ConflictResolution::AggregateHandles as i32,
            ],
        }),
    };
    let original = ComponentDescriptor {
        name: "list".into(),
        kind: ComponentKind::Aggregate as i32,
        inputs: vec![Target {
            domain: "lists".into(),
        }],
        state: Some(state),
        rules_version: "1.4.2+build.7".into(),
        starting_types: vec![],
        output_domain: String::new(),
    };
    let decoded = round_trip(&original);
    assert_eq!(decoded, original);
    assert_eq!(decoded.kind(), ComponentKind::Aggregate);
    assert_eq!(decoded.rules_version, "1.4.2+build.7");
    let resolutions: Vec<ConflictResolution> = decoded
        .state
        .unwrap()
        .options
        .unwrap()
        .conflict_resolutions()
        .collect();
    assert_eq!(
        resolutions,
        [
            ConflictResolution::Commutative,
            ConflictResolution::Strict,
            ConflictResolution::AggregateHandles
        ]
    );
    // The state is field 6 and the rules version field 4.
    let bare = ComponentDescriptor {
        state: Some(StateDescriptor::default()),
        ..Default::default()
    };
    assert_eq!(bare.encode_to_vec(), [0x32, 0]);
    let versioned = ComponentDescriptor {
        rules_version: "1".into(),
        ..Default::default()
    };
    assert_eq!(versioned.encode_to_vec(), [0x22, 1, b'1']);
}

#[test]
fn key_types_are_eq_and_hash_and_bytes_are_shared() {
    // Compile-time: each key type is Eq + Hash (prost derives both for
    // float-free messages). A float added to one fails this build.
    fn key<T: Eq + Hash + Clone>(value: T) -> usize {
        HashSet::from([value.clone(), value]).len()
    }
    assert_eq!(key(uuid(1)), 1);
    assert_eq!(key(cause()), 1);
    assert_eq!(key(cause().stream.unwrap()), 1);
    assert_eq!(key(fact_origin()), 1);
    assert_eq!(key(Target::default()), 1);
    assert_eq!(key(StateOptions::default()), 1);
    assert_eq!(key(StateDescriptor::default()), 1);
    assert_eq!(key(ConflictUnit::default()), 1);

    // `bytes` fields are bytes::Bytes: a clone shares the buffer.
    let id = uuid(7);
    let copy = id.clone();
    let _: &bytes::Bytes = &id.value;
    assert_eq!(id.value.as_ptr(), copy.value.as_ptr());
}

#[test]
fn dead_letter_names_its_component_kind_and_takes_the_cover_from_its_payload() {
    let rejected = request_book(handling());
    let original = AngzarrDeadLetter {
        payload: Some(angzarr_dead_letter::Payload::RejectedCommand(
            rejected.clone(),
        )),
        source_component: "list-to-draw".into(),
        source_component_kind: ComponentKind::Policy as i32,
        ..AngzarrDeadLetter::default()
    };
    let decoded = round_trip(&original);
    assert_eq!(decoded, original);
    assert_eq!(decoded.source_component_kind(), ComponentKind::Policy);
    match decoded.payload {
        Some(angzarr_dead_letter::Payload::RejectedCommand(b)) => {
            assert_eq!(b.cover, rejected.cover)
        }
        other => panic!("expected rejected command, got {other:?}"),
    }
}

#[test]
fn type_names_are_io_angzarr_v1() {
    assert_eq!(EventPage::full_name(), "io.angzarr.v1.EventPage");
    assert_eq!(EventMeta::full_name(), "io.angzarr.v1.EventMeta");
    assert_eq!(EventRef::full_name(), "io.angzarr.v1.EventRef");
    assert_eq!(Handling::full_name(), "io.angzarr.v1.Handling");
    assert_eq!(CommandPage::full_name(), "io.angzarr.v1.CommandPage");
    assert_eq!(Command::full_name(), "io.angzarr.v1.Command");
    assert_eq!(Fact::full_name(), "io.angzarr.v1.Fact");
    assert_eq!(CommandBook::full_name(), "io.angzarr.v1.CommandBook");
    assert_eq!(FactOrigin::full_name(), "io.angzarr.v1.FactOrigin");
    assert_eq!(Outcome::full_name(), "io.angzarr.v1.Outcome");
    assert_eq!(StateOptions::full_name(), "io.angzarr.v1.StateOptions");
    assert_eq!(
        StateDescriptor::full_name(),
        "io.angzarr.v1.StateDescriptor"
    );
    assert_eq!(UpcastRequest::full_name(), "io.angzarr.v1.UpcastRequest");
    // angzarr's TYPE_URL_PREFIX is "/" (see types.proto), which is prost's
    // default when no type-name domain is configured.
    assert_eq!(EventPage::type_url(), "/io.angzarr.v1.EventPage");
    assert_eq!(CommandBook::type_url(), "/io.angzarr.v1.CommandBook");
    assert_eq!(Fact::type_url(), "/io.angzarr.v1.Fact");
    assert_eq!(
        angzarr_proto::sererr::v1::CapturedError::full_name(),
        "sererr.v1.CapturedError"
    );

    let p = page(Kind::Policy);
    let packed = prost_types::Any::from_msg(&p).expect("pack");
    assert_eq!(packed.type_url, "/io.angzarr.v1.EventPage");
    assert_eq!(packed.to_msg::<EventPage>().expect("unpack"), p);

    let b = book(Kind::Fact, None);
    let packed = prost_types::Any::from_msg(&b).expect("pack");
    assert_eq!(packed.type_url, "/io.angzarr.v1.CommandBook");
    assert_eq!(packed.to_msg::<CommandBook>().expect("unpack"), b);
}

#[test]
fn a_process_manager_descriptor_lists_its_starting_facts() {
    // Ruling B1, adapted (decision 14.31): a PM's descriptor names the
    // input types that may start an instance. A PM subscribes to nothing,
    // so it has no inputs.
    let original = ComponentDescriptor {
        name: "buy-in".into(),
        kind: ComponentKind::ProcessManager as i32,
        starting_types: vec!["/io.angzarr.examples.v1.StartBuyIn".into()],
        rules_version: "1.0.0".into(),
        ..Default::default()
    };
    let decoded = round_trip(&original);
    assert_eq!(decoded, original);
    assert_eq!(
        decoded.starting_types,
        ["/io.angzarr.examples.v1.StartBuyIn"]
    );
    assert!(decoded.inputs.is_empty());
    // starting_types is ComponentDescriptor's field 7: 7 << 3 | 2 = 0x3a.
    let descriptor = ComponentDescriptor {
        starting_types: vec!["/a".into()],
        ..Default::default()
    };
    assert_eq!(descriptor.encode_to_vec(), [0x3a, 2, b'/', b'a']);
}

#[test]
fn a_policy_descriptor_carries_its_output_domain_as_field_8() {
    // Decision 14.42: the policy's coordinator checks every command against
    // the descriptor's one output domain.
    let original = ComponentDescriptor {
        name: "order-to-inventory".into(),
        kind: ComponentKind::Policy as i32,
        inputs: vec![Target {
            domain: "order".into(),
        }],
        rules_version: "1.0.0".into(),
        output_domain: "inventory".into(),
        ..Default::default()
    };
    let decoded = round_trip(&original);
    assert_eq!(decoded, original);
    assert_eq!(decoded.output_domain, "inventory");
    // output_domain is ComponentDescriptor's field 8: 8 << 3 | 2 = 0x42.
    let descriptor = ComponentDescriptor {
        output_domain: "i".into(),
        ..Default::default()
    };
    assert_eq!(descriptor.encode_to_vec(), [0x42, 1, b'i']);
}

#[test]
fn the_standard_refusal_fact_carries_the_refused_command_its_typed_refusal_and_trigger() {
    // Decision 14.31: the policy builds CommandRefused { command, refusal (the
    // foreign domain's typed refusal object, packed), trigger } and nothing
    // else; its id is the refused command's id.
    let refused = book(Kind::Policy, None);
    let typed = any(
        "/io.angzarr.examples.v1.InsufficientFunds",
        &[8, 100, 16, 200, 3],
    );
    let original = CommandRefused {
        command: Some(refused.clone()),
        refusal: Some(typed.clone()),
        trigger: Some(cause()),
    };
    let decoded = round_trip(&original);
    assert_eq!(decoded, original);
    assert_eq!(decoded.refusal, Some(typed.clone()));
    assert_eq!(
        decoded.command.unwrap().page.unwrap().command_id,
        refused.page.unwrap().command_id
    );
    // The refused policy receives the same typed object in RefusalResponse.
    let response = RefusalResponse {
        reason: Some(refusal_response::Reason::Refusal(typed)),
    };
    assert_eq!(round_trip(&response), response);
}

/// Decision 14.34: `CommandOptions.fact` (field 5, a bool) says whether a
/// `(command)` message is a command (unset: refusable) or a fact (set:
/// unrefusable), beside the fields before it.
#[test]
fn command_options_say_command_or_fact_in_field_5() {
    use angzarr_proto::v1::CommandOptions;
    let fact = CommandOptions {
        component: "io.angzarr.examples.v1.BuyInState".into(),
        starts: true,
        fact: true,
        ..CommandOptions::default()
    };
    assert_eq!(
        CommandOptions::decode(&*fact.encode_to_vec()).unwrap(),
        fact
    );
    let only_fact = CommandOptions {
        fact: true,
        ..CommandOptions::default()
    };
    assert_eq!(
        only_fact.encode_to_vec(),
        [5 << 3, 1],
        "field 5, varint true"
    );
    assert!(!CommandOptions::default().fact, "unset: a command");
}
