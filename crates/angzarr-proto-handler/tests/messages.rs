//! The handler layer's messages (decision 14.78) as the merged spec shapes
//! them: each kind's request carries core's books, and each response only
//! what business code decides. Field numbers are contiguous from 1, oneof
//! members included (decision 14.100).

use angzarr_proto::v1::{
    Command, CommandBook, CommandPage, Cover, EventBook, EventPage, EventRef, ReactionDelivery,
    RefusalStatus, Uuid,
};
use angzarr_proto_handler::v1::{
    output, upcast_request, upcasted, HandleRequest, HandleResponse, NoSequence, Output,
    ProjectRequest, ProjectResponse, ReactRequest, ReactResponse, Refusal, TranslateRequest,
    TranslateResponse, UpcastPages, UpcastRequest, UpcastResponse, Upcasted,
};
use prost::{Message, Name};

fn any(type_url: &str) -> prost_types::Any {
    prost_types::Any {
        type_url: type_url.into(),
        value: vec![1],
    }
}

fn round_trip<M: Message + Default + PartialEq + std::fmt::Debug>(m: &M) {
    assert_eq!(&M::decode(&*m.encode_to_vec()).unwrap(), m);
}

/// The tags of a message's top-level fields, in wire order (every value
/// here is length-delimited and under 128 bytes).
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

const fn ld(n: u8) -> u8 {
    (n << 3) | 2
}

fn root() -> Uuid {
    Uuid {
        value: vec![7; 16].into(),
    }
}

fn trigger() -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: "player".into(),
            root: Some(root()),
            edition: None,
        }),
        pages: vec![EventPage::default()],
        ..EventBook::default()
    }
}

#[test]
fn handle_takes_the_input_and_its_history_and_answers_success_only() {
    // Decisions 14.98 and 14.95: one Handle for commands and facts; the
    // answer is the events, the state and the rules version, nothing else.
    let request = HandleRequest {
        input: Some(CommandBook {
            cover: trigger().cover,
            page: Some(CommandPage {
                body: Some(angzarr_proto::v1::command_page::Body::Command(Command {
                    payload: Some(any("/table.v1.PlaceBet")),
                    handling: None,
                })),
                ..CommandPage::default()
            }),
        }),
        history: Some(trigger()),
        recorded_at: Some(prost_types::Timestamp::default()),
        causation: Some(EventRef::default()),
    };
    round_trip(&request);
    assert_eq!(tags(&request), [ld(1), ld(2), ld(3), ld(4)]);
    let response = HandleResponse {
        events: vec![any("/table.v1.BetPlaced")],
        state: Some(any("/table.v1.TableState")),
        rules_version: "1.4.2".into(),
    };
    round_trip(&response);
    assert_eq!(tags(&response), [ld(1), ld(2), ld(3)]);
}

#[test]
fn a_refusal_is_typed_and_carries_its_business_status() {
    let refusal = Refusal {
        status: RefusalStatus::FailedPrecondition as i32,
        typed: Some(any("/table.v1.AlreadyBet")),
    };
    round_trip(&refusal);
    assert_eq!(refusal.status(), RefusalStatus::FailedPrecondition);
    assert_eq!(refusal.encode_to_vec()[..2], [1 << 3, 9]);
}

#[test]
fn react_takes_one_trigger_and_answers_outputs_by_arm() {
    round_trip(&ReactRequest {
        trigger: Some(trigger()),
    });
    let command = Output {
        root: Some(root()),
        body: Some(output::Body::Command(any("/table.v1.SitDown"))),
    };
    let fact = Output {
        root: Some(root()),
        body: Some(output::Body::Fact(any("/table.v1.Seated"))),
    };
    // root 1, command 2, fact 3.
    assert_eq!(tags(&command), [ld(1), ld(2)]);
    assert_eq!(tags(&fact), [ld(1), ld(3)]);
    round_trip(&ReactResponse {
        outputs: vec![command, fact],
    });
}

#[test]
fn translate_takes_the_trigger_the_delivery_and_the_refusal() {
    let request = TranslateRequest {
        trigger: Some(trigger()),
        command: Some(ReactionDelivery::default()),
        refusal: Some(Refusal::default()),
    };
    round_trip(&request);
    assert_eq!(tags(&request), [ld(1), ld(2), ld(3)]);
    round_trip(&TranslateResponse {
        fact: Some(any("/player.v1.MoveRefused")),
    });
}

#[test]
fn project_takes_one_streams_run() {
    round_trip(&ProjectRequest {
        events: Some(trigger()),
    });
    round_trip(&ProjectResponse {
        projection: Some(any("/ledger.v1.WalletLedgerProjection")),
    });
}

#[test]
fn an_upcast_is_keyed_by_sequence_or_no_sequence() {
    // Decisions 14.96 and 14.97: a stream's pages are answered by sequence,
    // a lone payload by NoSequence; never by position.
    let pages = UpcastRequest {
        domain: "table".into(),
        input: Some(upcast_request::Input::Pages(UpcastPages {
            events: vec![EventPage::default()],
        })),
    };
    let lone = UpcastRequest {
        domain: "table".into(),
        input: Some(upcast_request::Input::LonePayload(any(
            "/table.v1.PlaceBet",
        ))),
    };
    round_trip(&pages);
    round_trip(&lone);
    // domain 1, pages 2, lone_payload 3.
    assert_eq!(tags(&pages), [ld(1), ld(2)]);
    assert_eq!(tags(&lone), [ld(1), ld(3)]);
    let response = UpcastResponse {
        pages: vec![
            Upcasted {
                key: Some(upcasted::Key::Sequence(4)),
                payload: Some(any("/table.v2.BetPlaced")),
            },
            Upcasted {
                key: Some(upcasted::Key::NoSequence(NoSequence {})),
                payload: Some(any("/table.v2.PlaceBet")),
            },
        ],
    };
    round_trip(&response);
    // sequence 1 (a varint), no_sequence 2, payload 3.
    let keyed = |key| Upcasted {
        key: Some(key),
        payload: None,
    };
    assert_eq!(
        keyed(upcasted::Key::Sequence(4)).encode_to_vec(),
        [1 << 3, 4]
    );
    assert_eq!(
        keyed(upcasted::Key::NoSequence(NoSequence {})).encode_to_vec(),
        [ld(2), 0]
    );
}

#[test]
fn the_messages_are_the_handler_package() {
    assert_eq!(
        HandleRequest::full_name(),
        "io.angzarr.handler.v1.HandleRequest"
    );
    assert_eq!(NoSequence::type_url(), "/io.angzarr.handler.v1.NoSequence");
    let set =
        prost_types::FileDescriptorSet::decode(angzarr_proto_handler::FILE_DESCRIPTOR_SET).unwrap();
    let mut handler = 0;
    for file in set
        .file
        .iter()
        .filter(|f| f.name().starts_with("io/angzarr/handler/"))
    {
        handler += 1;
        assert_eq!(file.package(), "io.angzarr.handler.v1");
        assert!(
            file.service.is_empty(),
            "{} declares a service",
            file.name()
        );
        for import in &file.dependency {
            assert!(
                import.starts_with("io/angzarr/v1/")
                    || import.starts_with("io/angzarr/handler/v1/")
                    || import.starts_with("google/protobuf/"),
                "{} imports {import}: the layer depends on core only",
                file.name()
            );
        }
    }
    assert_eq!(handler, 4, "command_handler, policy, projector, upcaster");
}
