//! The context a handler is handed ([`command_context`]), read from the
//! `HandleRequest` alone: the full context, read-only (decision 46, Q1).

use super::*;

fn uuid(byte: u8) -> pb::Uuid {
    pb::Uuid {
        value: vec![byte; 16].into(),
    }
}

fn cover() -> pb::Cover {
    pb::Cover {
        domain: "order".to_string(),
        root: Some(uuid(9)),
        edition: Some("what-if".to_string()),
    }
}

fn origin() -> pb::FactOrigin {
    pb::FactOrigin {
        external_id: "pi_1".to_string(),
        description: "Stripe".to_string(),
    }
}

fn request(body: pb::command_page::Body) -> hpb::HandleRequest {
    hpb::HandleRequest {
        input: Some(pb::CommandBook {
            cover: Some(cover()),
            page: Some(pb::CommandPage {
                body: Some(body),
                command_id: Some(uuid(1)),
                correlation_id: Some(uuid(2)),
            }),
        }),
        ..Default::default()
    }
}

fn command(expected_sequence: Option<u64>) -> pb::command_page::Body {
    pb::command_page::Body::Command(pb::Command {
        payload: None,
        handling: Some(pb::Handling { expected_sequence }),
    })
}

fn fact() -> pb::command_page::Body {
    pb::command_page::Body::Fact(pb::Fact {
        payload: None,
        origin: Some(origin()),
    })
}

// Decision 14.37 (C-0671): the input's acceptance time, stamped by the
// coordinator or the angzarr-embedded kernel, reaches the handler as
// delivered, so business code reads no clock of its own.
#[test]
fn the_context_carries_when_the_input_was_accepted() {
    let accepted = prost_types::Timestamp {
        seconds: 1_793_188_800,
        nanos: 5,
    };
    let req = hpb::HandleRequest {
        recorded_at: Some(accepted),
        ..Default::default()
    };
    assert_eq!(command_context(&req).recorded_at, Some(accepted));
    assert_eq!(
        command_context(&hpb::HandleRequest::default()).recorded_at,
        None
    );
}

// The input's identity, cover and arm, as delivered: a command's expected
// sequence as the client sent it (the coordinator never stamps it; decision
// 14.94), a fact's origin.
#[test]
fn the_context_is_the_inputs_identity_cover_and_arm() {
    let ctx = command_context(&request(command(Some(4))));
    assert_eq!(ctx.cover, Some(cover()));
    assert_eq!(ctx.command_id, Some(uuid(1)));
    assert_eq!(ctx.correlation_id, Some(uuid(2)));
    assert_eq!(
        ctx.arm,
        InputArm::Command {
            expected_sequence: Some(4)
        }
    );
    assert_eq!(
        command_context(&request(command(None))).arm,
        InputArm::Command {
            expected_sequence: None
        },
        "unset: the client stated none"
    );
    assert_eq!(
        command_context(&request(fact())).arm,
        InputArm::Fact {
            origin: Some(origin())
        }
    );
    // A page with no body is a command with no expected sequence: the
    // router guards no envelope.
    assert_eq!(
        command_context(&hpb::HandleRequest::default()).arm,
        InputArm::default()
    );
    assert_eq!(
        InputArm::default(),
        InputArm::Command {
            expected_sequence: None
        }
    );
}

// A command with no Handling has no expected sequence.
#[test]
fn a_command_with_no_handling_states_no_expected_sequence() {
    let body = pb::command_page::Body::Command(pb::Command::default());
    assert_eq!(
        command_context(&request(body)).arm,
        InputArm::Command {
            expected_sequence: None
        }
    );
}

// The history's head and whether it holds any (pages or a starting state);
// a policy's input carries its trigger.
#[test]
fn the_context_carries_the_head_history_and_trigger() {
    let trigger = pb::EventRef {
        stream: Some(cover()),
        sequence: 3,
    };
    let mut req = request(command(None));
    req.history = Some(pb::EventBook {
        pages: vec![pb::EventPage::default()],
        next_sequence: 6,
        ..Default::default()
    });
    req.causation = Some(trigger.clone());
    let ctx = command_context(&req);
    assert_eq!(ctx.next_sequence, 6);
    assert!(ctx.had_prior_events);
    assert_eq!(ctx.causation, Some(trigger));

    req.history = Some(pb::EventBook {
        snapshot: Some(pb::Snapshot::default()),
        ..Default::default()
    });
    assert!(command_context(&req).had_prior_events, "a starting state");
    req.history = Some(pb::EventBook::default());
    assert!(!command_context(&req).had_prior_events, "an empty history");
    req.history = None;
    let ctx = command_context(&req);
    assert!(!ctx.had_prior_events, "no history");
    assert_eq!(ctx.next_sequence, 0);
}

// The full-context rule: the meta a decision's events are stamped and
// folded with is the context's, field for field, under the rules version
// given, its causation the input's arm (decision 14.102).
#[test]
fn event_meta_is_the_contexts_identity_under_the_rules_version() {
    let at = prost_types::Timestamp {
        seconds: 3,
        nanos: 4,
    };
    let mut req = request(command(Some(1)));
    req.recorded_at = Some(at);
    assert_eq!(
        command_context(&req).event_meta("1.4.2"),
        pb::EventMeta {
            causation: Some(pb::event_meta::Causation::Command(pb::CommandCause {
                command_id: Some(uuid(1)),
            })),
            correlation_id: Some(uuid(2)),
            recorded_at: Some(at),
            rules_version: "1.4.2".to_string(),
        }
    );
    assert_eq!(
        command_context(&request(fact())).event_meta("2"),
        pb::EventMeta {
            causation: Some(pb::event_meta::Causation::Fact(pb::FactCause {
                command_id: Some(uuid(1)),
                origin: Some(origin()),
            })),
            correlation_id: Some(uuid(2)),
            recorded_at: None,
            rules_version: "2".to_string(),
        }
    );
}
