use std::sync::Mutex;

use angzarr_diagnostic::Diagnostic;
use angzarr_proto::v1::event_page::Body;
use angzarr_proto::v1::{EventMeta, EventPage, MetaEvent};

use super::*;
use crate::support::block_on;
use crate::Code;

fn any(name: &str, value: u8) -> Any {
    Any {
        type_url: format!("/test.{name}"),
        value: vec![value],
    }
}

fn page(sequence: u64, name: &str) -> EventPage {
    EventPage {
        meta: Some(EventMeta::default()),
        sequence,
        body: Some(Body::Event(any(name, sequence as u8))),
    }
}

fn upcasted(sequence: u64, name: &str) -> Upcasted {
    Upcasted {
        key: Key::Sequence(sequence),
        payload: any(name, 100 + sequence as u8),
    }
}

/// What the port was asked.
#[derive(Debug, PartialEq)]
enum Asked {
    Pages(Vec<EventPage>),
    Lone(Any),
}

/// A port that answers what the test scripted, once, and remembers what it
/// was asked.
struct Scripted {
    answer: Mutex<Option<Result<Vec<Upcasted>, UpcastError>>>,
    asked: Mutex<Vec<(String, Asked)>>,
}

impl Scripted {
    fn answering(answer: Result<Vec<Upcasted>, UpcastError>) -> Scripted {
        Scripted {
            answer: Mutex::new(Some(answer)),
            asked: Mutex::new(Vec::new()),
        }
    }

    fn asked(&self) -> Vec<(String, Asked)> {
        std::mem::take(&mut self.asked.lock().unwrap())
    }
}

impl Upcast for Scripted {
    fn upcast<'a>(
        &'a self,
        domain: &'a str,
        input: Input<'a>,
    ) -> BoxFuture<'a, Result<Vec<Upcasted>, UpcastError>> {
        let asked = match input {
            Input::Pages(pages) => Asked::Pages(pages.to_vec()),
            Input::Lone(payload) => Asked::Lone(payload.clone()),
        };
        self.asked.lock().unwrap().push((domain.to_string(), asked));
        let answer = self.answer.lock().unwrap().take().expect("one request");
        Box::pin(std::future::ready(answer))
    }
}

fn down() -> UpcastError {
    UpcastError::Unavailable {
        cause: Report::new("annals-upcaster-io").arg("errno", 111u64),
    }
}

/// A stream's pages go to the port as one request, and what comes back,
/// keyed by sequence and in ANY order, is put in the page of its sequence:
/// the pages keep their order, their sequences and their meta, and only
/// their payloads are the upcast ones.
#[test]
fn upcast_pages_round_trips_keyed_pages() {
    let pages = vec![page(0, "A1"), page(1, "B1"), page(2, "C1")];
    let port = Scripted::answering(Ok(vec![
        upcasted(2, "C2"),
        upcasted(0, "A2"),
        upcasted(1, "B2"),
    ]));
    let upcast = block_on(upcast_pages(&port, "player", &pages)).expect("upcast");
    assert_eq!(upcast.len(), 3);
    for (n, (before, after)) in pages.iter().zip(&upcast).enumerate() {
        assert_eq!(after.sequence, before.sequence);
        assert_eq!(after.meta, before.meta);
        assert_eq!(
            after.body,
            Some(Body::Event(Any {
                type_url: format!("/test.{}2", ["A", "B", "C"][n]),
                value: vec![100 + n as u8],
            }))
        );
    }
    assert_eq!(
        port.asked(),
        [("player".to_string(), Asked::Pages(pages))],
        "one request: the domain and every page, as read"
    );
}

/// A page nothing answered stays as it was; an answer for a sequence that
/// was not sent is ignored; the first answer for a sequence wins.
#[test]
fn a_page_nothing_answered_is_unchanged_and_a_stray_answer_is_ignored() {
    let pages = vec![page(5, "A1"), page(6, "B1")];
    let port = Scripted::answering(Ok(vec![
        upcasted(9, "Z2"),
        upcasted(6, "B2"),
        upcasted(6, "B3"),
    ]));
    let upcast = block_on(upcast_pages(&port, "player", &pages)).expect("upcast");
    assert_eq!(upcast.len(), 2);
    assert_eq!(upcast[0], pages[0]);
    assert_eq!(upcast[1].body, Some(Body::Event(any("B2", 106))));
}

/// Upcasting is of events: a framework meta-event, or a page with no body,
/// is never replaced, even by an answer under its sequence.
#[test]
fn a_meta_event_and_an_empty_page_are_never_replaced() {
    let meta = EventPage {
        meta: None,
        sequence: 0,
        body: Some(Body::MetaEvent(MetaEvent::default())),
    };
    let empty = EventPage {
        meta: None,
        sequence: 1,
        body: None,
    };
    let pages = vec![meta, empty];
    let port = Scripted::answering(Ok(vec![upcasted(0, "A2"), upcasted(1, "B2")]));
    let upcast = block_on(upcast_pages(&port, "player", &pages)).expect("upcast");
    assert_eq!(upcast, pages);
}

/// No pages, no request: the port is not asked at all.
#[test]
fn no_pages_is_no_request() {
    let port = Scripted::answering(Err(down()));
    let upcast = block_on(upcast_pages(&port, "player", &[])).expect("upcast");
    assert!(upcast.is_empty());
    assert_eq!(port.asked(), []);
}

/// A lone payload goes as a lone payload, and the answer with no sequence
/// is taken; one keyed by a sequence is not the lone payload's.
#[test]
fn upcast_input_takes_the_answer_with_no_sequence() {
    let sent = any("Cmd1", 1);
    let answer = Upcasted {
        key: Key::NoSequence,
        payload: any("Cmd2", 2),
    };
    let port = Scripted::answering(Ok(vec![upcasted(0, "Z2"), answer.clone()]));
    let got = block_on(upcast_input(&port, "player", &sent)).expect("upcast");
    assert_eq!(got, answer.payload);
    assert_eq!(port.asked(), [("player".to_string(), Asked::Lone(sent))]);
}

/// A port that answers none for the lone payload leaves it as sent.
#[test]
fn upcast_input_with_no_answer_is_as_sent() {
    let sent = any("Cmd1", 1);
    for answer in [Vec::new(), vec![upcasted(0, "Z2")]] {
        let port = Scripted::answering(Ok(answer));
        assert_eq!(
            block_on(upcast_input(&port, "player", &sent)),
            Ok(sent.clone())
        );
    }
}

/// An upcaster that cannot be reached is the spec's
/// `ANGZARR_REASON_UPCASTER_UNAVAILABLE`, UNAVAILABLE, whichever driver
/// asked, naming the domain and carrying the failure as `cause`.
#[test]
fn a_down_upcaster_is_unavailable_with_its_key() {
    let want = |error: Coded| {
        assert_eq!(error.key, "ANGZARR_REASON_UPCASTER_UNAVAILABLE");
        assert_eq!(error.code, Code::Unavailable);
        assert_eq!(
            error.args.into_iter().collect::<Vec<_>>(),
            [
                ("cause".to_string(), "annals-upcaster-io".to_string()),
                ("cause.errno".to_string(), "111".to_string()),
                ("domain".to_string(), "player".to_string()),
            ]
        );
    };
    let port = Scripted::answering(Err(down()));
    want(block_on(upcast_pages(&port, "player", &[page(0, "A1")])).unwrap_err());
    let port = Scripted::answering(Err(down()));
    want(block_on(upcast_input(&port, "player", &any("Cmd", 1))).unwrap_err());
    want(down().coded("player"));
}

/// An upcaster's own refusal is passed on as it is: its key, its status.
#[test]
fn an_upcasters_refusal_is_passed_on_unchanged() {
    let refusal = Coded::new(
        Code::InvalidArgument,
        "player-upcast-bad-version",
        [("version".to_string(), "9".to_string())],
    );
    let port = Scripted::answering(Err(UpcastError::Refused(refusal.clone())));
    assert_eq!(
        block_on(upcast_pages(&port, "player", &[page(0, "A1")])),
        Err(refusal.clone())
    );
    let port = Scripted::answering(Err(UpcastError::Refused(refusal.clone())));
    assert_eq!(
        block_on(upcast_input(&port, "player", &any("Cmd", 1))),
        Err(refusal.clone())
    );
    assert_eq!(
        UpcastError::Refused(refusal.clone()).coded("player"),
        refusal
    );
}

/// The failure's own code and args: what a log shows and the catalog
/// renders.
#[test]
fn an_upcast_error_has_its_code_and_args() {
    let unavailable = down();
    assert_eq!(
        unavailable.code(),
        "angzarr-coordinator-base-upcast-unavailable"
    );
    assert_eq!(
        unavailable.to_string(),
        "angzarr-coordinator-base-upcast-unavailable cause=annals-upcaster-io cause.errno=111"
    );
    let refused = UpcastError::Refused(Coded::new(Code::Internal, "player-bad", []));
    assert_eq!(refused.code(), "angzarr-coordinator-base-upcast-refused");
    assert_eq!(
        refused.to_string(),
        "angzarr-coordinator-base-upcast-refused reason=player-bad"
    );
    assert_eq!(samples().len(), 2);
}

/// The port is object-safe, as a coordinator holds it.
#[test]
fn the_port_is_object_safe() {
    let port = Scripted::answering(Ok(Vec::new()));
    let dynamic: &dyn Upcast = &port;
    let got = block_on(upcast_input(dynamic, "d", &any("X", 1))).expect("upcast");
    assert_eq!(got, any("X", 1));
}
