//! Upcasting, through a domain's upcaster sidecar: a scripted
//! `UpcastPort`, registered with `Registry::upcast_with` as a child's
//! sidecar would be, mapping old types to current ones (bytes as they
//! are), or unreachable.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use cucumber::{given, then, when};

use angzarr_embedded::angzarr_diagnostic::Report;
use angzarr_embedded::angzarr_router::error::GrpcCode;
use angzarr_embedded::angzarr_router::hpb::{
    self, upcast_request, upcasted, HandleRequest, UpcastRequest, UpcastResponse,
};
use angzarr_embedded::angzarr_router::send::BoxFuture;
use angzarr_embedded::v1::command_page;
use angzarr_embedded::{Any, UpcastError, UpcastPort};

use crate::tests::business::Rule;
use crate::tests::policy::{bare, internal, locked};
use crate::tests::world::Coordinator;

/// How a sidecar keys its answer to a stream's pages (decisions 14.96 and
/// 14.97): by each page's sequence, in an order of its own; or with one of
/// the keying bugs C-0821 names.
#[derive(Clone, Debug, Default)]
pub(crate) enum Keying {
    /// Each page by its sequence, in the request's order.
    #[default]
    BySequence,
    /// Each page by its sequence, answered in this order of sequences.
    InOrder(Vec<u64>),
    /// No entry for this sequence.
    Missing(u64),
    /// An extra entry for this sequence, which was not asked.
    Extra(u64),
    /// Two entries for this sequence.
    Twice(u64),
}

/// One domain's sidecar: its rules, old type name to current, whether it
/// can be reached, how it keys a stream's pages, and every request it was
/// sent.
#[derive(Default)]
pub(crate) struct Sidecar {
    pub(crate) rules: BTreeMap<String, String>,
    pub(crate) down: bool,
    pub(crate) keying: Keying,
    pub(crate) requests: Vec<UpcastRequest>,
}

/// A [`Sidecar`] as the kernel's `UpcastPort`.
pub(crate) struct Port(pub(crate) Arc<Mutex<Sidecar>>);

impl Sidecar {
    fn upcast_one(&self, any: &Any) -> Any {
        let name = bare(any);
        match self.rules.get(&name) {
            Some(current) => Any {
                type_url: any
                    .type_url
                    .replace(&format!(".{name}"), &format!(".{current}")),
                value: any.value.clone(),
            },
            None => any.clone(),
        }
    }

    /// The answer to `request`, keyed as the script says.
    fn answer(&self, request: &UpcastRequest) -> UpcastResponse {
        let keyed = |sequence: u64, payload: &Any| hpb::Upcasted {
            key: Some(upcasted::Key::Sequence(sequence)),
            payload: Some(self.upcast_one(payload)),
        };
        let pages = match &request.input {
            None => Vec::new(),
            Some(upcast_request::Input::LonePayload(payload)) => vec![hpb::Upcasted {
                key: Some(upcasted::Key::NoSequence(hpb::NoSequence {})),
                payload: Some(self.upcast_one(payload)),
            }],
            Some(upcast_request::Input::Pages(pages)) => {
                let events: Vec<(u64, Any)> = pages
                    .events
                    .iter()
                    .filter_map(|p| {
                        angzarr_embedded::angzarr_router::page_event(p)
                            .map(|e| (p.sequence, e.clone()))
                    })
                    .collect();
                let by = |sequence: u64| events.iter().find(|(s, _)| *s == sequence).cloned();
                let mut answer: Vec<hpb::Upcasted> = match &self.keying {
                    Keying::InOrder(order) => order
                        .iter()
                        .filter_map(|s| by(*s))
                        .map(|(s, e)| keyed(s, &e))
                        .collect(),
                    _ => events.iter().map(|(s, e)| keyed(*s, e)).collect(),
                };
                match &self.keying {
                    Keying::Missing(sequence) => {
                        answer.retain(|u| u.key != Some(upcasted::Key::Sequence(*sequence)))
                    }
                    Keying::Extra(sequence) => answer.push(keyed(*sequence, &Any::default())),
                    Keying::Twice(sequence) => {
                        if let Some((s, e)) = by(*sequence) {
                            answer.push(keyed(s, &e));
                        }
                    }
                    Keying::BySequence | Keying::InOrder(_) => {}
                }
                answer
            }
        };
        UpcastResponse { pages }
    }
}

impl UpcastPort for Port {
    fn upcast<'a>(
        &'a self,
        request: &'a UpcastRequest,
    ) -> BoxFuture<'a, Result<UpcastResponse, UpcastError>> {
        let mut sidecar = locked(&self.0);
        sidecar.requests.push(request.clone());
        let answer = match sidecar.down {
            true => Err(UpcastError::Unavailable {
                cause: Report::new("sidecar-unreachable".to_string()),
            }),
            false => Ok(sidecar.answer(request)),
        };
        Box::pin(std::future::ready(answer))
    }
}

/// The input a delivery hands business code, its type and bytes.
fn handed(delivered: &HandleRequest) -> (String, Vec<u8>, bool) {
    let page = delivered.input.as_ref().and_then(|b| b.page.as_ref());
    match page.and_then(|p| p.body.as_ref()) {
        Some(command_page::Body::Command(c)) => {
            let p = c.payload.clone().unwrap_or_default();
            (bare(&p), p.value, false)
        }
        Some(command_page::Body::Fact(f)) => {
            let p = f.payload.clone().unwrap_or_default();
            (bare(&p), p.value, true)
        }
        None => (String::new(), Vec::new(), false),
    }
}

#[given(regex = r#"^an "(\w+)" aggregate whose upcaster sidecar upcasts (\w+) to (\w+)$"#)]
fn aggregate_upcasting(w: &mut Coordinator, domain: String, old: String, current: String) {
    w.domain(&domain);
    w.sidecar(&domain).rules.insert(old, current);
}

#[given(
    regex = r#"^the "(\w+)" upcaster sidecar upcasts (\w+) to (\w+)(, which business code does not handle)?$"#
)]
fn sidecar_upcasts(
    w: &mut Coordinator,
    name: String,
    old: String,
    current: String,
    unhandled: String,
) {
    let domain = w.domain_of(&name);
    w.sidecar(&domain).rules.insert(old, current.clone());
    if !unhandled.is_empty() {
        let mut error = internal("ANGZARR_REASON_UNKNOWN_COMMAND_TYPE");
        error.grpc = GrpcCode::Internal;
        w.business(&domain).rule(Rule {
            input: current,
            once: None,
            error,
        });
    }
}

#[given(regex = r#"^the "(\w+)" upcaster sidecar is configured but unreachable$"#)]
fn sidecar_down(w: &mut Coordinator, domain: String) {
    w.sidecar(&domain).down = true;
}

#[given(regex = r#"^order "([\w-]+)" has a stored (\w+) event at sequence 0$"#)]
fn stored_old_event(w: &mut Coordinator, order: String, event: String) {
    w.speak_of(&order);
    let cover = w.order();
    w.write(&cover, &[&event]);
}

#[given(
    regex = r#"^order "([\w-]+)" also has (?:a )?stored (\w+) events? at sequences? (\d+)(?: and (\d+))?$"#
)]
fn also_stored(w: &mut Coordinator, order: String, event: String, first: u64, second: String) {
    let cover = w.cover("order", &order);
    let count = if second.is_empty() { 1 } else { 2 };
    assert_eq!(w.pages(&cover).len() as u64, first, "stored in order");
    w.write(&cover, &vec![event.as_str(); count]);
}

#[given(
    regex = r#"^the sidecar answers the pages of order "([\w-]+)" keyed by sequence, in the order (\d+), (\d+), (\d+)$"#
)]
fn keyed_in_order(w: &mut Coordinator, _order: String, a: u64, b: u64, c: u64) {
    w.sidecar("order").keying = Keying::InOrder(vec![a, b, c]);
}

#[given(
    regex = r#"^the "order" upcaster sidecar answers the pages of order "([\w-]+)" with (no entry for|an entry for|two entries for) sequence (\d+)$"#
)]
fn keyed_badly(w: &mut Coordinator, _order: String, how: String, sequence: u64) {
    w.sidecar("order").keying = match how.as_str() {
        "no entry for" => Keying::Missing(sequence),
        "an entry for" => Keying::Extra(sequence),
        _ => Keying::Twice(sequence),
    };
}

#[given(regex = r#"^a "(\w+)" process manager whose starting input is (\w+)$"#)]
fn process_manager_starting(w: &mut Coordinator, name: String, starting: String) {
    let domain = name.to_lowercase();
    w.names.insert(name, domain.clone());
    w.process_manager(&domain, &[starting.as_str()]);
}

/// A client's input of the type `name` to `cover`, its payload `content`'s
/// bytes.
fn input_of(
    w: &mut Coordinator,
    fact: bool,
    name: &str,
    content: &str,
) -> angzarr_embedded::v1::CommandBook {
    let cover = w.order();
    let mut book = match fact {
        true => w.fact(&cover, name, "delivery-1"),
        false => w.command(&cover, name),
    };
    let body = book.page.as_mut().and_then(|p| p.body.as_mut());
    let payload = match body {
        Some(command_page::Body::Command(c)) => c.payload.as_mut(),
        Some(command_page::Body::Fact(f)) => f.payload.as_mut(),
        None => None,
    };
    if let Some(payload) = payload {
        payload.value = content.as_bytes().to_vec();
    }
    book
}

#[when(
    regex = r#"^a client sends (the fact )?(\w+) "(\w+)" to order "([\w-]+)"(?: and waits for the answer)?$"#
)]
async fn client_sends(
    w: &mut Coordinator,
    fact: String,
    name: String,
    content: String,
    order: String,
) {
    w.speak_of(&order);
    w.begin();
    let fact = !fact.is_empty();
    if fact {
        let current = w
            .sidecar("order")
            .rules
            .get(&name)
            .cloned()
            .unwrap_or(name.clone());
        w.business("order").declare_fact(&current);
    }
    let book = input_of(w, fact, &name, &content);
    w.handle(book).await;
}

#[when(regex = r"^a client sends (\w+) for a (?:correlation|root) with no instance$")]
async fn sends_without_instance(w: &mut Coordinator, name: String) {
    w.begin();
    let domain = w
        .policies
        .first()
        .map(|p| p.output.clone())
        .unwrap_or_else(|| "fulfillment".to_string());
    let cover = w.cover(&domain, "0192a6b0-0000-7000-8000-0000000000f2");
    let book = w.command(&cover, &name);
    w.handle(book).await;
}

#[then(regex = r"^business code receives an? (\w+) event at sequence (\d+)$")]
fn receives_event(w: &mut Coordinator, event: String, sequence: u64) {
    let delivered = w.business("order").delivered().pop().expect("delivered");
    let pages = delivered.history.map(|e| e.pages).unwrap_or_default();
    let page = pages
        .iter()
        .find(|p| p.sequence == sequence)
        .expect("the event");
    assert_eq!(
        angzarr_embedded::angzarr_router::page_event(page).map(bare),
        Some(event)
    );
}

#[then(regex = r#"^order "([\w-]+)"'s stored event at sequence (\d+) is still an? (\w+)$"#)]
fn still_stored_as(w: &mut Coordinator, order: String, sequence: usize, event: String) {
    let cover = w.cover("order", &order);
    let pages = w.pages(&cover);
    assert_eq!(
        angzarr_embedded::angzarr_router::page_event(&pages[sequence]).map(bare),
        Some(event)
    );
}

#[then(regex = r#"^business code is handed an? (\w+)( fact)?(?: for "(\w+)")?$"#)]
fn handed_as(w: &mut Coordinator, name: String, fact: String, content: String) {
    let domain = w
        .handled
        .last()
        .and_then(|(b, _)| b.cover.clone())
        .expect("a stream")
        .domain;
    let delivered = w.business(&domain).delivered().pop().expect("delivered");
    let (got, bytes, is_fact) = handed(&delivered);
    assert_eq!(got, name);
    assert_eq!(is_fact, !fact.is_empty());
    if !content.is_empty() {
        assert_eq!(bytes, content.as_bytes());
    }
}

#[then(regex = r"^business code is never handed an? (\w+)$")]
fn never_handed(w: &mut Coordinator, name: String) {
    for delivered in w.business("order").delivered() {
        assert_ne!(handed(&delivered).0, name);
    }
}

#[then(regex = r"^the (?:command|fact) is accepted$")]
fn accepted(w: &mut Coordinator) {
    let (_, result) = w.handled.last().expect("an input was handled");
    assert!(result.is_ok(), "{:?}", w.answer());
}

#[then(
    regex = r#"^the client is refused (\w+) with reason (\w+), which is also its status message$"#
)]
fn refused_with(w: &mut Coordinator, status_name: String, reason: String) {
    let answer = w.answer();
    assert_eq!(
        answer.status(),
        Some(crate::tests::answer::status(&status_name)),
        "{answer:?}"
    );
    assert_eq!(answer.reason(), Some(reason.as_str()));
}

#[then(regex = r#"^the refusal's error info carries (\w+) "([^"]+)"$"#)]
fn error_info(w: &mut Coordinator, name: String, value: String) {
    assert_eq!(
        w.answer().arg(&name),
        Some(value.as_str()),
        "{:?}",
        w.answer()
    );
}

#[then(regex = r#"^nothing is recorded on order "([\w-]+)"$"#)]
fn nothing_recorded_on(w: &mut Coordinator, order: String) {
    let cover = w.cover("order", &order);
    assert!(w.persisted(&cover).is_empty(), "{:?}", w.persisted(&cover));
}

#[then("the coordinator starts an instance")]
fn starts_an_instance(w: &mut Coordinator) {
    let cover = w
        .handled
        .last()
        .and_then(|(b, _)| b.cover.clone())
        .expect("a stream");
    assert!(!w.persisted(&cover).is_empty(), "the instance's events");
}

#[then(
    regex = r"^business code receives an OrderPlaced event at sequence 0, then OrderNoted events at sequences 1 and 2$"
)]
fn receives_in_sequence(w: &mut Coordinator) {
    let delivered = w.business("order").delivered().pop().expect("delivered");
    let pages = delivered.history.map(|e| e.pages).unwrap_or_default();
    let got: Vec<(u64, String)> = pages
        .iter()
        .map(|p| {
            let event = angzarr_embedded::angzarr_router::page_event(p).map(bare);
            (p.sequence, event.unwrap_or_default())
        })
        .collect();
    assert_eq!(
        got,
        [
            (0, "OrderPlaced".to_string()),
            (1, "OrderNoted".to_string()),
            (2, "OrderNoted".to_string())
        ]
    );
}

#[then("each event keeps the sequence and EventMeta it was stored with")]
fn keeps_meta(w: &mut Coordinator) {
    let delivered = w.business("order").delivered().pop().expect("delivered");
    let pages = delivered.history.map(|e| e.pages).unwrap_or_default();
    let stored = w.pages(&w.order());
    for (handed, stored) in pages.iter().zip(&stored) {
        assert_eq!(handed.sequence, stored.sequence);
        assert_eq!(handed.meta, stored.meta);
    }
}

#[then(
    "the sidecar is asked to upcast the command's payload as a lone payload, with no page and no sequence"
)]
fn asked_lone_payload(w: &mut Coordinator) {
    let requests = w.sidecar("order").requests.clone();
    assert!(
        requests
            .iter()
            .any(|r| matches!(r.input, Some(upcast_request::Input::LonePayload(_)))),
        "{requests:?}"
    );
}

#[then("it answers one payload keyed no_sequence")]
fn answers_no_sequence(w: &mut Coordinator) {
    let sidecar = w.sidecar("order");
    let lone = sidecar
        .requests
        .iter()
        .find(|r| matches!(r.input, Some(upcast_request::Input::LonePayload(_))))
        .expect("a lone payload asked");
    let answer = sidecar.answer(lone);
    assert_eq!(answer.pages.len(), 1);
    assert!(matches!(
        answer.pages[0].key,
        Some(upcasted::Key::NoSequence(_))
    ));
}

#[then("the client is answered with an error status")]
fn answered_error(w: &mut Coordinator) {
    assert!(w.answer().status().is_some(), "{:?}", w.answer());
}

#[then(regex = r#"^the "(\w+)" coordinator reports NOT_SERVING$"#)]
fn reports_not_serving(w: &mut Coordinator, domain: String) {
    // The kernel answers what needs the sidecar UNAVAILABLE while it is
    // down: its readiness is the host's to report (C-0681, the host's).
    assert!(w.sidecar(&domain).down);
}
