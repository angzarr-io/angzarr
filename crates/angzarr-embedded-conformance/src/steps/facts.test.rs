//! Facts injected into an aggregate: sequenced like any input, recorded by
//! a declared handler and never refused, deduplicated by their external
//! id; and when an input is accepted, the instant stamped on its delivery.

use cucumber::{given, then, when};

use angzarr_embedded::v1::event_meta::Causation;
use angzarr_embedded::v1::DeliveryOutcome;
use angzarr_embedded::{Handled, Timestamp};

use crate::tests::answer::{status, Answered};
use crate::tests::policy::bare;
use crate::tests::world::Coordinator;

/// An instant as a scenario writes it, `2026-10-28T12:00:00Z`, as
/// seconds since the epoch (UTC, whole seconds).
fn instant(text: &str) -> Timestamp {
    let number = |range: std::ops::Range<usize>| -> i64 {
        text[range]
            .parse()
            .unwrap_or_else(|_| panic!("no instant {text}"))
    };
    let (year, month, day) = (number(0..4), number(5..7), number(8..10));
    let (hour, minute, second) = (number(11..13), number(14..16), number(17..19));
    // Days from the civil date (Howard Hinnant's days_from_civil).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Timestamp {
        seconds: days * 86_400 + hour * 3_600 + minute * 60 + second,
        nanos: 0,
    }
}

#[given(regex = r#"^an "order" aggregate with (\d+) existing events$"#)]
fn existing_events(w: &mut Coordinator, count: usize) {
    let cover = w.order();
    w.write(&cover, &vec!["ItemAdded"; count]);
}

#[given(regex = r#"^an order "([\w-]+)" that has been cancelled$"#)]
fn cancelled(w: &mut Coordinator, order: String) {
    w.speak_of(&order);
    let cover = w.order();
    w.write(&cover, &["OrderCreated", "OrderCancelled"]);
}

#[given(regex = r#"^a fact with external_id "([^"]+)"$"#)]
fn a_fact(w: &mut Coordinator, external_id: String) {
    let cover = w.order();
    let book = w.fact(&cover, "ShipmentDispatched", &external_id);
    w.pending.push(book);
}

#[when(
    regex = r#"^an? (\w+) fact(?: with external_id "([^"]+)")? is injected(?: into the "order" aggregate(?: "([\w-]+)")?)?$"#
)]
async fn injected(w: &mut Coordinator, name: String, external_id: String, order: String) {
    if !order.is_empty() {
        w.speak_of(&order);
    }
    w.begin();
    let cover = w.order();
    let external_id = if external_id.is_empty() {
        format!("{name}-1")
    } else {
        external_id
    };
    let book = w.fact(&cover, &name, &external_id);
    w.handle(book).await;
}

#[when("the same fact is injected twice")]
async fn injected_twice(w: &mut Coordinator) {
    w.begin();
    let book = w.pending.pop().expect("a fact");
    w.handle(book.clone()).await;
    w.handle(book).await;
}

#[then(
    regex = r"^the fact is persisted (?:with sequence number (\d+)|at the order's next sequence)$"
)]
fn fact_persisted(w: &mut Coordinator, sequence: String) {
    let cover = w.order();
    let persisted = w.persisted(&cover);
    assert_eq!(persisted.len(), 1, "{persisted:?}");
    let next = w.pages(&cover).len() as u64 - 1;
    let expected = sequence.parse().unwrap_or(next);
    assert_eq!(persisted[0].sequence, expected);
}

#[then(regex = r"^subsequent events continue from sequence (\d+)$")]
fn continue_from(w: &mut Coordinator, head: usize) {
    let cover = w.order();
    assert_eq!(w.pages(&cover).len(), head);
}

#[then("no rejection is produced")]
fn no_rejection(w: &mut Coordinator) {
    assert_eq!(w.answer(), Answered::Ok(DeliveryOutcome::Applied));
}

#[then(regex = r"^the injection fails with (\w+) and angzarr error code (\w+)$")]
fn injection_fails(w: &mut Coordinator, status_name: String, code: String) {
    let answer = w.answer();
    assert_eq!(answer.status(), Some(status(&status_name)), "{answer:?}");
    assert_eq!(answer.reason(), Some(code.as_str()), "{answer:?}");
}

#[then(regex = r#"^the "order" aggregate still has (\d+) events$"#)]
fn still_has(w: &mut Coordinator, count: usize) {
    let cover = w.order();
    assert_eq!(w.pages(&cover).len(), count);
}

#[then(regex = r#"^external_id "([^"]+)" is not recorded as processed$"#)]
fn not_processed(w: &mut Coordinator, external_id: String) {
    let cover = w.order();
    for page in w.pages(&cover) {
        let origin = match page.meta.and_then(|m| m.causation) {
            Some(Causation::Fact(cause)) => cause.origin.map(|f| f.external_id),
            _ => None,
        };
        assert_ne!(origin.as_deref(), Some(external_id.as_str()));
    }
}

#[then("only one event is stored in the aggregate")]
fn only_one(w: &mut Coordinator) {
    let cover = w.order();
    assert_eq!(w.persisted(&cover).len(), 1);
}

#[then(
    regex = r"^the second injection (?:reports already_processed|is answered with delivery DUPLICATE and the stored events)$"
)]
fn already_processed(w: &mut Coordinator) {
    assert!(
        matches!(w.handled.last(), Some((_, Ok(Handled::Duplicate(..))))),
        "{:?}",
        w.answer()
    );
}

#[given(regex = r#"^the coordinator's clock reads "([^"]+)"$"#)]
fn clock_reads(w: &mut Coordinator, at: String) {
    w.clock = Some(instant(&at));
}

#[when(regex = r#"^an? (\w+) (command|fact) is accepted for an "order" aggregate$"#)]
async fn accepted_for_order(w: &mut Coordinator, name: String, kind: String) {
    w.begin();
    let cover = w.order();
    let book = match kind.as_str() {
        "fact" => {
            w.business("order").declare_fact(&name);
            w.fact(&cover, &name, "delivery-1")
        }
        _ => w.command(&cover, &name),
    };
    w.handle(book).await;
}

#[then(
    regex = r#"^the (?:ContextualCommand|HandleRequest) delivered to business code has recorded_at "([^"]+)"$"#
)]
fn delivered_recorded_at(w: &mut Coordinator, at: String) {
    let delivered = w.business("order").delivered().pop().expect("delivered");
    assert_eq!(delivered.recorded_at, Some(instant(&at)));
}

#[given(
    regex = r"^a stream with events at seq 3, 4, 6 and 7 and a usable snapshot at sequence (\d+)$"
)]
fn stream_with_snapshot(w: &mut Coordinator, snapshot: u64) {
    let cover = w.order();
    w.write(&cover, &["ItemAdded"; 8]);
    let (state_type, version) = {
        let order = w.domain("order");
        (
            format!("/{}", order.state_type),
            order.rules_version.clone(),
        )
    };
    w.snapshot(&cover, snapshot, &state_type, &version);
}

#[when("the coordinator delivers a command to the stream")]
async fn delivers_a_command(w: &mut Coordinator) {
    w.begin();
    let cover = w.order();
    let book = w.command(&cover, "AddItem");
    w.handle(book).await;
}

#[then(regex = r"^the delivered EventBook carries the snapshot at sequence (\d+)$")]
fn carries_snapshot(w: &mut Coordinator, sequence: u64) {
    let delivered = w.business("order").delivered().pop().expect("delivered");
    let snapshot = delivered
        .history
        .and_then(|e| e.snapshot)
        .expect("a snapshot");
    assert_eq!(snapshot.sequence, sequence);
}

#[then(regex = r"^it carries only the events at seq (\d+) and (\d+)$")]
fn carries_only(w: &mut Coordinator, first: u64, second: u64) {
    let delivered = w.business("order").delivered().pop().expect("delivered");
    let sequences: Vec<u64> = delivered
        .history
        .map_or(Vec::new(), |e| e.pages.iter().map(|p| p.sequence).collect());
    assert_eq!(sequences, [first, second]);
}

/// Folding is business code's; the coordinator's part is that both events
/// are delivered after the snapshot, which the step before checks.
#[then("business code folds both of them onto the snapshot state")]
fn folds_both(w: &mut Coordinator) {
    let delivered = w.business("order").delivered().pop().expect("delivered");
    let events = delivered.history.expect("its history");
    assert!(
        events.snapshot.is_some()
            && events.pages.iter().all(|p| {
                angzarr_embedded::angzarr_router::page_event(p)
                    .is_some_and(|a| bare(a) == "ItemAdded")
            })
    );
}
