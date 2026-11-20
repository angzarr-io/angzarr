//! The rules version: registered with a domain, reported with every
//! answer, stamped on what the answer produced, and the rule by which a
//! stored snapshot is offered with a delivery.

use cucumber::{given, then, when};

use crate::tests::answer::status;
use crate::tests::policy::Root;
use crate::tests::world::Coordinator;

/// The order stream `name`, from here on the one the scenario speaks of.
fn order_named(w: &mut Coordinator, name: &str) -> angzarr_embedded::v1::Cover {
    w.speak_of(name);
    w.order()
}

/// `cover` holding events up to `last`: the missing ones written.
fn events_up_to(w: &mut Coordinator, cover: &angzarr_embedded::v1::Cover, last: u64) {
    let head = w.pages(cover).len() as u64;
    let missing: Vec<&str> = (head..=last).map(|_| "ItemAdded").collect();
    w.write(cover, &missing);
}

#[given(
    regex = r#"^an "(\w+)" aggregate registered with state type "([^"]+)" and rules version "([^"]*)"$"#
)]
fn registered_with(w: &mut Coordinator, domain: String, state_type: String, version: String) {
    w.domain(&domain).state_type = state_type;
    w.registered_at(&domain, &version);
}

#[given(regex = r#"^a process manager "(\w+)" registered with rules version "([^"]*)"$"#)]
fn process_manager_at(w: &mut Coordinator, name: String, version: String) {
    let domain = name.to_lowercase();
    w.process_manager(&domain, &["StartFulfillment"]);
    w.registered_at(&domain, &version);
}

#[given(
    regex = r#"^order "([\w-]+)" has a snapshot at sequence (\d+) of state type "([^"]+)" recorded under rules version "([^"]*)"$"#
)]
fn snapshot_of(
    w: &mut Coordinator,
    order: String,
    sequence: u64,
    state_type: String,
    version: String,
) {
    let version = w.resolve(&version);
    let cover = order_named(w, &order);
    events_up_to(w, &cover, sequence);
    w.snapshot(&cover, sequence, &state_type, &version);
}

#[given(regex = r#"^order "([\w-]+)" has events at sequences (\d+) (?:and|to) (\d+)$"#)]
fn events_at(w: &mut Coordinator, order: String, first: u64, last: u64) {
    let cover = order_named(w, &order);
    assert!(
        w.pages(&cover).len() as u64 >= first,
        "no gap before sequence {first}"
    );
    events_up_to(w, &cover, last);
}

#[given(
    regex = r#"^two instances of the "order" business binary, at rules versions "([^"]+)" and "([^"]+)"$"#
)]
fn two_instances(_w: &mut Coordinator, _first: String, _second: String) {
    // One process at a time is the instance: each is registered, as it is
    // described, when it decides (below).
}

#[given(r#"the "order" domain has the default snapshot storage policy"#)]
fn default_storage(_w: &mut Coordinator) {}

#[when(
    regex = r#"^the coordinator delivers (?:an|the next) AddItem command for order "([\w-]+)"$"#
)]
async fn delivers_add_item(w: &mut Coordinator, order: String) {
    let cover = order_named(w, &order);
    w.begin();
    let book = w.command(&cover, "AddItem");
    w.handle(book).await;
}

#[when(regex = r#"^the instance at "([^"]+)" decides an AddItem command for order "([\w-]+)"$"#)]
async fn instance_decides(w: &mut Coordinator, version: String, order: String) {
    w.registered_at("order", &version);
    delivers_add_item(w, order).await;
}

/// What business code answers: the events of `answered` ("ItemAdded and
/// ShippingQuoted events", "an ItemAdded event and a snapshot", "an
/// ItemAdded event and a state of type "/orders.v1.LegacyOrderState""),
/// reporting `version`.
fn answering(w: &mut Coordinator, domain: &str, answered: &str, version: &str) {
    let state_type = answered
        .split(" and ")
        .find(|part| part.starts_with("a state of type"))
        .and_then(|part| part.split('"').nth(1))
        .map(str::to_string);
    let names: Vec<String> = answered
        .split(" and ")
        .filter(|part| !part.contains("snapshot") && !part.starts_with("a state of type"))
        .map(|part| {
            let part = part.trim_start_matches("an ").trim_start_matches("a ");
            part.split(' ').next().unwrap_or_default().to_string()
        })
        .collect();
    let version = version.to_string();
    w.business(domain).every(|a| {
        a.events = Some(names);
        a.rules_version = Some(version);
        if state_type.is_some() {
            a.state_type = state_type;
        }
    });
}

/// Every caller waits for the whole chain: one sent "awaiting the
/// decision", "awaiting the projections" or "without waiting" is handled
/// alike (the last is tagged: there is no asynchronous send); one "by a
/// policy" is a policy's reaction to a payment.
#[when(
    regex = r#"^business code answers an AddItem command (?:(?:for order "order-1" )?sent ([\w ]+?) )?with (.+?), reporting rules version "([^"]*)"$"#
)]
async fn answers_add_item(w: &mut Coordinator, how: String, answered: String, version: String) {
    answering(w, "order", &answered, &version);
    if how != "by a policy" {
        return delivers_add_item(w, "order-1".to_string()).await;
    }
    let to = Root::Named("order-1".to_string());
    crate::tests::policies::reactor(w, "order", "AddItem", to);
    w.begin();
    let book = crate::tests::policies::payment_received(w);
    w.handle(book).await;
}

#[when(
    regex = r#"^a policy's (\w+) command is answered by the process manager with a (\w+) event, reporting rules version "([^"]*)"$"#
)]
async fn policy_command_answered(
    w: &mut Coordinator,
    command: String,
    event: String,
    version: String,
) {
    answering(w, "fulfillment", &format!("a {event} event"), &version);
    crate::tests::policies::reactor(w, "fulfillment", &command, Root::TriggerCorrelation);
    w.begin();
    let book = crate::tests::policies::payment_received(w);
    w.handle(book).await;
}

#[when(
    regex = r#"^business code answers an AddItem command for order "([\w-]+)" with an ItemAdded event at sequence (\d+), reporting rules version "([^"]*)"$"#
)]
async fn answers_at(w: &mut Coordinator, order: String, sequence: u64, version: String) {
    let cover = order_named(w, &order);
    if sequence > 0 {
        events_up_to(w, &cover, sequence - 1);
    }
    answering(w, "order", "an ItemAdded event", &version);
    delivers_add_item(w, order).await;
}

#[when(regex = r#"^business code answers three AddItem commands for order "([\w-]+)"$"#)]
async fn answers_three(w: &mut Coordinator, order: String) {
    for _ in 0..3 {
        delivers_add_item(w, order.clone()).await;
    }
}

#[when(
    regex = r#"^the process manager handles a (\w+) command with a (\w+) event, reporting rules version "([^"]*)"$"#
)]
async fn process_manager_handles(
    w: &mut Coordinator,
    command: String,
    event: String,
    version: String,
) {
    answering(w, "fulfillment", &format!("a {event} event"), &version);
    w.begin();
    let cover = w.cover("fulfillment", "0192a6b0-0000-7000-8000-000000000001");
    let book = w.command(&cover, &command);
    w.handle(book).await;
}

/// The `nth` (from 1) persisted event named `name`, on any stream an input
/// went to, with its meta.
fn nth_event(w: &Coordinator, name: &str, nth: usize) -> angzarr_embedded::v1::EventPage {
    let mut covers: Vec<_> = w
        .handled
        .iter()
        .filter_map(|(book, _)| book.cover.clone())
        .collect();
    covers.dedup();
    covers
        .iter()
        .flat_map(|cover| w.persisted(cover))
        .filter(|page| {
            angzarr_embedded::angzarr_router::page_event(page)
                .is_some_and(|p| p.type_url.ends_with(&format!(".{name}")))
        })
        .nth(nth - 1)
        .unwrap_or_else(|| panic!("no {name} event {nth}"))
}

#[then(regex = r#"^the (?:(first|second) )?(\w+) event records rules version "([^"]*)"$"#)]
fn event_records_version(w: &mut Coordinator, ordinal: String, name: String, version: String) {
    let nth = match ordinal.as_str() {
        "second" => 2,
        _ => 1,
    };
    let meta = nth_event(w, &name, nth).meta.expect("stamped");
    assert_eq!(meta.rules_version, version);
}

#[then(regex = r#"^the snapshot records (rules version|state type) "([^"]*)"$"#)]
fn snapshot_records(w: &mut Coordinator, what: String, value: String) {
    let cover = w.order();
    let snapshot = w.stored_snapshot(&cover).expect("a snapshot stored");
    match what.as_str() {
        "rules version" => assert_eq!(snapshot.rules_version, value),
        _ => assert_eq!(snapshot.state.expect("a state").type_url, value),
    }
}

#[then(regex = r"^business code receives the snapshot at sequence (\d+)$")]
fn receives_snapshot(w: &mut Coordinator, sequence: u64) {
    let delivered = w.business("order").delivered().pop().expect("delivered");
    let snapshot = delivered
        .history
        .and_then(|e| e.snapshot)
        .expect("a snapshot");
    assert_eq!(snapshot.sequence, sequence);
}

#[then(regex = r"^business code receives the events at sequences (\d+) (?:and|to) (\d+)$")]
fn receives_events(w: &mut Coordinator, first: u64, last: u64) {
    let delivered = w.business("order").delivered().pop().expect("delivered");
    let sequences: Vec<u64> = delivered
        .history
        .map_or(Vec::new(), |e| e.pages.iter().map(|p| p.sequence).collect());
    assert_eq!(sequences, (first..=last).collect::<Vec<_>>());
}

#[then(regex = r"^the snapshot is (offered|not offered)$")]
fn snapshot_offered(w: &mut Coordinator, offered: String) {
    let delivered = w.business("order").delivered().pop().expect("delivered");
    let snapshot = delivered.history.and_then(|e| e.snapshot);
    assert_eq!(snapshot.is_some(), offered == "offered", "{snapshot:?}");
}

#[then("the caller is answered with INTERNAL status")]
fn answered_internal(w: &mut Coordinator) {
    assert_eq!(
        w.answer().status(),
        Some(status("INTERNAL")),
        "{:?}",
        w.answer()
    );
}

#[then("the command is dead-lettered and an alert is raised")]
fn dead_lettered(w: &mut Coordinator) {
    assert!(!w.log.letters().is_empty(), "a dead letter");
    assert!(!w.alerts().is_empty(), "an alert");
}

#[then(regex = r#"^no (\w+) event is persisted$"#)]
fn no_event_of(w: &mut Coordinator, name: String) {
    let mut covers: Vec<_> = w
        .handled
        .iter()
        .filter_map(|(book, _)| book.cover.clone())
        .collect();
    covers.dedup();
    let suffix = format!(".{name}");
    assert!(covers
        .iter()
        .flat_map(|cover| w.persisted(cover))
        .all(|page| {
            !angzarr_embedded::angzarr_router::page_event(&page)
                .is_some_and(|p| p.type_url.ends_with(&suffix))
        }));
}

#[then(
    regex = r#"^a snapshot of order "([\w-]+)" is stored at sequence (\d+) with rules version "([^"]*)"$"#
)]
fn snapshot_stored(w: &mut Coordinator, order: String, sequence: u64, version: String) {
    let cover = order_named(w, &order);
    let snapshot = w.stored_snapshot(&cover).expect("a snapshot stored");
    assert_eq!(
        (snapshot.sequence, snapshot.rules_version.as_str()),
        (sequence, version.as_str())
    );
}

/// The second delivery is offered the state the first answer returned,
/// byte for byte.
#[then("business code receives that snapshot's stored bytes unchanged")]
fn snapshot_bytes(w: &mut Coordinator) {
    let business = w.business("order");
    let delivered = business.delivered().pop().expect("delivered");
    let offered = delivered
        .history
        .and_then(|e| e.snapshot)
        .expect("a snapshot");
    let returned = business.returned();
    assert_eq!(returned.len(), 2);
    assert_eq!(offered.state.as_ref(), Some(&returned[0]));
}

#[then(regex = r#"^order "([\w-]+)" has exactly one stored snapshot$"#)]
fn one_snapshot(w: &mut Coordinator, order: String) {
    let cover = order_named(w, &order);
    assert!(
        w.stored_snapshot(&cover).is_some(),
        "the store keeps one snapshot per stream"
    );
}

#[then("it is the state returned with the third answer")]
fn third_answer_state(w: &mut Coordinator) {
    let cover = w.order();
    let snapshot = w.stored_snapshot(&cover).expect("a snapshot stored");
    let returned = w.business("order").returned();
    assert_eq!(returned.len(), 3);
    assert_eq!(snapshot.state.as_ref(), Some(&returned[2]));
}
