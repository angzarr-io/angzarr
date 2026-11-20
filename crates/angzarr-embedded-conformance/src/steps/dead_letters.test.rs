//! A policy's failure on an event it reacts to, dead-lettered: what the
//! dead letter carries (the trigger, its one why, its retries, the
//! component), never rendered text. A dead letter is kept by the store and
//! told to the dead-letter log; "published to angzarr.dlq.{domain}" is its
//! stream's domain.

use cucumber::{given, then, when};

use angzarr_embedded::v1::angzarr_dead_letter::Payload;
use angzarr_embedded::v1::{AngzarrDeadLetter, ComponentKind};
use angzarr_embedded::OnRetryable;

use crate::tests::answer::status;
use crate::tests::outcomes::{named, reason_of};
use crate::tests::policy::{bare, internal, locked};
use crate::tests::world::Coordinator;

/// The one dead letter the log was told of.
fn the_letter(w: &Coordinator) -> AngzarrDeadLetter {
    let letters = w.log.letters();
    assert_eq!(letters.len(), 1, "{letters:?}; answered {:?}", w.answers());
    letters[0].clone()
}

#[given(
    regex = r#"^a policy "(\w+)" subscribed to "([\w-]+)" whose handler (?:succeeds|fails (\w+) with reason (\w+), its cause (\w+)(.*))$"#
)]
fn subscribed(
    w: &mut Coordinator,
    name: String,
    input: String,
    grpc: String,
    reason: String,
    cause: String,
    rest: String,
) {
    let script = w.policy(&name, &input, "inventory");
    if reason.is_empty() {
        return;
    }
    let mut error = internal(&reason);
    error.grpc = status(&grpc);
    error.extras = std::iter::once(("cause".to_string(), cause))
        .chain(
            named(rest.split(", and logs").next().unwrap_or_default())
                .into_iter()
                .map(|(name, value)| (format!("cause.{name}"), value)),
        )
        .collect();
    locked(&script).fails = Some(error);
}

#[given(regex = r"^the policy's (?:retry budget|retry cap) is (\d+) attempts?$")]
fn retry_budget(w: &mut Coordinator, attempts: u32) {
    w.retry = OnRetryable {
        max_retries: attempts - 1,
        ..w.retry
    };
}

#[when(regex = r#"^an OrderCreated event for "([\w-]+)" is delivered to the policy$"#)]
async fn delivered_to_policy(w: &mut Coordinator, order: String) {
    w.begin();
    let cover = w.cover("order", &order);
    w.record(&cover, "OrderCreated", None).await;
}

#[then(regex = r"^the policy handler is attempted (once|\d+ times)$")]
fn attempted(w: &mut Coordinator, times: String) {
    let expected: usize = match times.as_str() {
        "once" => 1,
        n => n.trim_end_matches(" times").parse().expect("a count"),
    };
    let sources = locked(&w.policies.last().expect("a policy").script)
        .sources
        .len();
    assert_eq!(sources, expected);
}

#[then(regex = r#"^a dead letter is published to "angzarr\.dlq\.([\w-]+)"$"#)]
fn published(w: &mut Coordinator, domain: String) {
    let letter = the_letter(w);
    let cover = match &letter.payload {
        Some(Payload::RejectedEvents(book)) => book.cover.clone(),
        Some(Payload::RejectedCommand(book)) => book.cover.clone(),
        _ => None,
    };
    assert_eq!(cover.map(|c| c.domain), Some(domain));
    assert!(!w.alerts().is_empty(), "alerted");
}

#[then("no dead letter is published")]
fn none_published(w: &mut Coordinator) {
    assert!(w.log.letters().is_empty(), "{:?}", w.log.letters());
}

#[then(regex = r"^the dead letter carries the (\w+) event as rejected_events$")]
fn carries_event(w: &mut Coordinator, event: String) {
    let Some(Payload::RejectedEvents(book)) = the_letter(w).payload else {
        panic!("rejected events");
    };
    let names: Vec<String> = book
        .pages
        .iter()
        .filter_map(|p| angzarr_embedded::angzarr_router::page_event(p).map(bare))
        .collect();
    assert_eq!(names, [event]);
}

#[then(regex = r"^the dead letter's retry_count is (\d+)$")]
fn retry_count(w: &mut Coordinator, retries: u32) {
    assert_eq!(the_letter(w).retry_count, retries);
}

#[then(
    regex = r#"^the dead letter's source_component is "(\w+)" with source_component_kind (\w+)$"#
)]
fn source_component(w: &mut Coordinator, component: String, kind: String) {
    let letter = the_letter(w);
    assert_eq!(letter.source_component, component);
    let expected = ComponentKind::from_str_name(&kind).expect("a component kind");
    assert_eq!(letter.source_component_kind(), expected);
}

/// The args a step lists after a code: `arg name "value"` (quoted, checked
/// equal), `arg name VALUE` (bare, checked equal) and `arg name` (present).
fn listed_args(text: &str) -> Vec<(String, Option<String>)> {
    let mut args = Vec::new();
    for part in text.split("arg ").skip(1) {
        let part = part
            .trim()
            .trim_end_matches(',')
            .trim_end_matches(" and")
            .trim();
        let mut words = part.splitn(2, ' ');
        let name = words.next().unwrap_or_default().trim_end_matches(',');
        let value = words
            .next()
            .map(|v| v.trim().trim_end_matches(',').trim_end_matches(" and"))
            .map(|v| v.trim().trim_matches('"').to_string())
            .filter(|v| !v.is_empty());
        args.push((name.to_string(), value));
    }
    args
}

/// `diagnostic` has `code` and the args `rest` lists.
pub(crate) fn has(diagnostic: Option<&angzarr_embedded::v1::Diagnostic>, code: &str, rest: &str) {
    let diagnostic = diagnostic.expect("a reason");
    assert_eq!(diagnostic.code, code, "{diagnostic:?}");
    for (name, value) in listed_args(rest) {
        let given = diagnostic
            .args
            .get(&name)
            .unwrap_or_else(|| panic!("no {name} in {diagnostic:?}"));
        if let Some(value) = value {
            assert_eq!(given, &value, "{name} of {diagnostic:?}");
        }
    }
}

#[then(regex = r"^the dead letter's why is the reason arm, code (\w+)(.*)$")]
fn reason_has(w: &mut Coordinator, code: String, rest: String) {
    has(reason_of(&the_letter(w)), &code, &rest);
}

#[then(regex = r#"^no field of the dead letter carries the text "([^"]+)"$"#)]
fn no_text(w: &mut Coordinator, text: String) {
    let letter = format!("{:?}", the_letter(w));
    assert!(!letter.contains(&text), "{letter}");
}
