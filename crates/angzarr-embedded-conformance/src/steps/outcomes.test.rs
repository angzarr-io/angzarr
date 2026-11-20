//! Thens about what came of the inputs: each caller's answer (its status,
//! reason key and args, or its outcome), the events persisted, and what was
//! dead-lettered and alerted.

use std::collections::BTreeMap;

use cucumber::then;

use angzarr_embedded::v1::angzarr_dead_letter::{Payload, Why};
use angzarr_embedded::v1::{AngzarrDeadLetter, DeliveryOutcome};
use angzarr_embedded::{type_name, Handled};

use crate::tests::answer::{status, Answered};
use crate::tests::world::Coordinator;

/// The `name "value"` args a step lists, and a `refusal_type X` (or
/// `refusal_type naming X`) it names, unquoted.
pub(crate) fn named(text: &str) -> BTreeMap<String, String> {
    // `..., its cause ANGZARR_REASON_X with attempts "3"`: the cause's key,
    // and the args after it its own (`cause.attempts`).
    if let Some((before, after)) = text.split_once("cause ANGZARR_REASON_") {
        let (key, rest) = after.split_once(' ').unwrap_or((after, ""));
        let mut args = named(before);
        args.insert("cause".to_string(), format!("ANGZARR_REASON_{key}"));
        for (name, value) in named(&format!(" {rest}")) {
            args.insert(format!("cause.{name}"), value);
        }
        return args;
    }
    let mut args = BTreeMap::new();
    if let Some(after) = text.split("refusal_type ").nth(1) {
        let name = after.trim_start_matches("naming ");
        let name = name
            .split([' ', ','])
            .next()
            .unwrap_or_default()
            .trim_matches('"');
        if !name.is_empty() {
            args.insert("refusal_type".to_string(), name.to_string());
        }
    }
    let mut rest = text;
    while let Some(open) = rest.find(" \"") {
        let name = rest[..open].rsplit([' ', ',']).next().unwrap_or_default();
        let after = &rest[open + 2..];
        let close = after.find('"').expect("a closing quote");
        args.insert(name.to_string(), after[..close].to_string());
        rest = &after[close + 1..];
    }
    args
}

/// The args a step lists, as [`named`] reads them, but its `stream`
/// named as a scenario names one (`stream the stream of order "order-1"`,
/// `stream "table" "t-1"`, `stream wallet "p-1"`, `stream "order-1"` for an
/// order), in the edition the step names, else the scenario's: as the
/// reason's arg spells it (decision 14.65).
pub(crate) fn args_of(w: &Coordinator, text: &str) -> BTreeMap<String, String> {
    let Some((before, after)) = text.split_once("stream ") else {
        return named(text);
    };
    let quoted: Vec<&str> = after.split('"').collect();
    // After the stream phrase: what follows its last quoted word.
    let (domain, root, rest) = if let Some(order) = after.strip_prefix("the stream of order \"") {
        let (root, rest) = order.split_once('"').expect("a closing quote");
        ("order".to_string(), root.to_string(), rest)
    } else if after.starts_with('"') && quoted.get(2).is_some_and(|gap| *gap == " ") {
        let rest = after.splitn(5, '"').nth(4).unwrap_or_default();
        (quoted[1].to_string(), quoted[3].to_string(), rest)
    } else if after.starts_with('"') {
        let rest = after.splitn(3, '"').nth(2).unwrap_or_default();
        ("order".to_string(), quoted[1].to_string(), rest)
    } else if let Some((domain, quote)) = after.split_once(" \"") {
        if domain.contains(' ') {
            return named(text);
        }
        let (root, rest) = quote.split_once('"').expect("a closing quote");
        (domain.to_string(), root.to_string(), rest)
    } else {
        return named(text);
    };
    let edition = edition_named(rest).or_else(|| w.edition.clone());
    let cover = angzarr_embedded::v1::Cover {
        edition,
        ..w.cover(&domain, &root)
    };
    let mut args = named(&format!("{before}{rest}"));
    args.insert("stream".to_string(), crate::tests::streams::text(&cover));
    args
}

/// The edition `edition "E"` a step's text names.
fn edition_named(text: &str) -> Option<String> {
    text.split("edition \"")
        .nth(1)
        .and_then(|e| e.split('"').next())
        .map(str::to_string)
}

/// Whether a step says the input persisted nothing.
fn says_nothing_persisted(text: &str) -> bool {
    text.contains("nothing is persisted") || text.contains("persists nothing")
}

/// The last answer is a refusal under `status_name` with `reason` and at
/// least `args`.
fn assert_refused(
    w: &Coordinator,
    status_name: &str,
    reason: &str,
    args: &BTreeMap<String, String>,
) {
    let answer = w.answer();
    assert_eq!(answer.status(), Some(status(status_name)), "{answer:?}");
    assert_eq!(answer.reason(), Some(reason), "{answer:?}");
    for (name, value) in args {
        let given = answer.arg(name).unwrap_or_default();
        // A type is named by its full or its bare name.
        assert!(
            given == value || given.ends_with(&format!(".{value}")),
            "{name} of {answer:?}"
        );
    }
}

/// A reaction refused with a typed refusal: what reaches the policy's
/// Translate (decision 14.95), the refusal under `status_name` whose typed
/// object is the `refusal_type` named. The reaction's sender is the
/// policy's coordinator, not the scenario's caller.
fn reaction_refused(w: &Coordinator, status_name: &str, args: &BTreeMap<String, String>) {
    let def = w.policies.last().expect("a policy");
    let script = crate::tests::policy::locked(&def.script);
    let [request] = &script.refusals[..] else {
        panic!("one refusal translated: {:?}", script.refusals);
    };
    let refusal = request.refusal.clone().expect("a refusal");
    let grpc = format!("REFUSAL_STATUS_{status_name}");
    assert_eq!(refusal.status().as_str_name(), grpc, "{refusal:?}");
    let typed = refusal.typed.expect("its typed object");
    let wanted = args.get("refusal_type").expect("a refusal_type");
    assert!(
        type_name(&typed.type_url).ends_with(wanted.as_str()),
        "{typed:?}"
    );
}

fn assert_nothing_persisted(w: &Coordinator) {
    let cover = w.order();
    assert!(w.persisted(&cover).is_empty(), "{:?}", w.persisted(&cover));
}

/// The types of the order stream's events persisted since the first when.
fn persisted_types(w: &Coordinator) -> Vec<(u64, String)> {
    w.persisted(&w.order())
        .into_iter()
        .map(|page| {
            let url = angzarr_embedded::angzarr_router::page_event(&page)
                .map(|event| event.type_url.clone())
                .unwrap_or_default();
            let name = type_name(&url)
                .rsplit('.')
                .next()
                .unwrap_or_default()
                .to_string();
            (page.sequence, name)
        })
        .collect()
}

#[then(regex = r"^the (?:command|reaction) succeeds$")]
fn succeeds(w: &mut Coordinator) {
    assert_eq!(w.answer(), Answered::Ok(DeliveryOutcome::Applied));
}

#[then("both commands succeed")]
fn both_succeed(w: &mut Coordinator) {
    let answers = w.answers();
    assert_eq!(answers.len(), 2);
    assert!(
        answers
            .iter()
            .all(|a| *a == Answered::Ok(DeliveryOutcome::Applied)),
        "{answers:?}"
    );
}

#[then("no events are persisted and no snapshot is stored")]
fn nothing_persisted_no_snapshot(w: &mut Coordinator) {
    assert_nothing_persisted(w);
    let cover = w.order();
    assert!(
        w.stored_snapshot(&cover).is_none(),
        "{:?}",
        w.stored_snapshot(&cover)
    );
}

#[then(
    regex = r"^(?:no events are persisted|nothing is persisted|the (?:command|fact|step|input|reaction) persists nothing)$"
)]
fn nothing_persisted(w: &mut Coordinator) {
    assert_nothing_persisted(w);
}

#[then(regex = r"^(?:the command succeeds and )?an? (\w+) event is persisted at sequence (\d+)$")]
fn persisted_at(w: &mut Coordinator, name: String, sequence: u64) {
    assert!(
        persisted_types(w).contains(&(sequence, name)),
        "{:?}",
        persisted_types(w)
    );
}

#[then(regex = r"^(\w+) events are persisted at sequences (\d+) and (\d+)$")]
fn persisted_at_both(w: &mut Coordinator, name: String, first: u64, second: u64) {
    assert_eq!(
        persisted_types(w),
        vec![(first, name.clone()), (second, name)]
    );
}

#[then(regex = r"^exactly one (\w+) event is persisted$")]
fn exactly_one(w: &mut Coordinator, name: String) {
    let types = persisted_types(w);
    assert_eq!(
        types.iter().filter(|(_, n)| *n == name).count(),
        1,
        "{types:?}"
    );
}

#[then(regex = r"^the command fails with (\w+) status$")]
fn fails_with_status(w: &mut Coordinator, status_name: String) {
    assert_eq!(
        w.answer().status(),
        Some(status(&status_name)),
        "{:?}",
        w.answer()
    );
}

#[then(regex = r"^the command fails (\w+) and nothing is persisted$")]
fn fails_persisting_nothing(w: &mut Coordinator, status_name: String) {
    fails_with_status(w, status_name);
    assert_nothing_persisted(w);
}

#[then(regex = r"^the (command|reaction) fails (\w+) with reason (\w+)(.*)$")]
fn fails_with_reason(
    w: &mut Coordinator,
    input: String,
    status_name: String,
    reason: String,
    rest: String,
) {
    match input.as_str() {
        "reaction" if reason == "ANGZARR_REASON_COMMAND_REFUSED" => {
            reaction_refused(w, &status_name, &args_of(w, &rest));
        }
        _ => assert_refused(w, &status_name, &reason, &args_of(w, &rest)),
    }
    if says_nothing_persisted(&rest) {
        assert_nothing_persisted(w);
    }
}

#[then(regex = r"^the (?:caller|client|sender) is answered (\w+) with reason (\w+)(.*)$")]
fn answered_with_reason(w: &mut Coordinator, status_name: String, reason: String, rest: String) {
    assert_refused(w, &status_name, &reason, &args_of(w, &rest));
    if rest.contains("with no wait before it") {
        assert!(w.waits.waited().is_empty(), "{:?}", w.waits.waited());
    }
}

#[then(regex = r"^the (?:command|caller|client) is never answered (\w+)(?: itself)?$")]
fn never_answered(w: &mut Coordinator, reason: String) {
    let answers = w.answers();
    assert!(
        answers.iter().all(|a| a.reason() != Some(reason.as_str())),
        "{answers:?}"
    );
}

#[then(
    regex = r"^the caller is answered (?:with delivery )?DUPLICATE (?:with|and) the (\w+) event at sequence (\d+)$"
)]
fn duplicate(w: &mut Coordinator, name: String, sequence: u64) {
    let (_, result) = w.handled.last().expect("an input was handled");
    let Ok(Handled::Duplicate(book, _)) = result else {
        panic!(
            "a duplicate, not {:?}",
            result.as_ref().map(Handled::delivery)
        );
    };
    let pages: Vec<_> = book
        .pages
        .iter()
        .map(|p| {
            (
                p.sequence,
                angzarr_embedded::angzarr_router::page_event(p)
                    .map(|a| a.type_url.clone())
                    .unwrap_or_default(),
            )
        })
        .collect();
    assert_eq!(pages.len(), 1, "{pages:?}");
    assert_eq!(pages[0].0, sequence);
    assert!(pages[0].1.ends_with(&format!(".{name}")), "{pages:?}");
}

#[then(
    regex = r"^the second delivery (?:returns|answers delivery DUPLICATE with) the events of the first$"
)]
fn second_returns_first(w: &mut Coordinator) {
    let [(_, first), (_, second)] = &w.handled[..] else {
        panic!("two deliveries, not {}", w.handled.len());
    };
    let first = first.as_ref().expect("the first applied").books();
    let Ok(Handled::Duplicate(book, _)) = second else {
        panic!("the second a duplicate");
    };
    let first_pages: Vec<_> = first.iter().flat_map(|b| b.pages.clone()).collect();
    assert_eq!(first_pages, book.pages);
}

/// The dead letters the log was given.
fn letters(w: &Coordinator) -> Vec<AngzarrDeadLetter> {
    w.log.letters()
}

#[then(regex = r"^nothing is dead-lettered(?: and no alert is raised)?$")]
fn nothing_dead_lettered(w: &mut Coordinator) {
    assert!(letters(w).is_empty(), "{:?}", letters(w));
    assert!(w.alerts().is_empty());
}

/// `the fact is dead-lettered with reason R, its args, and an alert is
/// raised`; `... as rejected_command`; `..., its reason the handler
/// failure's key` (the failure business code answered,
/// `TIMEOUT_HANDLER_FAILED` in the process manager's script).
#[then(
    regex = r"^the (?:command|fact|reaction) is dead-lettered(.*?)(?:, and an alert is raised| and alerted)$"
)]
fn dead_lettered(w: &mut Coordinator, text: String) {
    let letters = letters(w);
    let reason = text
        .split("with reason ")
        .nth(1)
        .and_then(|r| r.split([' ', ',']).next())
        .map(str::to_string);
    let letter = match &reason {
        Some(reason) => letters
            .iter()
            .find(|l| reason_of(l).is_some_and(|r| r.code == *reason))
            .unwrap_or_else(|| panic!("no dead letter of {reason}: {letters:?}")),
        None => letters.last().unwrap_or_else(|| panic!("a dead letter")),
    };
    if text.contains("as rejected_reaction") {
        assert!(
            matches!(letter.payload, Some(Payload::RejectedReaction(_))),
            "{letter:?}"
        );
    }
    if text.contains("as rejected_command") {
        assert!(
            matches!(letter.payload, Some(Payload::RejectedCommand(_))),
            "{letter:?}"
        );
    }
    let why = reason_of(letter).expect("a reason");
    if text.contains("its reason the handler failure's key") {
        assert!(why.code.ends_with("TIMEOUT_HANDLER_FAILED"), "{letter:?}");
    }
    let rest = match &reason {
        Some(reason) => text
            .split(reason.as_str())
            .nth(1)
            .unwrap_or_default()
            .to_string(),
        None => text.clone(),
    };
    for (name, value) in named(&rest) {
        let arg = why
            .args
            .get(&name)
            .unwrap_or_else(|| panic!("no {name} in {letter:?}"));
        // A type is named by its bare name: `AddItem` for orders.v1.AddItem.
        assert!(
            arg == &value || arg.ends_with(&format!(".{value}")),
            "{name} of {letter:?}"
        );
    }
    assert!(!w.alerts().is_empty(), "an alert");
}

#[then(
    regex = r#"^"([\w-]+)"'s router answers the fact INTERNAL with reason (\w+), naming its type_url$"#
)]
fn router_answers_fact(w: &mut Coordinator, _domain: String, reason: String) {
    // The router's answer is what the kernel dead-letters the fact with.
    let letters = letters(w);
    let letter = letters
        .iter()
        .find(|l| reason_of(l).is_some_and(|r| r.code == reason))
        .unwrap_or_else(|| panic!("no answer of {reason}: {letters:?}"));
    let why = reason_of(letter).expect("a reason");
    assert!(
        why.args.get("type_url").is_some_and(|t| !t.is_empty()),
        "{letter:?}"
    );
}

/// A dead letter's why, if it is a reason (decision 14.101).
pub(crate) fn reason_of(letter: &AngzarrDeadLetter) -> Option<&angzarr_embedded::v1::Diagnostic> {
    match &letter.why {
        Some(Why::Reason(reason)) => Some(reason),
        _ => None,
    }
}

/// The last answer's typed refusal, an `ExpectedSequenceMismatch`.
fn mismatch(w: &Coordinator) -> angzarr_embedded::v1::ExpectedSequenceMismatch {
    use prost::Message as _;
    let answer = w.answer();
    let refusal = answer.refusal().expect("a typed refusal");
    assert!(
        refusal
            .type_url
            .ends_with("io.angzarr.v1.ExpectedSequenceMismatch"),
        "{refusal:?}"
    );
    angzarr_embedded::v1::ExpectedSequenceMismatch::decode(&refusal.value[..]).expect("decodes")
}

#[then(
    regex = r"^the status details hold an ExpectedSequenceMismatch with expected (\d+) and head (\d+)$"
)]
fn status_details_mismatch(w: &mut Coordinator, expected: u64, head: u64) {
    let refusal = mismatch(w);
    assert_eq!((refusal.expected, refusal.head), (expected, head));
}

#[then(regex = r"^the (?:command|reaction) is never refused ExpectedSequenceMismatch$")]
fn never_mismatched(w: &mut Coordinator) {
    for answer in w.answers() {
        let typed = answer
            .refusal()
            .map(|r| r.type_url.clone())
            .unwrap_or_default();
        assert!(!typed.ends_with("ExpectedSequenceMismatch"), "{answer:?}");
    }
}

#[then("the command is never answered under any other key")]
fn only_this_key(w: &mut Coordinator) {
    let answers = w.answers();
    assert!(
        answers
            .iter()
            .all(|a| a.reason() == Some("ANGZARR_REASON_COMMAND_REFUSED")),
        "{answers:?}"
    );
}
