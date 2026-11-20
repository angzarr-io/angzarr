//! Givens and thens about a policy's facts (policy_facts.feature): what
//! its destination recorded of them, the identity the kernel stamped on
//! each, and what was dead-lettered when one could not be recorded or was
//! of an arm its destination does not declare.

use cucumber::{given, then};

use angzarr_embedded::angzarr_router::hpb::HandleRequest;
use angzarr_embedded::ids::policy_command_id;
use angzarr_embedded::v1::angzarr_dead_letter::Payload;
use angzarr_embedded::v1::{
    command_page, reaction_delivery, AngzarrDeadLetter, CommandPage, EventPage, EventRef,
};

use crate::tests::business::{typed_refusal, Rule};
use crate::tests::outcomes::reason_of;
use crate::tests::policies::sends_as_fact;
use crate::tests::policy::bare;
use crate::tests::world::{cause_of, origin_of, Coordinator};

/// The page a delivery carried, with its causation.
fn page(delivered: &HandleRequest) -> (CommandPage, Option<EventRef>) {
    let page = delivered
        .input
        .as_ref()
        .and_then(|b| b.page.clone())
        .expect("a page");
    (page, delivered.causation.clone())
}

/// A page's payload type, by bare name, and whether it is a fact.
fn input(page: &CommandPage) -> (String, bool) {
    match &page.body {
        Some(command_page::Body::Command(c)) => {
            (c.payload.as_ref().map(bare).unwrap_or_default(), false)
        }
        Some(command_page::Body::Fact(f)) => {
            (f.payload.as_ref().map(bare).unwrap_or_default(), true)
        }
        None => (String::new(), false),
    }
}

/// The one delivery to `domain`'s business since the first when that is a
/// fact (`fact`) or a command of type `name` (any type, `name` empty).
fn delivered(
    w: &mut Coordinator,
    domain: &str,
    name: &str,
    fact: bool,
) -> (CommandPage, Option<EventRef>) {
    let found: Vec<(CommandPage, Option<EventRef>)> = w
        .business(domain)
        .delivered()
        .iter()
        .map(page)
        .filter(|(p, _)| {
            let (kind, is_fact) = input(p);
            is_fact == fact && (name.is_empty() || kind == name)
        })
        .collect();
    assert_eq!(
        found.len(),
        1,
        "one delivery to {domain} of {name}: {found:?}"
    );
    found[0].clone()
}

/// The trigger the order stream `order`'s event of type `event` is: its
/// stream and its sequence.
fn trigger(w: &Coordinator, order: &str, event: &str) -> EventRef {
    let cover = w.cover("order", order);
    let sequence = w
        .pages(&cover)
        .iter()
        .find(|p| angzarr_embedded::angzarr_router::page_event(p).is_some_and(|a| bare(a) == event))
        .map(|p| p.sequence)
        .unwrap_or_else(|| panic!("an {event} on order {order}"));
    EventRef {
        stream: Some(cover),
        sequence,
    }
}

/// The page of `cover` recording an input of type `name`.
fn recording(w: &Coordinator, domain: &str, name: &str, of: &str) -> EventPage {
    let cover = w.cover(domain, name);
    w.pages(&cover)
        .into_iter()
        .find(|p| angzarr_embedded::angzarr_router::page_event(p).is_some_and(|a| bare(a) == of))
        .unwrap_or_else(|| panic!("{of} recorded on {domain} {name}"))
}

fn letters(w: &Coordinator) -> Vec<AngzarrDeadLetter> {
    w.log.letters()
}

/// A dead letter's rejected reaction, its type by bare name and whether
/// it is a fact.
fn rejected(letter: &AngzarrDeadLetter) -> Option<(String, bool)> {
    match &letter.payload {
        Some(Payload::RejectedReaction(delivery)) => match &delivery.body {
            Some(reaction_delivery::Body::Command(any)) => Some((bare(any), false)),
            Some(reaction_delivery::Body::Fact(any)) => Some((bare(any), true)),
            None => None,
        },
        _ => None,
    }
}

#[given(
    regex = r#"^an? (\w+) event was recorded for order "([\w-]+)" and its (\w+) applied by "([\w-]+)" "([^"]+)"$"#
)]
async fn recorded_and_applied(
    w: &mut Coordinator,
    event: String,
    order: String,
    command: String,
    domain: String,
    name: String,
) {
    let cover = w.cover("order", &order);
    w.record(&cover, &event, None).await;
    recording(w, &domain, &name, &command);
}

#[given(
    regex = r#"^"([\w-]+)" now declares (\w+) with the \(fact\) option instead of \(command\), and the policy's generated code sends it in the fact arm$"#
)]
fn now_a_fact(w: &mut Coordinator, domain: String, name: String) {
    w.business(&domain).declare_fact(&name);
    sends_as_fact(w, &name);
}

#[then(regex = r#"^"([\w-]+)" "([^"]+)" records the (\w+) fact$"#)]
fn records_the_fact(w: &mut Coordinator, domain: String, name: String, fact: String) {
    let (delivered, _) = delivered(w, &domain, &fact, true);
    let page = recording(w, &domain, &name, &fact);
    let meta = page.meta.expect("stamped");
    let (cause, origin) = cause_of(&meta);
    assert_eq!(origin, origin_of(&delivered), "the fact's origin");
    assert_eq!(cause, delivered.command_id, "its causation");
}

#[then(
    regex = r#"^the (?:fact|(\w+) command)'s command_id is the reaction command id of that (\w+) event, the policy "(\w+)" and index (\d+)$"#
)]
fn reaction_id(w: &mut Coordinator, command: String, event: String, policy: String, index: u32) {
    let output = w.policies.last().expect("a policy").output.clone();
    let (delivered, causation) = delivered(w, &output, &command, command.is_empty());
    let trigger = trigger(w, "order-1", &event);
    let derived = policy_command_id(&trigger, &policy, index).expect("a reaction id");
    assert_eq!(delivered.command_id, Some(derived));
    assert_eq!(causation, Some(trigger), "its causation");
}

#[then(
    regex = r#"^the events it records carry the fact cause with that command_id and an origin whose external_id is that command_id's canonical text and whose description names (\w+) and "(\w+)"$"#
)]
fn fact_cause(w: &mut Coordinator, fact: String, policy: String) {
    let output = w.policies.last().expect("a policy").output.clone();
    let (delivered, _) = delivered(w, &output, "", true);
    let id = delivered.command_id.clone().expect("an id");
    let cover = w
        .store_covers(&output)
        .into_iter()
        .find(|c| !w.persisted(c).is_empty())
        .expect("a stream it recorded on");
    for page in w.persisted(&cover) {
        let (cause, origin) = cause_of(&page.meta.expect("stamped"));
        assert_eq!(cause, Some(id.clone()));
        let origin = origin.expect("the fact arm, its origin");
        assert_eq!(origin.external_id, id.to_string());
        assert!(origin.description.contains(&fact), "{origin:?}");
        assert!(origin.description.contains(&policy), "{origin:?}");
    }
}

#[then(regex = r"^the (\w+) fact carries the command_id the (\w+) command carried$")]
fn same_id(w: &mut Coordinator, _fact: String, command: String) {
    let def = w.policies.last().expect("a policy");
    let (policy, output) = (def.name.clone(), def.output.clone());
    let applied = w
        .store_covers(&output)
        .iter()
        .flat_map(|cover| w.pages(cover))
        .find(|p| {
            angzarr_embedded::angzarr_router::page_event(p).is_some_and(|a| bare(a) == command)
        })
        .expect("the command's event");
    let carried = applied.meta.as_ref().and_then(|m| cause_of(m).0);
    // The id the kernel stamps on the policy's fact (C-0775): the
    // reaction's, whatever its arm.
    let stamped = policy_command_id(&trigger(w, "order-1", "OrderPlaced"), &policy, 0).ok();
    assert_eq!(stamped, carried);
}

#[then(
    regex = r#"^it is answered DUPLICATE, and nothing new is appended to stream "([\w-]+)" root "([^"]+)"$"#
)]
fn answered_duplicate(w: &mut Coordinator, domain: String, name: String) {
    // Found by its id before the handler: never delivered.
    assert!(w.business(&domain).delivered().is_empty());
    let cover = w.cover(&domain, &name);
    assert!(w.persisted(&cover).is_empty(), "{:?}", w.persisted(&cover));
}

#[then(
    regex = r#"^the (?:(\w+) )?fact is dead-lettered (?:at "[\w-]+" )?as rejected_reaction, and an alert is raised$"#
)]
fn fact_dead_lettered(w: &mut Coordinator, fact: String) {
    let letters = letters(w);
    assert!(
        letters.iter().any(|l| match rejected(l) {
            Some((name, true)) => fact.is_empty() || name == fact,
            _ => false,
        }),
        "{letters:?}"
    );
    assert!(!w.alerts().is_empty(), "an alert");
}

#[given(
    regex = r#"^(?:"([\w-]+)"'s business process answers the|the "([\w-]+)" business process answers every) (\w+) fact FAILED_PRECONDITION$"#
)]
fn rejects_fact(w: &mut Coordinator, of: String, every: String, fact: String) {
    let domain = if of.is_empty() { every } else { of };
    let business = w.business(&domain);
    business.declare_fact(&fact);
    business.rule(Rule {
        input: fact,
        once: None,
        error: typed_refusal(
            &crate::tests::world::package(&domain),
            "FactRejected",
            angzarr_embedded::angzarr_router::error::BusinessStatus::FailedPrecondition,
        ),
    });
}

#[then(
    regex = r#"^the policy's delivery of it is answered INTERNAL with reason ANGZARR_REASON_FACT_DEAD_LETTERED(?:, naming the stream "([\w-]+)" "([^"]+)" and its cause)?$"#
)]
fn delivery_answered_dead_lettered(w: &mut Coordinator, domain: String, name: String) {
    // In process, the destination's answer to the policy's delivery is the
    // kernel's own: the fact dead-lettered there, its step complete (its
    // position moved past, below). The letter names the stream.
    let letters = letters(w);
    let letter = letters
        .iter()
        .find(|l| matches!(rejected(l), Some((_, true))))
        .unwrap_or_else(|| panic!("a fact dead-lettered: {letters:?}"));
    if !domain.is_empty() {
        let Some(Payload::RejectedReaction(delivery)) = &letter.payload else {
            panic!("a rejected reaction");
        };
        assert_eq!(delivery.cover, Some(w.cover(&domain, &name)));
    }
    assert!(
        reason_of(letter).is_some(),
        "its cause, the reason: {letter:?}"
    );
}

#[then(regex = r#"^no refusal fact is recorded on "([\w-]+)" "([^"]+)"$"#)]
fn no_refusal_fact(w: &mut Coordinator, domain: String, name: String) {
    // The trigger is recorded there; a refusal fact would be recorded
    // after it, in the fact arm.
    let cover = w.cover(&domain, &name);
    let facts: Vec<_> = w
        .persisted(&cover)
        .into_iter()
        .filter(|p| {
            p.meta.as_ref().is_some_and(|m| {
                matches!(
                    m.causation,
                    Some(angzarr_embedded::v1::event_meta::Causation::Fact(_))
                )
            })
        })
        .collect();
    assert!(facts.is_empty(), "{facts:?}");
    let def = w.policies.last().expect("a policy");
    assert!(
        crate::tests::policy::locked(&def.script)
            .refusals
            .is_empty(),
        "Translate called"
    );
}

#[then(regex = r"^the (\w+) trigger is not dead-lettered$")]
fn trigger_not_dead_lettered(w: &mut Coordinator, _event: String) {
    let letters = letters(w);
    assert!(
        !letters
            .iter()
            .any(|l| matches!(l.payload, Some(Payload::RejectedEvents(_)))),
        "{letters:?}"
    );
}

#[then(regex = r#"^the policy "(\w+)"'s position on order "([\w-]+)" is past the (\w+) event$"#)]
fn position_past(w: &mut Coordinator, policy: String, order: String, event: String) {
    let trigger = trigger(w, &order, &event);
    let cover = trigger.stream.clone().expect("its stream");
    assert!(w.position(&policy, &cover) > trigger.sequence);
}

#[then(
    regex = r#"^the (\w+) (command|fact) is refused INTERNAL with reason (\w+) by "([\w-]+)"'s router, dead-lettered as rejected_reaction, and an alert is raised$"#
)]
fn arm_refused(w: &mut Coordinator, name: String, arm: String, reason: String, domain: String) {
    let letters = letters(w);
    let letter = letters
        .iter()
        .find(|l| reason_of(l).is_some_and(|r| r.code == reason))
        .unwrap_or_else(|| panic!("no dead letter of {reason}: {letters:?}"));
    assert_eq!(rejected(letter), Some((name, arm == "fact")));
    let addressed = match &letter.payload {
        Some(Payload::RejectedReaction(delivery)) => {
            delivery.cover.as_ref().map(|c| c.domain.clone())
        }
        _ => None,
    };
    assert_eq!(addressed.as_deref(), Some(domain.as_str()));
    assert!(!w.alerts().is_empty(), "an alert");
}
