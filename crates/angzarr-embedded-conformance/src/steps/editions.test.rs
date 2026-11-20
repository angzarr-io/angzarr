//! Editions as the kernel meets them (decisions 14.32, 14.106 and 14.107):
//! every stream of an edition is forked explicitly, a fork an empty
//! `EditionCreated` in its page's meta-event arm at its fork sequence, and
//! an input (a client's or a policy's) to a stream not forked in its
//! edition is refused `ANGZARR_REASON_EDITION_NOT_FORKED` before business
//! code; a source event's edition is stamped on what a policy sends, and a
//! process manager's events land in its input's edition. angzarr-embedded
//! serves no edition administration RPC (those are the distributed
//! transport's): a scenario's "ForkStream" is the application forking the
//! stream through its store, as `Coordinator::fork` writes it, and
//! "DeleteEdition" its store removing each forked stream's rows.

use cucumber::{given, then, when};

use angzarr_embedded::angzarr_ports::Editions;
use angzarr_embedded::angzarr_router::hpb::HandleRequest;
use angzarr_embedded::testing::Ready as _;
use angzarr_embedded::v1::angzarr_dead_letter::Payload;
use angzarr_embedded::v1::{event_page, reaction_delivery, Cover};
use angzarr_embedded::Failure;

use crate::tests::answer::{status, Answered};
use crate::tests::outcomes::reason_of;
use crate::tests::policies::{last_policy, reaction, reacts, Address, Header};
use crate::tests::policy::{bare, locked, Arm, Root, Translation};
use crate::tests::world::{sref, Coordinator};

/// `cover` in `edition` (`None`: main).
pub(crate) fn in_edition(cover: &Cover, edition: Option<&str>) -> Cover {
    Cover {
        edition: edition.map(str::to_string),
        ..cover.clone()
    }
}

/// An edition as a scenario spells it: `""`, `"angzarr"`, a name, or
/// `unset`.
fn spelled(text: &str) -> Option<String> {
    match text {
        "unset" => None,
        quoted => Some(quoted.trim_matches('"').to_string()),
    }
}

/// The edition of a delivery's input.
fn edition_of(delivered: &HandleRequest) -> Option<String> {
    delivered
        .input
        .as_ref()
        .and_then(|b| b.cover.as_ref())
        .and_then(|c| c.edition.clone())
}

/// Whether `page` is a domain event (not a meta-event).
fn is_event(page: &angzarr_embedded::v1::EventPage) -> bool {
    matches!(page.body, Some(event_page::Body::Event(_)))
}

/// The pages `cover` holds of its own in its edition: what it reads, less
/// main's below its fork.
fn own_pages(w: &Coordinator, cover: &Cover) -> Vec<angzarr_embedded::v1::EventPage> {
    let main = in_edition(cover, None);
    let mains = w.pages(&main);
    w.pages(cover)
        .into_iter()
        .filter(|page| !mains.contains(page))
        .collect()
}

/// `edition`'s every forked stream removed, as DeleteEdition removes
/// them: enumerated by their fork events, each stream's rows removed.
fn delete_edition(w: &mut Coordinator, edition: &str) {
    let editions = w.store.list_editions().ready().expect("the editions list");
    let forks = editions
        .into_iter()
        .filter(|e| e.name == edition)
        .flat_map(|e| e.forks);
    for fork in forks {
        let stream = fork.stream.expect("a fork's stream");
        w.store
            .remove_edition_stream(sref(&stream))
            .ready()
            .expect("the edition stream is removed");
    }
}

#[given(regex = r#"^the source event has edition "([\w-]+)"$"#)]
fn source_edition(w: &mut Coordinator, edition: String) {
    w.edition = Some(edition);
}

#[given("the source event has no edition set")]
fn source_main(w: &mut Coordinator) {
    w.edition = None;
}

#[given(regex = r#"^the command's cover has edition "([\w-]+)"$"#)]
fn command_edition(w: &mut Coordinator, edition: String) {
    w.edition = Some(edition);
}

#[given(regex = r#"^the PM records an? (\w+) event on a (\w+) command$"#)]
fn pm_records(w: &mut Coordinator, event: String, _command: String) {
    w.business("fulfillment")
        .every(|a| a.events = Some(vec![event]));
}

#[given(regex = r#"^the destination "([\w-]+)" stream is forked in edition "([\w-]+)"$"#)]
fn destination_forked(w: &mut Coordinator, domain: String, edition: String) {
    let reached: Vec<Cover> = w
        .policies
        .iter()
        .flat_map(|p| locked(&p.script).reactions.clone())
        .filter(|r| r.domain == domain)
        .filter_map(|r| match &r.root {
            Root::Named(name) => Some(w.cover(&domain, name)),
            _ => None,
        })
        .collect();
    for cover in reached {
        w.fork(&in_edition(&cover, Some(&edition)));
    }
}

#[given(regex = r#"^the PM's stream is forked in edition "([\w-]+)"$"#)]
fn pm_forked(w: &mut Coordinator, edition: String) {
    let main = w.cover("fulfillment", "0192a6b0-0000-7000-8000-0000000000f1");
    w.fork(&in_edition(&main, Some(&edition)));
}

#[given(regex = r"^the policy translates a refusal of (\w+) into an? (\w+) fact (\w+)$")]
fn translates_into(w: &mut Coordinator, command: String, _domain: String, fact: String) {
    let header = last_policy(w);
    let script = w.policy(&header.name, &header.input, &header.output);
    let mut script = locked(&script);
    if !script.reactions.iter().any(|r| r.command == command) {
        script.reactions.push(reaction(
            "OrderCreated",
            &header.output,
            &command,
            Root::Named("inv-1".to_string()),
        ));
    }
    script.translation = Some(Translation::Fact(fact));
}

#[given(regex = r"^the (\w+) aggregate refuses (\w+) with a typed refusal$")]
fn aggregate_refuses(w: &mut Coordinator, domain: String, command: String) {
    let package = crate::tests::world::package(&domain);
    w.business(&domain).rule(crate::tests::business::Rule {
        input: command,
        once: None,
        error: crate::tests::business::rejection(&package, "OutOfStock"),
    });
}

#[then(regex = r#"^no command reaches "([\w-]+)"$"#)]
fn no_command_reaches(w: &mut Coordinator, domain: String) {
    let delivered = w.business(&domain).delivered();
    assert!(delivered.is_empty(), "{delivered:?}");
}

#[when(regex = r#"^a client sends an AddItem command to "([\w-]+)" with edition (unset|"[^"]*")$"#)]
async fn sends_with_edition(w: &mut Coordinator, name: String, edition: String) {
    w.begin();
    let cover = in_edition(&w.cover("order", &name), spelled(&edition).as_deref());
    let book = w.command(&cover, "AddItem");
    w.handle(book).await;
}

#[when(regex = r"^an OrderCreated event is dispatched to the policy$")]
async fn dispatched_to_policy(w: &mut Coordinator) {
    let header = last_policy(w);
    if locked(&w.policies.last().expect("a policy").script)
        .reactions
        .is_empty()
    {
        let reaction = reaction(
            "OrderCreated",
            &header.output,
            "ReserveStock",
            Root::Named("inv-1".to_string()),
        );
        reacts(w, &header, reaction);
    }
    w.begin();
    let main = w.cover("order", "order-1");
    let edition = w.edition.clone();
    if edition.is_some() {
        w.fork(&in_edition(&main, edition.as_deref()));
    }
    let cover = in_edition(&main, edition.as_deref());
    w.record(&cover, "OrderCreated", None).await;
}

#[when(regex = r"^(?:the|a) StartFulfillment command is delivered to the PM$")]
async fn delivered_to_pm(w: &mut Coordinator) {
    w.begin();
    let main = w.cover("fulfillment", "0192a6b0-0000-7000-8000-0000000000f1");
    let cover = in_edition(&main, w.edition.clone().as_deref());
    let book = w.command(&cover, "StartFulfillment");
    w.handle(book).await;
}

#[then(
    regex = r#"^the delivered (?:command|fact)'s cover has (?:edition "([\w-]+)"|no edition set)$"#
)]
fn delivered_cover_edition(w: &mut Coordinator, edition: String) {
    let output = w.policies.last().expect("a policy").output.clone();
    let delivered = w
        .business(&output)
        .delivered()
        .pop()
        .expect("the output delivered");
    let expected = (!edition.is_empty()).then_some(edition);
    assert_eq!(edition_of(&delivered), expected);
}

#[then(regex = r#"^the (\w+) event is persisted on the PM's stream in edition "([\w-]+)"$"#)]
fn pm_event_in_edition(w: &mut Coordinator, event: String, edition: String) {
    let main = w.cover("fulfillment", "0192a6b0-0000-7000-8000-0000000000f1");
    let forked = in_edition(&main, Some(&edition));
    let own: Vec<String> = own_pages(w, &forked)
        .iter()
        .filter_map(angzarr_embedded::angzarr_router::page_event)
        .map(bare)
        .collect();
    assert_eq!(own, [event], "its events in {edition}");
}

#[then("the PM's stream on the main timeline is unchanged")]
fn pm_main_unchanged(w: &mut Coordinator) {
    let main = w.cover("fulfillment", "0192a6b0-0000-7000-8000-0000000000f1");
    assert!(w.persisted(&main).is_empty(), "none on main");
}

#[then(regex = r#"^the (\w+) fact is delivered to the trigger's stream with edition "([\w-]+)"$"#)]
fn refusal_fact_in_edition(w: &mut Coordinator, fact: String, edition: String) {
    let input = w.policies.last().expect("a policy").input.clone();
    let facts: Vec<HandleRequest> = w
        .business(&input)
        .delivered()
        .into_iter()
        .filter(|d| {
            d.input
                .as_ref()
                .and_then(|b| b.page.as_ref())
                .and_then(angzarr_embedded::angzarr_router::fact_payload)
                .is_some_and(|f| bare(f) == fact)
        })
        .collect();
    assert_eq!(facts.len(), 1, "{facts:?}");
    assert_eq!(edition_of(&facts[0]), Some(edition));
    let trigger = facts[0].causation.as_ref().and_then(|c| c.stream.clone());
    let cover = facts[0].input.as_ref().and_then(|b| b.cover.clone());
    assert_eq!(cover, trigger, "the trigger's exact stream");
}

#[given(
    regex = r#"^an "order" aggregate "([\w-]+)" with (\d+) events written with edition (unset|"[\w-]*")$"#
)]
fn written_with_edition(w: &mut Coordinator, name: String, count: usize, written: String) {
    let cover = in_edition(&w.cover("order", &name), spelled(&written).as_deref());
    let names = vec!["ItemAdded"; count];
    w.write(&cover, &names);
}

#[given(regex = r#"^an? "([\w-]+)" aggregate "([\w-]+)" with (\d+) events on the main timeline$"#)]
fn with_events_on_main(w: &mut Coordinator, domain: String, name: String, count: usize) {
    let cover = w.cover(&domain, &name);
    let names = vec!["Happened"; count];
    w.write(&cover, &names);
}

#[given(
    regex = r#"^"([\w-]+)" "([\w-]+)" is forked into edition "([\w-]+)"(?: at sequence (\d+))?$"#
)]
fn forked_into(w: &mut Coordinator, domain: String, name: String, edition: String, at: String) {
    let cover = in_edition(&w.cover(&domain, &name), Some(&edition));
    let head = w.pages(&w.cover(&domain, &name)).len() as u64;
    if let Ok(at) = at.parse::<u64>() {
        assert_eq!(at, head, "a fork at main's head");
    }
    w.fork(&cover);
}

#[given(
    regex = r#"^"([\w-]+)" "([\w-]+)" with (\d+) events on the main timeline (?:has no fork in|is not forked in) (?:edition )?"[\w-]+"$"#
)]
fn unforked(w: &mut Coordinator, domain: String, name: String, count: usize) {
    with_events_on_main(w, domain, name, count);
}

#[given(regex = r#"^"([\w-]+)" "([\w-]+)" has no fork in edition "([\w-]+)"$"#)]
fn has_no_fork(w: &mut Coordinator, domain: String, name: String, edition: String) {
    let cover = in_edition(&w.cover(&domain, &name), Some(&edition));
    assert!(own_pages(w, &cover).is_empty(), "no fork");
}

#[given(
    regex = r#"^"(\w+)" sends (\w+), a fact "([\w-]+)" declares, to ("[\w-]+" "[^"]+") on (\w+) in place of (\w+)$"#
)]
fn sends_fact_instead(
    w: &mut Coordinator,
    policy: String,
    fact: String,
    domain: String,
    to: Address,
    on: String,
    instead: String,
) {
    w.business(&domain).declare_fact(&fact);
    let def = w
        .policies
        .iter()
        .find(|p| p.name == policy)
        .expect("the policy");
    let mut script = locked(&def.script);
    script.reactions.retain(|r| r.command != instead);
    let mut reaction = reaction(&on, &to.domain, &fact, Root::Named(to.root));
    reaction.arm = Arm::Fact;
    script.reactions.push(reaction);
}

#[given(
    regex = r#"^a client's (\w+) command to "([\w-]+)" "([\w-]+)" in edition "([\w-]+)" was refused (\w+)$"#
)]
async fn was_refused(
    w: &mut Coordinator,
    command: String,
    domain: String,
    name: String,
    edition: String,
    reason: String,
) {
    let cover = in_edition(&w.cover(&domain, &name), Some(&edition));
    let book = w.command(&cover, &command);
    w.handle(book).await;
    assert_eq!(
        w.answer().reason(),
        Some(reason.as_str()),
        "{:?}",
        w.answer()
    );
}

#[when(
    regex = r#"^ForkStream forks "([\w-]+)" "([\w-]+)" into edition "([\w-]+)"(?: at fork_sequence (\d+))?$"#
)]
fn fork_stream(w: &mut Coordinator, domain: String, name: String, edition: String, at: String) {
    forked_into(w, domain, name, edition, at);
}

#[given(regex = r#"^ForkStream has forked "([\w-]+)" "([^"]+)" into edition "([\w-]+)"$"#)]
fn fork_stream_given(w: &mut Coordinator, domain: String, name: String, edition: String) {
    forked_into(w, domain, name, edition, String::new());
}

#[when("the client resends the SeatPlayer command with the same command_id")]
async fn resends(w: &mut Coordinator) {
    w.begin();
    let book = w.last_book();
    w.handle(book).await;
}

#[then(
    regex = r#"^the (\w+) command is handled in edition "([\w-]+)", its events at sequence (\d+) and after$"#
)]
fn handled_in_edition(w: &mut Coordinator, _command: String, edition: String, from: u64) {
    let book = w.last_book();
    let cover = book.cover.expect("a cover");
    assert_eq!(cover.edition.as_deref(), Some(edition.as_str()));
    let own: Vec<u64> = own_pages(w, &cover)
        .iter()
        .filter(|p| is_event(p))
        .map(|p| p.sequence)
        .collect();
    assert!(!own.is_empty() && own.iter().all(|s| *s >= from), "{own:?}");
}

#[then(regex = r#"^the main timeline of "([\w-]+)" "([\w-]+)" is unchanged$"#)]
fn main_unchanged(w: &mut Coordinator, domain: String, name: String) {
    let main = w.cover(&domain, &name);
    assert!(w.persisted(&main).is_empty(), "{:?}", w.persisted(&main));
}

#[when(regex = r#"^"([\w-]+)" "([\w-]+)" records (\w+) in edition "([\w-]+)"$"#)]
async fn records_in_edition(
    w: &mut Coordinator,
    domain: String,
    name: String,
    event: String,
    edition: String,
) {
    w.begin();
    let cover = in_edition(&w.cover(&domain, &name), Some(&edition));
    w.record(&cover, &event, None).await;
}

/// The stream text a scenario's `stream "D" "R"` in `edition` names.
fn stream_text(w: &Coordinator, domain: &str, name: &str, edition: &str) -> String {
    let cover = in_edition(&w.cover(domain, name), Some(edition));
    format!(
        "{}/{}@{}",
        cover.domain,
        cover.root.expect("a root"),
        edition
    )
}

#[then(
    regex = r#"^the (\w+) command is refused at "([\w-]+)" "([\w-]+)" before business code, with reason (\w+), stream "[\w-]+" "[\w-]+" and edition "([\w-]+)"$"#
)]
fn reaction_refused(
    w: &mut Coordinator,
    _command: String,
    domain: String,
    name: String,
    reason: String,
    edition: String,
) {
    let answer = refusal(w);
    assert_eq!(
        answer.status(),
        Some(status("FAILED_PRECONDITION")),
        "{answer:?}"
    );
    assert_eq!(answer.reason(), Some(reason.as_str()));
    assert_eq!(
        answer.arg("stream"),
        Some(stream_text(w, &domain, &name, &edition).as_str())
    );
    assert_eq!(answer.arg("edition"), Some(edition.as_str()));
    assert!(
        w.business(&domain).delivered().is_empty(),
        "before business code"
    );
}

#[then(
    regex = r#"^it is dead-lettered as a rejected reaction with that reason, and an alert is raised$"#
)]
fn dead_lettered_reaction(w: &mut Coordinator) {
    let reason = refusal(w).reason().map(str::to_string);
    let letters = w.log.letters();
    let letter = letters
        .iter()
        .find(|l| reason_of(l).map(|r| r.code.clone()) == reason)
        .unwrap_or_else(|| panic!("no letter of {reason:?}: {letters:?}"));
    assert!(
        matches!(letter.payload, Some(Payload::RejectedReaction(_))),
        "{letter:?}"
    );
    assert!(!w.alerts().is_empty(), "alerted");
}

#[then(
    regex = r#"^the (\w+) fact is dead-lettered as a rejected reaction with reason (\w+), stream "([\w-]+)" "([\w-]+)" and edition "([\w-]+)", and an alert is raised$"#
)]
fn fact_dead_lettered(
    w: &mut Coordinator,
    fact: String,
    reason: String,
    domain: String,
    name: String,
    edition: String,
) {
    let letters = w.log.letters();
    let letter = letters
        .iter()
        .find(|l| reason_of(l).is_some_and(|r| r.code == reason))
        .unwrap_or_else(|| panic!("no letter of {reason}: {letters:?}"));
    let Some(Payload::RejectedReaction(delivery)) = &letter.payload else {
        panic!("a rejected reaction: {letter:?}");
    };
    let Some(reaction_delivery::Body::Fact(payload)) = &delivery.body else {
        panic!("a fact: {delivery:?}");
    };
    assert_eq!(bare(payload), fact);
    let args = &reason_of(letter).expect("a reason").args;
    assert_eq!(
        args.get("stream"),
        Some(&stream_text(w, &domain, &name, &edition))
    );
    assert_eq!(args.get("edition"), Some(&edition));
    assert!(!w.alerts().is_empty(), "alerted");
}

#[then("the policy's Translate is never called")]
fn translate_never_called(w: &mut Coordinator) {
    for def in &w.policies {
        assert!(locked(&def.script).refusals.is_empty(), "{}", def.name);
    }
}

#[then("the router is never called")]
fn router_never_called(w: &mut Coordinator) {
    for domain in w.domains.keys().cloned().collect::<Vec<_>>() {
        let delivered = w.business(&domain).delivered();
        assert!(delivered.is_empty(), "{domain}: {delivered:?}");
    }
}

#[then(regex = r#"^"([\w-]+)" "([\w-]+)" holds no event in edition "([\w-]+)"$"#)]
fn holds_no_event_in(w: &mut Coordinator, domain: String, name: String, edition: String) {
    let cover = in_edition(&w.cover(&domain, &name), Some(&edition));
    let main = w.cover(&domain, &name);
    assert_eq!(w.pages(&cover), w.pages(&main), "it reads as main's");
}

#[then(regex = r#"^the command is handled by the process manager in edition "([\w-]+)"$"#)]
fn pm_handled_in(w: &mut Coordinator, edition: String) {
    let output = w.policies.last().expect("a policy").output.clone();
    let delivered = w.business(&output).delivered();
    assert_eq!(
        delivered.len(),
        1,
        "{:?} {:?}",
        w.answers(),
        w.log.letters()
    );
    assert_eq!(edition_of(&delivered[0]), Some(edition));
}

#[then(
    regex = r#"^"([\w-]+)" "([^"]+)" in edition "([\w-]+)" holds exactly one EditionCreated event, the one ForkStream wrote$"#
)]
fn one_fork(w: &mut Coordinator, domain: String, name: String, edition: String) {
    let cover = in_edition(&w.cover(&domain, &name), Some(&edition));
    let forks = w
        .pages(&cover)
        .iter()
        .filter(|p| angzarr_embedded::angzarr_ports::is_fork(p))
        .count();
    assert_eq!(forks, 1);
}

#[given(regex = r#"^a policy "(\w+)" that sends a command to "([\w-]+)" "([^"]+)" on (\w+)$"#)]
fn policy_sends_command(
    w: &mut Coordinator,
    name: String,
    domain: String,
    root: String,
    on: String,
) {
    let input = w
        .policies
        .first()
        .map_or("player".to_string(), |p| p.input.clone());
    let header = Header {
        name,
        input,
        output: domain.clone(),
    };
    let reaction = reaction(&on, &domain, "RecordPlayerReady", Root::Named(root));
    reacts(w, &header, reaction);
}

#[when(
    regex = r#"^a client sends a (command|fact) to "([\w-]+)" "([\w-]+)" in edition "([\w-]+)"(?:, which has no fork there)?$"#
)]
async fn sends_in_edition(
    w: &mut Coordinator,
    kind: String,
    domain: String,
    name: String,
    edition: String,
) {
    w.begin();
    let cover = in_edition(&w.cover(&domain, &name), Some(&edition));
    let book = match kind.as_str() {
        "fact" => w.fact(&cover, "SeatReserved", "seat-1"),
        _ => w.command(&cover, "SeatPlayer"),
    };
    w.handle(book).await;
}

#[when(regex = r#"^a command is sent to "([\w-]+)" "([\w-]+)" in edition "([\w-]+)"$"#)]
async fn command_sent_in_edition(
    w: &mut Coordinator,
    domain: String,
    name: String,
    edition: String,
) {
    w.begin();
    let cover = in_edition(&w.cover(&domain, &name), Some(&edition));
    let book = w.command(&cover, "RecordPlayerReady");
    w.handle(book).await;
}

/// A policy's reaction to `domain` `name` in `edition`, its trigger
/// written on a forked source stream and its owed step run, as a catch-up
/// runs it: no business code runs but the destination's.
async fn reaction_arrives(w: &mut Coordinator, domain: &str, name: &str, edition: &str) {
    let header = Header {
        name: "Arrival".to_string(),
        input: "payment".to_string(),
        output: domain.to_string(),
    };
    let reaction = reaction(
        "PaymentReceived",
        domain,
        "SeatPlayer",
        Root::Named(name.to_string()),
    );
    reacts(w, &header, reaction);
    let source = in_edition(&w.cover("payment", "p-1"), Some(edition));
    w.fork(&source);
    w.write(&source, &["PaymentReceived"]);
    w.begin();
    let resumed = w.resume().await;
    w.resumed.push(resumed);
}

#[when(
    regex = r#"^a policy's command arrives at "([\w-]+)" "([\w-]+)" in edition "([\w-]+)", which has been deleted$"#
)]
async fn arrives_deleted(w: &mut Coordinator, domain: String, name: String, edition: String) {
    let cover = in_edition(&w.cover(&domain, &name), Some(&edition));
    w.fork(&cover);
    delete_edition(w, &edition);
    reaction_arrives(w, &domain, &name, &edition).await;
}

#[when(regex = r#"^a policy command in edition "([\w-]+)" arrives at "([\w-]+)" "([\w-]+)"$"#)]
async fn policy_command_arrives(
    w: &mut Coordinator,
    edition: String,
    domain: String,
    name: String,
) {
    reaction_arrives(w, &domain, &name, &edition).await;
}

/// The failure the last catch-up came to.
fn resumed_failure(w: &Coordinator) -> Answered {
    match w.resumed.last().expect("a catch-up ran") {
        Err(Failure::Downstream { error, .. }) => Answered::of_step(error),
        Err(other) => panic!("a failure down the chain, not {other:?}"),
        Ok(_) => panic!("a failure, not a catch-up that recorded"),
    }
}

/// The answer the last input or catch-up came to, as its failure: a step's
/// when a catch-up ran.
fn refusal(w: &Coordinator) -> Answered {
    if let Some(letter) = reaction_letter(w) {
        return letter;
    }
    match w.resumed.is_empty() {
        true => match w.handled.last().map(|(_, r)| r) {
            Some(Err(Failure::Downstream { error, .. })) => Answered::of_step(error),
            _ => w.answer(),
        },
        false => resumed_failure(w),
    }
}

/// A reaction the framework refused, as its dead letter records it (the
/// reaction has no caller to answer): its reason under the status the key
/// is declared with.
pub(crate) fn reaction_letter(w: &Coordinator) -> Option<Answered> {
    use angzarr_embedded::SpecKey as _;
    let letters = w.log.letters();
    let letter = letters
        .iter()
        .rfind(|l| matches!(l.payload, Some(Payload::RejectedReaction(_))))?;
    let reason = reason_of(letter)?;
    let key = angzarr_embedded::v1::AngzarrReason::from_str_name(&reason.code)?;
    Some(Answered::Failed {
        status: key.grpc()?,
        reason: reason.code.clone(),
        args: reason
            .args
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        refusal: None,
    })
}

#[then(
    regex = r#"^(?:it|the command) is refused (?:(\w+) |before business code )with reason (\w+)(?:, naming the stream and the edition|, stream "([\w-]+)" "([\w-]+)" and edition "([\w-]+)")$"#
)]
fn refused_not_forked(
    w: &mut Coordinator,
    grpc: String,
    reason: String,
    domain: String,
    name: String,
    edition: String,
) {
    let answer = refusal(w);
    if !grpc.is_empty() {
        assert_eq!(answer.status(), Some(status(&grpc)), "{answer:?}");
    }
    assert_eq!(answer.reason(), Some(reason.as_str()), "{answer:?}");
    match domain.is_empty() {
        true => {
            assert!(answer.arg("stream").is_some(), "{answer:?}");
            assert!(answer.arg("edition").is_some(), "{answer:?}");
        }
        false => {
            assert_eq!(
                answer.arg("stream"),
                Some(stream_text(w, &domain, &name, &edition).as_str())
            );
            assert_eq!(answer.arg("edition"), Some(edition.as_str()));
        }
    }
}

#[then(regex = r#"^nothing is written(?: in edition "([\w-]+)")?$"#)]
fn nothing_written(w: &mut Coordinator, _edition: String) {
    for domain in w.domains.keys().cloned().collect::<Vec<_>>() {
        for cover in w.store_covers(&domain) {
            let own: Vec<_> = w.persisted(&cover).into_iter().filter(is_event).collect();
            assert!(own.is_empty(), "{cover:?}: {own:?}");
        }
    }
}

#[given(regex = r#"^no fork event of edition "([\w-]+)" exists anywhere$"#)]
fn no_fork_anywhere(w: &mut Coordinator, edition: String) {
    let editions = w.store.list_editions().ready().expect("the editions list");
    assert!(editions.iter().all(|e| e.name != edition));
}

#[given(regex = r#"^edition "([\w-]+)" was forked and then deleted with DeleteEdition$"#)]
fn forked_then_deleted(w: &mut Coordinator, edition: String) {
    let cover = in_edition(&w.cover("player", "p-1"), Some(&edition));
    w.fork(&cover);
    delete_edition(w, &edition);
}

#[given(regex = r#"^edition "([\w-]+)" was deleted and no row of it remains$"#)]
fn deleted_no_rows(w: &mut Coordinator, edition: String) {
    forked_then_deleted(w, edition);
}

#[then(regex = r#"^ListEditions does not list edition "([\w-]+)"$"#)]
fn not_listed(w: &mut Coordinator, edition: String) {
    no_fork_anywhere(w, edition);
}

#[then(regex = r#"^only ForkStream can bring the name "([\w-]+)" back$"#)]
fn only_fork_brings_back(w: &mut Coordinator, edition: String) {
    // No input re-created it: no fork of it is listed.
    no_fork_anywhere(w, edition);
}

#[given(regex = r#"^a (?:policy "\w+"|projector) with a handler for every "([\w-]+)" event$"#)]
fn handler_for_every(w: &mut Coordinator, domain: String) {
    w.domain(&domain);
}

#[when(regex = r#"^"([\w-]+)" "([\w-]+)" is loaded in edition "([\w-]+)" for a command$"#)]
async fn loaded_for_command(w: &mut Coordinator, domain: String, name: String, edition: String) {
    w.begin();
    let cover = in_edition(&w.cover(&domain, &name), Some(&edition));
    let book = w.command(&cover, "RecordPlayerChecked");
    w.handle(book).await;
}

#[then(
    "the system layer delivers the EditionCreated to no policy, process manager, aggregate or projector handler"
)]
fn no_handler_given_fork(w: &mut Coordinator) {
    for def in &w.policies {
        let script = locked(&def.script);
        for source in &script.sources {
            assert!(source.pages.iter().all(is_event), "{source:?}");
        }
    }
}

#[then("business code is handed only pages of the event arm")]
fn only_event_pages(w: &mut Coordinator) {
    for domain in w.domains.keys().cloned().collect::<Vec<_>>() {
        for delivered in w.business(&domain).delivered() {
            let pages = delivered.history.map(|h| h.pages).unwrap_or_default();
            assert!(pages.iter().all(is_event), "{pages:?}");
        }
    }
}

#[then("the router is never given it")]
fn router_never_given(w: &mut Coordinator) {
    only_event_pages(w);
}

#[when(
    regex = r#"^a client sends a command whose payload is an io\.angzarr\.v1\.EditionCreated to "([\w-]+)" "([\w-]+)"$"#
)]
async fn sends_edition_created(w: &mut Coordinator, domain: String, name: String) {
    w.begin();
    let cover = w.cover(&domain, &name);
    let mut book = w.command(&cover, "Placeholder");
    if let Some(angzarr_embedded::v1::command_page::Body::Command(command)) =
        book.page.as_mut().and_then(|p| p.body.as_mut())
    {
        command.payload = Some(angzarr_embedded::pack(
            &angzarr_embedded::v1::EditionCreated {},
        ));
    }
    w.business(&domain).rule(crate::tests::business::Rule {
        input: "EditionCreated".to_string(),
        once: None,
        error: crate::tests::business::unknown_command_type("/io.angzarr.v1.EditionCreated"),
    });
    w.handle(book).await;
}

#[then("no EditionCreated event is written anywhere")]
fn no_fork_written(w: &mut Coordinator) {
    let editions = w.store.list_editions().ready().expect("the editions list");
    let main_forks: usize = w
        .domains
        .keys()
        .cloned()
        .collect::<Vec<_>>()
        .iter()
        .flat_map(|d| w.store_covers(d))
        .map(|c| {
            w.pages(&c)
                .iter()
                .filter(|p| angzarr_embedded::angzarr_ports::is_fork(p))
                .count()
        })
        .sum();
    assert_eq!(main_forks, 0);
    assert!(editions.is_empty() || editions.iter().all(|e| !e.forks.is_empty()));
}

#[then(regex = r#"^the client is refused (\w+) with reason (\w+), type_url "([^"]+)"$"#)]
fn refused_type_url(w: &mut Coordinator, grpc: String, reason: String, type_url: String) {
    let answer = w.answer();
    assert_eq!(answer.status(), Some(status(&grpc)), "{answer:?}");
    assert_eq!(answer.reason(), Some(reason.as_str()));
    assert_eq!(answer.arg("type_url"), Some(type_url.as_str()));
}

#[given(
    regex = r#"^"([\w-]+)" "([\w-]+)" (?:on the main timeline|in edition "([\w-]+)") gains (\d+) (?:more events|events?(?: of its own)?)$"#
)]
fn gains(w: &mut Coordinator, domain: String, name: String, edition: String, count: usize) {
    let edition = (!edition.is_empty()).then_some(edition);
    let cover = in_edition(&w.cover(&domain, &name), edition.as_deref());
    // An edition's own event is told apart from main's by its type.
    let event = match edition {
        Some(_) => "HappenedInEdition",
        None => "Happened",
    };
    w.write(&cover, &vec![event; count]);
}

#[when(regex = r#"^"([\w-]+)" "([\w-]+)" is loaded in edition "([\w-]+)"$"#)]
fn loaded_in(w: &mut Coordinator, domain: String, name: String, edition: String) {
    // The load is the store's read of the edition's stream, the one the
    // kernel loads a stream through: a then reads it again.
    w.speak_of_stream(in_edition(&w.cover(&domain, &name), Some(&edition)));
}

#[then(
    regex = r"^the loaded history is main's events 0 to (\d+), then the fork event at (\d+), then the edition's own event at (\d+)$"
)]
fn loaded_history(w: &mut Coordinator, last_main: u64, fork: u64, own: u64) {
    let pages = w.pages(&w.order());
    let main = w.pages(&in_edition(&w.order(), None));
    let sequences: Vec<u64> = pages.iter().map(|p| p.sequence).collect();
    assert_eq!(sequences, (0..=own).collect::<Vec<_>>());
    assert_eq!(&pages[..=last_main as usize], &main[..=last_main as usize]);
    assert!(
        !is_event(&pages[fork as usize]),
        "the fork event: {:?}",
        pages[fork as usize]
    );
    assert!(is_event(&pages[own as usize]));
}

#[then(regex = r"^none of main's events at or after sequence (\d+) are in it$")]
fn none_of_mains_after(w: &mut Coordinator, from: usize) {
    let pages = w.pages(&w.order());
    let main = w.pages(&in_edition(&w.order(), None));
    for page in &main[from..] {
        assert!(!pages.contains(page), "{page:?}");
    }
}
