//! A refused policy command becoming one fact for the policy's own source
//! (policy_refusal.feature; decision 46 #5): the target's typed refusal,
//! the policy's one Translate callback (decision 14.89), and the fact the
//! coordinator addresses to the trigger's exact stream and identifies with
//! the refused command's id. A failure of any other class never reaches
//! Translate.

use cucumber::{given, then, when};

use angzarr_embedded::angzarr_router::error::{BusinessStatus, CodedError};
use angzarr_embedded::angzarr_router::hpb::HandleRequest;
use angzarr_embedded::v1::angzarr_dead_letter::Payload;
use angzarr_embedded::v1::{
    command_page, reaction_delivery, AngzarrDeadLetter, ComponentKind, Cover, RefusalStatus,
};
use angzarr_embedded::{Any, Failure};

use crate::tests::answer::{status, Answered};
use crate::tests::business::Rule;
use crate::tests::outcomes::reason_of;
use crate::tests::policies::{reaction, reacts, Header};
use crate::tests::policy::{bare, internal, locked, payload, Root, Translation};
use crate::tests::world::{origin_of, root, Coordinator};

/// The refusal object a wallet refuses with, as the wire carries it:
/// `available` and `requested`, varint fields 1 and 2.
fn insufficient_funds(available: u64, requested: u64) -> Any {
    fn varint(mut n: u64, out: &mut Vec<u8>) {
        while n >= 0x80 {
            out.push((n as u8) | 0x80);
            n >>= 7;
        }
        out.push(n as u8);
    }
    let mut value = Vec::new();
    for (tag, n) in [(0x08, available), (0x10, requested)] {
        if n > 0 {
            value.push(tag);
            varint(n, &mut value);
        }
    }
    Any {
        type_url: payload("wallet", "InsufficientFunds").type_url,
        value,
    }
}

/// A typed refusal of `object`, under `status`, as the router answers it.
fn refused_with(status: BusinessStatus, object: Any) -> CodedError {
    CodedError::refused(status, object)
}

/// The payload of a delivery's fact, if it is one.
fn fact_payload(delivered: &HandleRequest) -> Option<Any> {
    let page = delivered.input.as_ref()?.page.as_ref()?;
    match page.body.as_ref()? {
        command_page::Body::Fact(fact) => fact.payload.clone(),
        command_page::Body::Command(_) => None,
    }
}

/// The facts delivered to `domain` (of type `name`, any when empty).
fn facts(w: &mut Coordinator, domain: &str, name: &str) -> Vec<HandleRequest> {
    w.business(domain)
        .delivered()
        .into_iter()
        .filter(|d| fact_payload(d).is_some_and(|p| name.is_empty() || bare(&p) == name))
        .collect()
}

/// The policy that builds HoldFunds, and its script.
fn holds_funds(w: &Coordinator) -> &crate::tests::world::PolicyDef {
    w.policies
        .iter()
        .find(|p| p.name == "BuyInToWallet")
        .expect("BuyInToWallet")
}

/// The policy named `name`.
fn named_policy<'w>(w: &'w Coordinator, name: &str) -> &'w crate::tests::world::PolicyDef {
    w.policies
        .iter()
        .find(|p| p.name == name)
        .unwrap_or_else(|| panic!("the policy {name}"))
}

#[given(
    regex = r#"^a "wallet" aggregate whose HoldFunds lists the refusal InsufficientFunds, declared FAILED_PRECONDITION, and refuses with it when funds are short$"#
)]
fn wallet(w: &mut Coordinator) {
    w.domain("wallet");
}

#[given(
    regex = r#"^"(\w+)"'s one refusal callback answers the "([\w-]+)" fact (\w+) for an? (\w+) refusal of (\w+)$"#
)]
fn one_callback(
    w: &mut Coordinator,
    name: String,
    _domain: String,
    fact: String,
    _refusal: String,
    _command: String,
) {
    locked(&named_policy(w, &name).script).translation = Some(Translation::Fact(fact));
}

#[given(regex = r#"^wallet "([\w-]+)" has too little available to hold (\d+)$"#)]
fn too_little(w: &mut Coordinator, wallet: String, amount: u64) {
    short(w, &wallet, 0, amount);
}

#[given(regex = r#"^wallet "([\w-]+)" has (\d+) available and is asked to hold (\d+)$"#)]
fn available_and_asked(w: &mut Coordinator, wallet: String, available: u64, amount: u64) {
    short(w, &wallet, available, amount);
}

/// Wallet `name` short, holding `available`: HoldFunds refused with
/// InsufficientFunds for `requested`.
fn short(w: &mut Coordinator, name: &str, available: u64, requested: u64) {
    let cover = w.cover("wallet", name);
    w.write(&cover, &["FundsShort"]);
    w.business("wallet").rule(Rule {
        input: "HoldFunds".to_string(),
        once: Some("FundsShort".to_string()),
        error: refused_with(
            BusinessStatus::FailedPrecondition,
            insufficient_funds(available, requested),
        ),
    });
}

#[given(regex = r#"^wallet "([\w-]+)" answers HoldFunds with (.+)$"#)]
fn answers_with_status(w: &mut Coordinator, _wallet: String, answer: String) {
    let error = match answer.as_str() {
        "INVALID_ARGUMENT with a typed refusal the command lists" => {
            refused_with(BusinessStatus::InvalidArgument, insufficient_funds(0, 500))
        }
        "ABORTED with a typed refusal the command lists" => {
            refused_with(BusinessStatus::Aborted, insufficient_funds(0, 500))
        }
        grpc => {
            let mut error = internal(&format!("WALLET_{grpc}"));
            error.grpc = status(grpc);
            error
        }
    };
    w.business("wallet").rule(Rule {
        input: "HoldFunds".to_string(),
        once: None,
        error,
    });
}

#[given(regex = r#"^"(\w+)" is written by hand and its router has no refusal callback$"#)]
fn no_callback(w: &mut Coordinator, name: String) {
    locked(&named_policy(w, &name).script).translation = None;
}

#[given(regex = r#"^"(\w+)"'s refusal callback fails with INTERNAL$"#)]
fn callback_fails(w: &mut Coordinator, name: String) {
    let code = internal("ANGZARR_REASON_UNHANDLED_HANDLER_ERROR");
    locked(&named_policy(w, &name).script).translation = Some(Translation::Fails(code));
}

#[given(regex = r#"^"(\w+)"'s Translate answers a fact with no payload$"#)]
fn empty_translation(w: &mut Coordinator, name: String) {
    locked(&named_policy(w, &name).script).translation = Some(Translation::Empty);
}

#[given(
    regex = r#"^a process manager "(\w+)" owning domain "([\w-]+)" whose (\w+) lists the refusal (\w+) and refuses with it once completed$"#
)]
fn process_manager_refusing(
    w: &mut Coordinator,
    name: String,
    domain: String,
    input: String,
    refusal: String,
) {
    w.names.insert(name.clone(), domain.clone());
    w.process_manager(&domain, &[]);
    let package = crate::tests::world::package(&domain);
    w.business(&domain).rule(Rule {
        input,
        once: Some(format!("{name}Completed")),
        error: crate::tests::business::rejection(&package, &refusal),
    });
    let cover = w.cover(&domain, "the-stock-correlation");
    w.write(&cover, &[&format!("{name}Completed")]);
}

#[given(
    regex = r#"^a policy "(\w+)" translating from "([\w-]+)" to "([\w-]+)" whose refusal callback answers the "[\w-]+" fact (\w+) for (\w+)$"#
)]
fn translating_policy(
    w: &mut Coordinator,
    name: String,
    input: String,
    output: String,
    fact: String,
    _refusal: String,
) {
    let header = Header {
        name: name.clone(),
        input,
        output: output.clone(),
    };
    let reaction = reaction(
        "StockReserved",
        &output,
        "RecordStockReserved",
        Root::TriggerCorrelation,
    );
    reacts(w, &header, reaction);
    locked(&named_policy(w, &name).script).translation = Some(Translation::Fact(fact));
}

#[given(regex = r#"^a policy "(\w+)" that builds (\w+) from (\w+)$"#)]
fn builds_another(w: &mut Coordinator, name: String, command: String, on: String) {
    let def = named_policy(w, &name);
    let header = Header {
        name,
        input: def.input.clone(),
        output: def.output.clone(),
    };
    let output = header.output.clone();
    reacts(
        w,
        &header,
        reaction(&on, &output, &command, Root::Named("p-1".to_string())),
    );
}

#[given(regex = r#"^the "wallet" aggregate's (\w+) lists no refusal$"#)]
fn lists_no_refusal(w: &mut Coordinator, _command: String) {
    w.domain("wallet");
}

#[given(
    regex = r#"^wallet "([\w-]+)"'s (\w+) handler fails with its own key (\w+) and arg wallet "([\w-]+)"$"#
)]
fn handler_fails_own_key(
    w: &mut Coordinator,
    _wallet: String,
    command: String,
    key: String,
    arg: String,
) {
    w.business("wallet").rule(Rule {
        input: command,
        once: None,
        error: CodedError::bug(&key, [("wallet".to_string(), arg)]),
    });
}

#[given(
    regex = r#"^"buy-in" root "([^"]+)" is forked in edition "([\w-]+)" and wallet "([\w-]+)" is not$"#
)]
fn buy_in_forked(w: &mut Coordinator, correlation: String, edition: String, _wallet: String) {
    let cover = Cover {
        edition: Some(edition),
        ..w.cover("buy-in", &correlation)
    };
    w.fork(&cover);
}

#[when(
    regex = r#"^"(\w+)" records (BuyInStarted|BuyInCancelled) (?:for \d+ from|for) wallet "([\w-]+)"(?: in edition "([\w-]+)")?(?: under correlation "([^"]+)")?$"#
)]
async fn records_buy_in(
    w: &mut Coordinator,
    name: String,
    event: String,
    wallet: String,
    edition: String,
    correlation: String,
) {
    let correlation = if correlation.is_empty() {
        "the-buy-in".to_string()
    } else {
        correlation
    };
    let def = holds_funds(w);
    for reaction in locked(&def.script).reactions.iter_mut() {
        reaction.root = Root::Named(wallet.clone());
    }
    w.begin();
    let domain = w.domain_of(&name);
    // BuyIn's handler records its event as it starts an instance.
    w.domain(&domain).starting.push(format!("Record{event}"));
    let cover = Cover {
        edition: (!edition.is_empty()).then_some(edition),
        ..w.cover(&domain, &correlation)
    };
    w.record(&cover, &event, Some(&correlation)).await;
}

#[when(regex = r#"^"(\w+)" refuses the (\w+) built from (\w+) of item "([\w-]+)"$"#)]
async fn refuses_built_from(
    w: &mut Coordinator,
    _name: String,
    _command: String,
    event: String,
    item: String,
) {
    w.begin();
    let cover = w.cover("inventory", &item);
    w.record(&cover, &event, Some("the-stock-correlation"))
        .await;
}

/// The one Translate request the policy was handed.
fn the_translation(
    w: &Coordinator,
    name: &str,
) -> angzarr_embedded::angzarr_router::hpb::TranslateRequest {
    let script = locked(&named_policy(w, name).script);
    assert_eq!(script.refusals.len(), 1, "{:?}", script.refusals);
    script.refusals[0].clone()
}

#[then(
    regex = r#"^wallet "([\w-]+)" refuses HoldFunds FAILED_PRECONDITION with reason ANGZARR_REASON_COMMAND_REFUSED, refusal_type (\w+), and records nothing$"#
)]
fn wallet_refuses(w: &mut Coordinator, wallet: String, refusal: String) {
    assert_eq!(w.business("wallet").delivered().len(), 1);
    let translation = the_translation(w, "BuyInToWallet");
    let refused = translation.refusal.expect("a refusal");
    assert_eq!(refused.status(), RefusalStatus::FailedPrecondition);
    assert_eq!(refused.typed.as_ref().map(bare), Some(refusal));
    let cover = w.cover("wallet", &wallet);
    assert!(w.persisted(&cover).is_empty());
}

#[then(
    regex = r#"^"(\w+)"'s Translate is called once, with a trigger book holding only the (\w+) event, the (\w+) ReactionDelivery as it was delivered, and a refusal of status (\w+) whose typed object is that (\w+)$"#
)]
fn translate_called_once(
    w: &mut Coordinator,
    name: String,
    event: String,
    command: String,
    refusal_status: String,
    refusal: String,
) {
    let translation = the_translation(w, &name);
    let trigger = translation.trigger.expect("a trigger");
    let events: Vec<String> = trigger
        .pages
        .iter()
        .filter_map(angzarr_embedded::angzarr_router::page_event)
        .map(bare)
        .collect();
    assert_eq!(events, [event]);
    let delivery = translation.command.expect("the refused command");
    let Some(reaction_delivery::Body::Command(any)) = &delivery.body else {
        panic!("a command: {delivery:?}");
    };
    assert_eq!(bare(any), command);
    let delivered = w.business("wallet").delivered().pop().expect("delivered");
    let page = delivered.input.and_then(|b| b.page).expect("a page");
    assert_eq!(delivery.command_id, page.command_id, "as it was delivered");
    let refused = translation.refusal.expect("a refusal");
    assert_eq!(refused.status().as_str_name(), refusal_status);
    assert_eq!(refused.typed.as_ref().map(bare), Some(refusal));
}

#[then(regex = r"^Translate answers the (\w+) payload alone$")]
fn answers_payload_alone(w: &mut Coordinator, fact: String) {
    let input = holds_funds(w).input.clone();
    assert_eq!(facts(w, &input, &fact).len(), 1);
}

#[then(
    regex = r#"^a (\w+) fact is delivered to "([\w-]+)" (?:root|item) "([^"]+)"(?: as a ReactionDelivery in its fact arm)?$"#
)]
fn fact_delivered(w: &mut Coordinator, fact: String, domain: String, name: String) {
    let delivered = facts(w, &domain, &fact);
    assert_eq!(delivered.len(), 1, "{delivered:?}");
    let cover = delivered[0].input.as_ref().and_then(|b| b.cover.clone());
    assert_eq!(cover, Some(w.cover(&domain, &name)));
    assert!(delivered[0].causation.is_some(), "a reaction's");
}

#[then(
    regex = r#"^"(\w+)" handles it as an ordinary fact, with no reference to the wallet's protos$"#
)]
fn ordinary_fact(w: &mut Coordinator, name: String) {
    let domain = w.domain_of(&name);
    for delivered in facts(w, &domain, "") {
        let payload = fact_payload(&delivered).expect("a fact");
        assert!(!payload.type_url.contains("wallet"), "{payload:?}");
    }
}

#[then(
    regex = r#"^exactly one fact is delivered to "([\w-]+)" for the refusal, and its payload is the one Translate answered$"#
)]
fn exactly_one_fact(w: &mut Coordinator, domain: String) {
    let delivered = facts(w, &domain, "");
    assert_eq!(delivered.len(), 1, "{delivered:?}");
    assert_eq!(
        fact_payload(&delivered[0]).map(|p| bare(&p)),
        Some("HoldFundsDeclined".to_string())
    );
}

#[then("the coordinator builds no fact of its own from the refusal")]
fn no_own_fact(w: &mut Coordinator) {
    let input = holds_funds(w).input.clone();
    let delivered = facts(w, &input, "");
    assert!(
        delivered
            .iter()
            .all(|d| fact_payload(d).is_some_and(|p| bare(&p) == "HoldFundsDeclined")),
        "{delivered:?}"
    );
}

#[then(regex = r#"^no handler of "(\w+)" is chosen by the refused command's type (\w+)$"#)]
fn no_handler_by_command(w: &mut Coordinator, name: String, command: String) {
    let domain = w.domain_of(&name);
    for delivered in w.business(&domain).delivered() {
        let page = delivered.input.and_then(|b| b.page).expect("a page");
        let any = match page.body {
            Some(command_page::Body::Command(c)) => c.payload,
            Some(command_page::Body::Fact(f)) => f.payload,
            None => None,
        };
        assert_ne!(any.map(|a| bare(&a)), Some(command.clone()));
    }
}

/// The refusal fact delivered to the policy's source.
fn the_refusal_fact(w: &mut Coordinator) -> HandleRequest {
    let input = holds_funds(w).input.clone();
    let delivered = facts(w, &input, "HoldFundsDeclined");
    assert_eq!(delivered.len(), 1, "{delivered:?}");
    delivered[0].clone()
}

/// The HoldFunds the wallet was delivered.
fn the_hold(w: &mut Coordinator) -> HandleRequest {
    w.business("wallet")
        .delivered()
        .pop()
        .expect("HoldFunds delivered")
}

#[then("the refusal fact's ReactionDelivery command_id is the refused HoldFunds' command_id")]
fn same_command_id(w: &mut Coordinator) {
    let fact = the_refusal_fact(w)
        .input
        .and_then(|b| b.page)
        .expect("a page");
    let hold = the_hold(w).input.and_then(|b| b.page).expect("a page");
    assert_eq!(fact.command_id, hold.command_id);
}

#[then(
    regex = r#"^the fact origin "([\w-]+)" derives has external_id that command_id's canonical text$"#
)]
fn origin_external_id(w: &mut Coordinator, _domain: String) {
    let fact = the_refusal_fact(w)
        .input
        .and_then(|b| b.page)
        .expect("a page");
    let id = fact.command_id.clone().expect("an id").to_string();
    assert_eq!(origin_of(&fact).map(|o| o.external_id), Some(id));
}

#[then("the ReactionDelivery's causation names the BuyInStarted event")]
fn causation_names_trigger(w: &mut Coordinator) {
    let fact = the_refusal_fact(w);
    let causation = fact.causation.expect("a causation");
    let stream = causation.stream.clone().expect("its stream");
    let page = w
        .pages(&stream)
        .into_iter()
        .find(|p| p.sequence == causation.sequence)
        .expect("the trigger");
    let event = angzarr_embedded::angzarr_router::page_event(&page).map(bare);
    assert_eq!(event.as_deref(), Some("BuyInStarted"));
}

#[then(regex = r#"^its correlation_id is "([^"]+)"$"#)]
fn its_correlation(w: &mut Coordinator, correlation: String) {
    let fact = the_refusal_fact(w)
        .input
        .and_then(|b| b.page)
        .expect("a page");
    assert_eq!(fact.correlation_id, Some(root(&correlation)));
}

#[then(regex = r#"^its component is "(\w+)"$"#)]
fn its_component(w: &mut Coordinator, component: String) {
    // The component reaches the destination as the derived origin's
    // description (docs/design/identity-and-streams.md §6.1).
    let fact = the_refusal_fact(w)
        .input
        .and_then(|b| b.page)
        .expect("a page");
    let origin = origin_of(&fact).expect("an origin");
    assert!(origin.description.contains(&component), "{origin:?}");
}

#[then(regex = r#"^wallet "([\w-]+)"'s stream holds no event with that command_id$"#)]
fn no_event_with_id(w: &mut Coordinator, wallet: String) {
    let hold = the_hold(w).input.and_then(|b| b.page).expect("a page");
    let cover = w.cover("wallet", &wallet);
    for page in w.pages(&cover) {
        let cause = page
            .meta
            .as_ref()
            .and_then(|m| crate::tests::world::cause_of(m).0);
        assert_ne!(cause, hold.command_id);
    }
}

#[then(
    regex = r#"^"(\w+)"'s Translate (is not called|is called with the refusal|is called with the (\w+) refusal)$"#
)]
fn translate_called(w: &mut Coordinator, name: String, called: String, refusal: String) {
    let script = locked(&named_policy(w, &name).script);
    assert_eq!(
        script.refusals.len(),
        usize::from(called != "is not called")
    );
    if !refusal.is_empty() {
        let typed = script.refusals[0]
            .refusal
            .as_ref()
            .and_then(|r| r.typed.as_ref())
            .map(bare);
        assert_eq!(typed, Some(refusal));
    }
}

/// How often HoldFunds was delivered.
fn holds(w: &mut Coordinator) -> usize {
    w.business("wallet").delivered().len()
}

#[then(
    regex = r"^HoldFunds is retried with backoff up to its configured cap, then dead-lettered and alerted$"
)]
fn retried_then_dead_lettered(w: &mut Coordinator) {
    assert_eq!(holds(w), w.retry.max_retries as usize + 1);
    assert_eq!(w.waits.waited().len(), w.retry.max_retries as usize);
    assert!(!w.log.letters().is_empty() && !w.alerts().is_empty());
}

#[then(
    regex = r"^the policy step is reloaded and re-run with backoff up to its configured cap, then dead-lettered and alerted$"
)]
fn rerun_then_dead_lettered(w: &mut Coordinator) {
    retried_then_dead_lettered(w);
}

/// The dead letters of HoldFunds itself: its rejected reaction.
fn hold_funds_letters(w: &Coordinator) -> Vec<AngzarrDeadLetter> {
    w.log
        .letters()
        .into_iter()
        .filter(|l| match &l.payload {
            Some(Payload::RejectedReaction(delivery)) => delivery
                .cover
                .as_ref()
                .is_some_and(|c| c.domain == "wallet"),
            _ => false,
        })
        .collect()
}

#[then("HoldFunds is neither retried nor dead-lettered")]
fn neither_retried_nor_dead_lettered(w: &mut Coordinator) {
    assert_eq!(holds(w), 1);
    assert!(
        hold_funds_letters(w).is_empty(),
        "{:?}",
        hold_funds_letters(w)
    );
}

#[then("HoldFunds is dead-lettered and an alert is raised")]
fn hold_funds_dead_lettered(w: &mut Coordinator) {
    assert_eq!(holds(w), 1);
    assert!(!hold_funds_letters(w).is_empty() && !w.alerts().is_empty());
}

/// The events recorded in `domain` since the first when, but for the
/// trigger the scenario recorded itself.
fn recorded_in(w: &Coordinator, domain: &str) -> Vec<String> {
    if !w.domains.contains_key(domain) {
        return Vec::new();
    }
    w.store_covers(domain)
        .iter()
        .flat_map(|c| w.persisted(c))
        .filter_map(|p| angzarr_embedded::angzarr_router::page_event(&p).map(bare))
        .filter(|name| name != "BuyInStarted" && name != "BuyInCancelled")
        .collect()
}

#[then(regex = r#"^nothing is recorded in "([\w-]+)"(?: or "([\w-]+)")?$"#)]
fn nothing_recorded(w: &mut Coordinator, first: String, second: String) {
    for domain in [first, second].iter().filter(|d| !d.is_empty()) {
        assert!(
            recorded_in(w, domain).is_empty(),
            "{domain}: {:?}",
            recorded_in(w, domain)
        );
    }
}

/// The chain's failure, as its failed step answered it.
fn step_failure(w: &Coordinator) -> Answered {
    let (_, result) = w.handled.last().expect("an input was handled");
    match result {
        Err(Failure::Downstream { error, .. }) => Answered::of_step(error),
        other => panic!(
            "a failure down the chain, not {:?}",
            other.as_ref().map(angzarr_embedded::Handled::delivery)
        ),
    }
}

#[then(
    regex = r#"^"(\w+)"'s Translate is refused INTERNAL with reason (\w+), naming command_type (\w+)$"#
)]
fn translate_refused(w: &mut Coordinator, name: String, reason: String, command_type: String) {
    assert_eq!(locked(&named_policy(w, &name).script).refusals.len(), 1);
    let answer = step_failure(w);
    assert_eq!(answer.status(), Some(status("INTERNAL")), "{answer:?}");
    assert_eq!(answer.reason(), Some(reason.as_str()), "{answer:?}");
    assert!(
        answer
            .arg("command_type")
            .is_some_and(|t| t.ends_with(&command_type)),
        "{answer:?}"
    );
}

#[then(
    regex = r"^the policy step is dead-lettered and (?:alerted at the policy|an alert is raised)$"
)]
fn step_dead_lettered(w: &mut Coordinator) {
    let letters = w.log.letters();
    assert!(
        letters
            .iter()
            .any(|l| l.source_component_kind() == ComponentKind::Policy),
        "{letters:?}"
    );
    assert!(!w.alerts().is_empty(), "alerted");
}

#[then(
    regex = r#"^the refusal fact is refused INTERNAL by "([\w-]+)"'s router with reason (\w+)$"#
)]
fn refusal_fact_refused(w: &mut Coordinator, domain: String, reason: String) {
    let letters = w.log.letters();
    let letter = letters
        .iter()
        .find(|l| reason_of(l).is_some_and(|r| r.code == reason))
        .unwrap_or_else(|| panic!("no letter of {reason}: {letters:?} {:?}", w.answers()));
    let Some(Payload::RejectedReaction(delivery)) = &letter.payload else {
        panic!("a rejected reaction: {letter:?}");
    };
    assert_eq!(
        delivery.cover.as_ref().map(|c| c.domain.as_str()),
        Some(domain.as_str())
    );
}

#[then(regex = r#"^it is dead-lettered and alerted at "([\w-]+)"$"#)]
fn dead_lettered_at(w: &mut Coordinator, domain: String) {
    let letters = w.log.letters();
    assert!(
        letters.iter().any(|l| l.source_component == domain),
        "{letters:?}"
    );
    assert!(!w.alerts().is_empty());
}

#[then(regex = r#"^"(\w+)" records nothing$"#)]
fn records_nothing(w: &mut Coordinator, name: String) {
    let domain = w.domain_of(&name);
    assert!(
        recorded_in(w, &domain).is_empty(),
        "{:?}",
        recorded_in(w, &domain)
    );
}

/// Decoded varint fields 1 and 2 of an InsufficientFunds.
fn funds(object: &Any) -> (u64, u64) {
    let (mut available, mut requested) = (0, 0);
    let mut bytes = object.value.iter().copied();
    while let Some(tag) = bytes.next() {
        let mut n = 0u64;
        let mut shift = 0;
        for byte in bytes.by_ref() {
            n |= u64::from(byte & 0x7f) << shift;
            shift += 7;
            if byte < 0x80 {
                break;
            }
        }
        match tag {
            0x08 => available = n,
            0x10 => requested = n,
            _ => {}
        }
    }
    (available, requested)
}

#[then(
    regex = r#"^wallet "([\w-]+)"'s HoldFunds is refused FAILED_PRECONDITION with reason ANGZARR_REASON_COMMAND_REFUSED, refusal_type (\w+), and the (\w+) object with available (\d+) and requested (\d+) in the status details$"#
)]
fn refused_with_object(
    w: &mut Coordinator,
    _wallet: String,
    refusal: String,
    _object: String,
    available: u64,
    requested: u64,
) {
    let translation = the_translation(w, "BuyInToWallet");
    let refused = translation.refusal.expect("a refusal");
    assert_eq!(refused.status(), RefusalStatus::FailedPrecondition);
    let typed = refused.typed.expect("its object");
    assert_eq!(bare(&typed), refusal);
    assert_eq!(funds(&typed), (available, requested));
}

#[then(
    regex = r#"^"(\w+)"'s Translate receives a refusal whose status is (\w+) and whose typed object unpacks to (\w+) with available (\d+) and requested (\d+)$"#
)]
fn receives_refusal(
    w: &mut Coordinator,
    name: String,
    refusal_status: String,
    refusal: String,
    available: u64,
    requested: u64,
) {
    let translation = the_translation(w, &name);
    let refused = translation.refusal.expect("a refusal");
    assert_eq!(refused.status().as_str_name(), refusal_status);
    let typed = refused.typed.expect("its object");
    assert_eq!(bare(&typed), refusal);
    assert_eq!(funds(&typed), (available, requested));
}

#[then("the refusal carries no text")]
fn no_text(w: &mut Coordinator) {
    let translation = the_translation(w, "BuyInToWallet");
    let refused = translation.refusal.expect("a refusal");
    // A status and a typed object: there is no field for text.
    assert!(refused.typed.is_some());
}

#[then(
    regex = r#"^CloseHold is answered INTERNAL with reason (\w+), cause (\w+) and cause\.wallet "([\w-]+)"$"#
)]
fn close_hold_answered(w: &mut Coordinator, reason: String, cause: String, wallet: String) {
    let answer = step_failure(w);
    assert_eq!(answer.status(), Some(status("INTERNAL")), "{answer:?}");
    assert_eq!(answer.reason(), Some(reason.as_str()), "{answer:?}");
    assert_eq!(answer.arg("cause"), Some(cause.as_str()));
    assert_eq!(answer.arg("cause.wallet"), Some(wallet.as_str()));
}

#[then(
    regex = r"^the (\w+) delivery is dead-lettered (?:and alerted|with that reason and alerted)$"
)]
fn delivery_dead_lettered(w: &mut Coordinator, command: String) {
    let letters = hold_funds_letters(w);
    assert!(
        letters.iter().any(|l| match &l.payload {
            Some(Payload::RejectedReaction(delivery)) => match &delivery.body {
                Some(reaction_delivery::Body::Command(any)) => bare(any) == command,
                _ => false,
            },
            _ => false,
        }),
        "{letters:?}"
    );
    assert!(!w.alerts().is_empty());
}

#[then(
    regex = r#"^HoldFunds is refused FAILED_PRECONDITION with reason (\w+), stream wallet "([\w-]+)" in edition "([\w-]+)" and edition "[\w-]+"$"#
)]
fn hold_funds_not_forked(w: &mut Coordinator, reason: String, wallet: String, edition: String) {
    // The reaction's refusal has no caller: its dead letter records it.
    let answer = crate::tests::editions::reaction_letter(w).expect("the reaction's dead letter");
    assert_eq!(
        answer.status(),
        Some(status("FAILED_PRECONDITION")),
        "{answer:?}"
    );
    assert_eq!(answer.reason(), Some(reason.as_str()));
    let cover = w.cover("wallet", &wallet);
    let stream = format!("wallet/{}@{edition}", cover.root.expect("a root"));
    assert_eq!(answer.arg("stream"), Some(stream.as_str()));
}
