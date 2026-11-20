//! Givens and whens about policies: what each one's business code answers
//! a source event with, and the source events recorded for them to react
//! to.

use cucumber::{given, when};

use angzarr_embedded::v1::Cover;

use crate::tests::policy::{locked, Arm, Reaction, Root, Translation};
use crate::tests::world::Coordinator;

/// A policy as a scenario introduces it: `"OrderToInventory" translating
/// from "order" to "inventory"`.
#[derive(Debug, Default)]
pub(crate) struct Header {
    pub(crate) name: String,
    pub(crate) input: String,
    pub(crate) output: String,
}

/// The quoted words of `text`, in order.
fn quoted(text: &str) -> Vec<String> {
    text.split('"')
        .skip(1)
        .step_by(2)
        .map(str::to_string)
        .collect()
}

impl std::str::FromStr for Header {
    type Err = String;

    fn from_str(text: &str) -> Result<Header, String> {
        match &quoted(text)[..] {
            [name, input, output] => Ok(Header {
                name: name.clone(),
                input: input.clone(),
                output: output.clone(),
            }),
            _ => Err(format!("no policy header: {text}")),
        }
    }
}

/// A stream as a scenario names it: `"inventory" "inv-7"`.
#[derive(Debug, Default)]
pub(crate) struct Address {
    pub(crate) domain: String,
    pub(crate) root: String,
}

impl std::str::FromStr for Address {
    type Err = String;

    fn from_str(text: &str) -> Result<Address, String> {
        match &quoted(text)[..] {
            [domain, root] => Ok(Address {
                domain: domain.clone(),
                root: root.clone(),
            }),
            _ => Err(format!("no stream: {text}")),
        }
    }
}

/// A reaction on `on`: a `command` of `domain`'s package, at `root`.
pub(crate) fn reaction(on: &str, domain: &str, command: &str, root: Root) -> Reaction {
    Reaction {
        on: on.to_string(),
        domain: domain.to_string(),
        command: command.to_string(),
        arm: Arm::Command,
        root,
    }
}

/// `reaction` added to the policy `name`'s script, the policy registered.
pub(crate) fn reacts(w: &mut Coordinator, header: &Header, reaction: Reaction) {
    let script = w.policy(&header.name, &header.input, &header.output);
    locked(&script).reactions.push(reaction);
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+")(?:, whose descriptor names output domain "[\w-]+")?$"#
)]
fn policy(w: &mut Coordinator, header: Header) {
    w.policy(&header.name, &header.input, &header.output);
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+") that sends (\w+) to ("[\w-]+" "[^"]+") on (\w+)$"#
)]
fn sends_to_root(w: &mut Coordinator, header: Header, command: String, to: Address, on: String) {
    let reaction = reaction(&on, &to.domain, &command, Root::Named(to.root));
    reacts(w, &header, reaction);
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+") that sends (\w+) to "([\w-]+)" root the trigger's correlation on (\w+)$"#
)]
fn sends_to_correlation(
    w: &mut Coordinator,
    header: Header,
    command: String,
    domain: String,
    on: String,
) {
    let reaction = reaction(&on, &domain, &command, Root::TriggerCorrelation);
    reacts(w, &header, reaction);
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+") that sends (\w+) with no root on (\w+)$"#
)]
fn sends_with_no_root(w: &mut Coordinator, header: Header, command: String, on: String) {
    let reaction = reaction(&on, &header.output, &command, Root::Unnamed);
    reacts(w, &header, reaction);
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+") that builds (?:a )?(\w+)(?: command)? from (\w+)(?:, setting its root to the trigger's correlation)?$"#
)]
fn builds(w: &mut Coordinator, header: Header, command: String, on: String) {
    let reaction = reaction(&on, &header.output, &command, Root::TriggerCorrelation);
    reacts(w, &header, reaction);
}

/// The policy registered last.
pub(crate) fn last_policy(w: &Coordinator) -> Header {
    let def = w.policies.last().expect("a policy");
    Header {
        name: def.name.clone(),
        input: def.input.clone(),
        output: def.output.clone(),
    }
}

#[given(
    regex = r#"^its business code answers an (\w+) event with an? (\w+) command for ("[\w-]+" "[^"]+")(?: and an? (\w+) command for "([\w-]+)")?$"#
)]
fn its_business_code_answers(
    w: &mut Coordinator,
    on: String,
    first: String,
    to: Address,
    second: String,
    other: String,
) {
    let header = last_policy(w);
    reacts(
        w,
        &header,
        reaction(&on, &to.domain, &first, Root::Named(to.root)),
    );
    if !second.is_empty() {
        let elsewhere = reaction(&on, &other, &second, Root::Named("elsewhere-1".to_string()));
        reacts(w, &header, elsewhere);
    }
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+") whose business code answers every (\w+) event with an? (\w+) command for another order, a new one each time$"#
)]
fn answers_another_stream(w: &mut Coordinator, header: Header, on: String, command: String) {
    let reaction = reaction(&on, &header.output, &command, Root::Another);
    reacts(w, &header, reaction);
}

#[given(regex = r"^the policy handles (\w+) by emitting an? (\w+) (command|fact)$")]
fn handles_by_emitting(w: &mut Coordinator, on: String, command: String, arm: String) {
    let header = last_policy(w);
    let mut reaction = reaction(
        &on,
        &header.output,
        &command,
        Root::Named("t-1".to_string()),
    );
    reaction.arm = arm_of(&arm);
    reacts(w, &header, reaction);
}

/// An arm as a scenario names it.
pub(crate) fn arm_of(text: &str) -> Arm {
    match text {
        "fact" => Arm::Fact,
        _ => Arm::Command,
    }
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+") that sends (?:the fact (\w+)|(\w+) as an? (command|fact)) to ("[\w-]+" "[^"]+") on (\w+)$"#
)]
fn sends_arm_to_root(
    w: &mut Coordinator,
    header: Header,
    fact: String,
    named: String,
    arm: String,
    to: Address,
    on: String,
) {
    let (command, arm) = match fact.is_empty() {
        false => (fact, Arm::Fact),
        true => (named, arm_of(&arm)),
    };
    let mut reaction = reaction(&on, &to.domain, &command, Root::Named(to.root));
    reaction.arm = arm;
    reacts(w, &header, reaction);
}

/// Two outputs as a scenario lists them, in order: `a ReserveStock command
/// and then a StockHoldRequested fact`, each a type and whether it is a
/// fact.
#[derive(Debug, Default)]
struct Outputs(Vec<(String, Arm)>);

impl std::str::FromStr for Outputs {
    type Err = String;

    fn from_str(text: &str) -> Result<Outputs, String> {
        text.split(" and then ")
            .map(|output| match output.split(' ').collect::<Vec<_>>()[..] {
                ["a" | "an", name, arm @ ("command" | "fact")] => {
                    Ok((name.to_string(), arm_of(arm)))
                }
                _ => Err(format!("no output: {output}")),
            })
            .collect::<Result<_, _>>()
            .map(Outputs)
    }
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+") whose business code answers an (\w+) event with (an? \w+ (?:command|fact) and then an? \w+ (?:command|fact)) for ("[\w-]+" "[^"]+")$"#
)]
fn answers_in_order(
    w: &mut Coordinator,
    header: Header,
    on: String,
    outputs: Outputs,
    to: Address,
) {
    for (name, arm) in outputs.0 {
        let mut reaction = reaction(&on, &to.domain, &name, Root::Named(to.root.clone()));
        reaction.arm = arm;
        reacts(w, &header, reaction);
    }
}

/// From here on the last policy sends each of its reactions of type
/// `command` as a fact.
pub(crate) fn sends_as_fact(w: &Coordinator, command: &str) {
    let def = w.policies.last().expect("a policy");
    for reaction in &mut locked(&def.script).reactions {
        if reaction.command == command {
            reaction.arm = Arm::Fact;
        }
    }
}

#[given(
    regex = r#"^no component of "([\w-]+)" is registered with the policy's coordinator, and it knows no coordinator for "[\w-]+"$"#
)]
fn unregistered(w: &mut Coordinator, domain: String) {
    w.absent.insert(domain);
}

#[given(regex = r"^a coordinator whose chain depth limit is (\d+)$")]
fn depth_limit(w: &mut Coordinator, limit: usize) {
    w.depth = Some(limit);
}

#[given(regex = r#"^no policy translates from "([\w-]+)" to "([\w-]+)"$"#)]
fn no_policy(w: &mut Coordinator, input: String, output: String) {
    assert!(!w
        .policies
        .iter()
        .any(|p| p.input == input && p.output == output));
}

#[given(regex = r#"^"(\w+)"'s callback fails with INTERNAL$"#)]
fn callback_fails(w: &mut Coordinator, name: String) {
    let def = w
        .policies
        .iter()
        .find(|p| p.name == name)
        .expect("the policy");
    let code = crate::tests::policy::internal("CALLBACK_FAILED");
    locked(&def.script).translation = Some(Translation::Fails(code));
}

#[when(
    regex = r#"^an? (\w+) event (?:with correlation "([^"]+)" )?is recorded for order "([\w-]+)"(?: on the main timeline| in edition "([\w-]+)"| at sequence (\d+))?$"#
)]
async fn recorded_for_order(
    w: &mut Coordinator,
    event: String,
    correlation: String,
    order: String,
    edition: String,
    at: String,
) {
    let cover = Cover {
        edition: (!edition.is_empty()).then_some(edition),
        ..w.cover("order", &order)
    };
    if let Ok(at) = at.parse::<usize>() {
        // The events before it, written as a given's are.
        let before = at - w.pages(&cover).len();
        w.write(&cover, &vec!["OrderNoteAdded"; before]);
    }
    w.begin();
    let correlation = match correlation.is_empty() {
        true => w.order_correlation.clone(),
        false => Some(correlation),
    };
    w.record(&cover, &event, correlation.as_deref()).await;
}

#[when(regex = r#"^an AddItem command for "order" "([\w-]+)" is handled$"#)]
async fn add_item_handled(w: &mut Coordinator, order: String) {
    w.begin();
    let cover = w.cover("order", &order);
    let book = w.command(&cover, "AddItem");
    w.handle(book).await;
}

/// The policy a scenario's "policy-emitted" command comes from: `Reactor`,
/// from `payment` to `domain`, sending `command` to the stream `root` on
/// each PaymentReceived.
pub(crate) fn reactor(w: &mut Coordinator, domain: &str, command: &str, root: Root) {
    let header = Header {
        name: "Reactor".to_string(),
        input: "payment".to_string(),
        output: domain.to_string(),
    };
    let reaction = reaction("PaymentReceived", domain, command, root);
    reacts(w, &header, reaction);
}

/// A PaymentReceived recorded on the payment stream: what sets
/// [`reactor`] off. Pending, for the scenario's when to handle.
pub(crate) fn payment_received(w: &mut Coordinator) -> angzarr_embedded::v1::CommandBook {
    let cover = w.cover("payment", "p-1");
    w.command(&cover, "RecordPaymentReceived")
}

#[given(
    regex = r"^a policy's (\w+) reaction(?:, delivered as a ReactionDelivery with no Handling)?$"
)]
fn policy_emitted(w: &mut Coordinator, command: String) {
    reactor(w, "order", &command, Root::Named("order-1".to_string()));
    let book = payment_received(w);
    w.pending.push(book);
}

#[given(
    regex = r#"^a policy "(\w+)" translating from "([\w-]+)" to "([\w-]+)" that sends (\w+) to ("[\w-]+" "[^"]+") on (\w+), with no expected sequence$"#
)]
fn sends_unhandled(
    w: &mut Coordinator,
    name: String,
    input: String,
    output: String,
    command: String,
    to: Address,
    on: String,
) {
    let header = Header {
        name,
        input,
        output,
    };
    let reaction = reaction(&on, &to.domain, &command, Root::Named(to.root.clone()));
    reacts(w, &header, reaction);
    // The stream it reaches holds three events, its head at 3.
    let cover = w.cover(&to.domain, &to.root);
    if w.pages(&cover).is_empty() {
        w.write(&cover, &["OrderCreated", "ItemAdded", "OrderNoteAdded"]);
    }
    w.speak_of(&to.root);
}

#[given(regex = r#"^a (\w+) event at sequence (\d+) of a "([\w-]+)" stream$"#)]
fn event_at_sequence(w: &mut Coordinator, event: String, sequence: usize, domain: String) {
    let cover = w.cover(&domain, "p-1");
    let mut names = vec!["PaymentOpened"; sequence];
    names.push(&event);
    w.write(&cover, &names);
}

#[when(regex = r#"^the policy "(\w+)" runs on the (\w+) event$"#)]
async fn policy_runs(w: &mut Coordinator, _name: String, _event: String) {
    w.begin();
    let resumed = w.resume().await;
    w.resumed.push(resumed);
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+") whose business code answers an (\w+) event with one (command|fact) Output (\w+) for root "([^"]+)"$"#
)]
fn answers_one_output(
    w: &mut Coordinator,
    header: Header,
    on: String,
    arm: String,
    name: String,
    root: String,
) {
    let mut reaction = reaction(&on, &header.output, &name, Root::Named(root));
    reaction.arm = arm_of(&arm);
    reacts(w, &header, reaction);
    owned_elsewhere(w, &header.output);
}

/// `domain` owned by a coordinator elsewhere, so each `ReactionDelivery`
/// the policy's coordinator stamps for it is seen whole as it leaves.
fn owned_elsewhere(w: &mut Coordinator, domain: &str) {
    w.remotes.entry(domain.to_string()).or_default();
}

#[given(
    regex = r#"^its business code answers an (\w+) event with two command Outputs, (\w+) for root "([^"]+)" and (\w+) for root "([^"]+)"$"#
)]
fn answers_two_outputs(
    w: &mut Coordinator,
    on: String,
    first: String,
    first_root: String,
    second: String,
    second_root: String,
) {
    let header = last_policy(w);
    for (name, root) in [(first, first_root), (second, second_root)] {
        let reaction = reaction(&on, &header.output, &name, Root::Named(root));
        reacts(w, &header, reaction);
    }
    owned_elsewhere(w, &header.output);
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+") whose reaction to an? (\w+) event is an? (\w+) command for this "order" stream$"#
)]
fn reaction_for_this_stream(w: &mut Coordinator, header: Header, on: String, command: String) {
    let order = w.order().root.expect("a root").to_string();
    let reaction = reaction(&on, &header.output, &command, Root::Named(order));
    reacts(w, &header, reaction);
}

#[when("the policy's coordinator delivers the reaction through Deliver")]
async fn delivers_through_deliver(w: &mut Coordinator) {
    // The policy's coordinator is this kernel: its reaction to a source
    // event is delivered as a step of the source's chain.
    let def = w.policies.last().expect("a policy");
    let on = locked(&def.script).reactions[0].on.clone();
    let cover = w.cover(&def.input.clone(), "p-1");
    w.begin();
    w.record(&cover, &on, None).await;
}
