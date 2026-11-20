//! Givens declaring the protos: components and their `ComponentOptions`,
//! and the `(command)`, `(fact)` and `(event)` entries naming them.

use cucumber::given;

use crate::protos::{Status, full};
use crate::world::Generator;

/// `domain`'s aggregate, `<Domain>State`.
fn state_of(domain: &str) -> String {
    let mut chars = domain.chars();
    let first = chars
        .next()
        .map(|c| c.to_ascii_uppercase().to_string())
        .unwrap_or_default();
    format!("{first}{}State", chars.as_str())
}

/// The scenarios' kinds of component, as they name them.
fn kind_of(words: &str) -> &'static str {
    match words {
        "an aggregate" => "AGGREGATE",
        "a process manager" => "PROCESS_MANAGER",
        "a policy" => "POLICY",
        "a projector" => "PROJECTOR",
        other => panic!("no kind {other}"),
    }
}

/// A stateful component `name` owning `domain`, and what the scenario says
/// of it after: an input (or two, `commands "a" and "b"`) with the events it
/// emits, an event it folds, or that it names nothing.
#[given(
    regex = r#"^(an aggregate|a process manager) "([\w-]+)" owning domain "([\w-]+)"(?: with (?:the )?(?:(command|fact|message|input)s? )?"([^"]+)"(?: and "([^"]+)")?(?: whose emits lists "([^"]+)")?| that folds "([^"]+)"(?: and takes no fact)?| that takes no fact| that no \(command\), \(fact\) or \(event\) entry names)?$"#
)]
#[allow(clippy::too_many_arguments)]
fn stateful(
    w: &mut Generator,
    words: String,
    name: String,
    domain: String,
    kind: String,
    input: String,
    second: String,
    emits: String,
    folded: String,
) {
    w.component(&name, kind_of(&words), &domain).domain = Some(domain.clone());
    let component = w.named(&name);
    match kind.as_str() {
        "message" => {
            w.protos.message(&full(&input));
        }
        _ if !input.is_empty() => {
            for input in [input, second].iter().filter(|i| !i.is_empty()) {
                let declared = w.input(input, &name);
                declared.fact = kind == "fact";
            }
            if !emits.is_empty() {
                // An emitted event is one of the component's: it folds it.
                let input = w.last_input.clone().expect("an input");
                let event = full(&emits);
                w.protos
                    .message(&input)
                    .input
                    .as_mut()
                    .expect("an input")
                    .emits
                    .push(event.clone());
                w.protos
                    .message(&event)
                    .events
                    .push((component.clone(), None));
            }
        }
        _ => {}
    }
    if !folded.is_empty() {
        w.protos
            .message(&full(&folded))
            .events
            .push((component, None));
    }
}

/// A process manager with an input, and the option the input carries.
#[given(
    regex = r#"^a process manager "([\w-]+)" owning domain "([\w-]+)" with input "([^"]+)" carrying \((command|fact)\)$"#
)]
fn pm_carrying(w: &mut Generator, name: String, domain: String, input: String, option: String) {
    w.component(&name, "PROCESS_MANAGER", &domain).domain = Some(domain.clone());
    w.input(&input, &name).fact = option == "fact";
}

#[given(
    regex = r#"^(an aggregate|a process manager)(?: component)? "(\w+)" owning domain "([\w-]+)" whose state message is "([^"]+)"(?: with input "([^"]+)")?$"#
)]
fn with_state(
    w: &mut Generator,
    words: String,
    name: String,
    domain: String,
    state: String,
    input: String,
) {
    w.declare(&name, &full(&state), kind_of(&words)).domain = Some(domain);
    if !input.is_empty() {
        w.input(&input, &name);
    }
}

/// An aggregate or a process manager component, named by its domain (the
/// outline of C-0716 leaves a trailing space where its row names no
/// events).
#[given(regex = r#"^(an aggregate|a process manager) component "([\w-]+)"\s*$"#)]
fn component_of_kind(w: &mut Generator, words: String, name: String) {
    w.component(&name, kind_of(&words), &name).domain = Some(name.clone());
}

/// The projector row of C-0716: the projector names its event. The row
/// declares no aggregate, though its comment says the event's domain has
/// one folding it: the entry carries the domain, as for an owner outside
/// the set.
#[given(regex = r#"^a projector component "([\w-]+)" whose \(event\) entries name "([^"]+)"$"#)]
fn projector_component_with_event(w: &mut Generator, name: String, event: String) {
    w.component(&name, "PROJECTOR", &name);
    let component = w.named(&name);
    let domain = event.split('.').next().unwrap_or_default().to_string();
    w.protos
        .message(&full(&event))
        .events
        .push((component, Some(domain)));
}

#[given(
    regex = r#"^an aggregate anchored on "([^"]+)" owning domain "([\w-]+)" whose component_name is (empty|"[^"]*")$"#
)]
fn anchored(w: &mut Generator, anchor: String, domain: String, name: String) {
    let component = w.declare(&anchor, &full(&anchor), "AGGREGATE");
    component.domain = Some(domain);
    component.component_name = Some(name.trim_matches('"').replace("empty", ""));
}

/// The aggregates of the domains named, `<Domain>State` each (C-0582,
/// which has no event to fold).
#[given(regex = r#"^aggregates owning domains "([\w-]+)" and "([\w-]+)"$"#)]
fn aggregates(w: &mut Generator, first: String, second: String) {
    for domain in [first, second] {
        let name = state_of(&domain);
        w.component(&name, "AGGREGATE", &domain).domain = Some(domain.clone());
    }
}

#[given(
    regex = r#"^aggregates "(\w+)" owning domain "([\w-]+)" and "(\w+)" owning domain "([\w-]+)"$"#
)]
fn two_aggregates(w: &mut Generator, first: String, a: String, second: String, b: String) {
    for (name, domain) in [(first, a), (second, b)] {
        w.component(&name, "AGGREGATE", &domain).domain = Some(domain.clone());
    }
}

/// A policy, in a package of its own: it owns no domain.
#[given(
    regex = r#"^a policy "([\w-]+)" with (?:output_domain "([\w-]+)"|no output_domain)(?: whose \(event\) entries name "([^"]+)")?$"#
)]
fn policy(w: &mut Generator, name: String, output: String, event: String) {
    w.component(&name, "POLICY", "policies").output_domain = Some(output).filter(|o| !o.is_empty());
    if !event.is_empty() {
        w.consumed(&event, &name);
    }
}

#[given(regex = r#"^a projector "([\w-]+)" whose \(event\) entries name "([^"]+)" and "([^"]+)"$"#)]
fn projector(w: &mut Generator, name: String, first: String, second: String) {
    w.component(&name, "PROJECTOR", "projectors");
    w.consumed(&first, &name);
    w.consumed(&second, &name);
}

/// The component declared by `name` has `declared` starting types.
#[given(regex = r#"^"([\w-]+)" declares starting_types "([^"]+)"$"#)]
fn starting_types(w: &mut Generator, name: String, listed: String) {
    let anchor = w.named(&name);
    let listed = full(&listed);
    w.protos
        .message(&anchor)
        .component
        .as_mut()
        .expect("a component")
        .starting_types
        .push(listed);
}

#[given(regex = r#"^"([\w-]+)" declares no starting_types$"#)]
fn no_starting_types(w: &mut Generator, name: String) {
    let anchor = w.named(&name);
    let component = w
        .protos
        .message(&anchor)
        .component
        .as_mut()
        .expect("a component");
    assert!(component.starting_types.is_empty());
}

#[given(regex = r#"^"([\w-]+)" takes the command "([^"]+)" and the fact "([^"]+)"$"#)]
fn takes(w: &mut Generator, name: String, command: String, fact: String) {
    w.input(&command, &name);
    w.input(&fact, &name).fact = true;
}

#[given(regex = r#"^the message "([^"]+)" carries \((command|fact)\) naming "(\w+)"$"#)]
fn carries(w: &mut Generator, message: String, option: String, component: String) {
    w.input(&message, &component).fact = option == "fact";
}

#[given(
    regex = r#"^an \(event\) entry for "([^"]+)" naming "([\w-]+)" with the domain override "([\w-]+)"$"#
)]
fn event_override(w: &mut Generator, event: String, component: String, domain: String) {
    let component = w.named(&component);
    w.protos
        .message(&full(&event))
        .events
        .push((component, Some(domain)));
}

#[given(regex = r#"^no \(event\) entry for "([^"]+)" names "(\w+)"$"#)]
fn no_event_entry(w: &mut Generator, event: String, component: String) {
    let component = w.named(&component);
    let message = w.protos.message(&full(&event));
    message.events.retain(|(named, _)| *named != component);
}

#[given(regex = r#"^the entry for "([\w-]+)" has no domain override$"#)]
fn no_override(w: &mut Generator, component: String) {
    let component = w.named(&component);
    let overridden = w.protos.messages.values().any(|m| {
        m.events
            .iter()
            .any(|(named, domain)| *named == component && domain.is_some())
    });
    assert!(!overridden, "{component} has an override");
}

#[given(
    regex = r#"^no (?:component in the descriptor set|aggregate or process manager) folds "([^"]+)"$"#
)]
fn no_folder(w: &mut Generator, event: String) {
    let event = full(&event);
    let entries = w.protos.message(&event).events.clone();
    for (component, _) in entries {
        let kind = w
            .protos
            .messages
            .get(&component)
            .and_then(|m| m.component.as_ref())
            .map(|c| c.kind.clone());
        assert!(
            !matches!(kind.as_deref(), Some("AGGREGATE" | "PROCESS_MANAGER")),
            "{component} folds {event}"
        );
    }
}

#[given(
    regex = r#"^a command "([^"]+)" for "(\w+)" whose \(command\)\.(emits|refuses) lists "([^"]+)"$"#
)]
fn command_listing(
    w: &mut Generator,
    command: String,
    component: String,
    list: String,
    listed: String,
) {
    let input = w.input(&command, &component);
    match list.as_str() {
        "emits" => input.emits.push(full(&listed)),
        _ => input.refuses.push(full(&listed)),
    }
    // A refusal type is a message of its own, unless the scenario names
    // one that does not resolve (`wallet.NoSuchRefusal`).
    if list == "refuses" && !listed.contains("NoSuch") {
        w.protos.message(&full(&listed));
    }
}

#[given(regex = r#"^the business binary is built as version "([^"]+)"$"#)]
fn built_as(w: &mut Generator, version: String) {
    w.version = Some(version);
}

/// A policy or projector, in a package of its own, that no `(event)` entry
/// names.
#[given(
    regex = r#"^(a policy with output_domain "([\w-]+)"|a projector) "([\w-]+)" that no \(event\) entry names$"#
)]
fn names_no_event(w: &mut Generator, _words: String, output: String, name: String) {
    let (kind, package) = if output.is_empty() {
        ("PROJECTOR", "projectors")
    } else {
        ("POLICY", "policies")
    };
    w.component(&name, kind, package).output_domain = Some(output).filter(|o| !o.is_empty());
}

/// `component` names each of `events`, whose owners are the aggregates of
/// their packages' domains.
#[given(regex = r#"^\(event\) entries naming "([\w-]+)" for "([^"]+)" and "([^"]+)"$"#)]
fn event_entries(w: &mut Generator, component: String, first: String, second: String) {
    w.consumed(&first, &component);
    w.consumed(&second, &component);
}

/// An `(event)` entry with no override, folding or not as the component
/// is.
#[given(regex = r#"^an \(event\) entry for "([^"]+)" naming "([\w-]+)"$"#)]
fn event_entry(w: &mut Generator, event: String, component: String) {
    let component = w.named(&component);
    w.protos
        .message(&full(&event))
        .events
        .push((component, None));
}

/// An `(event)` entry naming a component by its anchor message's full
/// name, whatever the set holds.
#[given(regex = r#"^an \(event\) entry for "([^"]+)" naming the component "([^"]+)"$"#)]
fn event_entry_for_component(w: &mut Generator, event: String, component: String) {
    w.protos
        .message(&full(&event))
        .events
        .push((component, None));
}

#[given(regex = r#"^no message "([^"]+)" is in the descriptor set being generated$"#)]
fn no_message(w: &mut Generator, message: String) {
    assert!(!w.protos.messages.contains_key(&message), "{message}");
}

#[given(regex = r#"^"([^"]+)" refuses with "([^"]+)"(?: and "([^"]+)")?$"#)]
fn refuses_with(w: &mut Generator, command: String, refusal: String, second: String) {
    for refusal in [refusal, second].iter().filter(|r| !r.is_empty()) {
        let input = w
            .protos
            .message(&full(&command))
            .input
            .as_mut()
            .expect("a command");
        input.refuses.push(full(refusal));
        w.protos.message(&full(refusal));
    }
}

#[given(regex = r#"^"([^"]+)" declares no refuses$"#)]
fn declares_no_refuses(w: &mut Generator, command: String) {
    let input = w
        .protos
        .message(&full(&command))
        .input
        .as_ref()
        .expect("a command");
    assert!(input.refuses.is_empty(), "{command} refuses");
}

#[given(
    regex = r#"^the message "([^"]+)"(?:, which no command's refuses names,)? (carries no \(refusal_status\)|is marked \(refusal_status\) (\w+))$"#
)]
fn refusal_status(w: &mut Generator, message: String, said: String, status: String) {
    w.protos.message(&full(&message)).status = if status.is_empty() {
        assert!(said.starts_with("carries no"));
        Status::Absent
    } else {
        Status::Marked(status)
    };
}

/// The framework's own message, imported, marked as the scenario says.
#[given(
    regex = r#"^the descriptor set includes "io\.angzarr\.v1\.ExpectedSequenceMismatch", marked \(refusal_status\) (\w+)$"#
)]
fn includes_sequence_mismatch(w: &mut Generator, status: String) {
    assert_eq!(status, "REFUSAL_STATUS_FAILED_PRECONDITION");
    w.protos
        .imports
        .push("io/angzarr/v1/types.proto".to_string());
}

#[given(regex = r#"^no command's refuses names "([^"]+)"$"#)]
fn no_command_refuses(w: &mut Generator, message: String) {
    let refused = w
        .protos
        .messages
        .values()
        .filter_map(|m| m.input.as_ref())
        .any(|i| i.refuses.contains(&message));
    assert!(!refused, "{message}");
}

#[given(regex = r#"^the message "([^"]+)" carries \(command\) and \(fact\) naming "(\w+)"$"#)]
fn carries_both(w: &mut Generator, message: String, component: String) {
    let input = w.input(&message, &component);
    input.fact = true;
    input.both = true;
}
