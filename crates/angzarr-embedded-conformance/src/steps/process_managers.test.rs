//! Process managers: each a domain behind its own coordinator, its
//! instances streams rooted at a correlation, started only by its starting
//! inputs. An instance's state is its stream: a scenario's "state
//! awaiting stock" is an `AwaitingStock` event on it.

use cucumber::{given, then, when};

use angzarr_embedded::angzarr_router::error::GrpcCode;
use angzarr_embedded::angzarr_router::hpb::HandleRequest;
use angzarr_embedded::v1::command_page;
use angzarr_embedded::Handled;

use crate::tests::business::{rejection, Rule};
use crate::tests::policy::{bare, internal, locked, Reaction, Root};
use crate::tests::world::{camel, cause_of, origin_of, root, Coordinator};

/// `name`'s process manager owning `domain`, starting from `starting` (by
/// default `Start<Name>`).
fn process_manager(w: &mut Coordinator, name: &str, domain: &str, starting: &str) {
    let starting = match starting.is_empty() {
        true => format!("Start{name}"),
        false => starting.to_string(),
    };
    w.names.insert(name.to_string(), domain.to_string());
    w.process_manager(domain, &[starting.as_str()]);
}

/// The event an instance's state is: `awaiting stock` is `AwaitingStock`.
fn state_event(state: &str) -> String {
    camel(state)
}

#[given(regex = r#"^an? "([\w-]+)" aggregate "([^"]+)" on the main timeline$"#)]
fn aggregate_on_main(w: &mut Coordinator, domain: String, _root: String) {
    w.domain(&domain);
}

#[given(
    regex = r#"^a process manager "(\w+)" owning domain "([\w-]+)"(?: whose (?:(\w+) command is a starting input|starting_types lists (\w+)))?$"#
)]
fn owning_domain(
    w: &mut Coordinator,
    name: String,
    domain: String,
    starting: String,
    listed: String,
) {
    process_manager(w, &name, &domain, &(starting + &listed));
}

#[given(
    regex = r#"^a process manager "(\w+)" owning domain "([\w-]+)" with an instance (?:for correlation|at root) "([^"]+)"( that is completed)?$"#
)]
fn with_instance(
    w: &mut Coordinator,
    name: String,
    domain: String,
    correlation: String,
    completed: String,
) {
    process_manager(w, &name, &domain, "");
    let cover = w.cover(&domain, &correlation);
    w.write(&cover, &[&format!("{name}Started")]);
    if !completed.is_empty() {
        w.write(&cover, &[&format!("{name}Completed")]);
    }
}

#[given(regex = r#"^process managers "(\w+)" and "(\w+)", each behind its own coordinator$"#)]
fn two_process_managers(w: &mut Coordinator, first: String, second: String) {
    for name in [first, second] {
        let domain = name.to_lowercase();
        process_manager(w, &name, &domain, "");
    }
}

#[given(
    regex = r#"^a process manager "(\w+)" owning domain "([\w-]+)" that records (\w+) on (\w+)$"#
)]
fn records_on(w: &mut Coordinator, name: String, domain: String, event: String, input: String) {
    process_manager(w, &name, &domain, &input);
    w.business(&domain).every(|a| a.events = Some(vec![event]));
}

#[given(
    regex = r#"^"(\w+)" also takes an? (\w+) (command|fact), which (?:is not a starting input|its starting_types does not list)$"#
)]
fn also_takes(w: &mut Coordinator, name: String, input: String, kind: String) {
    let domain = w.domain_of(&name);
    if kind == "fact" {
        w.business(&domain).declare_fact(&input);
    }
}

#[given(regex = r#"^"(\w+)" has no instance (?:for correlation|at root) "([^"]+)"$"#)]
fn no_instance(w: &mut Coordinator, name: String, correlation: String) {
    let domain = w.domain_of(&name);
    let cover = w.cover(&domain, &correlation);
    assert!(w.pages(&cover).is_empty());
}

#[given(
    regex = r#"^"(\w+)" has an instance (?:for correlation|at root) "([^"]+)" with state "([^"]+)"$"#
)]
fn instance_with_state(w: &mut Coordinator, name: String, correlation: String, state: String) {
    let domain = w.domain_of(&name);
    let cover = w.cover(&domain, &correlation);
    w.write(&cover, &[&state_event(&state)]);
}

#[given(
    regex = r#"^"(\w+)" has state "([^"]+)" and "(\w+)" has state "([^"]+)" (?:for the order's correlation|at the order's root)$"#
)]
fn states_for_correlation(
    w: &mut Coordinator,
    first: String,
    first_state: String,
    second: String,
    second_state: String,
) {
    let correlation = "the-order-correlation".to_string();
    for (name, state) in [(first, first_state), (second, second_state)] {
        instance_with_state(w, name, correlation.clone(), state);
    }
    w.order_correlation = Some(correlation);
}

#[given(
    regex = r#"^a policy from "([\w-]+)" to "(\w+)"'s domain and a policy from "[\w-]+" to "(\w+)"'s domain, each turning (\w+) into a command$"#
)]
fn policy_to_each(w: &mut Coordinator, input: String, first: String, second: String, on: String) {
    for name in [first, second] {
        let domain = w.domain_of(&name);
        let script = w.policy(&format!("OrderTo{name}"), &input, &domain);
        locked(&script).reactions.push(Reaction {
            on: on.clone(),
            domain: domain.clone(),
            command: format!("Start{name}"),
            arm: crate::tests::policy::Arm::Command,
            root: Root::TriggerCorrelation,
        });
    }
}

#[given(
    regex = r#"^"(\w+)"(?: refuses (\w+)|'s (\w+) lists the refusal AlreadyCompleted, declared FAILED_PRECONDITION, and refuses with it) once completed$"#
)]
fn refuses_once_completed(w: &mut Coordinator, name: String, input: String, listed: String) {
    let input = input + &listed;
    let domain = w.domain_of(&name);
    w.business(&domain).rule(Rule {
        input,
        once: Some(format!("{name}Completed")),
        error: rejection(&crate::tests::world::package(&domain), "AlreadyCompleted"),
    });
}

#[given(regex = r#"^"(\w+)"'s (\w+) handler fails with INTERNAL$"#)]
fn handler_fails(w: &mut Coordinator, name: String, input: String) {
    let domain = w.domain_of(&name);
    let business = w.business(&domain);
    business.declare_fact(&input);
    business.rule(Rule {
        input,
        once: None,
        error: internal("TIMEOUT_HANDLER_FAILED"),
    });
}

#[given(
    regex = r#"^a (\w+) command with command_id "([^"]+)" has been applied by "(\w+)" (?:for correlation|at root) "([^"]+)"$"#
)]
async fn applied_by(
    w: &mut Coordinator,
    command: String,
    command_id: String,
    name: String,
    correlation: String,
) {
    let domain = w.domain_of(&name);
    let cover = w.cover(&domain, &correlation);
    let mut book = w.command(&cover, &command);
    if let Some(page) = book.page.as_mut() {
        page.command_id = Some(command_id.parse().expect("a UUID"));
    }
    w.handle(book).await;
    assert!(matches!(
        w.handled.last(),
        Some((_, Ok(Handled::Applied(_))))
    ));
}

#[given(
    regex = r#"^"(\w+)" has applied the policy's command for the (\w+) event at sequence 0 of order "([\w-]+)"$"#
)]
async fn applied_the_policys(w: &mut Coordinator, name: String, event: String, order: String) {
    let cover = w.cover("order", &order);
    assert!(w.pages(&cover).is_empty(), "the event at sequence 0");
    w.record(&cover, &event, Some("the-order-correlation"))
        .await;
    let domain = w.domain_of(&name);
    assert_eq!(w.business(&domain).delivered().len(), 1, "applied");
}

#[when(
    regex = r#"^(?:a client sends )?an? (\w+) (command|fact)(?: with (?:correlation|external_id) "([^"]+)")? (?:is sent )?to "([\w-]+)" root "([^"]+)"(?: with correlation_id "([^"]+)")?$"#
)]
async fn sent_to(
    w: &mut Coordinator,
    input: String,
    kind: String,
    with: String,
    domain: String,
    name: String,
    correlated: String,
) {
    w.begin();
    let cover = w.cover(&domain, &name);
    let mut book = match kind.as_str() {
        "fact" => {
            let external_id = if with.is_empty() {
                "external-1"
            } else {
                with.as_str()
            };
            w.fact(&cover, &input, external_id)
        }
        _ => w.command(&cover, &input),
    };
    let correlation = [correlated, with]
        .into_iter()
        .find(|c| !c.is_empty() && kind == "command");
    if let (Some(page), Some(correlation)) = (book.page.as_mut(), correlation) {
        page.correlation_id = Some(root(&correlation));
    }
    w.handle(book).await;
}

#[when("the same command is sent again")]
async fn sent_again(w: &mut Coordinator) {
    w.begin();
    let book = w.last_book();
    w.handle(book).await;
}

#[when(regex = r"^the policy is run again over that (\w+) event$")]
async fn run_again(w: &mut Coordinator, _event: String) {
    w.begin();
    let def = w.policies.last().expect("a policy");
    let (name, input) = (def.name.clone(), def.input.clone());
    for cover in w.store_covers(&input) {
        w.rewind(&name, &cover);
    }
    w.resume().await.expect("the policy runs again");
}

/// The deliveries to the component `name`.
fn delivered_to(w: &mut Coordinator, name: &str) -> Vec<HandleRequest> {
    let domain = w.domain_of(name);
    w.business(&domain).delivered()
}

#[then(regex = r#"^"(\w+)" is not called$"#)]
fn not_called(w: &mut Coordinator, name: String) {
    assert!(
        delivered_to(w, &name).is_empty(),
        "{:?}",
        delivered_to(w, &name)
    );
}

#[then(
    regex = r#"^"(\w+)"'s coordinator delivers (?:the|its) command to "(\w+)" with (?:its state "([^"]+)"|an empty state)$"#
)]
fn delivers_with_state(w: &mut Coordinator, coordinator: String, name: String, state: String) {
    assert_eq!(
        coordinator, name,
        "a process manager behind its own coordinator"
    );
    let delivered = delivered_to(w, &name).pop().expect("delivered");
    let events = delivered.history.unwrap_or_default();
    let held: Vec<String> = events
        .pages
        .iter()
        .filter_map(|p| angzarr_embedded::angzarr_router::page_event(p).map(bare))
        .collect();
    match state.is_empty() {
        true => assert!(held.is_empty() && events.snapshot.is_none(), "{held:?}"),
        false => assert!(held.contains(&state_event(&state)), "{held:?}"),
    }
}

#[then(regex = r#"^the events are appended to stream "([\w-]+)" root "([^"]+)"$"#)]
fn appended_to(w: &mut Coordinator, domain: String, name: String) {
    let cover = w.cover(&domain, &name);
    assert!(!w.persisted(&cover).is_empty());
}

#[then(
    regex = r"^the sender is answered (APPLIED|DUPLICATE)(?: with (no events|the stored events))?$"
)]
fn sender_answered(w: &mut Coordinator, outcome: String, events: String) {
    let (_, result) = w.handled.last().expect("an input was handled");
    let handled = result.as_ref().expect("answered OK");
    let books = handled.books();
    let pages: usize = books.iter().map(|b| b.pages.len()).sum();
    match outcome.as_str() {
        "APPLIED" => assert!(matches!(handled, Handled::Applied(_))),
        _ => assert!(matches!(handled, Handled::Duplicate(..))),
    }
    match events.as_str() {
        "no events" => assert_eq!(pages, 0),
        "the stored events" => assert!(pages > 0),
        _ => {}
    }
}

#[then("the client is answered FAILED_PRECONDITION")]
fn failed_precondition(w: &mut Coordinator) {
    assert_eq!(
        w.answer().status(),
        Some(GrpcCode::FailedPrecondition),
        "{:?}",
        w.answer()
    );
}

#[then("the process manager's events for it are appended to that instance's stream")]
fn appended_to_instance(w: &mut Coordinator) {
    let book = w.last_book();
    let cover = book.cover.expect("a stream");
    assert!(!w.persisted(&cover).is_empty());
}

#[then(
    regex = r"^each of those events records (?:the fact's command_id as its causation and the fact's origin|the fact cause: the fact's command_id and its origin)$"
)]
fn fact_causation(w: &mut Coordinator) {
    let book = w.last_book();
    let cover = book.cover.clone().expect("a stream");
    let delivered = w
        .business(&cover.domain)
        .delivered()
        .pop()
        .expect("delivered");
    let page = delivered.input.and_then(|b| b.page).expect("a page");
    let command_id = page.command_id.clone().expect("the fact's derived id");
    for event in w.persisted(&cover) {
        let meta = event.meta.expect("stamped");
        let (cause, origin) = cause_of(&meta);
        assert_eq!(cause, Some(command_id.clone()));
        assert_eq!(
            origin,
            origin_of(&page),
            "the fact cause, its origin copied"
        );
    }
}

#[then("each answer's events are appended to that process manager's own stream")]
fn own_streams(w: &mut Coordinator) {
    let correlation = w
        .order_correlation
        .clone()
        .expect("the order's correlation");
    for def in w
        .policies
        .iter()
        .map(|p| p.output.clone())
        .collect::<Vec<_>>()
    {
        let cover = w.cover(&def, &correlation);
        assert_eq!(w.persisted(&cover).len(), 1, "{def}");
    }
}

#[then("each answer's state is stored for that process manager alone")]
fn own_states(w: &mut Coordinator) {
    let correlation = w
        .order_correlation
        .clone()
        .expect("the order's correlation");
    for domain in w
        .policies
        .iter()
        .map(|p| p.output.clone())
        .collect::<Vec<_>>()
    {
        let cover = w.cover(&domain, &correlation);
        let snapshot = w.stored_snapshot(&cover).expect("its state stored");
        let returned = w.business(&domain).returned();
        assert_eq!(snapshot.state.as_ref(), returned.last(), "{domain}");
    }
}

#[then("the events record the command's derived command_id as their causation")]
fn derived_causation(w: &mut Coordinator) {
    let domain = w.policies.last().expect("a policy").output.clone();
    let delivered = w.business(&domain).delivered().pop().expect("delivered");
    let cover = delivered
        .input
        .as_ref()
        .and_then(|b| b.cover.clone())
        .expect("a stream");
    let command_id = delivered
        .input
        .and_then(|b| b.page)
        .and_then(|p| p.command_id)
        .expect("an id");
    for event in w.persisted(&cover) {
        let meta = event.meta.expect("stamped");
        assert_eq!(cause_of(&meta).0, Some(command_id.clone()));
    }
}

#[then("the policy's command carries the same command_id as before")]
fn same_command_id(w: &mut Coordinator) {
    // The re-run's command met its first delivery's events under its id:
    // the process manager's stream holds one answer's events, no more.
    let domain = w.policies.last().expect("a policy").output.clone();
    let covers = w.store_covers(&domain);
    assert_eq!(covers.len(), 1);
    assert_eq!(w.pages(&covers[0]).len(), 1);
}

#[then(regex = r#"^"(\w+)" sends no command$"#)]
fn sends_no_command(w: &mut Coordinator, name: String) {
    // A process manager is a domain: what it answers is its own events.
    assert!(
        !w.policies.iter().any(|p| p.name == name),
        "{name} is no policy"
    );
}

#[then(regex = r#"^"(\w+)" delivers (\w+) to "([\w-]+)" with correlation "([^"]+)"$"#)]
fn policy_delivers(
    w: &mut Coordinator,
    _policy: String,
    command: String,
    domain: String,
    correlated: String,
) {
    let delivered = w.business(&domain).delivered().pop().expect("delivered");
    let page = delivered.input.and_then(|b| b.page).expect("a page");
    let body = page.body.as_ref().and_then(|b| match b {
        command_page::Body::Command(c) => c.payload.as_ref(),
        command_page::Body::Fact(_) => None,
    });
    assert_eq!(body.map(bare), Some(command));
    assert_eq!(page.correlation_id, Some(root(&correlated)));
}

#[then("the fact is dead-lettered and an alert is raised")]
fn fact_dead_lettered(w: &mut Coordinator) {
    assert!(!w.log.letters().is_empty(), "dead-lettered");
    assert!(!w.alerts().is_empty(), "alerted");
}

#[then("the events record the command cause with the reaction's derived command_id")]
fn caused_by_derived_id(w: &mut Coordinator) {
    let def = w.policies.last().expect("a policy");
    let (policy, output) = (def.name.clone(), def.output.clone());
    let trigger = delivered_to_domain(w, &output)
        .last()
        .and_then(|d| d.causation.clone())
        .expect("the reaction's trigger");
    let derived = angzarr_embedded::ids::policy_command_id(&trigger, &policy, 0).ok();
    let covers = w.store_covers(&output);
    let pages: Vec<_> = covers.iter().flat_map(|c| w.persisted(c)).collect();
    assert!(!pages.is_empty());
    for page in pages {
        let meta = page.meta.expect("stamped");
        assert_eq!(cause_of(&meta).0, derived, "{meta:?}");
    }
}

/// Every delivery to `domain`'s business code.
fn delivered_to_domain(w: &mut Coordinator, domain: &str) -> Vec<HandleRequest> {
    w.business(domain).delivered()
}
