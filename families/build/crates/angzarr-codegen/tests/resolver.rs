//! The resolver's rules at the edges the codegen tier's scenarios do not
//! state: one function holds them for the lint, the descriptors and the
//! generators alike, and each rule refuses only what it names.

use angzarr_codegen::{CodegenError, MarkupError, Warning, descriptors, warnings};

// The tier's own builder of protos; the scenarios' naming helpers it holds
// are not used here.
#[path = "spec/protos.rs"]
#[allow(dead_code)]
mod protos;

use protos::{Component, Input, Protos, Status, compile};

const VERSION: &str = "1.0.0";

fn aggregate(domain: &str) -> Component {
    Component {
        kind: "AGGREGATE".to_string(),
        component_name: Some(format!("{domain}-state")),
        domain: Some(domain.to_string()),
        ..Default::default()
    }
}

fn policy(name: &str, output: &str) -> Component {
    Component {
        kind: "POLICY".to_string(),
        component_name: Some(name.to_string()),
        output_domain: Some(output.to_string()),
        ..Default::default()
    }
}

fn input(component: &str) -> Input {
    Input {
        component: component.to_string(),
        ..Default::default()
    }
}

/// The set of `protos`, compiled; the file names, `imports` after.
fn set(protos: &Protos) -> (Vec<u8>, Vec<String>) {
    let files = protos.files();
    let names = files.keys().cloned().collect();
    (compile(&files), names)
}

fn lint(protos: &Protos) -> Result<Vec<Warning>, CodegenError> {
    let (set, names) = set(protos);
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    warnings(&set, &names)
}

fn describe(protos: &Protos) -> Result<usize, CodegenError> {
    let (set, names) = set(protos);
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    descriptors(&set, &names, VERSION).map(|d| d.len())
}

/// An order domain: its aggregate folding `OrderPlaced` and `OrderShipped`.
fn orders() -> Protos {
    let mut p = Protos::default();
    p.message("order.OrderState").component = Some(aggregate("order"));
    for event in ["order.OrderPlaced", "order.OrderShipped"] {
        p.message(event)
            .events
            .push(("order.OrderState".to_string(), None));
    }
    p
}

#[test]
fn a_policy_naming_no_event_is_refused_by_the_descriptors_as_by_the_lint() {
    let mut p = orders();
    p.message("policies.Idle").component = Some(policy("idle", "order"));
    let refused = CodegenError::Markup(MarkupError::NoEvent {
        component: "policies.Idle".to_string(),
    });
    assert_eq!(describe(&p), Err(refused.clone()));
    assert_eq!(lint(&p), Err(refused));
}

#[test]
fn a_policy_naming_an_event_is_not_refused() {
    let mut p = orders();
    p.message("policies.Ship").component = Some(policy("ship", "order"));
    p.message("order.OrderPlaced")
        .events
        .push(("policies.Ship".to_string(), None));
    assert_eq!(describe(&p), Ok(2));
    assert_eq!(lint(&p).map(|w| w.len()), Ok(0));
}

#[test]
fn an_event_naming_a_policy_with_no_owner_and_no_domain_has_no_source() {
    let mut p = Protos::default();
    p.message("policies.Ship").component = Some(policy("ship", "order"));
    p.message("order.OrderPlaced")
        .events
        .push(("policies.Ship".to_string(), None));
    assert_eq!(
        describe(&p),
        Err(CodegenError::Markup(MarkupError::EventWithoutSource {
            component: "policies.Ship".to_string(),
            event: "order.OrderPlaced".to_string(),
        }))
    );
    // With the entry's domain, the event needs no owner in the set.
    let mut p = Protos::default();
    p.message("policies.Ship").component = Some(policy("ship", "order"));
    p.message("order.OrderPlaced")
        .events
        .push(("policies.Ship".to_string(), Some("order".to_string())));
    assert_eq!(describe(&p), Ok(1));
}

#[test]
fn an_aggregate_folding_its_event_twice_is_one_consumer() {
    let mut p = orders();
    p.message("order.OrderPlaced")
        .events
        .push(("order.OrderState".to_string(), None));
    assert_eq!(describe(&p), Ok(1));
}

#[test]
fn an_event_two_stateful_components_fold_names_the_first_two() {
    let mut p = orders();
    p.message("pm.PmState").component = Some(Component {
        kind: "PROCESS_MANAGER".to_string(),
        component_name: Some("pm".to_string()),
        domain: Some("pm".to_string()),
        ..Default::default()
    });
    p.message("order.OrderPlaced")
        .events
        .push(("pm.PmState".to_string(), None));
    assert_eq!(
        describe(&p),
        Err(CodegenError::Markup(MarkupError::EventFoldedTwice {
            event: "order.OrderPlaced".to_string(),
            first: "order.OrderState".to_string(),
            second: "pm.PmState".to_string(),
        }))
    );
}

#[test]
fn an_override_is_refused_on_the_folding_entry_too() {
    let mut p = Protos::default();
    p.message("order.OrderState").component = Some(aggregate("order"));
    p.message("order.OrderPlaced")
        .events
        .push(("order.OrderState".to_string(), Some("order".to_string())));
    assert_eq!(
        describe(&p),
        Err(CodegenError::Markup(MarkupError::OverrideOnOwned {
            event: "order.OrderPlaced".to_string(),
            component: "order.OrderState".to_string(),
            domain: "order".to_string(),
        }))
    );
}

#[test]
fn a_policy_of_two_events_of_one_domain_has_one_source() {
    let mut p = orders();
    p.message("policies.Ship").component = Some(policy("ship", "order"));
    for event in ["order.OrderPlaced", "order.OrderShipped"] {
        p.message(event)
            .events
            .push(("policies.Ship".to_string(), None));
    }
    assert_eq!(describe(&p), Ok(2));
}

#[test]
fn a_policy_of_three_domains_names_the_first_two() {
    let mut p = orders();
    for (domain, event) in [("pay", "pay.Paid"), ("stock", "stock.Held")] {
        p.message(&format!("{domain}.State")).component = Some(aggregate(domain));
        p.message(event)
            .events
            .push((format!("{domain}.State"), None));
    }
    p.message("policies.Ship").component = Some(policy("ship", "order"));
    for event in ["order.OrderPlaced", "pay.Paid", "stock.Held"] {
        p.message(event)
            .events
            .push(("policies.Ship".to_string(), None));
    }
    assert_eq!(
        describe(&p),
        Err(CodegenError::Markup(MarkupError::PolicySources {
            policy: "policies.Ship".to_string(),
            first: "order".to_string(),
            second: "pay".to_string(),
        }))
    );
}

/// A process manager listing `starting` among its inputs `commands` and
/// `facts`.
fn manager(starting: &[&str], commands: &[&str], facts: &[&str]) -> Protos {
    let mut p = Protos::default();
    p.message("pm.PmState").component = Some(Component {
        kind: "PROCESS_MANAGER".to_string(),
        component_name: Some("pm".to_string()),
        domain: Some("pm".to_string()),
        starting_types: starting.iter().map(ToString::to_string).collect(),
        ..Default::default()
    });
    for command in commands {
        p.message(command).input = Some(input("pm.PmState"));
    }
    for fact in facts {
        p.message(fact).input = Some(Input {
            fact: true,
            ..input("pm.PmState")
        });
    }
    p
}

#[test]
fn a_starting_type_may_be_a_command_or_a_fact_of_the_process_manager() {
    let p = manager(&["pm.Start", "pm.Lapsed"], &["pm.Start"], &["pm.Lapsed"]);
    assert_eq!(describe(&p), Ok(1));
}

#[test]
fn the_first_starting_type_that_is_no_input_is_refused() {
    let p = manager(&["pm.Start", "pm.Nothing", "pm.Other"], &["pm.Start"], &[]);
    assert_eq!(
        describe(&p),
        Err(CodegenError::Markup(MarkupError::StartingTypeNotInput {
            component: "pm.PmState".to_string(),
            starting_type: "pm.Nothing".to_string(),
        }))
    );
}

/// An order policy sending to the wallet domain, whose `Hold` command
/// refuses with `Insufficient`; the order aggregate takes `facts`.
fn translating(facts: &[&str], refuses: bool) -> Protos {
    let mut p = orders();
    p.message("wallet.WalletState").component = Some(aggregate("wallet"));
    p.message("wallet.Held")
        .events
        .push(("wallet.WalletState".to_string(), None));
    let mut hold = input("wallet.WalletState");
    if refuses {
        hold.refuses = vec!["wallet.Insufficient".to_string()];
        p.message("wallet.Insufficient");
    }
    p.message("wallet.Hold").input = Some(hold);
    for fact in facts {
        p.message(fact).input = Some(Input {
            fact: true,
            ..input("order.OrderState")
        });
    }
    p.message("policies.ToWallet").component = Some(policy("to-wallet", "wallet"));
    p.message("order.OrderPlaced")
        .events
        .push(("policies.ToWallet".to_string(), None));
    p
}

#[test]
fn a_source_with_a_fact_translates_a_refusal() {
    assert_eq!(describe(&translating(&["order.StockGone"], true)), Ok(3));
}

#[test]
fn a_target_command_that_cannot_refuse_needs_no_fact() {
    assert_eq!(describe(&translating(&[], false)), Ok(3));
}

#[test]
fn a_command_that_can_refuse_needs_the_source_to_take_a_fact() {
    assert_eq!(
        describe(&translating(&[], true)),
        Err(CodegenError::Markup(MarkupError::SourceTakesNoFact {
            policy: "policies.ToWallet".to_string(),
            domain: "order".to_string(),
        }))
    );
}

#[test]
fn a_target_fact_alone_is_no_refusal_to_translate() {
    // A fact of the target cannot refuse, so it asks nothing of the source.
    let mut p = translating(&[], false);
    p.message("wallet.Credited").input = Some(Input {
        fact: true,
        ..input("wallet.WalletState")
    });
    assert_eq!(describe(&p), Ok(3));
}

#[test]
fn a_policy_whose_target_is_outside_the_set_asks_nothing_of_its_source() {
    let mut p = orders();
    p.message("policies.Away").component = Some(policy("away", "elsewhere"));
    p.message("order.OrderPlaced")
        .events
        .push(("policies.Away".to_string(), None));
    assert_eq!(describe(&p), Ok(2));
}

/// A command refusing with `Closed`, its status as `status` says.
fn refusing(status: Status) -> Protos {
    let mut p = orders();
    let mut add = input("order.OrderState");
    add.refuses = vec!["order.Closed".to_string()];
    p.message("order.Add").input = Some(add);
    p.message("order.Closed").status = status;
    p
}

#[test]
fn a_refusal_declaring_a_status_is_accepted() {
    assert_eq!(
        describe(&refusing(Status::Marked(
            "REFUSAL_STATUS_NOT_FOUND".to_string()
        ))),
        Ok(1)
    );
}

#[test]
fn a_refusal_without_a_status_or_with_the_unspecified_one_is_refused() {
    for status in [
        Status::Absent,
        Status::Marked("REFUSAL_STATUS_UNSPECIFIED".to_string()),
    ] {
        assert_eq!(
            describe(&refusing(status)),
            Err(CodegenError::Markup(MarkupError::RefusalStatusMissing {
                refusal: "order.Closed".to_string(),
            }))
        );
    }
}

#[test]
fn a_status_on_a_message_no_command_refuses_with_is_refused() {
    let mut p = orders();
    p.message("order.Summary").status = Status::Marked("REFUSAL_STATUS_NOT_FOUND".to_string());
    assert_eq!(
        describe(&p),
        Err(CodegenError::Markup(MarkupError::RefusalStatusMisplaced {
            message: "order.Summary".to_string(),
        }))
    );
}

#[test]
fn a_message_with_no_status_that_no_command_refuses_with_is_ordinary() {
    let mut p = orders();
    p.message("order.Summary");
    assert_eq!(describe(&p), Ok(1));
}

#[test]
fn a_message_with_both_options_is_refused_whatever_else_is_wrong_with_it() {
    let mut p = orders();
    p.message("order.Both").input = Some(Input {
        both: true,
        fact: true,
        ..input("order.OrderState")
    });
    assert_eq!(
        describe(&p),
        Err(CodegenError::Markup(MarkupError::CommandAndFact {
            message: "order.Both".to_string(),
        }))
    );
}

#[test]
fn a_consumer_outside_the_set_is_warned_of_by_the_lint_and_skipped_by_the_descriptors() {
    let mut p = orders();
    p.message("order.OrderPlaced")
        .events
        .push(("shipping.Away".to_string(), None));
    assert_eq!(describe(&p), Ok(1));
    assert_eq!(
        lint(&p),
        Ok(vec![Warning::ConsumerSkipped {
            consumer: "shipping.Away".to_string(),
            event: "order.OrderPlaced".to_string(),
        }])
    );
}
