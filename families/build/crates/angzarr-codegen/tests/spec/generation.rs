//! Whens running the generator, and thens on what it made or why it
//! refused. "The protos are linted" is `angzarr lint app`'s reading of
//! them (`angzarr_codegen::warnings`): a declaration the contract forbids
//! refused, its exit status non-zero, and its warnings, which leave the
//! status 0; the tier's warning labels (`ANZ100`) are the crate's codes
//! ([`coded`]). Generating the component's descriptor is the `descriptors`
//! the build writes.

use angzarr_codegen::{CodegenError, MarkupError, Warning};
use angzarr_diagnostic::Diagnostic as _;
use angzarr_proto::v1::component_descriptor::Kind;
use cucumber::{then, when};

use crate::protos::full;
use crate::world::Generator;

/// Every arg of `error`, as text.
fn args(error: &CodegenError) -> Vec<String> {
    error
        .args()
        .iter()
        .map(|(_, arg)| format!("{arg:?}"))
        .collect()
}

/// Whether `arg` names `wanted`: each of its dot-separated parts, in
/// order.
fn names_in(arg: &str, wanted: &str) -> bool {
    let mut parts = arg.split(['.', '"', '(', ')', ' ']);
    wanted.split('.').all(|w| parts.any(|p| p == w))
}

/// Whether `error` names `name` (a full name, or its last part).
fn names(error: &CodegenError, name: &str) -> bool {
    let full = full(name);
    args(error)
        .iter()
        .any(|a| names_in(a, &full) || names_in(a, name))
}

#[when("the protos are linted")]
fn linted(w: &mut Generator) {
    w.lint_protos();
}

/// Generating reads the declarations as a lint does; the one channel of
/// warnings is the lint's.
#[when("the protos are generated")]
fn generated_protos(w: &mut Generator) {
    w.lint_protos();
}

#[when("the component's descriptor is generated")]
fn generate(w: &mut Generator) {
    w.generate();
}

#[then("lint exits with status 0")]
fn zero(w: &mut Generator) {
    let _ = w.generated();
}

/// Whether `warning` names `name` (a full name, or its last part).
fn warns_of(warning: &Warning, name: &str) -> bool {
    let full = full(name);
    warning
        .args()
        .iter()
        .map(|(_, arg)| format!("{arg:?}"))
        .any(|a| names_in(&a, &full) || names_in(&a, name))
}

/// The cli's warning for the condition the tier labels `label` (`ANZ100`),
/// by its variant.
fn coded(label: &str) -> fn(&Warning) -> bool {
    match label {
        "ANZ100" => |w| matches!(w, Warning::UnappliedEmit { .. }),
        "ANZ101" => |w| matches!(w, Warning::UnownedTarget { .. }),
        "ANZ102" => |w| matches!(w, Warning::UnproducedSource { .. }),
        "ANZ103" => |w| matches!(w, Warning::Idle { .. }),
        other => panic!("no cli code for the tier's {other}"),
    }
}

/// The warning `is` of `component`, naming each quoted name of `rest` too.
fn assert_warned(w: &Generator, is: fn(&Warning) -> bool, component: &str, rest: &str) {
    let named = w.named(component);
    let quoted: Vec<&str> = rest.split('"').skip(1).step_by(2).collect();
    let found = w.warnings.iter().any(|warning| {
        is(warning)
            && (warns_of(warning, &named) || warns_of(warning, component))
            && quoted.iter().all(|q| warns_of(warning, q))
    });
    assert!(found, "no warning for {component}{rest}: {:?}", w.warnings);
}

#[then(regex = r#"^lint reports warning (ANZ\d+) naming (?:command )?"([^"]+)"(.*)$"#)]
fn warning_naming(w: &mut Generator, label: String, name: String, rest: String) {
    assert_warned(w, coded(&label), &name, &rest);
}

#[then(regex = r#"^lint reports a warning naming "([^"]+)" and its empty starting_types$"#)]
fn warning_starting_types(w: &mut Generator, name: String) {
    assert_warned(
        w,
        |w| matches!(w, Warning::NoStartingInput { .. }),
        &name,
        "",
    );
}

#[then("lint exits with a non-zero status")]
fn non_zero(w: &mut Generator) {
    let _ = w.refused();
}

#[then(regex = r#"^lint reports an error naming "([^"]+)"(.*)$"#)]
fn error_naming(w: &mut Generator, name: String, rest: String) {
    let named = w.named(&name);
    let error = w.refused();
    assert!(
        names(error, &named) || names(error, &name),
        "{error:?} names no {name}"
    );
    for quoted in rest.split('"').skip(1).step_by(2) {
        assert!(names(error, quoted), "{error:?} names no {quoted}");
    }
    // The error is the one the scenario describes, not another that names
    // the same declaration.
    let why = |is: fn(&MarkupError) -> bool| matches!(error, CodegenError::Markup(m) if is(m));
    let expected = if rest.contains("its component_name") {
        Some(why(|m| matches!(m, MarkupError::ComponentName { .. })))
    } else if rest.contains("its missing output_domain") {
        Some(why(|m| matches!(m, MarkupError::NoOutputDomain { .. })))
    } else if rest.contains("unresolved refuses type") {
        Some(why(|m| matches!(m, MarkupError::UnknownRefusal { .. })))
    } else if rest.contains("its refuses type") {
        Some(why(|m| matches!(m, MarkupError::ForeignRefusal { .. })))
    } else if rest.contains(": it names no event") {
        Some(why(|m| matches!(m, MarkupError::NoEvent { .. })))
    } else if rest.contains(": it has no owner") {
        Some(why(|m| matches!(m, MarkupError::EventWithoutSource { .. })))
    } else if rest.contains("its source domains") {
        Some(why(|m| matches!(m, MarkupError::PolicySources { .. })))
    } else if rest.contains("the override on its entry") {
        Some(why(|m| matches!(m, MarkupError::OverrideOnOwned { .. })))
    } else if rest.contains("its two stateful consumers") {
        Some(why(|m| matches!(m, MarkupError::EventFoldedTwice { .. })))
    } else if rest.contains("its starting type") {
        Some(why(|m| {
            matches!(m, MarkupError::StartingTypeNotInput { .. })
        }))
    } else if rest.contains("its (refusal_status)") {
        // C-0862 (none declared) and C-0746 (declared where none is
        // refused) word the same.
        Some(why(|m| {
            matches!(
                m,
                MarkupError::RefusalStatusMissing { .. }
                    | MarkupError::RefusalStatusMisplaced { .. }
            )
        }))
    } else if rest.contains("both its options") {
        Some(why(|m| matches!(m, MarkupError::CommandAndFact { .. })))
    } else if rest.contains("no fact to translate a refusal into") {
        Some(why(|m| matches!(m, MarkupError::SourceTakesNoFact { .. })))
    } else {
        None
    };
    assert_ne!(expected, Some(false), "{error:?} is not the error of{rest}");
}

#[then(regex = r#"^lint reports no error naming "([^"]+)"$"#)]
fn no_error_naming(w: &mut Generator, name: String) {
    if let Some(Err(error)) = &w.outcome {
        assert!(!names(error, &name), "{error:?} names {name}");
    }
}

#[then(
    regex = r#"^generation warns that consumer "([^"]+)" of "([^"]+)" is outside this generation and was skipped$"#
)]
fn warns_skipped(w: &mut Generator, consumer: String, event: String) {
    let _ = w.generated();
    let found = w.warnings.iter().any(|warning| {
        matches!(warning, Warning::ConsumerSkipped { .. })
            && warns_of(warning, &consumer)
            && warns_of(warning, &event)
    });
    assert!(found, "no warning of {consumer}: {:?}", w.warnings);
}

#[then("no handler is generated for that entry")]
fn no_handler(w: &mut Generator) {
    // The consumer is no component of the generation: nothing is made for
    // it (the Rust handlers themselves are the output generators').
    let made = &w.generated().descriptors;
    assert!(made.iter().all(|d| !d.component.contains("ShippingPolicy")));
}

#[then("generation exits with status 0")]
fn generation_zero(w: &mut Generator) {
    let _ = w.generated();
}

#[then(regex = r#"^lint reports no error for "([^"]+)"$"#)]
fn no_error(w: &mut Generator, _name: String) {
    let _ = w.generated();
}

#[then(regex = r#"^the descriptor carries rules version "([^"]+)"$"#)]
fn rules_version(w: &mut Generator, version: String) {
    assert_eq!(w.descriptor().rules_version, version);
}

#[then(regex = r#"^its aggregate arm carries domain "([\w-]+)" and state type_name "([^"]+)"$"#)]
fn aggregate_arm(w: &mut Generator, domain: String, state: String) {
    let Some(Kind::Aggregate(aggregate)) = w.descriptor().kind else {
        panic!("no aggregate arm");
    };
    assert_eq!(aggregate.domain, domain);
    assert_eq!(aggregate.state.map(|s| s.type_name), Some(state));
}

#[then(
    regex = r#"^the descriptor's policy arm carries input_domain "([\w-]+)" and output_domain "([\w-]+)"$"#
)]
fn policy_arm(w: &mut Generator, input: String, output: String) {
    let Some(Kind::Policy(policy)) = w.descriptor().kind else {
        panic!("no policy arm");
    };
    assert_eq!((policy.input_domain, policy.output_domain), (input, output));
}

/// The name of the arm `kind` is.
fn arm(kind: &Option<Kind>) -> &'static str {
    match kind {
        Some(Kind::Aggregate(_)) => "aggregate",
        Some(Kind::ProcessManager(_)) => "process_manager",
        Some(Kind::Policy(_)) => "policy",
        Some(Kind::Projector(_)) => "projector",
        None => "none",
    }
}

#[then("the descriptor has no aggregate, process_manager or projector arm")]
fn only_policy(w: &mut Generator) {
    assert_eq!(arm(&w.descriptor().kind), "policy");
}

#[then(regex = r#"^the descriptor's (\w+) arm is set, carrying (.+)$"#)]
fn arm_is_set(w: &mut Generator, wanted: String, carrying: String) {
    let kind = w.descriptor().kind;
    assert_eq!(arm(&kind), wanted);
    let carried = match kind {
        Some(Kind::Aggregate(a)) => !a.domain.is_empty() && a.state.is_some(),
        Some(Kind::ProcessManager(p)) => !p.domain.is_empty() && p.state.is_some(),
        Some(Kind::Projector(_)) => carrying == "input_domains only",
        other => panic!("{other:?}"),
    };
    assert!(carried, "{wanted} does not carry {carrying}");
}

#[then("no other kind arm is set")]
fn no_other_arm(_w: &mut Generator) {
    // The kind is a oneof: the arm asserted is the only one.
}

#[then(
    regex = r#"^(?:its|the descriptor's) process_manager arm (?:carries|names) domain "([\w-]+)"$"#
)]
fn pm_domain(w: &mut Generator, domain: String) {
    assert_eq!(pm(w).domain, domain);
}

#[then(regex = r#"^it names domain "([\w-]+)"$"#)]
fn it_names_domain(w: &mut Generator, domain: String) {
    assert_eq!(pm(w).domain, domain);
}

fn pm(w: &Generator) -> angzarr_proto::v1::ProcessManagerDescriptor {
    let Some(Kind::ProcessManager(pm)) = w.descriptor().kind else {
        panic!("no process_manager arm");
    };
    pm
}

#[then(regex = r#"^its state type_name is "([^"]+)"$"#)]
fn pm_state(w: &mut Generator, state: String) {
    assert_eq!(pm(w).state.map(|s| s.type_name), Some(full(&state)));
}

#[then(regex = r#"^its starting_types are "([^"]+)", a name with no type URL prefix$"#)]
fn pm_starting_types(w: &mut Generator, wanted: String) {
    let starting = pm(w).starting_types;
    assert_eq!(starting, [full(&wanted)]);
    assert!(starting.iter().all(|t| !t.starts_with('/')));
}

#[then(regex = r#"^the descriptor's process_manager arm lists starting type "([^"]+)" only$"#)]
fn pm_lists_only(w: &mut Generator, wanted: String) {
    assert_eq!(pm(w).starting_types, [full(&wanted)]);
}

#[then(regex = r#"^the PM's descriptor lists "([^"]+)" among its starting types$"#)]
fn pm_lists(w: &mut Generator, wanted: String) {
    let starting = pm(w).starting_types;
    assert!(starting.contains(&full(&wanted)), "{starting:?}");
}

#[then(regex = r#"^its projector arm lists input_domains "([\w-]+)" and "([\w-]+)"$"#)]
fn projector_domains(w: &mut Generator, first: String, second: String) {
    let Some(Kind::Projector(projector)) = w.descriptor().kind else {
        panic!("no projector arm");
    };
    assert_eq!(projector.input_domains, [first, second]);
}
