//! A component's name in an application (decisions 14.50 and 14.87): its
//! one required `component_name`, no default, through `component_names`
//! (what the generator, the app config and hand-written code share).

mod common;

use angzarr_codegen::{
    CodegenError, ComponentName, MarkupError, NameKind, component_names, markup_domain,
};
use common::*;

#[test]
fn test_component_names_are_the_blackjack_spec_names() {
    let names: Vec<(String, NameKind, String, String)> =
        component_names(&descriptors(BLACKJACK), BLACKJACK)
            .unwrap()
            .into_iter()
            .map(
                |ComponentName {
                     component,
                     kind,
                     name,
                     domain,
                 }| (component, kind, name, domain),
            )
            .collect();
    let at = |component: &str| {
        names
            .iter()
            .find(|n| n.0 == format!("io.angzarr.examples.v1.{component}"))
            .unwrap_or_else(|| panic!("{component}: {names:?}"))
    };
    let s = |v: &str| v.to_string();
    assert_eq!(
        at("TableState"),
        &(
            s("io.angzarr.examples.v1.TableState"),
            NameKind::Aggregate,
            s("table"),
            s("table")
        )
    );
    // The name is the declared one, not a formula of the kind and domains.
    assert_eq!(at("BuyInState").2, "pmg-buy-in");
    assert_eq!(at("BuyInState").1, NameKind::ProcessManager);
    assert_eq!(at("TablePlayerPolicy").2, "policy-table-player");
    assert_eq!(at("TablePlayerPolicy").3, "");
    assert_eq!(at("WalletLedgerProjection").2, "projector-wallet-ledger");
    assert_eq!(at("TableLedgerProjection").2, "projector-table-ledger");
    assert_eq!(at("TableLedgerProjection").1, NameKind::Projector);
}

#[test]
fn test_generation_refuses_a_malformed_component_name() {
    const FILE: &str = "bad/v1/component_name.proto";
    assert_eq!(
        markup_domain(&descriptors(&[FILE]), &[FILE], &[("bad.v1", "crate::wire")])
            .expect_err("refused"),
        CodegenError::Markup(MarkupError::ComponentName {
            component: "bad.v1.ThingState".to_string(),
            name: "Things_1".to_string(),
        })
    );
}

/// Generation refuses `file`'s declaration of `component` for its name.
fn refused_name(file: &str, component: &str, name: &str) {
    assert_eq!(
        markup_domain(&descriptors(&[file]), &[file], &[("bad.v1", "crate::wire")])
            .expect_err("refused"),
        CodegenError::Markup(MarkupError::ComponentName {
            component: component.to_string(),
            name: name.to_string(),
        })
    );
}

#[test]
fn test_a_component_declaring_no_name_is_refused_with_no_default() {
    refused_name("bad/v1/component_name_empty.proto", "bad.v1.ThingState", "");
}

#[test]
fn test_a_name_is_lower_case_letters_digits_and_dashes_from_a_letter() {
    refused_name(
        "bad/v1/component_name_digit.proto",
        "bad.v1.ThingState",
        "1-thing",
    );
    refused_name(
        "bad/v1/component_name_case.proto",
        "bad.v1.ThingState",
        "Thing",
    );
    refused_name(
        "bad/v1/component_name.proto",
        "bad.v1.ThingState",
        "Things_1",
    );
}

#[test]
fn test_a_name_of_letters_digits_and_dashes_is_the_name() {
    const FILE: &str = "bad/v1/component_name_ok.proto";
    let names = component_names(&descriptors(&[FILE]), &[FILE]).unwrap();
    assert_eq!(names.len(), 1);
    assert_eq!(names[0].name, "a1-b");
    assert_eq!(names[0].kind, NameKind::Aggregate);
}
