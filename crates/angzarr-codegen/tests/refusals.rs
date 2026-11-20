//! Typed refusals (decision 14.50): a refusal's diagnostic key from its
//! full name and its `Reason`, its args the fields that are set, and a
//! command refusing several types. The fixture is
//! `kinds/v1/kinds.proto`, one refusal per field kind; the generated layer
//! is pinned by golden files, which `angzarr-build`'s `tests/local`
//! kinds-app type-checks against angzarr-embedded.

mod common;

use angzarr_codegen::{
    CodegenError, MarkupError, ProtoError, adapters, markup_domain, refusal_key, scaffold,
};
use common::*;

const KINDS: &str = "kinds/v1/kinds.proto";
const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden");

/// `actual` is the golden file at `path` (rewritten under `ANGZARR_BLESS`).
fn golden(path: &str, actual: &str) {
    let path = format!("{GOLDEN}/{path}");
    if std::env::var_os("ANGZARR_BLESS").is_some() {
        std::fs::write(&path, actual).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|_| panic!("no {path}"));
    assert_eq!(actual, expected, "{path}");
}

fn kinds_domain() -> String {
    let generated = markup_domain(
        &descriptors(&[KINDS]),
        &[KINDS],
        &[("kinds.v1", "crate::wire")],
    )
    .unwrap();
    let [one] = generated.as_slice() else {
        panic!("one package expected");
    };
    one.source.clone()
}

#[test]
fn test_the_kinds_domain_layer_matches_the_golden_file() {
    golden("kinds.domain.rs", &kinds_domain());
}

#[test]
fn test_the_kinds_adapters_match_the_golden_file() {
    let source = adapters(&descriptors(&[KINDS]), &[KINDS], &[]).unwrap();
    golden("kinds.adapters.rs", &source);
}

#[test]
fn test_the_kinds_scaffold_matches_the_golden_files() {
    for file in scaffold(&descriptors(&[KINDS]), &[KINDS])
        .unwrap()
        .iter()
        .filter(|f| f.owner == angzarr_codegen::Owner::User)
    {
        golden(&format!("rust/kinds/{}", file.path), &file.source);
    }
}

#[test]
fn test_a_refusal_key_is_its_full_name_then_its_reason_in_kebab_case() {
    assert_eq!(
        refusal_key("refusals.v1.BetRefused", Some("ALREADY_BET")),
        "refusals-v1-betrefused-already-bet"
    );
    assert_eq!(
        refusal_key("io.angzarr.examples.v1.SeatNotConfirmable", None),
        "io-angzarr-examples-v1-seatnotconfirmable"
    );
}

#[test]
fn test_a_command_refusing_several_types_refuses_with_their_choice() {
    let interface = kinds_domain();
    assert!(
        interface.contains(
            "fn post(&self, cmd: &Post, ctx: &Context) -> Result<Vec<LedgerStateEvent>, Failed<PostRefuses>>;"
        ),
        "{interface}"
    );
    let adapters = adapters(&descriptors(&[KINDS]), &[KINDS], &[]).unwrap();
    assert!(
        adapters.contains(".map_err(PostRefuses::widen)"),
        "{adapters}"
    );
}

#[test]
fn test_markup_domain_refuses_a_type_named_as_a_refusal_choice() {
    const FILE: &str = "bad/v1/refusal_choice.proto";
    assert_eq!(
        markup_domain(&descriptors(&[FILE]), &[FILE], &[("bad.v1", "crate::wire")])
            .expect_err("refused"),
        CodegenError::Proto(ProtoError::RefusalChoiceName {
            message: "bad.v1.BookRefuses".to_string(),
            name: "BookRefuses".to_string(),
            command: "bad.v1.Book".to_string(),
        })
    );
}

#[test]
fn test_a_command_refusing_one_type_has_no_choice_to_collide_with() {
    const FILE: &str = "lone/v1/lone.proto";
    let generated = markup_domain(
        &descriptors(&[FILE]),
        &[FILE],
        &[("lone.v1", "crate::wire")],
    )
    .unwrap();
    assert!(generated[0].source.contains("pub struct BookRefuses"));
}

/// A refusal is refused under the status its message declares
/// (`(refusal_status)`, decision 14.95: no default): every refusal message
/// of the domain layer implements `Refusal`, whose `REFUSAL_STATUS` says
/// which, for a hand-written handler too, and the adapter refuses under
/// it, for one refusal type and for a choice of several, whether the
/// message has a `Reason` or none.
#[test]
fn test_a_refusals_status_is_its_messages_refusal_status() {
    let domain = kinds_domain();
    let file = syn::parse_file(&domain).unwrap();
    let statuses: Vec<(String, String)> = file
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Impl(imp)
                if imp
                    .trait_
                    .as_ref()
                    .is_some_and(|(path, _)| text(path) == "Refusal") =>
            {
                Some((text(&imp.self_ty), text(imp.items.first()?)))
            }
            _ => None,
        })
        .collect();
    let status = |refusal: &str| -> &str {
        &statuses
            .iter()
            .find(|(ty, _)| ty == refusal)
            .unwrap_or_else(|| panic!("no Refusal for {refusal}:\n{domain}"))
            .1
    };
    // PostRefused declares OUT_OF_RANGE as its message's option, whatever
    // its reasons; the others declare FAILED_PRECONDITION.
    assert_eq!(
        status("PostRefused"),
        "constREFUSAL_STATUS:BusinessStatus=BusinessStatus::OutOfRange;"
    );
    for other in ["Unreasoned", "Listed", "Elsewhere", "Mislabeled"] {
        assert_eq!(
            status(other),
            "constREFUSAL_STATUS:BusinessStatus=BusinessStatus::FailedPrecondition;",
            "{other}"
        );
    }
    // No status is read from a reason: the old per-reason `status`.
    assert!(!domain.contains("pub fn status(&self)"), "{domain}");
    assert!(!domain.contains("with_code_status"), "{domain}");
    // A choice refuses under the status of the type it holds.
    let flat = domain.split_whitespace().collect::<String>();
    assert!(
        flat.contains("PostRefuses::Unreasoned(_)=><UnreasonedasRefusal>::REFUSAL_STATUS"),
        "{domain}"
    );
    let adapters = adapters(&descriptors(&[KINDS]), &[KINDS], &[]).unwrap();
    for refused in [
        ".map_err(PostRefuses::with_declared_status).map_err(PostRefuses::widen)",
        ".map_err(PostRefused::with_declared_status).map_err(Failed::widen)",
    ] {
        assert!(
            adapters
                .split_whitespace()
                .collect::<String>()
                .contains(refused),
            "{refused}:\n{adapters}"
        );
    }
}

/// Each status a refusal may declare maps to the business status of the
/// same name, and each refusal type and the choice read them alike.
#[test]
fn test_each_declared_status_is_the_business_status_of_its_name() {
    const FILE: &str = "statuses/v1/statuses.proto";
    let generated = markup_domain(
        &descriptors(&[FILE]),
        &[FILE],
        &[("statuses.v1", "crate::wire")],
    )
    .unwrap();
    let flat = generated[0].source.split_whitespace().collect::<String>();
    for (refusal, status) in [
        ("NotInRange", "OutOfRange"),
        ("BadInput", "InvalidArgument"),
        ("Missing", "NotFound"),
        ("Taken", "AlreadyExists"),
        ("Forbidden", "PermissionDenied"),
        ("NotReady", "FailedPrecondition"),
        ("Stale", "Aborted"),
    ] {
        let expected = format!(
            "implRefusalfor{refusal}{{constREFUSAL_STATUS:BusinessStatus=BusinessStatus::{status};}}"
        );
        assert!(
            flat.contains(&expected),
            "{refusal}: {}",
            generated[0].source
        );
    }
}

/// Every refusal declares its status, and the option marks a refusal
/// message alone (C-0862, C-0746): a message some command refuses with
/// that declares none, or `REFUSAL_STATUS_UNSPECIFIED`, and a status on a
/// message no command refuses with, generation fails naming the message.
#[test]
fn test_a_refusal_status_anywhere_but_a_refusals_message_is_refused() {
    let missing = |refusal: &str| MarkupError::RefusalStatusMissing {
        refusal: refusal.to_string(),
    };
    for (file, error) in [
        (
            "bad/v1/refusal_status_missing.proto",
            missing("bad.v1.PokeRefused"),
        ),
        (
            "bad/v1/refusal_status_unspecified.proto",
            missing("bad.v1.PokeRefused"),
        ),
        (
            "bad/v1/refusal_status_unrefused.proto",
            MarkupError::RefusalStatusMisplaced {
                message: "bad.v1.Poked".to_string(),
            },
        ),
    ] {
        assert_eq!(
            markup_domain(&descriptors(&[file]), &[file], &[("bad.v1", "crate::wire")])
                .expect_err(file),
            CodegenError::Markup(error),
            "{file}"
        );
    }
}

/// A domain type taking the name the domain layer gives a component's
/// interface (its trait), or its facts' choice, is refused, naming it; a
/// domain taking no fact has no choice, and leaves the name free (item S).
#[test]
fn test_markup_domain_refuses_a_type_named_as_an_interface_or_a_fact_choice() {
    for (file, message, name, component) in [
        (
            "bad/v1/interface_name.proto",
            "bad.v1.ShelfStateInterface",
            "ShelfStateInterface",
            "bad.v1.ShelfState",
        ),
        (
            "bad/v1/fact_choice_name.proto",
            "bad.v1.BinStateFact",
            "BinStateFact",
            "bad.v1.BinState",
        ),
    ] {
        assert_eq!(
            markup_domain(&descriptors(&[file]), &[file], &[("bad.v1", "crate::wire")])
                .expect_err("refused"),
            CodegenError::Proto(ProtoError::InterfaceName {
                message: message.to_string(),
                name: name.to_string(),
                component: component.to_string(),
            }),
            "{file}"
        );
    }
    const FREE: &str = "bad/v1/no_fact_choice.proto";
    let layer = markup_domain(&descriptors(&[FREE]), &[FREE], &[("bad.v1", "crate::wire")])
        .expect("no choice of facts, no clash");
    assert!(
        !layer[0].source.contains("enum CrateFact"),
        "{}",
        layer[0].source
    );
}
