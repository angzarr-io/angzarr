//! What generating the blackjack table (the spec's table.proto and the
//! LedgerProjection beside it) needs, decision 14.50 and the owner's
//! generator rulings: table.proto generating alone, the projector of two
//! domains, typed refusals' args and Reason codes, the trigger root from the
//! policy context, the spec's policy names, and the generated register
//! composing with a hand-written component. The plan is
//! `rust-table-generation.plan.md` (session inept-sixth-stick). Every
//! fixture is the `angzarr-project` package's protos ([`spec`]) except
//! `refusals/v1/refusals.proto`, a declared rejection code's shape
//! (decision 14.50) in small.

mod common;

use angzarr_codegen::{Generated, adapters, markup_domain, register, scaffold};
use common::*;

use test_dir::TestDir;

const TABLE: &str = "io/angzarr/examples/v1/table.proto";
const LEDGER: &str = "io/angzarr/examples/v1/ledger.proto";
const WIRE: &[(&str, &str)] = &[("io.angzarr.examples.v1", "crate::wire")];

fn one(generated: Vec<Generated>) -> String {
    let [Generated { source, .. }] = generated.as_slice() else {
        panic!("one package expected, got {} packages", generated.len());
    };
    source.clone()
}

/// The source of the generated `impl <trait> for <ty>`, without spacing.
fn impl_text(source: &str, trait_: &str, ty: &str) -> String {
    let file = syn::parse_file(source).expect("generated source parses");
    file.items
        .iter()
        .find_map(|i| match i {
            syn::Item::Impl(imp)
                if text(&imp.self_ty) == ty
                    && imp
                        .trait_
                        .as_ref()
                        .is_some_and(|(path, _)| text(path) == trait_) =>
            {
                Some(text(imp))
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("no impl {trait_} for {ty}"))
}

// ------------------------------------------------- external components

/// table.proto alone (protoc with its own imports only) names three
/// consumers it does not declare (LedgerProjection, TableBuyInPolicy,
/// TableWalletPolicy): an `(event)` entry naming a component outside the
/// set is a reference to a consumer generated elsewhere, not a refusal
/// (decision 14.50). A `(command)` naming one
/// stays refused (`bad/v1/unknown_component.proto`): its owner is the
/// component generated here.
#[test]
fn test_markup_domain_of_table_proto_alone_takes_unknown_consumers_as_references() {
    let generated = markup_domain(&descriptors(&[TABLE]), &[TABLE], WIRE)
        .expect("an event's consumer outside the set is a reference");
    assert!(one(generated).contains("pub enum TableStateEvent"));
}

// ------------------------------------------------------------ projectors

/// The projector's domain layer: every event it projects (the table's and
/// the wallet's) as a domain type, and the context its handlers read.
#[test]
fn test_markup_domain_generates_the_ledger_projectors_events_and_context() {
    let source =
        one(markup_domain(&descriptors(BLACKJACK), &[LEDGER], WIRE)
            .expect("a projector is generated"));
    for event in [
        "RoundSettled",
        "PlayerSeated",
        "FundsDeposited",
        "FundsHeld",
    ] {
        assert!(
            source.contains(&format!("pub struct {event} ")),
            "{event} missing:\n{source}"
        );
    }
    assert!(
        source.contains("pub use ::angzarr_embedded::markup::ProjectionContext;"),
        "{source}"
    );
}

/// A projector's interface, as every other kind's, is the domain layer's:
/// one async method per projected event (decision 14.50's ruling: an async
/// user interface); its implementation, the user's, holds the projection.
/// The ledger is two projectors, each of one source domain (decision 14.96).
#[test]
fn test_scaffold_writes_the_ledger_projectors_interface_and_implementation() {
    let files = scaffold(&descriptors(BLACKJACK), &[LEDGER]).unwrap();
    let interface = one(markup_domain(&descriptors(BLACKJACK), &[LEDGER], WIRE).unwrap());
    for expected in [
        "pub trait TableLedgerProjectionInterface {",
        "    fn round_settled(\n        &mut self,\n        event: &RoundSettled,\n        ctx: &ProjectionContext,\n    ) -> impl std::future::Future<Output = Result<(), Bug>> + Send;",
        "pub trait WalletLedgerProjectionInterface {",
        "    fn funds_held(\n        &mut self,\n        event: &FundsHeld,\n        ctx: &ProjectionContext,\n    ) -> impl std::future::Future<Output = Result<(), Bug>> + Send;",
    ] {
        assert!(interface.contains(expected), "{expected}:\n{interface}");
    }
    for (path, state) in [
        (
            "table_ledger_projection_impl.rs",
            "pub state: crate::wire::TableLedgerProjection,",
        ),
        (
            "wallet_ledger_projection_impl.rs",
            "pub state: crate::wire::WalletLedgerProjection,",
        ),
    ] {
        let implementation = files
            .iter()
            .find(|f| f.path == path)
            .unwrap_or_else(|| panic!("{path}"));
        assert!(
            implementation.source.contains(state),
            "{}",
            implementation.source
        );
    }
}

/// The adapter marks each ledger projector up for angzarr-embedded: one
/// `#[projector]` each, over its one source domain, named by its
/// component_name, delegating to the user's implementation.
#[test]
fn test_adapters_mark_up_the_ledger_projectors_each_for_its_source_domain() {
    let source = adapters(&descriptors(BLACKJACK), &[LEDGER], &[]).unwrap();
    assert_eq!(source.matches("#[projector(").count(), 2, "{source}");
    for projector in [
        "#[projector(name = \"projector-wallet-ledger\", sources = [\"wallet\"])]",
        "#[projector(name = \"projector-table-ledger\", sources = [\"table\"])]",
    ] {
        assert!(source.contains(projector), "{projector}:\n{source}");
    }
    assert!(source.contains("#[projects(RoundSettled)]"), "{source}");
    assert!(source.contains("#[projects(FundsHeld)]"), "{source}");
}

/// `register` registers the projector, as every other kind.
#[test]
fn test_register_registers_the_ledger_projector() {
    let source = register(&descriptors(BLACKJACK), BLACKJACK, &[]).unwrap();
    assert!(source.contains("registry.projector("), "{source}");
}

// --------------------------------------------------------- refusal codes

/// A typed refusal's fields are its diagnostic args: SeatNotConfirmable's
/// `reason`, as the hand-written one in examples-rust has it.
#[test]
fn test_a_typed_refusals_fields_are_its_diagnostic_args() {
    let source = one(markup_domain(&descriptors(BLACKJACK), &[TABLE], WIRE).unwrap());
    let diagnostic = impl_text(
        &source,
        "::angzarr_embedded::coded_diagnostic::Diagnostic",
        "SeatNotConfirmable",
    );
    assert!(diagnostic.contains("(\"reason\","), "{diagnostic}");
}

/// A refusal declaring its codes as a nested `Reason` enum (decision
/// 14.50, table.proto's TableRefused): each value is mapped, and the
/// diagnostic names the reason by the value's name without its enum's
/// prefix, the ErrorInfo.reason on the wire.
#[test]
fn test_a_refusals_reason_enum_is_its_rejection_code() {
    const FILE: &str = "refusals/v1/refusals.proto";
    let source = one(markup_domain(
        &descriptors(&[FILE]),
        &[FILE],
        &[("refusals.v1", "crate::wire")],
    )
    .unwrap());
    assert!(source.contains("pub enum BetRefusedReason"), "{source}");
    let diagnostic = impl_text(
        &source,
        "::angzarr_embedded::coded_diagnostic::Diagnostic",
        "BetRefused",
    );
    for expected in [
        "(\"reason\",",
        "\"ALREADY_BET\"",
        "\"BET_OUT_OF_RANGE\"",
        "(\"max_bet\",",
    ] {
        assert!(diagnostic.contains(expected), "{expected}: {diagnostic}");
    }
}

// ------------------------------------------------------------- policies

/// The trigger's root is the context's (`PolicyContext::source_root`),
/// which a hand-written policy calls too, so nothing is generated for it.
#[test]
fn test_policy_adapters_read_the_trigger_root_from_the_context() {
    let files = &[
        TABLE,
        "io/angzarr/examples/v1/player.proto",
        "io/angzarr/examples/v1/policies.proto",
    ];
    let source = adapters(&descriptors(BLACKJACK), files, &["player"]).unwrap();
    assert!(!source.contains("fn source_root"), "{source}");
    assert!(source.contains("ctx.source_root()"), "{source}");
}

/// A policy is named for its spec name, policy-<source>-<target>,
/// with no hand edit (examples-rust's two HAND-EDITs went with the split
/// scaffold).
#[test]
fn test_policy_adapters_carry_the_spec_names() {
    let files = &[
        TABLE,
        "io/angzarr/examples/v1/player.proto",
        "io/angzarr/examples/v1/policies.proto",
    ];
    let source = adapters(&descriptors(BLACKJACK), files, &["player"]).unwrap();
    for name in ["\"policy-table-player\"", "\"policy-player-table\""] {
        assert!(source.contains(name), "{name}:\n{source}");
    }
}

// ----------------------------------------------------------- escape hatch

/// The escape hatch: the generated `register` is a plain
/// public function the application composes with components it writes by
/// hand, registered through angzarr-embedded's own public API
/// (`Registry::projector_handler`, which takes its source domains),
/// nothing of the generator's. Compiled with warnings denied against a stub
/// of that API; angzarr-build's kinds-app does the same on the real
/// angzarr-embedded.
#[test]
fn test_generated_register_composes_with_a_hand_written_component() {
    const SHOP: &str = "shop/v1/shop.proto";
    let generated = register(&descriptors(&[SHOP]), &[SHOP], &["carts"]).unwrap();
    let app = format!(
        "{generated}\n\
         pub struct HandWrittenLedger;\n\
         impl ::angzarr_embedded::router::ProjectorHandler for HandWrittenLedger {{}}\n\
         /// The application's registration: the generated, then its own.\n\
         pub fn register_all(\n\
             registry: &mut ::angzarr_embedded::Registry,\n\
         ) -> Result<(), ::angzarr_embedded::Misconfigured> {{\n\
             register(registry)?;\n\
             registry.projector_handler(&[\"carts\"], HandWrittenLedger)?;\n\
             Ok(())\n\
         }}\n"
    );
    let stub = "pub mod router { pub trait ProjectorHandler {} }\n\
                pub struct Misconfigured;\n\
                pub struct Registry; impl Registry {\n\
                    pub fn projector_handler(&mut self, _sources: &[&str], _p: impl crate::router::ProjectorHandler + 'static) -> Result<&mut Self, Misconfigured> { Ok(self) }\n\
                }";
    if let Err(diagnostics) = compile_with_stub(stub, &app) {
        panic!("{app}\n{diagnostics}");
    }
}

/// rustc with warnings denied on `source` as a library, `::angzarr_embedded`
/// the crate `stub`: the diagnostics, if rustc refused it.
fn compile_with_stub(stub: &str, source: &str) -> Result<(), String> {
    use std::process::Command;
    let dir = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-table-generation"));
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    let rustc = |args: &[&str]| {
        Command::new(&rustc)
            .current_dir(&dir)
            .args(["--edition=2021", "--crate-type=lib", "-Dwarnings"])
            .args(args)
            .output()
            .expect("rustc on PATH")
    };
    std::fs::write(dir.join("stub.rs"), stub).unwrap();
    let built = rustc(&["--crate-name=angzarr_embedded", "stub.rs"]);
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    std::fs::write(dir.join("app.rs"), source).unwrap();
    let out = rustc(&[
        "--crate-name=app",
        "--extern=angzarr_embedded=libangzarr_embedded.rlib",
        "app.rs",
    ]);
    if out.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).into_owned())
    }
}
