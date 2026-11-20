//! The domain layer for marked-up components: every domain type the
//! components reach, with its wire conversions and its angzarr-embedded-macros
//! `DomainMessage` impl, and nothing the markup generates itself; and what
//! the generator refuses. Whether the generated code compiles and converts
//! correctly is held by the crates that build on it (angzarr-build's local
//! app); these tests hold what is generated, read back as Rust items.

mod common;

use angzarr_codegen::{CodegenError, Generated, MarkupError, ProtoError, UseError, markup_domain};
use common::*;

const SHOP: &str = "shop/v1/shop.proto";
const SHOP_WIRE: &[(&str, &str)] = &[("shop.v1", "::shop_proto::v1")];
const BAD_WIRE: &[(&str, &str)] = &[("bad.v1", "::bad_proto::v1")];

fn buyin() -> syn::File {
    let generated = markup_domain(&descriptors(BUYIN), BUYIN, BUYIN_WIRE).unwrap();
    let [Generated { package, source }] = generated.as_slice() else {
        panic!("one package expected, got {generated:#?}");
    };
    assert_eq!(package, "buyin.v1");
    syn::parse_file(source).expect("generated source parses")
}

fn shop() -> syn::File {
    let generated = markup_domain(&descriptors(&[SHOP]), &[SHOP], SHOP_WIRE).unwrap();
    let [Generated { package, source }] = generated.as_slice() else {
        panic!("one package expected, got {generated:#?}");
    };
    assert_eq!(package, "shop.v1");
    syn::parse_file(source).expect("generated source parses")
}

fn refusal(file: &str) -> CodegenError {
    markup_domain(&descriptors(&[file]), &[file], BAD_WIRE).expect_err("refused")
}

fn item_struct<'a>(file: &'a syn::File, name: &str) -> &'a syn::ItemStruct {
    file.items
        .iter()
        .find_map(|i| match i {
            syn::Item::Struct(s) if s.ident == name => Some(s),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no struct {name}"))
}

fn fields(file: &syn::File, name: &str) -> Vec<(String, String)> {
    item_struct(file, name)
        .fields
        .iter()
        .map(|f| (f.ident.as_ref().unwrap().to_string(), text(&f.ty)))
        .collect()
}

fn variants(file: &syn::File, name: &str) -> Vec<(String, String)> {
    let e = file
        .items
        .iter()
        .find_map(|i| match i {
            syn::Item::Enum(e) if e.ident == name => Some(e),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no enum {name}"));
    e.variants
        .iter()
        .map(|v| (v.ident.to_string(), text(&v.fields)))
        .collect()
}

/// `(trait, type)` of every impl in the file.
fn impls(file: &syn::File) -> Vec<(String, String)> {
    file.items
        .iter()
        .filter_map(|i| match i {
            syn::Item::Impl(imp) => Some((
                imp.trait_
                    .as_ref()
                    .map(|(path, _)| text(path))
                    .unwrap_or_default(),
                text(&imp.self_ty),
            )),
            _ => None,
        })
        .collect()
}

#[test]
fn test_markup_domain_implements_domain_message_for_every_anchor_input_event_and_refusal() {
    let impls = impls(&buyin());
    let mut messages: Vec<&str> = impls
        .iter()
        .filter(|(t, _)| t == "::angzarr_embedded::markup::DomainMessage")
        .map(|(_, ty)| ty.as_str())
        .collect();
    messages.sort();
    assert_eq!(
        messages,
        [
            "BuyInFailed",
            "BuyInHeld",
            "BuyInRefusalRecorded",
            "BuyInRefused",
            "BuyInStarted",
            "BuyInState",
            "Deposit",
            "DepositRefused",
            "Deposited",
            "Held",
            "Hold",
            "HoldLost",
            "InsufficientFunds",
            "RecordBuyInRefused",
            "RecordHeld",
            "StartBuyIn",
            "WalletState",
        ]
    );
}

#[test]
fn test_markup_domain_leaves_the_state_to_the_markup() {
    let impls = impls(&buyin());
    let left: Vec<&(String, String)> = impls
        .iter()
        .filter(|(t, _)| t == "::angzarr_embedded::State")
        .collect();
    assert_eq!(left, Vec::<&(String, String)>::new());
}

/// Every command and event enum the layer names carries its `Payload`:
/// the one nameable payload of its domain, which the markup takes
/// (`command = ..`, `event = ..`) instead of writing its own.
#[test]
fn test_markup_domain_implements_payload_for_every_command_and_event_enum() {
    let file = buyin();
    let enums: Vec<String> = file
        .items
        .iter()
        .filter_map(|i| match i {
            syn::Item::Enum(e) => Some(e.ident.to_string()),
            _ => None,
        })
        .filter(|name| {
            name.ends_with("Command") || name.ends_with("Event") || name.ends_with("Refusal")
        })
        .collect();
    let mut payloads: Vec<String> = impls(&file)
        .into_iter()
        .filter(|(t, _)| t == "::angzarr_embedded::Payload")
        .map(|(_, ty)| ty)
        .collect();
    payloads.sort();
    let mut expected = enums.clone();
    expected.sort();
    assert!(!expected.is_empty(), "the buy-in names its enums");
    assert_eq!(payloads, expected);
}

/// An enum's payload packs each variant as its message, reads each of its
/// types back by type URL, refuses any other, and lists its types: the
/// markup's own functions, so the hand-written path is the same code.
#[test]
fn test_markup_domain_payload_is_written_on_markups_functions() {
    let generated = markup_domain(&descriptors(BUYIN), BUYIN, BUYIN_WIRE).unwrap();
    let source = &generated[0].source;
    for call in [
        "::angzarr_embedded::markup::pack_message(value)",
        "::angzarr_embedded::markup::decode_message::<Hold>(any)?",
        "markup::type_url_of::<Hold>()",
        "::angzarr_embedded::PayloadError::UnknownType",
    ] {
        assert!(source.contains(call), "no `{call}` in:\n{source}");
    }
}

#[test]
fn test_markup_domain_accepts_policy_components() {
    // COMPONENT_KIND_POLICY marker messages are components, not refused as
    // a kind the generator does not emit, and carry no domain type.
    let generated = markup_domain(&descriptors(BUYIN), BUYIN, BUYIN_WIRE);
    let generated = generated.expect("policies are accepted");
    assert!(
        generated
            .iter()
            .all(|g| !g.source.contains("struct BuyInWalletPolicy")),
        "a policy's marker message is no domain type"
    );
    assert!(!generated.is_empty());
}

/// A message named as one of the names the domain layer gives the
/// runtime's types (`Context`, `PolicyContext`, `CommandContext`,
/// `Rejection`, `Bug`, `Failed`, `BusinessStatus`, `RetryableStatus`) is
/// refused, naming
/// both, never renamed.
#[test]
fn test_markup_domain_refuses_a_message_named_as_a_reserved_name() {
    const FILE: &str = "bad/v1/reserved_name.proto";
    assert_eq!(
        markup_domain(
            &descriptors(&[FILE]),
            &[FILE],
            &[("bad.v1", "::bad_proto::v1")]
        ),
        Err(CodegenError::Proto(ProtoError::ReservedName {
            message: "bad.v1.Context".to_string(),
            name: "Context".to_string(),
        }))
    );
}

/// `RetryableStatus` is reserved as well: the status a handler's
/// retryable bug goes out under, re-exported by the domain layer.
#[test]
fn test_markup_domain_refuses_a_message_named_retryable_status() {
    const FILE: &str = "bad/v1/reserved_retryable_status.proto";
    assert_eq!(
        markup_domain(
            &descriptors(&[FILE]),
            &[FILE],
            &[("bad.v1", "::bad_proto::v1")]
        ),
        Err(CodegenError::Proto(ProtoError::ReservedName {
            message: "bad.v1.RetryableStatus".to_string(),
            name: "RetryableStatus".to_string(),
        }))
    );
}

/// `Bug` is reserved as well: the name a defect's class takes in the
/// domain layers.
#[test]
fn test_markup_domain_refuses_a_message_named_bug() {
    const FILE: &str = "bad/v1/reserved_bug.proto";
    assert_eq!(
        markup_domain(
            &descriptors(&[FILE]),
            &[FILE],
            &[("bad.v1", "::bad_proto::v1")]
        ),
        Err(CodegenError::Proto(ProtoError::ReservedName {
            message: "bad.v1.Bug".to_string(),
            name: "Bug".to_string(),
        }))
    );
}

/// The names of the file's top-level enums and structs.
fn items(file: &syn::File) -> Vec<String> {
    file.items
        .iter()
        .filter_map(|i| match i {
            syn::Item::Enum(e) => Some(e.ident.to_string()),
            syn::Item::Struct(s) => Some(s.ident.to_string()),
            _ => None,
        })
        .collect()
}

#[test]
fn test_markup_domain_has_each_components_command_event_and_refusal_enums() {
    let names = items(&buyin());
    for name in [
        "WalletStateCommand",
        "WalletStateEvent",
        "WalletStateRefusal",
        "BuyInStateCommand",
        "BuyInStateEvent",
        "BuyInStateRefusal",
    ] {
        assert!(names.contains(&name.to_string()), "{name}: {names:?}");
    }
}

/// The domain layer gives the user's files the runtime's contexts and what
/// a handler fails with under its own names, so a user file imports only
/// `crate::domain`: a handler's `Context`, a policy reaction's
/// `PolicyContext`, a refused command's `CommandContext`, the refused answer `Refused`, `Failed`,
/// `Unrefusable`, `BusinessStatus` and `RetryableStatus`, and `Bug`, a diagnostic key and its args, which a
/// command handler's `Failed` of its rejection or of a typed refusal takes
/// by `?`.
#[test]
fn test_markup_domain_gives_the_users_files_the_contexts_and_the_bug() {
    let file = buyin();
    let reexports: Vec<String> = file
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Use(u) => Some(
                quote::ToTokens::to_token_stream(&u.tree)
                    .to_string()
                    .replace(' ', ""),
            ),
            _ => None,
        })
        .collect();
    for name in [
        "Context",
        "PolicyContext",
        "CommandContext",
        "Refused",
        "{BusinessStatus,Failed,RetryableStatus,Unrefusable,}",
    ] {
        assert!(
            reexports.contains(&format!("angzarr_embedded::markup::{name}")),
            "{name}: {reexports:?}"
        );
    }
    assert!(
        items(&file).contains(&"Bug".to_string()),
        "{:?}",
        items(&file)
    );
    let impls = impls(&file);
    assert!(impls.contains(&(
        "::angzarr_embedded::angzarr_diagnostic::Diagnostic".to_string(),
        "Bug".to_string()
    )));
    for failed in ["Failed<Unrefusable>", "Failed<InsufficientFunds>"] {
        assert!(
            impls.contains(&("::core::convert::From<Bug>".to_string(), failed.to_string())),
            "{failed}: {impls:?}"
        );
    }
}

#[test]
fn test_markup_domain_gives_a_typed_refusal_its_diagnostic() {
    assert!(impls(&buyin()).contains(&(
        "::angzarr_embedded::angzarr_diagnostic::Diagnostic".to_string(),
        "InsufficientFunds".to_string()
    )));
}

#[test]
fn test_markup_domain_maps_a_nested_enum_after_its_message() {
    const FILES: &[&str] = &[
        "io/angzarr/examples/v1/table.proto",
        "io/angzarr/examples/v1/player.proto",
    ];
    let generated = markup_domain(
        &descriptors(BLACKJACK),
        FILES,
        &[("io.angzarr.examples.v1", "crate::wire")],
    )
    .expect("SeatOutcome.Outcome is mapped");
    let file = syn::parse_file(&generated[0].source).unwrap();
    assert!(
        items(&file).contains(&"SeatOutcomeOutcome".to_string()),
        "{:?}",
        items(&file)
    );
}

/// A domain type derives `Default` when every field has one: a closed
/// enum has none (its zero is no value), so a type holding one singular
/// has none either; a list of them, or an `Option`, has.
#[test]
fn test_markup_domain_derives_default_where_every_field_has_one() {
    let defaulted: Vec<String> = shop()
        .items
        .iter()
        .filter_map(|i| match i {
            syn::Item::Struct(s)
                if s.attrs
                    .iter()
                    .any(|a| a.path().is_ident("derive") && text(a).contains("Default")) =>
            {
                Some(s.ident.to_string())
            }
            _ => None,
        })
        .collect::<Vec<String>>();
    assert!(
        defaulted.contains(&"CheckedOut".to_string()),
        "{defaulted:?}"
    );
    assert!(!defaulted.contains(&"Line".to_string()), "{defaulted:?}");
}

#[test]
fn test_markup_domain_command_enum_holds_the_commands_not_the_facts() {
    let file = buyin();
    let variants: Vec<String> = file
        .items
        .iter()
        .find_map(|i| match i {
            syn::Item::Enum(e) if e.ident == "BuyInStateCommand" => {
                Some(e.variants.iter().map(|v| v.ident.to_string()).collect())
            }
            _ => None,
        })
        .expect("BuyInStateCommand");
    assert!(variants.contains(&"StartBuyIn".to_string()), "{variants:?}");
    assert!(!variants.contains(&"HoldLost".to_string()), "{variants:?}");
}

#[test]
fn test_markup_domain_message_fields_are_required_and_optional_scalars_stay_optional() {
    let file = shop();
    assert_eq!(
        fields(&file, "CheckedOut"),
        s(&[
            ("owner", "String"),
            ("lines", "Vec<Line>"),
            ("totals", "::std::collections::BTreeMap<String,i64>"),
            ("discount", "Option<i32>"),
            ("opened_at", "::angzarr_embedded::Timestamp"),
            ("by_sku", "::std::collections::BTreeMap<String,Line>"),
        ])
    );
}

#[test]
fn test_markup_domain_enums_are_closed_and_omit_the_zero_value() {
    let file = shop();
    assert_eq!(
        fields(&file, "Line"),
        s(&[
            ("sku", "String"),
            ("count", "i64"),
            ("status", "Status"),
            ("history", "Vec<Status>"),
            ("last", "Option<Status>"),
            ("note", "Vec<u8>"),
        ])
    );
    assert_eq!(
        variants(&file, "Status"),
        s(&[("Open", ""), ("Shipped", "")])
    );
}

#[test]
fn test_markup_domain_converts_each_type_to_and_from_its_wire_message() {
    let impls = impls(&shop());
    for name in ["Line", "AddLine", "Checkout", "LineAdded", "CheckedOut"] {
        let wire = format!("::shop_proto::v1::{name}");
        assert!(
            impls.contains(&(format!("From<{name}>"), wire.clone())),
            "{name} into {wire}: {impls:?}"
        );
        assert!(
            impls.contains(&(format!("TryFrom<{wire}>"), name.to_string())),
            "{wire} into {name}: {impls:?}"
        );
    }
    assert!(impls.contains(&("From<Status>".to_string(), "i32".to_string())));
    assert!(impls.contains(&("TryFrom<i32>".to_string(), "Status".to_string())));
}

#[test]
fn test_markup_domain_maps_a_map_field_without_a_type_for_its_entry() {
    let names: Vec<String> = shop()
        .items
        .iter()
        .filter_map(|i| match i {
            syn::Item::Struct(s) => Some(s.ident.to_string()),
            _ => None,
        })
        .collect();
    assert!(names.contains(&"Line".to_string()), "{names:?}");
    assert!(!names.iter().any(|n| n.ends_with("Entry")), "{names:?}");
}

#[test]
fn test_markup_domain_without_a_component_generates_nothing() {
    let generated = markup_domain(
        &descriptors(&["io/angzarr/v1/options.proto"]),
        &["io/angzarr/v1/options.proto"],
        &[],
    )
    .unwrap();
    assert_eq!(generated, Vec::new());
}

#[test]
fn test_markup_domain_refuses_a_command_naming_an_unknown_component() {
    assert_eq!(
        refusal("bad/v1/unknown_component.proto"),
        CodegenError::Markup(MarkupError::UnknownComponent {
            message: "bad.v1.Poke".to_string(),
            component: "bad.v1.Nowhere".to_string(),
        })
    );
}

#[test]
fn test_markup_domain_refuses_a_command_naming_a_message_that_is_no_component() {
    assert_eq!(
        refusal("bad/v1/not_a_component.proto"),
        CodegenError::Markup(MarkupError::UnknownComponent {
            message: "bad.v1.Poke".to_string(),
            component: "bad.v1.Plain".to_string(),
        })
    );
}

#[test]
fn test_markup_domain_refuses_a_command_emitting_a_non_event() {
    assert_eq!(
        refusal("bad/v1/unknown_emit.proto"),
        CodegenError::Markup(MarkupError::UnknownEmit {
            command: "bad.v1.Poke".to_string(),
            event: "bad.v1.Poke".to_string(),
        })
    );
}

#[test]
fn test_markup_domain_refuses_an_enum_whose_zero_is_a_value() {
    assert_eq!(
        refusal("bad/v1/enum_zero.proto"),
        CodegenError::Proto(ProtoError::EnumZero {
            enum_name: "bad.v1.Mood".to_string(),
            value: "MOOD_CALM".to_string(),
        })
    );
}

/// A oneof is mapped: a choice of its arms (`<Message><Oneof>`), held
/// once in the message.
#[test]
fn test_markup_domain_maps_a_oneof() {
    let generated = markup_domain(
        &descriptors(&["bad/v1/oneof.proto"]),
        &["bad/v1/oneof.proto"],
        BAD_WIRE,
    )
    .expect("a oneof is mapped");
    let file = syn::parse_file(&generated[0].source).unwrap();
    assert_eq!(
        variants(&file, "PokeShape"),
        s(&[("Round", "(String)"), ("Square", "(String)")])
    );
    assert_eq!(fields(&file, "Poke"), s(&[("shape", "PokeShape")]));
}

/// A nested message is mapped, named after its enclosing message
/// (`<Message><Nested>`), as a nested enum is.
#[test]
fn test_markup_domain_maps_a_nested_message() {
    let generated = markup_domain(
        &descriptors(&["bad/v1/nested.proto"]),
        &["bad/v1/nested.proto"],
        BAD_WIRE,
    )
    .expect("a nested message is mapped");
    let file = syn::parse_file(&generated[0].source).unwrap();
    assert_eq!(fields(&file, "Poke"), s(&[("inner", "PokeInner")]));
    assert_eq!(fields(&file, "PokeInner"), s(&[("x", "String")]));
}

#[test]
fn test_markup_domain_refuses_a_component_of_no_kind() {
    assert_eq!(
        refusal("bad/v1/unspecified_kind.proto"),
        CodegenError::Markup(MarkupError::UnsupportedKind {
            component: "bad.v1.ThingState".to_string(),
            kind: 0,
        })
    );
}

#[test]
fn test_markup_domain_refuses_a_well_known_type_it_does_not_map() {
    assert_eq!(
        refusal("bad/v1/well_known.proto"),
        CodegenError::Proto(ProtoError::WellKnownType {
            message: "bad.v1.Poke".to_string(),
            field: "timeout".to_string(),
            type_name: "google.protobuf.Struct".to_string(),
        })
    );
}

#[test]
fn test_markup_domain_refuses_a_package_without_a_wire_path() {
    assert_eq!(
        markup_domain(&descriptors(&[SHOP]), &[SHOP], &[]),
        Err(CodegenError::Use(UseError::NoWirePath {
            package: "shop.v1".to_string(),
        }))
    );
}

#[test]
fn test_markup_domain_refuses_a_file_not_in_the_set() {
    assert_eq!(
        markup_domain(&descriptors(&[SHOP]), &["shop/v1/absent.proto"], SHOP_WIRE),
        Err(CodegenError::Use(UseError::FileNotInSet {
            file: "shop/v1/absent.proto".to_string(),
        }))
    );
}

#[test]
fn test_markup_domain_refuses_bytes_that_are_not_a_descriptor_set() {
    assert!(matches!(
        markup_domain(&[0xff, 0xff], &[SHOP], SHOP_WIRE),
        Err(CodegenError::Proto(ProtoError::Descriptors { .. }))
    ));
}

#[test]
fn test_every_error_has_a_catalog_message_with_its_args() {
    let samples = angzarr_codegen::diagnostic_samples();
    let problems = angzarr_diagnostic::catalog::check(
        angzarr_codegen::EN_US.0,
        angzarr_codegen::EN_US.1,
        &samples,
    );
    assert!(problems.is_empty(), "{problems:#?}");
    // Fluent's own parser and formatter agree: every message formats with
    // exactly its error's args.
    let fluent =
        angzarr_presentation::Catalog::en_us(&[angzarr_codegen::EN_US]).expect("the catalog loads");
    for sample in &samples {
        fluent
            .render(&**sample)
            .unwrap_or_else(|e| panic!("{}: {e:?}", sample.code()));
    }
}

#[test]
fn test_every_catalog_message_is_the_code_of_a_sample() {
    let mut messages: Vec<&str> = angzarr_codegen::EN_US
        .1
        .lines()
        .filter_map(|line| line.split_once(" = ").map(|(id, _)| id))
        .collect();
    messages.sort_unstable();
    let samples = angzarr_codegen::diagnostic_samples();
    let mut codes: Vec<&str> = samples.iter().map(|d| d.code()).collect();
    codes.sort_unstable();
    assert_eq!(codes, messages);
}

/// Every error displays as its code and args, never English (decision
/// 14.37), and core's catalog check agrees with the catalog: the English is
/// rendered where it is shown (angzarr-build's build script, the CLI).
#[test]
fn test_every_error_displays_as_its_code_and_args() {
    let (krate, ftl) = angzarr_codegen::EN_US;
    let problems =
        angzarr_diagnostic::catalog::check(krate, ftl, &angzarr_codegen::diagnostic_samples());
    assert_eq!(problems, Vec::new());
}

/// A policy's interface is in its target's layer, beside the outputs it
/// names, even when the policy is declared in a package with no domain
/// types of its own (item S).
#[test]
fn test_a_policys_interface_is_its_targets_layers() {
    const FILES: &[&str] = &["split/v1/depot.proto", "split/policy/v1/route.proto"];
    let layers = markup_domain(&descriptors(FILES), FILES, &[("split.v1", "crate::wire")])
        .expect("one layer, the depot's");
    assert_eq!(layers.len(), 1);
    assert!(
        layers[0]
            .source
            .contains("pub trait RoutePolicyInterface {"),
        "{}",
        layers[0].source
    );
}

/// Each domain's `<Component>Output` is a `CommandTarget` (decision 14.74:
/// the destination a process manager's compensation step names): its
/// domain and its commands, never its facts; a domain taking facts alone
/// has none, typed as the list it returns.
#[test]
fn test_each_output_is_a_command_target_of_its_commands() {
    const VIEWS: &str = "pyshop/v1/views.proto";
    let generated = markup_domain(
        &descriptors(&[VIEWS]),
        &[VIEWS],
        &[("pyshop.v1", "crate::wire")],
    )
    .unwrap()
    .remove(0)
    .source;
    let file = syn::parse_file(&generated).unwrap();
    let target = impls(&file)
        .into_iter()
        .find(|(t, ty)| t.ends_with("CommandTarget") && ty == "TallyStateOutput")
        .map(|(_, ty)| ty);
    assert_eq!(target.as_deref(), Some("TallyStateOutput"), "{generated}");
    let flat: String = generated.split_whitespace().collect();
    assert!(
        flat.contains("constDOMAIN:&'staticstr=\"tally\";fncommand_types()->Vec<String>{vec![]}"),
        "{generated}"
    );
    let buyin: String = markup_domain(&descriptors(BUYIN), BUYIN, BUYIN_WIRE)
        .unwrap()
        .remove(0)
        .source
        .split_whitespace()
        .collect();
    assert!(
        buyin.contains(
            "fncommand_types()->Vec<String>{vec![\"buyin.v1.Deposit\".to_string(),\"buyin.v1.Hold\".to_string()]}"
        ),
        "{buyin}"
    );
}
