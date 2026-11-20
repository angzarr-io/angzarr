//! The generated proto3 JSON of wire types (`json`), read back as Rust
//! items: an impl per message, the serde impls of the files' own, each
//! enum's names as its options say, and what the generator refuses.
//! Whether it compiles, round-trips and agrees with prost-reflect is
//! angzarr-build's `tests/local` json-app.

mod common;

use angzarr_codegen::{CodegenError, ProtoError, UseError, json};
use common::*;

const JSON: &str = "json/v1/json.proto";
const WIRE: &[(&str, &str)] = &[("json.v1", "crate::wire::json::v1")];

fn generated() -> String {
    json(&descriptors(&[JSON]), &[JSON], WIRE).unwrap()
}

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

/// The json fixture's generated JSON is the runtime, verbatim, and the
/// golden file around it: the text angzarr-build's `tests/local` json-app
/// compiles, holds to its golden JSON, round-trips and cross-checks with
/// prost-reflect.
#[test]
fn test_the_generated_json_is_the_runtime_and_the_golden_file() {
    let runtime = include_str!("../src/json/runtime.rs");
    let generated = generated();
    let (head, rest) = generated
        .split_once(runtime)
        .expect("the runtime, verbatim");
    golden("json.json.rs", &format!("{head}// (the runtime)\n{rest}"));
}

fn file() -> syn::File {
    syn::parse_file(&generated()).expect("the generated JSON parses")
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

fn flat(source: &str) -> String {
    source.split_whitespace().collect()
}

const MESSAGES: &[&str] = &[
    "Plain", "Known", "Choice", "Site", "Finished", "Started", "Output",
];

#[test]
fn test_every_message_has_its_json_and_serde_impls() {
    let impls = impls(&file());
    for message in MESSAGES {
        let ty = format!("crate::wire::json::v1::{message}");
        for t in [
            "JsonMessage",
            "::serde::Serialize",
            "::serde::Deserialize<'de>",
        ] {
            assert!(
                impls.contains(&(t.to_string(), ty.clone())),
                "{t} for {ty}: {impls:?}"
            );
        }
    }
}

/// The runtime writes the well-known types and angzarr's UUID; a wire
/// type of another crate (angzarr-proto's) gets no serde impl.
#[test]
fn test_the_runtime_writes_the_well_known_types_and_uuid() {
    let impls = impls(&file());
    for ty in [
        "::prost_types::Timestamp",
        "::prost_types::Duration",
        "::prost_types::Any",
        "::angzarr_proto::v1::Uuid",
    ] {
        assert!(
            impls.contains(&("JsonMessage".to_string(), ty.to_string())),
            "{ty}"
        );
        assert!(
            !impls.contains(&("::serde::Serialize".to_string(), ty.to_string())),
            "{ty}"
        );
    }
}

/// An enum's names: kebab-case under JSON_CASE_KEBAB, a value's own
/// `json_name` over it, each read by its proto name too; another enum's
/// its proto names.
#[test]
fn test_each_enums_names_are_as_its_options_say() {
    let source = flat(&generated());
    for expected in [
        r#"(2i32,"not-called",&["not-called","STATUS_NOT_CALLED"])"#,
        r#"(3i32,"broke",&["broke","STATUS_FAILED"])"#,
        r#"(2i32,"TONE_LOUD",&["TONE_LOUD","TONE_LOUD"])"#,
    ] {
        assert!(source.contains(expected), "no {expected}");
    }
}

/// A field's key: its `json_name` by default, its proto name under
/// JSON_NAMES_PROTO, its `json_key` over both; a message's tag.
#[test]
fn test_keys_and_tags_are_as_the_options_say() {
    let source = flat(&generated());
    for expected in [
        r#"out.push(("snakeCaseField".to_string()"#,
        r#"out.push(("otherName".to_string()"#,
        r#"out.push(("took_ms".to_string()"#,
        r#"out.push(("reason".to_string()"#,
        r#"Some(("event","run.finished"))"#,
    ] {
        assert!(source.contains(expected), "no {expected}");
    }
}

#[test]
fn test_a_well_known_type_the_runtime_does_not_write_is_refused() {
    const FILE: &str = "bad/v1/well_known.proto";
    assert_eq!(
        json(&descriptors(&[FILE]), &[FILE], &[("bad.v1", "crate::wire")]),
        Err(CodegenError::Proto(ProtoError::WellKnownType {
            message: "bad.v1.Poke".to_string(),
            field: "timeout".to_string(),
            type_name: "google.protobuf.Struct".to_string(),
        }))
    );
}

#[test]
fn test_a_well_known_type_as_a_maps_values_is_refused() {
    const FILE: &str = "bad/v1/json_map_struct.proto";
    assert_eq!(
        json(&descriptors(&[FILE]), &[FILE], &[("bad.v1", "crate::wire")]),
        Err(CodegenError::Proto(ProtoError::WellKnownType {
            message: "bad.v1.Shelf".to_string(),
            field: "values".to_string(),
            type_name: "google.protobuf.Struct".to_string(),
        }))
    );
}

#[test]
fn test_flattening_a_map_is_refused() {
    const FILE: &str = "bad/v1/json_flatten_map.proto";
    assert_eq!(
        json(&descriptors(&[FILE]), &[FILE], &[("bad.v1", "crate::wire")]),
        Err(CodegenError::Proto(ProtoError::JsonFlatten {
            message: "bad.v1.Flat".to_string(),
            field: "inner".to_string(),
        }))
    );
}

#[test]
fn test_flattening_a_scalar_is_refused() {
    const FILE: &str = "bad/v1/json_flatten.proto";
    assert_eq!(
        json(&descriptors(&[FILE]), &[FILE], &[("bad.v1", "crate::wire")]),
        Err(CodegenError::Proto(ProtoError::JsonFlatten {
            message: "bad.v1.Flat".to_string(),
            field: "text".to_string(),
        }))
    );
}

#[test]
fn test_a_package_without_a_wire_path_is_refused() {
    assert_eq!(
        json(&descriptors(&[JSON]), &[JSON], &[]),
        Err(CodegenError::Use(UseError::NoWirePath {
            package: "json.v1".to_string(),
        }))
    );
}

#[test]
fn test_a_file_not_in_the_set_is_refused() {
    assert_eq!(
        json(&descriptors(&[JSON]), &["json/v1/absent.proto"], WIRE),
        Err(CodegenError::Use(UseError::FileNotInSet {
            file: "json/v1/absent.proto".to_string(),
        }))
    );
}

/// Without the options proto imported, the canonical mapping: no tag, the
/// `json_name` keys.
#[test]
fn test_without_options_the_mapping_is_canonical() {
    let source = json(
        &descriptors(&["shop/v1/shop.proto"]),
        &["shop/v1/shop.proto"],
        &[("shop.v1", "crate::wire")],
    )
    .unwrap();
    assert!(!flat(&source).contains("Some((\""), "{source}");
    assert!(flat(&source).contains(r#"out.push(("openedAt".to_string()"#));
}
