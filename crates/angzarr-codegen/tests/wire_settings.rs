//! `markup_domain` documents the prost settings it is generated for
//! (bytes as `Vec<u8>`, maps as `BTreeMap`) and checks them where the wire
//! types are compiled: a prost crate built otherwise fails with the named
//! error. The checks are compiled here with rustc against a wire module of
//! each kind.

mod common;

use angzarr_codegen::markup_domain;
use angzarr_diagnostic::code;
use common::*;
use quote::ToTokens;

use angzarr_test_dir::TestDir;

const WIRE: &str = "wire/v1/wire.proto";

/// The generated layer's settings checks alone: its `wire_settings` module
/// and the `const _` items naming the wire types.
fn checks() -> String {
    let generated = markup_domain(
        &descriptors(&[WIRE]),
        &[WIRE],
        &[("wire.v1", "crate::wire")],
    )
    .unwrap();
    let file = syn::parse_file(&generated[0].source).unwrap();
    file.items
        .iter()
        .filter(|item| match item {
            syn::Item::Mod(m) => m.ident == "wire_settings",
            syn::Item::Const(c) => c.ident == "_",
            _ => false,
        })
        .map(|item| item.to_token_stream().to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

/// rustc on `source` as a library with warnings denied: its diagnostics,
/// if it refused it.
fn rustc(source: &str) -> Result<(), String> {
    use std::process::Command;
    let dir = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-wire-settings"));
    std::fs::write(dir.join("lib.rs"), source).unwrap();
    let out = Command::new(std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string()))
        .current_dir(&dir)
        .args(["--edition=2021", "--crate-type=lib", "-Dwarnings", "lib.rs"])
        .output()
        .expect("rustc on PATH");
    if out.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).into_owned())
    }
}

fn wire(bytes: &str, map: &str) -> String {
    format!(
        "pub mod wire {{\n\
         pub struct Store {{ pub blob: {bytes}, pub maybe: Option<{bytes}>, pub many: Vec<{bytes}> }}\n\
         pub struct Stored {{ pub index: {map}<String, i32>, pub count: i32 }}\n\
         }}\n"
    )
}

#[test]
fn test_the_settings_checks_pass_on_angzarrs_prost_settings() {
    let source = format!(
        "{}\n{}",
        wire("Vec<u8>", "::std::collections::BTreeMap"),
        checks()
    );
    if let Err(diagnostics) = rustc(&source) {
        panic!("{source}\n{diagnostics}");
    }
}

#[test]
fn test_bytes_built_otherwise_fail_with_the_named_error() {
    let source = format!(
        "{}\n{}",
        wire("String", "::std::collections::BTreeMap"),
        checks()
    );
    let diagnostics = rustc(&source).expect_err("refused");
    assert!(diagnostics.contains(code!("wire-bytes")), "{diagnostics}");
    assert!(!diagnostics.contains(code!("wire-map")), "{diagnostics}");
}

#[test]
fn test_maps_built_otherwise_fail_with_the_named_error() {
    let source = format!(
        "{}\n{}",
        wire("Vec<u8>", "::std::collections::HashMap"),
        checks()
    );
    let diagnostics = rustc(&source).expect_err("refused");
    assert!(diagnostics.contains(code!("wire-map")), "{diagnostics}");
    assert!(!diagnostics.contains(code!("wire-bytes")), "{diagnostics}");
}

#[test]
fn test_a_layer_without_bytes_or_maps_checks_nothing() {
    let generated = markup_domain(
        &descriptors(&[REFUSALS]),
        &[REFUSALS],
        &[("refusals.v1", "crate::wire")],
    )
    .unwrap();
    assert!(!generated[0].source.contains("wire_settings"));
}

const REFUSALS: &str = "refusals/v1/refusals.proto";

/// `angzarr-wire.rs`: each package's prost types in modules nested as the
/// package is, the package's own module carrying the allowance prost's
/// output needs (item S), angzarr's own and google's left to angzarr-proto
/// and prost.
#[test]
fn test_wire_modules_nest_each_package_with_its_allowance() {
    let mut files = BLACKJACK.to_vec();
    files.extend(["io/angzarr/v1/options.proto", "google/protobuf/any.proto"]);
    let source = angzarr_codegen::wire_modules(&common::descriptors(BLACKJACK), &files).unwrap();
    let flat = source.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        flat.contains(
            "pub mod io { pub mod angzarr { pub mod examples { #[allow(clippy::excessive_nesting)] \
             pub mod v1 { include!(concat!(env!(\"OUT_DIR\"), \"/io.angzarr.examples.v1.rs\")); } } } }"
        ),
        "{source}"
    );
    assert!(!source.contains("/io.angzarr.v1.rs"), "{source}");
    assert!(!source.contains("google"), "{source}");
    assert!(angzarr_codegen::is_generated(&source), "{source}");
}
