//! What a build script gets: angzarr's prost types for a wire crate, the
//! domain layer, and the descriptors, each written into the output
//! directory, and the failures it reports.

use std::path::Path;

use angzarr_build::{
    BuildError, Existing, ScaffoldConfig, Scaffolded, adapters_into, adapters_with_into, json_into,
    markup_domain_into, markup_domain_with_into, scaffold_with_into, wire_into,
};

use test_dir::TestDir;

const SHOP: &str = "shop/v1/shop.proto";

fn fixtures() -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../angzarr-codegen/tests/fixtures")
        .display()
        .to_string()
}

/// A fresh, empty output directory.
fn out_dir() -> TestDir {
    TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-test"))
}

#[test]
fn test_wire_into_writes_prost_types_by_package() {
    let out = out_dir();
    wire_into(&out, &[SHOP], &[&fixtures()]).unwrap();
    let source = std::fs::read_to_string(out.join("shop.v1.rs")).expect("shop.v1.rs");
    let file = syn::parse_file(&source).unwrap();
    assert!(
        file.items
            .iter()
            .any(|i| matches!(i, syn::Item::Struct(s) if s.ident == "CartState")),
        "prost's CartState"
    );
}

/// The shapes fixtures, which import the codegen options: importable
/// without an include of their own, as angzarr's protos are.
const SHAPES: &[&str] = &[
    "shapes/common/v1/common.proto",
    "shapes/catalog/v1/catalog.proto",
    "shapes/order/v1/order.proto",
];

#[test]
fn test_markup_domain_with_into_writes_a_layer_per_package_each_at_its_path() {
    let out = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-domain-with"));
    let packages = ["shapes.common.v1", "shapes.catalog.v1", "shapes.order.v1"];
    let wire: Vec<(&str, &str)> = packages.iter().map(|p| (*p, "crate::wire")).collect();
    let domain = [
        ("shapes.common.v1", "crate::common"),
        ("shapes.catalog.v1", "crate::catalog"),
        ("shapes.order.v1", "crate::order"),
    ];
    markup_domain_with_into(&out, SHAPES, &[&fixtures()], &wire, &domain).unwrap();
    for package in packages {
        assert!(
            out.join(format!("{package}.domain.rs")).exists(),
            "{package}"
        );
    }
    let order = std::fs::read_to_string(out.join("shapes.order.v1.domain.rs")).unwrap();
    assert!(order.contains("Vec<crate::catalog::Item>"), "{order}");
}

/// The adapters, each package's reading its own layer at its path, and
/// with none given the one `crate::domain`.
#[test]
fn test_adapters_with_into_reads_each_packages_layer_at_its_path() {
    let out = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-adapters-with"));
    let domain = [
        ("shapes.catalog.v1", "crate::catalog"),
        ("shapes.order.v1", "crate::order"),
    ];
    let read = || std::fs::read_to_string(out.join("angzarr-adapters.rs")).unwrap();
    adapters_with_into(&out, SHAPES, &[&fixtures()], &[], &domain).unwrap();
    let source = read();
    assert!(source.contains("use crate::order::*;"), "{source}");
    assert!(
        source.contains("crate::catalog::CatalogStateInterface::list_item"),
        "{source}"
    );
    adapters_into(&out, SHAPES, &[&fixtures()], &[]).unwrap();
    let source = read();
    assert!(source.contains("use crate::domain::*;"), "{source}");
    assert!(!source.contains("crate::order::"), "{source}");
}

/// The scaffold, each component's implementation importing its package's
/// layer at its path.
#[test]
fn test_scaffold_with_into_imports_each_packages_layer_at_its_path() {
    let src = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-scaffold-with"));
    let domain = [
        ("shapes.catalog.v1", "crate::catalog"),
        ("shapes.order.v1", "crate::order"),
    ];
    let done = scaffold_with_into(
        &src,
        SHAPES,
        &[&fixtures()],
        Existing::Keep,
        &[],
        &domain,
        &ScaffoldConfig::default(),
    )
    .unwrap();
    assert!(
        done.contains(&Scaffolded::Created(src.join("order_state_impl.rs"))),
        "{done:?}"
    );
    let order = std::fs::read_to_string(src.join("order_state_impl.rs")).unwrap();
    assert!(order.contains("use crate::order::*;\n"), "{order}");
}

/// The JSON of the wire types is written only when asked for, as
/// `angzarr-json.rs`.
#[test]
fn test_json_into_writes_the_json_of_the_wire_types() {
    let out = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-json"));
    wire_into(&out, &["json/v1/json.proto"], &[&fixtures()]).unwrap();
    assert!(
        !out.join("angzarr-json.rs").exists(),
        "wire alone writes no JSON"
    );
    json_into(
        &out,
        &["json/v1/json.proto"],
        &[&fixtures()],
        &[("json.v1", "crate::wire")],
    )
    .unwrap();
    let source = std::fs::read_to_string(out.join("angzarr-json.rs")).unwrap();
    assert!(
        source.contains("impl JsonMessage for crate::wire::Plain"),
        "{source}"
    );
}

#[test]
fn test_markup_domain_into_reports_the_generators_refusal() {
    let out = out_dir();
    let error = markup_domain_into(&out, &[SHOP], &[&fixtures()], &[]).expect_err("no wire path");
    assert!(matches!(error, BuildError::Codegen(_)), "{error:?}");
}

#[test]
fn test_markup_domain_into_reports_a_proto_protoc_cannot_find() {
    let out = out_dir();
    let error = markup_domain_into(&out, &["shop/v1/absent.proto"], &[&fixtures()], &[])
        .expect_err("no such proto");
    assert!(matches!(error, BuildError::Protoc { .. }), "{error:?}");
}

#[test]
fn test_every_error_has_a_catalog_message_with_its_args() {
    let ftl = angzarr_build::EN_US.1;
    let samples = angzarr_build::diagnostic_samples();
    let problems = angzarr_diagnostic::catalog::check(angzarr_build::EN_US.0, ftl, &samples);
    assert!(problems.is_empty(), "{problems:#?}");
    // Fluent's own parser and formatter agree: every message formats with
    // exactly its error's args.
    let fluent = presentation::Catalog::en_us(&[angzarr_build::EN_US]).expect("the catalog loads");
    for sample in &samples {
        fluent
            .render(&**sample)
            .unwrap_or_else(|e| panic!("{}: {e:?}", sample.code()));
    }
    // A sample of every message the catalog has.
    let mut codes: Vec<&str> = samples.iter().map(|d| d.code()).collect();
    codes.sort_unstable();
    let mut messages: Vec<&str> = ftl
        .lines()
        .filter_map(|l| l.split_once(" = ").map(|(id, _)| id))
        .collect();
    messages.sort_unstable();
    assert_eq!(codes, messages);
}

/// A build script's `main` returning the error prints its `Debug`: that is
/// where a person reads it, so it is the English, the generator's refusal
/// as its own.
#[test]
fn test_a_failing_build_script_prints_the_errors_english() {
    let s = || "x y".to_string();
    let cases = [
        (
            BuildError::NoOutDir,
            "OUT_DIR is unset: run this from a build script",
        ),
        (
            BuildError::Io {
                path: "a/b".to_string(),
                detail: s(),
            },
            "a/b: x y",
        ),
        (BuildError::Protoc { detail: s() }, "protoc failed: x y"),
        (BuildError::Prost { detail: s() }, "prost-build failed: x y"),
        (BuildError::Buf { detail: s() }, "buf generate failed: x y"),
        (BuildError::Uv { detail: s() }, "uv failed: x y"),
        (
            BuildError::Codegen(angzarr_codegen::CodegenError::Use(
                angzarr_codegen::UseError::NoWirePath {
                    package: "shop.v1".to_string(),
                },
            )),
            "no Rust path is given for the prost types of package shop.v1",
        ),
    ];
    for (error, english) in cases {
        assert_eq!(format!("{error:?}"), english);
    }
}

/// Every error displays as its code and args, never English (decision
/// 14.37), and core's catalog check agrees with the catalog.
#[test]
fn test_every_error_displays_as_its_code_and_args() {
    let (krate, ftl) = angzarr_build::EN_US;
    let problems =
        angzarr_diagnostic::catalog::check(krate, ftl, &angzarr_build::diagnostic_samples());
    assert_eq!(problems, Vec::new());
}

#[test]
fn test_component_names_are_the_protos_with_their_application_names() {
    let names: Vec<(String, String)> = angzarr_build::component_names(
        &[
            "buyin/v1/wallet.proto",
            "buyin/v1/buy_in.proto",
            "buyin/v1/policies.proto",
        ],
        &[&fixtures()],
    )
    .unwrap()
    .into_iter()
    .map(|c| (c.component, c.name))
    .collect();
    let s = |a: &str, b: &str| (a.to_string(), b.to_string());
    assert_eq!(
        names,
        [
            s("buyin.v1.WalletState", "wallet"),
            s("buyin.v1.BuyInState", "pmg-buyin"),
            s("buyin.v1.BuyInWalletPolicy", "policy-buyin-wallet"),
            s("buyin.v1.WalletBuyInPolicy", "policy-wallet-buyin"),
        ]
    );
}

#[test]
fn test_descriptors_into_writes_each_components_descriptor_with_the_version() {
    let out = out_dir();
    angzarr_build::descriptors_into(
        &out,
        &["orders/v1/orders.proto"],
        &[&fixtures()],
        &[],
        "2.0.0",
    )
    .unwrap();
    let source = std::fs::read_to_string(out.join("angzarr-descriptors.rs"))
        .expect("angzarr-descriptors.rs");
    let file = syn::parse_file(&source).unwrap();
    assert!(
        file.items
            .iter()
            .any(|i| matches!(i, syn::Item::Const(c) if c.ident == "DESCRIPTORS")),
        "const DESCRIPTORS"
    );
    // The version is in the encoded descriptors.
    assert!(source.contains("2.0.0"), "{source}");
}

/// A child-served domain's stand-in, generated from its proto into the
/// output directory, for a crate that hand-writes the policies naming it.
#[test]
fn test_stand_ins_into_writes_the_remote_domains_stand_in() {
    let out = out_dir();
    angzarr_build::stand_ins_into(&out, &["buyin/v1/wallet.proto"], &[&fixtures()]).unwrap();
    let source = std::fs::read_to_string(out.join("angzarr-stand-ins.rs")).unwrap();
    assert!(source.contains("impl CommandTarget for Wallet"), "{source}");
    assert!(source.contains("impl Decides<Hold> for Wallet"), "{source}");
}
