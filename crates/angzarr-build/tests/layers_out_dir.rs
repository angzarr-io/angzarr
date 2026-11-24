//! `markup_domain_with`, `adapters_with` and `json`, with no arguments but
//! their protos and paths: the output directory is Cargo's. Alone in its
//! binary, as it sets `OUT_DIR` for the process.

use std::path::Path;

use test_dir::TestDir;

const SHAPES: &[&str] = &[
    "shapes/common/v1/common.proto",
    "shapes/catalog/v1/catalog.proto",
    "shapes/order/v1/order.proto",
];
const DOMAIN: &[(&str, &str)] = &[
    ("shapes.common.v1", "crate::domain::common"),
    ("shapes.catalog.v1", "crate::domain::catalog"),
    ("shapes.order.v1", "crate::domain::order"),
];

#[test]
fn test_the_layers_adapters_and_json_are_written_into_out_dir() {
    let out = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-layers-out-dir"));
    // SAFETY: this binary's one test; nothing else reads the environment.
    unsafe { std::env::set_var("OUT_DIR", &out) };
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../angzarr-codegen/tests/fixtures")
        .display()
        .to_string();
    let read = |file: &str| {
        std::fs::read_to_string(out.join(file)).unwrap_or_else(|e| panic!("{file}: {e}"))
    };
    let wire: Vec<(&str, &str)> = DOMAIN.iter().map(|(p, _)| (*p, "crate::wire")).collect();
    angzarr_build::markup_domain_with(SHAPES, &[&fixtures], &wire, DOMAIN).unwrap();
    let order = read("shapes.order.v1.domain.rs");
    assert!(
        order.contains("Vec<crate::domain::catalog::Item>"),
        "{order}"
    );
    angzarr_build::adapters_with(SHAPES, &[&fixtures], &[], DOMAIN).unwrap();
    let adapters = read("angzarr-adapters.rs");
    assert!(
        adapters.contains("use crate::domain::order::*;"),
        "{adapters}"
    );
    angzarr_build::json(
        &["json/v1/json.proto"],
        &[&fixtures],
        &[("json.v1", "crate::wire")],
    )
    .unwrap();
    let json = read("angzarr-json.rs");
    assert!(
        json.contains("impl JsonMessage for crate::wire::Plain"),
        "{json}"
    );
}
