//! What a build script calls with no arguments but its protos: the output
//! directory and the version are Cargo's. Alone in its binary, as it sets
//! `OUT_DIR` for the process.

use std::path::Path;

use angzarr_test_dir::TestDir;

#[test]
fn test_descriptors_writes_into_out_dir_with_the_crates_version() {
    let out = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-out-dir"));
    // SAFETY: this binary's one test; nothing else reads the environment.
    unsafe { std::env::set_var("OUT_DIR", &out) };
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../angzarr-codegen/tests/fixtures")
        .display()
        .to_string();
    angzarr_build::descriptors(&["orders/v1/orders.proto"], &[&fixtures], &[]).unwrap();
    let source = std::fs::read_to_string(out.join("angzarr-descriptors.rs")).unwrap();
    // The doc names the version, and so do the bytes.
    assert!(
        source.contains(&format!("{:?}", env!("CARGO_PKG_VERSION"))),
        "{source}"
    );
    assert!(source.contains("projector-audit-trail-view"), "{source}");
    // The bytes: every component's `rules_version` (field 2, a string) is
    // the version, not an empty one.
    let version = env!("CARGO_PKG_VERSION");
    let field = format!("\\x12\\x{:02X}{version}", version.len());
    let components = source.matches("b\"\\n").count();
    assert!(components > 1, "{source}");
    assert_eq!(source.matches(&field).count(), components, "{source}");
}
