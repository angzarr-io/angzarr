//! `angzarr_build::app` and `app_without_domain`, as the build script
//! `angzarr scaffold app` writes calls them: the config relative to the
//! crate (`CARGO_MANIFEST_DIR`), the output Cargo's (`OUT_DIR`). Alone in
//! its binary, as it sets both for the process.

use std::path::Path;

use angzarr_test_dir::TestDir;

#[test]
fn test_app_reads_the_crates_config_and_writes_into_out_dir() {
    let root = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-app-out"));
    let out = root.join("out");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(
        root.join("angzarr.yaml"),
        "schema_version: 1\
         \ncomponent:\
         \n  - name: carts\
         \n    kind: domain\
         \n    language: rust\
         \n",
    )
    .unwrap();
    // SAFETY: this binary's one test; nothing else reads the environment.
    unsafe {
        std::env::set_var("OUT_DIR", &out);
        std::env::set_var("CARGO_MANIFEST_DIR", &root);
    }
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../angzarr-codegen/tests/fixtures")
        .display()
        .to_string();
    angzarr_build::app("angzarr.yaml", &["shop/v1/shop.proto"], &[&fixtures]).unwrap();
    let register = std::fs::read_to_string(out.join("angzarr-register.rs")).unwrap();
    assert!(register.contains("adapters::Cart"), "{register}");
    // Over a domain crate: the same layer, but no wire types or domain
    // layer of its own.
    let over = root.join("over");
    std::fs::create_dir_all(&over).unwrap();
    std::fs::write(
        root.join("Cargo.toml"),
        "[package]\nname = \"x\"\n\n[package.metadata.angzarr.domain]\ncrate = \"shop_proto\"\n",
    )
    .unwrap();
    // SAFETY: as above.
    unsafe { std::env::set_var("OUT_DIR", &over) };
    angzarr_build::app_without_domain("angzarr.yaml", &["shop/v1/shop.proto"], &[&fixtures])
        .unwrap();
    assert!(over.join("angzarr-register.rs").exists());
    assert!(!over.join("shop.v1.domain.rs").exists());
    let layer = std::fs::read_to_string(over.join("angzarr-app.rs")).unwrap();
    assert!(layer.contains("\npub use shop_proto::domain;\n"), "{layer}");
}
