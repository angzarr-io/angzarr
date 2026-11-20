//! The generated `register(&mut Registry)`: every component the protos
//! declare, its adapter registered with angzarr-embedded, so an application
//! calls one function; a domain served by a child process is left to the
//! host. Pinned by a golden file; the
//! end-to-end test (angzarr-build's `tests/local.rs`) runs it.

mod common;

use angzarr_codegen::register;
use common::*;

use angzarr_test_dir::TestDir;

const GOLDEN: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/golden/buyin.register.rs"
);

fn buyin_register() -> String {
    register(&descriptors(BUYIN), BUYIN, &[]).unwrap()
}

#[test]
fn test_register_matches_the_golden_file() {
    let actual = buyin_register();
    // `ANGZARR_BLESS=1` rewrites the golden file; review the diff.
    if std::env::var_os("ANGZARR_BLESS").is_some() {
        std::fs::write(GOLDEN, &actual).unwrap();
        return;
    }
    assert_eq!(actual, std::fs::read_to_string(GOLDEN).unwrap());
}

/// Each registration in `register`'s body, as text without spacing.
fn registrations(source: &str) -> Vec<String> {
    let file = syn::parse_file(source).expect("register parses");
    let body = file
        .items
        .iter()
        .find_map(|i| match i {
            syn::Item::Fn(f) if f.sig.ident == "register" => Some(f),
            _ => None,
        })
        .expect("fn register");
    assert_eq!(
        // syn keeps `pub` in `vis`, apart from the signature.
        format!("{}{}", text(&body.vis), text(&body.sig)),
        "pubfnregister(registry:&mut::angzarr_embedded::Registry,)->::core::result::Result<(),::angzarr_embedded::Misconfigured>"
    );
    let mut stmts: Vec<String> = body.block.stmts.iter().map(text).collect();
    assert_eq!(
        stmts.pop().as_deref(),
        Some("::core::result::Result::Ok(())"),
        "every registration refused is the caller's: it ends Ok"
    );
    stmts
}

#[test]
fn test_register_registers_every_component_once_by_its_kind() {
    assert_eq!(
        registrations(&buyin_register()),
        [
            "registry.aggregate::<crate::adapters::WalletState,>(crate::adapters::WalletState::default)?;",
            "registry.process_manager::<crate::adapters::BuyInState,>(crate::adapters::BuyInState::default)?;",
            "registry.policy(crate::adapters::BuyInWalletPolicy::default())?;",
            "registry.policy(crate::adapters::WalletBuyInPolicy::default())?;",
        ]
    );
}

#[test]
fn test_register_of_one_aggregate_registers_it_alone() {
    const SHOP: &str = "shop/v1/shop.proto";
    let source = register(&descriptors(&[SHOP]), &[SHOP], &[]).unwrap();
    assert_eq!(
        registrations(&source),
        ["registry.aggregate::<crate::adapters::CartState>(crate::adapters::CartState::default)?;"]
    );
}

#[test]
fn test_register_leaves_a_remote_domain_to_the_host_and_keeps_its_policies() {
    let source = register(&descriptors(BUYIN), BUYIN, &["buyin"]).unwrap();
    assert_eq!(
        registrations(&source),
        [
            "registry.aggregate::<crate::adapters::WalletState,>(crate::adapters::WalletState::default)?;",
            "registry.policy(crate::adapters::BuyInWalletPolicy::default())?;",
            "registry.policy(crate::adapters::WalletBuyInPolicy::default())?;",
        ]
    );
}

#[test]
fn test_register_leaves_out_a_policy_whose_domain_is_not_in_the_files() {
    // The blackjack table and its policies, without the wallet:
    // TableWalletPolicy has no target here, TablePlayerPolicy and
    // PlayerTablePolicy have the remote player.
    const FILES: &[&str] = &[
        "io/angzarr/examples/v1/table.proto",
        "io/angzarr/examples/v1/player.proto",
        "io/angzarr/examples/v1/policies.proto",
    ];
    let source = register(&descriptors(BLACKJACK), FILES, &["player"]).unwrap();
    assert_eq!(
        registrations(&source),
        [
            "registry.aggregate::<crate::adapters::TableState>(crate::adapters::TableState::default)?;",
            "registry.policy(crate::adapters::TablePlayerPolicy::default())?;",
            "registry.policy(crate::adapters::PlayerTablePolicy::default())?;",
        ]
    );
}

/// rustc with warnings denied on `source` as a library, `::angzarr_embedded`
/// a stub holding only the `Registry` and `Misconfigured` the signature
/// names: the
/// diagnostics, if rustc refused it.
fn compile_denying_warnings(source: &str) -> Result<(), String> {
    use std::process::Command;
    let dir = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-register-test"));
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    let rustc = |args: &[&str]| {
        Command::new(&rustc)
            .current_dir(&dir)
            .args(["--edition=2021", "--crate-type=lib", "-Dwarnings"])
            .args(args)
            .output()
            .expect("rustc on PATH")
    };
    std::fs::write(
        dir.join("stub.rs"),
        "pub struct Registry; pub struct Misconfigured;",
    )
    .unwrap();
    let stub = rustc(&["--crate-name=angzarr_embedded", "stub.rs"]);
    assert!(
        stub.status.success(),
        "{}",
        String::from_utf8_lossy(&stub.stderr)
    );
    std::fs::write(dir.join("register.rs"), source).unwrap();
    let out = rustc(&[
        "--crate-name=generated",
        "--extern=angzarr_embedded=libangzarr_embedded.rlib",
        "register.rs",
    ]);
    if out.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).into_owned())
    }
}

#[test]
fn test_register_with_every_domain_remote_compiles_with_warnings_denied() {
    // The one domain, carts, is remote: nothing is registered in-process.
    const SHOP: &str = "shop/v1/shop.proto";
    let source = register(&descriptors(&[SHOP]), &[SHOP], &["carts"]).unwrap();
    if let Err(diagnostics) = compile_denying_warnings(&source) {
        panic!("{source}\n{diagnostics}");
    }
}
