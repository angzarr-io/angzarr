//! A domain served by a child process, as a hand-written Rust policy names
//! it: its stand-in, generated from its proto (`stand_ins`), its command
//! set and its event set. Blackjack's table crate hosts the table and its
//! two policies in process and the Python player as a child: the player's
//! stand-in is what those policies take as their target and source.

mod common;

use angzarr_codegen::stand_ins;
use angzarr_test_dir::TestDir;
use common::*;

const PLAYER: &str = "io/angzarr/examples/v1/player.proto";

/// Every impl of `ty` in `source`, as `(trait, body)` without spacing.
fn impls(source: &str, ty: &str) -> Vec<(String, String)> {
    let file = syn::parse_file(source).unwrap_or_else(|e| panic!("{e}:\n{source}"));
    file.items
        .iter()
        .filter_map(|i| match i {
            syn::Item::Impl(imp) if text(&imp.self_ty) == ty => Some((
                imp.trait_.as_ref().map(|t| text(&t.0)).unwrap_or_default(),
                imp.items.iter().map(text).collect(),
            )),
            _ => None,
        })
        .collect()
}

#[test]
fn test_a_stand_in_is_its_domains_command_and_event_sets_from_its_proto() {
    let source = stand_ins(&descriptors(BLACKJACK), &[PLAYER]).unwrap();
    let found = impls(&source, "PlayerState");
    let traits: Vec<&str> = found.iter().map(|(t, _)| t.as_str()).collect();
    assert_eq!(
        traits,
        [
            "CommandTarget",
            "Decides<DecideMove>",
            "EventSource",
            "Ingests<RecordMoveRefused>"
        ],
        "{source}"
    );
    let body = |name: &str| &found.iter().find(|(t, _)| t == name).unwrap().1;
    assert_eq!(
        body("CommandTarget"),
        "constDOMAIN:&'staticstr=\"player\";fncommand_types()->Vec<String>{vec![\"io.angzarr.examples.v1.DecideMove\".to_string()]}"
    );
    assert_eq!(
        body("EventSource"),
        "constDOMAIN:&'staticstr=\"player\";fnevent_types()->Vec<String>{vec![\"io.angzarr.examples.v1.MoveDecided\".to_string(),\"io.angzarr.examples.v1.MoveRefused\".to_string()]}"
    );
    assert!(
        source.starts_with(&format!("// {}", angzarr_codegen::LICENCE_HEADER)),
        "{source}"
    );
}

/// Each aggregate and process manager of the files gets one, its facts
/// as `Ingests`; a policy or a projector gets none.
#[test]
fn test_every_domain_of_the_files_gets_a_stand_in_and_nothing_else_does() {
    let source = stand_ins(&descriptors(BUYIN), BUYIN).unwrap();
    let buy_in: Vec<String> = impls(&source, "BuyInState")
        .into_iter()
        .map(|(t, _)| t)
        .collect();
    assert_eq!(
        buy_in,
        [
            "CommandTarget",
            "Decides<StartBuyIn>",
            "Decides<RecordHeld>",
            "EventSource",
            "Ingests<HoldLost>"
        ]
    );
    assert!(!impls(&source, "WalletState").is_empty(), "{source}");
    for policy in ["BuyInWalletPolicy", "WalletBuyInPolicy"] {
        assert!(impls(&source, policy).is_empty(), "{policy}:\n{source}");
        assert!(!source.contains(&format!("struct {policy}")), "{source}");
    }
}

/// `app` (a crate root naming `domain` and `stand_ins`) compiles, warnings
/// denied, against the markup's traits (a stub of angzarr-embedded's, their
/// shapes as its `markup`'s).
fn assert_compiles(app: &str) {
    use std::process::Command;
    let dir = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-stand-ins"));
    std::fs::write(
        dir.join("stub.rs"),
        "pub mod markup {\n\
         pub trait DomainMessage: Clone + Send + Sync + Sized + 'static {}\n\
         pub trait CommandTarget: 'static { const DOMAIN: &'static str; fn command_types() -> Vec<String>; }\n\
         pub trait EventSource: 'static { const DOMAIN: &'static str; fn event_types() -> Vec<String>; }\n\
         pub trait Decides<C: DomainMessage>: CommandTarget {}\n\
         pub trait Ingests<F: DomainMessage>: EventSource {}\n\
         }\n",
    )
    .unwrap();
    std::fs::write(dir.join("app.rs"), app).unwrap();
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    let run = |args: &[&str]| {
        Command::new(&rustc)
            .current_dir(&dir)
            .args(["--edition=2021", "--crate-type=lib", "-Dwarnings"])
            .args(args)
            .output()
            .expect("rustc on PATH")
    };
    let stub = run(&["--crate-name=angzarr_embedded", "stub.rs"]);
    assert!(
        stub.status.success(),
        "{}",
        String::from_utf8_lossy(&stub.stderr)
    );
    let built = run(&[
        "--crate-name=app",
        "--extern=angzarr_embedded=libangzarr_embedded.rlib",
        "app.rs",
    ]);
    assert!(
        built.status.success(),
        "{app}\n{}",
        String::from_utf8_lossy(&built.stderr)
    );
}

/// The stand-in compiles and a policy's bounds take it: its command set
/// as a target, its event set as a source.
#[test]
fn test_a_stand_in_compiles_and_is_a_policys_target_and_source() {
    let source = stand_ins(&descriptors(BLACKJACK), &[PLAYER]).unwrap();
    assert_compiles(&format!(
        "pub mod domain {{\n\
         #[derive(Clone)] pub struct DecideMove;\n\
         impl ::angzarr_embedded::markup::DomainMessage for DecideMove {{}}\n\
         #[derive(Clone)] pub struct RecordMoveRefused;\n\
         impl ::angzarr_embedded::markup::DomainMessage for RecordMoveRefused {{}}\n\
         }}\n\
         pub mod stand_ins {{\n{source}\n}}\n\
         fn target<T: ::angzarr_embedded::markup::Decides<domain::DecideMove>>() -> &'static str {{\n\
         <T as ::angzarr_embedded::markup::CommandTarget>::DOMAIN\n}}\n\
         fn source<T: ::angzarr_embedded::markup::EventSource>() -> Vec<String> {{ T::event_types() }}\n\
         pub fn check() -> (&'static str, Vec<String>) {{\n\
         (target::<stand_ins::PlayerState>(), source::<stand_ins::PlayerState>())\n}}\n"
    ));
}

/// A stand-in takes a policy's fact (angzarr decision 14.69): the bound
/// `Commands::fact` puts on its target, `Ingests` of the fact, whose
/// supertrait is the event set.
#[test]
fn test_a_stand_in_takes_a_policys_fact() {
    let source = stand_ins(&descriptors(BUYIN), BUYIN).unwrap();
    let messages = [
        "StartBuyIn",
        "RecordHeld",
        "HoldLost",
        "Deposit",
        "Hold",
        "InsufficientFunds",
        "RecordBuyInRefused",
    ];
    let domain: String = messages
        .iter()
        .map(|m| {
            format!(
                "#[derive(Clone)] pub struct {m};\n\
                 impl ::angzarr_embedded::markup::DomainMessage for {m} {{}}\n"
            )
        })
        .collect();
    assert_compiles(&format!(
        "#![allow(dead_code)]\n\
         pub mod domain {{\n{domain}}}\n\
         pub mod stand_ins {{\n{source}\n}}\n\
         fn fact_target<T: ::angzarr_embedded::markup::Ingests<domain::HoldLost>>() -> &'static str {{\n\
         <T as ::angzarr_embedded::markup::EventSource>::DOMAIN\n}}\n\
         pub fn check() -> &'static str {{ fact_target::<stand_ins::BuyInState>() }}\n"
    ));
}
