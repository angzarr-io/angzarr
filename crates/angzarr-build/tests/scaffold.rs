//! The file side of the scaffold and of the generated layer: the scaffold
//! (each component's implementation) goes into the user's `src` once and
//! is never overwritten, `Existing::Complete` (`--missing`) adds only its
//! absent required methods, and what it lacks is reported; a generated
//! path holding a file without the generated marker is refused, never
//! overwritten; an orphaned generated file under `_gen` is deleted; and the
//! adapters and `register` go into the output directory, regenerated every
//! build.

use std::path::Path;

use angzarr_build::{
    BuildError, Existing, Notes, ScaffoldConfig, Scaffolded, adapters_into, host_scaffold,
    markup_domain_into, register_into, scaffold_into,
};
use angzarr_codegen::{CodegenError, UseError};

use angzarr_test_dir::TestDir;

const BUYIN: &[&str] = &[
    "buyin/v1/wallet.proto",
    "buyin/v1/buy_in.proto",
    "buyin/v1/policies.proto",
];
const FILES: &[&str] = &[
    "buy_in_state_impl.rs",
    "buy_in_wallet_policy_impl.rs",
    "wallet_buy_in_policy_impl.rs",
    "wallet_state_impl.rs",
];

fn fixtures() -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../angzarr-codegen/tests/fixtures")
        .display()
        .to_string()
}

/// A fresh, empty directory.
fn empty_dir() -> TestDir {
    TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-scaffold-test"))
}

fn scaffold(src: &Path, existing: Existing) -> Vec<Scaffolded> {
    let mut done = scaffold_into(src, BUYIN, &[&fixtures()], existing).unwrap();
    done.sort_by_key(|d| format!("{d:?}"));
    done
}

#[test]
fn test_scaffold_into_writes_each_absent_file() {
    let src = empty_dir();
    let created: Vec<Scaffolded> = FILES
        .iter()
        .map(|f| Scaffolded::Created(src.join(f)))
        .collect();
    assert_eq!(scaffold(&src, Existing::Keep), created);
    for file in FILES {
        let source = std::fs::read_to_string(src.join(file)).expect(file);
        syn::parse_file(&source).expect("the scaffold parses");
    }
}

#[test]
fn test_scaffold_into_never_overwrites_a_users_file_and_reports_what_it_lacks() {
    let src = empty_dir();
    scaffold(&src, Existing::Keep);
    let mine = "//! The user's wallet.\n\nimpl WalletStateInterface for WalletStateImpl {}\n";
    std::fs::write(src.join("wallet_state_impl.rs"), mine).unwrap();
    let done = scaffold(&src, Existing::Keep);
    let lacks = done
        .iter()
        .find_map(|d| match d {
            Scaffolded::Kept { path, notes } if path.ends_with("wallet_state_impl.rs") => {
                Some(notes.lacks.clone())
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("{done:?}"));
    assert_eq!(
        lacks,
        [
            "deposit",
            "hold",
            "record_buy_in_refused",
            "apply_buy_in_refusal_recorded",
            "apply_deposited",
            "apply_held"
        ]
    );
    assert_eq!(
        std::fs::read_to_string(src.join("wallet_state_impl.rs")).unwrap(),
        mine
    );
}

/// The interface an earlier scaffold wrote beside the implementation is
/// the domain layer's now: reported where it is, never deleted.
#[test]
fn test_scaffold_into_reports_an_earlier_interface_file_as_obsolete() {
    let src = empty_dir();
    scaffold(&src, Existing::Keep);
    let old = "//! The wallet's interface, as an earlier scaffold wrote it.\n";
    std::fs::write(src.join("wallet_state.rs"), old).unwrap();
    let done = scaffold(&src, Existing::Keep);
    assert!(
        done.contains(&Scaffolded::Obsolete(src.join("wallet_state.rs"))),
        "{done:?}"
    );
    assert_eq!(
        std::fs::read_to_string(src.join("wallet_state.rs")).unwrap(),
        old
    );
}

/// A policy's refusal callback is required (decisions 14.89, 14.99): a kept file
/// lacking one reports it, and `--missing` adds it, as any required method.
#[test]
fn test_scaffold_into_adds_a_missing_refused_method_to_an_existing_file() {
    let src = empty_dir();
    scaffold(&src, Existing::Keep);
    let path = src.join("buy_in_wallet_policy_impl.rs");
    let fresh = std::fs::read_to_string(&path).unwrap();
    let start = fresh.find("    fn refused(").unwrap();
    let end = start + fresh[start..].find("    }\n").unwrap() + "    }\n".len();
    let mine = format!("{}{}", &fresh[..start], &fresh[end..]);
    std::fs::write(&path, &mine).unwrap();
    let kept = scaffold(&src, Existing::Keep);
    assert!(
        kept.contains(&Scaffolded::Kept {
            path: path.clone(),
            notes: Notes {
                lacks: vec!["refused".to_string()],
                ..Notes::default()
            },
        }),
        "{kept:?}"
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), mine);
    let done = scaffold(&src, Existing::Complete);
    assert!(
        done.contains(&Scaffolded::Completed {
            path: path.clone(),
            notes: Notes::default(),
        }),
        "{done:?}"
    );
    let after = std::fs::read_to_string(&path).unwrap();
    assert!(after.contains("fn refused("), "{after}");
}

#[test]
fn test_scaffold_into_with_complete_refuses_an_input_the_proto_contradicts() {
    let src = empty_dir();
    scaffold(&src, Existing::Keep);
    let path = src.join("buy_in_state_impl.rs");
    // The user took HoldLost, a fact, as a command.
    let mine = std::fs::read_to_string(&path)
        .unwrap()
        .replace("_fact: &HoldLost", "_cmd: &HoldLost");
    std::fs::write(&path, &mine).unwrap();
    let error = scaffold_into(&src, BUYIN, &[&fixtures()], Existing::Complete)
        .expect_err("a mismatched marker");
    assert!(
        matches!(
            &error,
            BuildError::Codegen(CodegenError::Use(UseError::InputMismatch { message, .. }))
                if message == "buyin.v1.HoldLost"
        ),
        "{error:?}"
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), mine);
}

#[test]
fn test_register_into_writes_the_generated_register() {
    let out = empty_dir();
    register_into(&out, BUYIN, &[&fixtures()], &[]).unwrap();
    let source = std::fs::read_to_string(out.join("angzarr-register.rs")).expect("register");
    assert_eq!(
        source,
        include_str!("../../angzarr-codegen/tests/golden/buyin.register.rs")
    );
}

#[test]
fn test_markup_domain_into_writes_the_markup_domain_layer_by_package() {
    let out = empty_dir();
    markup_domain_into(
        &out,
        BUYIN,
        &[&fixtures()],
        &[("buyin.v1", "::buyin_proto::v1")],
    )
    .unwrap();
    let source = std::fs::read_to_string(out.join("buyin.v1.domain.rs")).expect("domain file");
    assert!(
        source.contains("::angzarr_embedded::markup::DomainMessage"),
        "{source}"
    );
}

#[test]
fn test_adapters_into_writes_the_generated_adapters() {
    let out = empty_dir();
    adapters_into(&out, BUYIN, &[&fixtures()], &[]).unwrap();
    let source = std::fs::read_to_string(out.join("angzarr-adapters.rs")).expect("adapters");
    // Under the licence header's line (owner ruling 2026-10-31).
    let (header, source) = source.split_once('\n').unwrap();
    assert!(
        header.starts_with("// Generated by angzarr-cli"),
        "{header}"
    );
    assert!(
        source.starts_with("// @generated by angzarr-codegen"),
        "{source}"
    );
    assert!(
        source.contains("pub struct WalletState(pub crate::wallet_state_impl::WalletStateImpl);"),
        "{source}"
    );
    assert!(source.contains("pub mod adapters {"), "{source}");
}

#[test]
fn test_scaffold_into_rescaffolded_leaves_every_users_file_byte_identical() {
    // A scaffold run after the first, with --missing, over files a user has
    // written in: none changes, because nothing is absent.
    let src = empty_dir();
    scaffold(&src, Existing::Keep);
    for file in FILES {
        let path = src.join(file);
        let mine = std::fs::read_to_string(&path)
            .unwrap()
            .replace("todo!()", "todo!(\"mine\")");
        std::fs::write(&path, mine).unwrap();
    }
    let before: Vec<String> = FILES
        .iter()
        .map(|f| std::fs::read_to_string(src.join(f)).unwrap())
        .collect();
    let done = scaffold(&src, Existing::Complete);
    assert!(
        done.iter().all(|d| matches!(d, Scaffolded::Kept { .. })),
        "{done:?}"
    );
    let after: Vec<String> = FILES
        .iter()
        .map(|f| std::fs::read_to_string(src.join(f)).unwrap())
        .collect();
    assert_eq!(before, after);
}

// ------------------------------------------------------------- generated

/// The spec's protos: the angzarr-project submodule's `proto`.
fn spec() -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../angzarr-project/proto")
        .display()
        .to_string()
}

/// The player's Python project scaffolded into `root` (no buf, no lock).
fn player(root: &Path) -> Result<Vec<Scaffolded>, BuildError> {
    let app = angzarr_build::PythonApp {
        domain: "player".to_string(),
        protos: angzarr_build::ProtoInput::Directory(spec()),
        rules_version: "0.1.0".to_string(),
    };
    angzarr_build::scaffold_python_into(
        root,
        &["io/angzarr/examples/v1/player.proto"],
        &[&spec()],
        &app,
        Existing::Keep,
    )
}

const GEN: &str = "src/angzarr_player/_gen";

/// A generated path holding a file without the generated marker (a hand
/// edit, or a hand-written file in its place) is refused, never
/// overwritten, and the run writes nothing (item S, H1).
#[test]
fn test_a_generated_file_without_the_marker_is_refused_and_nothing_written() {
    let root = empty_dir();
    player(&root).unwrap();
    let domain = root.join(GEN).join("domain.py");
    let mine = "\"\"\"My own domain, written in place of the generated one.\"\"\"\n";
    std::fs::write(&domain, mine).unwrap();
    let logic = root.join(GEN).join("logic.py");
    std::fs::remove_file(&logic).unwrap();
    let error = player(&root).expect_err("an unmarked generated file");
    assert_eq!(
        error,
        BuildError::GeneratedUnmarked {
            path: domain.display().to_string()
        }
    );
    assert!(error.remedy().is_some());
    assert_eq!(std::fs::read_to_string(&domain).unwrap(), mine);
    assert!(!logic.exists(), "nothing is written when one is refused");
}

/// A marked generated file that differs is the CLI's, and rewritten.
#[test]
fn test_a_marked_generated_file_is_regenerated() {
    let root = empty_dir();
    player(&root).unwrap();
    let main = root.join(GEN).join("main.py");
    let fresh = std::fs::read_to_string(&main).unwrap();
    std::fs::write(&main, format!("{fresh}# edited\n")).unwrap();
    let done = player(&root).unwrap();
    assert!(
        done.contains(&Scaffolded::Regenerated(main.clone())),
        "{done:?}"
    );
    assert_eq!(std::fs::read_to_string(&main).unwrap(), fresh);
}

/// A file at a user's path that carries the generated marker is an earlier
/// CLI's generated file (`__main__.py` was one), rewritten as the user's;
/// a user's file without it is never touched.
#[test]
fn test_an_earlier_clis_generated_file_at_a_users_path_becomes_the_users() {
    let root = empty_dir();
    player(&root).unwrap();
    let dunder = root.join("src/angzarr_player/__main__.py");
    let fresh = std::fs::read_to_string(&dunder).unwrap();
    std::fs::write(
        &dunder,
        "# @generated by angzarr-codegen. Do not edit.\nfrom angzarr_player.main import main\n\nmain()\n",
    )
    .unwrap();
    let done = player(&root).unwrap();
    assert!(
        done.contains(&Scaffolded::Regenerated(dunder.clone())),
        "{done:?}"
    );
    assert_eq!(std::fs::read_to_string(&dunder).unwrap(), fresh);
}

/// Under `_gen`, a marked file this run does not write is an orphan and is
/// deleted; buf's message types (written after) and anything without the
/// marker are kept (item S Q9).
#[test]
fn test_an_orphaned_generated_file_under_gen_is_deleted() {
    let root = empty_dir();
    player(&root).unwrap();
    let v1 = root.join(GEN).join("io/angzarr/examples/v1");
    let orphan = v1.join("gone_angzarr.py");
    let marked = std::fs::read_to_string(root.join(GEN).join("__init__.py")).unwrap();
    std::fs::write(&orphan, &marked).unwrap();
    let pb2 = v1.join("player_pb2.py");
    std::fs::write(&pb2, &marked).unwrap();
    let notes = v1.join("NOTES.txt");
    std::fs::write(&notes, "mine\n").unwrap();
    // Outside `_gen`, a marked file is never swept.
    let stray = root.join("src/angzarr_player/stray.py");
    std::fs::write(&stray, &marked).unwrap();
    let done = player(&root).unwrap();
    assert!(stray.exists(), "only a _gen is swept");
    assert!(
        done.contains(&Scaffolded::Deleted(orphan.clone())),
        "{done:?}"
    );
    assert!(!orphan.exists());
    assert!(pb2.exists(), "buf's own is not an orphan");
    assert!(notes.exists(), "an unmarked file is never deleted");
}

/// A host config gives the scaffold's configuration: the components its
/// entries' `scaffold.snapshots` set; a config that is not there is
/// refused, naming it.
#[test]
fn test_host_scaffold_reads_the_apps_host_config() {
    let dir = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-host-scaffold"));
    let config = dir.join("angzarr.yaml");
    std::fs::write(&config, WALLET_SNAPSHOTS).unwrap();
    assert_eq!(
        host_scaffold(&config).unwrap(),
        ScaffoldConfig {
            snapshots: vec!["wallet".to_string()],
        }
    );
    let absent = dir.join("absent").join("angzarr.yaml");
    assert!(
        matches!(host_scaffold(&absent), Err(BuildError::Io { .. })),
        "{:?}",
        host_scaffold(&absent)
    );
}

/// A host config taking the wallet's snapshots.
const WALLET_SNAPSHOTS: &str = "schema_version: 4\ncomponent:\n  \
    - {name: wallet, kind: domain, language: rust, scaffold: {snapshots: true}}\n";

/// `scaffold_into`, what `angzarr scaffold rust` calls, reads the host
/// config beside the source directory, in the crate's root: the wallet's
/// implementation takes snapshots there, and with no config beside it,
/// none does.
#[test]
fn test_scaffold_into_writes_as_the_host_config_beside_src_says() {
    let dir = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-scaffold-beside"));
    let configured = dir.join("configured");
    std::fs::create_dir_all(configured.join("src")).unwrap();
    std::fs::write(configured.join("angzarr.yaml"), WALLET_SNAPSHOTS).unwrap();
    let bare = dir.join("bare");
    std::fs::create_dir_all(bare.join("src")).unwrap();
    for (root, snapshots) in [(&configured, true), (&bare, false)] {
        let src = root.join("src");
        scaffold_into(&src, BUYIN, &[&fixtures()], Existing::Keep).unwrap();
        let wallet = std::fs::read_to_string(src.join("wallet_state_impl.rs")).unwrap();
        assert_eq!(wallet.contains("fn snapshot(&self)"), snapshots, "{wallet}");
        let buy_in = std::fs::read_to_string(src.join("buy_in_state_impl.rs")).unwrap();
        assert!(!buy_in.contains("fn snapshot(&self)"), "{buy_in}");
    }
}
