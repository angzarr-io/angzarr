//! Versioned loading: chain checks, every load error, a v0 to v3 migration
//! with the helpers, pass-through, write-back and determinism.

use crate::versioned::{
    map_seq, move_key, remove_key, rename_key, set_default, version_at, version_of, BadVersion,
    ChainError, ChainProblem, Fixture, LoadProblem, Loader, MigrationError, Migrations,
    DEFAULT_KEY,
};
use crate::{parse, write, Value};
use angzarr_diagnostic::{code, Arg, Diagnostic, Report};

fn ok(_: &mut Value) -> Result<(), MigrationError> {
    Ok(())
}

fn chain_problem(m: Migrations) -> ChainProblem {
    match m.build() {
        Err(ChainError { kind, problem }) => {
            assert_eq!(kind, "toy");
            problem
        }
        Ok(_) => panic!("the chain should not build"),
    }
}

#[test]
fn chains_must_be_whole() {
    assert!(Migrations::new("toy", 0).build().is_ok());
    assert!(Migrations::new("toy", 3).build().is_ok());
    assert!(Migrations::new("toy", 3)
        .step(1, ok)
        .step(2, ok)
        .build()
        .is_ok());
    assert!(Migrations::new("toy", 3)
        .step(2, ok)
        .step(0, ok)
        .step(1, ok)
        .build()
        .is_ok());

    assert_eq!(
        chain_problem(Migrations::new("toy", 3).step(0, ok).step(2, ok)),
        ChainProblem::Gap { from: 1 }
    );
    assert_eq!(
        chain_problem(Migrations::new("toy", 3).step(0, ok).step(1, ok)),
        ChainProblem::Gap { from: 2 }
    );
    assert_eq!(
        chain_problem(
            Migrations::new("toy", 3)
                .step(1, ok)
                .step(1, ok)
                .step(2, ok)
        ),
        ChainProblem::Duplicate { from: 1 }
    );
    assert_eq!(
        chain_problem(Migrations::new("toy", 2).step(1, ok).step(2, ok)),
        ChainProblem::BeyondCurrent {
            from: 2,
            current: 2
        }
    );
    let e = Migrations::new("toy", 3)
        .step(0, ok)
        .step(2, ok)
        .build()
        .unwrap_err();
    assert_eq!(
        e.to_string(),
        concat!(code!("chain-gap"), " kind=toy from=1 to=2")
    );
}

// The toy schema's history:
//   v0: `name`, `hp`, `units: [{kind, hp}]`
//   v1: `hp` renamed `health`, in the root and in each unit
//   v2: `speed` defaults to 1; `legacy` dropped
//   v3: `name` moves under `meta`, as `meta.title`

fn v0_to_v1(doc: &mut Value) -> Result<(), MigrationError> {
    rename_key(doc, "hp", "health")?;
    map_seq(doc, "units", |u| rename_key(u, "hp", "health"))
}

fn v1_to_v2(doc: &mut Value) -> Result<(), MigrationError> {
    set_default(doc, "speed", Value::scalar("1"))?;
    remove_key(doc, "legacy")?;
    Ok(())
}

fn v2_to_v3(doc: &mut Value) -> Result<(), MigrationError> {
    move_key(doc, &["name"], &["meta", "title"])
}

fn toy() -> Loader {
    Migrations::new("toy", 3)
        .step(2, v2_to_v3)
        .step(0, v0_to_v1)
        .step(1, v1_to_v2)
        .build()
        .unwrap()
}

const V0: &str = "# an old file\nschema_version: 0\nname: Ogre\nhp: 12\nlegacy: yes\nunits:\n  - {kind: club, hp: 3}\n  - {kind: rock, hp: 1}\n";
const V3: &str = "schema_version: 3\nhealth: 12\nunits:\n  - {kind: club, health: 3}\n  - {kind: rock, health: 1}\nspeed: 1\nmeta:\n  title: Ogre\n";

#[test]
fn migrates_v0_to_v3_with_the_helpers() {
    let got = toy().load(Some("ogre.yaml"), V0).unwrap();
    assert_eq!(got, parse(V3).unwrap());
    // Keys keep their places; new ones go last.
    let keys: Vec<&str> = got
        .as_map()
        .unwrap()
        .iter()
        .map(|(k, _)| k.as_str())
        .collect();
    assert_eq!(keys, ["schema_version", "health", "units", "speed", "meta"]);
    // Each intermediate version migrates from where it is.
    let v2 = "schema_version: 2\nname: Ogre\nhealth: 12\nunits:\n  - {kind: club, health: 3}\n  - {kind: rock, health: 1}\nspeed: 1\n";
    assert_eq!(toy().load(None, v2).unwrap(), parse(V3).unwrap());
}

#[test]
fn a_current_document_passes_through_untouched() {
    let doc = parse(V3).unwrap();
    assert_eq!(toy().migrate(None, doc.clone()).unwrap(), doc);
    assert_eq!(toy().upgrade(None, V3, 1).unwrap(), None);
    // No steps at all: the identity on current documents.
    let none = Migrations::new("toy", 1).build().unwrap();
    let text = "schema_version: 1\nb: 2\na: [1, 2]\n";
    assert_eq!(none.load(None, text).unwrap(), parse(text).unwrap());
}

#[test]
fn writes_back_the_migrated_tree() {
    let written = toy()
        .upgrade(Some("ogre.yaml"), V0, 1)
        .unwrap()
        .expect("an old file upgrades");
    assert_eq!(written, write(&parse(V3).unwrap(), 1));
    // The written file reads back as the current version, and is current.
    assert_eq!(parse(&written).unwrap(), parse(V3).unwrap());
    assert_eq!(toy().upgrade(None, &written, 1).unwrap(), None);
    assert_eq!(toy().load(None, &written).unwrap(), parse(V3).unwrap());
}

#[test]
fn the_same_input_gives_the_same_bytes() {
    let first = toy().upgrade(None, V0, 2).unwrap().unwrap();
    for _ in 0..10 {
        assert_eq!(
            toy().upgrade(None, V0, 2).unwrap().unwrap().as_bytes(),
            first.as_bytes()
        );
    }
}

fn problem(text: &str) -> LoadProblem {
    let e = toy().load(Some("bad.yaml"), text).unwrap_err();
    assert_eq!(e.kind, "toy");
    assert_eq!(e.source.as_deref(), Some("bad.yaml"));
    e.problem
}

/// A caller's own error, with the caller's own code.
fn missing() -> Report {
    Report::new("toy-missing-key").arg("key", "missing")
}

/// The problem with the version stamp `text` gives.
fn bad_version(text: &str) -> BadVersion {
    match problem(text) {
        LoadProblem::BadVersion { key, problem } => {
            assert_eq!(key, DEFAULT_KEY);
            problem
        }
        other => panic!("{text:?} should have a bad stamp, not {other:?}"),
    }
}

#[test]
fn a_document_that_is_no_mapping_fails() {
    assert!(matches!(
        problem("schema_version: 3\na: [1, 2\n"),
        LoadProblem::Yaml(_)
    ));
    assert_eq!(problem("- schema_version: 3\n"), LoadProblem::NotAMapping);
    // An empty file is version 0, not a corrupt one (the config-versioning
    // rule): it is no failure, and every step runs on it.
    assert_eq!(
        toy().load(None, "").unwrap(),
        parse("schema_version: 3\nspeed: 1\n").unwrap()
    );
}

#[test]
fn a_stamp_that_is_no_version_fails() {
    for text in ["two", "-1", "1.0"] {
        assert_eq!(
            bad_version(&format!("schema_version: {text}\n")),
            BadVersion::NotAnInteger { text: text.into() }
        );
    }
    assert_eq!(bad_version("schema_version:\n"), BadVersion::Empty);
    assert_eq!(bad_version("schema_version: [1]\n"), BadVersion::Sequence);
    assert!(matches!(
        bad_version("schema_version: 99999999999\n"),
        BadVersion::TooLarge { .. }
    ));
}

#[test]
fn a_version_the_chain_does_not_reach_fails() {
    assert_eq!(
        problem("schema_version: 4\n"),
        LoadProblem::TooNew {
            found: 4,
            current: 3
        }
    );
    let trimmed = Migrations::new("toy", 3)
        .step(1, v1_to_v2)
        .step(2, v2_to_v3)
        .build()
        .unwrap();
    let e = trimmed.load(Some("ancient.yaml"), V0).unwrap_err();
    assert_eq!(
        e.problem,
        LoadProblem::TooOld {
            found: 0,
            oldest: 1
        }
    );
    // The error messages name the problem.
    assert_eq!(
        toy()
            .load(Some("new.yaml"), "schema_version: 7\n")
            .unwrap_err()
            .to_string(),
        concat!(
            code!("load-failed-at"),
            " kind=toy source=new.yaml \
         problem=",
            code!("too-new"),
            " problem.found=7 problem.current=3"
        )
    );
}

/// A step that fails: which step, and what went wrong.
#[test]
fn a_failing_step_names_itself_and_its_error() {
    let e = toy()
        .load(Some("bad.yaml"), "schema_version: 0\nhp: 1\nhealth: 2\n")
        .unwrap_err();
    assert_eq!(
        e.problem,
        LoadProblem::Step {
            from: 0,
            error: Box::new(MigrationError::RenameTargetExists {
                from: "hp".into(),
                to: "health".into()
            })
        }
    );
    assert_eq!(
        e.to_string(),
        concat!(
            code!("load-failed-at"),
            " kind=toy source=bad.yaml \
         problem=",
            code!("step-failed"),
            " problem.from=0 problem.to=1 \
         problem.error=",
            code!("rename-target-exists"),
            " problem.error.from=hp \
         problem.error.to=health"
        )
    );
    let e = toy()
        .load(
            None,
            "schema_version: 0\nunits: [{hp: 1}, {hp: 2, health: 2}]\n",
        )
        .unwrap_err();
    assert_eq!(
        e.to_string(),
        concat!(
            code!("load-failed"),
            " kind=toy problem=",
            code!("step-failed"),
            " \
         problem.from=0 problem.to=1 problem.error=",
            code!("map-item"),
            " \
         problem.error.key=units problem.error.index=1 \
         problem.error.error=",
            code!("rename-target-exists"),
            " \
         problem.error.error.from=hp problem.error.error.to=health"
        )
    );
    // A step that breaks the root.
    fn clobber(doc: &mut Value) -> Result<(), MigrationError> {
        *doc = Value::Null;
        Ok(())
    }
    let broken = Migrations::new("toy", 1).step(0, clobber).build().unwrap();
    assert!(matches!(
        broken
            .load(None, "schema_version: 0\n")
            .unwrap_err()
            .problem,
        LoadProblem::Step { from: 0, .. }
    ));
}

/// The decoder's error, the caller's own code, nested as it is.
#[test]
fn a_failing_decoder_fails_with_its_own_error() {
    let e = toy()
        .load_as(Some("ogre.yaml"), V0, |doc| {
            doc.get("missing").cloned().ok_or_else(missing)
        })
        .unwrap_err();
    assert_eq!(e.problem, LoadProblem::Decode(missing()));
    match &e.args()[..] {
        [("kind", Arg::Str(kind)), ("source", Arg::Str(source)), ("problem", Arg::Nested(problem))] =>
        {
            assert_eq!((kind.as_str(), source.as_str()), ("toy", "ogre.yaml"));
            assert_eq!(problem.code(), "toy-missing-key");
            assert!(matches!(&problem.args()[..], [("key", Arg::Str(k))] if k == "missing"));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        e.to_string(),
        concat!(
            code!("load-failed-at"),
            " kind=toy source=ogre.yaml problem=toy-missing-key \
         problem.key=missing"
        )
    );
}

#[test]
fn load_as_decodes_the_current_tree() {
    let health: u32 = toy()
        .load_as(None, V0, |doc| {
            doc.get("health")
                .and_then(Value::as_str)
                .unwrap()
                .parse::<u32>()
                .map_err(|_| missing())
        })
        .unwrap();
    assert_eq!(health, 12);
}

#[test]
fn helpers_handle_the_edges() {
    let mut doc = parse("a: 1\nb: 2\nn:\n").unwrap();
    rename_key(&mut doc, "missing", "c").unwrap();
    // A missing key is nothing to do, even when its new name is taken.
    rename_key(&mut doc, "missing", "b").unwrap();
    rename_key(&mut doc, "a", "a").unwrap();
    assert_eq!(
        rename_key(&mut Value::scalar("x"), "a", "b"),
        Err(MigrationError::RenameNotAMapping { from: "a".into() })
    );
    set_default(&mut doc, "a", Value::scalar("9")).unwrap();
    set_default(&mut doc, "n", Value::scalar("0")).unwrap();
    assert_eq!(doc, parse("a: 1\nb: 2\nn: 0\n").unwrap());
    assert_eq!(remove_key(&mut doc, "b").unwrap(), Some(Value::scalar("2")));
    assert_eq!(remove_key(&mut doc, "b").unwrap(), None);
    // Moves make the mappings they need, and refuse to overwrite.
    move_key(&mut doc, &["a"], &["x", "y", "z"]).unwrap();
    assert_eq!(doc, parse("n: 0\nx: {y: {z: 1}}\n").unwrap());
    move_key(&mut doc, &["x", "y", "z"], &["a"]).unwrap();
    move_key(&mut doc, &["nowhere", "deep"], &["a"]).unwrap();
    assert_eq!(
        move_key(&mut doc, &["n"], &["a"]),
        Err(MigrationError::MoveTargetExists {
            from: "n".into(),
            to: "a".into(),
            key: "a".into()
        })
    );
    let before = doc.clone();
    assert_eq!(
        move_key(&mut doc, &["n"], &["a", "b"]),
        Err(MigrationError::MoveNotAMapping {
            from: "n".into(),
            to: "a.b".into()
        })
    );
    assert_eq!(doc, before, "a failed move changes nothing");
    assert_eq!(
        move_key(&mut doc, &["a", "x"], &["c"]),
        Err(MigrationError::MoveThroughNotAMapping {
            from: "a.x".into(),
            to: "c".into(),
            key: "a".into()
        })
    );
    assert_eq!(
        move_key(&mut doc, &[], &["a"]),
        Err(MigrationError::MoveEmptyPath {
            from: "".into(),
            to: "a".into()
        })
    );
    assert_eq!(
        map_seq(&mut doc, "n", |_| Ok(())),
        Err(MigrationError::MapNotASequence { key: "n".into() })
    );
    map_seq(&mut doc, "missing", |_| {
        Err(MigrationError::RootNotAMapping)
    })
    .unwrap();
}

#[test]
fn fixtures_must_migrate_to_their_expected_trees() {
    let loader = toy();
    loader.assert_fixtures(&[
        Fixture {
            name: "v0.yaml",
            input: V0,
            expected: Some(V3),
        },
        Fixture {
            name: "v3.yaml",
            input: V3,
            expected: None,
        },
    ]);

    let failures = loader
        .check_fixtures(&[
            // At an old version with no expected output: stale.
            Fixture {
                name: "stale.yaml",
                input: V0,
                expected: None,
            },
            // Migrates, but not to what it should.
            Fixture {
                name: "wrong.yaml",
                input: V0,
                expected: Some("schema_version: 3\n"),
            },
            // Expected output not at the current version.
            Fixture {
                name: "old-expected.yaml",
                input: V0,
                expected: Some(V0),
            },
            // Does not load.
            Fixture {
                name: "broken.yaml",
                input: "schema_version: 9\n",
                expected: None,
            },
        ])
        .unwrap_err();
    let named = |n: &str| failures.iter().any(|f| f.contains(n));
    assert!(named("stale.yaml: at version 0"), "{failures:?}");
    assert!(named("wrong.yaml: migrated to"), "{failures:?}");
    assert!(
        named("old-expected.yaml: the expected output must be at the current version"),
        "{failures:?}"
    );
    assert!(
        named(concat!(
            "broken.yaml problem=",
            code!("too-new"),
            " problem.found=9"
        )),
        "{failures:?}"
    );
    assert!(loader.check_fixtures(&[]).is_err());
}

#[test]
#[should_panic(expected = "toy fixtures do not migrate")]
fn assert_fixtures_panics_on_failure() {
    toy().assert_fixtures(&[Fixture {
        name: "v0.yaml",
        input: V0,
        expected: None,
    }]);
}

#[test]
fn a_document_without_a_version_is_version_0() {
    let unstamped = V0.replace("schema_version: 0\n", "");
    assert_eq!(version_of(&crate::parse(&unstamped).unwrap()), Ok(0));
    assert_eq!(
        toy().load(None, &unstamped).unwrap(),
        toy().load(None, V0).unwrap()
    );
    let from_1 = Migrations::new("toy", 3)
        .step(1, v1_to_v2)
        .step(2, v2_to_v3)
        .build()
        .unwrap();
    assert_eq!(
        from_1.load(None, "name: Ogre\n").unwrap_err().problem,
        LoadProblem::TooOld {
            found: 0,
            oldest: 1
        }
    );
}

#[test]
fn a_leading_bom_does_not_hide_the_version() {
    // Editors on Windows save a byte-order mark; a current file must still
    // read as current, not as version 0 with every step re-run.
    let with_bom = format!("\u{feff}{V3}");
    assert_eq!(version_of(&parse(&with_bom).unwrap()), Ok(3));
    assert_eq!(toy().load(None, &with_bom).unwrap(), parse(V3).unwrap());
    assert_eq!(toy().upgrade(None, &with_bom, 2).unwrap(), None);
}

#[test]
fn a_callers_step_raises_its_own_code() {
    fn needs_a_number(doc: &mut Value) -> Result<(), MigrationError> {
        match doc.get("hp").and_then(Value::as_str) {
            Some(hp) if hp.parse::<u32>().is_ok() => Ok(()),
            hp => Err(Report::new("toy-hp-not-a-number")
                .arg("hp", hp.unwrap_or(""))
                .into()),
        }
    }
    let loader = Migrations::new("toy", 1)
        .step(0, needs_a_number)
        .build()
        .unwrap();
    let e = loader
        .load(None, "schema_version: 0\nhp: lots\n")
        .unwrap_err();
    let LoadProblem::Step { from: 0, error } = &e.problem else {
        panic!("{e:?}")
    };
    assert_eq!(error.code(), "toy-hp-not-a-number");
    assert!(matches!(&error.args()[..], [("hp", Arg::Str(hp))] if hp == "lots"));
    assert_eq!(
        e.to_string(),
        concat!(
            code!("load-failed"),
            " kind=toy problem=",
            code!("step-failed"),
            " \
         problem.from=0 problem.to=1 problem.error=toy-hp-not-a-number problem.error.hp=lots"
        )
    );
    assert_eq!(e.source, None);
    assert!(matches!(e.problem, LoadProblem::Step { .. }), "{e:?}");
    assert!(matches!(
        &e.problem.args()[..],
        [
            ("from", Arg::Uint(0)),
            ("to", Arg::Uint(1)),
            ("error", Arg::Nested(_))
        ]
    ));
}

// ------------------------------------------------------- upgrade, keeping

fn rename_hp(doc: &mut Value) -> Result<(), MigrationError> {
    rename_key(doc.get_mut("stats").expect("stats"), "hp", "health")
}

fn hp_loader() -> Loader {
    Migrations::new("toy", 2)
        .step(1, rename_hp)
        .build()
        .unwrap()
}

#[test]
fn upgrade_keeping_rewrites_only_what_the_migration_changed() {
    let old = "\
# A toy.
schema_version: 1   # the format
name: imp    # its name
stats:
  # Hit points.
  hp: 3
  speed: 2   # tiles per turn
";
    let up = hp_loader()
        .upgrade_keeping(Some("toy.yaml"), old, 2)
        .unwrap()
        .unwrap();
    assert!(up.comments_kept);
    assert_eq!(
        up.text,
        "\
# A toy.
schema_version: 2   # the format
name: imp    # its name
stats:
  speed: 2   # tiles per turn
  health: 3
"
    );
    // A renamed key moves to the end of its mapping; the tree is otherwise
    // the migration's.
    let back = parse(&up.text).unwrap();
    assert_eq!(
        back.get("stats").and_then(|st| st.get("health")),
        Some(&Value::scalar("3"))
    );
    assert_eq!(back.get("stats").and_then(|st| st.get("hp")), None);
}

#[test]
fn upgrade_keeping_a_current_document_is_none() {
    assert!(hp_loader()
        .upgrade_keeping(None, "schema_version: 2\nstats: {health: 1}\n", 2)
        .unwrap()
        .is_none());
}

#[test]
fn upgrade_keeping_reports_a_bad_document_like_load() {
    assert!(hp_loader()
        .upgrade_keeping(None, "schema_version: 9\n", 2)
        .is_err());
    assert!(hp_loader().upgrade_keeping(None, "a: &x 1\n", 2).is_err());
}

#[test]
fn a_loader_says_what_it_reads() {
    let l = toy();
    assert_eq!((l.kind(), l.oldest(), l.current()), ("toy", 0, 3));
    let from_2 = Migrations::new("deck", 5)
        .step(2, ok)
        .step(3, ok)
        .step(4, ok)
        .build()
        .unwrap();
    assert_eq!(
        (from_2.kind(), from_2.oldest(), from_2.current()),
        ("deck", 2, 5)
    );
}

#[test]
fn a_version_0_loader_leaves_an_unstamped_document_unstamped() {
    let none = Migrations::new("toy", 0).build().unwrap();
    assert_eq!(none.load(None, "a: 1\n").unwrap(), parse("a: 1\n").unwrap());
}

#[test]
fn a_load_error_gives_its_cause_as_its_source() {
    use std::error::Error as _;
    let source = |text: &str| {
        toy()
            .load(None, text)
            .unwrap_err()
            .source()
            .map(|s| s.to_string())
    };
    let yaml = source("a: [1\n").expect("a parse error has a source");
    assert_eq!(yaml, crate::parse("a: [1\n").unwrap_err().to_string());
    assert_eq!(
        source("schema_version: 0\nhp: 1\nhealth: 2\n").as_deref(),
        Some(concat!(code!("rename-target-exists"), " from=hp to=health"))
    );
    let decode = toy()
        .load_as(None, V0, |doc| {
            doc.get("missing").cloned().ok_or_else(missing)
        })
        .unwrap_err();
    assert_eq!(
        decode.source().map(|s| s.to_string()).as_deref(),
        Some("toy-missing-key key=missing")
    );
    assert!(source("schema_version: 9\n").is_none());
    assert!(source("- a\n").is_none());
}

// ------------------------------------------------------------- the stamp key

#[test]
fn the_stamp_key_is_schema_version_unless_a_kind_names_its_own() {
    assert_eq!(DEFAULT_KEY, "schema_version");
    assert_eq!(toy().key(), "schema_version");
    assert_eq!(toy().legacy_key(), None);
    // Under the default key, `version` is just some other key: the document
    // is version 0.
    let doc = parse("version: 3\nname: Ogre\n").unwrap();
    assert_eq!(version_of(&doc), Ok(0));
    assert_eq!(version_at(&doc, "version"), Ok(3));
    let from_1 = Migrations::new("toy", 3)
        .step(1, v1_to_v2)
        .step(2, v2_to_v3)
        .build()
        .unwrap();
    assert_eq!(
        from_1
            .load(None, "version: 3\nname: Ogre\n")
            .unwrap_err()
            .problem,
        LoadProblem::TooOld {
            found: 0,
            oldest: 1
        }
    );

    // A kind with its own key reads and stamps that key, and the default is
    // just some other key.
    let own = Migrations::new("manifest", 2)
        .key("version")
        .step(0, ok)
        .step(1, ok)
        .build()
        .unwrap();
    assert_eq!(own.key(), "version");
    assert_eq!(
        own.load(None, "version: 1\nschema_version: x\n").unwrap(),
        parse("version: 2\nschema_version: x\n").unwrap()
    );
    assert_eq!(
        own.load(None, "a: 1\n").unwrap(),
        parse("version: 2\na: 1\n").unwrap()
    );
    assert_eq!(
        own.load(None, "version: x\n").unwrap_err().problem,
        LoadProblem::BadVersion {
            key: "version".into(),
            problem: BadVersion::NotAnInteger { text: "x".into() }
        }
    );
    assert_eq!(own.upgrade(None, "version: 2\n", 2).unwrap(), None);
}

/// bosun's manifests and the cli's project files are stamped `version`: a
/// kind declares it as its legacy key, and a pre-step renames it, in place,
/// to the kind's key before the stamp is read (as Go schemaver's kinds
/// rename a key in a step, here before the gate).
fn manifest() -> Loader {
    Migrations::new("manifest", 2)
        .legacy_key("version")
        .step(1, rename_hp_at_root)
        .build()
        .unwrap()
}

fn rename_hp_at_root(doc: &mut Value) -> Result<(), MigrationError> {
    rename_key(doc, "hp", "health")
}

#[test]
fn a_legacy_key_is_renamed_to_the_kinds_key_before_the_stamp_is_read() {
    let l = manifest();
    assert_eq!(
        (l.key(), l.legacy_key()),
        ("schema_version", Some("version"))
    );
    // Renamed in its place, then migrated and stamped under the new key.
    assert_eq!(
        l.load(None, "name: imp\nversion: 1\nhp: 3\n").unwrap(),
        parse("name: imp\nschema_version: 2\nhealth: 3\n").unwrap()
    );
    // A current document under the legacy key loads with the key renamed,
    // and is not current on disk: upgrading rewrites it.
    let current = "version: 2   # format\nhealth: 3\n";
    assert_eq!(
        l.load(None, current).unwrap(),
        parse("schema_version: 2\nhealth: 3\n").unwrap()
    );
    assert_eq!(
        l.upgrade(None, current, 2).unwrap().as_deref(),
        Some("schema_version: 2\nhealth: 3\n")
    );
    let kept = l.upgrade_keeping(None, current, 2).unwrap().unwrap();
    assert_eq!(
        parse(&kept.text).unwrap(),
        parse("schema_version: 2\nhealth: 3\n").unwrap()
    );
    // A document stamped under the kind's key is read as it is, and its
    // `version`, if any, is some other key.
    assert_eq!(
        l.load(None, "schema_version: 2\nhealth: 3\n").unwrap(),
        parse("schema_version: 2\nhealth: 3\n").unwrap()
    );
    assert_eq!(
        l.upgrade(None, "schema_version: 2\nhealth: 3\n", 2)
            .unwrap(),
        None
    );
}

#[test]
fn a_legacy_stamp_is_judged_like_any_stamp_and_both_at_once_is_refused() {
    let l = manifest();
    assert_eq!(
        l.load(None, "version: 3\n").unwrap_err().problem,
        LoadProblem::TooNew {
            found: 3,
            current: 2
        }
    );
    // The bad stamp is named by the key it was written under.
    assert_eq!(
        l.load(None, "version: one\n").unwrap_err().problem,
        LoadProblem::BadVersion {
            key: "version".into(),
            problem: BadVersion::NotAnInteger { text: "one".into() }
        }
    );
    let both = l
        .load(Some("m.yaml"), "version: 1\nschema_version: 1\n")
        .unwrap_err();
    assert_eq!(
        both.problem,
        LoadProblem::LegacyAndStamp {
            key: "schema_version".into(),
            legacy: "version".into()
        }
    );
    assert_eq!(
        both.to_string(),
        concat!(
            code!("load-failed-at"),
            " kind=manifest source=m.yaml \
         problem=",
            code!("legacy-and-stamp"),
            " problem.key=schema_version \
         problem.legacy=version"
        )
    );
    assert!(l
        .upgrade(None, "version: 1\nschema_version: 1\n", 2)
        .is_err());
    // A legacy key the same as the kind's key is no legacy key.
    let same = Migrations::new("toy", 1)
        .key("version")
        .legacy_key("version")
        .build()
        .unwrap();
    assert_eq!(same.legacy_key(), None);
    assert!(same.load(None, "version: 1\n").is_ok());
}
