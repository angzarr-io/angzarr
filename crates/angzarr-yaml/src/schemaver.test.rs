//! Go ctxloom's `internal/shared/schemaver` tests, ported where they apply:
//! the reference contract this loader is the Rust twin of. Each test names
//! the Go test it ports. Where the two differ by design the test says so:
//!
//! - Go refuses a kind declared at generation 0 and a chain registered out
//!   of order; here a kind may be at version 0, and steps are sorted.
//! - Go passes a document that is not a well-formed mapping through to the
//!   kind's own decode, which refuses it; here the loader refuses it itself,
//!   as the parse or root error it is (never a version error).
//! - Go's `---` bare document is refused here by the parser (below), so it
//!   is not among the generation-0 inputs.
//! - Multi-document streams are refused here by the parser (`---`), where Go
//!   calls them unreadable.

use super::*;
use crate::{parse, ParseProblem, Value};

/// Go's fakeStep: sets `key` to `yes`, or edits nothing (a marker).
fn added_a(doc: &mut Value) -> Result<(), MigrationError> {
    *doc.entry("added_a") = Value::scalar("yes");
    Ok(())
}

fn added_b(doc: &mut Value) -> Result<(), MigrationError> {
    *doc.entry("added_b") = Value::scalar("yes");
    Ok(())
}

fn marker(_: &mut Value) -> Result<(), MigrationError> {
    Ok(())
}

/// Go's `withSteps`: generations 1 to 3, two steps.
fn with_steps() -> Loader {
    Migrations::new("widget", 3)
        .step(1, added_a)
        .step(2, added_b)
        .build()
        .unwrap()
}

/// Go's `zeroSteps`: a kind with no migrations registered.
fn zero_steps() -> Loader {
    Migrations::new("gadget", 2).build().unwrap()
}

fn doc(version: u32, rest: &str) -> String {
    format!("{DEFAULT_KEY}: {version}\n{rest}")
}

fn problem(l: &Loader, text: &str) -> LoadProblem {
    l.load(None, text).unwrap_err().problem
}

/// TestDefine_CurrentIsDeclaredAndOldestFollowsTheSteps
#[test]
fn current_is_declared_and_oldest_follows_the_steps() {
    assert_eq!((with_steps().current(), with_steps().oldest()), (3, 1));
    assert_eq!((zero_steps().current(), zero_steps().oldest()), (2, 2));
}

/// TestDefine_DroppingTheOldestStepRaisesOldestOnly
#[test]
fn dropping_the_oldest_step_raises_oldest_only() {
    let dropped = Migrations::new("widget", 3)
        .step(2, added_b)
        .build()
        .unwrap();
    assert_eq!(dropped.current(), with_steps().current());
    assert_eq!(dropped.oldest(), 2);
    assert!(
        dropped.load(None, &doc(3, "")).is_ok(),
        "a current file still reads"
    );
    assert_eq!(
        problem(&dropped, &doc(1, "")),
        LoadProblem::TooOld {
            found: 1,
            oldest: 2
        }
    );
}

/// TestDefine_PanicsOnABrokenChain (here a ChainError, not a panic)
#[test]
fn a_broken_chain_is_refused() {
    let gap = |m: Migrations| m.build().unwrap_err().problem;
    assert_eq!(
        gap(Migrations::new("k", 3).step(1, added_a).step(3, marker)),
        ChainProblem::BeyondCurrent {
            from: 3,
            current: 3
        }
    );
    assert_eq!(
        gap(Migrations::new("k", 2).step(2, added_b)),
        ChainProblem::BeyondCurrent {
            from: 2,
            current: 2
        }
    );
    // "a step below the top": steps to 2 and 3, current 4.
    assert_eq!(
        gap(Migrations::new("k", 4).step(1, added_a).step(2, added_b)),
        ChainProblem::Gap { from: 3 }
    );
    assert_eq!(
        gap(Migrations::new("k", 4).step(1, added_a).step(3, added_b)),
        ChainProblem::Gap { from: 2 }
    );
    // Out of order is sorted, not refused (a difference from Go).
    assert!(Migrations::new("k", 3)
        .step(2, added_b)
        .step(1, added_a)
        .build()
        .is_ok());
}

/// TestUpgrade_CurrentPassesThroughByteIdentical
#[test]
fn a_current_document_passes_through() {
    for l in [with_steps(), zero_steps()] {
        let text = doc(l.current(), "# keep me\nkept:   x\n");
        assert_eq!(l.upgrade(None, &text, 2).unwrap(), None, "{}", l.kind());
        assert_eq!(l.upgrade_keeping(None, &text, 2).unwrap(), None);
        assert_eq!(l.load(None, &text).unwrap(), parse(&text).unwrap());
    }
}

/// TestUpgrade_OlderIsMigratedAndStamped, TestUpgrade_RunsOnlyTheStepsAboveFound
/// and TestUpgrade_NoopStepStillAdvancesAndStamps, as fixtures.
#[test]
fn older_documents_migrate_from_where_they_are_and_are_stamped() {
    with_steps().assert_fixtures(&[
        Fixture {
            name: "v1.yaml",
            input: &doc(1, "kept: x\n"),
            expected: Some(&doc(3, "kept: x\nadded_a: yes\nadded_b: yes\n")),
        },
        // A generation-2 document does not re-run the 1 to 2 step.
        Fixture {
            name: "v2.yaml",
            input: &doc(2, "kept: x\n"),
            expected: Some(&doc(3, "kept: x\nadded_b: yes\n")),
        },
        Fixture {
            name: "v3.yaml",
            input: &doc(3, "kept: x\n"),
            expected: None,
        },
    ]);
    // A step that edits nothing still marks a generation.
    let marked = Migrations::new("marker", 1)
        .step(0, marker)
        .build()
        .unwrap();
    marked.assert_fixtures(&[Fixture {
        name: "v0.yaml",
        input: &doc(0, "kept: x\n"),
        expected: Some(&doc(1, "kept: x\n")),
    }]);
    assert_eq!(
        marked
            .upgrade(None, &doc(0, "kept: x\n"), 2)
            .unwrap()
            .as_deref(),
        Some(doc(1, "kept: x\n").as_str())
    );
}

/// TestUpgrade_NewerIsRefused
#[test]
fn newer_is_refused() {
    for l in [with_steps(), zero_steps()] {
        let found = l.current() + 4;
        let e = l.load(None, &doc(found, "")).unwrap_err();
        assert_eq!(
            e.problem,
            LoadProblem::TooNew {
                found,
                current: l.current()
            }
        );
        assert_eq!(e.kind, l.kind());
    }
}

/// TestUpgrade_BelowFloorIsRefused
#[test]
fn below_the_floor_is_refused() {
    for l in [with_steps(), zero_steps()] {
        assert_eq!(
            problem(&l, &doc(l.oldest() - 1, "")),
            LoadProblem::TooOld {
                found: l.oldest() - 1,
                oldest: l.oldest()
            }
        );
    }
}

/// TestUpgrade_KeylessBelowFloorIsRefused: a keyless document is version
/// 0, below every floor here; `version:` is just some other key.
#[test]
fn keyless_below_the_floor_is_refused() {
    let one = Migrations::new("one", 1).build().unwrap();
    for text in ["kept: x\n", "version: 1\n"] {
        for l in [with_steps(), zero_steps(), one.clone()] {
            assert_eq!(
                problem(&l, text),
                LoadProblem::TooOld {
                    found: 0,
                    oldest: l.oldest()
                },
                "{}: {text:?}",
                l.kind()
            );
        }
    }
}

/// TestUpgrade_Unreadable: a stamp that is no integer, and more than one
/// document, are refused.
#[test]
fn an_unreadable_stamp_or_stream_is_refused() {
    let bad = |problem| LoadProblem::BadVersion {
        key: DEFAULT_KEY.into(),
        problem,
    };
    for l in [with_steps(), zero_steps()] {
        assert_eq!(
            problem(&l, "schema_version: banana\n"),
            bad(BadVersion::NotAnInteger {
                text: "banana".into()
            })
        );
        assert_eq!(
            problem(&l, "schema_version: 1.5\n"),
            bad(BadVersion::NotAnInteger { text: "1.5".into() })
        );
        assert_eq!(
            problem(&l, "schema_version: {a: 1}\n"),
            bad(BadVersion::Mapping)
        );
        for stream in [
            "schema_version: 1\n---\nb: 2\n",
            "schema_version: 1\n---\n",
            "schema_version: 1\n---\na: [unterminated\n",
        ] {
            assert!(
                matches!(
                    problem(&l, stream),
                    LoadProblem::Yaml(crate::Error {
                        problem: ParseProblem::DocumentMarker { .. },
                        ..
                    })
                ),
                "{stream:?}"
            );
        }
    }
}

/// TestUpgrade_NotAWellFormedMappingPassesThroughToTheKindsParse: never a
/// version fault (here refused by the loader itself).
#[test]
fn a_document_that_is_no_well_formed_mapping_is_its_parse_failure() {
    for l in [with_steps(), zero_steps()] {
        for text in [
            "a: [unterminated\n",
            "schema_version: 1\nkept: [unterminated\n",
            "schema_version: 1\nkept: x\nkept: y\n",
            "schema_version: 1\nschema_version: 2\n",
        ] {
            assert!(
                matches!(problem(&l, text), LoadProblem::Yaml(_)),
                "{text:?}"
            );
        }
        for text in ["- a\n- b\n", "hello\n"] {
            assert_eq!(problem(&l, text), LoadProblem::NotAMapping, "{text:?}");
        }
    }
}

/// TestUpgrade_EmptyAndCommentOnlyAreGenerationZero. The config-versioning
/// rule: a missing stamp is the oldest generation, every migration applies,
/// and "an empty or comment-only file is this case, not a corrupt one".
#[test]
fn empty_and_comment_only_are_version_zero() {
    let gen0 = Migrations::new("fresh", 1)
        .step(0, added_a)
        .build()
        .unwrap();
    for text in ["", "\n\n", "# just a comment\n", "~\n", "null\n"] {
        // No step from 0: too old, never a parse or root error.
        assert_eq!(
            problem(&zero_steps(), text),
            LoadProblem::TooOld {
                found: 0,
                oldest: 2
            },
            "{text:?}"
        );
        // A step from 0: it runs, and the result is stamped.
        assert_eq!(
            gen0.load(None, text).unwrap(),
            parse("schema_version: 1\nadded_a: yes\n").unwrap(),
            "{text:?}"
        );
        assert_eq!(version_of(&parse(text).unwrap()), Ok(0), "{text:?}");
        assert_eq!(gen0.version_of(&parse(text).unwrap()), Ok(0), "{text:?}");
    }
}

/// A kind with no older generation (current 0): an empty file is current,
/// and decodes as an empty mapping.
#[test]
fn an_empty_file_of_a_version_zero_kind_is_current() {
    #[derive(Debug, PartialEq, serde::Deserialize)]
    struct Settings {
        name: Option<String>,
    }
    let l = Migrations::new("settings", 0).build().unwrap();
    for text in ["", "# nothing set yet\n", "~\n"] {
        assert_eq!(l.load(None, text).unwrap(), Value::Map(vec![]), "{text:?}");
        assert_eq!(
            l.load_into::<Settings>(None, text).unwrap(),
            Settings { name: None },
            "{text:?}"
        );
        assert_eq!(l.upgrade(None, text, 2).unwrap(), None, "{text:?}");
        assert_eq!(l.upgrade_keeping(None, text, 2).unwrap(), None, "{text:?}");
    }
}

/// TestUpgrade_CommentOnlyKeepsItsComments: a comment-only file's comments
/// are its whole content; an upgrade does not throw them away.
#[test]
fn a_comment_only_file_keeps_its_comments_when_upgraded() {
    let l = Migrations::new("fresh", 1)
        .step(0, added_a)
        .build()
        .unwrap();
    let up = l.upgrade_keeping(None, "# keep me\n", 2).unwrap().unwrap();
    assert!(up.text.contains("# keep me"), "{:?}", up.text);
    assert!(up.comments_kept);
    assert_eq!(
        parse(&up.text).unwrap(),
        parse("schema_version: 1\nadded_a: yes\n").unwrap()
    );
    // The comments come first, on lines of their own; an empty file gets
    // no blank line.
    let migrated = "schema_version: 1\nadded_a: yes\n";
    for (text, want) in [
        ("# keep me\n", format!("# keep me\n{migrated}")),
        ("# no newline", format!("# no newline\n{migrated}")),
        ("", migrated.to_string()),
    ] {
        let up = l.upgrade_keeping(None, text, 2).unwrap().unwrap();
        assert_eq!(up.text, want, "{text:?}");
        assert!(up.comments_kept, "{text:?}");
    }
}

/// TestStamp: an absent stamp goes first; a present one is set in place.
#[test]
fn the_stamp_goes_first_when_absent_and_stays_put_when_present() {
    let l = Migrations::new("fresh", 1).step(0, marker).build().unwrap();
    let keys =
        |v: &Value| -> Vec<String> { v.as_map().unwrap().iter().map(|(k, _)| k.clone()).collect() };
    assert_eq!(
        keys(&l.load(None, "a: 1\n").unwrap()),
        ["schema_version", "a"]
    );
    assert_eq!(
        keys(&l.load(None, "a: 1\nschema_version: 0\n").unwrap()),
        ["a", "schema_version"]
    );
}

/// TestDecode_MigratesThenDecodesTheTree, TestDecode_RefusesAsUpgradeDoes
/// and TestDecode_MalformedIsTheParseFailure, through load_into.
#[test]
fn decoding_migrates_then_decodes_and_refuses_as_loading_does() {
    #[derive(Debug, PartialEq, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Widget {
        schema_version: u32,
        kept: String,
        added_a: Option<String>,
        added_b: Option<String>,
    }
    let l = with_steps();
    assert_eq!(
        l.load_into::<Widget>(None, &doc(2, "kept: x\n")).unwrap(),
        Widget {
            schema_version: 3,
            kept: "x".into(),
            added_a: None,
            added_b: Some("yes".into())
        }
    );
    assert_eq!(
        l.load_into::<Widget>(None, &doc(3, "kept: y\n")).unwrap(),
        Widget {
            schema_version: 3,
            kept: "y".into(),
            added_a: None,
            added_b: None
        }
    );
    assert!(matches!(
        l.load_into::<Widget>(None, &doc(4, ""))
            .unwrap_err()
            .problem,
        LoadProblem::TooNew { found: 4, .. }
    ));
    assert!(matches!(
        l.load_into::<Widget>(None, &doc(0, ""))
            .unwrap_err()
            .problem,
        LoadProblem::TooOld { found: 0, .. }
    ));
    assert!(matches!(
        l.load_into::<Widget>(None, "kept: [unterminated\n")
            .unwrap_err()
            .problem,
        LoadProblem::Yaml(_)
    ));
}
