//! Editing mappings in place: set or remove one key of a mapping found by a
//! path of keys and sequence items, and bring a whole document to a target
//! value by editing only what differs. Every byte outside the edited entries
//! stays as written, comments and blank lines included.

use crate::edit::{Document, EditProblem, Seg};
use crate::{parse, Value};

fn s(x: &str) -> Value {
    Value::scalar(x)
}

fn map(entries: &[(&str, Value)]) -> Value {
    Value::Map(
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    )
}

fn doc(src: &str) -> Document {
    Document::parse(src).unwrap()
}

const MANIFEST: &str = "\
# bosun's own toolbelt.
version: 1   # format
targets: [linux/amd64]

packages:
  # The task runner.
  - name: just
    spec: 1.40   # pinned for the CI image
    install:
      linux: {backend: apt}

  - name: fd
    # Debian calls it fd-find.
    spec: 10.2.0
    install:
      linux: {backends: [apt, cargo], package: fd-find}

# trailing note
";

#[test]
fn set_replaces_a_scalar_keeping_the_lines_comment() {
    let mut d = doc(MANIFEST);
    d.set(&[], "version", &s("2"), 2).unwrap();
    assert_eq!(
        d.text(),
        MANIFEST.replace("version: 1   # format", "version: 2   # format")
    );
    assert_eq!(d.value(), &parse(&d.text()).unwrap());
}

#[test]
fn set_adds_a_missing_key_at_the_end_of_its_mapping() {
    let mut d = doc("a: 1\nb:\n  c: 2\n# end\n");
    d.set(&[Seg::Key("b")], "d", &s("3"), 2).unwrap();
    assert_eq!(d.text(), "a: 1\nb:\n  c: 2\n  d: 3\n# end\n");
}

#[test]
fn set_inside_a_sequence_item_changes_only_that_entry() {
    let mut d = doc(MANIFEST);
    d.set(
        &[Seg::Key("packages"), Seg::Item(1)],
        "spec",
        &s("10.3.0"),
        2,
    )
    .unwrap();
    assert_eq!(
        d.text(),
        MANIFEST.replace("    spec: 10.2.0", "    spec: 10.3.0")
    );
}

#[test]
fn set_writes_a_collection_value_at_the_entrys_indent() {
    let mut d = doc(MANIFEST);
    let block = map(&[(
        "backends",
        map(&[
            ("apt", map(&[("package", s("fd-find"))])),
            ("cargo", map(&[("package", s("fd-find"))])),
        ]),
    )]);
    d.set(
        &[Seg::Key("packages"), Seg::Item(1), Seg::Key("install")],
        "linux",
        &block,
        2,
    )
    .unwrap();
    assert_eq!(
        d.text(),
        MANIFEST.replace(
            "      linux: {backends: [apt, cargo], package: fd-find}\n",
            "      linux:\n        backends:\n          apt: {package: fd-find}\n          cargo: {package: fd-find}\n"
        )
    );
    assert_eq!(d.value(), &parse(&d.text()).unwrap());
}

#[test]
fn remove_key_takes_its_lines_and_the_comment_directly_above() {
    let mut d = doc("# keep\na: 1\n# about b\nb:\n  - x\n  - y\nc: 3\n");
    d.remove_key(&[], "b").unwrap();
    assert_eq!(d.text(), "# keep\na: 1\nc: 3\n");
}

#[test]
fn remove_key_inside_a_sequence_item() {
    let mut d = doc(MANIFEST);
    d.remove_key(&[Seg::Key("packages"), Seg::Item(1)], "spec")
        .unwrap();
    assert_eq!(
        d.text(),
        MANIFEST.replace("    # Debian calls it fd-find.\n    spec: 10.2.0\n", "")
    );
}

#[test]
fn removing_a_missing_key_is_not_found() {
    let mut d = doc("a: 1\n");
    let err = d.remove_key(&[], "b").unwrap_err();
    assert_eq!(err.problem, EditProblem::NotFound);
    assert_eq!(d.text(), "a: 1\n");
}

#[test]
fn the_key_on_an_items_dash_line_is_edited_with_the_item() {
    let mut d = doc(MANIFEST);
    let err = d
        .set(
            &[Seg::Key("packages"), Seg::Item(0)],
            "name",
            &s("just2"),
            2,
        )
        .unwrap_err();
    assert_eq!(err.problem, EditProblem::ItemFirstKey);
    assert_eq!(d.text(), MANIFEST);
}

#[test]
fn an_item_index_past_the_end_is_no_such_item() {
    let mut d = doc(MANIFEST);
    let err = d
        .set(&[Seg::Key("packages"), Seg::Item(5)], "spec", &s("1"), 2)
        .unwrap_err();
    assert_eq!(err.problem, EditProblem::NoSuchItem { index: 5, len: 2 });
}

#[test]
fn update_to_edits_only_what_differs_and_keeps_the_comments() {
    let mut target = parse(MANIFEST).unwrap();
    *target.entry("version") = s("2");
    target.remove("targets");
    let fd = match target.get_mut("packages") {
        Some(Value::Seq(items)) => &mut items[1],
        _ => unreachable!(),
    };
    *fd.get_mut("install").unwrap().entry("linux") = map(&[(
        "backends",
        map(&[
            ("apt", map(&[("package", s("fd-find"))])),
            ("cargo", map(&[("package", s("fd-find"))])),
        ]),
    )]);

    let mut d = doc(MANIFEST);
    d.update_to(&target, 2).unwrap();
    assert_eq!(
        d.text(),
        "\
# bosun's own toolbelt.
version: 2   # format

packages:
  # The task runner.
  - name: just
    spec: 1.40   # pinned for the CI image
    install:
      linux: {backend: apt}

  - name: fd
    # Debian calls it fd-find.
    spec: 10.2.0
    install:
      linux:
        backends:
          apt: {package: fd-find}
          cargo: {package: fd-find}

# trailing note
"
    );
    assert_eq!(d.value(), &target);
}

#[test]
fn update_to_rewrites_an_item_whose_first_key_changes() {
    let mut d = doc("items:\n  # first\n  - name: a\n    n: 1\n  - name: b\n");
    let target = map(&[(
        "items",
        Value::Seq(vec![
            map(&[("name", s("z")), ("n", s("1"))]),
            map(&[("name", s("b"))]),
        ]),
    )]);
    // flow_depth counts from the root, as for `write`: 3 keeps items as blocks.
    d.update_to(&target, 3).unwrap();
    assert_eq!(
        d.text(),
        "items:\n  # first\n  - name: z\n    n: 1\n  - name: b\n"
    );
}

#[test]
fn update_to_an_equal_value_changes_nothing() {
    let mut d = doc(MANIFEST);
    let same = parse(MANIFEST).unwrap();
    d.update_to(&same, 2).unwrap();
    assert_eq!(d.text(), MANIFEST);
}

#[test]
fn update_to_a_root_that_is_no_longer_a_mapping_is_refused() {
    let mut d = doc("a: 1\n");
    assert!(d.update_to(&Value::Seq(vec![s("x")]), 2).is_err());
    assert_eq!(d.text(), "a: 1\n");
}

// ------------------------------------------------------------------ shapes

#[test]
fn errors_name_the_full_path() {
    let mut d = doc(MANIFEST);
    let err = d
        .set(&[Seg::Key("packages"), Seg::Item(5)], "spec", &s("1"), 2)
        .unwrap_err();
    assert_eq!(err.path, vec!["packages".to_string(), "5".to_string()]);
    let err = d
        .remove_key(&[Seg::Key("packages"), Seg::Item(1)], "nope")
        .unwrap_err();
    assert_eq!(
        err.path,
        vec!["packages".to_string(), "1".to_string(), "nope".to_string()]
    );
}

#[test]
fn a_sequence_is_not_a_mapping_to_set_in() {
    let mut d = doc(MANIFEST);
    let err = d.set(&[Seg::Key("packages")], "x", &s("1"), 2).unwrap_err();
    assert_eq!(err.problem, EditProblem::NotAMapping);
    let err = d.set(&[Seg::Key("version")], "x", &s("1"), 2).unwrap_err();
    assert_eq!(err.problem, EditProblem::NotAMapping);
}

#[test]
fn a_flow_value_is_edited_only_whole() {
    let mut d = doc("tags: [a, b]\nopts: {x: 1}\n");
    for key in ["tags", "opts"] {
        let err = d.set(&[Seg::Key(key)], "y", &s("1"), 2).unwrap_err();
        assert_eq!(err.problem, EditProblem::InsideFlow, "{key}");
    }
}

#[test]
fn a_key_with_no_value_has_nothing_to_edit_in() {
    let mut d = doc("a:\nb: 1\n");
    let err = d.set(&[Seg::Key("a")], "c", &s("1"), 2).unwrap_err();
    assert_eq!(err.problem, EditProblem::NotAMapping);
}

#[test]
fn removing_a_key_between_blank_lines_leaves_one() {
    let mut d = doc("a: 1\n\nb: 2\n\nc: 3\n");
    d.remove_key(&[], "b").unwrap();
    assert_eq!(d.text(), "a: 1\n\nc: 3\n");
}

#[test]
fn removing_the_first_key_after_a_blank_keeps_the_blank() {
    let mut d = doc("a: 1\n\nb: 2\nc: 3\n");
    d.remove_key(&[], "b").unwrap();
    assert_eq!(d.text(), "a: 1\n\nc: 3\n");
}

#[test]
fn a_key_whose_sequence_sits_at_its_own_indent_goes_whole() {
    let mut d = doc("a:\n- x\n- y\nb: 1\n");
    d.remove_key(&[], "a").unwrap();
    assert_eq!(d.text(), "b: 1\n");
    let mut d = doc("a:\n- x\n- y\nb: 1\n");
    d.set(&[], "a", &s("z"), 2).unwrap();
    assert_eq!(d.text(), "a: z\nb: 1\n");
}

#[test]
fn an_item_at_the_left_margin_is_edited_at_its_contents_indent() {
    let mut d = doc("items:\n- name: a\n  n: 1\n");
    d.set(&[Seg::Key("items"), Seg::Item(0)], "m", &s("2"), 2)
        .unwrap();
    assert_eq!(d.text(), "items:\n- name: a\n  n: 1\n  m: 2\n");
}

#[test]
fn an_item_with_spaces_after_its_dash_is_edited_at_its_contents_indent() {
    let mut d = doc("items:\n  -   name: a\n      n: 1\n");
    d.set(&[Seg::Key("items"), Seg::Item(0)], "n", &s("2"), 2)
        .unwrap();
    assert_eq!(d.text(), "items:\n  -   name: a\n      n: 2\n");
}

#[test]
fn an_item_of_a_nested_sequence_is_found_by_its_own_indent() {
    let mut d = doc("items:\n  - name: a\n    subs:\n      - k: 1\n  - name: b\n    n: 1\n");
    d.set(&[Seg::Key("items"), Seg::Item(1)], "n", &s("2"), 2)
        .unwrap();
    assert_eq!(
        d.text(),
        "items:\n  - name: a\n    subs:\n      - k: 1\n  - name: b\n    n: 2\n"
    );
}

#[test]
fn a_new_key_goes_after_the_last_entrys_deeper_comments() {
    let mut d = doc("a:\n  b: 1\n    # about b\nc: 2\n");
    d.set(&[Seg::Key("a")], "d", &s("3"), 2).unwrap();
    assert_eq!(d.text(), "a:\n  b: 1\n    # about b\n  d: 3\nc: 2\n");
}

#[test]
fn update_to_a_longer_sequence_replaces_it() {
    let mut d = doc("# top\nitems: [a]\nn: 1\n");
    let target = map(&[("items", Value::Seq(vec![s("a"), s("b")])), ("n", s("1"))]);
    // flow_depth 1: a top-level value goes on its key's line.
    d.update_to(&target, 1).unwrap();
    assert_eq!(d.text(), "# top\nitems: [a, b]\nn: 1\n");
}

#[test]
fn update_to_matches_keys_in_any_order_inside_sequence_items() {
    // `m` is added to an item but sits before `n` in the target: the edit
    // appends it, and the document still counts as reaching the target.
    let mut d = doc("items:\n  - name: a\n    n: 1\n");
    let item = map(&[("name", s("a")), ("m", s("2")), ("n", s("1"))]);
    let target = map(&[("items", Value::Seq(vec![item]))]);
    d.update_to(&target, 3).unwrap();
    assert_eq!(d.text(), "items:\n  - name: a\n    n: 1\n    m: 2\n");
}

#[test]
fn update_to_refuses_a_target_it_cannot_reach_and_leaves_the_text() {
    // A scalar where the root mapping was: no edit can get there.
    let mut d = doc("a: 1\n");
    let err = d.update_to(&s("x"), 2).unwrap_err();
    assert_eq!(err.problem, EditProblem::NotAMapping);
    assert_eq!(d.text(), "a: 1\n");
}

#[test]
fn removing_the_last_key_after_a_blank_leaves_no_trailing_blank() {
    let mut d = doc("a: 1\n\nb: 2\n");
    d.remove_key(&[], "b").unwrap();
    assert_eq!(d.text(), "a: 1\n");
}

#[test]
fn a_key_path_through_a_sequence_is_not_a_mapping() {
    let mut d = doc(MANIFEST);
    let err = d
        .set(&[Seg::Key("packages"), Seg::Key("x")], "y", &s("1"), 2)
        .unwrap_err();
    assert_eq!(err.problem, EditProblem::NotAMapping);
}

#[test]
fn a_comment_at_the_mappings_own_indent_stays_after_a_new_key() {
    // Indented like the keys, it is about what follows, not the last entry.
    let mut d = doc("a:\n  b: 1\n  # next\nc: 2\n");
    d.set(&[Seg::Key("a")], "d", &s("3"), 2).unwrap();
    assert_eq!(d.text(), "a:\n  b: 1\n  d: 3\n  # next\nc: 2\n");
}

#[test]
fn an_item_with_its_dash_alone_on_a_line_is_edited_below_it() {
    let mut d = doc("items:\n  -\n    name: a\n    n: 1\n");
    d.set(&[Seg::Key("items"), Seg::Item(0)], "n", &s("2"), 2)
        .unwrap();
    assert_eq!(d.text(), "items:\n  -\n    name: a\n    n: 2\n");
}

// ------------------------------------------------------------------ property

use crate::rng::Rng;
use crate::write;

const NAMES: &[&str] = &[
    "a",
    "fd",
    "just",
    "a b",
    "x: y",
    "#c",
    "- d",
    "\u{65e5}\u{672c}",
    "",
    "null",
    "[e]",
];

fn name(rng: &mut Rng) -> Value {
    s(NAMES[rng.below(NAMES.len() as u32) as usize])
}

fn random_item(rng: &mut Rng) -> Value {
    let n = name(rng);
    match rng.below(4) {
        0 => n,
        1 => map(&[("name", n)]),
        2 => map(&[("name", n), ("tags", Value::Seq(vec![s("t"), s("u")]))]),
        _ => map(&[
            ("name", n),
            ("install", map(&[("linux", map(&[("backend", s("apt"))]))])),
        ]),
    }
}

/// `v` with one random change somewhere: a scalar replaced, a key added or
/// removed, or an item replaced.
fn mutate(rng: &mut Rng, v: &mut Value) {
    match v {
        Value::Map(entries) if !entries.is_empty() => {
            let i = rng.below(entries.len() as u32) as usize;
            match rng.below(4) {
                0 => entries[i].1 = name(rng),
                1 => entries.push((format!("k{}", rng.below(5)), name(rng))),
                2 if entries.len() > 1 => {
                    entries.remove(i);
                }
                _ => mutate(rng, &mut entries[i].1),
            }
            // A duplicate key from the push would not be a mapping.
            let mut seen = Vec::new();
            entries.retain(|(k, _)| {
                let fresh = !seen.contains(k);
                seen.push(k.clone());
                fresh
            });
        }
        Value::Seq(items) if !items.is_empty() => {
            let i = rng.below(items.len() as u32) as usize;
            if rng.below(2) == 0 {
                items[i] = random_item(rng);
            } else {
                mutate(rng, &mut items[i]);
            }
        }
        other => *other = name(rng),
    }
}

#[test]
fn update_to_reaches_any_nearby_target_and_keeps_unchanged_lines() {
    for seed in 0..2_000 {
        let mut rng = Rng::new(seed, 1);
        let items: Vec<Value> = (0..1 + rng.below(3))
            .map(|_| random_item(&mut rng))
            .collect();
        let v = map(&[
            ("version", s("1")),
            ("packages", Value::Seq(items)),
            ("after", s("x")),
        ]);
        let depth = 2 + rng.below(3) as usize;
        let mut src = String::new();
        for line in write(&v, depth).lines() {
            if rng.below(4) == 0 {
                src.push_str("# note\n");
            }
            src.push_str(line);
            src.push('\n');
        }
        let mut target = v.clone();
        for _ in 0..1 + rng.below(3) {
            mutate(&mut rng, &mut target);
        }
        let mut d = Document::parse(&src).unwrap();
        d.update_to(&target, depth).unwrap_or_else(|e| {
            panic!("seed {seed}: {e}\n{src}\n--- to\n{}", write(&target, depth))
        });
        let back = parse(&d.text()).unwrap_or_else(|e| panic!("seed {seed}: {e}\n{}", d.text()));
        assert_eq!(&back, d.value(), "seed {seed}");
        // `after` never changes unless the mutation touched it; when it
        // didn't, its line is still there as written.
        if target.get("after") == Some(&s("x")) {
            assert!(
                d.text().lines().any(|l| l == "after: x"),
                "seed {seed}:\n{}",
                d.text()
            );
        }
    }
}
