//! Editing a document in place: push, remove and replace a sequence item while
//! every byte outside the item stays as the person wrote it, comments and
//! blank lines included. The property at the end drives random edits over
//! written documents and checks each result reads back as the value expected.

use crate::edit::{Document, EditError, EditProblem};
use crate::rng::Rng;
use crate::{parse, write, Value};
use angzarr_diagnostic::Diagnostic;

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

fn tool(name: &str) -> Value {
    map(&[("name", s(name)), ("spec", s("1.0"))])
}

fn doc(src: &str) -> Document {
    Document::parse(src).unwrap()
}

const TOOLS: &str = "\
# Tools for this repo.
version: 1

packages:
  # The task runner.
  - name: just
    spec: 1.40   # pinned for the CI image

  - name: jq
    spec: 1.7.1
    # jq's spec follows Debian.

# trailing note
";

#[test]
fn push_appends_at_the_sequences_indent_and_keeps_everything_else() {
    let mut d = doc(TOOLS);
    d.push(&["packages"], &tool("gum"), 2).unwrap();
    assert_eq!(
        d.text(),
        "\
# Tools for this repo.
version: 1

packages:
  # The task runner.
  - name: just
    spec: 1.40   # pinned for the CI image

  - name: jq
    spec: 1.7.1
    # jq's spec follows Debian.

  - name: gum
    spec: 1.0

# trailing note
"
    );
    assert_eq!(d.value(), &parse(&d.text()).unwrap());
}

#[test]
fn push_without_blank_lines_between_items_adds_none() {
    let mut d = doc("packages:\n  - name: a\n  - name: b\nafter: x\n");
    d.push(&["packages"], &map(&[("name", s("c"))]), 2).unwrap();
    assert_eq!(
        d.text(),
        "packages:\n  - name: a\n  - name: b\n  - name: c\nafter: x\n"
    );
}

#[test]
fn push_into_a_sequence_at_its_keys_own_indent() {
    let mut d = doc("packages:\n- a\n- b\n");
    d.push(&["packages"], &s("c"), 2).unwrap();
    assert_eq!(d.text(), "packages:\n- a\n- b\n- c\n");
}

#[test]
fn a_sequence_at_its_keys_indent_ends_at_the_next_key() {
    let mut d = doc("packages:\n- a\nafter: x\n");
    d.push(&["packages"], &s("b"), 2).unwrap();
    assert_eq!(d.text(), "packages:\n- a\n- b\nafter: x\n");
}

#[test]
fn a_root_sequence_is_edited_with_an_empty_path() {
    let mut d = doc("- a\n- b\n");
    d.push(&[], &s("c"), 2).unwrap();
    assert_eq!(d.text(), "- a\n- b\n- c\n");
    d.remove(&[], 0).unwrap();
    assert_eq!(d.text(), "- b\n- c\n");
    d.replace(&[], 0, &s("z"), 2).unwrap();
    assert_eq!(d.text(), "- z\n- c\n");
}

#[test]
fn push_writes_nested_values_at_the_flow_depth_asked_for() {
    let item = map(&[
        ("name", s("fd")),
        (
            "install",
            map(&[(
                "linux",
                map(&[("backend", s("apt")), ("package", s("fd-find"))]),
            )]),
        ),
    ]);
    let mut d = doc("packages:\n  - name: a\n");
    d.push(&["packages"], &item, 3).unwrap();
    assert_eq!(d.text(), "packages:\n  - name: a\n  - name: fd\n    install:\n      linux: {backend: apt, package: fd-find}\n");
}

#[test]
fn push_to_a_missing_key_adds_it_at_the_end_of_its_mapping() {
    let mut d = doc("version: 1\n# end\n");
    d.push(&["packages"], &s("a"), 2).unwrap();
    assert_eq!(d.text(), "version: 1\npackages:\n  - a\n# end\n");
    assert_eq!(d.value().get("packages"), Some(&Value::Seq(vec![s("a")])));
}

#[test]
fn push_to_a_key_with_no_value_starts_a_block_sequence() {
    let mut d = doc("packages:   # none yet\nafter: x\n");
    d.push(&["packages"], &s("a"), 2).unwrap();
    assert_eq!(d.text(), "packages:   # none yet\n  - a\nafter: x\n");
}

#[test]
fn a_flow_sequence_stays_on_its_line_when_it_still_fits() {
    let mut d = doc("targets: [linux/amd64]   # more later\n");
    d.push(&["targets"], &s("linux/arm64"), 2).unwrap();
    assert_eq!(
        d.text(),
        "targets: [linux/amd64, linux/arm64]   # more later\n"
    );
}

#[test]
fn an_empty_flow_sequence_becomes_a_block_for_a_mapping_item() {
    let mut d = doc("packages: []   # none yet\n");
    d.push(&["packages"], &tool("a"), 2).unwrap();
    assert_eq!(
        d.text(),
        "packages:   # none yet\n  - name: a\n    spec: 1.0\n"
    );
}

#[test]
fn push_into_a_nested_mapping() {
    let mut d = doc("a:\n  b:\n    - x\n  c: y\n");
    d.push(&["a", "b"], &s("z"), 2).unwrap();
    assert_eq!(d.text(), "a:\n  b:\n    - x\n    - z\n  c: y\n");
}

#[test]
fn remove_takes_the_item_and_its_leading_comment_and_one_blank_line() {
    let mut d = doc(TOOLS);
    d.remove(&["packages"], 0).unwrap();
    assert_eq!(
        d.text(),
        "\
# Tools for this repo.
version: 1

packages:
  - name: jq
    spec: 1.7.1
    # jq's spec follows Debian.

# trailing note
"
    );
}

#[test]
fn remove_keeps_a_comment_indented_inside_the_item_with_it() {
    let mut d = doc(TOOLS);
    d.remove(&["packages"], 1).unwrap();
    assert_eq!(
        d.text(),
        "\
# Tools for this repo.
version: 1

packages:
  # The task runner.
  - name: just
    spec: 1.40   # pinned for the CI image

# trailing note
"
    );
}

#[test]
fn a_comment_at_the_items_indent_below_it_belongs_to_the_next_item() {
    let mut d = doc("packages:\n  - a\n  # about b\n  - b\n");
    d.remove(&["packages"], 0).unwrap();
    assert_eq!(d.text(), "packages:\n  # about b\n  - b\n");
}

#[test]
fn removing_the_last_of_blank_separated_items_takes_the_blank_before_it() {
    let mut d = doc("packages:\n  - a\n\n  - b\nafter: x\n");
    d.remove(&["packages"], 1).unwrap();
    assert_eq!(d.text(), "packages:\n  - a\nafter: x\n");
}

#[test]
fn removing_a_middle_item_keeps_the_blank_before_it() {
    let mut d = doc("packages:\n  - a\n\n  - b\n  - c\n");
    d.remove(&["packages"], 1).unwrap();
    assert_eq!(d.text(), "packages:\n  - a\n\n  - c\n");
}

#[test]
fn removing_the_last_item_leaves_an_empty_sequence_not_a_null() {
    let mut d = doc("packages:\n  - a\nafter: x\n");
    d.remove(&["packages"], 0).unwrap();
    assert_eq!(d.text(), "packages: []\nafter: x\n");
    assert_eq!(d.value().get("packages"), Some(&Value::Seq(vec![])));
}

#[test]
fn remove_from_a_flow_sequence() {
    let mut d = doc("targets: [a, b, c]\n");
    d.remove(&["targets"], 1).unwrap();
    assert_eq!(d.text(), "targets: [a, c]\n");
}

#[test]
fn replace_keeps_the_leading_comment_and_the_separation() {
    let mut d = doc(TOOLS);
    d.replace(&["packages"], 0, &tool("just"), 2).unwrap();
    assert_eq!(
        d.text(),
        "\
# Tools for this repo.
version: 1

packages:
  # The task runner.
  - name: just
    spec: 1.0

  - name: jq
    spec: 1.7.1
    # jq's spec follows Debian.

# trailing note
"
    );
}

#[test]
fn push_then_remove_restores_the_text() {
    for src in [
        TOOLS,
        "packages:\n- a\n",
        "packages:\n  - name: a\n  - name: b\nafter: x\n",
        "packages: [a]\n",
    ] {
        let mut d = doc(src);
        let n = d.value().get("packages").unwrap().as_seq().unwrap().len();
        d.push(&["packages"], &s("new"), 2).unwrap();
        d.remove(&["packages"], n).unwrap();
        assert_eq!(d.text(), src);
    }
}

#[test]
fn line_endings_and_a_byte_order_mark_are_kept() {
    let mut d = doc("\u{feff}packages:\r\n  - a\r\n");
    d.push(&["packages"], &s("b"), 2).unwrap();
    assert_eq!(d.text(), "\u{feff}packages:\r\n  - a\r\n  - b\r\n");
}

#[test]
fn a_document_without_a_final_newline() {
    let mut d = doc("packages:\n  - a");
    d.push(&["packages"], &s("b"), 2).unwrap();
    assert_eq!(d.text(), "packages:\n  - a\n  - b");
}

#[test]
fn into_string_gives_the_text() {
    let mut d = doc("packages: []\n");
    d.push(&["packages"], &s("a"), 2).unwrap();
    assert_eq!(d.into_string(), "packages: [a]\n");
}

// ------------------------------------------------------------------ errors

fn problem(r: Result<(), EditError>) -> EditProblem {
    r.unwrap_err().problem
}

#[test]
fn a_document_that_does_not_parse_is_refused() {
    let e = Document::parse("a: &x 1\n").unwrap_err();
    assert_eq!(e.line, 1);
}

#[test]
fn edit_errors_name_the_path() {
    let mut d = doc("a: 1\nb:\n  c: [x]\nd: {e: [y]}\n");
    assert_eq!(problem(d.remove(&["z"], 0)), EditProblem::NotFound);
    assert_eq!(
        problem(d.push(&["z", "q"], &s("v"), 2)),
        EditProblem::NotFound
    );
    assert_eq!(
        problem(d.push(&["a"], &s("v"), 2)),
        EditProblem::NotASequence
    );
    assert_eq!(
        problem(d.push(&["a", "q"], &s("v"), 2)),
        EditProblem::NotAMapping
    );
    assert_eq!(
        problem(d.remove(&["b", "c"], 1)),
        EditProblem::NoSuchItem { index: 1, len: 1 }
    );
    assert_eq!(
        problem(d.push(&["d", "e"], &s("v"), 2)),
        EditProblem::InsideFlow
    );
    assert_eq!(problem(d.push(&[], &s("v"), 2)), EditProblem::NotASequence);
    let mut seq = doc("packages:\n  - name: a\n");
    assert_eq!(
        problem(seq.push(&["packages", "name"], &s("v"), 2)),
        EditProblem::NotAMapping
    );
    let e = d.remove(&["b", "x"], 0).unwrap_err();
    assert_eq!(e.path, vec!["b".to_string(), "x".to_string()]);
    assert_eq!(e.to_string(), "angzarr-yaml-edit-not-found path=b.x");
}

#[test]
fn a_failed_edit_changes_nothing() {
    let mut d = doc(TOOLS);
    assert!(d.remove(&["packages"], 9).is_err());
    assert_eq!(d.text(), TOOLS);
}

#[test]
fn edit_errors_are_diagnostics() {
    let mut d = doc("a: 1\n");
    let e = d.push(&["a"], &s("v"), 2).unwrap_err();
    assert_eq!(e.code(), "angzarr-yaml-edit-not-a-sequence");
    let names: Vec<&str> = e.args().iter().map(|(k, _)| *k).collect();
    assert_eq!(names, vec!["path"]);
}

// ------------------------------------------------------------------ property

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

fn random_item(rng: &mut Rng) -> Value {
    let name = s(NAMES[rng.below(NAMES.len() as u32) as usize]);
    match rng.below(4) {
        0 => name,
        1 => map(&[("name", name)]),
        2 => map(&[("name", name), ("tags", Value::Seq(vec![s("t"), s("u")]))]),
        _ => map(&[
            ("name", name),
            ("install", map(&[("linux", map(&[("backend", s("apt"))]))])),
        ]),
    }
}

/// A document holding `items` under `packages`, written at a random flow
/// depth, with comments and blank lines dropped between its lines.
fn random_document(rng: &mut Rng, items: &[Value]) -> String {
    let v = map(&[
        ("version", s("1")),
        ("packages", Value::Seq(items.to_vec())),
        ("after", s("x")),
    ]);
    let written = write(&v, 2 + rng.below(3) as usize);
    let mut out = String::new();
    for line in written.lines() {
        match rng.below(6) {
            0 => out.push('\n'),
            1 => out.push_str("# note\n"),
            _ => {}
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

/// Whether every line of `small` appears in `big`, in order.
fn subsequence(small: &[&str], big: &[&str]) -> bool {
    let mut big = big.iter();
    small.iter().all(|line| big.any(|b| b == line))
}

#[test]
fn random_edits_read_back_as_the_value_expected() {
    for seed in 0..2_000 {
        let mut rng = Rng::new(seed, 0);
        let mut items: Vec<Value> = (0..rng.below(4)).map(|_| random_item(&mut rng)).collect();
        let src = random_document(&mut rng, &items);
        let mut d = Document::parse(&src).unwrap_or_else(|e| panic!("seed {seed}: {e}\n{src}"));
        for _ in 0..6 {
            let before = d.text();
            let was_empty = items.is_empty();
            let op = rng.below(3);
            let depth = 1 + rng.below(3) as usize;
            if op == 0 || items.is_empty() {
                let item = random_item(&mut rng);
                d.push(&["packages"], &item, depth).unwrap();
                items.push(item);
            } else if op == 1 {
                let i = rng.below(items.len() as u32) as usize;
                d.remove(&["packages"], i).unwrap();
                items.remove(i);
            } else {
                let i = rng.below(items.len() as u32) as usize;
                let item = random_item(&mut rng);
                d.replace(&["packages"], i, &item, depth).unwrap();
                items[i] = item;
            }
            // Bytes outside the item stay: a push only adds lines, a remove
            // only takes them. The sequence's own key line is left out: it is
            // rewritten when a flow sequence changes or becomes a block.
            if !was_empty && !items.is_empty() {
                let key_line = |l: &&str| !l.starts_with("packages:");
                let (old, new) = (
                    before.lines().filter(key_line).collect::<Vec<_>>(),
                    d.text(),
                );
                let new = new.lines().filter(key_line).collect::<Vec<_>>();
                match op {
                    0 => assert!(
                        subsequence(&old, &new),
                        "seed {seed}: push changed a line:\n{before}\n---\n{}",
                        d.text()
                    ),
                    1 => assert!(
                        subsequence(&new, &old),
                        "seed {seed}: remove changed a line:\n{before}\n---\n{}",
                        d.text()
                    ),
                    _ => {}
                }
            }
            let back =
                parse(&d.text()).unwrap_or_else(|e| panic!("seed {seed}: {e}\n{}", d.text()));
            assert_eq!(
                back.get("packages"),
                Some(&Value::Seq(items.clone())),
                "seed {seed}:\n{}",
                d.text()
            );
            assert_eq!(back.get("after"), Some(&s("x")), "seed {seed}");
            assert_eq!(&back, d.value(), "seed {seed}");
        }
    }
}

#[test]
fn same_ignores_only_the_order_of_mapping_keys() {
    use super::same;
    let ab = parse("a: 1\nb: [x, {c: 2, d: 3}]\n").unwrap();
    let ba = parse("b: [x, {d: 3, c: 2}]\na: 1\n").unwrap();
    assert!(same(&ab, &ba));
    for other in [
        "a: 1\n",
        "a: 1\nb: [x, {c: 2, d: 3}]\ne: 4\n",
        "a: 1\nz: [x, {c: 2, d: 3}]\n",
        "a: 1\nb: [x, {c: 2, d: 4}]\n",
        "a: 1\nb: [x]\n",
        "a: 1\nb: [x, {c: 2, d: 3}, y]\n",
        "a: 1\nb: [y, {c: 2, d: 3}]\n",
        "a: 1\nb: x\n",
    ] {
        assert!(!same(&ab, &parse(other).unwrap()), "{other:?}");
    }
    assert!(!same(&s("x"), &s("y")));
    assert!(same(&Value::Null, &Value::Null));
}

/// A document whose kept value says something its text does not: what an
/// edit must refuse rather than write, as it would a bug in its own splicing.
fn out_of_step(text: &str, value: &str) -> Document {
    Document {
        bom: false,
        lines: super::split(text),
        value: parse(value).unwrap(),
    }
}

#[test]
fn an_edit_whose_text_reads_back_wrong_is_refused() {
    let unfaithful = |path: &[&str]| EditError {
        path: path.iter().map(|k| k.to_string()).collect(),
        problem: EditProblem::Unfaithful,
    };
    let text = "a: 1\nl:\n  - x\n";
    let mut d = out_of_step(text, "a: 2\nl: [x]\n");
    assert_eq!(d.set(&[], "b", &s("y"), 0), Err(unfaithful(&["b"])));
    assert_eq!(d.push(&["l"], &s("y"), 0), Err(unfaithful(&["l"])));
    assert_eq!(d.text(), text, "a refused edit changes nothing");
}

/// A flow sequence split across lines (Go-era configs have them) is edited
/// whole, written back on its key's line; the lines around it stay.
#[test]
fn a_flow_sequence_split_across_lines_is_edited_whole() {
    let src = "# Tags.\nns:\n  area: [a, b,   # first\n         c]\n  # next\n  other: open\n";
    let mut d = doc(src);
    d.push(&["ns", "area"], &s("d"), 2).unwrap();
    assert_eq!(
        d.text(),
        "# Tags.\nns:\n  area: [a, b, c, d]   # first\n  # next\n  other: open\n"
    );
    let mut d = doc(src);
    d.remove(&["ns", "area"], 0).unwrap();
    assert_eq!(
        d.text(),
        "# Tags.\nns:\n  area: [b, c]   # first\n  # next\n  other: open\n"
    );
    // An edit elsewhere leaves the split lines as written.
    let mut d = doc(src);
    d.push(&["ns", "more"], &s("x"), 2).unwrap();
    assert_eq!(d.text(), format!("{src}  more:\n    - x\n"));
}
