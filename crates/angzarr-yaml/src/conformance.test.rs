//! Conformance against the official YAML test suite
//! (github.com/yaml/yaml-test-suite), its data release `data-2022-01-17`
//! (commit 6e6c296ac9c9d2d5c4134b4b64d01b29ac19ff6f), vendored under
//! `testdata/yaml-test-suite/` with the suite's MIT `License` (taken from
//! its `v2022-01-17` tag, 45db50aecf9b1520f8258938c88f396e96f30831, as the
//! data release carries none). Vendored, not fetched: the run is offline and
//! hermetic, and a case can change only in a commit that says so. Each case
//! keeps its `===` (its name), `in.yaml`, `test.event` and `error` (present
//! when the input is invalid YAML); the rest of the release is not used.
//!
//! Every case runs. A valid case passes when the reader reads its input as
//! the suite's event stream says, in this crate's model: every scalar its
//! text, a plain empty, `~` or `null` a null, an empty stream a null. An
//! invalid case passes when the reader refuses it. Every case that does not
//! pass is in `testdata/yaml-test-suite-unsupported.txt`, with how it fails
//! and why; a listed case that passes, an unlisted one that fails, or a
//! listed one that fails another way fails this test, so every change in
//! conformance is seen and said.
//!
//! One adaptation: this reader takes one document and no markers, so a valid
//! case of one explicit document whose `---` stands alone on its first line
//! (and whose `...`, if any, alone on its last) is read without those lines,
//! as a caller that splits a stream into documents hands it over.

use super::*;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const SUITE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/yaml-test-suite");
const UNSUPPORTED: &str = include_str!("../testdata/yaml-test-suite-unsupported.txt");

/// How a case came out.
enum Outcome {
    Pass,
    /// A valid case refused.
    Refused(ParseProblem),
    /// A valid case read as another value (or the suite's value is not one
    /// this crate's model holds: an alias, a key that is no scalar).
    Misread(String),
    /// An invalid case read as a value.
    AcceptsInvalid,
}

impl Outcome {
    fn kind(&self) -> &'static str {
        match self {
            Outcome::Pass => "pass",
            Outcome::Refused(_) => "refused",
            Outcome::Misread(_) => "misread",
            Outcome::AcceptsInvalid => "accepts-invalid",
        }
    }

    /// The outcome said, with what it holds.
    fn said(&self) -> String {
        match self {
            Outcome::Refused(problem) => format!("refused: {problem:?}"),
            Outcome::Misread(how) => format!("misread: {how}"),
            other => other.kind().to_string(),
        }
    }
}

/// Every case directory: one holding an `in.yaml`, named by its path under
/// the suite (`2G84/00`).
fn cases() -> Vec<String> {
    fn walk(dir: &Path, root: &Path, out: &mut Vec<String>) {
        let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
            .map(|e| e.unwrap().path())
            .filter(|p| p.is_dir())
            .collect();
        entries.sort();
        for p in entries {
            if p.join("in.yaml").is_file() {
                let id = p.strip_prefix(root).unwrap().to_string_lossy();
                out.push(id.replace('\\', "/"));
            }
            walk(&p, root, out);
        }
    }
    let root = Path::new(SUITE);
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out
}

/// An event's text unescaped: the suite writes `\\`, `\b`, `\n`, `\r`, `\t`.
fn unescape_event(s: &str) -> String {
    let mut out = String::new();
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('r') => out.push('\r'),
            Some('b') => out.push('\u{8}'),
            Some('\\') => out.push('\\'),
            Some(other) => panic!("an event escape the harness does not know: \\{other}"),
            None => out.push('\\'),
        }
    }
    out
}

/// A collection being built from the events.
enum Frame {
    Seq(Vec<Value>),
    Map(Vec<(String, Value)>, Option<Option<String>>),
}

/// The documents an event stream describes, as this crate's values, or why
/// they are no such values.
fn expected(events: &str) -> Result<Vec<Value>, String> {
    let mut docs = Vec::new();
    let mut stack: Vec<Frame> = Vec::new();
    let mut done: Option<Value> = None;
    for line in events.lines() {
        let (head, rest) = line.split_once(' ').unwrap_or((line, ""));
        let value = match head {
            "+STR" | "-STR" | "+DOC" => continue,
            "-DOC" => {
                docs.push(done.take().unwrap_or(Value::Null));
                continue;
            }
            "+SEQ" => {
                stack.push(Frame::Seq(Vec::new()));
                continue;
            }
            "+MAP" => {
                stack.push(Frame::Map(Vec::new(), None));
                continue;
            }
            "-SEQ" | "-MAP" => match stack.pop() {
                Some(Frame::Seq(items)) => Value::Seq(items),
                Some(Frame::Map(entries, _)) => Value::Map(entries),
                None => return Err("unbalanced events".into()),
            },
            "=ALI" => return Err(format!("an alias ({rest})")),
            "=VAL" => {
                let mut v = rest;
                while v.starts_with(['&', '<']) {
                    v = v.split_once(' ').map_or("", |(_, r)| r);
                }
                let (style, text) = v.split_at(1);
                let text = unescape_event(text);
                if style == ":" && matches!(text.as_str(), "" | "~" | "null") {
                    Value::Null
                } else {
                    Value::Scalar(text)
                }
            }
            other => return Err(format!("an event the harness does not know: {other}")),
        };
        match stack.last_mut() {
            None => done = Some(value),
            Some(Frame::Seq(items)) => items.push(value),
            Some(Frame::Map(entries, key)) => match key.take() {
                None => match value {
                    Value::Scalar(k) => *key = Some(Some(k)),
                    Value::Null => return Err("a null key".into()),
                    _ => return Err("a key that is a collection".into()),
                },
                Some(Some(k)) => entries.push((k, value)),
                Some(None) => unreachable!("a null key is refused above"),
            },
        }
    }
    Ok(docs)
}

/// The line `---` or `...` is, alone or before a comment.
fn marker_alone(line: &str, marker: &str) -> bool {
    line.strip_prefix(marker).is_some_and(|rest| {
        let rest = rest.trim_matches([' ', '\t']);
        rest.is_empty() || rest.starts_with('#')
    })
}

/// `src` without the `---` alone on its first meaningful line and the
/// `...` alone on its last, if it starts with that `---`.
fn without_markers(src: &str) -> Option<String> {
    let mut lines: Vec<&str> = src.split('\n').collect();
    let meaningful = |l: &&str| {
        let t = l.trim_start_matches([' ', '\t']);
        !t.is_empty() && !t.starts_with('#')
    };
    let first = lines.iter().position(&meaningful)?;
    if !marker_alone(lines[first], "---") {
        return None;
    }
    lines[first] = "";
    if let Some(last) = lines.iter().rposition(meaningful) {
        if marker_alone(lines[last], "...") {
            lines[last] = "";
        }
    }
    Some(lines.join("\n"))
}

fn outcome(id: &str) -> Outcome {
    let dir = Path::new(SUITE).join(id);
    let read = |name: &str| {
        std::fs::read_to_string(dir.join(name)).unwrap_or_else(|e| panic!("{id}/{name}: {e}"))
    };
    let src = read("in.yaml");
    if dir.join("error").exists() {
        return match parse(&src) {
            Ok(_) => Outcome::AcceptsInvalid,
            Err(_) => Outcome::Pass,
        };
    }
    let events = read("test.event");
    let explicit_one = events.matches("+DOC").count() == 1 && events.contains("+DOC ---");
    let src = match explicit_one.then(|| without_markers(&src)).flatten() {
        Some(bare) => bare,
        None => src,
    };
    let got = match parse(&src) {
        Ok(v) => v,
        Err(e) => return Outcome::Refused(e.problem),
    };
    match expected(&events) {
        Ok(docs) if docs.is_empty() && got == Value::Null => Outcome::Pass,
        Ok(docs) if docs.len() == 1 && docs[0] == got => Outcome::Pass,
        Ok(docs) => Outcome::Misread(format!("expected {docs:?}, read {got:?}")),
        Err(why) => Outcome::Misread(format!("the suite's value is {why}; read {got:?}")),
    }
}

/// The committed list: each case id, how it fails, and why.
fn unsupported() -> BTreeMap<String, (String, String)> {
    let mut out = BTreeMap::new();
    for line in UNSUPPORTED.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (id, rest) = line
            .split_once(' ')
            .unwrap_or_else(|| panic!("no kind: {line}"));
        let (kind, reason) = rest
            .trim()
            .split_once(": ")
            .unwrap_or_else(|| panic!("no reason: {line}"));
        assert!(
            out.insert(
                id.to_string(),
                (kind.to_string(), reason.trim().to_string())
            )
            .is_none(),
            "listed twice: {id}"
        );
    }
    out
}

#[test]
fn every_case_of_the_yaml_test_suite_passes_or_is_listed_with_its_reason() {
    let listed = unsupported();
    let all = cases();
    assert!(
        all.len() > 300,
        "the suite went missing: {} cases",
        all.len()
    );
    let mut wrong = Vec::new();
    let mut passed = 0;
    for id in &all {
        let o = outcome(id);
        passed += usize::from(matches!(o, Outcome::Pass));
        match (listed.get(id), &o) {
            (None, Outcome::Pass) => {}
            (Some((kind, _)), _) if kind == o.kind() => {}
            (Some((kind, _)), Outcome::Pass) => wrong.push(format!(
                "{id}: listed {kind}, now passes: take it off the list"
            )),
            (Some((kind, _)), o) => wrong.push(format!("{id}: listed {kind}, now {}", o.said())),
            (None, o) => wrong.push(format!("{id} {}", o.said())),
        }
    }
    for id in listed.keys().filter(|id| !all.contains(id)) {
        wrong.push(format!("{id}: listed, and no such case"));
    }
    assert!(
        wrong.is_empty(),
        "{passed} of {} cases pass; {} differ from the list:\n{}",
        all.len(),
        wrong.len(),
        wrong.join("\n")
    );
    assert_eq!(passed + listed.len(), all.len());
}

/// The cases of today's defect are supported: plain scalars over several
/// lines, among them the spec's Example 7.12 "Plain Lines".
#[test]
fn the_suites_multi_line_plain_scalars_pass() {
    for id in [
        "HS5T", // Spec Example 7.12. Plain Lines
        "36F6", // Multiline plain scalar with empty line
        "A984", // Multiline Scalar in Mapping
        "9YRD", // Multiline Scalar at Top Level
        "EX5H", // Multiline Scalar at Top Level [1.3]
        "NB6Z", // Multiline plain value with tabs on empty lines
    ] {
        let o = outcome(id);
        assert!(matches!(o, Outcome::Pass), "{id}: {}", o.said());
    }
}
