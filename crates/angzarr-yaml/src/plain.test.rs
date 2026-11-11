//! A plain (unquoted) scalar that continues onto the lines under it, folded
//! into one value as YAML 1.2 folds it (§7.3.3, §6.5): each line break
//! between two lines of text is a space, each empty line between them a line
//! break. Hand-written documents wrap long prose this way, and editors that
//! wrap at a column write it too; refusing it refused real files.

use super::*;
use crate::edit::Document;

fn s(x: &str) -> Value {
    Value::scalar(x)
}

fn value_of(src: &str, key: &str) -> Value {
    parse(src)
        .unwrap_or_else(|e| panic!("{src:?}: {e}"))
        .get(key)
        .cloned()
        .unwrap_or_else(|| panic!("{src:?}: no {key}"))
}

fn refused(src: &str) -> (usize, ParseProblem) {
    let e = parse(src).expect_err(src);
    (e.line, e.problem)
}

/// A front-matter description wrapped by an editor: the value goes on at
/// the key's indent plus two, and the document goes on after it.
#[test]
fn a_value_wrapped_onto_deeper_lines_is_one_line_of_text() {
    let src = "\
tags:
  - wire
  - format
description: You are writing a reader or a writer \u{2014} defining a wire
  format, a struct contract, an API response, or
  a parser for input someone else produced.
title: Contracts
";
    let v = parse(src).unwrap();
    assert_eq!(
        v.get("description"),
        Some(&s(
            "You are writing a reader or a writer \u{2014} defining a wire \
                 format, a struct contract, an API response, or \
                 a parser for input someone else produced."
        ))
    );
    assert_eq!(v.get("title"), Some(&s("Contracts")));
    assert_eq!(
        v.get("tags"),
        Some(&Value::Seq(vec![s("wire"), s("format")]))
    );
}

/// A generated document: a nested key whose value wraps deeper than the key,
/// then its siblings, a sequence and a block scalar among them.
#[test]
fn a_nested_value_wrapped_onto_deeper_lines_ends_at_its_next_sibling() {
    let src = "\
sections:
  notes:
    alpha:
      premise: You are about to change a record, judge whether
        something already written covers it, or decide whether a
        finding should become a row at all.
      tags:
        - records
      content: |
        # Alpha

        Text.
";
    let alpha = parse(src).unwrap();
    let alpha = alpha
        .get("sections")
        .and_then(|v| v.get("notes"))
        .and_then(|v| v.get("alpha"))
        .unwrap();
    assert_eq!(
        alpha.get("premise"),
        Some(&s("You are about to change a record, judge whether \
                 something already written covers it, or decide whether a \
                 finding should become a row at all."))
    );
    assert_eq!(alpha.get("tags"), Some(&Value::Seq(vec![s("records")])));
    assert_eq!(alpha.get("content"), Some(&s("# Alpha\n\nText.\n")));
}

/// The spec's own Example 7.12 "Plain Lines": a document that is one plain
/// scalar. The empty line is a line break, the next break a space, and the
/// space and tab around each line are not its text.
#[test]
fn the_specs_plain_lines() {
    assert_eq!(
        parse("1st non-empty\n\n 2nd non-empty \n\t3rd non-empty\n").unwrap(),
        s("1st non-empty\n2nd non-empty 3rd non-empty")
    );
    // The same lines as a key's value.
    assert_eq!(
        value_of(
            "a: 1st non-empty\n\n  2nd non-empty \n  \t3rd non-empty\n",
            "a"
        ),
        s("1st non-empty\n2nd non-empty 3rd non-empty")
    );
}

#[test]
fn a_continuation_ends_at_a_line_no_deeper_than_its_key() {
    let v = parse("a: b\n  c\nd: e\n").unwrap();
    assert_eq!(v.get("a"), Some(&s("b c")));
    assert_eq!(v.get("d"), Some(&s("e")));
    // At the end of the file, with or without a final line break.
    assert_eq!(value_of("a: b\n  c", "a"), s("b c"));
    // Several lines, each its own indent, deeper than the key.
    assert_eq!(value_of("a: b\n      c\n  d\n", "a"), s("b c d"));
    // Nested: deeper than the nested key, and the parent's next key ends it.
    let v = parse("x:\n  a: b\n   c\ny: z\n").unwrap();
    assert_eq!(v.get("x").unwrap().get("a"), Some(&s("b c")));
    assert_eq!(v.get("y"), Some(&s("z")));
    // A line at the key's own indent is the next key, not the value.
    let v = parse("x:\n  a: b\n  c: d\n").unwrap();
    assert_eq!(v.get("x").unwrap().get("a"), Some(&s("b")));
    assert_eq!(v.get("x").unwrap().get("c"), Some(&s("d")));
}

#[test]
fn an_items_value_continues_deeper_than_its_dash() {
    assert_eq!(
        parse("- b\n  c\n- d\n").unwrap(),
        Value::Seq(vec![s("b c"), s("d")])
    );
    assert_eq!(
        parse("- - b\n    c\n").unwrap(),
        Value::Seq(vec![Value::Seq(vec![s("b c")])])
    );
    // `- key: value`: deeper than the key, which is past the dash; at the
    // key's column it is the item's next key.
    let v = parse("- k: b\n    c\n  j: d\n").unwrap();
    assert_eq!(v.as_seq().unwrap()[0].get("k"), Some(&s("b c")));
    assert_eq!(v.as_seq().unwrap()[0].get("j"), Some(&s("d")));
    let v = parse("- k: b\n   c\n").unwrap();
    assert_eq!(v.as_seq().unwrap()[0].get("k"), Some(&s("b c")));
}

/// A scalar alone under a key may continue at its own indent.
#[test]
fn a_scalar_under_its_key_continues_at_its_own_indent() {
    assert_eq!(value_of("a:\n  b\n  c\nd: e\n", "a"), s("b c"));
    assert_eq!(value_of("a:\n  b\n    c\n", "a"), s("b c"));
    // Less indented than its first line is no continuation.
    assert_eq!(
        refused("a:\n   b\n  c\n"),
        (3, ParseProblem::UnexpectedIndent)
    );
}

#[test]
fn an_empty_line_inside_is_a_line_break() {
    assert_eq!(value_of("a: b\n\n  c\n", "a"), s("b\nc"));
    assert_eq!(value_of("a: b\n\n\n  c\n", "a"), s("b\n\nc"));
    // A line of spaces or tabs alone is empty, whatever its indent.
    assert_eq!(value_of("a: b\n \t \n  c\n", "a"), s("b\nc"));
    assert_eq!(value_of("a: b\n      \n  c\n", "a"), s("b\nc"));
    // Empty lines after the last line of text are layout, not the value's.
    let v = parse("a: b\n  c\n\n\nd: e\n").unwrap();
    assert_eq!(v.get("a"), Some(&s("b c")));
    assert_eq!(v.get("d"), Some(&s("e")));
    assert_eq!(value_of("a: b\n  c\n\n", "a"), s("b c"));
}

#[test]
fn a_comment_ends_the_value() {
    // After the last line's text: the comment is no part of it.
    let v = parse("a: b\n  c # note\nd: e\n").unwrap();
    assert_eq!(v.get("a"), Some(&s("b c")));
    assert_eq!(v.get("d"), Some(&s("e")));
    assert_eq!(value_of("a: b\n  c\t# note\n", "a"), s("b c"));
    // `#` with no space before it is text.
    assert_eq!(value_of("a: b\n  c#d\n", "a"), s("b c#d"));
    // A line after the comment is refused, as YAML refuses it: a comment
    // ends a plain scalar, so nothing deeper can follow.
    assert_eq!(
        refused("a: b # note\n  c\n"),
        (2, ParseProblem::UnexpectedIndent)
    );
    assert_eq!(
        refused("a: b\n  c # note\n  d\n"),
        (3, ParseProblem::UnexpectedIndent)
    );
    assert_eq!(
        refused("a: b\n  # note\n  c\n"),
        (3, ParseProblem::UnexpectedIndent)
    );
    // A comment on its own line after the value, then the next key.
    let v = parse("a: b\n  c\n  # note\nd: e\n").unwrap();
    assert_eq!(v.get("a"), Some(&s("b c")));
    assert_eq!(v.get("d"), Some(&s("e")));
}

/// What would start something else on a line of its own is text on a
/// continuation line: quotes, indicators and colons not followed by a space.
#[test]
fn a_continuation_line_is_text() {
    assert_eq!(value_of("a: b\n  it's \"c\"\n", "a"), s("b it's \"c\""));
    assert_eq!(value_of("a: b\n  c:d\n", "a"), s("b c:d"));
    assert_eq!(
        value_of("a: see\n  https://example.com/x\n", "a"),
        s("see https://example.com/x")
    );
    assert_eq!(value_of("a: b\n  [c] {d}\n", "a"), s("b [c] {d}"));
    assert_eq!(
        value_of("a: b\n  &c *d !e |f >g\n", "a"),
        s("b &c *d !e |f >g")
    );
    assert_eq!(value_of("a: b\n  -c\n", "a"), s("b -c"));
    // The first line's own text is unchanged: `~` and `null` start text.
    assert_eq!(value_of("a: ~\n  b\n", "a"), s("~ b"));
    assert_eq!(value_of("a: null\n  b\n", "a"), s("null b"));
    // A quoted key's plain value folds the same.
    assert_eq!(value_of("\"a b\": c\n  d\n", "a b"), s("c d"));
}

/// A deeper line that reads as a key or an item is refused, as before: YAML
/// ends a plain scalar at `: `, so `a: 1` over `   b: 2` is a key indented
/// too deep, never the text `1 b: 2`; and a dash there is most often an item
/// indented too deep, which this strict reader refuses rather than reads as
/// text.
#[test]
fn a_deeper_line_that_reads_as_a_key_or_an_item_is_still_refused() {
    assert_eq!(
        refused("a: 1\n   b: 2\n"),
        (2, ParseProblem::UnexpectedIndent)
    );
    assert_eq!(
        refused("a: 1\n   b:\n"),
        (2, ParseProblem::UnexpectedIndent)
    );
    assert_eq!(
        refused("a: 1\n   \"b\": 2\n"),
        (2, ParseProblem::UnexpectedIndent)
    );
    assert_eq!(
        refused("a: b\n  c\n  d: e\n"),
        (3, ParseProblem::UnexpectedIndent)
    );
    assert_eq!(
        refused("a: b\n  - c\n"),
        (2, ParseProblem::UnexpectedIndent)
    );
    assert_eq!(refused("- a\n  - b\n"), (2, ParseProblem::UnexpectedIndent));
    assert_eq!(refused("a: b\n  -\n"), (2, ParseProblem::UnexpectedIndent));
    // A scalar under its key, then a key at its indent: no mapping there.
    assert_eq!(
        refused("a:\n  b\n  c: d\n"),
        (3, ParseProblem::UnexpectedIndent)
    );
}

/// Only a plain value continues: a quoted one, a flow collection and a block
/// scalar end on their own terms, and a deeper line after them is refused as
/// before.
#[test]
fn only_a_plain_value_continues() {
    assert_eq!(
        refused("a: 'b'\n  c\n"),
        (2, ParseProblem::UnexpectedIndent)
    );
    assert_eq!(
        refused("a: \"b\"\n  c\n"),
        (2, ParseProblem::UnexpectedIndent)
    );
    assert_eq!(
        refused("a: [b]\n  c\n"),
        (2, ParseProblem::UnexpectedIndent)
    );
    assert_eq!(
        refused("a: {b: 1}\n  c\n"),
        (2, ParseProblem::UnexpectedIndent)
    );
    assert_eq!(value_of("a: |\n  b\n  c\n", "a"), s("b\nc\n"));
    // A key with no value on its line has a block under it.
    assert_eq!(
        value_of("a:\n  b: c\n", "a"),
        Value::Map(vec![("b".into(), s("c"))])
    );
}

/// A flow collection split across lines already reads each break as a
/// space, a plain scalar inside it included.
#[test]
fn a_plain_scalar_split_inside_a_flow_collection_folds_with_a_space() {
    assert_eq!(
        value_of("a: [b\n  c, d]\n", "a"),
        Value::Seq(vec![s("b c"), s("d")])
    );
    assert_eq!(
        value_of("a: {k: b\n  c}\n", "a"),
        Value::Map(vec![("k".into(), s("b c"))])
    );
}

/// The refusals of a line's start hold on a continuation line.
#[test]
fn a_continuation_line_keeps_the_line_refusals() {
    // A tab is no indent: at the key's column the line is refused.
    assert_eq!(refused("a: b\n\tc\n"), (2, ParseProblem::TabIndent));
    // Past the indent, a tab is space between, not text.
    assert_eq!(value_of("a: b\n  \tc\n", "a"), s("b c"));
    // Another space character cannot start a line.
    assert_eq!(
        refused("a: b\n  \u{a0}c\n"),
        (
            2,
            ParseProblem::NonAsciiIndent {
                character: '\u{a0}'
            }
        )
    );
    assert_eq!(
        refused("b\n---\n"),
        (
            2,
            ParseProblem::DocumentMarker {
                marker: "---".into()
            }
        )
    );
    assert_eq!(
        refused("b\n...\n"),
        (
            2,
            ParseProblem::DocumentMarker {
                marker: "...".into()
            }
        )
    );
    assert_eq!(
        refused("a: b\n  c\rd\n"),
        (2, ParseProblem::LoneCarriageReturn)
    );
    // CRLF line endings are line breaks, not text.
    assert_eq!(value_of("a: b\r\n  c\r\n\r\n  d\r\n", "a"), s("b c\nd"));
}

/// An edit sees a wrapped value as one: removing its item removes every
/// line of it, and the rest of the document stays as written.
#[test]
fn an_edit_takes_a_wrapped_value_whole() {
    let mut d = Document::parse("items:\n  - a long\n    value\n\n  - b\nafter: 1\n").unwrap();
    d.remove(&["items"], 0).unwrap();
    assert_eq!(d.text(), "items:\n\n  - b\nafter: 1\n");
    let mut d = Document::parse("items:\n  - a\n  - b long\n    value\nafter: 1\n").unwrap();
    d.remove(&["items"], 1).unwrap();
    assert_eq!(d.text(), "items:\n  - a\nafter: 1\n");
}
