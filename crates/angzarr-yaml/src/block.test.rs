//! Block scalars (`|`, `|-`, `|+`, `>`, `>-`, `>+`): read as YAML 1.2 reads
//! them, written for multi-line strings, and edited in place, every written
//! document reading back as the value written.

use crate::edit::{Document, Seg};
use crate::{parse, write, ParseProblem, Value};

fn s(x: &str) -> Value {
    Value::scalar(x)
}

fn value_of(src: &str, key: &str) -> Value {
    parse(src)
        .unwrap_or_else(|e| panic!("{src:?}: {e}"))
        .get(key)
        .cloned()
        .unwrap_or_else(|| panic!("{src:?} has no {key}"))
}

#[test]
fn literal_keeps_its_lines_and_chomps_as_its_header_says() {
    let body = "  one\n   two\n\n  # not a comment: 'nor' \"a quote\"\n\n\nnext: x\n";
    assert_eq!(
        value_of(&format!("a: |\n{body}"), "a"),
        s("one\n two\n\n# not a comment: 'nor' \"a quote\"\n")
    );
    assert_eq!(
        value_of(&format!("a: |-\n{body}"), "a"),
        s("one\n two\n\n# not a comment: 'nor' \"a quote\"")
    );
    assert_eq!(
        value_of(&format!("a: |+\n{body}"), "a"),
        s("one\n two\n\n# not a comment: 'nor' \"a quote\"\n\n\n")
    );
    // What follows is read as ever.
    assert_eq!(value_of(&format!("a: |\n{body}"), "next"), s("x"));
}

#[test]
fn folded_joins_lines_except_around_empty_and_more_indented_ones() {
    let body = "  one\n  two\n\n  three\n    indented\n  four\n\n\n  five\n\nnext: x\n";
    assert_eq!(
        value_of(&format!("a: >\n{body}"), "a"),
        s("one two\nthree\n  indented\nfour\n\nfive\n")
    );
    assert_eq!(
        value_of(&format!("a: >-\n{body}"), "a"),
        s("one two\nthree\n  indented\nfour\n\nfive")
    );
    assert_eq!(
        value_of(&format!("a: >+\n{body}"), "a"),
        s("one two\nthree\n  indented\nfour\n\nfive\n\n")
    );
    // Leading empty lines are line breaks.
    assert_eq!(value_of("a: >\n\n  x\n  y\n", "a"), s("\nx y\n"));
    assert_eq!(value_of("a: |\n\n  x\n", "a"), s("\nx\n"));
}

#[test]
fn a_block_scalar_sits_in_mappings_sequences_and_items() {
    let src = "\
top: |   # the header may have a comment
  line one
  line two
list:
  - |-
    in a list
  - plain
  - key: >-
      folded
      here
    other: 1
nested:
  deeper: |
    x
  after: y
";
    let v = parse(src).unwrap();
    assert_eq!(v.get("top"), Some(&s("line one\nline two\n")));
    let list = v.get("list").unwrap().as_seq().unwrap();
    assert_eq!(list[0], s("in a list"));
    assert_eq!(list[1], s("plain"));
    assert_eq!(list[2].get("key"), Some(&s("folded here")));
    assert_eq!(list[2].get("other"), Some(&s("1")));
    let nested = v.get("nested").unwrap();
    assert_eq!(nested.get("deeper"), Some(&s("x\n")));
    assert_eq!(nested.get("after"), Some(&s("y")));
    // A sequence at the key's indent, of block scalars.
    let v = parse("k:\n- |\n  a\n- >-\n  b\n  c\n").unwrap();
    assert_eq!(v.get("k"), Some(&Value::Seq(vec![s("a\n"), s("b c")])));
    // An item's key is the column its block scalar's content must pass.
    assert_eq!(
        parse("- k: |\n   x\n").unwrap(),
        Value::Seq(vec![Value::Map(vec![("k".into(), s("x\n"))])])
    );
    assert_eq!(
        parse("- k: |\n  x\n").unwrap_err().problem,
        ParseProblem::ExpectedKeyValue { found: "x".into() }
    );
    // A sequence in a sequence.
    let v = parse("- - |\n    deep\n").unwrap();
    assert_eq!(v, Value::Seq(vec![Value::Seq(vec![s("deep\n")])]));
}

#[test]
fn the_edges_of_a_block_scalar() {
    // Nothing under it: empty.
    assert_eq!(value_of("a: |\nb: 1\n", "a"), s(""));
    assert_eq!(value_of("a: |+\n\nb: 1\n", "a"), s("\n"));
    assert_eq!(value_of("a: >\n", "a"), s(""));
    // The end of the file, with or without a final line break.
    assert_eq!(value_of("a: |\n  x", "a"), s("x\n"));
    assert_eq!(value_of("a: |-\n  x\n", "a"), s("x"));
    // Content must be deeper than its key; at the key's indent it is the
    // next key.
    let v = parse("a:\n  b: |\n  c: 1\n").unwrap();
    assert_eq!(v.get("a").unwrap().get("b"), Some(&s("")));
    assert_eq!(v.get("a").unwrap().get("c"), Some(&s("1")));
    // A line of spaces only, deeper than the content, keeps the rest.
    assert_eq!(value_of("a: |\n  x\n     \n  y\n", "a"), s("x\n   \ny\n"));
    // Less indented than the first content line ends the scalar.
    let e = parse("a: |\n    x\n  y\n").unwrap_err();
    assert_eq!(e.problem, ParseProblem::UnexpectedIndent);
    assert_eq!(e.line, 3);
    // A carriage return alone is refused inside a block scalar too.
    let e = parse("a: |\n  x\ry\n").unwrap_err();
    assert_eq!((e.line, e.problem), (2, ParseProblem::LoneCarriageReturn));
    // CRLF line endings are line breaks, not content.
    assert_eq!(value_of("a: |\r\n  x\r\n  y\r\n", "a"), s("x\ny\n"));
}

#[test]
fn headers_and_places_it_does_not_take_are_refused() {
    for src in [
        "a: |2\n  x\n",
        "a: >1-\n x\n",
        "a: |x\n",
        "|\n  x\n",
        "a:\n  |\n  x\n",
    ] {
        let e = parse(src).unwrap_err();
        assert_eq!(e.problem, ParseProblem::BlockScalar, "{src:?}");
    }
    // A `|` that is text is still text.
    assert_eq!(value_of("a: \"|\"\n", "a"), s("|"));
    assert_eq!(value_of("a: x |\n", "a"), s("x |"));
}

#[test]
fn a_multi_line_string_is_written_as_a_literal_block() {
    let doc = |v: &str| Value::Map(vec![("a".into(), s(v)), ("b".into(), s("1"))]);
    assert_eq!(write(&doc("x\ny"), 1), "a: |-\n  x\n  y\nb: 1\n");
    assert_eq!(write(&doc("x\ny\n"), 1), "a: |\n  x\n  y\nb: 1\n");
    assert_eq!(write(&doc("x\n\n\n"), 1), "a: |+\n  x\n\n\nb: 1\n");
    assert_eq!(write(&doc("\nx\n  y"), 1), "a: |-\n\n  x\n    y\nb: 1\n");
    // In a sequence, and as an item's first key.
    let seq = Value::Seq(vec![s("p\nq"), Value::Map(vec![("k".into(), s("r\n"))])]);
    assert_eq!(write(&seq, 9), "- |-\n  p\n  q\n- k: |\n    r\n");
    // What a literal block cannot hold is quoted: a first line starting
    // with a space, a tab or other escaped character, nothing but breaks.
    for quoted in [" x\ny", "x\ty\nz", "\n\n", "x\r\ny"] {
        let written = write(&doc(quoted), 1);
        assert!(written.starts_with("a: \""), "{quoted:?}: {written}");
    }
    // Inside a flow collection, quoted too.
    assert_eq!(
        write(
            &Value::Map(vec![("k".into(), Value::Seq(vec![s("x\ny")]))]),
            1
        ),
        "k: [\"x\\ny\"]\n"
    );
}

#[test]
fn every_written_block_scalar_reads_back() {
    let texts = [
        "x\ny",
        "x\ny\n",
        "x\n\n\n",
        "\nx",
        "\n\nx\n\n",
        "a\n  b\n c\n",
        "a\n\n  \nb",
        "# hash\n'quote'\n\"dq\" : colon\n- dash\n",
        "trailing space  \nx",
        "---\n...\n",
        "\u{e9}p\u{e9}e\n\u{5263}",
    ];
    for t in texts {
        for v in [
            Value::Map(vec![("a".into(), s(t)), ("b".into(), s(t))]),
            Value::Seq(vec![s(t), s(t)]),
            Value::Map(vec![(
                "a".into(),
                Value::Seq(vec![Value::Map(vec![("k".into(), s(t))])]),
            )]),
        ] {
            for depth in 0..4 {
                let written = write(&v, depth);
                assert_eq!(
                    parse(&written).as_ref(),
                    Ok(&v),
                    "{t:?} at {depth}:\n{written}"
                );
            }
        }
    }
}

#[test]
fn edits_keep_block_scalars_whole() {
    let src = "\
# notes
a: |   # keep me
  one
  two

b: 1
list:
  - |-
    item
  - z
";
    // Setting another key leaves the block as written.
    let mut d = Document::parse(src).unwrap();
    d.set(&[], "b", &s("2"), 2).unwrap();
    assert_eq!(d.text(), src.replace("b: 1", "b: 2"));
    // Setting the block's own key rewrites its lines, keeping the comment.
    let mut d = Document::parse(src).unwrap();
    d.set(&[], "a", &s("three\nfour"), 2).unwrap();
    assert_eq!(
        d.text(),
        src.replace(
            "a: |   # keep me\n  one\n  two\n",
            "a: |-   # keep me\n  three\n  four\n"
        )
    );
    assert_eq!(parse(&d.text()).unwrap().get("a"), Some(&s("three\nfour")));
    // Removing it takes its lines with it.
    let mut d = Document::parse(src).unwrap();
    d.remove_key(&[], "a").unwrap();
    assert_eq!(parse(&d.text()).unwrap().get("a"), None);
    assert!(!d.text().contains("one"), "{}", d.text());
    assert_eq!(parse(&d.text()).unwrap().get("b"), Some(&s("1")));
    // A new key goes after the block, not inside it.
    let mut d = Document::parse("a: |\n  x\n").unwrap();
    d.set(&[], "c", &s("y"), 2).unwrap();
    assert_eq!(d.text(), "a: |\n  x\nc: y\n");
    // Sequence items: replace, remove and push around a block item.
    let mut d = Document::parse(src).unwrap();
    d.replace(&["list"], 0, &s("new\nitem\n"), 2).unwrap();
    assert_eq!(
        parse(&d.text()).unwrap().get("list"),
        Some(&Value::Seq(vec![s("new\nitem\n"), s("z")]))
    );
    let mut d = Document::parse(src).unwrap();
    d.remove(&["list"], 0).unwrap();
    assert_eq!(
        parse(&d.text()).unwrap().get("list"),
        Some(&Value::Seq(vec![s("z")]))
    );
    let mut d = Document::parse("list:\n  - |\n    x\n").unwrap();
    d.push(&["list"], &s("y"), 2).unwrap();
    assert_eq!(d.text(), "list:\n  - |\n    x\n  - y\n");
    // A whole update, and a rename.
    let mut d = Document::parse(src).unwrap();
    let mut target = d.value().clone();
    *target.get_mut("a").unwrap() = s("changed\n");
    d.update_to(&target, 2).unwrap();
    assert!(
        d.text()
            .starts_with("# notes\na: |   # keep me\n  changed\n"),
        "{}",
        d.text()
    );
    let mut d = Document::parse(src).unwrap();
    d.rename_key(&[], "a", "notes").unwrap();
    assert_eq!(d.text(), src.replace("a: |", "notes: |"));
    // A path through a block scalar is no mapping.
    let mut d = Document::parse(src).unwrap();
    assert!(d.set(&[Seg::Key("a")], "x", &s("1"), 2).is_err());
    assert!(d.push(&["a"], &s("1"), 2).is_err());
}
