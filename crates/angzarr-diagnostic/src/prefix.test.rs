use super::*;

/// Every message's id is prefixed; comments, blank lines and the text of
/// every value are kept byte for byte.
#[test]
fn every_message_id_is_prefixed_and_nothing_else_of_plain_messages() {
    let source = "# A comment.\n## A group: not-an-id = x\n\n\
                  too-new = the file is newer, { $found }\n\
                  gap = a gap = still text\r\n";
    assert_eq!(
        prefixed("k", source),
        "# A comment.\n## A group: not-an-id = x\n\n\
         k-too-new = the file is newer, { $found }\n\
         k-gap = a gap = still text\r\n"
    );
}

/// A reference to a message, with or without an attribute, is prefixed as
/// the message is; the attribute's name, a variable, a string literal, a
/// number and a function's name are not.
#[test]
fn a_message_reference_is_prefixed_and_nothing_else_in_a_placeable() {
    assert_eq!(
        prefixed(
            "k",
            "a = { b } and {b.hint} and { $b } and { \"b\" } and { 2.5 } and { -1 }\n"
        ),
        "k-a = { k-b } and {k-b.hint} and { $b } and { \"b\" } and { 2.5 } and { -1 }\n"
    );
    assert_eq!(
        prefixed(
            "k",
            "a = { NUMBER($n, minimumFractionDigits: 2) } { F(b, style : \"x\") }\n"
        ),
        "k-a = { NUMBER($n, minimumFractionDigits: 2) } { F(k-b, style : \"x\") }\n"
    );
    // A brace in a string literal opens nothing.
    assert_eq!(
        prefixed("k", "a = { \"{ \\\" }\" } { b }\n"),
        "k-a = { \"{ \\\" }\" } { k-b }\n"
    );
    // A placeable in a placeable.
    assert_eq!(prefixed("k", "a = { { b } }\n"), "k-a = { { k-b } }\n");
}

/// A term's id and every reference to one are prefixed too, so catalogs of
/// several crates share one bundle; a term's call names its args, which are
/// not.
#[test]
fn a_term_and_its_references_are_prefixed() {
    assert_eq!(
        prefixed(
            "k",
            "-brand = Ang\n    .gender = n\na = { -brand } { -brand.gender } { -brand(case: \"x\") }\n"
        ),
        "-k-brand = Ang\n    .gender = n\nk-a = { -k-brand } { -k-brand.gender } { -k-brand(case: \"x\") }\n"
    );
}

/// In a select, the variant keys and each variant's text are kept; a
/// reference in a variant's placeable is prefixed; the select may close at
/// the margin; and the next entry's id is prefixed.
#[test]
fn a_select_keeps_its_keys_and_text_and_prefixes_its_references() {
    let source = "conflict =\n    stream { $stream } holds { $actual ->\n        \
                  [one] one event { other }\n       *[other] { $actual } events\n    \
                  }, not { $expected }\n    .hint = see { help }\n\
                  nested = { $a ->\n    [x] { $b ->\n        [y] { deep }\n       \
                  *[z] z\n    }\n   *[w] w\n}\nafter = { other }\n";
    assert_eq!(
        prefixed("k", source),
        "k-conflict =\n    stream { $stream } holds { $actual ->\n        \
         [one] one event { k-other }\n       *[other] { $actual } events\n    \
         }, not { $expected }\n    .hint = see { k-help }\n\
         k-nested = { $a ->\n    [x] { $b ->\n        [y] { k-deep }\n       \
         *[z] z\n    }\n   *[w] w\n}\nk-after = { k-other }\n"
    );
}

/// An indented line, a blank line between them, continues its entry: its
/// words are text even where they read as `id = value`.
#[test]
fn a_continuation_is_text_even_after_a_blank_line() {
    assert_eq!(
        prefixed("k", "a =\n    one\n\n    b = two { c }\nd = e\n"),
        "k-a =\n    one\n\n    b = two { k-c }\nk-d = e\n"
    );
}

/// What is no entry is kept as it is, and the next entry is still read.
#[test]
fn a_line_that_is_no_entry_is_kept() {
    assert_eq!(
        prefixed("k", "not an entry\n= x\na = b\n"),
        "not an entry\n= x\nk-a = b\n"
    );
    assert_eq!(prefixed("k", ""), "");
    assert_eq!(prefixed("k", "a = no newline"), "k-a = no newline");
}

/// Every Fluent construct, held to Fluent's own parser: the prefixed
/// catalog parses, and is the source with exactly its message and term ids,
/// and the references to them, prefixed.
#[test]
fn fluents_parser_reads_the_source_with_every_id_and_reference_prefixed() {
    use fluent_syntax::ast;
    use fluent_syntax::parser::parse;
    use fluent_syntax::serializer::serialize;

    fn id(i: &mut ast::Identifier<String>) {
        i.name = format!("k-{}", i.name);
    }
    fn pattern(p: &mut ast::Pattern<String>) {
        for e in &mut p.elements {
            if let ast::PatternElement::Placeable { expression } = e {
                expr(expression);
            }
        }
    }
    fn expr(e: &mut ast::Expression<String>) {
        match e {
            ast::Expression::Inline(i) => inline(i),
            ast::Expression::Select { selector, variants } => {
                inline(selector);
                for v in variants {
                    pattern(&mut v.value);
                }
            }
        }
    }
    fn call(a: &mut ast::CallArguments<String>) {
        a.positional.iter_mut().for_each(inline);
        a.named.iter_mut().for_each(|n| inline(&mut n.value));
    }
    fn inline(i: &mut ast::InlineExpression<String>) {
        match i {
            ast::InlineExpression::MessageReference { id: i, .. } => id(i),
            ast::InlineExpression::TermReference {
                id: i, arguments, ..
            } => {
                id(i);
                arguments.iter_mut().for_each(call);
            }
            ast::InlineExpression::FunctionReference { arguments, .. } => call(arguments),
            ast::InlineExpression::Placeable { expression } => expr(expression),
            _ => {}
        }
    }
    fn entries(r: &mut ast::Resource<String>) {
        for e in &mut r.body {
            let (i, value, attributes) = match e {
                ast::Entry::Message(m) => (&mut m.id, m.value.as_mut(), &mut m.attributes),
                ast::Entry::Term(t) => (&mut t.id, Some(&mut t.value), &mut t.attributes),
                _ => continue,
            };
            id(i);
            value.into_iter().for_each(pattern);
            attributes.iter_mut().for_each(|a| pattern(&mut a.value));
        }
    }

    let source = "### Resource comment\n\n# Message comment\n\
        plain = text, = kept\n\
        refs = { other } {other.attr} { -term } { -term(case: \"gen\") }\n\
        vars = { $v } { \"lit { b }\" } { 12 } { -3.5 }\n\
        call = { NUMBER($n, minimumFractionDigits: 2) } { DATETIME(other, month: \"long\") }\n\
        select = { $n ->\n    [one] one { other }\n    [few] { -term.attr ->\n        \
        [a] a { other.attr }\n       *[b] b\n    }\n   *[many] many\n}\n\
        attrs =\n    .label = { other }\n    .hint = see { $v }\n\
        multi =\n    first\n\n    second { other }\n\
        -term = Term { other }\n    .attr = t\n\
        other = o\n    .attr = { -term }\n";
    let (mut expected, errors) = match parse(source.to_string()) {
        Ok(r) => (r, vec![]),
        Err((r, e)) => (r, e),
    };
    assert!(errors.is_empty(), "the corpus is Fluent: {errors:?}");
    entries(&mut expected);
    let output = prefixed("k", source);
    let actual = parse(output.clone()).unwrap_or_else(|(_, e)| panic!("{output}\n{e:?}"));
    assert_eq!(serialize(&actual), serialize(&expected), "{output}");
}
