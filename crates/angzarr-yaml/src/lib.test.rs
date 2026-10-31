use super::*;

fn s(x: &str) -> Value {
    Value::scalar(x)
}

#[test]
fn reads_block_mappings_and_sequences() {
    let v = parse(
            "# a comment\nname: helmet   # trailing\nwards:\n  - Concussion\n  - \"Cracked: Rib\"\nwounds:\n- absorb: Helmet Deflects\n  depth: 250\n- flesh: Helmet Rings\n  depth: 200\nempty:\n",
        )
        .unwrap();
    assert_eq!(v.get("name"), Some(&s("helmet")));
    assert_eq!(
        v.get("wards"),
        Some(&Value::Seq(vec![s("Concussion"), s("Cracked: Rib")]))
    );
    let wounds = v.get("wounds").unwrap().as_seq().unwrap();
    assert_eq!(wounds.len(), 2);
    assert_eq!(wounds[0].get("depth"), Some(&s("250")));
    assert_eq!(wounds[1].get("flesh"), Some(&s("Helmet Rings")));
    assert_eq!(v.get("empty"), Some(&Value::Null));
}

#[test]
fn reads_flow_collections() {
    let v = parse("modes: [Attack (Snap), Attack (Aimed)]\nstat: {reflex: 3, will: 1}\nnone: []\n")
        .unwrap();
    assert_eq!(
        v.get("modes"),
        Some(&Value::Seq(vec![s("Attack (Snap)"), s("Attack (Aimed)")]))
    );
    assert_eq!(v.get("stat").unwrap().get("will"), Some(&s("1")));
    assert_eq!(v.get("none"), Some(&Value::Seq(vec![])));
}

#[test]
fn scalars_keep_their_text() {
    let v = parse("a: no\nb: 1.10\nc: 'it''s'\nd: .45 ACP\ne: -1/4\n").unwrap();
    assert_eq!(v.get("a"), Some(&s("no")));
    assert_eq!(v.get("b"), Some(&s("1.10")));
    assert_eq!(v.get("c"), Some(&s("it's")));
    assert_eq!(v.get("d"), Some(&s(".45 ACP")));
    assert_eq!(v.get("e"), Some(&s("-1/4")));
}

#[test]
fn refuses_what_it_does_not_support() {
    let found = |s: &str| s.to_string();
    for (src, line, problem) in [
        ("a: &x 1\n", 1, ParseProblem::Anchor),
        ("a: *x\n", 1, ParseProblem::Alias),
        ("- !tag x\n", 1, ParseProblem::Tag),
        ("a: |2\n  text\n", 1, ParseProblem::BlockScalar),
        ("|\n  text\n", 1, ParseProblem::BlockScalar),
        ("a:\n\t- x\n", 2, ParseProblem::TabIndent),
        (
            "---\na: 1\n",
            1,
            ParseProblem::DocumentMarker {
                marker: found("---"),
            },
        ),
        (
            "a: 1\na: 2\n",
            2,
            ParseProblem::DuplicateKey { key: found("a") },
        ),
        (
            "a: {b: 1, b: 2}\n",
            1,
            ParseProblem::DuplicateKey { key: found("b") },
        ),
        ("a: [1, 2\n", 1, ParseProblem::ExpectedSeqSeparator),
        ("a: {b 1}\n", 1, ParseProblem::ExpectedColon),
        ("a: {b: 1\n", 1, ParseProblem::ExpectedMapSeparator),
        ("a: {: 1}\n", 1, ParseProblem::KeyNotScalar),
        ("a: 1\n   b: 2\n", 2, ParseProblem::UnexpectedIndent),
        (
            "a: 1\nb\n",
            2,
            ParseProblem::ExpectedKeyValue { found: found("b") },
        ),
        (
            "a: [1] x\n",
            1,
            ParseProblem::Unexpected { found: found("x") },
        ),
        ("a: \"x\n", 1, ParseProblem::UnterminatedQuote),
    ] {
        assert_eq!(parse(src), Err(Error { line, problem }), "{src:?}");
    }
    assert_eq!(
        parse("a: 1\na: 2\n").unwrap_err().to_string(),
        "angzarr-yaml-duplicate-key line=2 key=a"
    );
    assert_eq!(
        parse("a: &x 1\n").unwrap_err().to_string(),
        "angzarr-yaml-anchor line=1"
    );
}

#[test]
fn writes_what_it_reads() {
    let src = "items:\n  - name: helmet\n    wards: [Concussion, \"Cracked, Rib\"]\n    wounds:\n      - {absorb: Helmet Deflects, depth: 250, soak: 3}\n  - name: \"key: value\"\n    deltas:\n      stat: {reflex: 3}\n";
    let v = parse(src).unwrap();
    for depth in 0..5 {
        let written = write(&v, depth);
        assert_eq!(
            parse(&written).unwrap(),
            v,
            "flow depth {depth}:\n{written}"
        );
    }
}

#[test]
fn a_leading_bom_is_not_part_of_the_first_key() {
    let v = parse("\u{feff}version: 3\nname: x\n").unwrap();
    assert_eq!(v.get("version"), Some(&s("3")));
    let seq = parse("\u{feff}- a\n- b\n").unwrap();
    assert_eq!(seq, Value::Seq(vec![s("a"), s("b")]));
    // Only one: a second mark is content.
    let two = parse("\u{feff}\u{feff}x: 1\n").unwrap();
    assert_eq!(two.as_map().unwrap()[0].0, "\u{feff}x");
}

#[test]
fn only_a_space_or_tab_before_hash_starts_a_comment() {
    assert_eq!(
        parse("name: a\u{a0}#b\n").unwrap().get("name"),
        Some(&s("a\u{a0}#b"))
    );
    assert_eq!(
        parse("name: a\u{3000}#b\n").unwrap().get("name"),
        Some(&s("a\u{3000}#b"))
    );
    assert_eq!(parse("name: a\t#b\n").unwrap().get("name"), Some(&s("a")));
    assert_eq!(parse("name: a #b\n").unwrap().get("name"), Some(&s("a")));
}

#[test]
fn a_quote_opens_only_at_the_start_of_a_scalar() {
    assert_eq!(
        parse("name: l'\u{e9}p\u{e9}e # the sword\n")
            .unwrap()
            .get("name"),
        Some(&s("l'\u{e9}p\u{e9}e"))
    );
    assert_eq!(
        parse("name: 5\"2 # tall\n").unwrap().get("name"),
        Some(&s("5\"2"))
    );
    assert_eq!(
        parse("name: 'it''s # in' # out\n").unwrap().get("name"),
        Some(&s("it's # in"))
    );
    assert_eq!(
        parse("name: \"a\\\" # in\" # out\n").unwrap().get("name"),
        Some(&s("a\" # in"))
    );
    assert_eq!(
        parse("v: [l'a, 'b # c'] # d\n").unwrap().get("v"),
        Some(&Value::Seq(vec![s("l'a"), s("b # c")]))
    );
    // A quote after `,` or `[` opens only inside a flow collection, and one
    // closes when its `]` does.
    assert_eq!(
        parse("name: b,'c # d'\n").unwrap().get("name"),
        Some(&s("b,'c"))
    );
    assert_eq!(
        parse("name: x[y,'b # c'\n").unwrap().get("name"),
        Some(&s("x[y,'b"))
    );
    assert_eq!(
        parse("name: [b],'c # d'\n").unwrap_err().problem,
        ParseProblem::Unexpected {
            found: ",'c".into()
        }
    );
    // A backslash escapes nothing inside single quotes.
    assert_eq!(
        parse("name: 'x\\' # c\n").unwrap().get("name"),
        Some(&s("x\\"))
    );
    // A quoted key ends at its closing quote, escapes and all.
    let keys = parse("\"a\\\": b\": 1\n'it''s: x' : 2\na]b: 3\n").unwrap();
    assert_eq!(keys.get("a\": b"), Some(&s("1")));
    assert_eq!(keys.get("it's: x"), Some(&s("2")));
    assert_eq!(keys.get("a]b"), Some(&s("3")));
}

#[test]
fn indentation_is_ascii_spaces_only() {
    for src in [
        "a:\n\u{a0}\u{a0}b: 1\n",
        "a:\n\u{3000}b: 1\n",
        "a:\n  \u{a0}b: 1\n",
        "\u{a0}\n",
    ] {
        let e = parse(src).unwrap_err();
        assert!(
            matches!(e.problem, ParseProblem::NonAsciiIndent { .. }),
            "{src:?}: {e}"
        );
        assert_eq!(
            e.line,
            if src.starts_with('\u{a0}') { 1 } else { 2 },
            "{src:?}"
        );
    }
    // Not whitespace to the grammar: content, kept.
    assert_eq!(
        parse("a: \u{a0}x\u{a0}\n").unwrap().get("a"),
        Some(&s("\u{a0}x\u{a0}"))
    );
    assert_eq!(
        parse("a: {k: \u{3000}x}\n")
            .unwrap()
            .get("a")
            .unwrap()
            .get("k"),
        Some(&s("\u{3000}x"))
    );
    assert_eq!(
        parse("a:\u{a0}b: 1\n").unwrap().get("a:\u{a0}b"),
        Some(&s("1"))
    );
    assert_eq!(parse("a:\tb\n").unwrap().get("a"), Some(&s("b")));
}

#[test]
fn line_breaks_are_lf_or_crlf_and_a_lone_cr_is_refused() {
    let crlf = parse("a: 1\r\nb:\r\n  - x\r\n  - \"y\"\r\n").unwrap();
    assert_eq!(crlf, parse("a: 1\nb:\n  - x\n  - \"y\"\n").unwrap());
    for (src, line) in [
        ("a: 1\rb: 2\r", 1),
        ("a: 1\nb: x\ry\n", 2),
        ("a: \"x\ry\"", 1),
        ("a: 1\r", 1),
    ] {
        let e = parse(src).unwrap_err();
        assert_eq!(e.line, line, "{src:?}");
        assert_eq!(e.problem, ParseProblem::LoneCarriageReturn, "{src:?}: {e}");
    }
}

/// `^` stands for a backslash, so the escapes under test stay readable.
fn bs(s: &str) -> String {
    s.replace('^', "\\")
}

#[test]
fn double_quotes_take_unicode_cr_and_nul_escapes() {
    let v = parse(&bs(r#"a: "^u00e9^r^0^t^n^^^"^U0001F5E1""#)).unwrap();
    assert_eq!(v.get("a"), Some(&s("\u{e9}\r\0\t\n\\\"\u{1f5e1}")));
    assert_eq!(
        parse(&bs(r#"a: "^u00E9""#)).unwrap().get("a"),
        Some(&s("\u{e9}"))
    );
    for bad in [
        "^uD83D^uDDE1",
        "^uDDE1",
        "^u00e",
        "^U00110000",
        "^x41",
        "^q",
        "^u+0e9",
    ] {
        let e = parse(&bs(&format!("a: \"{bad}\"\n"))).unwrap_err();
        assert!(
            matches!(
                e.problem,
                ParseProblem::BadUnicodeEscape { .. } | ParseProblem::UnknownEscape { .. }
            ),
            "{bad}: {e}"
        );
    }
    let e = parse(&bs(r#"a: "^q""#)).unwrap_err();
    assert_eq!(e.problem, ParseProblem::UnknownEscape { escape: bs("^q") });
    assert_eq!(
        e.to_string(),
        bs(r#"angzarr-yaml-unknown-escape line=1 escape="^^q""#)
    );
    let e = parse(&bs(r#"a: "^uDDE1""#)).unwrap_err();
    assert_eq!(
        e.problem,
        ParseProblem::BadUnicodeEscape {
            escape: bs("^uDDE1")
        }
    );
}

fn round_trips(v: &Value) {
    for depth in 0..4 {
        let written = write(v, depth);
        assert_eq!(
            parse(&written).as_ref(),
            Ok(v),
            "flow depth {depth}:\n{written}"
        );
    }
}

#[test]
fn keys_with_brackets_or_colons_are_quoted() {
    for key in [
        "a[1",
        "a{1",
        "a]b",
        "a}b",
        "x:y",
        "x:",
        "a,b",
        "a\t#b",
        "a #b",
        "a\u{a0}#b",
    ] {
        let flat = Value::Map(vec![(key.to_string(), s("v"))]);
        round_trips(&flat);
        round_trips(&Value::Map(vec![("k".into(), flat.clone())]));
        round_trips(&Value::Seq(vec![flat]));
    }
}

#[test]
fn control_characters_are_escaped() {
    for text in [
        "a\u{0}b",
        "\u{1b}[31m",
        "a\rb",
        "a\u{85}b",
        "a\u{2028}b",
        "\u{feff}bom",
        "tab\there",
        "a\u{7f}",
        "a\u{9c}",
    ] {
        let v = Value::Map(vec![
            (text.to_string(), s(text)),
            ("seq".into(), Value::Seq(vec![s(text)])),
        ]);
        round_trips(&v);
        let written = write(&v, 0);
        assert!(
            !written.chars().any(|c| c.is_control() && c != '\n'),
            "{written:?}"
        );
    }
}

#[test]
fn keys_compare_by_bytes_and_ascii_ids_have_one_spelling() {
    let v = parse("\u{e9}: 1\ne\u{301}: 2\n").unwrap();
    assert_eq!(v.get("\u{e9}"), Some(&s("1")));
    assert_eq!(v.get("e\u{301}"), Some(&s("2")));
    for id in ["sword", "Iron_Sword-2", "units.club", "a"] {
        assert!(is_ascii_id(id), "{id}");
    }
    for id in [
        "",
        "\u{e9}",
        "e\u{301}",
        "\u{e9}p\u{e9}e",
        "a b",
        "a:b",
        "\u{ff53}word",
        "a\u{200b}",
    ] {
        assert!(!is_ascii_id(id), "{id:?}");
    }
}

#[test]
fn merge_lays_a_patch_over() {
    let mut v = parse("profile: {power: 4, aim_ms: 600}\nwards: [A]\n").unwrap();
    v.merge(&parse("profile: {power: 8}\nwards: [B]\nweight: 2\n").unwrap());
    assert_eq!(v.get("profile").unwrap().get("power"), Some(&s("8")));
    assert_eq!(v.get("profile").unwrap().get("aim_ms"), Some(&s("600")));
    assert_eq!(v.get("wards"), Some(&Value::Seq(vec![s("A"), s("B")])));
    assert_eq!(v.get("weight"), Some(&s("2")));
}

mod roundtrip {
    //! The round-trip property: whatever the writer writes, the reader reads back
    //! as the same value, at every flow depth, for any text. Hand-picked strings
    //! cover each hazard by name (scripts, combining marks, odd spaces, line and
    //! control characters, YAML indicators); seeded random strings over a mixed
    //! alphabet cover their combinations.

    // The corpus is written as the characters themselves, not escapes: what
    // each string tests should be visible at a glance. Characters that do not
    // show (controls, odd spaces, combining marks) stay escaped, and
    // invisible_characters and unicode_not_nfc stay denied here.
    #![allow(clippy::non_ascii_literal)]

    use crate::rng::Rng;
    use crate::{parse, write, Value};

    /// Hand-picked text: every kind of character the writer must get right.
    const CORPUS: &[&str] = &[
        // Plain words, accents, scripts, emoji.
        "helmet",
        "épée",
        "naïve café",
        "Ærøskøbing",
        "日本語",
        "剣: 鋼",
        "剣：鋼",
        "한국어",
        "Привет",
        "Ελληνικά",
        "🗡️ sword",
        "👨\u{200d}👩\u{200d}👧",
        "🏳️\u{200d}🌈",
        "👍🏽",
        "🇫🇷",
        // Right to left, and bidi controls.
        "سيف",
        "חרב",
        "שלום עולם",
        "مرحبا: عالم",
        "\u{200f}rtl-mark",
        "a\u{202e}b",
        "\u{200e}",
        // Combining marks, NFC against NFD.
        "\u{e9}",
        "e\u{301}",
        "\u{301}lead",
        "a\u{301}\u{302}\u{303}",
        // Spaces that are not grammar whitespace, and ones that are.
        "a\u{a0}#b",
        "a\u{3000}#b",
        "\u{a0}lead",
        "trail\u{a0}",
        "\u{3000}ideo",
        "\u{a0}",
        "\u{3000}",
        "a\u{2009}b",
        " lead",
        "trail ",
        " ",
        "  ",
        "a  b",
        "a\t#b",
        "tab\there",
        "\t",
        "a\u{200b}b",
        // Line breaks and control characters.
        "a\nb",
        "\n",
        "a\r\nb",
        "a\rb",
        "a\r",
        "a\u{2028}b",
        "a\u{2029}b",
        "a\u{85}b",
        "a\u{0}b",
        "\u{0}",
        "\u{1b}[31m",
        "a\u{7f}",
        "a\u{9c}",
        "\u{feff}bom",
        "a\u{feff}",
        "\u{b}\u{c}",
        // Quotes and backslashes.
        "l'épée",
        "it's",
        "'",
        "''",
        "\"",
        "\"quoted\"",
        "'single'",
        "a \"b\" c",
        "a 'b",
        "5'2\"",
        "\\",
        "a\\nb",
        "\\u00e9",
        "a,'b",
        "[\"x",
        "{'y",
        "x' #y",
        "x\" #y",
        "a [b, 'c",
        "a [b,'c # d",
        // Indicators, anywhere.
        "#",
        "#é",
        "é #x",
        "a#b",
        ":",
        "x:",
        "x:y",
        ":x",
        "a: b",
        "a:\u{a0}b",
        "a:\tb",
        "?",
        "?é",
        "-",
        "- é",
        "-é",
        "--",
        "---",
        "...",
        "- ",
        "-1/4",
        ",",
        "a,b",
        "[",
        "]",
        "a[1",
        "a]b",
        "{",
        "}",
        "a{1",
        "a}b",
        "[a, b]",
        "{a: 1}",
        "&a",
        "*a",
        "!tag",
        "|",
        ">",
        "%é",
        "@x",
        "`x`",
        "~",
        "null",
        "Null",
        "no",
        "1.10",
        ".45 ACP",
        // Empty.
        "",
    ];

    /// Characters the random strings draw from: mostly hazards.
    const ALPHABET: &[char] = &[
        'a', 'b', 'z', 'A', '0', '9', ' ', ' ', ' ', '\t', '\n', '\r', '\0', '\u{1b}', '\u{7f}',
        '\u{85}', '\u{9c}', '#', ':', ',', '[', ']', '{', '}', '-', '?', '&', '*', '!', '|', '>',
        '%', '@', '`', '~', '\'', '"', '\\', '.', '\u{a0}', '\u{2009}', '\u{3000}', '\u{2028}',
        '\u{2029}', '\u{feff}', '\u{200b}', '\u{200d}', '\u{200e}', '\u{200f}', '\u{202e}',
        '\u{301}', '\u{fe0f}', 'é', 'ß', '日', '剣', '：', 'س', 'ח', '🗡', '👨', '🇫',
    ];

    /// Words that mean something to the grammar on their own.
    const WORDS: &[&str] = &[
        "", "null", "~", "-", "- x", "---", "...", "#", ":", "[]", "{}", "'", "\"",
    ];

    /// Random documents per run: seeds `0..SEEDS`.
    const SEEDS: u64 = 5_000;

    fn round_trip(v: &Value, what: &str) {
        for depth in 0..4 {
            let written = write(v, depth);
            match parse(&written) {
                Ok(back) => assert_eq!(&back, v, "{what}, flow depth {depth}, wrote:\n{written}"),
                Err(e) => panic!("{what}, flow depth {depth}: {e}; wrote:\n{written}"),
            }
        }
    }

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

    /// Every place a scalar can sit: block and flow, key and value, root, nested.
    fn placements(t: &str) -> Vec<Value> {
        vec![
            s(t),
            map(&[(t, s("v"))]),
            map(&[("k", s(t))]),
            map(&[(t, s(t)), ("after", s("z"))]),
            map(&[("k", Value::Seq(vec![s(t), s("z")]))]),
            map(&[("k", map(&[(t, s(t)), ("z", s("1"))]))]),
            map(&[(t, map(&[(t, Value::Seq(vec![s(t)]))]))]),
            map(&[(t, Value::Null), ("k", Value::Seq(vec![]))]),
            Value::Seq(vec![s(t), Value::Null]),
            Value::Seq(vec![Value::Seq(vec![s(t)]), map(&[(t, s(t)), ("b", s(t))])]),
            Value::Seq(vec![map(&[(t, map(&[("x", s(t))]))])]),
        ]
    }

    #[test]
    fn every_corpus_string_round_trips_in_every_place() {
        for t in CORPUS {
            for (i, v) in placements(t).iter().enumerate() {
                round_trip(v, &format!("{t:?} in placement {i}"));
            }
        }
    }

    #[test]
    fn every_corpus_pair_round_trips_as_key_and_value() {
        for a in CORPUS {
            let keys = |skip: Option<&str>| {
                CORPUS
                    .iter()
                    .filter(|b| Some(**b) != skip)
                    .map(|b| (b.to_string(), s(a)))
                    .collect::<Vec<_>>()
            };
            round_trip(
                &Value::Map(keys(None)),
                &format!("block keys with value {a:?}"),
            );
            round_trip(
                &map(&[("k", Value::Map(keys(None)))]),
                &format!("flow keys with value {a:?}"),
            );
            let items = Value::Seq(CORPUS.iter().map(|b| s(b)).collect());
            round_trip(
                &map(&[(a, items)]),
                &format!("every string in a sequence under {a:?}"),
            );
        }
    }

    fn pick<T: Copy>(rng: &mut Rng, from: &[T]) -> T {
        from[rng.below(from.len() as u32) as usize]
    }

    fn random_text(rng: &mut Rng) -> String {
        match rng.below(8) {
            0 => pick(rng, WORDS).to_string(),
            1 => pick(rng, CORPUS).to_string(),
            _ => (0..rng.below(9)).map(|_| pick(rng, ALPHABET)).collect(),
        }
    }

    fn random_value(rng: &mut Rng, depth: u32) -> Value {
        match rng.below(if depth == 0 { 2 } else { 4 }) {
            0 => Value::Null,
            1 => Value::Scalar(random_text(rng)),
            2 => Value::Seq(
                (0..rng.below(4))
                    .map(|_| random_value(rng, depth - 1))
                    .collect(),
            ),
            _ => {
                let mut entries: Vec<(String, Value)> = Vec::new();
                for _ in 0..rng.below(5) {
                    let key = random_text(rng);
                    if !entries.iter().any(|(k, _)| *k == key) {
                        entries.push((key, random_value(rng, depth - 1)));
                    }
                }
                Value::Map(entries)
            }
        }
    }

    #[test]
    fn random_documents_round_trip() {
        for seed in 0..SEEDS {
            let mut rng = Rng::new(seed, 0x9a17);
            // A mapping at the root, as content files are; anything below.
            let doc = match random_value(&mut rng, 4) {
                m @ Value::Map(_) => m,
                other => Value::Map(vec![("root".into(), other)]),
            };
            round_trip(&doc, &format!("seed {seed}: {doc:?}"));
            let any = random_value(&mut rng, 3);
            round_trip(&any, &format!("seed {seed}, any root: {any:?}"));
        }
    }

    /// Hand-written documents: parse, write, parse again, same value.
    const DOCUMENTS: &[&str] = &[
    "\u{feff}version: 3\nname: épée\n",
    "\u{feff}- a\n- b\n",
    "a: 1\r\nb:\r\n  - x\r\n  - \"y\"\r\n",
    "name: l'épée # the sword\nalt: 'l''épée' # quoted\n",
    "名前: 剣\n説明: \"鋼の剣 # 一\"\nタグ: [武器, 剣]\n",
    "سيف: حاد\nחרב: [חדה, ארוכה]\n",
    "a: a\u{a0}#b\nb: x\u{3000}#y # a real comment\n",
    "k: {\"x:y\": 1, 'a,b': [\"c]\", d]}\n",
    "\"a[1\": 1\na]b: 2\n",
    "e: \"\\t\\r\\0\\n\\\\\\\"\"\n",
    "- {a: é, b: [日, 本]}\n- - x\n  - y\n-\n  k: v\n",
    "outer:\n  inner:\n    - name: 🗡️\n      tags: [👨\u{200d}👩\u{200d}👧, \u{e9}, e\u{301}]\n",
];

    #[test]
    fn hand_written_documents_survive_parse_write_parse() {
        for src in DOCUMENTS {
            let v = parse(src).unwrap_or_else(|e| panic!("{src:?}: {e}"));
            round_trip(&v, &format!("{src:?}"));
        }
    }
}

#[test]
fn a_flow_collection_line_is_a_value_not_a_key() {
    let m = Value::Map(vec![("a".into(), s("1"))]);
    assert_eq!(parse("{a: 1}\n").unwrap(), m);
    assert_eq!(parse("- {a: 1}\n").unwrap(), Value::Seq(vec![m.clone()]));
    assert_eq!(
        parse("k:\n  {a: 1}\n").unwrap(),
        Value::Map(vec![("k".into(), m)])
    );
}

#[test]
fn an_item_indented_past_its_sequence_is_refused() {
    let e = parse("- a\n  - b\n").unwrap_err();
    assert_eq!((e.line, e.problem), (2, ParseProblem::UnexpectedIndent));
}

#[test]
fn tilde_and_null_are_null() {
    let v = parse("a: ~\nb: null\nc: nulls\nd: [~, null]\n").unwrap();
    assert_eq!(v.get("a"), Some(&Value::Null));
    assert_eq!(v.get("b"), Some(&Value::Null));
    assert_eq!(v.get("c"), Some(&s("nulls")));
    assert_eq!(
        v.get("d"),
        Some(&Value::Seq(vec![Value::Null, Value::Null]))
    );
}

#[test]
fn writes_plain_what_reads_back_plain() {
    let v = Value::Map(vec![
        ("n".into(), s("-5")),
        ("dash".into(), s("-")),
        ("words".into(), s("a b")),
        ("quoted".into(), s("a 'b'")),
    ]);
    assert_eq!(
        write(&v, 0),
        "n: -5\ndash: \"-\"\nwords: a b\nquoted: \"a 'b'\"\n"
    );
}

#[test]
fn a_sequence_in_a_sequence_is_indented_under_its_dash() {
    let v = Value::Seq(vec![Value::Seq(vec![s("a"), s("b")])]);
    assert_eq!(write(&v, 9), "-\n  - a\n  - b\n");
    let deeper = Value::Map(vec![("k".into(), v)]);
    assert_eq!(write(&deeper, 9), "k:\n  -\n    - a\n    - b\n");
    // Its own items are a level deeper: at flow depth 2, the third level is
    // on one line.
    let three = Value::Seq(vec![Value::Seq(vec![Value::Seq(vec![s("a")])])]);
    assert_eq!(write(&three, 2), "-\n  - [a]\n");
}

/// The crate's en-US catalog, exported as `EN_US`, has a message for every
/// code, using exactly its args, and no message without an error.
#[test]
fn the_catalog_has_a_message_for_every_code_with_exactly_its_args() {
    let (name, text) = EN_US;
    assert_eq!(name, "angzarr-yaml");
    let file = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/locales/en-US/angzarr-yaml.ftl"
    ))
    .unwrap();
    assert_eq!(text, file);
    assert_eq!(
        angzarr_diagnostic::catalog::check(name, text, &diagnostic_samples()),
        []
    );
}
