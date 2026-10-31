use super::*;
use crate::Report;

const CATALOG: &str = r#"# A comment.
## A group comment.

demo-plain = no args at all
demo-one = line { $line }: "{ $key }" twice
demo-select =
    stream { $stream } holds { $actual ->
        [one] one event
       *[other] { $actual } events
    }
demo-literal = expected , or {"}"} and not {"$fake"}
demo-attr = top
    .hint = see { $path }
-demo-term = a term { $ignored }
demo-call = { NUMBER($count) } items
"#;

fn vars(c: &Catalog, id: &str) -> Vec<String> {
    c.variables(id)
        .unwrap_or_else(|| panic!("{id} missing"))
        .iter()
        .cloned()
        .collect()
}

#[test]
fn a_catalog_is_its_messages_and_the_variables_each_uses() {
    let c = Catalog::parse(CATALOG).expect("parses");
    let ids: Vec<&str> = c.ids().collect();
    assert_eq!(
        ids,
        [
            "demo-attr",
            "demo-call",
            "demo-literal",
            "demo-one",
            "demo-plain",
            "demo-select"
        ]
    );
    assert!(vars(&c, "demo-plain").is_empty());
    assert_eq!(vars(&c, "demo-one"), ["key", "line"]);
    assert_eq!(vars(&c, "demo-select"), ["actual", "stream"]);
    // A string literal's text is not a variable.
    assert!(vars(&c, "demo-literal").is_empty());
    // An attribute's variables are its message's.
    assert_eq!(vars(&c, "demo-attr"), ["path"]);
    assert_eq!(vars(&c, "demo-call"), ["count"]);
    // A term is no message.
    assert_eq!(c.variables("-demo-term"), None);
    assert_eq!(c.variables("demo-term"), None);
}

#[test]
fn a_message_twice_or_a_line_that_is_no_entry_is_refused() {
    let twice = Catalog::parse("a-b = one\na-b = two\n").unwrap_err();
    assert_eq!(
        twice,
        [Problem::MessageTwice {
            id: "a-b".to_string()
        }]
    );
    let junk = Catalog::parse("a-b = one\nnot an entry\n# fine\nc = { $x\n").unwrap_err();
    assert_eq!(
        junk,
        [Problem::Syntax { line: 2 }, Problem::Syntax { line: 4 }]
    );
    // An empty value is no message.
    assert_eq!(
        Catalog::parse("a-b =\n").unwrap_err(),
        [Problem::Syntax { line: 1 }]
    );
}

#[derive(Clone, Debug)]
struct Shown(Report, &'static str);

impl fmt::Display for Shown {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.1)
    }
}

impl Diagnostic for Shown {
    fn code(&self) -> &str {
        self.0.code()
    }
    fn args(&self) -> Vec<(&str, Arg)> {
        self.0.args()
    }
}

fn boxed(d: impl Diagnostic + 'static) -> Box<dyn Diagnostic> {
    Box::new(d)
}

const SMALL: &str = "demo-one = line { $line }: { $key }\ndemo-plain = plain\n";

#[test]
fn samples_that_match_their_catalog_exactly_pass() {
    let samples = [
        boxed(Report::new("demo-one").arg("line", 3u32).arg("key", "a")),
        boxed(Report::new("demo-plain")),
    ];
    assert_eq!(check("demo", SMALL, &samples), []);
}

#[test]
fn every_way_a_sample_and_its_catalog_disagree_is_reported() {
    let samples = [
        // No message.
        boxed(Report::new("demo-absent")),
        // Its message uses `key`, which it does not give, and it gives
        // `extra`.
        boxed(Report::new("demo-one").arg("line", 1u32).arg("extra", 2u32)),
        // An arg twice.
        boxed(Report::new("demo-plain").arg("n", 1u32).arg("n", 2u32)),
        // Not this crate's prefix, and not kebab-case.
        boxed(Report::new("other-plain")),
        boxed(Report::new("demo-Upper")),
        boxed(Report::new("demo-")),
        boxed(Report::new("demo--x")),
        // English in its Display.
        boxed(Shown(Report::new("demo-plain"), "a plain error")),
    ];
    let problems = check("demo", SMALL, &samples);
    let s = |x: &str| x.to_string();
    assert_eq!(
        problems,
        [
            Problem::NoMessage {
                code: s("demo-absent")
            },
            Problem::ArgsDiffer {
                code: s("demo-one"),
                message: vec![s("key"), s("line")],
                error: vec![s("extra"), s("line")],
            },
            Problem::ArgTwice {
                code: s("demo-plain"),
                arg: s("n")
            },
            Problem::ArgsDiffer {
                code: s("demo-plain"),
                message: vec![],
                error: vec![s("n")],
            },
            Problem::BadCode {
                code: s("other-plain")
            },
            Problem::NoMessage {
                code: s("other-plain")
            },
            Problem::BadCode {
                code: s("demo-Upper")
            },
            Problem::NoMessage {
                code: s("demo-Upper")
            },
            Problem::BadCode { code: s("demo-") },
            Problem::NoMessage { code: s("demo-") },
            Problem::BadCode { code: s("demo--x") },
            Problem::NoMessage { code: s("demo--x") },
            Problem::NotKeyed {
                code: s("demo-plain"),
                shown: s("a plain error"),
            },
        ]
    );
}

#[test]
fn a_message_no_sample_produces_is_reported() {
    let samples = [boxed(Report::new("demo-plain"))];
    assert_eq!(
        check("demo", SMALL, &samples),
        [Problem::Unsampled {
            id: "demo-one".to_string()
        }]
    );
}

#[test]
fn a_nested_cause_is_checked_when_it_is_the_crates_own_and_skipped_when_not() {
    let catalog = "demo-outer = outer { $cause }\ndemo-inner = inner { $n }\n";
    let own = Report::new("demo-inner").arg("wrong", 1u32);
    let foreign = Report::new("adapter-io").arg("errno", 5u32);
    let samples = [
        boxed(Report::new("demo-outer").arg("cause", Arg::nested(&own))),
        boxed(Report::new("demo-outer").arg("cause", Arg::nested(&foreign))),
    ];
    let s = |x: &str| x.to_string();
    // The inner one is sampled through its holder, and its args are judged.
    assert_eq!(
        check("demo", catalog, &samples),
        [Problem::ArgsDiffer {
            code: s("demo-inner"),
            message: vec![s("n")],
            error: vec![s("wrong")],
        }]
    );
}

#[test]
fn a_catalog_that_does_not_parse_is_reported_and_nothing_else() {
    let samples = [boxed(Report::new("demo-plain"))];
    assert_eq!(
        check("demo", "demo-plain = a\ndemo-plain = b\n", &samples),
        [Problem::MessageTwice {
            id: "demo-plain".to_string()
        }]
    );
}

#[test]
fn what_is_and_is_not_a_variable() {
    let c = Catalog::parse(concat!(
        "demo-dollar = costs $5, or $name\n",
        "demo-names = { $parent_sequence } { $ext-err }\n",
        "demo-lone = { $ } and { \"$quoted\" }\n",
        "demo-blank = first\n",
        "\n",
        "    then { $later }\n",
        "demo-sel = { $n ->\n",
        "        [one] one\n",
        "\n",
        "       *[other] { $n } of { $m }\n",
        "    }\n",
        "demo-margin = { $len ->\n",
        "    [one] { $len } item\n",
        "   *[other] { $len } items\n",
        "}\n",
        "demo-after = { $x }\n",
    ))
    .expect("parses");
    // `$` outside a placeable is text.
    assert!(vars(&c, "demo-dollar").is_empty());
    assert_eq!(vars(&c, "demo-names"), ["ext-err", "parent_sequence"]);
    assert!(vars(&c, "demo-lone").is_empty());
    // A blank line neither ends a pattern nor a placeable.
    assert_eq!(vars(&c, "demo-blank"), ["later"]);
    assert_eq!(vars(&c, "demo-sel"), ["m", "n"]);
    // A placeable closes at the margin, and the next entry is its own.
    assert_eq!(vars(&c, "demo-margin"), ["len"]);
    assert_eq!(vars(&c, "demo-after"), ["x"]);
}

#[test]
fn a_name_that_is_no_id_a_stray_brace_or_an_open_literal_is_refused() {
    let problems = Catalog::parse(concat!(
        "two words = x\n",
        "9-bad = x\n",
        "demo-stray = a } b\n",
        "demo-later = a\n",
        "    b } c\n",
        "demo-dup = y\n",
        "demo-dup = z\n",
        "demo-open = { \"abc\n",
        "demo-swallowed = { $x }\n",
    ))
    .unwrap_err();
    assert_eq!(
        problems,
        [
            Problem::Syntax { line: 1 },
            Problem::Syntax { line: 2 },
            Problem::Syntax { line: 3 },
            Problem::Syntax { line: 5 },
            Problem::MessageTwice {
                id: "demo-dup".to_string()
            },
            // An open placeable takes every line after it, and is reported
            // at the last.
            Problem::Syntax { line: 9 },
        ]
    );
}
