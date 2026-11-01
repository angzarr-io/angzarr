//! The strict serde decode: scalars are text, parsed only as the target type
//! asks; unknown keys are refused whether or not the type says
//! `deny_unknown_fields`; every failure is a code, a key path, what was
//! expected and what was found, never English.

use std::collections::BTreeMap;

use angzarr_diagnostic::{Arg, Diagnostic};
use serde::Deserialize;

use super::*;
use crate::parse;

fn decode<'de, T: Deserialize<'de>>(v: &'de Value) -> Result<T, DecodeError> {
    from_value(v)
}

fn doc(text: &str) -> Value {
    parse(text).unwrap()
}

/// The error's code and its args as text: the key and the map.
fn keyed(e: &DecodeError) -> (String, Vec<(String, String)>) {
    let args = e
        .args()
        .into_iter()
        .map(|(name, arg)| {
            let text = match arg {
                Arg::Str(s) => s,
                Arg::Uint(n) => n.to_string(),
                Arg::Int(n) => n.to_string(),
                Arg::Nested(d) => d.code().to_string(),
            };
            (name.to_string(), text)
        })
        .collect();
    (e.code().to_string(), args)
}

fn args(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// A scalar refused: the type asked for, and the text as read.
fn scalar(expected: Expected, text: &str) -> DecodeProblem {
    DecodeProblem::Scalar {
        expected,
        text: text.into(),
    }
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    name: String,
    port: u16,
    debug: bool,
    ratio: f64,
    #[serde(default)]
    tags: Vec<String>,
    retry: Option<Retry>,
    mode: Mode,
    #[serde(default)]
    limits: BTreeMap<String, u32>,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct Retry {
    attempts: u8,
    backoff: Option<u64>,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Mode {
    Fast,
    Careful { level: i8 },
    Named(String),
}

const CONFIG: &str = "\
name: svc
port: 8080
debug: false
ratio: 0.5
tags: [a, 'b c']
retry:
  attempts: 3
mode: fast
limits: {cpu: 2, mem: 512}
";

#[test]
fn a_document_decodes_into_its_types_with_scalars_parsed_as_asked() {
    let c: Config = decode(&doc(CONFIG)).unwrap();
    assert_eq!(
        c,
        Config {
            name: "svc".into(),
            port: 8080,
            debug: false,
            ratio: 0.5,
            tags: vec!["a".into(), "b c".into()],
            retry: Some(Retry {
                attempts: 3,
                backoff: None
            }),
            mode: Mode::Fast,
            limits: [("cpu".to_string(), 2), ("mem".to_string(), 512)]
                .into_iter()
                .collect(),
        }
    );
    // A text field takes any scalar as written: no coercion, so `no`,
    // `1.10` and `0x1F` stay text.
    #[derive(Deserialize)]
    struct Texts {
        a: String,
        b: String,
        c: String,
        d: String,
    }
    let t: Texts = decode(&doc("a: no\nb: 1.10\nc: 0x1F\nd: ''\n")).unwrap();
    assert_eq!(
        (t.a, t.b, t.c, t.d),
        ("no".into(), "1.10".into(), "0x1F".into(), "".into())
    );
    // A borrowed &str works too.
    #[derive(Deserialize)]
    struct Borrowed<'a> {
        name: &'a str,
    }
    let v = doc("name: svc\n");
    let b: Borrowed<'_> = decode(&v).unwrap();
    assert_eq!(b.name, "svc");
}

#[test]
fn enums_are_a_scalar_or_a_one_key_mapping() {
    #[derive(Debug, PartialEq, Deserialize)]
    struct Holder {
        mode: Mode,
    }
    let m = |text: &str| decode::<Holder>(&doc(text)).map(|h| h.mode);
    assert_eq!(m("mode: fast\n").unwrap(), Mode::Fast);
    assert_eq!(
        m("mode: {careful: {level: -2}}\n").unwrap(),
        Mode::Careful { level: -2 }
    );
    assert_eq!(m("mode:\n  named: x\n").unwrap(), Mode::Named("x".into()));
    let e = m("mode: slow\n").unwrap_err();
    assert_eq!(
        keyed(&e),
        (
            "angzarr-yaml-decode-unknown-variant".into(),
            args(&[
                ("key_path", "mode"),
                ("expected", "fast,careful,named"),
                ("found", "slow")
            ])
        )
    );
    let e = m("mode: {fast: 1, careful: 2}\n").unwrap_err();
    assert_eq!(
        keyed(&e),
        (
            "angzarr-yaml-decode-type".into(),
            args(&[
                ("key_path", "mode"),
                ("expected", "enum"),
                ("found", "mapping")
            ])
        )
    );
    let e = m("mode: [fast]\n").unwrap_err();
    assert_eq!(
        e.problem,
        DecodeProblem::Type {
            expected: Expected::Enum,
            found: Kind::Sequence
        }
    );
    // A variant with content, given none.
    let e = m("mode: careful\n").unwrap_err();
    assert_eq!(
        keyed(&e),
        (
            "angzarr-yaml-decode-type".into(),
            args(&[
                ("key_path", "mode"),
                ("expected", "mapping"),
                ("found", "scalar")
            ])
        )
    );
    let e = m("mode: named\n").unwrap_err();
    assert_eq!(
        e.problem,
        DecodeProblem::Type {
            expected: Expected::Mapping,
            found: Kind::Scalar
        }
    );
    // A unit variant given content.
    let e = m("mode: {fast: 1}\n").unwrap_err();
    assert_eq!(
        keyed(&e),
        (
            "angzarr-yaml-decode-type".into(),
            args(&[
                ("key_path", "mode.fast"),
                ("expected", "null"),
                ("found", "scalar")
            ])
        )
    );
    // A tuple variant.
    #[derive(Debug, PartialEq, Deserialize)]
    enum Shape {
        Pair(u8, u8),
    }
    assert_eq!(
        decode::<Shape>(&doc("Pair: [1, 2]\n")).unwrap(),
        Shape::Pair(1, 2)
    );
    assert_eq!(
        decode::<Shape>(&doc("Pair: [1]\n")).unwrap_err().problem,
        DecodeProblem::Length {
            expected: 2,
            found: 1
        }
    );
    assert_eq!(
        decode::<Shape>(&doc("Pair: 1\n")).unwrap_err().problem,
        DecodeProblem::Type {
            expected: Expected::Sequence,
            found: Kind::Scalar
        }
    );
}

#[test]
fn an_unknown_key_is_refused_with_the_keys_it_could_have_been() {
    let text = format!("{CONFIG}colour: red\n");
    let e = decode::<Config>(&doc(&text)).unwrap_err();
    assert_eq!(
        keyed(&e),
        (
            "angzarr-yaml-decode-unknown-key".into(),
            args(&[
                ("key_path", "colour"),
                ("expected", "name,port,debug,ratio,tags,retry,mode,limits"),
                ("found", "colour")
            ])
        )
    );
    // Nested, the path leads to it.
    let text = CONFIG.replace("  attempts: 3\n", "  attempts: 3\n  jitter: 1\n");
    let e = decode::<Config>(&doc(&text)).unwrap_err();
    assert_eq!(e.key_path(), "retry.jitter");
    assert_eq!(
        e.problem,
        DecodeProblem::UnknownKey {
            key: "jitter".into(),
            known: vec!["attempts", "backoff"]
        }
    );
}

#[test]
fn an_unknown_key_is_refused_even_without_deny_unknown_fields() {
    // Strict for all (owner ruling): there is no keep-unknown mode.
    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    struct Lenient {
        a: u8,
    }
    let e = decode::<Lenient>(&doc("a: 1\nb:\n  c: 2\n")).unwrap_err();
    assert_eq!(
        keyed(&e),
        (
            "angzarr-yaml-decode-unknown-key".into(),
            args(&[("key_path", "b"), ("expected", ""), ("found", "b")])
        )
    );
    assert!(decode::<Lenient>(&doc("a: 1\n")).is_ok());
}

#[test]
fn a_missing_key_names_where_it_belongs() {
    let text = CONFIG.replace("port: 8080\n", "");
    let e = decode::<Config>(&doc(&text)).unwrap_err();
    assert_eq!(
        keyed(&e),
        (
            "angzarr-yaml-decode-missing-key".into(),
            args(&[
                ("key_path", "port"),
                ("expected", "port"),
                ("found", "absent")
            ])
        )
    );
    let text = CONFIG.replace("  attempts: 3\n", "  backoff: 2\n");
    let e = decode::<Config>(&doc(&text)).unwrap_err();
    assert_eq!(e.key_path(), "retry.attempts");
    assert_eq!(
        e.problem,
        DecodeProblem::MissingKey {
            key: "attempts".into()
        }
    );
}

#[test]
fn a_scalar_that_is_not_what_the_type_asks_is_refused_as_read() {
    let bad = |from: &str, to: &str| {
        let text = CONFIG.replace(from, to);
        decode::<Config>(&doc(&text)).unwrap_err()
    };
    let e = bad("port: 8080", "port: 80800");
    assert_eq!(
        keyed(&e),
        (
            "angzarr-yaml-decode-scalar".into(),
            args(&[
                ("key_path", "port"),
                ("expected", "u16"),
                ("found", "80800")
            ])
        )
    );
    assert_eq!(
        bad("port: 8080", "port: '-1'").problem,
        scalar(Expected::U16, "-1")
    );
    // Booleans are `true` and `false`, nothing else.
    for no in ["no", "False", "0", "off"] {
        let e = bad("debug: false", &format!("debug: {no}"));
        assert_eq!(e.problem, scalar(Expected::Bool, no), "{no}");
    }
    assert_eq!(
        bad("ratio: 0.5", "ratio: half").problem,
        scalar(Expected::F64, "half")
    );
    let e = bad("  attempts: 3", "  attempts: 3.0");
    assert_eq!(e.key_path(), "retry.attempts");
    assert_eq!(e.problem, scalar(Expected::U8, "3.0"));
    let e = bad("{cpu: 2, mem: 512}", "{cpu: 2, mem: lots}");
    assert_eq!(e.key_path(), "limits.mem");
    assert_eq!(e.problem, scalar(Expected::U32, "lots"));
    let e = bad("tags: [a, 'b c']", "tags: [a, [b]]");
    assert_eq!(e.key_path(), "tags[1]");
    assert_eq!(
        e.problem,
        DecodeProblem::Type {
            expected: Expected::String,
            found: Kind::Sequence
        }
    );
}

#[test]
fn a_value_of_the_wrong_shape_is_refused_with_what_it_is() {
    let bad = |from: &str, to: &str| {
        let text = CONFIG.replace(from, to);
        decode::<Config>(&doc(&text)).unwrap_err()
    };
    let e = bad("name: svc", "name: [svc]");
    assert_eq!(
        keyed(&e),
        (
            "angzarr-yaml-decode-type".into(),
            args(&[
                ("key_path", "name"),
                ("expected", "string"),
                ("found", "sequence")
            ])
        )
    );
    // An empty value is null, never an empty string or a zero.
    assert_eq!(
        bad("name: svc", "name:").problem,
        DecodeProblem::Type {
            expected: Expected::String,
            found: Kind::Null
        }
    );
    assert_eq!(
        bad("tags: [a, 'b c']", "tags: x").problem,
        DecodeProblem::Type {
            expected: Expected::Sequence,
            found: Kind::Scalar
        }
    );
    assert_eq!(
        bad("tags: [a, 'b c']", "tags:").problem,
        DecodeProblem::Type {
            expected: Expected::Sequence,
            found: Kind::Null
        }
    );
    assert_eq!(
        bad("limits: {cpu: 2, mem: 512}", "limits: [1]").problem,
        DecodeProblem::Type {
            expected: Expected::Mapping,
            found: Kind::Sequence
        }
    );
    let e = bad("retry:\n  attempts: 3", "retry: 3");
    assert_eq!(e.key_path(), "retry");
    assert_eq!(
        e.problem,
        DecodeProblem::Type {
            expected: Expected::Mapping,
            found: Kind::Scalar
        }
    );
    // The root.
    let e = decode::<Config>(&doc("- a\n")).unwrap_err();
    assert_eq!(e.key_path(), "");
    assert_eq!(
        e.problem,
        DecodeProblem::Type {
            expected: Expected::Mapping,
            found: Kind::Sequence
        }
    );
}

/// Every primitive serde reads, one field each.
#[derive(Debug, PartialEq, Deserialize)]
struct All {
    i8: i8,
    i16: i16,
    i32: i32,
    i64: i64,
    i128: i128,
    u8: u8,
    u16: u16,
    u32: u32,
    u64: u64,
    u128: u128,
    f32: f32,
    f64: f64,
    c: char,
    unit: (),
    pair: (u8, String),
    bytes: serde_bytes_like::Bytes,
    opt: Option<u8>,
    none: Option<u8>,
    newtype: Meters,
}

#[derive(Debug, PartialEq, Deserialize)]
struct Meters(u32);

/// An [`All`] at each primitive's edges.
const ALL: &str = "\
i8: -128
i16: -32768
i32: '-7'
i64: 9223372036854775807
i128: -170141183460469231731687303715884105728
u8: 255
u16: 65535
u32: 4294967295
u64: 18446744073709551615
u128: 340282366920938463463374607431768211455
f32: 1.5
f64: -2e3
c: x
unit:
pair: [7, seven]
bytes: abc
opt: 4
none: ~
newtype: 12
";

/// The problem decoding [`ALL`] with `key`'s value `value` instead.
fn all_with(key: &str, value: &str) -> DecodeProblem {
    let text = ALL
        .lines()
        .map(|l| {
            if l.starts_with(&format!("{key}:")) {
                format!("{key}: {value}")
            } else {
                l.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    decode::<All>(&doc(&text)).unwrap_err().problem
}

#[test]
fn every_primitive_is_parsed_from_its_text() {
    let all: All = decode(&doc(ALL)).unwrap();
    assert_eq!(
        all,
        All {
            i8: i8::MIN,
            i16: i16::MIN,
            i32: -7,
            i64: i64::MAX,
            i128: i128::MIN,
            u8: u8::MAX,
            u16: u16::MAX,
            u32: u32::MAX,
            u64: u64::MAX,
            u128: u128::MAX,
            f32: 1.5,
            f64: -2000.0,
            c: 'x',
            unit: (),
            pair: (7, "seven".into()),
            bytes: serde_bytes_like::Bytes(b"abc".to_vec()),
            opt: Some(4),
            none: None,
            newtype: Meters(12),
        }
    );
}

#[test]
fn every_primitive_refuses_what_does_not_fit() {
    assert_eq!(all_with("i8", "128"), scalar(Expected::I8, "128"));
    assert_eq!(all_with("i16", "x"), scalar(Expected::I16, "x"));
    assert_eq!(all_with("i32", "1e3"), scalar(Expected::I32, "1e3"));
    assert_eq!(all_with("i64", "0x10"), scalar(Expected::I64, "0x10"));
    assert_eq!(all_with("i128", "1_000"), scalar(Expected::I128, "1_000"));
    assert_eq!(all_with("u8", "-1"), scalar(Expected::U8, "-1"));
    assert_eq!(all_with("u16", "1.0"), scalar(Expected::U16, "1.0"));
    assert_eq!(
        all_with("u32", "4294967296"),
        scalar(Expected::U32, "4294967296")
    );
    assert_eq!(all_with("u64", "y"), scalar(Expected::U64, "y"));
    assert_eq!(all_with("u128", "-0x1"), scalar(Expected::U128, "-0x1"));
    assert_eq!(all_with("f32", "one"), scalar(Expected::F32, "one"));
    assert_eq!(all_with("c", "xy"), scalar(Expected::Char, "xy"));
    assert_eq!(
        all_with("unit", "x"),
        DecodeProblem::Type {
            expected: Expected::Null,
            found: Kind::Scalar
        }
    );
    assert_eq!(
        all_with("pair", "[1, a, b]"),
        DecodeProblem::Length {
            expected: 2,
            found: 3
        }
    );
    assert_eq!(
        all_with("pair", "x"),
        DecodeProblem::Type {
            expected: Expected::Sequence,
            found: Kind::Scalar
        }
    );
    assert_eq!(
        all_with("bytes", "[1]"),
        DecodeProblem::Type {
            expected: Expected::Bytes,
            found: Kind::Sequence
        }
    );
    assert_eq!(all_with("opt", "x"), scalar(Expected::U8, "x"));
    assert_eq!(all_with("newtype", "-1"), scalar(Expected::U32, "-1"));
}

/// YAML's spellings of the infinities and not-a-number, not Rust's; and a
/// sign alone, or a `+`, is no number.
#[test]
fn numbers_are_spelled_as_yaml_spells_them() {
    #[derive(Deserialize)]
    struct F {
        a: f64,
        b: f64,
        c: f64,
    }
    let f: F = decode(&doc("a: .inf\nb: -.inf\nc: .nan\n")).unwrap();
    assert_eq!((f.a, f.b), (f64::INFINITY, f64::NEG_INFINITY));
    assert!(f.c.is_nan());
    for rust in ["inf", "NaN", "infinity", "-inf"] {
        assert_eq!(
            decode::<f64>(&doc(&format!("{rust}\n")))
                .unwrap_err()
                .problem,
            scalar(Expected::F64, rust),
            "{rust}"
        );
    }
    assert_eq!(
        decode::<i8>(&doc("'-'\n")).unwrap_err().problem,
        scalar(Expected::I8, "-")
    );
    assert_eq!(
        decode::<u8>(&doc("'+1'\n")).unwrap_err().problem,
        scalar(Expected::U8, "+1")
    );
}

/// A byte buffer as serde_bytes would take it, without the dependency.
mod serde_bytes_like {
    #[derive(Debug, PartialEq)]
    pub struct Bytes(pub Vec<u8>);

    impl<'de> serde::Deserialize<'de> for Bytes {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Bytes, D::Error> {
            struct V;
            impl<'de> serde::de::Visitor<'de> for V {
                type Value = Bytes;
                fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("bytes")
                }
                fn visit_bytes<E>(self, v: &[u8]) -> Result<Bytes, E> {
                    Ok(Bytes(v.to_vec()))
                }
            }
            d.deserialize_bytes(V)
        }
    }
}

#[test]
fn a_types_own_refusal_keeps_its_text_as_the_external_arg() {
    #[derive(Debug)]
    #[allow(dead_code)]
    struct Even(u32);
    impl<'de> Deserialize<'de> for Even {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Even, D::Error> {
            let n = u32::deserialize(d)?;
            if n % 2 == 0 {
                Ok(Even(n))
            } else {
                Err(serde::de::Error::custom("odd"))
            }
        }
    }
    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    struct Holder {
        items: Vec<Even>,
    }
    let e = decode::<Holder>(&doc("items: [2, 3]\n")).unwrap_err();
    assert_eq!(
        keyed(&e),
        (
            "angzarr-yaml-decode-invalid".into(),
            args(&[
                ("key_path", "items[1]"),
                ("expected", "u32"),
                ("found", "3"),
                ("ext_err_txt", "odd")
            ])
        )
    );
    // A visitor's own invalid_type and invalid_value are judged against the
    // node, not serde's English.
    struct Picky;
    impl<'de> Deserialize<'de> for Picky {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Picky, D::Error> {
            struct V;
            impl<'de> serde::de::Visitor<'de> for V {
                type Value = Picky;
                fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("the word yes")
                }
                fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Picky, E> {
                    if v == "yes" {
                        Ok(Picky)
                    } else {
                        Err(E::invalid_value(serde::de::Unexpected::Str(v), &self))
                    }
                }
            }
            d.deserialize_str(V)
        }
    }
    let e = decode::<Picky>(&doc("nope\n")).err().unwrap();
    assert_eq!(e.problem, scalar(Expected::String, "nope"));
    // A visitor that takes no string: serde's invalid_type.
    let e = decode::<Vec<u8>>(&doc("{a: 1}\n")).unwrap_err();
    assert_eq!(
        e.problem,
        DecodeProblem::Type {
            expected: Expected::Sequence,
            found: Kind::Mapping
        }
    );
    #[derive(Debug, Deserialize)]
    struct Unit;
    let e = decode::<Unit>(&doc("x\n")).unwrap_err();
    assert_eq!(
        e.problem,
        DecodeProblem::Type {
            expected: Expected::Null,
            found: Kind::Scalar
        }
    );
}

#[test]
fn any_sees_scalars_as_text() {
    // Untyped targets (an untagged enum, serde's own buffering) see every
    // scalar as its text: nothing is guessed.
    #[derive(Debug, PartialEq, Deserialize)]
    #[serde(untagged)]
    enum Loose {
        Text(String),
        List(Vec<String>),
        Map(BTreeMap<String, String>),
        Nothing(()),
    }
    let l = |text: &str| decode::<Loose>(&doc(text)).unwrap();
    assert_eq!(l("12\n"), Loose::Text("12".into()));
    assert_eq!(l("[1, x]\n"), Loose::List(vec!["1".into(), "x".into()]));
    assert_eq!(
        l("{a: 1}\n"),
        Loose::Map([("a".to_string(), "1".to_string())].into_iter().collect())
    );
    assert_eq!(l("\n"), Loose::Nothing(()));
}

#[test]
fn map_keys_are_parsed_as_their_type_asks() {
    let v = doc("1: a\n20: b\n");
    let m: BTreeMap<u32, String> = decode(&v).unwrap();
    assert_eq!(
        m,
        [(1, "a".to_string()), (20, "b".to_string())]
            .into_iter()
            .collect()
    );
    let e = decode::<BTreeMap<u32, String>>(&doc("x: a\n")).unwrap_err();
    assert_eq!(e.key_path(), "x");
    assert_eq!(e.problem, scalar(Expected::U32, "x"));
    let b: BTreeMap<bool, u8> = decode(&doc("true: 1\n")).unwrap();
    assert_eq!(b.get(&true), Some(&1));
}

#[test]
fn a_decode_error_is_a_diagnostic_never_english() {
    let e = decode::<Config>(&doc("name: [x]\n")).unwrap_err();
    assert_eq!(
        e.to_string(),
        "angzarr-yaml-decode-type key_path=name expected=string found=sequence"
    );
    let root = decode::<u8>(&doc("x\n")).unwrap_err();
    assert_eq!(
        root.to_string(),
        "angzarr-yaml-decode-scalar key_path=\"\" expected=u8 found=x"
    );
    // Keys and items are written as a path: dotted keys, bracketed items.
    let deep =
        decode::<BTreeMap<String, Vec<BTreeMap<String, u8>>>>(&doc("a:\n  - {b: 1}\n  - {b: x}\n"))
            .unwrap_err();
    assert_eq!(deep.key_path(), "a[1].b");
    assert_eq!(
        deep.path,
        vec![
            PathSeg::Key("a".into()),
            PathSeg::Item(1),
            PathSeg::Key("b".into())
        ]
    );
}

#[test]
fn a_loader_decodes_the_migrated_tree_strictly() {
    use crate::versioned::{LoadProblem, Migrations};
    #[derive(Debug, PartialEq, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Toy {
        schema_version: u32,
        health: u32,
    }
    fn rename(doc: &mut Value) -> Result<(), crate::versioned::MigrationError> {
        crate::versioned::rename_key(doc, "hp", "health")
    }
    let l = Migrations::new("toy", 1).step(0, rename).build().unwrap();
    let toy: Toy = l.load_into(Some("t.yaml"), "hp: 3\n").unwrap();
    assert_eq!(
        toy,
        Toy {
            schema_version: 1,
            health: 3
        }
    );
    let e = l
        .load_into::<Toy>(Some("t.yaml"), "schema_version: 1\nhealth: 3\nextra: 1\n")
        .unwrap_err();
    assert!(
        matches!(&e.problem, LoadProblem::Decode(r) if r.code() == "angzarr-yaml-decode-unknown-key")
    );
    assert_eq!(
        e.to_string(),
        "angzarr-yaml-load-failed-at kind=toy source=t.yaml \
         problem=angzarr-yaml-decode-unknown-key problem.key_path=extra \
         problem.expected=schema_version,health problem.found=extra"
    );
    // A load failure comes first.
    assert!(matches!(
        l.load_into::<Toy>(None, "schema_version: 2\n")
            .unwrap_err()
            .problem,
        LoadProblem::TooNew { .. }
    ));
}

#[test]
fn a_refusal_after_the_node_was_read_names_what_was_asked_of_that_node_only() {
    // A type that reads a mapping whole, then refuses it: the last thing
    // asked was of a key inside, so what this node was asked is unknown.
    #[derive(Deserialize)]
    struct Raw {
        lo: u32,
        hi: u32,
    }
    #[derive(Debug, Deserialize)]
    #[serde(try_from = "Raw")]
    #[allow(dead_code)]
    struct Range(u32, u32);
    impl TryFrom<Raw> for Range {
        type Error = &'static str;
        fn try_from(r: Raw) -> Result<Range, &'static str> {
            if r.lo <= r.hi {
                Ok(Range(r.lo, r.hi))
            } else {
                Err("lo above hi")
            }
        }
    }
    let e = decode::<Range>(&doc("lo: 3\nhi: 2\n")).unwrap_err();
    assert_eq!(
        keyed(&e),
        (
            "angzarr-yaml-decode-invalid".into(),
            args(&[
                ("key_path", ""),
                ("expected", "any"),
                ("found", "mapping"),
                ("ext_err_txt", "lo above hi")
            ])
        )
    );
}

#[test]
fn a_type_that_wraps_a_failure_carries_its_code_and_args() {
    struct Wrapped;
    impl<'de> Deserialize<'de> for Wrapped {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Wrapped, D::Error> {
            u8::deserialize(d)
                .map(|_| Wrapped)
                .map_err(|e| serde::de::Error::custom(format!("wrapped: {e}")))
        }
    }
    #[derive(Deserialize)]
    struct Holder {
        #[allow(dead_code)]
        a: Wrapped,
    }
    let e = decode::<Holder>(&doc("a: x\n")).err().unwrap();
    assert_eq!(
        e.problem,
        DecodeProblem::Invalid {
            expected: Expected::U8,
            found: "x".into(),
            text: "wrapped: angzarr-yaml-decode-scalar key_path=a expected=u8 found=x".into(),
        }
    );
}

#[test]
fn an_enum_can_be_a_mapping_key() {
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
    #[serde(rename_all = "lowercase")]
    enum Side {
        Left,
        Right,
    }
    let m: BTreeMap<Side, u8> = decode(&doc("left: 1\nright: 2\n")).unwrap();
    assert_eq!(m, [(Side::Left, 1), (Side::Right, 2)].into_iter().collect());
    let e = decode::<BTreeMap<Side, u8>>(&doc("up: 1\n")).unwrap_err();
    assert_eq!(e.key_path(), "up");
    assert_eq!(
        e.problem,
        DecodeProblem::UnknownVariant {
            variant: "up".into(),
            known: vec!["left", "right"]
        }
    );
}
