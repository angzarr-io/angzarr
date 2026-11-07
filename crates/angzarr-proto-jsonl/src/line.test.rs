use super::*;
use crate::fixture::{descriptor, message, pool};
use crate::{Error, Problem};
use prost_reflect::Value;

fn problem_of(r: Result<DynamicMessage, Error>) -> Problem {
    r.expect_err("the line is refused").problem
}

/// The line is canonical proto3 JSON, compact, with keys sorted: 64-bit
/// integers as strings, bytes as base64, enums by name, camelCase names.
#[test]
fn a_message_is_one_compact_line_of_canonical_json_with_sorted_keys() {
    let pool = pool();
    let m = message(
        &pool,
        "Record",
        r#"{"text":"hi","big":"-9223372036854775808","ubig":"18446744073709551615",
            "blob":"AAEC","color":"GREEN","pickBig":"7","tags":["b","a"]}"#,
    );
    assert_eq!(
        encode_line(&m).unwrap(),
        concat!(
            r#"{"big":"-9223372036854775808","blob":"AAEC","color":"GREEN","#,
            r#""pickBig":"7","tags":["b","a"],"text":"hi","ubig":"18446744073709551615"}"#
        )
    );
}

#[test]
fn an_empty_message_is_an_empty_object() {
    let pool = pool();
    let m = DynamicMessage::new(descriptor(&pool, "Empty"));
    assert_eq!(encode_line(&m).unwrap(), "{}");
    assert_eq!(decode_line(&descriptor(&pool, "Empty"), "{}").unwrap(), m);
}

#[test]
fn the_extremes_of_64_bit_integers_survive() {
    let pool = pool();
    let mut m = DynamicMessage::new(descriptor(&pool, "Record"));
    m.set_field_by_name("big", Value::I64(i64::MAX));
    m.set_field_by_name("ubig", Value::U64(u64::MAX));
    let line = encode_line(&m).unwrap();
    assert_eq!(
        line,
        r#"{"big":"9223372036854775807","ubig":"18446744073709551615"}"#
    );
    assert_eq!(decode_line(&descriptor(&pool, "Record"), &line).unwrap(), m);
    m.set_field_by_name("big", Value::I64(i64::MIN));
    let line = encode_line(&m).unwrap();
    assert!(line.contains(r#""big":"-9223372036854775808""#), "{line}");
    assert_eq!(decode_line(&descriptor(&pool, "Record"), &line).unwrap(), m);
}

/// Repeated, map, oneof, nested, enum, bytes, Timestamp, Duration and Any
/// each come back equal.
#[test]
fn every_kind_of_field_round_trips() {
    let pool = pool();
    let record = descriptor(&pool, "Record");
    for json in [
        r#"{"tags":["x","","y"],"inners":[{"name":"a"},{},{"name":"c"}]}"#,
        r#"{"counts":{"a":"1","b":"-2"},"names":{"3":"three","-1":"minus"}}"#,
        r#"{"pickText":"chosen"}"#,
        r#"{"pickBig":"0"}"#,
        r#"{"inner":{"name":"nested"},"color":"RED","blob":"3q2+7w==","flag":true,"ratio":1.5,"small":-7}"#,
        r#"{"at":"2026-11-06T12:30:45.123456789Z","span":"3.000000001s"}"#,
        r#"{"any":{"@type":"type.googleapis.com/t.Inner","name":"packed"}}"#,
        r#"{"ratio":"NaN"}"#,
    ] {
        let m = message(&pool, "Record", json);
        let line = encode_line(&m).unwrap();
        assert!(!line.contains('\n'), "{line}");
        assert_eq!(decode_line(&record, &line).unwrap(), m, "{json}");
    }
}

#[test]
fn well_known_types_are_written_per_the_spec() {
    let pool = pool();
    let m = message(
        &pool,
        "Record",
        r#"{"at":"2026-11-06T12:30:45Z","span":"1.5s"}"#,
    );
    assert_eq!(
        encode_line(&m).unwrap(),
        r#"{"at":"2026-11-06T12:30:45Z","span":"1.5s"}"#
    );
}

/// A string holding line breaks (and the Unicode ones) is escaped: the
/// encoding is one line, and decodes back to the same text.
#[test]
fn a_line_break_in_a_string_is_escaped() {
    let pool = pool();
    let mut m = DynamicMessage::new(descriptor(&pool, "Record"));
    let text = "a\nb\r\nc\u{2028}d\u{85}e\t\"\\";
    m.set_field_by_name("text", Value::String(text.into()));
    let line = encode_line(&m).unwrap();
    assert!(!line.contains('\n') && !line.contains('\r'), "{line:?}");
    assert!(line.contains(r"a\nb\r\nc"), "{line:?}");
    let back = decode_line(&descriptor(&pool, "Record"), &line).unwrap();
    assert_eq!(back.get_field_by_name("text").unwrap().as_str(), Some(text));
}

/// A map's order in memory is a hash order that differs from one map to
/// the next; the line must not.
#[test]
fn the_same_message_is_the_same_bytes_every_time() {
    let pool = pool();
    let json = {
        let entries: Vec<String> = (0..40).map(|i| format!(r#""key{i}":"{i}""#)).collect();
        format!(r#"{{"counts":{{{}}},"text":"t"}}"#, entries.join(","))
    };
    let first = encode_line(&message(&pool, "Record", &json)).unwrap();
    for _ in 0..30 {
        let m = message(&pool, "Record", &json);
        assert_eq!(encode_line(&m).unwrap(), first);
        assert_eq!(encode_line(&m).unwrap(), first);
    }
    // Sorted: key0 < key1 < key10 < key11 ... as text.
    let (a, b) = (first.find("key0\"").unwrap(), first.find("key1\"").unwrap());
    assert!(a < b);
    assert!(first.find("key10\"").unwrap() < first.find("key2\"").unwrap());
}

#[test]
fn an_any_whose_type_the_pool_lacks_cannot_be_encoded() {
    let pool = pool();
    let mut m = DynamicMessage::new(descriptor(&pool, "Record"));
    let any = pool.get_message_by_name("google.protobuf.Any").unwrap();
    let mut a = DynamicMessage::new(any);
    a.set_field_by_name(
        "type_url",
        Value::String("type.googleapis.com/t.Missing".into()),
    );
    m.set_field_by_name("any", Value::Message(a));
    let e = encode_line(&m).unwrap_err();
    assert_eq!(e.line, 0);
    assert!(
        matches!(e.problem, Problem::EncodeFailed { ref ext_err_txt } if !ext_err_txt.is_empty())
    );
}

#[test]
fn text_that_is_not_json_is_malformed() {
    let pool = pool();
    let record = descriptor(&pool, "Record");
    for line in [
        "{",
        "{\"text\":",
        "not json",
        "{'text':'x'}",
        "{\"text\":\"x\",}",
    ] {
        let e = decode_line(&record, line).unwrap_err();
        assert_eq!(e.line, 0, "{line}");
        assert!(
            matches!(e.problem, Problem::MalformedLine { ref ext_err_txt } if !ext_err_txt.is_empty()),
            "{line}: {e:?}"
        );
    }
}

/// Strict, as angzarr-yaml's decode is.
#[test]
fn a_field_the_type_lacks_is_refused() {
    let pool = pool();
    let record = descriptor(&pool, "Record");
    assert!(matches!(
        problem_of(decode_line(&record, r#"{"text":"x","nope":1}"#)),
        Problem::MalformedLine { .. }
    ));
    let empty = descriptor(&pool, "Empty");
    assert!(matches!(
        problem_of(decode_line(&empty, r#"{"text":"x"}"#)),
        Problem::MalformedLine { .. }
    ));
}

#[test]
fn a_value_of_the_wrong_type_is_refused() {
    let pool = pool();
    let record = descriptor(&pool, "Record");
    for line in [
        r#"{"text":1}"#,
        r#"{"big":"not a number"}"#,
        r#"{"color":"PURPLE"}"#,
        r#"{"tags":"x"}"#,
        r#"{"inner":[]}"#,
        r#"{"flag":"yes"}"#,
        r#"{"blob":"***"}"#,
        r#"{"at":"yesterday"}"#,
    ] {
        assert!(
            matches!(
                problem_of(decode_line(&record, line)),
                Problem::MalformedLine { .. }
            ),
            "{line}"
        );
    }
}

/// A line holds a message: an object, not another JSON value.
#[test]
fn a_line_that_is_not_an_object_is_refused() {
    let pool = pool();
    let record = descriptor(&pool, "Record");
    for line in ["[]", "[{}]", "\"x\"", "3", "true", "null"] {
        assert!(
            matches!(
                problem_of(decode_line(&record, line)),
                Problem::MalformedLine { .. }
            ),
            "{line}"
        );
    }
}

#[test]
fn text_after_the_message_is_refused() {
    let pool = pool();
    let record = descriptor(&pool, "Record");
    for line in ["{} {}", "{}x", "{}{}", "{} ,"] {
        assert!(
            matches!(
                problem_of(decode_line(&record, line)),
                Problem::MalformedLine { .. }
            ),
            "{line}"
        );
    }
}

#[test]
fn a_blank_line_is_refused() {
    let pool = pool();
    let record = descriptor(&pool, "Record");
    for line in ["", " ", "\t", "  \t "] {
        let e = decode_line(&record, line).unwrap_err();
        assert_eq!((e.line, e.problem), (0, Problem::BlankLine), "{line:?}");
    }
}

#[test]
fn more_than_one_line_is_refused() {
    let pool = pool();
    let record = descriptor(&pool, "Record");
    for line in ["{}\n", "\n{}", "{\n}", "{}\n{}"] {
        let e = decode_line(&record, line).unwrap_err();
        assert_eq!((e.line, e.problem), (0, Problem::NotOneLine), "{line:?}");
    }
}

/// White space around the message is JSON's; a carriage return from a
/// CRLF file is such.
#[test]
fn white_space_around_the_message_is_allowed() {
    let pool = pool();
    let record = descriptor(&pool, "Record");
    let want = message(&pool, "Record", r#"{"text":"x"}"#);
    for line in [
        r#"{"text":"x"}"#,
        r#"  {"text":"x"}  "#,
        "{\"text\":\"x\"}\r",
        "{ \"text\" : \"x\" }",
    ] {
        assert_eq!(decode_line(&record, line).unwrap(), want, "{line:?}");
    }
}
