use super::*;
use crate::fixture::{descriptor, message, pool};
use crate::{Problem, Writer};
use std::io::{self, Cursor};

fn read_all(text: &[u8], type_name: &str) -> Vec<Result<DynamicMessage, crate::Error>> {
    let pool = pool();
    Reader::new(Cursor::new(text.to_vec()), descriptor(&pool, type_name)).collect()
}

#[test]
fn what_a_writer_wrote_a_reader_reads_back_equal() {
    let pool = pool();
    let sent = vec![
        DynamicMessage::new(descriptor(&pool, "Record")),
        message(
            &pool,
            "Record",
            r#"{"text":"a\nb","big":"-9223372036854775808","ubig":"18446744073709551615"}"#,
        ),
        message(
            &pool,
            "Record",
            r#"{"tags":["x"],"counts":{"k":"1","j":"2"},"pickText":"p","inner":{"name":"n"}}"#,
        ),
        message(
            &pool,
            "Record",
            r#"{"at":"2026-11-06T00:00:00Z","span":"1s","any":{"@type":"type.googleapis.com/t.Inner","name":"z"}}"#,
        ),
        message(&pool, "Record", r#"{"blob":"AP8=","color":"RED"}"#),
    ];
    let mut w = Writer::new(Vec::new());
    for m in &sent {
        w.write(m).unwrap();
    }
    let bytes = w.into_inner();
    let got: Vec<_> = Reader::new(Cursor::new(bytes), descriptor(&pool, "Record"))
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(got, sent);
}

#[test]
fn an_empty_stream_has_no_messages() {
    assert!(read_all(b"", "Record").is_empty());
}

#[test]
fn a_line_that_is_the_empty_message_is_a_message() {
    let got = read_all(b"{}\n{}\n", "Empty");
    assert_eq!(got.len(), 2);
    assert!(got.iter().all(Result::is_ok));
}

#[test]
fn the_reader_decodes_as_the_type_it_is_told() {
    let got = read_all(b"{\"text\":\"x\"}\n", "Empty");
    assert!(matches!(
        got[0].as_ref().unwrap_err().problem,
        Problem::MalformedLine { .. }
    ));
    let got = read_all(b"{\"name\":\"x\"}\n", "Inner");
    assert!(got[0].is_ok());
}

/// The error says which line, 1-based, and the reader goes on: the bad
/// line was consumed.
#[test]
fn a_bad_line_is_an_error_with_its_number_and_the_reader_continues() {
    let got = read_all(
        b"{\"text\":\"a\"}\n{\"nope\":1}\n{\"text\":\"c\"}\n[1]\n",
        "Record",
    );
    assert_eq!(got.len(), 4);
    assert!(got[0].is_ok());
    let e = got[1].as_ref().unwrap_err();
    assert_eq!(e.line, 2);
    assert!(matches!(e.problem, Problem::MalformedLine { .. }));
    assert!(got[2].is_ok());
    assert_eq!(got[3].as_ref().unwrap_err().line, 4);
}

#[test]
fn an_unknown_field_and_a_wrong_type_are_refused() {
    for text in [
        "{\"nope\":1}\n",
        "{\"text\":1}\n",
        "[]\n",
        "\"x\"\n",
        "{} {}\n",
    ] {
        let got = read_all(text.as_bytes(), "Record");
        assert!(
            matches!(
                got[0].as_ref().unwrap_err().problem,
                Problem::MalformedLine { .. }
            ),
            "{text}"
        );
    }
}

#[test]
fn a_blank_line_is_an_error_and_the_reader_continues() {
    let got = read_all(b"{}\n\n  \n{}\n", "Empty");
    assert_eq!(got.len(), 4);
    assert!(got[0].is_ok());
    let e = got[1].as_ref().unwrap_err();
    assert_eq!((e.line, &e.problem), (2, &Problem::BlankLine));
    let e = got[2].as_ref().unwrap_err();
    assert_eq!((e.line, &e.problem), (3, &Problem::BlankLine));
    assert!(got[3].is_ok());
}

/// A last line with no line feed may be a write cut short: it is neither
/// dropped nor accepted, even when it would parse. It is the end: the
/// reader returns nothing after it.
#[test]
fn a_last_line_without_a_line_feed_is_a_torn_tail() {
    for tail in ["{}", "{\"te", " ", "x"] {
        let text = format!("{{}}\n{tail}");
        let got = read_all(text.as_bytes(), "Empty");
        assert_eq!(got.len(), 2, "{tail:?}");
        assert!(got[0].is_ok());
        let e = got[1].as_ref().unwrap_err();
        assert_eq!(e.line, 2);
        assert_eq!(
            e.problem,
            Problem::TornTail { bytes: tail.len() },
            "{tail:?}"
        );
    }
}

#[test]
fn a_stream_of_one_torn_line_is_a_torn_tail_at_line_one() {
    let got = read_all(b"{}", "Empty");
    assert_eq!(got.len(), 1);
    let e = got[0].as_ref().unwrap_err();
    assert_eq!((e.line, &e.problem), (1, &Problem::TornTail { bytes: 2 }));
}

#[test]
fn a_bad_line_before_a_torn_tail_is_reported_first() {
    let got = read_all(b"[]\n{}", "Empty");
    assert_eq!(got.len(), 2);
    assert!(matches!(
        got[0].as_ref().unwrap_err().problem,
        Problem::MalformedLine { .. }
    ));
    assert_eq!(got[1].as_ref().unwrap_err().line, 2);
}

#[test]
fn after_a_torn_tail_there_is_nothing_more() {
    let pool = pool();
    let mut r = Reader::new(Cursor::new(b"{}".to_vec()), descriptor(&pool, "Empty"));
    assert!(r.next().unwrap().is_err());
    assert!(r.next().is_none());
    assert!(r.next().is_none());
}

#[test]
fn a_line_that_is_not_utf8_is_an_error_and_the_reader_continues() {
    let got = read_all(b"{}\n\xff\xfe\n{}\n", "Empty");
    assert_eq!(got.len(), 3);
    let e = got[1].as_ref().unwrap_err();
    assert_eq!((e.line, &e.problem), (2, &Problem::NotUtf8));
    assert!(got[2].is_ok());
}

#[test]
fn a_crlf_file_reads() {
    let got = read_all(b"{\"name\":\"a\"}\r\n{\"name\":\"b\"}\r\n", "Inner");
    assert_eq!(got.len(), 2);
    assert!(got.iter().all(Result::is_ok));
}

#[test]
fn a_very_long_line_is_fine() {
    let pool = pool();
    let text = "x".repeat(8 * 1024 * 1024);
    let m = message(&pool, "Inner", &format!("{{\"name\":\"{text}\"}}"));
    let mut w = Writer::new(Vec::new());
    w.write(&m).unwrap();
    w.write(&m).unwrap();
    let got: Vec<_> = Reader::new(Cursor::new(w.into_inner()), descriptor(&pool, "Inner"))
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(got, vec![m.clone(), m]);
}

/// Reads one good line, then fails.
struct Breaks {
    sent: bool,
}

impl io::Read for Breaks {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        unreachable!("read through BufRead")
    }
}

impl BufRead for Breaks {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.sent {
            Err(io::Error::other("device gone"))
        } else {
            self.sent = true;
            Ok(b"{}\n")
        }
    }
    fn consume(&mut self, _: usize) {}
}

/// After a failed read the stream's state is unknown: the error is the last
/// item.
#[test]
fn a_stream_that_fails_to_read_ends_with_the_error() {
    let pool = pool();
    let mut r = Reader::new(Breaks { sent: false }, descriptor(&pool, "Empty"));
    assert!(r.next().unwrap().is_ok());
    let e = r.next().unwrap().unwrap_err();
    assert_eq!(e.line, 2);
    assert_eq!(
        e.problem,
        Problem::ReadFailed {
            ext_err_txt: "device gone".into()
        }
    );
    assert!(r.next().is_none());
}
