use super::*;
use crate::fixture::{descriptor, message, pool};
use crate::Problem;
use prost_reflect::Value;
use std::io;

struct Failing;

impl Write for Failing {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("disk is full"))
    }
    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::other("flush refused"))
    }
}

#[test]
fn each_message_is_a_line_and_a_line_feed() {
    let pool = pool();
    let mut w = Writer::new(Vec::new());
    w.write(&message(&pool, "Record", r#"{"text":"a"}"#))
        .unwrap();
    w.write(&DynamicMessage::new(descriptor(&pool, "Empty")))
        .unwrap();
    w.write(&message(&pool, "Record", r#"{"text":"b\nc"}"#))
        .unwrap();
    w.flush().unwrap();
    assert_eq!(
        String::from_utf8(w.into_inner()).unwrap(),
        "{\"text\":\"a\"}\n{}\n{\"text\":\"b\\nc\"}\n"
    );
}

#[test]
fn a_stream_that_cannot_be_written_says_so() {
    let pool = pool();
    let mut w = Writer::new(Failing);
    let e = w.write(&message(&pool, "Record", "{}")).unwrap_err();
    assert_eq!(e.line, 0);
    assert_eq!(
        e.problem,
        Problem::WriteFailed {
            ext_err_txt: "disk is full".into()
        }
    );
    let e = w.flush().unwrap_err();
    assert_eq!(
        e.problem,
        Problem::WriteFailed {
            ext_err_txt: "flush refused".into()
        }
    );
}

#[test]
fn a_message_that_cannot_be_encoded_writes_nothing() {
    let pool = pool();
    let mut m = DynamicMessage::new(descriptor(&pool, "Record"));
    let mut any = DynamicMessage::new(pool.get_message_by_name("google.protobuf.Any").unwrap());
    any.set_field_by_name(
        "type_url",
        Value::String("type.googleapis.com/t.Missing".into()),
    );
    m.set_field_by_name("any", Value::Message(any));
    let mut w = Writer::new(Vec::new());
    let e = w.write(&m).unwrap_err();
    assert!(matches!(e.problem, Problem::EncodeFailed { .. }));
    assert_eq!(w.into_inner(), b"");
}
