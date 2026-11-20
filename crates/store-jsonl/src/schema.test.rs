//! The hand-written messages, the hand-built descriptor and the `.proto`
//! agree, and every message survives the trip through a JSON line.

use angzarr_proto_jsonl::{decode_line, encode_line};
use prost_types::field_descriptor_proto::{Label, Type};

use super::*;

const PROTO: &str = include_str!("../proto/store_jsonl/v1/journal.proto");

fn stream(edition: Option<&str>) -> Stream {
    Stream {
        domain: "orders".to_string(),
        edition: edition.map(str::to_string),
        root: "01010101-0101-0101-0101-010101010101".to_string(),
    }
}

fn at_sequence() -> Place {
    Place {
        at: Some(place::At::Sequence(9)),
    }
}

fn at_command() -> Place {
    Place {
        at: Some(place::At::Command(
            "02020202-0202-0202-0202-020202020202".to_string(),
        )),
    }
}

fn key() -> DeadLetterKey {
    DeadLetterKey {
        stream: Some(stream(Some("what-if"))),
        at: Some(at_command()),
        component: "policy".to_string(),
    }
}

/// An entry of every kind, each with every one of its fields set.
fn every_entry() -> Vec<Entry> {
    let op = |op| Entry { op: Some(op) };
    vec![
        op(entry::Op::Header(Header { format: FORMAT })),
        op(entry::Op::Write(Write {
            events: Some(Append {
                stream: Some(stream(None)),
                expected: 3,
                pages: vec![vec![1, 2, 3], vec![255, 0]],
            }),
            positions: vec![Position {
                handler: "projector".to_string(),
                stream: Some(stream(Some("what-if"))),
                next: 4,
            }],
            outbox: vec![OutboxEntry {
                id: "03030303-0303-0303-0303-030303030303".to_string(),
                origin: "policy".to_string(),
                message: vec![9, 9],
                edition: Some("what-if".to_string()),
            }],
            dead_letters: vec![
                DeadLetter {
                    stream: Some(stream(None)),
                    at: Some(at_sequence()),
                    letter: vec![7],
                },
                DeadLetter {
                    stream: Some(stream(None)),
                    at: Some(at_command()),
                    letter: vec![8],
                },
            ],
            cleared: vec!["05050505-0505-0505-0505-050505050505".to_string()],
        })),
        op(entry::Op::SnapshotWritten(SnapshotWritten {
            stream: Some(stream(None)),
            snapshot: vec![5, 6],
        })),
        op(entry::Op::Purged(Purged {
            stream: Some(stream(None)),
            expected: 12,
        })),
        op(entry::Op::EditionStreamRemoved(EditionStreamRemoved {
            stream: Some(stream(Some("what-if"))),
        })),
        op(entry::Op::OutboxCleared(OutboxCleared {
            ids: vec![
                "03030303-0303-0303-0303-030303030303".to_string(),
                "04040404-0404-0404-0404-040404040404".to_string(),
            ],
        })),
        op(entry::Op::DeadLettersDrained(DeadLettersDrained {
            stream: Some(stream(None)),
        })),
        op(entry::Op::DeadLettersRemoved(DeadLettersRemoved {
            keys: vec![key()],
        })),
    ]
}

#[test]
fn test_every_entry_survives_a_json_line() {
    let schema = Schema::new();
    for entry in every_entry() {
        let dynamic = schema.to_dynamic(&entry).expect("encodes");
        let line = encode_line(&dynamic).expect("a line");
        assert!(!line.contains('\n'), "{line}");
        let back = decode_line(schema.entry(), &line).expect("decodes");
        assert_eq!(schema.from_dynamic(&back).expect("typed"), entry, "{line}");
    }
}

#[test]
fn test_a_header_is_this_line() {
    let schema = Schema::new();
    let header = Entry {
        op: Some(entry::Op::Header(Header { format: FORMAT })),
    };
    let line = encode_line(&schema.to_dynamic(&header).unwrap()).unwrap();
    assert_eq!(line, r#"{"header":{"format":2}}"#);
}

#[test]
fn test_a_write_is_one_line_with_everything_it_changes() {
    let schema = Schema::new();
    let write = every_entry().remove(1);
    let line = encode_line(&schema.to_dynamic(&write).unwrap()).unwrap();
    for part in [
        r#""events":{"expected":"3","pages":["AQID","/wA="],"stream":{"domain":"orders","root":"01010101-0101-0101-0101-010101010101"}}"#,
        r#""positions":[{"handler":"projector","next":"4","#,
        r#""outbox":[{"edition":"what-if","id":"03030303-0303-0303-0303-030303030303","message":"CQk=","origin":"policy"}]"#,
        r#""deadLetters":["#,
    ] {
        assert!(line.contains(part), "{part} in {line}");
    }
}

#[test]
fn test_a_line_with_a_field_the_journal_lacks_is_refused() {
    let schema = Schema::new();
    assert!(decode_line(schema.entry(), r#"{"header":{"format":1,"extra":2}}"#).is_err());
    assert!(decode_line(schema.entry(), r#"{"nothing":{}}"#).is_err());
}

#[test]
fn test_json_names_are_lower_camel_case() {
    assert_eq!(json_name("dead_letters"), "deadLetters");
    assert_eq!(json_name("edition_stream_removed"), "editionStreamRemoved");
    assert_eq!(json_name("ids"), "ids");
}

/// The fields the `.proto` declares: (message, name, number, label, type),
/// every one of its messages' fields in the file's order.
fn proto_fields() -> Vec<(String, String, i32, String, String)> {
    let mut out = Vec::new();
    let mut message = String::new();
    let mut depth = 0;
    for line in PROTO.lines().map(str::trim) {
        if line.is_empty() || line.starts_with("//") {
            continue;
        }
        if let Some(name) = line.strip_prefix("message ") {
            message = name.trim_end_matches(" {").to_string();
            depth = 1;
        } else if line.starts_with("oneof ") {
            depth += 1;
        } else if line == "}" {
            depth -= 1;
        } else if depth > 0 {
            let tokens: Vec<&str> = line.trim_end_matches(';').split_whitespace().collect();
            let (label, rest) = match tokens[0] {
                l @ ("optional" | "repeated") => (l, &tokens[1..]),
                _ => ("", &tokens[..]),
            };
            assert_eq!(rest[2], "=", "{line}");
            out.push((
                message.clone(),
                rest[1].to_string(),
                rest[3].parse().expect("a field number"),
                label.to_string(),
                rest[0].to_string(),
            ));
        }
    }
    out
}

fn scalar_type(name: &str) -> Option<Type> {
    Some(match name {
        "string" => Type::String,
        "bytes" => Type::Bytes,
        "uint32" => Type::Uint32,
        "uint64" => Type::Uint64,
        _ => return None,
    })
}

#[test]
fn test_the_descriptor_holds_exactly_the_protos_fields() {
    let schema = Schema::new();
    let pool = schema.entry().parent_pool().clone();
    let declared = proto_fields();
    assert!(declared.len() > 30, "the proto was read: {declared:?}");
    let mut seen = 0;
    for (message, name, number, label, r#type) in &declared {
        let descriptor = pool
            .get_message_by_name(&format!("{PACKAGE}.{message}"))
            .unwrap_or_else(|| panic!("no message {message}"));
        let field = descriptor
            .get_field_by_name(name)
            .unwrap_or_else(|| panic!("no field {message}.{name}"));
        assert_eq!(field.number() as i32, *number, "{message}.{name}");
        assert_eq!(field.is_list(), label == "repeated", "{message}.{name}");
        let proto = field.field_descriptor_proto();
        assert_eq!(
            proto.proto3_optional.unwrap_or(false),
            label == "optional",
            "{message}.{name}"
        );
        match scalar_type(r#type) {
            Some(t) => assert_eq!(proto.r#type, Some(t as i32), "{message}.{name}"),
            None => assert_eq!(
                proto.type_name.as_deref(),
                Some(format!(".{PACKAGE}.{type}").as_str()),
                "{message}.{name}"
            ),
        }
        assert_ne!(proto.label, Some(Label::Required as i32));
        seen += 1;
    }
    // And nothing more: the descriptor's field count is the proto's.
    let in_descriptor: usize = file().message_type.iter().map(|m| m.field.len()).sum();
    assert_eq!(in_descriptor, seen);
    assert_eq!(file().message_type.len(), 16);
}

#[test]
fn test_the_file_is_a_proto3_file_of_the_journals_package() {
    let schema = Schema::new();
    let pool = schema.entry().parent_pool().clone();
    let file = pool
        .get_file_by_name("store_jsonl/v1/journal.proto")
        .expect("the file is in the pool");
    assert_eq!(file.package_name(), PACKAGE);
    assert_eq!(file.file_descriptor_proto().syntax(), "proto3");
}

#[test]
fn test_every_field_says_its_label_and_its_json_name() {
    let file = file();
    for message in &file.message_type {
        for field in &message.field {
            let name = field.name();
            let label = if matches!(
                (message.name(), name),
                ("Write", "positions" | "outbox" | "dead_letters" | "cleared")
                    | ("Append", "pages")
                    | ("OutboxCleared", "ids")
                    | ("DeadLettersRemoved", "keys")
            ) {
                Label::Repeated
            } else {
                Label::Optional
            };
            assert_eq!(field.label, Some(label as i32), "{}.{name}", message.name());
            assert_eq!(
                field.json_name.as_deref(),
                Some(json_name(name).as_str()),
                "{}.{name}",
                message.name()
            );
        }
    }
}

#[test]
fn test_the_oneofs_are_named() {
    let file = file();
    let oneofs = |message: &str| -> Vec<String> {
        file.message_type
            .iter()
            .find(|m| m.name() == message)
            .expect("the message")
            .oneof_decl
            .iter()
            .map(|o| o.name().to_string())
            .collect()
    };
    assert_eq!(oneofs("Entry"), ["op"]);
    assert_eq!(oneofs("Place"), ["at"]);
    assert_eq!(oneofs("Stream"), ["_edition"]);
    assert_eq!(oneofs("OutboxEntry"), ["_edition"]);
    assert_eq!(oneofs("Write"), ["_events"]);
    assert!(oneofs("Purged").is_empty());
}

#[test]
fn test_a_default_schema_is_the_schema() {
    assert_eq!(
        Schema::default().entry().full_name(),
        "store_jsonl.v1.Entry"
    );
}
