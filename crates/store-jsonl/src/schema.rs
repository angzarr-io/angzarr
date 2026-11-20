//! The journal's messages (`proto/store_jsonl/v1/journal.proto`) and the
//! descriptor the JSON Lines codec reads and writes them against.
//!
//! The messages are prost structs written by hand, and the descriptor is
//! built by hand to match, so building the crate needs no `protoc`; the
//! tests hold the three (this file, the descriptor, the `.proto`) to one
//! another: every field of the `.proto` is in the descriptor under its name
//! and number, and a message with every field set survives the trip through
//! JSON.

use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    OneofDescriptorProto,
};

/// The journal format this build writes: the header's `format`. Format 2
/// journals a write's `cleared` ids; a format 1 journal has none, and is read
/// as before and made format 2 when it is opened.
pub const FORMAT: u32 = 2;

/// The oldest journal format this build reads.
pub const OLDEST_FORMAT: u32 = 1;

/// The proto package of the journal's messages.
const PACKAGE: &str = "store_jsonl.v1";

/// One line of the journal.
#[derive(Clone, PartialEq, Message)]
pub struct Entry {
    #[prost(oneof = "entry::Op", tags = "1, 2, 3, 4, 5, 6, 7, 8")]
    pub op: Option<entry::Op>,
}

pub mod entry {
    use prost::Oneof;

    /// What the line records.
    #[derive(Clone, PartialEq, Oneof)]
    pub enum Op {
        #[prost(message, tag = "1")]
        Header(super::Header),
        #[prost(message, tag = "2")]
        Write(super::Write),
        #[prost(message, tag = "3")]
        SnapshotWritten(super::SnapshotWritten),
        #[prost(message, tag = "4")]
        Purged(super::Purged),
        #[prost(message, tag = "5")]
        EditionStreamRemoved(super::EditionStreamRemoved),
        #[prost(message, tag = "6")]
        OutboxCleared(super::OutboxCleared),
        #[prost(message, tag = "7")]
        DeadLettersDrained(super::DeadLettersDrained),
        #[prost(message, tag = "8")]
        DeadLettersRemoved(super::DeadLettersRemoved),
    }
}

/// The journal's first line.
#[derive(Clone, PartialEq, Message)]
pub struct Header {
    #[prost(uint32, tag = "1")]
    pub format: u32,
}

/// One stream.
#[derive(Clone, PartialEq, Message)]
pub struct Stream {
    #[prost(string, tag = "1")]
    pub domain: String,
    #[prost(string, optional, tag = "2")]
    pub edition: Option<String>,
    #[prost(string, tag = "3")]
    pub root: String,
}

/// One handling step's write.
#[derive(Clone, PartialEq, Message)]
pub struct Write {
    #[prost(message, optional, tag = "1")]
    pub events: Option<Append>,
    #[prost(message, repeated, tag = "2")]
    pub positions: Vec<Position>,
    #[prost(message, repeated, tag = "3")]
    pub outbox: Vec<OutboxEntry>,
    #[prost(message, repeated, tag = "4")]
    pub dead_letters: Vec<DeadLetter>,
    #[prost(string, repeated, tag = "5")]
    pub cleared: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Append {
    #[prost(message, optional, tag = "1")]
    pub stream: Option<Stream>,
    #[prost(uint64, tag = "2")]
    pub expected: u64,
    #[prost(bytes = "vec", repeated, tag = "3")]
    pub pages: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Position {
    #[prost(string, tag = "1")]
    pub handler: String,
    #[prost(message, optional, tag = "2")]
    pub stream: Option<Stream>,
    #[prost(uint64, tag = "3")]
    pub next: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct OutboxEntry {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(string, tag = "2")]
    pub origin: String,
    #[prost(bytes = "vec", tag = "3")]
    pub message: Vec<u8>,
    #[prost(string, optional, tag = "4")]
    pub edition: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct DeadLetter {
    #[prost(message, optional, tag = "1")]
    pub stream: Option<Stream>,
    #[prost(message, optional, tag = "2")]
    pub at: Option<Place>,
    #[prost(bytes = "vec", tag = "3")]
    pub letter: Vec<u8>,
}

/// Where on its stream a dead letter is.
#[derive(Clone, PartialEq, Message)]
pub struct Place {
    #[prost(oneof = "place::At", tags = "1, 2")]
    pub at: Option<place::At>,
}

pub mod place {
    use prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum At {
        #[prost(uint64, tag = "1")]
        Sequence(u64),
        #[prost(string, tag = "2")]
        Command(String),
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct DeadLetterKey {
    #[prost(message, optional, tag = "1")]
    pub stream: Option<Stream>,
    #[prost(message, optional, tag = "2")]
    pub at: Option<Place>,
    #[prost(string, tag = "3")]
    pub component: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct SnapshotWritten {
    #[prost(message, optional, tag = "1")]
    pub stream: Option<Stream>,
    #[prost(bytes = "vec", tag = "2")]
    pub snapshot: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Purged {
    #[prost(message, optional, tag = "1")]
    pub stream: Option<Stream>,
    #[prost(uint64, tag = "2")]
    pub expected: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct EditionStreamRemoved {
    #[prost(message, optional, tag = "1")]
    pub stream: Option<Stream>,
}

#[derive(Clone, PartialEq, Message)]
pub struct OutboxCleared {
    #[prost(string, repeated, tag = "1")]
    pub ids: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct DeadLettersDrained {
    #[prost(message, optional, tag = "1")]
    pub stream: Option<Stream>,
}

#[derive(Clone, PartialEq, Message)]
pub struct DeadLettersRemoved {
    #[prost(message, repeated, tag = "1")]
    pub keys: Vec<DeadLetterKey>,
}

// ---------------------------------------------------------------- descriptor

/// How a field is declared, for [`message`].
enum Kind {
    Scalar(Type),
    Message(&'static str),
}

/// A singular field.
fn field(name: &str, number: i32, kind: Kind) -> FieldDescriptorProto {
    let (r#type, type_name) = match kind {
        Kind::Scalar(t) => (t, None),
        Kind::Message(m) => (Type::Message, Some(format!(".{PACKAGE}.{m}"))),
    };
    FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(r#type as i32),
        type_name,
        json_name: Some(json_name(name)),
        ..FieldDescriptorProto::default()
    }
}

/// A repeated field.
fn repeated(name: &str, number: i32, kind: Kind) -> FieldDescriptorProto {
    FieldDescriptorProto {
        label: Some(Label::Repeated as i32),
        ..field(name, number, kind)
    }
}

/// A member of the message's oneof number `index`.
fn member(index: i32, mut f: FieldDescriptorProto) -> FieldDescriptorProto {
    f.oneof_index = Some(index);
    f
}

/// A proto3 `optional` field: a member of a synthetic oneof `_<name>`,
/// number `index`.
fn optional(index: i32, mut f: FieldDescriptorProto) -> FieldDescriptorProto {
    f.oneof_index = Some(index);
    f.proto3_optional = Some(true);
    f
}

/// The lowerCamelCase protoc gives a field's JSON name.
fn json_name(name: &str) -> String {
    let mut out = String::new();
    let mut upper = false;
    for c in name.chars() {
        if c == '_' {
            upper = true;
        } else if upper {
            out.extend(c.to_uppercase());
            upper = false;
        } else {
            out.push(c);
        }
    }
    out
}

fn message(name: &str, fields: Vec<FieldDescriptorProto>, oneofs: &[&str]) -> DescriptorProto {
    DescriptorProto {
        name: Some(name.to_string()),
        field: fields,
        oneof_decl: oneofs
            .iter()
            .map(|o| OneofDescriptorProto {
                name: Some((*o).to_string()),
                ..OneofDescriptorProto::default()
            })
            .collect(),
        ..DescriptorProto::default()
    }
}

fn message_field(name: &str, number: i32, of: &'static str) -> FieldDescriptorProto {
    field(name, number, Kind::Message(of))
}

fn scalar(name: &str, number: i32, t: Type) -> FieldDescriptorProto {
    field(name, number, Kind::Scalar(t))
}

/// The journal's file descriptor, the `.proto` as a descriptor.
fn file() -> FileDescriptorProto {
    let mut messages = vec![
        entry_message(),
        message("Header", vec![scalar("format", 1, Type::Uint32)], &[]),
    ];
    messages.extend(write_messages());
    messages.extend(operation_messages());
    FileDescriptorProto {
        name: Some("store_jsonl/v1/journal.proto".to_string()),
        package: Some(PACKAGE.to_string()),
        syntax: Some("proto3".to_string()),
        message_type: messages,
        ..FileDescriptorProto::default()
    }
}

fn stream(number: i32) -> FieldDescriptorProto {
    message_field("stream", number, "Stream")
}

fn place(number: i32) -> FieldDescriptorProto {
    message_field("at", number, "Place")
}

/// `Entry`: one operation, numbered in the order of the `.proto`.
fn entry_message() -> DescriptorProto {
    let operations = [
        ("header", "Header"),
        ("write", "Write"),
        ("snapshot_written", "SnapshotWritten"),
        ("purged", "Purged"),
        ("edition_stream_removed", "EditionStreamRemoved"),
        ("outbox_cleared", "OutboxCleared"),
        ("dead_letters_drained", "DeadLettersDrained"),
        ("dead_letters_removed", "DeadLettersRemoved"),
    ];
    let fields = operations
        .into_iter()
        .zip(1..)
        .map(|((name, of), number)| member(0, message_field(name, number, of)))
        .collect();
    message("Entry", fields, &["op"])
}

/// A stream, and the messages a write is made of.
fn write_messages() -> Vec<DescriptorProto> {
    vec![
        message(
            "Stream",
            vec![
                scalar("domain", 1, Type::String),
                optional(0, scalar("edition", 2, Type::String)),
                scalar("root", 3, Type::String),
            ],
            &["_edition"],
        ),
        message(
            "Write",
            vec![
                optional(0, message_field("events", 1, "Append")),
                repeated("positions", 2, Kind::Message("Position")),
                repeated("outbox", 3, Kind::Message("OutboxEntry")),
                repeated("dead_letters", 4, Kind::Message("DeadLetter")),
                repeated("cleared", 5, Kind::Scalar(Type::String)),
            ],
            &["_events"],
        ),
        message(
            "Append",
            vec![
                stream(1),
                scalar("expected", 2, Type::Uint64),
                repeated("pages", 3, Kind::Scalar(Type::Bytes)),
            ],
            &[],
        ),
        message(
            "Position",
            vec![
                scalar("handler", 1, Type::String),
                stream(2),
                scalar("next", 3, Type::Uint64),
            ],
            &[],
        ),
        message(
            "OutboxEntry",
            vec![
                scalar("id", 1, Type::String),
                scalar("origin", 2, Type::String),
                scalar("message", 3, Type::Bytes),
                optional(0, scalar("edition", 4, Type::String)),
            ],
            &["_edition"],
        ),
        message(
            "DeadLetter",
            vec![stream(1), place(2), scalar("letter", 3, Type::Bytes)],
            &[],
        ),
    ]
}

/// The place of a dead letter, and the messages of the other operations.
fn operation_messages() -> Vec<DescriptorProto> {
    vec![
        message(
            "Place",
            vec![
                member(0, scalar("sequence", 1, Type::Uint64)),
                member(0, scalar("command", 2, Type::String)),
            ],
            &["at"],
        ),
        message(
            "DeadLetterKey",
            vec![stream(1), place(2), scalar("component", 3, Type::String)],
            &[],
        ),
        message(
            "SnapshotWritten",
            vec![stream(1), scalar("snapshot", 2, Type::Bytes)],
            &[],
        ),
        message(
            "Purged",
            vec![stream(1), scalar("expected", 2, Type::Uint64)],
            &[],
        ),
        message("EditionStreamRemoved", vec![stream(1)], &[]),
        message(
            "OutboxCleared",
            vec![repeated("ids", 1, Kind::Scalar(Type::String))],
            &[],
        ),
        message("DeadLettersDrained", vec![stream(1)], &[]),
        message(
            "DeadLettersRemoved",
            vec![repeated("keys", 1, Kind::Message("DeadLetterKey"))],
            &[],
        ),
    ]
}

/// The journal's descriptors: its own pool, holding the journal's messages
/// and nothing else.
#[derive(Clone, Debug)]
pub struct Schema {
    entry: MessageDescriptor,
}

impl Default for Schema {
    fn default() -> Self {
        Schema::new()
    }
}

impl Schema {
    pub fn new() -> Schema {
        let set = FileDescriptorSet { file: vec![file()] };
        let pool = DescriptorPool::from_file_descriptor_set(set)
            .expect("the journal's descriptor is a valid file");
        let entry = pool
            .get_message_by_name(&format!("{PACKAGE}.Entry"))
            .expect("the journal's descriptor holds Entry");
        Schema { entry }
    }

    /// The descriptor a journal line is read against.
    pub fn entry(&self) -> &MessageDescriptor {
        &self.entry
    }

    /// The entry as a dynamic message, for the codec.
    pub fn to_dynamic(&self, entry: &Entry) -> Result<DynamicMessage, prost::DecodeError> {
        DynamicMessage::decode(self.entry.clone(), entry.encode_to_vec().as_slice())
    }

    /// The entry a dynamic message of the journal's `Entry` holds.
    pub fn from_dynamic(&self, message: &DynamicMessage) -> Result<Entry, prost::DecodeError> {
        Entry::decode(message.encode_to_vec().as_slice())
    }
}

#[cfg(test)]
#[path = "schema.test.rs"]
mod tests;
