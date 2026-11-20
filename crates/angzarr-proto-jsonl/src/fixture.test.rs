//! The tests' descriptors, built by hand as a FileDescriptorProto so that no
//! protoc or .proto compiler is involved. The file is, in proto3:
//!
//! ```proto
//! package t;
//! enum Color { COLOR_UNSPECIFIED = 0; RED = 1; GREEN = 2; }
//! message Empty {}
//! message Inner { string name = 1; }
//! message Record {
//!   string text = 1;  int64 big = 2;  uint64 ubig = 3;  int32 small = 4;
//!   bool flag = 5;  double ratio = 6;  bytes blob = 7;  Color color = 8;
//!   repeated string tags = 9;  map<string, int64> counts = 10;
//!   Inner inner = 11;  repeated Inner inners = 12;
//!   oneof choice { string pick_text = 13; int64 pick_big = 14; }
//!   google.protobuf.Timestamp at = 15;  google.protobuf.Duration span = 16;
//!   google.protobuf.Any any = 17;  map<int32, string> names = 18;
//! }
//! ```

use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, MessageOptions, OneofDescriptorProto,
};

fn field(name: &str, number: i32, ty: Type, type_name: Option<&str>) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(ty as i32),
        type_name: type_name.map(Into::into),
        json_name: None,
        ..Default::default()
    }
}

fn repeated(mut f: FieldDescriptorProto) -> FieldDescriptorProto {
    f.label = Some(Label::Repeated as i32);
    f
}

fn entry(name: &str, key: Type, value: Type) -> DescriptorProto {
    DescriptorProto {
        name: Some(name.into()),
        field: vec![field("key", 1, key, None), field("value", 2, value, None)],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn pool() -> DescriptorPool {
    let mut in_oneof_text = field("pick_text", 13, Type::String, None);
    in_oneof_text.oneof_index = Some(0);
    let mut in_oneof_big = field("pick_big", 14, Type::Int64, None);
    in_oneof_big.oneof_index = Some(0);
    let record = DescriptorProto {
        name: Some("Record".into()),
        field: vec![
            field("text", 1, Type::String, None),
            field("big", 2, Type::Int64, None),
            field("ubig", 3, Type::Uint64, None),
            field("small", 4, Type::Int32, None),
            field("flag", 5, Type::Bool, None),
            field("ratio", 6, Type::Double, None),
            field("blob", 7, Type::Bytes, None),
            field("color", 8, Type::Enum, Some(".t.Color")),
            repeated(field("tags", 9, Type::String, None)),
            repeated(field(
                "counts",
                10,
                Type::Message,
                Some(".t.Record.CountsEntry"),
            )),
            field("inner", 11, Type::Message, Some(".t.Inner")),
            repeated(field("inners", 12, Type::Message, Some(".t.Inner"))),
            in_oneof_text,
            in_oneof_big,
            field("at", 15, Type::Message, Some(".google.protobuf.Timestamp")),
            field("span", 16, Type::Message, Some(".google.protobuf.Duration")),
            field("any", 17, Type::Message, Some(".google.protobuf.Any")),
            repeated(field(
                "names",
                18,
                Type::Message,
                Some(".t.Record.NamesEntry"),
            )),
        ],
        nested_type: vec![
            entry("CountsEntry", Type::String, Type::Int64),
            entry("NamesEntry", Type::Int32, Type::String),
        ],
        oneof_decl: vec![OneofDescriptorProto {
            name: Some("choice".into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let value = |name: &str, number: i32| EnumValueDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        ..Default::default()
    };
    let file = FileDescriptorProto {
        name: Some("t.proto".into()),
        package: Some("t".into()),
        syntax: Some("proto3".into()),
        dependency: vec![
            "google/protobuf/timestamp.proto".into(),
            "google/protobuf/duration.proto".into(),
            "google/protobuf/any.proto".into(),
        ],
        enum_type: vec![EnumDescriptorProto {
            name: Some("Color".into()),
            value: vec![
                value("COLOR_UNSPECIFIED", 0),
                value("RED", 1),
                value("GREEN", 2),
            ],
            ..Default::default()
        }],
        message_type: vec![
            DescriptorProto {
                name: Some("Empty".into()),
                ..Default::default()
            },
            DescriptorProto {
                name: Some("Inner".into()),
                field: vec![field("name", 1, Type::String, None)],
                ..Default::default()
            },
            record,
        ],
        ..Default::default()
    };
    // The global pool is prost-reflect's well-known types; this is a copy,
    // so the fixture adds to it without touching the global.
    let mut pool = DescriptorPool::global();
    pool.add_file_descriptor_proto(file)
        .expect("the fixture file is valid");
    pool
}

pub fn descriptor(pool: &DescriptorPool, name: &str) -> MessageDescriptor {
    pool.get_message_by_name(&format!("t.{name}"))
        .expect("the fixture names this message")
}

/// A message of the fixture's type `name`, read from canonical JSON.
pub fn message(pool: &DescriptorPool, name: &str, json: &str) -> DynamicMessage {
    let mut de = serde_json::Deserializer::from_str(json);
    DynamicMessage::deserialize(descriptor(pool, name), &mut de)
        .expect("the test's JSON is a message of that type")
}
