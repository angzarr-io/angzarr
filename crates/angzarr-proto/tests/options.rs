//! angzarr's custom options, readable from descriptors: prost does not
//! generate `extend` declarations, so a code generator reads option values
//! off the raw descriptor bytes, decoding each payload with the generated
//! message (`ComponentOptions`, `CommandOptions`, `FactOptions`,
//! `EventConsumer`, `RefusalStatus`, `ReasonOptions`). The rewrite numbers
//! the extensions contiguously from 50100 (decision 14.100), and a refusal's
//! status is a message option (decision 14.82), one of the six business
//! statuses, each numbered as its `google.rpc.Code`.

use std::path::{Path, PathBuf};
use std::process::Command;

use angzarr_proto::v1::RefusalStatus;
use prost::Message;
use prost_types::field_descriptor_proto::Type;
use prost_types::FileDescriptorSet;

fn proto_root() -> PathBuf {
    PathBuf::from(env!("ANGZARR_PROTO_ROOT"))
}

/// `file` (under the proto root) compiled alone by protoc.
fn compiled(file: &str, out: &str) -> FileDescriptorSet {
    let out = Path::new(env!("CARGO_TARGET_TMPDIR")).join(out);
    let protoc = std::env::var("PROTOC").unwrap_or_else(|_| "protoc".into());
    let status = Command::new(protoc)
        .arg(format!("-I{}", proto_root().display()))
        .arg(format!("--descriptor_set_out={}", out.display()))
        .arg(file)
        .status()
        .expect("run protoc");
    assert!(status.success(), "protoc failed on {file}");
    FileDescriptorSet::decode(&*std::fs::read(out).expect("read descriptor set"))
        .expect("decode descriptor set")
}

#[test]
fn the_extensions_are_numbered_from_50100() {
    let options = compiled("io/angzarr/v1/options.proto", "options.binpb");
    let reasons = compiled("io/angzarr/v1/reasons.proto", "reasons_ext.binpb");
    let found: Vec<(String, i32, String, Type, String)> = [&options, &reasons]
        .iter()
        .flat_map(|set| set.file[0].extension.iter())
        .map(|e| {
            (
                e.name().to_string(),
                e.number(),
                e.extendee().to_string(),
                e.r#type(),
                e.type_name().to_string(),
            )
        })
        .collect();
    let message = ".google.protobuf.MessageOptions";
    let expected = [
        (
            "component",
            50100,
            message,
            Type::Message,
            ".io.angzarr.v1.ComponentOptions",
        ),
        (
            "command",
            50101,
            message,
            Type::Message,
            ".io.angzarr.v1.CommandOptions",
        ),
        (
            "fact",
            50102,
            message,
            Type::Message,
            ".io.angzarr.v1.FactOptions",
        ),
        (
            "event",
            50103,
            message,
            Type::Message,
            ".io.angzarr.v1.EventConsumer",
        ),
        (
            "refusal_status",
            50104,
            message,
            Type::Enum,
            ".io.angzarr.v1.RefusalStatus",
        ),
        (
            "reason",
            50105,
            ".google.protobuf.EnumValueOptions",
            Type::Message,
            ".io.angzarr.v1.ReasonOptions",
        ),
    ]
    .map(|(n, num, ext, t, ty)| (n.to_string(), num, ext.to_string(), t, ty.to_string()));
    assert_eq!(found, expected);
}

#[test]
fn a_refusal_status_is_a_business_status_numbered_as_its_grpc_code() {
    let values: Vec<(i32, &str)> = (0..=16)
        .filter_map(|n| {
            RefusalStatus::try_from(n)
                .ok()
                .map(|s| (n, s.as_str_name()))
        })
        .collect();
    assert_eq!(
        values,
        [
            (0, "REFUSAL_STATUS_UNSPECIFIED"),
            (3, "REFUSAL_STATUS_INVALID_ARGUMENT"),
            (5, "REFUSAL_STATUS_NOT_FOUND"),
            (6, "REFUSAL_STATUS_ALREADY_EXISTS"),
            (7, "REFUSAL_STATUS_PERMISSION_DENIED"),
            (9, "REFUSAL_STATUS_FAILED_PRECONDITION"),
            (11, "REFUSAL_STATUS_OUT_OF_RANGE"),
        ]
    );
}
