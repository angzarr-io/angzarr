//! The `(refusal_status)` option (options.proto, the refusal_status region;
//! decision 14.52): a refusal Reason value's status, one of the six
//! business statuses only, each numbered as its `google.rpc.Code`.

use std::path::{Path, PathBuf};
use std::process::Command;

use angzarr_proto::v1::RefusalStatus;
use prost::Message;
use prost_types::field_descriptor_proto::Type;
use prost_types::FileDescriptorSet;

/// Every RefusalStatus value, by number, until a number past the last
/// (google.rpc.Code's 16) has none.
fn values() -> Vec<(i32, &'static str)> {
    (0..=16)
        .filter_map(|n| {
            RefusalStatus::try_from(n)
                .ok()
                .map(|s| (n, s.as_str_name()))
        })
        .collect()
}

#[test]
fn a_refusal_status_is_a_business_status_numbered_as_its_grpc_code() {
    assert_eq!(
        values(),
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

fn proto_root() -> PathBuf {
    PathBuf::from(env!("ANGZARR_PROTO_ROOT"))
}

#[test]
fn refusal_status_extends_enum_value_options_at_50108() {
    let out = Path::new(env!("CARGO_TARGET_TMPDIR")).join("options_refusal_status.binpb");
    let protoc = std::env::var("PROTOC").unwrap_or_else(|_| "protoc".into());
    let status = Command::new(protoc)
        .arg(format!("-I{}", proto_root().display()))
        .arg(format!("--descriptor_set_out={}", out.display()))
        .arg("io/angzarr/v1/options.proto")
        .status()
        .expect("run protoc");
    assert!(status.success(), "protoc failed on options.proto");
    let set = FileDescriptorSet::decode(&*std::fs::read(out).expect("read descriptor set"))
        .expect("decode descriptor set");
    let file = &set.file[0];
    let ext = file
        .extension
        .iter()
        .find(|e| e.name() == "refusal_status")
        .expect("refusal_status is declared");
    assert_eq!(ext.number(), 50108);
    assert_eq!(ext.extendee(), ".google.protobuf.EnumValueOptions");
    assert_eq!(ext.r#type(), Type::Enum);
    assert_eq!(ext.type_name(), ".io.angzarr.v1.RefusalStatus");
}
