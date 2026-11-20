//! Each reason key's args and surface, and the spec's en-US catalog of the
//! keys, come from the spec (decisions 14.60 and 14.104): `args()` and
//! `surface()` are generated from each value's `(io.angzarr.v1.reason)`
//! option, `reason_args` holds one constant per name, and `REASONS_EN_US`
//! is the spec's `reasons.en-US.ftl`: core's keys and catalog (decision
//! 14.64; each transport's are its crate's).

use std::collections::BTreeSet;
use std::path::PathBuf;

use angzarr_proto::google::rpc::Code;
use angzarr_proto::v1::{reason_args, AngzarrReason};
use angzarr_proto::{reason_key_args, ReasonSurface, FILE_DESCRIPTOR_SET, REASONS_EN_US};
use prost::Message;
use prost_types::{EnumDescriptorProto, EnumValueDescriptorProto, FileDescriptorProto};

#[path = "../build_reasons.rs"]
mod build_reasons;
/// The generator itself, as build.rs runs it, with the descriptor reader it
/// reads through (`super::descriptor`).
#[path = "../build_descriptor.rs"]
mod descriptor;

/// Core's reasons.proto and its key enum, as build.rs names them.
const CORE: &str = "io/angzarr/v1/reasons.proto";
const ENUMS: &[&str] = &["AngzarrReason"];

/// Every live key, from 1 up to the first number with none.
fn every_key() -> Vec<AngzarrReason> {
    (1..)
        .map_while(|n| AngzarrReason::try_from(n).ok())
        .collect()
}

#[test]
fn a_keys_args_are_its_options() {
    assert_eq!(
        AngzarrReason::AnyDecodeFailed.args(),
        &[reason_args::MESSAGE_TYPE, reason_args::EXT_ERR_TXT]
    );
    assert_eq!(
        AngzarrReason::DuplicateComponent.args(),
        &["registered", "refused"]
    );
    assert_eq!(AngzarrReason::NoComponentsRegistered.args(), &[] as &[&str]);
    assert_eq!(
        AngzarrReason::RulesVersionMismatch.args(),
        &["stream", "component", "registered", "reported"]
    );
    assert_eq!(
        AngzarrReason::CommandRefused.args(),
        &[reason_args::REFUSAL_TYPE]
    );
}

#[test]
fn a_key_with_no_args_and_unspecified_have_none() {
    assert!(AngzarrReason::HandlerPanicked.args().is_empty());
    assert!(AngzarrReason::SyncNoProjectors.args().is_empty());
    assert!(AngzarrReason::Unspecified.args().is_empty());
}

#[test]
fn a_keys_args_are_found_by_its_name() {
    for key in every_key() {
        assert_eq!(
            reason_key_args(key.as_str_name()),
            Some(key.args()),
            "{key:?}"
        );
    }
    for unknown in [
        "ANGZARR_REASON_UNSPECIFIED",
        "ROUTER_REASON_UNKNOWN_COMMAND_TYPE",
        // A transport's key is its own crate's (decision 14.64).
        "EMBEDDED_REASON_CHILD_NOT_SERVING",
        "DISTRIBUTED_REASON_PAYLOAD_UNAVAILABLE",
        "ALREADY_BET",
        "",
    ] {
        assert_eq!(reason_key_args(unknown), None, "{unknown}");
    }
}

#[test]
fn every_declared_name_has_its_constant() {
    let names: BTreeSet<&str> = every_key()
        .into_iter()
        .flat_map(|key| key.args().iter().copied())
        .collect();
    let constants = [
        reason_args::ATTEMPTS,
        reason_args::CAP,
        reason_args::CAUSE,
        reason_args::COMMAND_TYPE,
        reason_args::COMPONENT,
        reason_args::COMPONENT_KIND,
        reason_args::DOMAIN,
        reason_args::EDITION,
        reason_args::EXT_ERR_TXT,
        reason_args::FIRST_SEQUENCE,
        reason_args::LAST_SEQUENCE,
        reason_args::LIMIT,
        reason_args::MESSAGE_TYPE,
        reason_args::PROJECTOR,
        reason_args::REFUSAL_TYPE,
        reason_args::REFUSED,
        reason_args::REGISTERED,
        reason_args::REPORTED,
        reason_args::SEQUENCE,
        reason_args::STATE_TYPE,
        reason_args::STATE_TYPE_URL,
        reason_args::STREAM,
        reason_args::TYPE_URL,
        reason_args::UPCAST_TYPE_URL,
        reason_args::UPCASTER,
    ];
    assert_eq!(names, constants.into_iter().collect::<BTreeSet<_>>());
}

#[test]
fn the_catalog_is_the_specs_file() {
    let path = PathBuf::from(env!("ANGZARR_PROTO_ROOT")).join("io/angzarr/v1/reasons.en-US.ftl");
    let file = std::fs::read_to_string(&path).expect("read the spec's catalog");
    assert_eq!(REASONS_EN_US, ("angzarr-reasons", file.as_str()));
}

#[test]
fn the_catalog_has_a_message_for_every_key_and_no_other() {
    let ids: BTreeSet<&str> = REASONS_EN_US
        .1
        .lines()
        .filter(|l| !l.starts_with('#') && !l.starts_with(' '))
        .filter_map(|l| l.split_once(" = ").map(|(id, _)| id))
        .collect();
    let keys: BTreeSet<&str> = every_key().into_iter().map(|r| r.as_str_name()).collect();
    assert_eq!(ids, keys);
}

#[test]
fn the_generator_reproduces_the_generated_code() {
    let generated = build_reasons::generate(FILE_DESCRIPTOR_SET, CORE, ENUMS);
    assert!(generated.contains("Self::AnyDecodeFailed => &[\"message_type\", \"ext_err_txt\"],"));
    assert!(generated.contains("    pub const EXT_ERR_TXT: &str = \"ext_err_txt\";"));
    assert_eq!(generated.matches("pub const fn args").count(), 1);
    assert_eq!(
        generated
            .matches("pub const fn surface(self) -> Option<crate::ReasonSurface>")
            .count(),
        1
    );
    assert!(generated.contains(
        "Self::PersistedEventCorrupt => Some(crate::ReasonSurface::GrpcStatus(crate::google::rpc::Code::DataLoss)),"
    ));
    assert!(
        generated.contains("Self::MissingRulesVersion => Some(crate::ReasonSurface::Readiness),")
    );
}

// Decisions 14.91 and 14.104: a key surfaces as its google.rpc.Code, or as
// a readiness refusal, which never travels as a status.
#[test]
fn a_keys_surface_is_its_options() {
    let status = |code| Some(ReasonSurface::GrpcStatus(code));
    let pinned = [
        (AngzarrReason::PersistedEventCorrupt, status(Code::DataLoss)),
        (
            AngzarrReason::NoComponentOfKind,
            status(Code::Unimplemented),
        ),
        (
            AngzarrReason::CommandRefused,
            status(Code::FailedPrecondition),
        ),
        (AngzarrReason::UnhandledHandlerError, status(Code::Internal)),
        (
            AngzarrReason::NoComponentsRegistered,
            status(Code::Internal),
        ),
        (AngzarrReason::HandlerUnavailable, status(Code::Unavailable)),
        (AngzarrReason::RetryCapExhausted, status(Code::Internal)),
        (AngzarrReason::FactDeadLettered, status(Code::Internal)),
        (
            AngzarrReason::EditionNotForked,
            status(Code::FailedPrecondition),
        ),
        (
            AngzarrReason::SyncProjectionFailed,
            status(Code::Unavailable),
        ),
        (
            AngzarrReason::SyncNoProjectors,
            status(Code::InvalidArgument),
        ),
        (
            AngzarrReason::MissingRulesVersion,
            Some(ReasonSurface::Readiness),
        ),
        (
            AngzarrReason::DescriptorNoKind,
            Some(ReasonSurface::Readiness),
        ),
        (AngzarrReason::Unspecified, None),
    ];
    for (key, expected) in pinned {
        assert_eq!(key.surface(), expected, "{key:?}");
    }
    assert_eq!(Code::Internal as i32, 13, "google.rpc.Code's own numbers");
}

#[test]
fn every_live_key_declares_a_surface_and_none_is_ok() {
    for key in every_key() {
        let surface = key.surface();
        assert!(surface.is_some(), "{key:?}");
        assert_ne!(
            surface,
            Some(ReasonSurface::GrpcStatus(Code::Ok)),
            "{key:?}"
        );
    }
}

// Decision 14.65: the coordinator's keys that concern one stream name it
// first.
#[test]
fn a_key_of_one_stream_names_it_first() {
    for key in [
        AngzarrReason::PmNoInstance,
        AngzarrReason::RetryCapExhausted,
        AngzarrReason::EditionRemovalFailed,
        AngzarrReason::HandlerUnavailable,
        AngzarrReason::RulesVersionMismatch,
        AngzarrReason::EditionNotForked,
        AngzarrReason::PolicyTargetsItsTrigger,
        AngzarrReason::FactDeadLettered,
        AngzarrReason::StateTypeMismatch,
        AngzarrReason::SyncProjectionFailed,
    ] {
        assert_eq!(key.args().first(), Some(&reason_args::STREAM), "{key:?}");
    }
    assert_eq!(
        AngzarrReason::EditionNotForked.args(),
        &[reason_args::STREAM, reason_args::EDITION]
    );
}

/// The vendored `google/rpc/code.proto`, as the real set holds it: every
/// crate's set includes it, for the generator to name a key's status.
fn google_rpc_code() -> FileDescriptorProto {
    prost_types::FileDescriptorSet::decode(FILE_DESCRIPTOR_SET)
        .unwrap()
        .file
        .into_iter()
        .find(|f| f.name() == "google/rpc/code.proto")
        .expect("google/rpc/code.proto in the set")
}

/// A descriptor set holding `reasons.proto` with `enums` (and
/// google/rpc/code.proto), encoded.
fn set_of(enums: Vec<EnumDescriptorProto>) -> Vec<u8> {
    prost_types::FileDescriptorSet {
        file: vec![
            FileDescriptorProto {
                name: Some("other.proto".into()),
                ..Default::default()
            },
            google_rpc_code(),
            FileDescriptorProto {
                name: Some("io/angzarr/v1/reasons.proto".into()),
                enum_type: enums,
                ..Default::default()
            },
        ],
    }
    .encode_to_vec()
}

fn enum_of(name: &str, values: &[(&str, i32)]) -> EnumDescriptorProto {
    EnumDescriptorProto {
        name: Some(name.into()),
        value: values
            .iter()
            .map(|(value, number)| EnumValueDescriptorProto {
                name: Some((*value).into()),
                number: Some(*number),
                options: None,
            })
            .collect(),
        ..Default::default()
    }
}

#[test]
fn unspecified_needs_no_option() {
    let set = set_of(vec![
        enum_of("AngzarrReason", &[("ANGZARR_REASON_UNSPECIFIED", 0)]),
        enum_of("OtherEnum", &[("OTHER_X", 1)]),
    ]);
    let generated = build_reasons::generate(&set, CORE, ENUMS);
    assert_eq!(generated.matches("Self::Unspecified => &[],").count(), 1);
    assert_eq!(generated.matches("Self::Unspecified => None,").count(), 1);
    assert!(!generated.contains("OtherEnum"));
}

#[test]
#[should_panic(expected = "ANGZARR_REASON_EXTRA declares no args")]
fn a_live_key_without_its_option_fails_the_build() {
    let set = set_of(vec![enum_of(
        "AngzarrReason",
        &[
            ("ANGZARR_REASON_UNSPECIFIED", 0),
            ("ANGZARR_REASON_EXTRA", 1),
        ],
    )]);
    build_reasons::generate(&set, CORE, ENUMS);
}

#[test]
#[should_panic(expected = "io/angzarr/v1/reasons.proto's key enums")]
fn a_missing_enum_fails_the_build() {
    build_reasons::generate(
        &set_of(vec![enum_of("OtherEnum", &[("OTHER_X", 0)])]),
        CORE,
        ENUMS,
    );
}

#[test]
fn a_transports_enum_is_generated_from_its_own_file() {
    // Each transport's reasons.proto is generated alone (decision 14.64):
    // the one enum it declares, from its own path, naming core's types
    // through its angzarr-proto dependency.
    let set = prost_types::FileDescriptorSet {
        file: vec![
            google_rpc_code(),
            FileDescriptorProto {
                name: Some("io/angzarr/transport/embedded/v1/reasons.proto".into()),
                enum_type: vec![enum_of(
                    "EmbeddedReason",
                    &[("EMBEDDED_REASON_UNSPECIFIED", 0)],
                )],
                ..Default::default()
            },
        ],
    }
    .encode_to_vec();
    let generated = build_reasons::generate(
        &set,
        "io/angzarr/transport/embedded/v1/reasons.proto",
        &["EmbeddedReason"],
    );
    assert_eq!(generated.matches("pub const fn args").count(), 1);
    assert!(generated.contains("impl EmbeddedReason {"));
    assert!(generated.contains("Option<::angzarr_proto::ReasonSurface>"));
}

#[test]
#[should_panic(expected = "is not in the descriptor set")]
fn a_set_without_reasons_proto_fails_the_build() {
    build_reasons::generate(
        &prost_types::FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("other.proto".into()),
                ..Default::default()
            }],
        }
        .encode_to_vec(),
        CORE,
        ENUMS,
    );
}

#[test]
fn a_packages_reason_keys_are_its_args_and_its_catalog_copied() {
    // What build.rs runs for a package with keys: its reasons.proto's args
    // to reason_args.rs, and the catalog beside it copied out unchanged.
    let scratch = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("reason_keys");
    let package = "io/angzarr/v1";
    let (root, out) = (scratch.join("proto"), scratch.join("out"));
    std::fs::create_dir_all(root.join(package)).unwrap();
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(root.join(package).join("reasons.en-US.ftl"), "KEY = text\n").unwrap();
    build_reasons::reason_keys(&root, &out, FILE_DESCRIPTOR_SET, package, ENUMS);
    assert_eq!(
        std::fs::read_to_string(out.join("reason_args.rs")).unwrap(),
        build_reasons::generate(FILE_DESCRIPTOR_SET, CORE, ENUMS)
    );
    assert_eq!(
        std::fs::read_to_string(out.join("reasons.en-US.ftl")).unwrap(),
        "KEY = text\n"
    );
}

#[test]
#[should_panic(expected = "before decision 14.64?")]
fn a_package_without_its_catalog_fails_the_build() {
    let scratch = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("reason_keys_missing");
    std::fs::create_dir_all(&scratch).unwrap();
    build_reasons::reason_keys(
        &scratch,
        &scratch,
        FILE_DESCRIPTOR_SET,
        "io/angzarr/v1",
        ENUMS,
    );
}

/// `bytes` as the length-delimited field `field`, on the wire.
fn delimited(field: u32, bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    prost::encoding::encode_key(field, prost::encoding::WireType::LengthDelimited, &mut out);
    prost::encoding::encode_varint(bytes.len() as u64, &mut out);
    out.extend_from_slice(bytes);
    out
}

/// The real descriptor set, with a core reasons.proto holding one live key,
/// `ANGZARR_REASON_EXTRA`, whose `(reason)` option (`ReasonOptions` on the
/// wire, extension 50105) is `reason`: google/rpc/code.proto stays in the
/// set, for the generator to name the status.
fn set_with_option(reason: &[u8]) -> Vec<u8> {
    let value = |name: &str, number: u8, options: Option<&[u8]>| {
        let mut v = delimited(1, name.as_bytes());
        v.extend([0x10, number]);
        if let Some(options) = options {
            v.extend(delimited(3, &delimited(50105, options)));
        }
        v
    };
    let mut key_enum = delimited(1, b"AngzarrReason");
    for v in [
        value("ANGZARR_REASON_UNSPECIFIED", 0, None),
        value("ANGZARR_REASON_EXTRA", 1, Some(reason)),
    ] {
        key_enum.extend(delimited(2, &v));
    }
    let mut file = delimited(1, CORE.as_bytes());
    file.extend(delimited(5, &key_enum));
    let mut set = prost_types::FileDescriptorSet {
        file: vec![google_rpc_code()],
    }
    .encode_to_vec();
    set.extend(delimited(1, &file));
    set
}

#[test]
fn a_live_keys_declared_status_is_generated() {
    // args ["line"], grpc_status 13 (INTERNAL).
    let mut reason = delimited(1, b"line");
    reason.extend([0x10, 13]);
    let generated = build_reasons::generate(&set_with_option(&reason), CORE, ENUMS);
    assert!(generated.contains("Self::Extra => &[\"line\"],"));
    assert!(generated.contains(
        "Self::Extra => Some(crate::ReasonSurface::GrpcStatus(crate::google::rpc::Code::Internal)),"
    ));
}

#[test]
fn a_readiness_key_is_generated() {
    // args [], readiness {} (field 3, empty).
    let generated = build_reasons::generate(&set_with_option(&[0x1a, 0]), CORE, ENUMS);
    assert!(generated.contains("Self::Extra => Some(crate::ReasonSurface::Readiness),"));
}

#[test]
#[should_panic(expected = "ANGZARR_REASON_EXTRA declares no surface")]
fn a_live_key_without_its_surface_fails_the_build() {
    build_reasons::generate(&set_with_option(&delimited(1, b"line")), CORE, ENUMS);
}

#[test]
#[should_panic(expected = "ANGZARR_REASON_EXTRA declares grpc_status OK")]
fn a_live_key_whose_status_is_ok_fails_the_build() {
    build_reasons::generate(&set_with_option(&[0x10, 0]), CORE, ENUMS);
}
