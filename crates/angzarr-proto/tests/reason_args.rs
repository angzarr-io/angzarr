//! Each reason key's args and the spec's en-US catalog of the keys come from
//! the spec (decision 14.60): `args()` is generated from each value's
//! `(io.angzarr.v1.reason)` option, `reason_args` holds one constant per
//! name, and `REASONS_EN_US` is the spec's `reasons.en-US.ftl`: core's
//! keys and catalog (decision 14.64; each transport's are its crate's).

use std::collections::BTreeSet;
use std::path::PathBuf;

use angzarr_proto::v1::{reason_args, CoordinatorReason, HostReason, RouterReason};
use angzarr_proto::{reason_key_args, FILE_DESCRIPTOR_SET, REASONS_EN_US};
use prost::Message;
use prost_types::{EnumDescriptorProto, EnumValueDescriptorProto, FileDescriptorProto};

#[path = "../build_reasons.rs"]
mod build_reasons;
/// The generator itself, as build.rs runs it, with the descriptor reader it
/// reads through (`super::descriptor`).
#[path = "../build_descriptor.rs"]
mod descriptor;

/// Core's reasons.proto and its key enums, as build.rs names them.
const CORE: &str = "io/angzarr/v1/reasons.proto";
const ENUMS: &[&str] = &["RouterReason", "HostReason", "CoordinatorReason"];

/// Every live key, with its args, of all three enums: every value from 1
/// up to the highest number any enum uses, since an enum may reserve the
/// numbers of keys that moved to a transport (decision 14.64).
fn every_key() -> Vec<(&'static str, &'static [&'static str])> {
    fn live<E: TryFrom<i32> + Copy>(
        name: fn(E) -> &'static str,
        args: fn(E) -> &'static [&'static str],
    ) -> Vec<(&'static str, &'static [&'static str])> {
        (1..=64)
            .filter_map(|n| E::try_from(n).ok())
            .map(|e| (name(e), args(e)))
            .collect()
    }
    let mut keys = live::<RouterReason>(|r| r.as_str_name(), RouterReason::args);
    keys.extend(live::<HostReason>(|r| r.as_str_name(), HostReason::args));
    keys.extend(live::<CoordinatorReason>(
        |r| r.as_str_name(),
        CoordinatorReason::args,
    ));
    keys
}

#[test]
fn a_keys_args_are_its_options() {
    assert_eq!(
        RouterReason::AnyDecodeFailed.args(),
        &[reason_args::MESSAGE_TYPE, reason_args::EXT_ERR_TXT]
    );
    assert_eq!(
        RouterReason::DuplicateComponent.args(),
        &["component", "component_kind", "registered", "refused"]
    );
    assert_eq!(
        HostReason::DescriptorRulesVersionMismatch.args(),
        &[
            reason_args::COMPONENT,
            reason_args::DESCRIBED,
            reason_args::REPORTED
        ]
    );
    assert_eq!(HostReason::NoComponentsRegistered.args(), &[] as &[&str]);
    assert_eq!(
        CoordinatorReason::RulesVersionMismatch.args(),
        &["stream", "component", "registered", "reported"]
    );
    assert_eq!(
        CoordinatorReason::ExpectedSequenceStale.args(),
        &["stream", "expected", "actual"]
    );
}

#[test]
fn a_key_with_no_args_and_unspecified_have_none() {
    assert!(RouterReason::HandlerPanicked.args().is_empty());
    assert!(CoordinatorReason::RefusalFactAddressed.args().is_empty());
    assert!(RouterReason::Unspecified.args().is_empty());
    assert!(HostReason::Unspecified.args().is_empty());
    assert!(CoordinatorReason::Unspecified.args().is_empty());
}

#[test]
fn a_keys_args_are_found_by_its_name() {
    for (key, args) in every_key() {
        assert_eq!(reason_key_args(key), Some(args), "{key}");
    }
    for unknown in [
        "ROUTER_REASON_UNSPECIFIED",
        "HOST_REASON_UNSPECIFIED",
        "COORDINATOR_REASON_UNSPECIFIED",
        "ROUTER_REASON_NO_HANDLER_REGISTERED",
        // A transport's key is its own crate's (decision 14.64).
        "HOST_REASON_HANDSHAKE_MALFORMED",
        "COORDINATOR_REASON_PAYLOAD_UNAVAILABLE",
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
        .flat_map(|(_, args)| args.iter().copied())
        .collect();
    let constants = [
        reason_args::ACTUAL,
        reason_args::ACTUAL_DOMAIN,
        reason_args::ACTUAL_EDITION,
        reason_args::ATTEMPTS,
        reason_args::CAP,
        reason_args::CAUSE,
        reason_args::COMMAND_TYPE,
        reason_args::COMMAND_UNITS,
        reason_args::COMPONENT,
        reason_args::COMPONENT_KIND,
        reason_args::DESCRIBED,
        reason_args::DOMAIN,
        reason_args::EDITION,
        reason_args::EVENT_STREAM,
        reason_args::EXPECTED,
        reason_args::EXPECTED_DOMAIN,
        reason_args::EXPECTED_EDITION,
        reason_args::EXT_ERR_TXT,
        reason_args::FIELD,
        reason_args::FIRST_SEQUENCE,
        reason_args::HEAD,
        reason_args::INPUT_COUNT,
        reason_args::LAST_SEQUENCE,
        reason_args::LIMIT,
        reason_args::MESSAGE_TYPE,
        reason_args::REFUSAL_TYPE,
        reason_args::REFUSED,
        reason_args::REGISTERED,
        reason_args::REPORTED,
        reason_args::SEQUENCE,
        reason_args::STATE_TYPE_URL,
        reason_args::STREAM,
        reason_args::TYPE_URL,
        reason_args::UPCAST_TYPE_URL,
        reason_args::WINDOW_UNITS,
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
    let keys: BTreeSet<&str> = every_key().into_iter().map(|(k, _)| k).collect();
    assert_eq!(ids, keys);
}

#[test]
fn the_generator_reproduces_the_generated_code() {
    let generated = build_reasons::generate(FILE_DESCRIPTOR_SET, CORE, ENUMS);
    assert!(generated.contains("Self::AnyDecodeFailed => &[\"message_type\", \"ext_err_txt\"],"));
    assert!(generated.contains("Self::RefusalFactNotAFact => &[],"));
    assert!(generated.contains("    pub const EXT_ERR_TXT: &str = \"ext_err_txt\";"));
    assert_eq!(generated.matches("pub const fn args").count(), 3);
    assert_eq!(
        generated
            .matches("pub fn status(self) -> ReasonStatus")
            .count(),
        3
    );
    assert!(generated.contains("Self::PersistedEventCorrupt => 15,"));
}

// Decision 14.65: a key's status is its option's, read off the descriptor;
// READINESS for a readiness refusal, which never travels as a status.
#[test]
fn a_keys_status_is_its_options() {
    use angzarr_proto::v1::ReasonStatus;
    let pinned = [
        (
            RouterReason::PersistedEventCorrupt.status(),
            ReasonStatus::DataLoss,
        ),
        (
            RouterReason::NoComponentOfKind.status(),
            ReasonStatus::Unimplemented,
        ),
        (
            RouterReason::CommandRefused.status(),
            ReasonStatus::FailedPrecondition,
        ),
        (
            RouterReason::UnhandledHandlerError.status(),
            ReasonStatus::Internal,
        ),
        (
            HostReason::NoComponentsRegistered.status(),
            ReasonStatus::Internal,
        ),
        (
            CoordinatorReason::ExpectedSequenceStale.status(),
            ReasonStatus::Aborted,
        ),
        (
            CoordinatorReason::ExpectedSequenceAhead.status(),
            ReasonStatus::InvalidArgument,
        ),
        (
            CoordinatorReason::HandlerUnavailable.status(),
            ReasonStatus::Unavailable,
        ),
        (
            CoordinatorReason::MissingRulesVersion.status(),
            ReasonStatus::Readiness,
        ),
        (
            CoordinatorReason::EventsNotItsStream.status(),
            ReasonStatus::Internal,
        ),
        (
            CoordinatorReason::EditionNotForked.status(),
            ReasonStatus::FailedPrecondition,
        ),
        (
            CoordinatorReason::Unspecified.status(),
            ReasonStatus::Unspecified,
        ),
    ];
    for (status, expected) in pinned {
        assert_eq!(status, expected);
    }
    assert_eq!(
        ReasonStatus::Internal as i32,
        13,
        "a status is its google.rpc.Code number"
    );
}

#[test]
fn every_live_key_declares_a_status() {
    fn live<E: TryFrom<i32> + Copy>(
        name: fn(E) -> &'static str,
        status: fn(E) -> angzarr_proto::v1::ReasonStatus,
    ) -> Vec<(&'static str, angzarr_proto::v1::ReasonStatus)> {
        (1..=64)
            .filter_map(|n| E::try_from(n).ok())
            .map(|e| (name(e), status(e)))
            .collect()
    }
    let mut keys = live::<RouterReason>(|r| r.as_str_name(), RouterReason::status);
    keys.extend(live::<HostReason>(|r| r.as_str_name(), HostReason::status));
    keys.extend(live::<CoordinatorReason>(
        |r| r.as_str_name(),
        CoordinatorReason::status,
    ));
    for (key, status) in keys {
        assert_ne!(
            status,
            angzarr_proto::v1::ReasonStatus::Unspecified,
            "{key}"
        );
    }
}

// Decision 14.65: the coordinator's keys that concern one stream name it
// first.
#[test]
fn a_key_of_one_stream_names_it_first() {
    for key in [
        CoordinatorReason::ConflictUnresolved,
        CoordinatorReason::ConflictOverlap,
        CoordinatorReason::PmNoInstance,
        CoordinatorReason::ExpectedSequenceStale,
        CoordinatorReason::RetryCapExhausted,
        CoordinatorReason::ExpectedSequenceAhead,
        CoordinatorReason::EditionRemovalFailed,
        CoordinatorReason::HandlerUnavailable,
        CoordinatorReason::StoreConflict,
        CoordinatorReason::RulesVersionMismatch,
        CoordinatorReason::EventsNotItsStream,
        CoordinatorReason::EditionNotForked,
    ] {
        assert_eq!(key.args().first(), Some(&reason_args::STREAM), "{key:?}");
    }
    assert_eq!(
        CoordinatorReason::EventsNotItsStream.args(),
        &[reason_args::STREAM, reason_args::EVENT_STREAM]
    );
    assert_eq!(
        CoordinatorReason::EditionNotForked.args(),
        &[reason_args::STREAM, reason_args::EDITION]
    );
}

/// A descriptor set holding only `reasons.proto` with `enums`, encoded.
fn set_of(enums: Vec<EnumDescriptorProto>) -> Vec<u8> {
    prost_types::FileDescriptorSet {
        file: vec![
            FileDescriptorProto {
                name: Some("other.proto".into()),
                ..Default::default()
            },
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
        enum_of("RouterReason", &[("ROUTER_REASON_UNSPECIFIED", 0)]),
        enum_of("HostReason", &[("HOST_REASON_UNSPECIFIED", 0)]),
        enum_of(
            "CoordinatorReason",
            &[("COORDINATOR_REASON_UNSPECIFIED", 0)],
        ),
        enum_of("OtherEnum", &[("OTHER_X", 1)]),
    ]);
    let generated = build_reasons::generate(&set, CORE, ENUMS);
    assert_eq!(generated.matches("Self::Unspecified => &[],").count(), 3);
    assert!(!generated.contains("OtherEnum"));
}

#[test]
#[should_panic(expected = "HOST_REASON_EXTRA declares no args")]
fn a_live_key_without_its_option_fails_the_build() {
    let set = set_of(vec![
        enum_of("RouterReason", &[("ROUTER_REASON_UNSPECIFIED", 0)]),
        enum_of(
            "HostReason",
            &[("HOST_REASON_UNSPECIFIED", 0), ("HOST_REASON_EXTRA", 1)],
        ),
        enum_of(
            "CoordinatorReason",
            &[("COORDINATOR_REASON_UNSPECIFIED", 0)],
        ),
    ]);
    build_reasons::generate(&set, CORE, ENUMS);
}

#[test]
#[should_panic(expected = "io/angzarr/v1/reasons.proto's key enums")]
fn a_missing_enum_fails_the_build() {
    build_reasons::generate(
        &set_of(vec![enum_of(
            "RouterReason",
            &[("ROUTER_REASON_UNSPECIFIED", 0)],
        )]),
        CORE,
        ENUMS,
    );
}

#[test]
fn a_transports_enum_is_generated_from_its_own_file() {
    // Each transport's reasons.proto is generated alone (decision 14.64):
    // the one enum it declares, from its own path.
    let set = prost_types::FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("io/angzarr/transport/embedded/v1/reasons.proto".into()),
            enum_type: vec![enum_of("HostReason", &[("HOST_REASON_UNSPECIFIED", 0)])],
            ..Default::default()
        }],
    }
    .encode_to_vec();
    let generated = build_reasons::generate(
        &set,
        "io/angzarr/transport/embedded/v1/reasons.proto",
        &["HostReason"],
    );
    assert_eq!(generated.matches("pub const fn args").count(), 1);
    assert!(generated.contains("impl HostReason {"));
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

/// A descriptor set whose core reasons.proto holds one live key,
/// `HOST_REASON_EXTRA`, whose `(reason)` option (`ReasonOptions` on the
/// wire, extension 50109) is `reason`, with the other two key enums empty.
fn set_with_option(reason: &[u8]) -> Vec<u8> {
    let value = |name: &str, number: u8, options: Option<&[u8]>| {
        let mut v = delimited(1, name.as_bytes());
        v.extend([0x10, number]);
        if let Some(options) = options {
            v.extend(delimited(3, &delimited(50109, options)));
        }
        v
    };
    let key_enum = |name: &str, values: &[Vec<u8>]| {
        let mut e = delimited(1, name.as_bytes());
        for v in values {
            e.extend(delimited(2, v));
        }
        e
    };
    let mut file = delimited(1, CORE.as_bytes());
    for e in [
        key_enum(
            "RouterReason",
            &[value("ROUTER_REASON_UNSPECIFIED", 0, None)],
        ),
        key_enum(
            "HostReason",
            &[
                value("HOST_REASON_UNSPECIFIED", 0, None),
                value("HOST_REASON_EXTRA", 1, Some(reason)),
            ],
        ),
        key_enum(
            "CoordinatorReason",
            &[value("COORDINATOR_REASON_UNSPECIFIED", 0, None)],
        ),
    ] {
        file.extend(delimited(5, &e));
    }
    delimited(1, &file)
}

#[test]
fn a_live_keys_declared_status_is_generated() {
    // args ["line"], status 13 (INTERNAL).
    let mut reason = delimited(1, b"line");
    reason.extend([0x10, 13]);
    let generated = build_reasons::generate(&set_with_option(&reason), CORE, ENUMS);
    assert!(generated.contains("Self::Extra => &[\"line\"],"));
    assert!(generated.contains("Self::Extra => 13,"));
}

#[test]
#[should_panic(expected = "HOST_REASON_EXTRA declares no status")]
fn a_live_key_without_its_status_fails_the_build() {
    build_reasons::generate(&set_with_option(&delimited(1, b"line")), CORE, ENUMS);
}
