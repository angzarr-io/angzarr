//! Each reason key's args and the spec's en-US catalog of the keys come from
//! the spec (decision 14.60): `args()` is generated from each value's
//! `(io.angzarr.v1.reason)` option, `reason_args` holds one constant per
//! name, and `REASONS_EN_US` is the spec's `reasons.en-US.ftl`.

use std::collections::BTreeSet;
use std::path::PathBuf;

use angzarr_proto::v1::{reason_args, CoordinatorReason, HostReason, RouterReason};
use angzarr_proto::{reason_key_args, FILE_DESCRIPTOR_SET, REASONS_EN_US};
use prost::Message;
use prost_types::{EnumDescriptorProto, EnumValueDescriptorProto, FileDescriptorProto};

/// The generator itself, as build.rs runs it.
#[path = "../build_reasons.rs"]
#[allow(dead_code)]
mod build_reasons;

/// Every live key, with its args, of all three enums.
fn every_key() -> Vec<(&'static str, &'static [&'static str])> {
    fn live<E: TryFrom<i32> + Copy>(
        name: fn(E) -> &'static str,
        args: fn(E) -> &'static [&'static str],
    ) -> Vec<(&'static str, &'static [&'static str])> {
        (1..)
            .map_while(|n| E::try_from(n).ok())
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
    assert_eq!(HostReason::HandshakeMalformed.args(), &["line"]);
    assert_eq!(
        CoordinatorReason::RulesVersionMismatch.args(),
        &["component", "registered", "reported"]
    );
    assert_eq!(
        CoordinatorReason::ExpectedSequenceStale.args(),
        &["expected", "actual"]
    );
}

#[test]
fn a_key_with_no_args_and_unspecified_have_none() {
    assert!(RouterReason::HandlerPanicked.args().is_empty());
    assert!(CoordinatorReason::ConflictUnresolved.args().is_empty());
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
        reason_args::LINE,
        reason_args::MESSAGE_TYPE,
        reason_args::REFUSAL_TYPE,
        reason_args::REFUSED,
        reason_args::REGISTERED,
        reason_args::REPORTED,
        reason_args::SEQUENCE,
        reason_args::STATE_TYPE_URL,
        reason_args::TYPE_URL,
        reason_args::UPCAST_TYPE_URL,
        reason_args::URI,
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
    let generated = build_reasons::generate(FILE_DESCRIPTOR_SET);
    assert!(generated.contains("Self::AnyDecodeFailed => &[\"message_type\", \"ext_err_txt\"],"));
    assert!(generated.contains("Self::RefusalFactNotAFact => &[],"));
    assert!(generated.contains("    pub const EXT_ERR_TXT: &str = \"ext_err_txt\";"));
    assert_eq!(generated.matches("pub const fn args").count(), 3);
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
    let generated = build_reasons::generate(&set);
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
    build_reasons::generate(&set);
}

#[test]
#[should_panic(expected = "lacks one of")]
fn a_missing_enum_fails_the_build() {
    build_reasons::generate(&set_of(vec![enum_of(
        "RouterReason",
        &[("ROUTER_REASON_UNSPECIFIED", 0)],
    )]));
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
    );
}
