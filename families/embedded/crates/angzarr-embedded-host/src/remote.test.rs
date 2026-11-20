use std::collections::BTreeMap;

use crate::rpc::google_rpc::{ErrorInfo, Status as RpcStatus, ERROR_INFO_TYPE_URL};
use angzarr_embedded::{class, Class};
use angzarr_proto::v1::reason_args as args;
use angzarr_router::error::AngzarrReason;
use angzarr_router::error::GrpcCode;
use prost::Message;
use prost_types::Any;
use tonic::Code;

use super::*;

/// A status as a child answers it: code and message, and its details the
/// encoded `google.rpc.Status` carrying them again with the details.
pub(crate) fn status_with_details(code: Code, message: &str, details: Vec<Any>) -> tonic::Status {
    let rpc = RpcStatus {
        code: code as i32,
        message: message.to_string(),
        details,
    };
    tonic::Status::with_details(code, message, rpc.encode_to_vec().into())
}

pub(crate) fn error_info(reason: &str, metadata: &[(&str, &str)]) -> Any {
    let info = ErrorInfo {
        reason: reason.to_string(),
        domain: angzarr_router::error::ERROR_INFO_DOMAIN.to_string(),
        metadata: metadata
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    };
    Any {
        type_url: ERROR_INFO_TYPE_URL.to_string(),
        value: info.encode_to_vec(),
    }
}

fn refusal() -> Any {
    Any {
        type_url: "/examples.blackjack.TableFull".to_string(),
        value: vec![8, 6],
    }
}

/// The typed refusal travels as the status's other detail; the router gets
/// it as `CodedError::refusal`, a FAILED_PRECONDITION, so its
/// `refusal_response` is what the refused policy is given.
#[test]
fn a_failed_precondition_is_the_routers_refusal() {
    let status = status_with_details(
        Code::FailedPrecondition,
        "table is full",
        vec![error_info("TABLE_FULL", &[("seats", "6")]), refusal()],
    );
    let coded = status_to_coded("player", &status);
    assert_eq!(
        coded,
        CodedError {
            code: "TABLE_FULL".to_string(),
            extras: BTreeMap::from([("seats".to_string(), "6".to_string())]),
            grpc: GrpcCode::FailedPrecondition,
            refusal: Some(Box::new(refusal())),
        }
    );
    assert!(!is_retryable(&coded));
}

/// The detail order is not the contract: the refusal is the detail that is
/// not the ErrorInfo.
#[test]
fn the_refusal_is_found_before_the_error_info_too() {
    let status = status_with_details(
        Code::FailedPrecondition,
        "table is full",
        vec![refusal(), error_info("TABLE_FULL", &[])],
    );
    let coded = status_to_coded("player", &status);
    assert_eq!(coded.code, "TABLE_FULL");
    assert_eq!(coded.refusal, Some(Box::new(refusal())));
}

#[test]
fn an_unavailable_status_is_retryable() {
    let coded = status_to_coded("player", &tonic::Status::unavailable("connection refused"));
    assert_eq!(
        coded.code,
        angzarr_proto::v1::AngzarrReason::HandlerUnavailable.as_str_name()
    );
    assert_eq!(
        coded.extras,
        BTreeMap::from([
            (args::DOMAIN.to_string(), "player".to_string()),
            (
                angzarr_proto::v1::reason_args::CAUSE.to_string(),
                causes::TRANSPORT.to_string()
            ),
            // The transport's own text, verbatim: a third party's.
            (
                angzarr_proto::v1::reason_args::EXT_ERR_TXT.to_string(),
                "connection refused".to_string()
            ),
        ])
    );
    assert!(is_retryable(&coded));
}

#[test]
fn a_child_not_running_is_retryable() {
    let coded = unavailable("player", causes::NOT_RUNNING);
    assert_eq!(
        coded,
        CodedError {
            code: angzarr_proto::v1::AngzarrReason::HandlerUnavailable
                .as_str_name()
                .to_string(),
            extras: BTreeMap::from([
                (args::DOMAIN.to_string(), "player".to_string()),
                (
                    angzarr_proto::v1::reason_args::CAUSE.to_string(),
                    causes::NOT_RUNNING.to_string()
                ),
            ]),
            grpc: GrpcCode::Unavailable,
            refusal: None,
        }
    );
    assert!(is_retryable(&coded));
}

/// An unreachable remote handler is answered under the spec's key
/// (decision 14.49): ANGZARR_REASON_HANDLER_UNAVAILABLE, UNAVAILABLE, a
/// retryable bug.
#[test]
fn unavailability_is_the_specs_key() {
    assert_eq!(
        unavailable("player", causes::TIMEOUT).code,
        "ANGZARR_REASON_HANDLER_UNAVAILABLE"
    );
    assert_eq!(
        unavailable("player", causes::TIMEOUT).grpc,
        GrpcCode::Unavailable
    );
}

/// Another coded failure keeps its code and its gRPC code, and is not
/// retried: it would fail the same way again. The router's own error on
/// malformed input arrives INTERNAL, a bug (the owner's ruling,
/// 2026-10-29).
#[test]
fn another_status_keeps_its_code() {
    let status = status_with_details(
        Code::Internal,
        "ANGZARR_REASON_UNKNOWN_COMMAND_TYPE",
        vec![error_info(
            AngzarrReason::UnknownCommandType.as_str_name(),
            &[("type_url", "/x.Y")],
        )],
    );
    let coded = status_to_coded("player", &status);
    assert_eq!(coded.code, AngzarrReason::UnknownCommandType.as_str_name());
    assert_eq!(
        coded.extras,
        BTreeMap::from([("type_url".to_string(), "/x.Y".to_string())])
    );
    assert_eq!(coded.grpc, GrpcCode::Internal);
    assert_eq!(coded.refusal, None);
    assert!(!is_retryable(&coded));
}

/// A status with no ErrorInfo is a failure no one classified.
#[test]
fn a_status_without_error_info_is_unhandled() {
    let coded = status_to_coded("player", &tonic::Status::internal("boom"));
    assert_eq!(
        coded.code,
        AngzarrReason::UnhandledHandlerError.as_str_name()
    );
    assert_eq!(coded.grpc, GrpcCode::Internal);
    assert!(!is_retryable(&coded));
}

/// Every gRPC code the router has a variant for survives exactly (its
/// `GrpcCode` has each status of the business set, and UNKNOWN), so the
/// status decides the class (decision 14.37). A status it has no variant
/// for (CANCELLED, UNAUTHENTICATED) keeps its class: a retryable bug (the
/// owner's ruling, 2026-10-29).
#[test]
fn each_status_code_maps_to_the_routers() {
    for (code, grpc) in [
        (Code::InvalidArgument, GrpcCode::InvalidArgument),
        (Code::NotFound, GrpcCode::NotFound),
        (Code::FailedPrecondition, GrpcCode::FailedPrecondition),
        (Code::AlreadyExists, GrpcCode::AlreadyExists),
        (Code::OutOfRange, GrpcCode::OutOfRange),
        (Code::PermissionDenied, GrpcCode::PermissionDenied),
        (Code::Aborted, GrpcCode::Aborted),
        (Code::Unimplemented, GrpcCode::Unimplemented),
        (Code::DataLoss, GrpcCode::DataLoss),
        (Code::Internal, GrpcCode::Internal),
        (Code::Unknown, GrpcCode::Unknown),
        (Code::Unavailable, GrpcCode::Unavailable),
        (Code::DeadlineExceeded, GrpcCode::DeadlineExceeded),
        (Code::ResourceExhausted, GrpcCode::ResourceExhausted),
    ] {
        let status = status_with_details(code, "m", vec![error_info("SOME_REASON", &[])]);
        assert_eq!(status_to_coded("player", &status).grpc, grpc, "{code:?}");
    }
    for code in [Code::Cancelled, Code::Unauthenticated] {
        let status = status_with_details(code, "m", vec![error_info("SOME_REASON", &[])]);
        assert_eq!(
            class(status_to_coded("player", &status).grpc as i32),
            Class::BugRetryable,
            "{code:?}"
        );
    }
}

/// No text crosses the boundary (decision 14.37): the child's status
/// message is never forwarded, in a field or an arg; the reason key and
/// its metadata are what the router gets.
#[test]
fn a_childs_status_message_is_not_forwarded() {
    for code in [Code::FailedPrecondition, Code::Internal, Code::Aborted] {
        let status = status_with_details(
            code,
            "the table is full, try another",
            vec![error_info("TABLE_FULL", &[("seats", "6")])],
        );
        let coded = status_to_coded("player", &status);
        assert!(
            !format!("{coded:?}").contains("the table is full"),
            "{coded:?}"
        );
        assert_eq!(coded.to_string(), "TABLE_FULL seats=6");
    }
}

/// A child's refusal under `code`, a business status (the owner's ruling,
/// 2026-10-29): its reason, metadata and typed refusal kept, under `grpc`,
/// and not retried.
fn refused_under(code: Code, grpc: GrpcCode) {
    let status = status_with_details(
        code,
        "The seat is taken",
        vec![error_info("SEAT_TAKEN", &[("seat", "3")]), refusal()],
    );
    let coded = status_to_coded("player", &status);
    assert_eq!(coded.code, "SEAT_TAKEN", "{code:?}");
    assert_eq!(
        coded.extras,
        BTreeMap::from([("seat".to_string(), "3".to_string())]),
        "{code:?}"
    );
    assert_eq!(coded.grpc, grpc, "{code:?}");
    assert_eq!(coded.refusal.as_deref(), Some(&refusal()), "{code:?}");
    assert!(!is_retryable(&coded), "{code:?}");
}

#[test]
fn a_childs_invalid_argument_is_a_refusal_with_its_object() {
    refused_under(Code::InvalidArgument, GrpcCode::InvalidArgument);
}

#[test]
fn a_childs_not_found_is_a_refusal_with_its_object() {
    refused_under(Code::NotFound, GrpcCode::NotFound);
}

/// ALREADY_EXISTS is a business status: the refusal arrives as itself.
#[test]
fn a_childs_already_exists_is_a_refusal_with_its_object() {
    refused_under(Code::AlreadyExists, GrpcCode::AlreadyExists);
}

/// OUT_OF_RANGE is a business status: the refusal arrives as itself.
#[test]
fn a_childs_out_of_range_is_a_refusal_with_its_object() {
    refused_under(Code::OutOfRange, GrpcCode::OutOfRange);
}

/// PERMISSION_DENIED is a business status: the refusal arrives as itself.
#[test]
fn a_childs_permission_denied_is_a_refusal_with_its_object() {
    refused_under(Code::PermissionDenied, GrpcCode::PermissionDenied);
}

/// A child's bug (INTERNAL, UNKNOWN, DATA_LOSS) keeps its reason and its
/// status, carries no typed refusal, and is not retried.
#[test]
fn a_childs_bug_status_is_no_refusal() {
    for (code, grpc) in [
        (Code::Internal, GrpcCode::Internal),
        (Code::Unknown, GrpcCode::Unknown),
        (Code::DataLoss, GrpcCode::DataLoss),
    ] {
        let status = status_with_details(
            code,
            "broke",
            vec![error_info("PLAYER_BROKE", &[]), refusal()],
        );
        let coded = status_to_coded("player", &status);
        assert_eq!(coded.code, "PLAYER_BROKE", "{code:?}");
        assert_eq!(coded.grpc, grpc, "{code:?}");
        assert_eq!(coded.refusal, None, "{code:?}");
        assert!(!is_retryable(&coded), "{code:?}");
    }
}

/// The retried statuses keep their class (decision 14.37): a child's
/// UNAVAILABLE with no details is unavailability; one it sent with its
/// reason, and a DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED or UNIMPLEMENTED,
/// keeps the reason, a retryable bug; an ABORTED keeps the reason, a
/// business answer to return at once. None carries a typed refusal.
#[test]
fn a_childs_retryable_status_keeps_its_class() {
    let bare = status_to_coded("player", &tonic::Status::unavailable("gone"));
    assert_eq!(
        bare.code,
        angzarr_proto::v1::AngzarrReason::HandlerUnavailable.as_str_name()
    );
    assert!(is_retryable(&bare));
    for code in [
        Code::Unavailable,
        Code::DeadlineExceeded,
        Code::ResourceExhausted,
        Code::Aborted,
        Code::Unimplemented,
    ] {
        let status =
            status_with_details(code, "m", vec![error_info("PLAYER_BUSY", &[]), refusal()]);
        let coded = status_to_coded("player", &status);
        assert_eq!(coded.code, "PLAYER_BUSY", "{code:?}");
        let expected = match code {
            Code::Aborted => Class::BusinessImmediate,
            _ => Class::BugRetryable,
        };
        assert_eq!(class(coded.grpc as i32), expected, "{code:?}");
        assert_eq!(coded.refusal, None, "{code:?}");
    }
}

/// UNKNOWN is the transport's only when the transport is its source; a
/// child's own UNKNOWN is a failure no one classified.
#[test]
fn a_childs_own_unknown_is_unhandled() {
    let coded = status_to_coded("player", &tonic::Status::unknown("what"));
    assert_eq!(
        coded.code,
        AngzarrReason::UnhandledHandlerError.as_str_name()
    );
    assert!(!is_retryable(&coded));
}

/// A typed detail on a status outside the business set is no refusal.
#[test]
fn only_a_business_status_carries_a_refusal() {
    let status = status_with_details(
        Code::Internal,
        "bad",
        vec![error_info("BAD", &[]), refusal()],
    );
    assert_eq!(status_to_coded("player", &status).refusal, None);
}

#[test]
fn only_unavailability_is_retryable() {
    let other = CodedError::refused(
        angzarr_router::error::BusinessStatus::InvalidArgument,
        refusal(),
    );
    assert!(!is_retryable(&other));
    let stale = CodedError::immediate("SEAT_MOVED", []);
    assert!(!is_retryable(&stale));
}
