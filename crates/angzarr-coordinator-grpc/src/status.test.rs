use angzarr_coordinator_base::Refusal;
use prost::Message;

use super::*;

fn coded() -> Coded {
    Coded::new(
        Code::FailedPrecondition,
        "ANGZARR_REASON_SEQUENCE_MISMATCH",
        [
            ("domain".to_string(), "player".to_string()),
            ("cause.errno".to_string(), "111".to_string()),
        ],
    )
}

/// A refusal travels as a status of its code, its key as the message, and
/// an `ErrorInfo` detail with the key as reason, angzarr's domain, and the
/// args as metadata.
#[test]
fn a_refusal_is_a_status_with_errorinfo_reason_and_args() {
    let status = status(&coded());
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(status.message(), "ANGZARR_REASON_SEQUENCE_MISMATCH");
    let info = error_info(&status).expect("an ErrorInfo detail");
    assert_eq!(info.reason, "ANGZARR_REASON_SEQUENCE_MISMATCH");
    assert_eq!(info.domain, "angzarr.io");
    assert_eq!(
        info.metadata.into_iter().collect::<Vec<_>>(),
        [
            ("cause.errno".to_string(), "111".to_string()),
            ("domain".to_string(), "player".to_string()),
        ]
    );
}

/// The detail is googleapis' own wire form, spelled out byte for byte: a
/// `google.rpc.Status` (code, message, one `Any` of `ErrorInfo`) in the
/// status's details, so any gRPC client reads it.
#[test]
fn the_detail_is_the_googleapis_wire_form() {
    let status = status(&Coded::new(
        Code::NotFound,
        "K",
        [("a".to_string(), "b".to_string())],
    ));
    // ErrorInfo: reason "K" (1), domain "angzarr.io" (2), metadata {a: b} (3).
    let mut info = vec![0x0A, 1, b'K', 0x12, 10];
    info.extend_from_slice(b"angzarr.io");
    info.extend_from_slice(&[0x1A, 6, 0x0A, 1, b'a', 0x12, 1, b'b']);
    // Any: type_url (1), value (2).
    let url = b"type.googleapis.com/google.rpc.ErrorInfo";
    let mut any = vec![0x0A, url.len() as u8];
    any.extend_from_slice(url);
    any.extend_from_slice(&[0x12, info.len() as u8]);
    any.extend_from_slice(&info);
    // Status: code 5 (1), message "K" (2), details (3).
    let mut expected = vec![0x08, 5, 0x12, 1, b'K', 0x1A, any.len() as u8];
    expected.extend_from_slice(&any);
    assert_eq!(status.details(), expected.as_slice());
}

/// The status code is the refusal's, number for number, for every one of
/// the fourteen.
#[test]
fn every_code_maps_to_the_grpc_code_of_its_number() {
    for code in Code::ALL {
        let mapped = tonic_code(code);
        assert_eq!(mapped as i32, code.number(), "{code:?}");
        assert_eq!(status(&Coded::new(code, "K", [])).code(), mapped);
    }
}

#[derive(Debug)]
struct Failing;

impl Refusal for Failing {
    fn coded(&self) -> Coded {
        Coded::new(Code::Unavailable, "annals-store-down", [])
    }
}

/// Anything that answers as a `Coded` is mapped as it answers.
#[test]
fn a_refusal_trait_object_is_mapped_through_its_coded() {
    let status = status_of(&Failing);
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert_eq!(status.message(), "annals-store-down");
    assert_eq!(error_info(&status).unwrap().reason, "annals-store-down");
}

#[test]
fn unauthenticated_is_grpc_unauthenticated() {
    let status = unauthenticated("auth-revoked", [("run".to_string(), "r1".to_string())]);
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
    assert_eq!(status.message(), "auth-revoked");
    let info = error_info(&status).unwrap();
    assert_eq!(info.reason, "auth-revoked");
    assert_eq!(info.metadata.get("run").map(String::as_str), Some("r1"));
}

/// A status that carries no detail, or one that is not an `ErrorInfo`, has
/// none to read.
#[test]
fn a_status_without_an_errorinfo_has_none() {
    assert_eq!(error_info(&tonic::Status::internal("x")), None);
    let other = RpcStatus {
        code: 3,
        message: "x".to_string(),
        details: vec![Any {
            type_url: "type.googleapis.com/google.rpc.Help".to_string(),
            value: vec![],
        }],
    };
    let status = tonic::Status::with_details(
        tonic::Code::InvalidArgument,
        "x",
        Bytes::from(other.encode_to_vec()),
    );
    assert_eq!(error_info(&status), None);
}
