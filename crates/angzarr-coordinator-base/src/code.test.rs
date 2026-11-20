use angzarr_proto::google::rpc;

use super::*;

/// The gRPC spec's names: what a status is called in a log and on the wire's
/// text form, and every one a distinct string.
#[test]
fn a_code_is_named_as_the_grpc_spec_names_it() {
    let names = [
        (Code::Unknown, "UNKNOWN"),
        (Code::InvalidArgument, "INVALID_ARGUMENT"),
        (Code::DeadlineExceeded, "DEADLINE_EXCEEDED"),
        (Code::NotFound, "NOT_FOUND"),
        (Code::AlreadyExists, "ALREADY_EXISTS"),
        (Code::PermissionDenied, "PERMISSION_DENIED"),
        (Code::ResourceExhausted, "RESOURCE_EXHAUSTED"),
        (Code::FailedPrecondition, "FAILED_PRECONDITION"),
        (Code::Aborted, "ABORTED"),
        (Code::OutOfRange, "OUT_OF_RANGE"),
        (Code::Unimplemented, "UNIMPLEMENTED"),
        (Code::Internal, "INTERNAL"),
        (Code::Unavailable, "UNAVAILABLE"),
        (Code::DataLoss, "DATA_LOSS"),
        (Code::Unauthenticated, "UNAUTHENTICATED"),
    ];
    assert_eq!(names.len(), Code::ALL.len());
    for (code, name) in names {
        assert_eq!(code.name(), name);
    }
}

/// The wire number is the gRPC one, and a number reads back as its code;
/// a number that is none of the fifteen (0 OK, 1 CANCELLED, anything else
/// but 16 UNAUTHENTICATED) is none.
#[test]
fn a_code_round_trips_through_its_wire_number() {
    let mut last = 0;
    for code in Code::ALL {
        assert!(code.number() > last, "ALL is in wire order");
        last = code.number();
        assert_eq!(Code::from_number(code.number()), Some(code));
    }
    assert_eq!(Code::Internal.number(), 13);
    assert_eq!(Code::DataLoss.number(), 15);
    for number in [-1, 0, 1, 17, 1000] {
        assert_eq!(Code::from_number(number), None, "{number}");
    }
}

/// A key's declared status (`google.rpc.Code`) is a `Code` when it is one of
/// the fifteen, and itself in the error when it is not.
#[test]
fn a_google_rpc_code_converts_when_it_is_one_of_the_fifteen() {
    assert_eq!(
        Code::try_from(rpc::Code::Unavailable),
        Ok(Code::Unavailable)
    );
    assert_eq!(Code::try_from(rpc::Code::DataLoss), Ok(Code::DataLoss));
    assert_eq!(
        Code::try_from(rpc::Code::FailedPrecondition),
        Ok(Code::FailedPrecondition)
    );
    assert_eq!(Code::try_from(rpc::Code::Ok), Err(rpc::Code::Ok));
    assert_eq!(
        Code::try_from(rpc::Code::Cancelled),
        Err(rpc::Code::Cancelled)
    );
    assert_eq!(
        Code::try_from(rpc::Code::Unauthenticated),
        Ok(Code::Unauthenticated)
    );
    for code in Code::ALL {
        let rpc = rpc::Code::try_from(code.number()).expect("a gRPC number");
        assert_eq!(Code::try_from(rpc), Ok(code));
    }
}

/// UNAUTHENTICATED (16) is the gate's answer to a bad or revoked credential:
/// it is a `Code`, last in wire order, and round-trips its number, its name
/// and `google.rpc.Code`.
#[test]
fn unauthenticated_is_code_16_and_round_trips_numbers_names_and_google_rpc_code() {
    let code = Code::from_number(16).expect("16 is a Code");
    assert_eq!(code, Code::Unauthenticated);
    assert_eq!(code.number(), 16);
    assert_eq!(code.name(), "UNAUTHENTICATED");
    assert_eq!(Code::ALL.last(), Some(&code));
    assert_eq!(Code::ALL.len(), 15);
    assert_eq!(Code::try_from(rpc::Code::Unauthenticated), Ok(code));
}
