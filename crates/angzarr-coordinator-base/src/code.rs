//! [`Code`]: the gRPC status a refusal travels with, as a plain enum (no
//! tonic). The router's table, duplicated: its `GrpcCode`, the same names
//! and wire numbers, plus UNAUTHENTICATED, an addition beyond the router's
//! table (`tests/status_table.rs` holds the two together).

use angzarr_proto::google::rpc;

/// A gRPC status code, by its wire number. The router's fourteen, and
/// `Unauthenticated` (16), the gate's answer to a bad or revoked credential,
/// which the router's table does not have. A key's declared status
/// ([`AngzarrReason::surface`](angzarr_proto::v1::AngzarrReason::surface))
/// is one of them ([`TryFrom`]); `OK` and `CANCELLED` are never a refusal's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum Code {
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl Code {
    /// Every code, in wire order.
    pub const ALL: [Code; 15] = [
        Code::Unknown,
        Code::InvalidArgument,
        Code::DeadlineExceeded,
        Code::NotFound,
        Code::AlreadyExists,
        Code::PermissionDenied,
        Code::ResourceExhausted,
        Code::FailedPrecondition,
        Code::Aborted,
        Code::OutOfRange,
        Code::Unimplemented,
        Code::Internal,
        Code::Unavailable,
        Code::DataLoss,
        Code::Unauthenticated,
    ];

    /// The code's number on the wire.
    pub const fn number(self) -> i32 {
        self as i32
    }

    /// The code's name as the gRPC spec writes it (`INVALID_ARGUMENT`).
    pub fn name(self) -> &'static str {
        match self {
            Code::Unknown => "UNKNOWN",
            Code::InvalidArgument => "INVALID_ARGUMENT",
            Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Code::NotFound => "NOT_FOUND",
            Code::AlreadyExists => "ALREADY_EXISTS",
            Code::PermissionDenied => "PERMISSION_DENIED",
            Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Code::FailedPrecondition => "FAILED_PRECONDITION",
            Code::Aborted => "ABORTED",
            Code::OutOfRange => "OUT_OF_RANGE",
            Code::Unimplemented => "UNIMPLEMENTED",
            Code::Internal => "INTERNAL",
            Code::Unavailable => "UNAVAILABLE",
            Code::DataLoss => "DATA_LOSS",
            Code::Unauthenticated => "UNAUTHENTICATED",
        }
    }

    /// The code of a wire number, if it is one of the fifteen.
    pub fn from_number(number: i32) -> Option<Code> {
        Code::ALL.into_iter().find(|code| code.number() == number)
    }
}

/// A key's declared status as a [`Code`]; the status itself when it is none
/// of the fifteen.
impl TryFrom<rpc::Code> for Code {
    type Error = rpc::Code;

    fn try_from(status: rpc::Code) -> Result<Code, rpc::Code> {
        Code::from_number(status as i32).ok_or(status)
    }
}

#[cfg(test)]
#[path = "code.test.rs"]
mod tests;
