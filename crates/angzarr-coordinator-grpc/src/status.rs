//! A refusal as a `tonic::Status`: the status code of its [`Code`], the key
//! as the message, and a `google.rpc.ErrorInfo` detail holding the key as
//! its reason and the args as its metadata (decision 14.37).

use std::collections::BTreeMap;

use angzarr_coordinator_base::{Code, Coded, Refusal};
use prost::Message;
use prost_types::Any;
use tonic::codegen::Bytes;
use tonic::Status;

/// `google.rpc.ErrorInfo`, as googleapis defines it (field numbers and
/// types): why a request failed, in a form a program reads.
#[derive(Clone, PartialEq, Message)]
pub struct ErrorInfo {
    /// The key: `ANGZARR_REASON_...` or a crate's kebab-case code.
    #[prost(string, tag = "1")]
    pub reason: String,
    /// Where the key is defined: [`ERROR_DOMAIN`].
    #[prost(string, tag = "2")]
    pub domain: String,
    /// The args, a wrapped cause flattened as `cause` and `cause.<arg>`.
    #[prost(btree_map = "string, string", tag = "3")]
    pub metadata: BTreeMap<String, String>,
}

/// `google.rpc.Status`, the message a status's details travel in.
#[derive(Clone, PartialEq, Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<Any>,
}

/// The `ErrorInfo.domain` of every key angzarr answers.
pub const ERROR_DOMAIN: &str = "angzarr.io";

/// The type URL of an `ErrorInfo` detail.
pub const ERROR_INFO_TYPE_URL: &str = "type.googleapis.com/google.rpc.ErrorInfo";

/// tonic's code for a [`Code`]: the same wire number.
pub fn tonic_code(code: Code) -> tonic::Code {
    match code {
        Code::Unknown => tonic::Code::Unknown,
        Code::InvalidArgument => tonic::Code::InvalidArgument,
        Code::DeadlineExceeded => tonic::Code::DeadlineExceeded,
        Code::NotFound => tonic::Code::NotFound,
        Code::AlreadyExists => tonic::Code::AlreadyExists,
        Code::PermissionDenied => tonic::Code::PermissionDenied,
        Code::ResourceExhausted => tonic::Code::ResourceExhausted,
        Code::FailedPrecondition => tonic::Code::FailedPrecondition,
        Code::Aborted => tonic::Code::Aborted,
        Code::OutOfRange => tonic::Code::OutOfRange,
        Code::Unimplemented => tonic::Code::Unimplemented,
        Code::Internal => tonic::Code::Internal,
        Code::Unauthenticated => tonic::Code::Unauthenticated,
        Code::Unavailable => tonic::Code::Unavailable,
        Code::DataLoss => tonic::Code::DataLoss,
    }
}

/// A status of `code`, its message `key`, its detail an `ErrorInfo` of
/// `key` and `args`.
fn with_error_info(
    code: tonic::Code,
    key: &str,
    args: impl IntoIterator<Item = (String, String)>,
) -> Status {
    let info = ErrorInfo {
        reason: key.to_string(),
        domain: ERROR_DOMAIN.to_string(),
        metadata: args.into_iter().collect(),
    };
    let details = RpcStatus {
        code: code as i32,
        message: key.to_string(),
        details: vec![Any {
            type_url: ERROR_INFO_TYPE_URL.to_string(),
            value: info.encode_to_vec(),
        }],
    };
    Status::with_details(code, key, Bytes::from(details.encode_to_vec()))
}

/// `coded` as the status it answers: its code, its key as the message, an
/// `ErrorInfo` with its key and args.
pub fn status(coded: &Coded) -> Status {
    with_error_info(
        tonic_code(coded.code),
        coded.status_message(),
        coded.args.clone(),
    )
}

/// Whatever answers as a [`Coded`], as the status it answers.
pub fn status_of(refusal: &dyn Refusal) -> Status {
    status(&refusal.coded())
}

/// UNAUTHENTICATED under `key`, with `args`: a credential that is missing,
/// stale, revoked or malformed. Not a [`Code`] of a refusal (a refusal is
/// never unauthenticated), so it has its own constructor.
pub fn unauthenticated(key: &str, args: impl IntoIterator<Item = (String, String)>) -> Status {
    with_error_info(tonic::Code::Unauthenticated, key, args)
}

/// The `ErrorInfo` a status carries, if it carries one.
pub fn error_info(status: &Status) -> Option<ErrorInfo> {
    let details = RpcStatus::decode(status.details()).ok()?;
    let any = details
        .details
        .into_iter()
        .find(|any| any.type_url == ERROR_INFO_TYPE_URL)?;
    ErrorInfo::decode(any.value.as_slice()).ok()
}

#[cfg(test)]
#[path = "status.test.rs"]
mod tests;
