//! What a caller is answered, in the spec's terms: a status, a reason key
//! and its args, or an outcome. The kernel answers in process (a
//! `Result<Handled, Failure<Rejected>>`); its wire form is the router's
//! `CodedError` where the kernel gives one (`KernelError::as_coded`, the
//! router's own, a refusal's), and otherwise the status its recovery
//! stands for: a malformed input INVALID_ARGUMENT, a bug INTERNAL, a
//! retryable failure UNAVAILABLE.

use std::collections::BTreeMap;

use angzarr_embedded::angzarr_diagnostic::{Arg, Diagnostic};
use angzarr_embedded::angzarr_router::error::{CodedError, GrpcCode};
use angzarr_embedded::v1::DeliveryOutcome;
use angzarr_embedded::{Any, Failure, Handled, KernelError, Recovery, Rejected, StepError};

/// One call's answer.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Answered {
    /// OK, delivered so.
    Ok(DeliveryOutcome),
    /// Refused: a status, its reason key and args, and, for a typed
    /// refusal, its object in the status details (decision 14.95).
    Failed {
        status: GrpcCode,
        reason: String,
        args: BTreeMap<String, String>,
        refusal: Option<Any>,
    },
}

/// A diagnostic's args, flat: a nested cause by its code, its own args
/// under `name.`.
fn flat(diagnostic: &dyn Diagnostic) -> BTreeMap<String, String> {
    let mut flat = BTreeMap::new();
    for (name, arg) in diagnostic.args() {
        match arg {
            Arg::Int(n) => flat.insert(name.to_string(), n.to_string()),
            Arg::Uint(n) => flat.insert(name.to_string(), n.to_string()),
            Arg::Str(s) => flat.insert(name.to_string(), s),
            Arg::Nested(cause) => {
                for (inner, value) in self::flat(cause.as_ref()) {
                    flat.insert(format!("{name}.{inner}"), value);
                }
                flat.insert(name.to_string(), cause.code().to_string())
            }
        };
    }
    flat
}

fn coded(error: CodedError) -> Answered {
    Answered::Failed {
        status: error.grpc,
        reason: error.code,
        args: error.extras,
        refusal: error.refusal.map(|refusal| *refusal),
    }
}

/// The status a failure with no wire form of its own stands for.
fn by_recovery(recovery: Recovery) -> GrpcCode {
    match recovery {
        Recovery::Invalid => GrpcCode::InvalidArgument,
        Recovery::Business => GrpcCode::FailedPrecondition,
        Recovery::Retry => GrpcCode::Unavailable,
        Recovery::Bug => GrpcCode::Internal,
    }
}

fn kernel(error: &KernelError) -> Answered {
    if let KernelError::Router(error) = error {
        return coded(error.clone());
    }
    if let Some(error) = error.as_coded() {
        return coded(error);
    }
    Answered::Failed {
        status: by_recovery(error.recovery()),
        reason: error.code().to_string(),
        args: flat(error),
        refusal: None,
    }
}

fn rejected(rejected: &Rejected) -> Answered {
    coded(rejected.as_coded())
}

impl Answered {
    /// `result`, as its caller is answered.
    pub(crate) fn of(result: &Result<Handled, Failure<Rejected>>) -> Answered {
        match result {
            Ok(handled) => Answered::Ok(handled.delivery()),
            Err(Failure::Rejected(r)) => rejected(r),
            Err(Failure::Kernel(error)) => kernel(error),
            Err(failure @ Failure::Downstream { .. })
            | Err(failure @ Failure::Projection { .. }) => Answered::Failed {
                status: by_recovery(failure.recovery()),
                reason: failure.code().to_string(),
                args: flat(failure),
                refusal: None,
            },
        }
    }

    /// A policy step's failure, as the step is answered: its own error.
    pub(crate) fn of_step(error: &StepError) -> Answered {
        match error {
            StepError::Kernel(error) => kernel(error),
            StepError::Rejected(report) => Answered::Failed {
                status: GrpcCode::FailedPrecondition,
                reason: report.code().to_string(),
                args: flat(report),
                refusal: None,
            },
            StepError::PolicyFailed(report) => Answered::Failed {
                status: GrpcCode::Internal,
                reason: report.code().to_string(),
                args: flat(report),
                refusal: None,
            },
        }
    }

    /// The status, `None` when OK.
    pub(crate) fn status(&self) -> Option<GrpcCode> {
        match self {
            Answered::Ok(_) => None,
            Answered::Failed { status, .. } => Some(*status),
        }
    }

    /// The reason key, `None` when OK.
    pub(crate) fn reason(&self) -> Option<&str> {
        match self {
            Answered::Ok(_) => None,
            Answered::Failed { reason, .. } => Some(reason),
        }
    }

    /// The typed refusal object, if refused with one.
    pub(crate) fn refusal(&self) -> Option<&Any> {
        match self {
            Answered::Ok(_) => None,
            Answered::Failed { refusal, .. } => refusal.as_ref(),
        }
    }

    /// The arg `name`, if refused with one.
    pub(crate) fn arg(&self, name: &str) -> Option<&str> {
        match self {
            Answered::Ok(_) => None,
            Answered::Failed { args, .. } => args.get(name).map(String::as_str),
        }
    }
}

/// A gRPC status by its spec name: `ABORTED`.
pub(crate) fn status(name: &str) -> GrpcCode {
    match name {
        "UNKNOWN" => GrpcCode::Unknown,
        "INVALID_ARGUMENT" => GrpcCode::InvalidArgument,
        "DEADLINE_EXCEEDED" => GrpcCode::DeadlineExceeded,
        "NOT_FOUND" => GrpcCode::NotFound,
        "ALREADY_EXISTS" => GrpcCode::AlreadyExists,
        "PERMISSION_DENIED" => GrpcCode::PermissionDenied,
        "RESOURCE_EXHAUSTED" => GrpcCode::ResourceExhausted,
        "FAILED_PRECONDITION" => GrpcCode::FailedPrecondition,
        "ABORTED" => GrpcCode::Aborted,
        "OUT_OF_RANGE" => GrpcCode::OutOfRange,
        "UNIMPLEMENTED" => GrpcCode::Unimplemented,
        "INTERNAL" => GrpcCode::Internal,
        "UNAVAILABLE" => GrpcCode::Unavailable,
        "DATA_LOSS" => GrpcCode::DataLoss,
        other => panic!("no gRPC status {other}"),
    }
}
