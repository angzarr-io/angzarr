//! The coded error model: a SCREAMING_SNAKE key and an argument map, and
//! the single handler-error → gRPC mapping table (decision 14.37). No
//! English crosses a boundary: the gRPC status message IS the key, the
//! `google.rpc.ErrorInfo` (domain [`ERROR_INFO_DOMAIN`]) carries the same
//! key as its reason and the args as its metadata, and text is rendered only
//! where it is shown, from a catalog keyed by the code.
//!
//! Every outcome but success is one of these (decision 14.95): a typed
//! refusal ([`CodedError::refused`], a command's only), a re-run
//! ([`CodedError::immediate`], ABORTED) or a bug ([`CodedError::bug`] and
//! the router's own keys). There is no untyped business rejection: a key
//! business code raises of its own is a bug's cause.
//!
//! The router answers a status and a key, and never classifies (decision
//! 14.42): which of decision 14.37's four classes a status is, and what
//! reaches a policy's Translate, are the coordinator's. No class,
//!
//! ```compile_fail
//! use angzarr_router::error::ErrorClass;
//! ```
//!
//! no status's class,
//!
//! ```compile_fail
//! let _ = angzarr_router::error::GrpcCode::Internal.class();
//! ```
//!
//! no business-refusal test,
//!
//! ```compile_fail
//! let _ = angzarr_router::error::GrpcCode::Internal.is_business_refusal();
//! ```
//!
//! and no refused policy's view of an answer:
//!
//! ```compile_fail
//! let _ = angzarr_router::error::CodedError::unhandled().refusal_response();
//! ```

use std::collections::BTreeMap;

/// google.rpc.ErrorInfo domain for all angzarr errors.
pub const ERROR_INFO_DOMAIN: &str = "angzarr.io";

/// gRPC status codes the router emits (numeric values are the wire codes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum GrpcCode {
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
}

impl GrpcCode {
    /// The code whose wire number is `number`; `None` for OK, CANCELLED,
    /// UNAUTHENTICATED and any number that is no status the router emits.
    pub fn from_wire(number: i32) -> Option<Self> {
        Some(match number {
            2 => GrpcCode::Unknown,
            3 => GrpcCode::InvalidArgument,
            4 => GrpcCode::DeadlineExceeded,
            5 => GrpcCode::NotFound,
            6 => GrpcCode::AlreadyExists,
            7 => GrpcCode::PermissionDenied,
            8 => GrpcCode::ResourceExhausted,
            9 => GrpcCode::FailedPrecondition,
            10 => GrpcCode::Aborted,
            11 => GrpcCode::OutOfRange,
            12 => GrpcCode::Unimplemented,
            13 => GrpcCode::Internal,
            14 => GrpcCode::Unavailable,
            15 => GrpcCode::DataLoss,
            _ => return None,
        })
    }

    /// The code a spec key travels with, from the surface its
    /// `(io.angzarr.v1.reason)` option declares (`AngzarrReason::surface`
    /// and the transports' keys'; decisions 14.65 and 14.104): its
    /// `google.rpc.Code`. `None` for a readiness refusal, which never
    /// travels as a status, and for `UNSPECIFIED`, which declares none.
    pub fn of_surface(surface: Option<angzarr_proto::ReasonSurface>) -> Option<Self> {
        match surface? {
            angzarr_proto::ReasonSurface::GrpcStatus(code) => Self::from_wire(code as i32),
            angzarr_proto::ReasonSurface::Readiness => None,
        }
    }
}

/// The status a handler picks for a bug it
/// marks retryable ([`CodedError::retryable_bug`]; decision 14.37, owner's
/// rulings 2026-10-29, latest). The statuses are not interchangeable, and
/// a retry loop honours the difference:
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryableStatus {
    /// UNAVAILABLE: resend. The dependency is down for now.
    Unavailable,
    /// DEADLINE_EXCEEDED: resend only if the call is idempotent; the work
    /// may have happened. (A command is idempotent by its `command_id`.)
    DeadlineExceeded,
    /// RESOURCE_EXHAUSTED: wait longer before resending; a quota or a
    /// capacity limit was hit.
    ResourceExhausted,
}

impl From<RetryableStatus> for GrpcCode {
    fn from(status: RetryableStatus) -> Self {
        match status {
            RetryableStatus::Unavailable => GrpcCode::Unavailable,
            RetryableStatus::DeadlineExceeded => GrpcCode::DeadlineExceeded,
            RetryableStatus::ResourceExhausted => GrpcCode::ResourceExhausted,
        }
    }
}

/// The statuses a typed refusal may carry, and only those: the business
/// statuses of `io.angzarr.v1.RefusalStatus`, one of which each refusal
/// message declares as its `(refusal_status)` (decisions 14.82 and 14.95):
/// the six business-fatal ones, and [`BusinessStatus::Aborted`] (decision
/// 14.111).
/// [`CodedError::refused`] takes one, so a bug's or a retryable status is
/// refused by type (owner ruling, 2026-10-29).
///
/// ```compile_fail
/// use angzarr_router::error::{CodedError, GrpcCode};
/// let _ = CodedError::refused(GrpcCode::Internal, prost_types::Any::default());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BusinessStatus {
    FailedPrecondition,
    InvalidArgument,
    NotFound,
    AlreadyExists,
    OutOfRange,
    PermissionDenied,
    /// ABORTED, "re-read and re-decide" (decision 14.111): the caller acted
    /// on a version that has moved. Its class is the coordinator's
    /// (decision 14.42): 14.37's immediate return, never a policy's
    /// Translate.
    Aborted,
}

impl From<BusinessStatus> for GrpcCode {
    fn from(status: BusinessStatus) -> Self {
        match status {
            BusinessStatus::FailedPrecondition => GrpcCode::FailedPrecondition,
            BusinessStatus::InvalidArgument => GrpcCode::InvalidArgument,
            BusinessStatus::NotFound => GrpcCode::NotFound,
            BusinessStatus::AlreadyExists => GrpcCode::AlreadyExists,
            BusinessStatus::OutOfRange => GrpcCode::OutOfRange,
            BusinessStatus::PermissionDenied => GrpcCode::PermissionDenied,
            BusinessStatus::Aborted => GrpcCode::Aborted,
        }
    }
}

/// A refusal message's declared status (`io.angzarr.v1.RefusalStatus`),
/// as generated code reads it: `UNSPECIFIED`, which no refusal may
/// declare (decision 14.95), is refused with itself.
impl TryFrom<angzarr_proto::v1::RefusalStatus> for BusinessStatus {
    type Error = angzarr_proto::v1::RefusalStatus;
    fn try_from(
        status: angzarr_proto::v1::RefusalStatus,
    ) -> Result<Self, angzarr_proto::v1::RefusalStatus> {
        use angzarr_proto::v1::RefusalStatus;
        match status {
            RefusalStatus::InvalidArgument => Ok(BusinessStatus::InvalidArgument),
            RefusalStatus::NotFound => Ok(BusinessStatus::NotFound),
            RefusalStatus::AlreadyExists => Ok(BusinessStatus::AlreadyExists),
            RefusalStatus::PermissionDenied => Ok(BusinessStatus::PermissionDenied),
            RefusalStatus::FailedPrecondition => Ok(BusinessStatus::FailedPrecondition),
            RefusalStatus::OutOfRange => Ok(BusinessStatus::OutOfRange),
            RefusalStatus::Aborted => Ok(BusinessStatus::Aborted),
            RefusalStatus::Unspecified => Err(status),
        }
    }
}

/// The handler layer's `RefusalStatus` of a business status: what a refused
/// policy's Translate is handed (`io.angzarr.handler.v1.Refusal.status`).
impl From<BusinessStatus> for angzarr_proto::v1::RefusalStatus {
    fn from(status: BusinessStatus) -> Self {
        use angzarr_proto::v1::RefusalStatus;
        match status {
            BusinessStatus::FailedPrecondition => RefusalStatus::FailedPrecondition,
            BusinessStatus::InvalidArgument => RefusalStatus::InvalidArgument,
            BusinessStatus::NotFound => RefusalStatus::NotFound,
            BusinessStatus::AlreadyExists => RefusalStatus::AlreadyExists,
            BusinessStatus::OutOfRange => RefusalStatus::OutOfRange,
            BusinessStatus::PermissionDenied => RefusalStatus::PermissionDenied,
            BusinessStatus::Aborted => RefusalStatus::Aborted,
        }
    }
}

/// A status a host or a binding hands over at run time is checked: a
/// non-business status is refused with itself.
impl TryFrom<GrpcCode> for BusinessStatus {
    type Error = GrpcCode;
    fn try_from(code: GrpcCode) -> Result<Self, GrpcCode> {
        match code {
            GrpcCode::FailedPrecondition => Ok(BusinessStatus::FailedPrecondition),
            GrpcCode::InvalidArgument => Ok(BusinessStatus::InvalidArgument),
            GrpcCode::NotFound => Ok(BusinessStatus::NotFound),
            GrpcCode::AlreadyExists => Ok(BusinessStatus::AlreadyExists),
            GrpcCode::OutOfRange => Ok(BusinessStatus::OutOfRange),
            GrpcCode::PermissionDenied => Ok(BusinessStatus::PermissionDenied),
            GrpcCode::Aborted => Ok(BusinessStatus::Aborted),
            other => Err(other),
        }
    }
}

/// The router's reason keys (decisions 14.37, 14.42, 14.44) are core's
/// [`AngzarrReason`], reasons.proto's one enum for core's layer (decision
/// 14.104): one value per condition, each answered as its full value name
/// ([`AngzarrReason::as_str_name`], `ANGZARR_REASON_` included), its own
/// status message and `ErrorInfo` reason. The router raises only the keys
/// whose comment says "Raised by the router"; the coordinator's are its
/// own.
pub use angzarr_proto::v1::AngzarrReason;

// The args' names: the `ErrorInfo` metadata keys of the reasons, each
// declared on its `AngzarrReason` value's `(io.angzarr.v1.reason)` option
// in reasons.proto and generated into angzarr-proto (decision 14.60), never
// typed here. A wrapped cause flattens under `cause` as `cause.<arg>`; a
// third-party library's own text travels verbatim as `ext_err_txt`
// (decision 14.37, owner's rulings 2026-10-29, late), the one text an arg
// may hold.
use angzarr_proto::v1::reason_args;

/// Where a failing fold failed: the page, when known, else the batch's
/// sequence range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldAt<'a> {
    Page { sequence: u64, type_url: &'a str },
    Range { first: u64, last: u64 },
}

/// A coded business/framework error: the form every failure takes before
/// crossing a boundary, a KEY and an ARGUMENT MAP (decision 14.37). `code`
/// is the cross-language SCREAMING_SNAKE key: the `ErrorInfo.reason` and
/// the status message alike ([`Self::status_message`]); `extras` are the
/// args, the `ErrorInfo` metadata, a wrapped cause flattened into them as
/// `cause` and `cause.<arg>`; `grpc` is resolved at construction, and
/// is the status it travels with. No constructor takes text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodedError {
    pub code: String,
    pub extras: BTreeMap<String, String>,
    pub grpc: GrpcCode,
    /// A refusal's TYPED refusal object, packed: one of the command's
    /// `CommandOptions.refuses` types (options.proto; decision 14.95).
    /// Carried in the refusal's status details; the refused policy's
    /// Translate receives it as `Refusal.typed`. Boxed, so the common error
    /// stays small.
    pub refusal: Option<Box<prost_types::Any>>,
}

/// What a business handler may fail with: a coded error (refusals and
/// classified failures keep their code and gRPC code) or an unclassified
/// error, which the boundary maps to ANGZARR_REASON_UNHANDLED_HANDLER_ERROR with no cause.
/// `Other`'s text is for the handler's own log; it never crosses a
/// boundary (decision 14.37).
#[derive(Debug)]
pub enum HandlerError {
    Coded(CodedError),
    Other(String),
}

impl From<CodedError> for HandlerError {
    fn from(e: CodedError) -> Self {
        HandlerError::Coded(e)
    }
}

/// A fact handler's failure: a CLASSIFIED BUG, the one failure a fact's
/// handling (its handler or any continuation step) may answer (decisions
/// 14.72 and 14.98: a fact never refuses and never asks for a re-run). It
/// carries a bug code, args and an optional retryable status, or bare text
/// for the handler's own log (which, like [`HandlerError::Other`]'s, never
/// crosses a boundary). It has no refusal and no typed refusal, and no way
/// to hold a [`CodedError`] that is one: its constructors take a bug code,
/// args and a retryable status only, and [`Bug::from_error`] turns a
/// refusal or a re-run into the bug it is.
///
/// A refusal cannot be put into a `Bug`:
///
/// ```compile_fail
/// use angzarr_router::error::{Bug, BusinessStatus, CodedError};
/// let refusal = CodedError::refused(BusinessStatus::FailedPrecondition, prost_types::Any::default());
/// let _ = Bug::coded(refusal, []);
/// ```
///
/// nor a typed refusal object, nor the error itself (the representation is
/// private):
///
/// ```compile_fail
/// use angzarr_router::error::{Bug, CodedError};
/// let _ = Bug(CodedError::bug("X", []));
/// ```
///
/// `?` does not convert a [`CodedError`] or a [`HandlerError`] into a
/// `Bug`: the conversion is the deliberate [`Bug::from_error`] or
/// [`BugExt::bug`].
///
/// ```compile_fail
/// use angzarr_router::error::{Bug, CodedError};
/// fn step(e: CodedError) -> Result<(), Bug> {
///     Err(e)?
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bug(BugRepr);

#[derive(Debug, Clone, PartialEq, Eq)]
enum BugRepr {
    /// Unclassified: text for the handler's own log.
    Text(String),
    /// Classified: a bug or retryable bug [`CodedError`], or a router key
    /// that is itself a bug; never a refusal or a re-run.
    Coded(CodedError),
}

impl Bug {
    /// An unclassified bug: `text` is for the handler's own log and is
    /// never sent ([`HandlerError::Other`]'s rule).
    pub fn new(text: impl std::fmt::Display) -> Self {
        Bug(BugRepr::Text(text.to_string()))
    }

    /// A bug under the handler's own `code` and `args` ([`CodedError::bug`]):
    /// INTERNAL, the code and args as the cause.
    pub fn coded(code: &str, args: impl IntoIterator<Item = (String, String)>) -> Self {
        Bug(BugRepr::Coded(CodedError::bug(code, args)))
    }

    /// A bug the handler marks retryable, under the status it picks
    /// ([`CodedError::retryable_bug`]).
    pub fn retryable(
        status: RetryableStatus,
        code: &str,
        args: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        Bug(BugRepr::Coded(CodedError::retryable_bug(
            status, code, args,
        )))
    }

    /// The bug `err` is, by a deliberate call: a [`CodedError`] keeps its
    /// code, args and status when it is already a bug or a router key (an
    /// undecodable payload stays ANGZARR_REASON_ANY_DECODE_FAILED), and a
    /// refusal or a re-run becomes a bug carrying its key as the cause
    /// ([`CodedError::as_fact_failure`]); any other error is an
    /// unclassified bug with its text for the log.
    pub fn from_error<E: std::error::Error + 'static>(err: E) -> Self {
        match (&err as &dyn std::any::Any).downcast_ref::<CodedError>() {
            Some(coded) => Bug(BugRepr::Coded(coded.clone().as_fact_failure())),
            None => Bug::new(err),
        }
    }
}

impl std::fmt::Display for Bug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            BugRepr::Text(text) => f.write_str(text),
            BugRepr::Coded(coded) => std::fmt::Display::fmt(coded, f),
        }
    }
}

/// A bug is what the boundary maps a fact's failure from: unclassified
/// text is [`HandlerError::Other`] (decision 14.72), a classified bug its
/// [`CodedError`].
impl From<Bug> for HandlerError {
    fn from(bug: Bug) -> Self {
        match bug.0 {
            BugRepr::Text(text) => HandlerError::Other(text),
            BugRepr::Coded(coded) => HandlerError::Coded(coded),
        }
    }
}

/// `.bug()` on a `Result` whose error is a std error: the deliberate
/// conversion into a [`Bug`] ([`Bug::from_error`]).
pub trait BugExt<T> {
    fn bug(self) -> Result<T, Bug>;
}

impl<T, E: std::error::Error + 'static> BugExt<T> for Result<T, E> {
    fn bug(self) -> Result<T, Bug> {
        self.map_err(Bug::from_error)
    }
}

/// `cause`'s code under `name`, each of its args under `<name>.<arg>`
/// (decision 14.37).
fn flatten(
    name: &str,
    code: &str,
    args: impl IntoIterator<Item = (String, String)>,
) -> impl Iterator<Item = (String, String)> {
    let name = name.to_string();
    std::iter::once((name.clone(), code.to_string())).chain(
        args.into_iter()
            .map(move |(arg, value)| (format!("{name}.{arg}"), value)),
    )
}

impl CodedError {
    fn coded(
        code: &str,
        extras: impl IntoIterator<Item = (String, String)>,
        grpc: GrpcCode,
    ) -> Self {
        CodedError {
            code: code.to_string(),
            extras: extras.into_iter().collect(),
            grpc,
            refusal: None,
        }
    }

    /// The gRPC status message: the reason key itself (decision 14.37).
    pub fn status_message(&self) -> &str {
        &self.code
    }

    /// This error as a key and args (types.proto, the diagnostic region).
    pub fn diagnostic(&self) -> angzarr_proto::v1::Diagnostic {
        angzarr_proto::v1::Diagnostic {
            code: self.code.clone(),
            args: self.extras.clone().into_iter().collect(),
        }
    }

    /// A command or fact whose type URL reaches no registered handler
    /// (`code` ANGZARR_REASON_UNKNOWN_COMMAND_TYPE or ANGZARR_REASON_NO_FACT_HANDLER): a bug (INTERNAL;
    /// owner ruling, 2026-10-29), its one arg `type_url` the type it was
    /// given (C-0682). The coordinator upcast it, and relays the type as sent
    /// beside it (decision 14.39).
    pub fn unknown_type(reason: AngzarrReason, type_url: &str) -> Self {
        Self::keyed(
            reason,
            [(reason_args::TYPE_URL.to_string(), type_url.to_string())],
        )
    }

    /// Bytes that do not decode as the message `message_type` (its
    /// fully-qualified name): ANGZARR_REASON_ANY_DECODE_FAILED, a bug (INTERNAL), one
    /// condition whatever the message, `message_type` its arg. prost's
    /// `error` is a third-party library's, so its own text travels
    /// verbatim as `ext_err_txt` (decision 14.37, owner's rulings
    /// 2026-10-29, late); prost gives it no numeric code.
    pub fn undecodable(message_type: &str, error: &prost::DecodeError) -> Self {
        Self::keyed(
            AngzarrReason::AnyDecodeFailed,
            [
                (
                    reason_args::MESSAGE_TYPE.to_string(),
                    message_type.to_string(),
                ),
                (reason_args::EXT_ERR_TXT.to_string(), error.to_string()),
            ],
        )
    }

    /// Classifies a store-sourced decode failure (DATA_LOSS on the wire).
    pub fn persisted_corrupt(type_url: &str) -> Self {
        Self::keyed(
            AngzarrReason::PersistedEventCorrupt,
            [(reason_args::TYPE_URL.to_string(), type_url.to_string())],
        )
    }

    /// A fold of events that failed (INTERNAL), under `code`: ANGZARR_REASON_FOLD_FAILED
    /// for persisted events, ANGZARR_REASON_FOLD_FAILED_EMITTED for the events the command
    /// emitted. Those events were accepted (or emitted) by the same apply
    /// logic, so the failure is a handler regression, never corrupt data.
    /// `at` names the failing page or, when it is unknown, the batch's
    /// range; `cause`, the fold's own failure as a key and args, is
    /// flattened in as `cause` and `cause.<arg>`.
    pub fn fold_failed(
        reason: AngzarrReason,
        at: FoldAt<'_>,
        state_type_url: &str,
        cause: Option<&angzarr_proto::v1::Diagnostic>,
    ) -> Self {
        let mut details = match at {
            FoldAt::Page { sequence, type_url } => vec![
                (reason_args::SEQUENCE.to_string(), sequence.to_string()),
                (reason_args::TYPE_URL.to_string(), type_url.to_string()),
            ],
            FoldAt::Range { first, last } => vec![
                (reason_args::FIRST_SEQUENCE.to_string(), first.to_string()),
                (reason_args::LAST_SEQUENCE.to_string(), last.to_string()),
            ],
        };
        details.push((
            reason_args::STATE_TYPE_URL.to_string(),
            state_type_url.to_string(),
        ));
        if let Some(cause) = cause {
            details.extend(flatten(reason_args::CAUSE, &cause.code, cause.args.clone()));
        }
        Self::keyed(reason, details)
    }

    /// An unclassified error that escaped a business handler (INTERNAL):
    /// ANGZARR_REASON_UNHANDLED_HANDLER_ERROR with no cause. What escaped stays in the
    /// handler's own log.
    pub fn unhandled() -> Self {
        Self::keyed(AngzarrReason::UnhandledHandlerError, [])
    }

    /// A business handler that panicked (INTERNAL): ANGZARR_REASON_UNHANDLED_HANDLER_ERROR
    /// with cause ANGZARR_REASON_HANDLER_PANICKED. The panic's message stays in the
    /// handler's own log.
    pub fn panicked() -> Self {
        Self::bug(AngzarrReason::HandlerPanicked.as_str_name(), [])
    }

    /// A typed refusal, a command's only (decision 14.95): `refusal` is
    /// one of the command's `refuses` types, packed, refused under
    /// `status`, the business status its message declares
    /// (`(io.angzarr.v1.refusal_status)`; generated code passes it). Its
    /// wire reason is ANGZARR_REASON_COMMAND_REFUSED, its `refusal_type`
    /// arg the object's fully-qualified type, and the object itself rides
    /// the status details (decision 14.37). The refusing domain records
    /// nothing.
    pub fn refused(status: BusinessStatus, refusal: prost_types::Any) -> Self {
        let full_name = refusal
            .type_url
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_string();
        CodedError {
            grpc: status.into(),
            ..Self::keyed(
                AngzarrReason::CommandRefused,
                [(reason_args::REFUSAL_TYPE.to_string(), full_name)],
            )
        }
        .with_refusal(refusal)
    }

    /// This error's typed refusal as a refused policy's Translate receives
    /// it (`io.angzarr.handler.v1.Refusal`; decision 14.95): the object and
    /// the business status it travelled as. `None` for anything but a
    /// typed refusal (a re-run or a bug never reaches Translate).
    pub fn as_refusal(&self) -> Option<angzarr_proto_handler::v1::Refusal> {
        let typed = self.refusal.as_deref()?;
        let status = BusinessStatus::try_from(self.grpc).ok()?;
        Some(angzarr_proto_handler::v1::Refusal {
            status: angzarr_proto::v1::RefusalStatus::from(status) as i32,
            typed: Some(typed.clone()),
        })
    }

    /// This error as a FACT's handling answers it (decisions 14.72 and
    /// 14.98): a fact's handling never refuses and never asks for a
    /// re-run, so a typed refusal or a business or ABORTED status becomes
    /// the bug it is, ANGZARR_REASON_UNHANDLED_HANDLER_ERROR (INTERNAL)
    /// with this error's key and args as its cause (a typed refusal's key is
    /// its type, and it has no args), the refusal object dropped; a bug, retryable or not, is answered unchanged.
    pub fn as_fact_failure(self) -> Self {
        let bug_status = matches!(self.grpc, GrpcCode::Aborted)
            || BusinessStatus::try_from(self.grpc).is_ok()
            || self.refusal.is_some();
        if !bug_status {
            return self;
        }
        if self.refusal.is_some() {
            // A refusal's key is its type (decision 14.95), not the wire
            // reason it travelled under.
            let key = self.extras.get(reason_args::REFUSAL_TYPE);
            return Self::bug(key.unwrap_or(&self.code), []);
        }
        Self::bug(&self.code, self.extras)
    }

    /// A bug: a defect, never a business answer, a domain's own key
    /// included (decision 14.95: a rejection that is not a typed refusal is
    /// a bug). It goes out as ANGZARR_REASON_UNHANDLED_HANDLER_ERROR
    /// (INTERNAL), with `code` and `details` as its cause, flattened as
    /// `cause=<code>` and `cause.<arg>=<value>` (decision 14.37); the
    /// runtime alerts it and dead-letters the input, and it never reaches
    /// a policy's Translate.
    pub fn bug(code: &str, details: impl IntoIterator<Item = (String, String)>) -> Self {
        Self::keyed(
            AngzarrReason::UnhandledHandlerError,
            flatten(reason_args::CAUSE, code, details),
        )
    }

    /// A bug the handler marks retryable (an external dependency that is
    /// down), under the status it picks (decision 14.37, owner's rulings
    /// 2026-10-29, latest): that status, ANGZARR_REASON_UNHANDLED_HANDLER_ERROR, `code`
    /// and `details` as its cause, flattened as [`Self::bug`]'s are. The
    /// runtime retries it with capped backoff, then dead-letters it; it
    /// never reaches a policy's Translate.
    pub fn retryable_bug(
        status: RetryableStatus,
        code: &str,
        details: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        Self::coded(
            AngzarrReason::UnhandledHandlerError.as_str_name(),
            flatten(reason_args::CAUSE, code, details),
            status.into(),
        )
    }

    /// A handler's immediate-return business error, "re-read and
    /// re-decide" (decision 14.37, owner's rulings 2026-10-29, latest):
    /// ABORTED under the handler's own `code`, its args `details`.
    /// Returned at once to the caller, never a policy's Translate; for a
    /// policy step the runtime reloads and re-runs the step. A command's
    /// only: from a fact's handling it is a bug ([`Self::as_fact_failure`]).
    pub fn immediate(code: &str, details: impl IntoIterator<Item = (String, String)>) -> Self {
        Self::coded(code, details, GrpcCode::Aborted)
    }

    /// The router's own key `reason` with its args `extras`, under the
    /// status its `(io.angzarr.v1.reason)` option declares (decision
    /// 14.65): a type no handler claims, a component's misconfiguration, a
    /// request for a component the router does not host. Every router key
    /// travels as a status, so none is a readiness refusal.
    pub(crate) fn keyed(
        reason: AngzarrReason,
        extras: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        let grpc = GrpcCode::of_surface(reason.surface()).unwrap_or(GrpcCode::Internal);
        Self::coded(reason.as_str_name(), extras, grpc)
    }

    /// This refusal carrying its typed refusal object (one of the
    /// command's `refuses` types, packed).
    fn with_refusal(self, refusal: prost_types::Any) -> Self {
        CodedError {
            refusal: Some(Box::new(refusal)),
            ..self
        }
    }
}

/// In process too, an error shows as its code then its args, ` name=value`
/// each, a wrapped cause flattened as on the wire, a value quoted where it
/// would not parse back ([`write_value`]); never English (decision 14.37,
/// owner's clarification of 2026-10-29).
impl std::fmt::Display for CodedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.code)?;
        for (name, value) in &self.extras {
            write!(f, " {name}=")?;
            write_value(f, value)?;
        }
        Ok(())
    }
}

/// `value` as a display writes it (decision 14.37, owner ruling
/// 2026-10-29): bare, unless it would not parse back (empty, or holding
/// whitespace, `=`, `"` or `\`), then double-quoted with its `"` and `\`
/// backslash-escaped.
fn write_value(f: &mut std::fmt::Formatter<'_>, value: &str) -> std::fmt::Result {
    let ambiguous = value.is_empty()
        || value
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, '=' | '"' | '\\'));
    if !ambiguous {
        return f.write_str(value);
    }
    f.write_str("\"")?;
    for c in value.chars() {
        if matches!(c, '"' | '\\') {
            f.write_str("\\")?;
        }
        write!(f, "{c}")?;
    }
    f.write_str("\"")
}

/// A CodedError is a std error, so an applier may fail with one: a failing
/// fold then carries it as its `cause`.
impl std::error::Error for CodedError {}

/// The single handler-error mapping: coded errors pass through carrying
/// their resolved gRPC code; unclassified errors become
/// ANGZARR_REASON_UNHANDLED_HANDLER_ERROR (INTERNAL) with no cause, their text left to the
/// handler's own log.
pub fn map_handler_error(err: HandlerError) -> CodedError {
    match err {
        HandlerError::Coded(coded) => coded,
        HandlerError::Other(escaped) => {
            tracing::error!(
                code = AngzarrReason::UnhandledHandlerError.as_str_name(),
                escaped = %escaped,
                "a handler's unclassified error"
            );
            CodedError::unhandled()
        }
    }
}

/// Runs a business handler, answering its panic as a bug
/// (ANGZARR_REASON_UNHANDLED_HANDLER_ERROR, cause ANGZARR_REASON_HANDLER_PANICKED; decision 14.37) and
/// mapping its error ([`map_handler_error`]). The panic's message stays in
/// the handler's own log; the router answers rather than unwinding into its
/// host.
pub(crate) fn call_handler<T>(
    handler: impl FnOnce() -> Result<T, HandlerError>,
) -> Result<T, CodedError> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(handler)) {
        Ok(answer) => answer.map_err(map_handler_error),
        Err(_) => Err(CodedError::panicked()),
    }
}

#[cfg(test)]
#[path = "error.test.rs"]
mod error_tests;
