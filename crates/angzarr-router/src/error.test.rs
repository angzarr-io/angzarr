//! The single error-mapping table, pinned: these wire codes are the
//! adapter-level contracts client-go's engine_test.go checks through its
//! gRPC handler (DATA_LOSS for corrupt persisted events, UNIMPLEMENTED
//! for a request the router hosts no component for, INTERNAL +
//! ANGZARR_REASON_UNHANDLED_HANDLER_ERROR for unclassified failures), asserted here
//! directly on the coded form the FFI boundary will serialize.
//!
//! Decision 14.37: every error is a key and an argument map, never English.
//! The status message IS the key; a wrapped cause is flattened into the
//! args as `cause` and `cause.<arg>`; no constructor takes or carries text.

use super::*;
use angzarr_proto::v1 as pb;

fn arg(name: &str, value: &str) -> (String, String) {
    (name.to_string(), value.to_string())
}

/// A prost DecodeError, as undecodable bytes raise it.
fn decode_error() -> prost::DecodeError {
    <pb::EventBook as prost::Message>::decode(&[0xff, 0xff, 0xff][..]).expect_err("undecodable")
}

fn typed_refusal() -> prost_types::Any {
    prost_types::Any {
        type_url: "/test.wallet.InsufficientFunds".to_string(),
        value: vec![8, 100, 16, 150],
    }
}

/// Every handler- and router-facing constructor, called as decision 14.37
/// shapes them: a key and args, no text. (`HandlerError::Other`'s text is
/// the handler's own, handed in to prove it goes no further.)
fn every_constructor() -> Vec<(&'static str, CodedError)> {
    vec![
        (
            "internal",
            CodedError::keyed(AngzarrReason::NoHandlersRegistered, []),
        ),
        (
            "internal with args",
            CodedError::keyed(
                AngzarrReason::DuplicateComponent,
                [
                    arg(reason_args::REGISTERED, "order"),
                    arg(reason_args::REFUSED, "payment"),
                ],
            ),
        ),
        (
            "unimplemented",
            CodedError::keyed(AngzarrReason::NoComponentOfKind, []),
        ),
        (
            "unknown_type",
            CodedError::unknown_type(AngzarrReason::UnknownCommandType, "/a.Sent"),
        ),
        (
            "undecodable",
            CodedError::undecodable("io.angzarr.v1.EventBook", &decode_error()),
        ),
        ("persisted_corrupt", CodedError::persisted_corrupt("/a.Evt")),
        (
            "fold_failed",
            CodedError::fold_failed(
                AngzarrReason::FoldFailed,
                FoldAt::Range { first: 1, last: 2 },
                "/a.State",
                None,
            ),
        ),
        ("unhandled", CodedError::unhandled()),
        ("panicked", CodedError::panicked()),
        (
            "refused",
            CodedError::refused(BusinessStatus::FailedPrecondition, typed_refusal()),
        ),
        (
            "bug",
            CodedError::bug("LEDGER_OFFLINE", [arg("ledger", "main")]),
        ),
        (
            "retryable_bug",
            CodedError::retryable_bug(
                RetryableStatus::Unavailable,
                "LEDGER_OFFLINE",
                [arg("ledger", "main")],
            ),
        ),
        (
            "immediate",
            CodedError::immediate("PRICE_MOVED", [arg("item", "apple")]),
        ),
        (
            "map_handler_error(Other)",
            map_handler_error(HandlerError::Other("connection reset".to_string())),
        ),
    ]
}

// --- decision 14.37: a key and args, never English ------------------------------

#[test]
fn every_constructors_status_message_is_its_code() {
    let wrong: Vec<String> = every_constructor()
        .into_iter()
        .filter(|(_, err)| err.status_message() != err.code || err.code.is_empty())
        .map(|(name, err)| {
            format!(
                "{name}: status message {:?}, code {:?}",
                err.status_message(),
                err.code
            )
        })
        .collect();
    assert!(
        wrong.is_empty(),
        "the gRPC status message IS the reason key (decision 14.37):\n{}",
        wrong.join("\n")
    );
}

/// Every arg the router answers a spec key with is one the key declares
/// on its `(io.angzarr.v1.reason)` option (decision 14.60), a flattened
/// `cause.<arg>` counting as `cause`; a handler's own code (not a spec key)
/// is its own.
#[test]
fn every_spec_keys_args_are_the_ones_it_declares() {
    let mut checked = 0;
    for (name, err) in every_constructor() {
        let Some(declared) = angzarr_proto::reason_key_args(&err.code) else {
            continue;
        };
        checked += 1;
        for arg in err.extras.keys() {
            let base = arg.split_once('.').map_or(arg.as_str(), |(base, _)| base);
            assert!(
                declared.contains(&base),
                "{name}: {} answers {arg}, which it does not declare ({declared:?})",
                err.code
            );
        }
    }
    assert!(
        checked >= 10,
        "only {checked} constructors answer a spec key"
    );
}

/// `value` as a display shows it: bare, or, where it would not parse back
/// (empty, or holding whitespace, `=`, `"` or `\`), double-quoted with
/// its `"` and `\` escaped.
fn shown_value(value: &str) -> String {
    let ambiguous = value.is_empty()
        || value
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, '=' | '"' | '\\'));
    if !ambiguous {
        return value.to_string();
    }
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

// In process too, an error shows as its code then its args, ` name=value`
// each (owner's clarification of 2026-10-29), a value quoted where it
// would not parse back.
#[test]
fn every_error_displays_as_its_code_then_its_args() {
    let wrong: Vec<String> = every_constructor()
        .into_iter()
        .filter_map(|(name, err)| {
            let expected: String = std::iter::once(err.code.clone())
                .chain(
                    err.extras
                        .iter()
                        .map(|(k, v)| format!(" {k}={}", shown_value(v))),
                )
                .collect();
            let shown = err.to_string();
            (shown != expected || shown.is_empty())
                .then(|| format!("{name}: displays {shown:?}, expected {expected:?}"))
        })
        .collect();
    assert!(
        wrong.is_empty(),
        "an error's Display is its code then its args, never English:\n{}",
        wrong.join("\n")
    );
}

#[test]
fn a_bug_displays_its_cause_flattened() {
    let err = CodedError::bug("LEDGER_OFFLINE", [arg("ledger", "main")]);
    assert_eq!(
        err.to_string(),
        "ANGZARR_REASON_UNHANDLED_HANDLER_ERROR cause=LEDGER_OFFLINE cause.ledger=main"
    );
}

// A handler's unclassified error is a bug with no cause: what escaped stays
// in the handler's own log (C-0669).
#[test]
fn an_unclassified_handler_errors_text_never_leaves_the_handler() {
    let err = map_handler_error(HandlerError::Other("connection reset".to_string()));
    assert_eq!(err.grpc, GrpcCode::Internal);
    assert_eq!(err.code, AngzarrReason::UnhandledHandlerError.as_str_name());
    assert!(
        !err.extras.contains_key(reason_args::CAUSE),
        "no cause: {:?}",
        err.extras
    );
    let carried = format!("{err:?}");
    assert!(
        !carried.contains("connection reset"),
        "the escaped error's text crossed: {carried}"
    );
}

// A panic's cause is ANGZARR_REASON_HANDLER_PANICKED; its message stays in the handler's
// log (decision 14.37).
#[test]
fn a_panic_is_unhandled_handler_error_caused_by_handler_panicked() {
    let err = CodedError::panicked();
    assert_eq!(err.grpc, GrpcCode::Internal);
    assert_eq!(err.code, AngzarrReason::UnhandledHandlerError.as_str_name());
    assert_eq!(
        err.extras,
        [arg("cause", "ANGZARR_REASON_HANDLER_PANICKED")].into(),
        "the panic's cause"
    );
}

// Owner's ruling on 14.37: a typed refusal's reason is ANGZARR_REASON_COMMAND_REFUSED,
// `refusal_type` its object's fully-qualified type, the object in the
// status details.
#[test]
fn a_typed_refusal_is_command_refused_naming_its_type() {
    let err = CodedError::refused(BusinessStatus::FailedPrecondition, typed_refusal());
    assert_eq!(
        err.grpc,
        GrpcCode::FailedPrecondition,
        "its declared status"
    );
    assert_eq!(err.code, "ANGZARR_REASON_COMMAND_REFUSED");
    assert_eq!(
        err.extras,
        [arg("refusal_type", "test.wallet.InsufficientFunds")].into()
    );
    assert_eq!(err.refusal.as_deref(), Some(&typed_refusal()));
}

#[test]
fn undecodable_bytes_name_the_message_type() {
    let err = CodedError::undecodable("io.angzarr.v1.EventBook", &decode_error());
    // The router's own malformed-input error: a bug (owner ruling,
    // 2026-10-29). prost's own text rides as ext_err_txt.
    assert_eq!(err.grpc, GrpcCode::Internal);
    assert_eq!(err.code, AngzarrReason::AnyDecodeFailed.as_str_name());
    assert_eq!(
        err.extras,
        [
            arg("message_type", "io.angzarr.v1.EventBook"),
            arg("ext_err_txt", &decode_error().to_string()),
        ]
        .into()
    );
}

// Decision 14.37: the fold's own failure is its `cause`, flattened; the
// `fold_message` text entry is gone.
#[test]
fn a_fold_failures_own_failure_is_its_cause_not_text() {
    let cause = pb::Diagnostic::with_text_args("BAD_VARINT", [arg("offset", "3")]);
    let err = CodedError::fold_failed(
        AngzarrReason::FoldFailed,
        FoldAt::Page {
            sequence: 7,
            type_url: "/test.Evt",
        },
        "/test.State",
        Some(&cause),
    );
    let extra = |key: &str| err.extras.get(key).map(String::as_str);
    assert_eq!(extra("cause"), Some("BAD_VARINT"), "{:?}", err.extras);
    assert_eq!(extra("cause.offset"), Some("3"), "{:?}", err.extras);
    assert_eq!(extra("fold_message"), None, "no fold text");
}

/// The keys reasons.proto says are "Raised by the router" (decision
/// 14.104): core's AngzarrReason holds the component host's and the
/// coordinator's too.
const ROUTER_KEYS: [AngzarrReason; 13] = [
    AngzarrReason::CommandRefused,
    AngzarrReason::UnhandledHandlerError,
    AngzarrReason::HandlerPanicked,
    AngzarrReason::FoldFailed,
    AngzarrReason::FoldFailedEmitted,
    AngzarrReason::PersistedEventCorrupt,
    AngzarrReason::AnyDecodeFailed,
    AngzarrReason::UnknownCommandType,
    AngzarrReason::NoFactHandler,
    AngzarrReason::NoRefusalHandler,
    AngzarrReason::NoComponentOfKind,
    AngzarrReason::NoHandlersRegistered,
    AngzarrReason::DuplicateComponent,
];

fn router_reasons() -> Vec<AngzarrReason> {
    ROUTER_KEYS.to_vec()
}

/// The answer the router gives with `reason`, from the constructor that
/// answers it, and the key it carries for it: its code, or for
/// ANGZARR_REASON_HANDLER_PANICKED, which travels only as a cause, its
/// `cause`. Exhaustive, so a value added to the enum fails to compile here
/// until the router answers it.
fn answer(reason: AngzarrReason) -> String {
    match reason {
        AngzarrReason::HandlerPanicked => CodedError::panicked().extras[reason_args::CAUSE].clone(),
        _ => answered(reason).code,
    }
}

/// The router's answer with `reason` as its key, from the constructor that
/// answers it (ANGZARR_REASON_HANDLER_PANICKED's is the bug it is the cause
/// of).
fn answered(reason: AngzarrReason) -> CodedError {
    match reason {
        AngzarrReason::UnhandledHandlerError => CodedError::unhandled(),
        AngzarrReason::HandlerPanicked => CodedError::panicked(),
        AngzarrReason::CommandRefused => {
            CodedError::refused(BusinessStatus::FailedPrecondition, typed_refusal())
        }
        AngzarrReason::FoldFailed | AngzarrReason::FoldFailedEmitted => CodedError::fold_failed(
            reason,
            FoldAt::Range { first: 1, last: 2 },
            "/a.State",
            None,
        ),
        AngzarrReason::PersistedEventCorrupt => CodedError::persisted_corrupt("/a.Evt"),
        AngzarrReason::AnyDecodeFailed => {
            CodedError::undecodable("io.angzarr.v1.EventBook", &decode_error())
        }
        AngzarrReason::UnknownCommandType | AngzarrReason::NoFactHandler => {
            CodedError::unknown_type(reason, "/a.Sent")
        }
        AngzarrReason::NoComponentOfKind => CodedError::keyed(reason, []),
        AngzarrReason::NoRefusalHandler
        | AngzarrReason::NoHandlersRegistered
        | AngzarrReason::DuplicateComponent => CodedError::keyed(reason, []),
        other => unreachable!("{other:?} is not raised by the router"),
    }
}

// Decision 14.65: each key travels under the status its (reason) option
// declares, generated into angzarr-proto, never one typed here.
#[test]
fn every_router_key_travels_under_its_declared_status() {
    for reason in ROUTER_KEYS {
        let declared = GrpcCode::of_surface(reason.surface());
        assert!(declared.is_some(), "{reason:?} travels as a status");
        let carrier = match reason {
            // A cause only: it travels as the bug it is the cause of.
            AngzarrReason::HandlerPanicked => AngzarrReason::UnhandledHandlerError,
            _ => reason,
        };
        assert_eq!(
            Some(answered(reason).grpc),
            GrpcCode::of_surface(carrier.surface()),
            "{reason:?}"
        );
    }
}

#[test]
fn a_status_is_its_wire_number_and_readiness_is_none() {
    use angzarr_proto::google::rpc::Code;
    use angzarr_proto::ReasonSurface;
    for (status, code) in [
        (Code::InvalidArgument, GrpcCode::InvalidArgument),
        (Code::FailedPrecondition, GrpcCode::FailedPrecondition),
        (Code::Aborted, GrpcCode::Aborted),
        (Code::Unimplemented, GrpcCode::Unimplemented),
        (Code::Internal, GrpcCode::Internal),
        (Code::Unavailable, GrpcCode::Unavailable),
        (Code::DataLoss, GrpcCode::DataLoss),
    ] {
        assert_eq!(
            GrpcCode::of_surface(Some(ReasonSurface::GrpcStatus(status))),
            Some(code)
        );
    }
    assert_eq!(GrpcCode::of_surface(Some(ReasonSurface::Readiness)), None);
    assert_eq!(GrpcCode::of_surface(None), None);
    assert_eq!(
        GrpcCode::of_surface(Some(ReasonSurface::GrpcStatus(Code::Ok))),
        None,
        "OK is no status the router emits"
    );
    for n in 2..=15 {
        assert_eq!(GrpcCode::from_wire(n).map(|c| c as i32), Some(n));
    }
    for n in [0, 1, 16, 100, -1] {
        assert_eq!(GrpcCode::from_wire(n), None, "{n}");
    }
}

// Decisions 14.42, 14.44 and 14.104: the router answers each of the keys
// raised by the router, under the value's full name, and no other key of
// core's: none the component host or the coordinator answers.
#[test]
fn the_router_answers_exactly_the_router_raised_keys() {
    let reasons = router_reasons();
    for reason in &reasons {
        assert_eq!(answer(*reason), reason.as_str_name(), "{reason:?}");
    }
    let unanswered: Vec<String> = every_constructor()
        .into_iter()
        .flat_map(|(name, err)| {
            let keys = std::iter::once(err.code.clone())
                .chain(err.extras.get(reason_args::CAUSE).cloned());
            keys.map(move |key| (name, key)).collect::<Vec<_>>()
        })
        .filter(|(_, key)| key.starts_with("ANGZARR_REASON_"))
        .filter(|(_, key)| {
            AngzarrReason::from_str_name(key).is_none_or(|r| !ROUTER_KEYS.contains(&r))
        })
        .map(|(name, key)| format!("{name}: {key}"))
        .collect();
    assert!(
        unanswered.is_empty(),
        "router keys outside the router-raised keys: {unanswered:?}"
    );
}

#[test]
fn every_router_reason_is_a_prefixed_screaming_snake_key_listed_once() {
    let mut seen = std::collections::BTreeSet::new();
    for reason in router_reasons() {
        let code = reason.as_str_name();
        assert!(
            code.strip_prefix("ANGZARR_REASON_")
                .is_some_and(|rest| !rest.is_empty())
                && code
                    .chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'),
            "{code:?} is no ANGZARR_REASON_ SCREAMING_SNAKE key"
        );
        assert_eq!(AngzarrReason::from_str_name(code), Some(reason), "{code}");
        assert!(seen.insert(code), "{code} listed twice");
    }
}

// --- the mapping table ------------------------------------------------------------

#[test]
fn persisted_corrupt_is_data_loss() {
    let err = CodedError::persisted_corrupt("type.googleapis.com/test.Evt");
    assert_eq!(err.grpc, GrpcCode::DataLoss);
    assert_eq!(err.code, AngzarrReason::PersistedEventCorrupt.as_str_name());
    assert_eq!(
        err.extras.get(reason_args::TYPE_URL).map(String::as_str),
        Some("type.googleapis.com/test.Evt")
    );
}

// Owner ruling 2026-10-19: a fold failing on an accepted event is a
// handler bug, named by the event and the state it folded into; decision
// 14.37 gives it its own key.
#[test]
fn fold_failed_is_internal_naming_the_event_and_state() {
    let err = CodedError::fold_failed(
        AngzarrReason::FoldFailed,
        FoldAt::Page {
            sequence: 7,
            type_url: "/test.Evt",
        },
        "/test.State",
        None,
    );
    assert_eq!(err.grpc, GrpcCode::Internal);
    assert_eq!(err.code, "ANGZARR_REASON_FOLD_FAILED");
    let extra = |key: &str| err.extras.get(key).map(String::as_str);
    assert_eq!(extra(reason_args::SEQUENCE), Some("7"));
    assert_eq!(extra(reason_args::TYPE_URL), Some("/test.Evt"));
    assert_eq!(extra(reason_args::STATE_TYPE_URL), Some("/test.State"));
    assert_eq!(extra(reason_args::FIRST_SEQUENCE), None);
    assert_eq!(extra(reason_args::CAUSE), None, "no cause, no key");
}

#[test]
fn an_unlocated_fold_failure_names_the_batchs_range() {
    let err = CodedError::fold_failed(
        AngzarrReason::FoldFailedEmitted,
        FoldAt::Range { first: 3, last: 5 },
        "",
        None,
    );
    assert_eq!(err.grpc, GrpcCode::Internal);
    assert_eq!(err.code, "ANGZARR_REASON_FOLD_FAILED_EMITTED");
    let extra = |key: &str| err.extras.get(key).map(String::as_str);
    assert_eq!(extra(reason_args::FIRST_SEQUENCE), Some("3"));
    assert_eq!(extra(reason_args::LAST_SEQUENCE), Some("5"));
    assert_eq!(extra(reason_args::SEQUENCE), None);
    assert_eq!(extra(reason_args::STATE_TYPE_URL), Some(""));
}

// A request the router hosts no component for: a misroute or a stale
// deployment, not a malformed payload (C-0670).
#[test]
fn no_component_for_the_request_is_unimplemented() {
    let err = CodedError::keyed(AngzarrReason::NoComponentOfKind, []);
    assert_eq!(
        err.grpc,
        GrpcCode::Unimplemented,
        "misrouted request / stale deployment, not a malformed payload"
    );
    assert_eq!(err.code, "ANGZARR_REASON_NO_COMPONENT_OF_KIND");
}

// The router's own errors on malformed input are bugs (owner ruling on
// decision 14.37, 2026-10-29): INTERNAL, never a policy's OnRefused.
#[test]
fn the_routers_own_malformed_input_is_a_bug() {
    crate::test_support::assert_bug(&CodedError::keyed(AngzarrReason::NoRefusalHandler, []));
    crate::test_support::assert_bug(&CodedError::unknown_type(
        AngzarrReason::UnknownCommandType,
        "/a.B",
    ));
    crate::test_support::assert_bug(&CodedError::undecodable(
        "io.angzarr.v1.EventBook",
        &decode_error(),
    ));
}

#[test]
fn coded_handler_error_passes_through_unchanged() {
    let original = CodedError::persisted_corrupt("type.googleapis.com/test.Evt");
    let mapped = map_handler_error(HandlerError::Coded(original.clone()));
    assert_eq!(mapped, original);
}

// Decision 14.37: a handler's bug goes out as ANGZARR_REASON_UNHANDLED_HANDLER_ERROR
// (INTERNAL), the handler's own code and args its cause, flattened as
// `cause=<code>` and `cause.<arg>=<value>`.
#[test]
fn a_bug_is_unhandled_handler_error_with_the_handlers_code_as_its_cause() {
    let err = CodedError::bug("LEDGER_INVARIANT_BROKEN", [arg("balance", "-1")]);
    assert_eq!(err.grpc, GrpcCode::Internal);
    assert_eq!(err.code, AngzarrReason::UnhandledHandlerError.as_str_name());
    assert_eq!(
        err.extras,
        [
            arg("cause", "LEDGER_INVARIANT_BROKEN"),
            arg("cause.balance", "-1"),
        ]
        .into()
    );
    assert_eq!(reason_args::CAUSE, "cause");
}

// --- the statuses a handler picks (owner ruling, 2026-10-29) ---------------------

const BUSINESS: [(BusinessStatus, GrpcCode); 7] = [
    (
        BusinessStatus::FailedPrecondition,
        GrpcCode::FailedPrecondition,
    ),
    (BusinessStatus::InvalidArgument, GrpcCode::InvalidArgument),
    (BusinessStatus::NotFound, GrpcCode::NotFound),
    (BusinessStatus::AlreadyExists, GrpcCode::AlreadyExists),
    (BusinessStatus::OutOfRange, GrpcCode::OutOfRange),
    (BusinessStatus::PermissionDenied, GrpcCode::PermissionDenied),
    // Decision 14.111: a refusal may declare ABORTED.
    (BusinessStatus::Aborted, GrpcCode::Aborted),
];
const BUGS: [GrpcCode; 3] = [GrpcCode::Internal, GrpcCode::Unknown, GrpcCode::DataLoss];
// Decision 14.37 (owner's rulings, 2026-10-29, latest): four classes.
// ABORTED is a business error returned at once ("re-read and re-decide");
// the router gains DEADLINE_EXCEEDED and RESOURCE_EXHAUSTED, retryable
// bugs beside UNAVAILABLE and UNIMPLEMENTED. A handler's own ABORTED is
// `immediate`; a typed refusal may declare it too (decision 14.111).
const RETRYABLE: [GrpcCode; 4] = [
    GrpcCode::Unavailable,
    GrpcCode::Unimplemented,
    GrpcCode::DeadlineExceeded,
    GrpcCode::ResourceExhausted,
];
// The statuses a handler may pick for a retryable bug, and their wire codes.
const RETRYABLE_STATUSES: [(RetryableStatus, GrpcCode); 3] = [
    (RetryableStatus::Unavailable, GrpcCode::Unavailable),
    (
        RetryableStatus::DeadlineExceeded,
        GrpcCode::DeadlineExceeded,
    ),
    (
        RetryableStatus::ResourceExhausted,
        GrpcCode::ResourceExhausted,
    ),
];

// The wire numbers are google.rpc.Code's.
#[test]
fn deadline_exceeded_and_resource_exhausted_carry_their_wire_numbers() {
    assert_eq!(GrpcCode::DeadlineExceeded as i32, 4);
    assert_eq!(GrpcCode::ResourceExhausted as i32, 8);
}

#[test]
fn each_business_status_builds_a_refusal_with_that_status() {
    let wrong: Vec<String> = BUSINESS
        .into_iter()
        .filter_map(|(status, grpc)| {
            let err = CodedError::refused(status, typed_refusal());
            (err.grpc != grpc).then(|| format!("{status:?} built a refusal with {:?}", err.grpc))
        })
        .collect();
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

// By type: `refused` takes a BusinessStatus, so a bug's status does not
// compile (the compile_fail doctest on BusinessStatus). A status a host
// hands over at run time is checked: TryFrom accepts the business set only.
#[test]
fn only_a_business_status_converts_to_a_business_status() {
    for (status, grpc) in BUSINESS {
        assert_eq!(
            BusinessStatus::try_from(grpc),
            Ok(status),
            "{grpc:?} is a business status"
        );
    }
    for grpc in BUGS.into_iter().chain(RETRYABLE) {
        assert_eq!(
            BusinessStatus::try_from(grpc),
            Err(grpc),
            "{grpc:?} is no business status"
        );
    }
}

#[test]
fn a_bug_stays_internal_with_its_cause() {
    let err = CodedError::bug("LEDGER_OFFLINE", [arg("ledger", "main")]);
    assert_eq!(err.grpc, GrpcCode::Internal);
    assert_eq!(err.code, AngzarrReason::UnhandledHandlerError.as_str_name());
    assert_eq!(
        err.extras,
        [arg("cause", "LEDGER_OFFLINE"), arg("cause.ledger", "main")].into()
    );
}

// Decision 14.37 (owner's rulings, 2026-10-29, latest): a bug the handler
// marks retryable (its ledger service is down) goes out under the status
// the handler PICKS, under ANGZARR_REASON_UNHANDLED_HANDLER_ERROR with its own code and
// args as the cause. Never OnRefused.
#[test]
fn a_retryable_bug_goes_out_under_the_status_the_handler_picks() {
    for (status, grpc) in RETRYABLE_STATUSES {
        let err = CodedError::retryable_bug(status, "LEDGER_OFFLINE", [arg("ledger", "main")]);
        assert_eq!(err.grpc, grpc, "{status:?}: the status the handler picked");
        assert_eq!(
            err.code,
            AngzarrReason::UnhandledHandlerError.as_str_name(),
            "{status:?}"
        );
        assert_eq!(
            err.status_message(),
            AngzarrReason::UnhandledHandlerError.as_str_name()
        );
        assert_eq!(
            err.extras,
            [arg("cause", "LEDGER_OFFLINE"), arg("cause.ledger", "main")].into(),
            "{status:?}"
        );
    }
}

// A handler's retryable bug crosses the handler boundary unchanged: the
// router neither reclasses it nor folds it into a fatal bug.
#[test]
fn a_handlers_retryable_bug_keeps_its_status_through_the_boundary() {
    let err = call_handler::<()>(|| {
        Err(CodedError::retryable_bug(
            RetryableStatus::ResourceExhausted,
            "LEDGER_OFFLINE",
            [arg("ledger", "main")],
        )
        .into())
    })
    .expect_err("the handler failed");
    assert_eq!(err.grpc, GrpcCode::ResourceExhausted);
    assert_eq!(err.code, AngzarrReason::UnhandledHandlerError.as_str_name());
    assert_eq!(
        err.extras.get("cause").map(String::as_str),
        Some("LEDGER_OFFLINE")
    );
}

// Decision 14.37 (owner's rulings, 2026-10-29, latest): a handler may
// choose the immediate-return business class, "re-read and re-decide":
// ABORTED under its OWN key and args, returned at once, never OnRefused.
#[test]
fn a_handlers_immediate_rejection_is_aborted_under_its_own_code() {
    let err = CodedError::immediate("PRICE_MOVED", [arg("item", "apple")]);
    assert_eq!(err.grpc, GrpcCode::Aborted);
    assert_eq!(err.code, "PRICE_MOVED");
    assert_eq!(err.status_message(), "PRICE_MOVED");
    assert_eq!(err.extras, [arg("item", "apple")].into());
}

// A value that would not parse back is quoted (decision 14.37's display
// rule, owner ruling 2026-10-29): one empty, or holding whitespace, `=`,
// `"` or `\`, is written double-quoted, its `"` and `\` escaped with a
// backslash; any other value stays bare.
#[test]
fn a_value_that_would_not_parse_back_is_quoted() {
    let err = CodedError::bug(
        "STORE_DOWN",
        [
            arg("ext_err_txt", "database is locked"),
            arg("empty", ""),
            arg("pair", "k=v"),
            arg("said", "say \"hi\""),
            arg("path", "C:\\dir"),
            arg("tab", "a\tb"),
            arg("bare", "account/7-x_y.z"),
        ],
    );
    assert_eq!(
        err.to_string(),
        "ANGZARR_REASON_UNHANDLED_HANDLER_ERROR cause=STORE_DOWN cause.bare=account/7-x_y.z \
         cause.empty=\"\" cause.ext_err_txt=\"database is locked\" cause.pair=\"k=v\" \
         cause.path=\"C:\\\\dir\" cause.said=\"say \\\"hi\\\"\" cause.tab=\"a\tb\""
    );
}

// --- a refusal as Translate receives it, and a fact's failures --------------------

// The handler layer's Refusal (decision 14.95): the typed object and the
// business status it travelled as; nothing else is a refusal.
#[test]
fn a_typed_refusal_is_the_handler_layers_refusal() {
    for (status, _) in BUSINESS {
        let refused = CodedError::refused(status, typed_refusal());
        assert_eq!(
            refused.as_refusal(),
            Some(crate::hpb::Refusal {
                status: pb::RefusalStatus::from(status) as i32,
                typed: Some(typed_refusal()),
            }),
            "{status:?}"
        );
    }
    for other in [
        CodedError::bug("LEDGER_OFFLINE", []),
        CodedError::immediate("PRICE_MOVED", []),
        CodedError::retryable_bug(RetryableStatus::Unavailable, "DOWN", []),
    ] {
        assert_eq!(other.as_refusal(), None, "{other:?}");
    }
}

#[test]
fn a_business_status_is_its_refusal_status_and_back() {
    use angzarr_proto::v1::RefusalStatus;
    for (status, grpc) in BUSINESS {
        let refusal = RefusalStatus::from(status);
        assert_eq!(
            refusal as i32, grpc as i32,
            "{status:?}: its google.rpc.Code"
        );
        assert_eq!(BusinessStatus::try_from(refusal), Ok(status));
    }
    assert_eq!(
        BusinessStatus::try_from(RefusalStatus::Unspecified),
        Err(RefusalStatus::Unspecified),
        "no refusal declares UNSPECIFIED (decision 14.95)"
    );
}

// Decision 14.111: a refusal message may declare REFUSAL_STATUS_ABORTED,
// and it goes out as ABORTED ("re-read and re-decide").
#[test]
fn a_refusal_declaring_aborted_goes_out_as_aborted() {
    use angzarr_proto::v1::RefusalStatus;
    assert_eq!(
        BusinessStatus::try_from(RefusalStatus::Aborted).map(GrpcCode::from),
        Ok(GrpcCode::Aborted)
    );
}

// Decisions 14.72 and 14.98: a fact's handling never refuses and never
// asks for a re-run; such an answer is the bug it is, its key and args the
// cause. A bug, retryable or not, is unchanged.
#[test]
fn a_facts_refusal_or_rerun_is_a_bug_and_its_bugs_are_unchanged() {
    let refused =
        CodedError::refused(BusinessStatus::FailedPrecondition, typed_refusal()).as_fact_failure();
    assert_eq!(refused.grpc, GrpcCode::Internal);
    assert_eq!(
        refused.code,
        AngzarrReason::UnhandledHandlerError.as_str_name()
    );
    assert_eq!(
        refused.extras,
        [arg("cause", "test.wallet.InsufficientFunds")].into(),
        "the refusal's key is its type"
    );
    assert_eq!(refused.refusal, None, "the refusal object is dropped");
    let rerun = CodedError::immediate("PRICE_MOVED", [arg("item", "apple")]).as_fact_failure();
    assert_eq!(rerun.grpc, GrpcCode::Internal);
    assert_eq!(
        rerun.extras,
        [arg("cause", "PRICE_MOVED"), arg("cause.item", "apple")].into()
    );
    for (status, _) in BUSINESS {
        let business = CodedError {
            grpc: status.into(),
            ..CodedError::unhandled()
        };
        assert_eq!(
            business.as_fact_failure().grpc,
            GrpcCode::Internal,
            "{status:?}"
        );
    }
    for unchanged in [
        CodedError::bug("LEDGER_OFFLINE", [arg("ledger", "main")]),
        CodedError::retryable_bug(RetryableStatus::Unavailable, "DOWN", []),
        CodedError::unhandled(),
        CodedError::persisted_corrupt("/a.B"),
    ] {
        assert_eq!(unchanged.clone().as_fact_failure(), unchanged);
    }
}

// A refusal travels under the status its message declares (decision
// 14.95), never a default.
#[test]
fn a_refusal_travels_under_its_declared_status() {
    let err = CodedError::refused(BusinessStatus::InvalidArgument, typed_refusal());
    assert_eq!(err.grpc, GrpcCode::InvalidArgument);
    assert_eq!(err.code, "ANGZARR_REASON_COMMAND_REFUSED");
}

// --- Bug: a fact's one failure, a classified bug and never a refusal ----------------

#[test]
fn an_unclassified_bug_lowers_to_other_and_a_classified_one_to_its_bug_error() {
    assert!(matches!(
        HandlerError::from(Bug::new("log text")),
        HandlerError::Other(text) if text == "log text"
    ));
    let coded = Bug::coded("LEDGER_OFFLINE", [arg("ledger", "main")]);
    assert!(matches!(
        HandlerError::from(coded),
        HandlerError::Coded(e) if e == CodedError::bug("LEDGER_OFFLINE", [arg("ledger", "main")])
    ));
    let retry = Bug::retryable(RetryableStatus::Unavailable, "LEDGER_OFFLINE", []);
    assert!(matches!(
        HandlerError::from(retry),
        HandlerError::Coded(e) if e == CodedError::retryable_bug(RetryableStatus::Unavailable, "LEDGER_OFFLINE", [])
    ));
}

#[test]
fn a_bug_shows_its_text_or_its_code() {
    assert_eq!(Bug::new("log text").to_string(), "log text");
    assert_eq!(
        Bug::coded("LEDGER_OFFLINE", [arg("ledger", "main")]).to_string(),
        CodedError::bug("LEDGER_OFFLINE", [arg("ledger", "main")]).to_string()
    );
}

#[test]
fn from_error_makes_a_bug_of_any_error_and_a_refusal_needs_that_deliberate_call() {
    let plain = Bug::from_error(decode_error());
    assert!(matches!(HandlerError::from(plain), HandlerError::Other(t) if !t.is_empty()));
    // A router key that is already a bug keeps its code and status.
    let undecodable = CodedError::undecodable("orders.v1.ItemDelivered", &decode_error());
    assert_eq!(
        HandlerError::from(Bug::from_error(undecodable.clone())).into_coded(),
        undecodable
    );
    // A refusal or a re-run becomes the bug it is: no refusal object, no
    // business status.
    let refusal = CodedError::refused(BusinessStatus::FailedPrecondition, typed_refusal());
    let bug = HandlerError::from(Bug::from_error(refusal)).into_coded();
    assert_eq!(bug.refusal, None);
    assert_eq!(bug.grpc, GrpcCode::Internal);
    assert_eq!(bug.code, AngzarrReason::UnhandledHandlerError.as_str_name());
    let rerun =
        HandlerError::from(Bug::from_error(CodedError::immediate("PRICE_MOVED", []))).into_coded();
    assert_eq!(rerun.grpc, GrpcCode::Internal);
    assert_eq!(
        rerun.extras.get("cause").map(String::as_str),
        Some("PRICE_MOVED")
    );
}

#[test]
fn the_bug_extension_converts_a_results_error_deliberately() {
    let ok: Result<u8, CodedError> = Ok(3);
    assert_eq!(ok.bug().expect("ok"), 3);
    let refused: Result<(), CodedError> = Err(CodedError::refused(
        BusinessStatus::FailedPrecondition,
        typed_refusal(),
    ));
    let bug = refused.bug().expect_err("a bug");
    assert_eq!(HandlerError::from(bug).into_coded().refusal, None);
}

impl HandlerError {
    /// The coded error a test expects.
    fn into_coded(self) -> CodedError {
        match self {
            HandlerError::Coded(e) => e,
            HandlerError::Other(t) => panic!("not coded: {t}"),
        }
    }
}
