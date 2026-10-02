//! The delivery policy is the single table translating a synchronous caller's
//! `CascadeErrorMode` (or its absence, for bus-driven work) into what a saga
//! or PM does with an undeliverable command.

use super::*;

#[test]
fn test_background_policy_dead_letters_and_continues() {
    let policy = DeliveryPolicy::from_mode(None);
    assert_eq!(policy, DeliveryPolicy::Background);
    assert!(policy.dead_letters());
    assert!(!policy.stops_on_failure());
}

#[test]
fn test_fail_fast_stops_and_reports_only() {
    let policy = DeliveryPolicy::from_mode(Some(CascadeErrorMode::CascadeErrorFailFast));
    assert_eq!(policy, DeliveryPolicy::FailFast);
    assert!(policy.stops_on_failure());
    assert!(!policy.dead_letters());
}

/// An unset mode (CASCADE_ERROR_UNSPECIFIED) is FAIL_FAST (C-0437).
#[test]
fn test_unspecified_mode_is_fail_fast() {
    let policy = DeliveryPolicy::from_mode(Some(CascadeErrorMode::CascadeErrorUnspecified));
    assert_eq!(policy, DeliveryPolicy::FailFast);
}

/// COMPENSATE stops and does not dead-letter.
#[test]
fn test_compensate_stops_without_dead_letters() {
    let policy = DeliveryPolicy::from_mode(Some(CascadeErrorMode::CascadeErrorCompensate));
    assert_eq!(policy, DeliveryPolicy::Compensate);
    assert!(policy.stops_on_failure());
    assert!(!policy.dead_letters());
}

/// CONTINUE runs everything and succeeds with what was delivered.
#[test]
fn test_continue_neither_stops_nor_reports() {
    let policy = DeliveryPolicy::from_mode(Some(CascadeErrorMode::CascadeErrorContinue));
    assert_eq!(policy, DeliveryPolicy::Continue);
    assert!(!policy.stops_on_failure());
    assert!(!policy.dead_letters());
}

#[test]
fn test_dead_letter_captures_without_reporting() {
    let policy = DeliveryPolicy::from_mode(Some(CascadeErrorMode::CascadeErrorDeadLetter));
    assert_eq!(policy, DeliveryPolicy::DeadLetter);
    assert!(policy.dead_letters());
    assert!(!policy.stops_on_failure());
}

/// A non-retryable status maps to Rejected with its code, its message and
/// the ErrorInfo.reason from its details (C-0505); without ErrorInfo the
/// machine code is empty (C-0506). A retryable status maps to Retryable.
#[test]
fn test_outcome_from_status_carries_error_info_reason() {
    use crate::proto_ext::grpc::{ErrorInfo, RpcStatus};
    use prost::Message;
    let details = RpcStatus {
        code: Code::InvalidArgument as i32,
        message: "card declined".into(),
        details: vec![prost_types::Any {
            type_url: "type.googleapis.com/google.rpc.ErrorInfo".into(),
            value: ErrorInfo {
                reason: "CARD_DECLINED".into(),
                ..Default::default()
            }
            .encode_to_vec(),
        }],
    };
    let status = tonic::Status::with_details(
        Code::InvalidArgument,
        "card declined",
        details.encode_to_vec().into(),
    );
    match CommandOutcome::from_status(status) {
        CommandOutcome::Rejected {
            code,
            message,
            error_code,
        } => {
            assert_eq!(code, Code::InvalidArgument);
            assert_eq!(message, "card declined");
            assert_eq!(error_code, "CARD_DECLINED");
        }
        other => panic!("expected Rejected, got {other:?}"),
    }

    match CommandOutcome::from_status(tonic::Status::invalid_argument("bad")) {
        CommandOutcome::Rejected { error_code, .. } => assert_eq!(error_code, ""),
        other => panic!("expected Rejected, got {other:?}"),
    }
    assert!(matches!(
        CommandOutcome::from_status(tonic::Status::unavailable("down")),
        CommandOutcome::Retryable { .. }
    ));
}
