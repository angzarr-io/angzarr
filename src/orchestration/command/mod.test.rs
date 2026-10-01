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
