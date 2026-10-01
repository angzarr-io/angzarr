//! The delivery policy is the single table translating a synchronous caller's
//! `CascadeErrorMode` (or its absence, for bus-driven work) into what a saga
//! or PM does with an undeliverable command.

use super::*;

#[test]
fn test_background_policy_compensates_and_dead_letters_silently() {
    let policy = DeliveryPolicy::from_mode(None);
    assert_eq!(policy, DeliveryPolicy::Background);
    assert!(policy.compensates());
    assert!(policy.dead_letters());
    assert!(!policy.stops_on_failure());
    assert!(!policy.reports_failures());
}

#[test]
fn test_fail_fast_stops_and_reports_only() {
    let policy = DeliveryPolicy::from_mode(Some(CascadeErrorMode::CascadeErrorFailFast));
    assert_eq!(policy, DeliveryPolicy::FailFast);
    assert!(policy.stops_on_failure());
    assert!(policy.reports_failures());
    assert!(!policy.compensates());
    assert!(!policy.dead_letters());
}

#[test]
fn test_compensate_stops_compensates_and_reports() {
    let policy = DeliveryPolicy::from_mode(Some(CascadeErrorMode::CascadeErrorCompensate));
    assert_eq!(policy, DeliveryPolicy::Compensate);
    assert!(policy.stops_on_failure());
    assert!(policy.compensates());
    assert!(policy.reports_failures());
    assert!(!policy.dead_letters());
}

#[test]
fn test_continue_reports_without_stopping() {
    let policy = DeliveryPolicy::from_mode(Some(CascadeErrorMode::CascadeErrorContinue));
    assert_eq!(policy, DeliveryPolicy::Continue);
    assert!(!policy.stops_on_failure());
    assert!(policy.reports_failures());
    assert!(!policy.compensates());
    assert!(!policy.dead_letters());
}

#[test]
fn test_dead_letter_captures_without_reporting() {
    let policy = DeliveryPolicy::from_mode(Some(CascadeErrorMode::CascadeErrorDeadLetter));
    assert_eq!(policy, DeliveryPolicy::DeadLetter);
    assert!(policy.dead_letters());
    assert!(!policy.stops_on_failure());
    assert!(!policy.reports_failures());
    assert!(!policy.compensates());
}
