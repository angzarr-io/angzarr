//! Tests for [`CodeDlqExt::classify_for_dlq`].
//!
//! Why this matters: saga, PM, projector and aggregate paths all decide
//! "dead-letter now, or retry first?" here. The contract has ONE retryable
//! set (command_handler.proto, "Status codes"); a code bucketed differently
//! here than in the retry loops would either burn retries on a terminal
//! failure or dead-letter a failure that would have succeeded on retry.

use super::*;
use crate::utils::retry::test_support::ALL_CODES;
use tonic::Code;

#[test]
fn every_code_follows_the_one_retryable_set() {
    for &(code, retryable) in ALL_CODES {
        let expected = if retryable || code == Code::Ok {
            DlqTrigger::RetryThenDlq(RetryThenDlq(code))
        } else {
            DlqTrigger::Immediate(Immediate(code))
        };
        assert_eq!(code.classify_for_dlq(), expected, "{code:?}");
    }
}

/// A business rejection (FAILED_PRECONDITION) and a handler bug (INTERNAL)
/// are terminal: retrying re-hits the same rule or the same bug.
#[test]
fn business_rejections_and_handler_bugs_dead_letter_now() {
    assert!(matches!(
        Code::FailedPrecondition.classify_for_dlq(),
        DlqTrigger::Immediate(_)
    ));
    assert!(matches!(
        Code::Internal.classify_for_dlq(),
        DlqTrigger::Immediate(_)
    ));
}

/// A stale sequence (ABORTED) is retryable: the framework re-reads and
/// re-issues commands whose sequence it stamped.
#[test]
fn aborted_is_retried_first() {
    assert!(matches!(
        Code::Aborted.classify_for_dlq(),
        DlqTrigger::RetryThenDlq(_)
    ));
}
