//! What a bus served: how many items its handlers returned handled, and
//! each failure, named by subscription, stream and sequence.

use super::*;
use angzarr_diagnostic::Report;
use angzarr_proto::v1::{Cover, Uuid};

fn failed(subscription: &str, sequence: Option<u64>) -> Failed {
    Failed {
        subscription: subscription.to_string(),
        stream: Cover {
            domain: "mutant".to_string(),
            root: Some(Uuid::from_bytes([2; 16])),
            edition: None,
        },
        sequence,
        cause: Report::new("angzarr-bus-test-failed"),
        outcome: Outcome::Held,
    }
}

#[test]
fn nothing_served_is_nothing_handled_and_no_failure() {
    let served = Served::new();
    assert_eq!(served.handled(), 0);
    assert!(served.failures().is_empty());
    assert!(served.is_empty());
    assert_eq!(served, Served::default());
}

#[test]
fn what_is_served_adds_up() {
    let mut served = Served::new();
    served.add_handled(2);
    served.add_handled(3);
    served.push(failed("runner", Some(4)));
    served.push(failed("fan-in", None));
    assert_eq!(served.handled(), 5);
    assert!(!served.is_empty());
    assert_eq!(
        served.failures(),
        [failed("runner", Some(4)), failed("fan-in", None)]
    );
}

#[test]
fn a_failure_alone_is_something_served() {
    let mut served = Served::new();
    served.push(failed("runner", Some(0)));
    assert!(!served.is_empty());
    let mut served = Served::new();
    served.add_handled(1);
    assert!(!served.is_empty());
}
