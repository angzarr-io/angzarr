//! The credit gate: instances in all, one of a key.

use std::num::NonZeroUsize;

use super::*;
use angzarr_proto::v1::{Cover, Uuid};

fn key(n: u8) -> OrderingKey {
    OrderingKey::of(&Cover {
        domain: "mutant".to_string(),
        root: Some(Uuid::from_bytes([n; 16])),
        edition: None,
    })
}

fn gate(instances: usize) -> CreditGate {
    CreditGate::new(Credit::new(NonZeroUsize::new(instances).unwrap()))
}

#[test]
fn a_key_is_admitted_once_at_a_time() {
    let mut gate = gate(4);
    assert!(gate.admits(&key(1)));
    gate.take(&key(1));
    assert!(!gate.admits(&key(1)), "one of a key");
    assert!(gate.admits(&key(2)), "another key goes on");
    gate.release(&key(1));
    assert!(gate.admits(&key(1)));
    assert_eq!(gate.unsettled(), 0);
}

#[test]
fn nothing_is_admitted_past_the_instances() {
    let mut gate = gate(2);
    gate.take(&key(1));
    assert!(!gate.full());
    gate.take(&key(2));
    assert!(gate.full());
    assert!(!gate.admits(&key(3)), "two instances");
    gate.release(&key(2));
    assert!(!gate.full());
    assert!(gate.admits(&key(3)));
}

#[test]
fn a_release_of_a_key_not_taken_changes_nothing_below_zero() {
    let mut gate = gate(2);
    gate.release(&key(1));
    assert_eq!(gate.unsettled(), 0);
    gate.take(&key(1));
    gate.release(&key(1));
    gate.release(&key(1));
    assert_eq!(gate.unsettled(), 0);
    assert!(gate.admits(&key(1)));
}

#[test]
fn a_key_taken_twice_is_released_twice() {
    let mut gate = gate(4);
    gate.take(&key(1));
    gate.take(&key(1));
    assert_eq!(gate.unsettled(), 2);
    gate.release(&key(1));
    assert_eq!(gate.unsettled(), 1);
    assert!(!gate.admits(&key(1)), "one of it is still unsettled");
    gate.release(&key(1));
    assert_eq!(gate.unsettled(), 0);
    assert!(gate.admits(&key(1)));
}

#[test]
fn what_is_unsettled_counts_every_key() {
    let mut gate = gate(4);
    gate.take(&key(1));
    gate.take(&key(2));
    gate.take(&key(3));
    assert_eq!(gate.unsettled(), 3);
    gate.release(&key(2));
    assert_eq!(gate.unsettled(), 2);
}
