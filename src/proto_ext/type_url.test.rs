//! Tests for type URL helpers.
//!
//! Why this matters: the coordinator compares type URLs from many producers
//! (prost stamps "/", other languages "type.googleapis.com/"); a wrong
//! comparison would offer a snapshot of the wrong state type, or refuse a
//! valid one.

use super::*;

#[test]
fn test_for_type() {
    assert_eq!(
        for_type("io.angzarr.examples.v1.OrderCreated"),
        "/io.angzarr.examples.v1.OrderCreated"
    );
}

#[test]
fn test_strip_prefix() {
    assert_eq!(
        strip_prefix("/io.angzarr.v1.EventPage"),
        "io.angzarr.v1.EventPage"
    );
    assert_eq!(strip_prefix("no.prefix.Here"), "no.prefix.Here");
}

#[test]
fn test_fqn_is_prefix_agnostic() {
    assert_eq!(fqn("/io.angzarr.v1.EventPage"), "io.angzarr.v1.EventPage");
    assert_eq!(
        fqn("type.googleapis.com/io.angzarr.v1.EventPage"),
        "io.angzarr.v1.EventPage"
    );
    assert_eq!(fqn("io.angzarr.v1.EventPage"), "io.angzarr.v1.EventPage");
}

/// types.proto: "/orders.OrderCreated", "type.googleapis.com/orders.OrderCreated"
/// and "orders.OrderCreated" all name the same message; a different package
/// or a bare short name never does.
#[test]
fn test_same_message_compares_full_names_exactly() {
    assert!(same_message(
        "/orders.OrderCreated",
        "type.googleapis.com/orders.OrderCreated"
    ));
    assert!(same_message("orders.OrderCreated", "/orders.OrderCreated"));
    assert!(!same_message("/orders.OrderCreated", "/OrderCreated"));
    assert!(!same_message(
        "/orders.OrderCreated",
        "/orders.v2.OrderCreated"
    ));
}
