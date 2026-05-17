//! Unit tests for NATS subject construction (H-09 regression suite).
//!
//! These tests pin the per-root ordering invariant: two events with the
//! same `(domain, root)` but DIFFERENT `edition` must land on the SAME
//! subject so JetStream's per-subject ordering preserves their publish
//! order.
//!
//! H-09 (from `plans/deep-review-remediation.md`):
//!   The old subject layout was `{prefix}.events.{domain}.{root}.{edition}`.
//!   Two events for the same root across two editions ended up on
//!   different subjects. JetStream only guarantees per-subject ordering;
//!   the consumer's subject_filter (`{prefix}.events.{domain}.>`) catches
//!   both, but cross-subject delivery order is undefined.
//!
//! Chosen contract:
//!   * Subject layout: `{prefix}.events.{domain}.{root}`. Edition is
//!     carried in the EventBook payload (Cover.edition), not in the
//!     subject. This preserves per-root ordering across editions.
//!   * Consumer subject_filter remains `{prefix}.events.{domain}.>`
//!     so it still catches every event for the domain.
//!
//! The construction helper `build_subject` is a pure function so the
//! test does not need an async-nats client or NATS server.

use super::build_subject;
use uuid::Uuid;

/// H-09: two events for the same (domain, root) across two editions must
/// produce the SAME subject so per-subject ordering preserves their
/// publish order.
///
/// Baseline behaviour: subject was
/// `{prefix}.events.{domain}.{root}.{edition}`. Editions "v1" and "v2"
/// landed on distinct subjects → cross-subject ordering undefined.
///
/// Fixed behaviour: subject is `{prefix}.events.{domain}.{root}`. Same
/// root → same subject regardless of edition.
#[test]
fn build_subject_same_root_across_editions_lands_on_same_subject() {
    let prefix = "angzarr";
    let domain = "orders";
    let root = Uuid::new_v4();

    let s_main = build_subject(prefix, domain, root, "");
    let s_v1 = build_subject(prefix, domain, root, "v1");
    let s_v2 = build_subject(prefix, domain, root, "v2");
    let s_default = build_subject(prefix, domain, root, "angzarr");

    assert_eq!(
        s_main, s_v1,
        "main-timeline and v1 must share a subject for the same root \
         (cross-edition ordering invariant); H-09"
    );
    assert_eq!(
        s_v1, s_v2,
        "two named editions must share a subject for the same root \
         (cross-edition ordering invariant); H-09"
    );
    assert_eq!(
        s_main, s_default,
        "main-timeline (empty edition) and \"angzarr\" sentinel must \
         share a subject (defense-in-depth)"
    );
}

/// Regression guard: distinct (domain, root) tuples must land on
/// DISTINCT subjects. The fix must not over-correct by collapsing all
/// roots into one subject.
#[test]
fn build_subject_distinct_roots_get_distinct_subjects() {
    let prefix = "angzarr";
    let domain = "orders";
    let root_a = Uuid::new_v4();
    let root_b = Uuid::new_v4();

    let s_a = build_subject(prefix, domain, root_a, "v1");
    let s_b = build_subject(prefix, domain, root_b, "v1");

    assert_ne!(
        s_a, s_b,
        "distinct roots must land on distinct subjects so JetStream \
         can parallelise across aggregates"
    );
}

/// Regression guard: distinct domains must land on distinct subjects.
#[test]
fn build_subject_distinct_domains_get_distinct_subjects() {
    let prefix = "angzarr";
    let root = Uuid::new_v4();

    let s_orders = build_subject(prefix, "orders", root, "");
    let s_payments = build_subject(prefix, "payments", root, "");

    assert_ne!(
        s_orders, s_payments,
        "distinct domains must land on distinct subjects"
    );
}

/// Regression guard: the subject must still match the consumer's
/// filter pattern `{prefix}.events.{domain}.>` so existing
/// subscriptions keep working after the fix.
#[test]
fn build_subject_matches_consumer_filter_prefix() {
    let prefix = "angzarr";
    let domain = "orders";
    let root = Uuid::new_v4();
    let subject = build_subject(prefix, domain, root, "v1");

    let expected_prefix = format!("{}.events.{}.", prefix, domain);
    assert!(
        subject.starts_with(&expected_prefix),
        "subject {:?} must start with {:?} to be caught by the \
         consumer's `{{prefix}}.events.{{domain}}.>` filter",
        subject,
        expected_prefix
    );
}
