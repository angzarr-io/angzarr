//! A subscription: a projector over a scope at its credit, or a policy over
//! its input domain, one event of a stream at a time.

use std::num::NonZeroUsize;

use super::*;
use angzarr_proto::v1::{Cover, Uuid};

fn n(count: usize) -> NonZeroUsize {
    NonZeroUsize::new(count).unwrap()
}

fn stream(domain: &str, edition: Option<&str>) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(Uuid::from_bytes([1; 16])),
        edition: edition.map(str::to_string),
    }
}

#[test]
fn a_scope_is_its_domains_in_order_each_once() {
    let scope = Scope::of("mutant", &["run", "mutant", "plan"]);
    assert_eq!(scope.domains(), ["mutant", "run", "plan"]);
    assert_eq!(Scope::domain("run").domains(), ["run"]);
    assert_eq!(Scope::of("run", &[]), Scope::domain("run"));
}

#[test]
fn a_scope_holds_every_stream_of_its_domains_in_every_edition() {
    let scope = Scope::of("mutant", &["run"]);
    assert!(scope.holds(&stream("mutant", None)));
    assert!(scope.holds(&stream("run", Some("what-if"))));
    assert!(!scope.holds(&stream("plan", None)));
}

#[test]
fn a_credit_is_its_instances() {
    assert_eq!(Credit::new(n(3)).instances(), n(3));
}

#[test]
fn a_projector_subscription_keeps_its_scope_credit_and_sink() {
    let credit = Credit::new(n(4));
    let subscription: Subscription<&str, ()> =
        Subscription::projector(Scope::domain("mutant"), credit, "runner");
    assert_eq!(subscription.credit(), credit);
    assert!(!subscription.is_policy());
    match subscription.handler() {
        Handler::Projector { scope, projector } => {
            assert_eq!((scope, *projector), (&Scope::domain("mutant"), "runner"));
        }
        Handler::Policy(()) => panic!("a projector subscription is a projector's"),
    }
    let (handler, kept, own) = subscription.into_parts();
    assert!(own.is_none(), "its positions are the hub's store's");
    assert_eq!(kept, credit);
    assert!(matches!(
        handler,
        Handler::Projector {
            projector: "runner",
            ..
        }
    ));
}

#[test]
fn a_policy_subscription_is_its_policy_at_its_instances() {
    let subscription: Subscription<(), &str> = Subscription::policy(n(2), "fan-out");
    assert!(subscription.is_policy());
    assert_eq!(subscription.credit(), Credit::new(n(2)));
    assert!(matches!(subscription.handler(), Handler::Policy("fan-out")));
    let (handler, credit, _) = subscription.into_parts();
    assert_eq!(credit.instances(), n(2));
    assert!(matches!(handler, Handler::Policy("fan-out")));
}

#[test]
fn a_subscription_may_keep_its_positions_in_memory() {
    let kept: Subscription<&str, ()> =
        Subscription::projector(Scope::domain("mutant"), Credit::new(n(1)), "runner");
    assert!(kept.tracker().is_none());
    let ephemeral = kept.clone().ephemeral();
    let own = ephemeral.tracker().expect("it has its own tracker");
    assert!(!own.atomic_with_outputs() && !own.resumes());
    assert!(
        kept.tracker().is_none(),
        "a clone is a different subscription"
    );
    assert!(format!("{ephemeral:?}").contains("its own"));
}

#[test]
fn a_subscription_may_choose_any_tracker() {
    let tracker: Arc<dyn Positions> = Arc::new(MemoryPositions::new());
    let subscription: Subscription<(), &str> =
        Subscription::policy(n(1), "fan-out").positions(tracker.clone());
    let own = subscription.tracker().expect("its own");
    assert!(Arc::ptr_eq(own, &tracker));
}
