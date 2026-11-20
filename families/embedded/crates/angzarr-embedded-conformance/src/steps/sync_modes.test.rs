//! sync_modes.feature's Handle: a command is accepted or refused to its
//! caller, who waits for the answer (angzarr-embedded's one mode). The
//! background declares the components the other scenarios of the feature
//! drive; a refusal is the business code's typed one.

use cucumber::{given, then, when};

use angzarr_embedded::Any;

use crate::tests::business::{rejection, Rule};
use crate::tests::policy::{bare, Root};
use crate::tests::world::{package, Coordinator};

/// The refusal OrderUnknown of the order package, naming `order` when it
/// does (field 1, a string).
fn order_unknown(order: Option<&str>) -> angzarr_embedded::angzarr_router::error::CodedError {
    let mut error = rejection(&package("order"), "OrderUnknown");
    if let (Some(order), Some(object)) = (order, error.refusal.as_mut()) {
        object.value = [&[0x0a, order.len() as u8][..], order.as_bytes()].concat();
    }
    error
}

#[given(
    regex = r#"^an "order" aggregate that accepts CreateOrder, and whose CancelOrder names the refusal OrderUnknown, declared FAILED_PRECONDITION, refused for an unknown order$"#
)]
fn order_aggregate(w: &mut Coordinator) {
    w.business("order").rule(Rule {
        input: "CancelOrder".to_string(),
        once: None,
        error: order_unknown(None),
    });
}

#[given(regex = r#"^a projector "(\w+)" subscribed to "([\w-]+)"( only)?$"#)]
fn projector_subscribed(w: &mut Coordinator, name: String, domain: String, _only: String) {
    w.domain(&domain);
    w.projectors.insert(
        name,
        crate::tests::processed::Projected {
            domains: vec![domain],
            ..Default::default()
        },
    );
}

#[given(regex = r#"^a policy "(\w+)" translating (\w+) into a (\w+) command for "([\w-]+)"$"#)]
fn policy_translating(w: &mut Coordinator, name: String, on: String, command: String, to: String) {
    let header = crate::tests::policies::Header {
        name,
        input: "order".to_string(),
        output: to.clone(),
    };
    let reaction = crate::tests::policies::reaction(&on, &to, &command, Root::TriggerCorrelation);
    crate::tests::policies::reacts(w, &header, reaction);
}

#[given(
    regex = r#"^the CancelOrder handler refuses an unknown order with an OrderUnknown refusal with order "([\w-]+)"$"#
)]
fn handler_refuses(w: &mut Coordinator, order: String) {
    w.business("order").replace_rule(Rule {
        input: "CancelOrder".to_string(),
        once: None,
        error: order_unknown(Some(&order)),
    });
}

#[when(
    regex = r#"^a CancelOrder command for (?:an unknown order|order "([\w-]+)") is sent through Handle$"#
)]
async fn cancel_through_handle(w: &mut Coordinator, order: String) {
    w.begin();
    let name = if order.is_empty() { "unknown" } else { &order };
    let cover = w.cover("order", name);
    let book = w.command(&cover, "CancelOrder");
    w.handle(book).await;
}

#[then(regex = r#"^the status's error info carries refusal_type naming (\w+)$"#)]
fn error_info_carries(w: &mut Coordinator, refusal: String) {
    let answer = w.answer();
    let given = answer.arg("refusal_type").unwrap_or_default();
    assert!(given.ends_with(&format!(".{refusal}")), "{answer:?}");
}

#[then(regex = r#"^the status details hold the (\w+) refusal with order "([\w-]+)"$"#)]
fn details_hold(w: &mut Coordinator, refusal: String, order: String) {
    let answer = w.answer();
    let object: &Any = answer.refusal().expect("its object in the status details");
    assert_eq!(bare(object), refusal);
    assert_eq!(
        object.value,
        [&[0x0a, order.len() as u8][..], order.as_bytes()].concat()
    );
}
