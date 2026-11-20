//! A domain with no events (`bare/v1/bare.proto`): its decisions return a
//! list of an event type with no value, and the adapter still builds each
//! decision's `Emit` of no events. The markup makes an `Emit` only of
//! values that are a `DomainMessage` convertible into the domain's event, so
//! the empty event type is one itself, and the helper starts from it.
//! `angzarr-build`'s `tests/local` bare-app type-checks the result against
//! angzarr-embedded.

mod common;

use angzarr_codegen::{adapters, markup_domain};
use common::*;

const BARE: &[&str] = &["bare/v1/bare.proto"];

fn domain() -> String {
    let generated = markup_domain(&descriptors(BARE), BARE, &[("bare.v1", "crate::wire")]).unwrap();
    let [one] = generated.as_slice() else {
        panic!("one package expected");
    };
    one.source.clone()
}

#[test]
fn a_domain_without_events_starts_its_empty_decision_from_its_own_event_type() {
    let source = adapters(&descriptors(BARE), BARE, &[]).unwrap();
    // Nothing the layer does not define.
    assert!(!source.contains("Rejection"), "{source}");
    let flat = source.replace([' ', '\n'], "");
    assert!(
        flat.contains("Emit::of(None::<BareStateEvent>)"),
        "{source}"
    );
}

#[test]
fn the_event_type_of_a_domain_without_events_is_a_domain_message_nothing_decodes_to() {
    let source = domain();
    let flat = source.replace([' ', '\n'], "");
    assert!(
        flat.contains("implDomainMessageforBareStateEvent")
            || flat.contains("impl::angzarr_embedded::markup::DomainMessageforBareStateEvent"),
        "{source}"
    );
    // Its wire type is the domain's state message: a prost message, and a
    // type URL no event of the domain has.
    assert!(
        flat.contains("typeWire=crate::wire::BareState;"),
        "{source}"
    );
    assert!(flat.contains("match*self{}"), "{source}");
}
