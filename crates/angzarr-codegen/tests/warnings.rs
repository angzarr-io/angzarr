//! The declarations' lint warnings that the codegen tier's scenarios state
//! only for the kind they warn of: no other kind, and no process manager
//! that lists a starting type, gets the warning of a process manager that
//! lists none (C-0611).

mod common;

use angzarr_codegen::{Warning, warnings};

const ORDERS: &str = "orders/v1/orders.proto";
const FULFILLMENT: &str = "fulfillment/fulfillment.proto";
const INTAKE: &str = "intake/v1/intake.proto";

/// The components that `files`' warnings say could never start an instance.
fn unstartable(files: &[&str]) -> Vec<String> {
    warnings(&common::descriptors(files), files)
        .unwrap()
        .into_iter()
        .filter_map(|w| match w {
            Warning::NoStartingInput { component } => Some(component),
            _ => None,
        })
        .collect()
}

#[test]
fn only_a_process_manager_listing_no_starting_type_is_warned_of() {
    // An aggregate, a policy and two projectors list none, and a process
    // manager lists one: none is warned of.
    assert_eq!(
        unstartable(&[ORDERS, FULFILLMENT, INTAKE]),
        Vec::<String>::new()
    );
}
