//! The generated `ComponentDescriptor` (spec decision 14.42,
//! `features/codegen/descriptor.feature`): the whole descriptor of each
//! component, generated at build time from its protos, with the build's
//! version as its rules version. Rust gets it in `OUT_DIR`
//! (`descriptors_rs`), a Python project in its generated `_descriptor.py`;
//! the business process answers `Describe` with it unchanged.

mod common;

use angzarr_codegen::{
    Descriptor, LICENCE_HEADER, ProtoInput, PythonApp, descriptors, descriptors_rs, scaffold_python,
};
use angzarr_proto::v1::component_descriptor::Kind;
use angzarr_proto::v1::{ComponentDescriptor, StateDescriptor};
use common::*;
use prost::Message;

const ORDERS: &str = "orders/v1/orders.proto";
const FULFILLMENT: &str = "fulfillment/fulfillment.proto";
const FILES: &[&str] = &[ORDERS, FULFILLMENT];
const VERSION: &str = "1.4.2+build.7";

fn generated() -> Vec<Descriptor> {
    descriptors(&descriptors_set(), FILES, VERSION).unwrap()
}

fn descriptors_set() -> Vec<u8> {
    common::descriptors(FILES)
}

/// The descriptor of the component named `name` (its name in an
/// application, decision 14.50), decoded.
fn descriptor(name: &str) -> ComponentDescriptor {
    let all = generated();
    let found = all
        .iter()
        .find(|d| d.name == name)
        .unwrap_or_else(|| panic!("no descriptor of {name}"));
    ComponentDescriptor::decode(found.encoded.as_slice()).expect("an encoded ComponentDescriptor")
}

/// The aggregate arm of `d`.
fn aggregate(d: ComponentDescriptor) -> angzarr_proto::v1::AggregateDescriptor {
    match d.kind {
        Some(Kind::Aggregate(a)) => a,
        other => panic!("not an aggregate: {other:?}"),
    }
}

/// The process manager arm of `d`.
fn process_manager(d: ComponentDescriptor) -> angzarr_proto::v1::ProcessManagerDescriptor {
    match d.kind {
        Some(Kind::ProcessManager(p)) => p,
        other => panic!("not a process manager: {other:?}"),
    }
}

/// The policy arm of `d`.
fn policy(d: ComponentDescriptor) -> angzarr_proto::v1::PolicyDescriptor {
    match d.kind {
        Some(Kind::Policy(p)) => p,
        other => panic!("not a policy: {other:?}"),
    }
}

/// The projector arm of `d`.
fn projector(d: ComponentDescriptor) -> angzarr_proto::v1::ProjectorDescriptor {
    match d.kind {
        Some(Kind::Projector(p)) => p,
        other => panic!("not a projector: {other:?}"),
    }
}

// C-0715
#[test]
fn a_policy_descriptor_names_its_derived_input_domain_and_its_output_domain() {
    let d = descriptor("policy-order-inventory");
    assert_eq!(d.rules_version, VERSION);
    let policy = policy(d);
    assert_eq!(policy.input_domain, "order");
    assert_eq!(policy.output_domain, "inventory");
}

// C-0535
#[test]
fn the_generated_descriptor_carries_the_build_version_and_the_states_name() {
    let order = descriptor("order");
    assert_eq!(order.name, "order");
    assert_eq!(order.rules_version, VERSION);
    let aggregate = aggregate(order);
    assert_eq!(aggregate.domain, "order");
    // A name, not a type URL (decision 14.101).
    assert_eq!(
        aggregate.state,
        Some(StateDescriptor {
            type_name: "orders.v1.OrderState".to_string()
        })
    );
}

// C-0716, C-0866
#[test]
fn a_process_manager_descriptor_names_its_domain_state_and_starting_types() {
    let pm = process_manager(descriptor("pmg-fulfillment"));
    assert_eq!(pm.domain, "fulfillment");
    assert_eq!(
        pm.state,
        Some(StateDescriptor {
            type_name: "fulfillment.FulfillmentState".to_string()
        })
    );
    // C-0607: copied from the declaration, names and no type URL.
    assert_eq!(pm.starting_types, ["fulfillment.StartFulfillment"]);
}

// C-0716
#[test]
fn each_kinds_descriptor_is_the_arm_of_its_kind() {
    let kind = |name: &str| match descriptor(name).kind {
        Some(Kind::Aggregate(_)) => "aggregate",
        Some(Kind::ProcessManager(_)) => "process_manager",
        Some(Kind::Policy(_)) => "policy",
        Some(Kind::Projector(_)) => "projector",
        None => "none",
    };
    assert_eq!(kind("order"), "aggregate");
    assert_eq!(kind("pmg-fulfillment"), "process_manager");
    assert_eq!(kind("policy-order-inventory"), "policy");
    assert_eq!(kind("projector-order-summary-view"), "projector");
}

// C-0867, decision 14.78
#[test]
fn a_projector_descriptor_lists_the_domains_of_the_events_it_names() {
    // Derived from the owner of the event it names, the order aggregate.
    let summary = projector(descriptor("projector-order-summary-view"));
    assert_eq!(summary.input_domains, ["order"]);
}

#[test]
fn a_projector_descriptor_takes_an_unowned_events_domain_from_the_entrys_override() {
    let audit = projector(descriptor("projector-audit-trail-view"));
    assert_eq!(audit.input_domains, ["audit"]);
}

/// shipping.proto's descriptors: its components' sources are derived from
/// owners declared in orders.proto and fulfillment.proto.
fn shipping() -> Vec<ComponentDescriptor> {
    const SHIPPING: &str = "orders/v1/shipping.proto";
    let set = common::descriptors(&[ORDERS, FULFILLMENT, SHIPPING]);
    descriptors(&set, &[SHIPPING], VERSION)
        .unwrap()
        .iter()
        .map(|d| ComponentDescriptor::decode(d.encoded.as_slice()).unwrap())
        .collect()
}

fn shipped(name: &str) -> ComponentDescriptor {
    shipping()
        .into_iter()
        .find(|d| d.name == name)
        .unwrap_or_else(|| panic!("no descriptor of {name}"))
}

/// The owner of an event may be declared in another file than the
/// component naming it (decision 14.78: the one aggregate or process
/// manager of the descriptor set folding it), and two events of one owner
/// are one source.
#[test]
fn a_policys_source_is_derived_from_an_owner_in_another_file() {
    let policy = policy(shipped("policy-order-shipping"));
    assert_eq!(policy.input_domain, "order");
    assert_eq!(policy.output_domain, "shipping");
}

/// A process manager owns the events it folds as an aggregate does.
#[test]
fn a_policys_source_is_a_process_managers_domain_when_it_owns_the_event() {
    let policy = policy(shipped("policy-fulfillment-shipping"));
    assert_eq!(policy.input_domain, "fulfillment");
}

/// A projector's sources are each owner's domain once, in the order of the
/// events it names.
#[test]
fn a_projectors_sources_are_each_owners_domain_once_and_an_unowned_event_none() {
    let board = projector(shipped("projector-shipping-board"));
    assert_eq!(board.input_domains, ["order", "fulfillment"]);
}

#[test]
fn only_the_files_asked_for_are_described() {
    let names: Vec<String> = shipping().into_iter().map(|d| d.name).collect();
    assert_eq!(
        names,
        [
            "policy-order-shipping",
            "policy-fulfillment-shipping",
            "projector-shipping-board"
        ]
    );
}

#[test]
fn every_component_has_one_descriptor_in_declaration_order() {
    let all: Vec<(String, String)> = generated()
        .into_iter()
        .map(|d| (d.component, d.name))
        .collect();
    assert_eq!(
        all,
        s(&[
            ("orders.v1.OrderState", "order"),
            ("orders.v1.OrderToInventoryPolicy", "policy-order-inventory"),
            ("orders.v1.OrderSummaryView", "projector-order-summary-view"),
            ("orders.v1.AuditTrailView", "projector-audit-trail-view"),
            ("fulfillment.FulfillmentState", "pmg-fulfillment"),
        ])
    );
}

/// `descriptors_rs`'s `DESCRIPTORS`: each `(name, bytes)` entry.
fn rust_entries(source: &str) -> Vec<(String, Vec<u8>)> {
    let file = syn::parse_file(source).expect("the generated Rust parses");
    let constant = file
        .items
        .iter()
        .find_map(|i| match i {
            syn::Item::Const(c) if c.ident == "DESCRIPTORS" => Some(c),
            _ => None,
        })
        .expect("const DESCRIPTORS");
    assert_eq!(text(&constant.ty), "&[(&str,&[u8])]");
    let syn::Expr::Reference(slice) = &*constant.expr else {
        panic!("DESCRIPTORS is a slice reference");
    };
    let syn::Expr::Array(array) = &*slice.expr else {
        panic!("DESCRIPTORS is an array");
    };
    array
        .elems
        .iter()
        .map(|e| {
            let syn::Expr::Tuple(pair) = e else {
                panic!("an entry is a pair");
            };
            let lit = |i: usize| match &pair.elems[i] {
                syn::Expr::Lit(l) => l.lit.clone(),
                other => panic!("a literal, not {}", text(other)),
            };
            let (syn::Lit::Str(name), syn::Lit::ByteStr(bytes)) = (lit(0), lit(1)) else {
                panic!("a name and its bytes");
            };
            (name.value(), bytes.value())
        })
        .collect()
}

#[test]
fn the_rust_build_layer_holds_each_descriptor_unchanged() {
    let source = descriptors_rs(&descriptors_set(), FILES, &[], VERSION).unwrap();
    let expected: Vec<(String, Vec<u8>)> = generated()
        .into_iter()
        .map(|d| (d.name, d.encoded))
        .collect();
    assert_eq!(rust_entries(&source), expected);
}

#[test]
fn the_rust_build_layer_leaves_a_remote_domain_to_its_process() {
    // The order domain is served by a child process: its aggregate's
    // descriptor is that process's; the policy and the projectors stay.
    let source = descriptors_rs(&descriptors_set(), FILES, &["order"], VERSION).unwrap();
    let names: Vec<String> = rust_entries(&source).into_iter().map(|e| e.0).collect();
    assert_eq!(
        names,
        [
            "policy-order-inventory",
            "projector-order-summary-view",
            "projector-audit-trail-view",
            "pmg-fulfillment"
        ]
    );
}

/// A policy or a projector a child process serves is left to it by its
/// name, as a remote domain is by its domain.
#[test]
fn the_rust_build_layer_leaves_a_child_policy_or_projector_to_its_process() {
    let remote = ["policy-order-inventory", "projector-audit-trail-view"];
    let source = descriptors_rs(&descriptors_set(), FILES, &remote, VERSION).unwrap();
    let names: Vec<String> = rust_entries(&source).into_iter().map(|e| e.0).collect();
    assert_eq!(
        names,
        ["order", "projector-order-summary-view", "pmg-fulfillment"]
    );
}

fn order_app() -> PythonApp {
    PythonApp {
        domain: "order".to_string(),
        protos: ProtoInput::Directory("proto".to_string()),
        rules_version: VERSION.to_string(),
    }
}

/// The bytes of `_descriptor.py`'s `bytes.fromhex(...)`: its hex strings,
/// concatenated.
fn python_bytes(source: &str) -> Vec<u8> {
    let start = source.find("bytes.fromhex(").expect("bytes.fromhex") + "bytes.fromhex(".len();
    let end = start + source[start..].find(')').expect("a closing paren");
    let hex: String = source[start..end].split('"').skip(1).step_by(2).collect();
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex"))
        .collect()
}

// C-0565: what the business process answers Describe with is the
// generated descriptor, unchanged.
#[test]
fn the_python_project_holds_its_components_generated_descriptor() {
    let files = scaffold_python(&descriptors_set(), &[ORDERS], &order_app()).unwrap();
    let file = files
        .iter()
        .find(|f| f.path == "src/angzarr_order/_gen/_descriptor.py")
        .expect("_descriptor.py");
    assert_eq!(file.owner, angzarr_codegen::Owner::Generated);
    assert!(
        file.source
            .contains("DESCRIPTOR = _t.ComponentDescriptor.FromString("),
        "{}",
        file.source
    );
    let order = generated().into_iter().find(|d| d.name == "order").unwrap();
    assert_eq!(python_bytes(&file.source), order.encoded);
    // The licence header is the one line past it: a comment, which Black
    // leaves as it is (owner ruling 2026-10-31).
    for line in file.source.lines().skip(1) {
        assert!(line.len() <= 88, "longer than Black's width: {line}");
    }
    assert!(file.source.starts_with(&format!("# {LICENCE_HEADER}\n")));
}
