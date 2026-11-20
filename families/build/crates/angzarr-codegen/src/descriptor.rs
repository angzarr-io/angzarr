//! Each component's whole `ComponentDescriptor` (types.proto), generated
//! at build time from its protos (decision 14.42): its name in an
//! application (decision 14.87), the build's version as its rules version,
//! and, in the arm of its kind (decision 14.78), what that kind has: an
//! aggregate's domain and state, a process manager's domain, state and
//! starting types, a policy's derived input domain and its output domain,
//! a projector's derived input domains. Types are named by their
//! fully-qualified name, never a type URL (decision 14.101). The business
//! process answers `ComponentService.Describe` with it unchanged; the
//! coordinator validates it.
//!
//! It is built as angzarr-proto's `ComponentDescriptor` (the descriptor set
//! need not hold it: options.proto does not import types.proto) and kept
//! encoded, so each language's generated code holds the same bytes.

use angzarr_proto::v1::component_descriptor::Kind;
use angzarr_proto::v1::{
    AggregateDescriptor, ComponentDescriptor, PolicyDescriptor, ProcessManagerDescriptor,
    ProjectorDescriptor, StateDescriptor,
};
use prost_reflect::prost::Message;
use quote::quote;

use crate::error::CodegenError;
use crate::model::{self, ALL_KINDS, Component, ComponentKind};

/// One component's generated descriptor.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Descriptor {
    /// The component's anchor message, e.g. `orders.v1.OrderState`.
    pub component: String,
    /// `ComponentDescriptor.name`: the component's name in an application
    /// (its `component_name`, decision 14.87).
    pub name: String,
    /// The `ComponentDescriptor`, encoded.
    pub encoded: Vec<u8>,
}

/// The descriptor of every component declared in `files`, in declaration
/// order, `rules_version` the build's version.
pub fn descriptors(
    descriptor_set: &[u8],
    files: &[&str],
    rules_version: &str,
) -> Result<Vec<Descriptor>, CodegenError> {
    Ok(build(descriptor_set, files, rules_version)?
        .into_iter()
        .map(|(_, d)| d)
        .collect())
}

/// Each component's descriptor beside the component.
pub(crate) fn build(
    descriptor_set: &[u8],
    files: &[&str],
    rules_version: &str,
) -> Result<Vec<(Component, Descriptor)>, CodegenError> {
    let components = model::components(descriptor_set, files, ALL_KINDS)?;
    Ok(components
        .into_iter()
        .map(|component| {
            let descriptor = Descriptor {
                component: component.anchor.full_name().to_string(),
                name: component.component_name.clone(),
                encoded: encode(&component, rules_version),
            };
            (component, descriptor)
        })
        .collect())
}

/// `component`'s `ComponentDescriptor`, encoded: its name and the build's
/// version, and in the arm of its kind what that kind has.
fn encode(component: &Component, rules_version: &str) -> Vec<u8> {
    let state = || {
        Some(StateDescriptor {
            type_name: component.anchor.full_name().to_string(),
        })
    };
    let kind = match component.kind {
        ComponentKind::Aggregate => Kind::Aggregate(AggregateDescriptor {
            domain: component.domain.clone(),
            state: state(),
        }),
        ComponentKind::ProcessManager => Kind::ProcessManager(ProcessManagerDescriptor {
            domain: component.domain.clone(),
            state: state(),
            starting_types: component.starting_types.clone(),
        }),
        ComponentKind::Policy => Kind::Policy(PolicyDescriptor {
            input_domain: component.input_domain.clone(),
            output_domain: component.output_domain.clone(),
        }),
        ComponentKind::Projector => Kind::Projector(ProjectorDescriptor {
            input_domains: component.sources.clone(),
        }),
    };
    ComponentDescriptor {
        name: component.component_name.clone(),
        rules_version: rules_version.to_string(),
        kind: Some(kind),
    }
    .encode_to_vec()
}

/// The Rust build layer's descriptors (`angzarr-descriptors.rs`): every
/// component's in `files` but those a child process serves, which has its
/// own: `remote` names a domain by its stream domain, a policy or a
/// projector by its name in the application.
pub fn descriptors_rs(
    descriptor_set: &[u8],
    files: &[&str],
    remote: &[&str],
    rules_version: &str,
) -> Result<String, CodegenError> {
    let entries = build(descriptor_set, files, rules_version)?
        .into_iter()
        .filter(|(c, _)| {
            !remote.contains(&c.domain.as_str()) && !remote.contains(&c.component_name.as_str())
        })
        .map(|(_, d)| {
            let name = d.name;
            let bytes = proc_macro2::Literal::byte_string(&d.encoded);
            quote!((#name, #bytes))
        });
    let doc = [
        " Each component's `ComponentDescriptor`, encoded, by name: generated from".to_string(),
        format!(" its protos with this build's version, {rules_version:?}, as its"),
        " rules_version (decision 14.42). It is answered to `Describe` unchanged.".to_string(),
    ];
    let source = crate::scaffold::file(quote! {
        #(#[doc = #doc])*
        pub const DESCRIPTORS: &[(&str, &[u8])] = &[#(#entries),*];
    });
    Ok(crate::headed(
        ".rs",
        format!(
            "// @generated by angzarr-codegen from {}. Do not edit.\n\n{source}",
            files.join(", ")
        ),
    ))
}
