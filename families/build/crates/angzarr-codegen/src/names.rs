//! A component's name in an application (decisions 14.50 and 14.87): what
//! the host config's `component` entry, the generated registration and
//! hand-written code call it, all through [`component_names`]. It is
//! `ComponentOptions.component_name`, the one name a declaration must give:
//! lower-case ASCII letters, digits and `-`, starting with a letter; an
//! empty or any other name fails generation
//! ([`crate::MarkupError::ComponentName`]). There is no default and no per-kind
//! formula. The generated code is named after the anchor message.

use crate::error::CodegenError;
use crate::model;

/// A component's kind.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NameKind {
    Aggregate,
    ProcessManager,
    Policy,
    Projector,
}

/// Lower-case ASCII letters, digits and `-`, starting with a letter.
pub(crate) fn well_formed(name: &str) -> bool {
    name.starts_with(|c: char| c.is_ascii_lowercase())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

/// One component of a descriptor set and its name in an application.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ComponentName {
    /// The anchor message's full name, `io.angzarr.examples.v1.TableState`.
    pub component: String,
    pub kind: NameKind,
    /// Its name in an application (its `component_name`).
    pub name: String,
    /// The stream domain it owns: an aggregate's or a process manager's,
    /// else empty.
    pub domain: String,
}

/// Every component declared in `files` of the descriptor set (protoc's
/// own bytes, as [`crate::markup_domain`] takes), with its name, in
/// declaration order: what an application's config and hand-written code
/// read the names from.
pub fn component_names(
    descriptor_set: &[u8],
    files: &[&str],
) -> Result<Vec<ComponentName>, CodegenError> {
    Ok(model::components(descriptor_set, files, model::ALL_KINDS)?
        .into_iter()
        .map(|c| ComponentName {
            component: c.anchor.full_name().to_string(),
            kind: c.kind.name_kind(),
            name: c.component_name,
            domain: c.domain,
        })
        .collect())
}
