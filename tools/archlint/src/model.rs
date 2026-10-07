//! The dependency model rules are checked against: every module of every
//! workspace target, and every edge from a module to what it references.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

/// Whether an edge is library code or test-only code (`#[cfg(test)]` items
/// and modules, `tests/`, benches, examples, dev-dependencies).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Scope {
    Lib,
    Test,
}

/// A reference from module `from` to `to`.
///
/// `to` is a workspace module path (`angzarr_router::error`), an external
/// path as written (`std::ffi::c_void`), or an on-disk target
/// (`path:bindings/go`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Edge {
    pub from: String,
    pub to: String,
    pub scope: Scope,
    pub file: PathBuf,
    pub line: usize,
    /// The reference as written in the source (or the manifest entry).
    pub via: String,
}

#[derive(Debug, Default)]
pub struct Model {
    /// Module path → the file that declares its items.
    pub modules: BTreeMap<String, PathBuf>,
    /// Library names of the workspace's crates.
    pub workspace_crates: BTreeSet<String>,
    pub edges: Vec<Edge>,
}

impl Model {
    /// True iff `target` lies inside the workspace: a module of a workspace
    /// crate, or an on-disk `path:` target.
    pub fn is_internal(&self, target: &str) -> bool {
        if target.starts_with("path:") {
            return true;
        }
        let head = target.split("::").next().unwrap_or_default();
        self.workspace_crates.contains(head)
    }
}

#[cfg(test)]
#[path = "model.test.rs"]
mod model_tests;
