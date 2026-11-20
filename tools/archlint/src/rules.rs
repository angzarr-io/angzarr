//! The rules file (`archlint.toml`) and its evaluation against a [`Model`].
//!
//! ```toml
//! [[rule]]
//! name  = "core-is-the-bottom"
//! why   = "the engine core never reaches up into the C ABI layer"
//! from  = ["angzarr_router::**"]          # modules the rule constrains
//! scope = "all"                           # all | lib | test (default all)
//! allow = []                              # optional: the ONLY workspace targets permitted
//! deny  = ["angzarr_router_ffi::**", "std::ffi::**"]  # targets never permitted
//! ```
//!
//! Patterns are `::`-separated module paths: `*` matches within one segment,
//! `**` matches zero or more segments. A leading `self` segment in `allow` or
//! `deny` stands for the edge's source module (`self::**` is its own
//! subtree). `path:<glob>` patterns match on-disk targets (path dependencies
//! outside the workspace, `include!` files) by `/`-separated segments.
//!
//! `allow` constrains edges into the workspace (workspace modules and `path:`
//! targets); edges to external crates are constrained only by `deny`. An edge
//! from a module to itself is never a violation.

use std::path::Path;

use serde::Deserialize;

use crate::model::{Edge, Model, Scope};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RulesFile {
    #[serde(rename = "rule", default)]
    pub rules: Vec<Rule>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub name: String,
    pub why: String,
    pub from: Vec<String>,
    #[serde(default)]
    pub scope: ScopeSel,
    pub allow: Option<Vec<String>>,
    #[serde(default)]
    pub deny: Vec<String>,
}

/// Which edges a rule applies to: library code, test code, or both.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScopeSel {
    #[default]
    All,
    Lib,
    Test,
}

impl ScopeSel {
    pub fn covers(self, scope: Scope) -> bool {
        match self {
            ScopeSel::All => true,
            ScopeSel::Lib => scope == Scope::Lib,
            ScopeSel::Test => scope == Scope::Test,
        }
    }
}

/// One edge that breaks one rule.
#[derive(Debug)]
pub struct Violation<'a> {
    pub rule: &'a Rule,
    pub edge: &'a Edge,
}

pub fn parse(text: &str) -> Result<RulesFile, String> {
    let file: RulesFile = toml::from_str(text).map_err(|e| e.to_string())?;
    for rule in &file.rules {
        if rule.allow.is_none() && rule.deny.is_empty() {
            return Err(format!(
                "rule `{}` has neither `allow` nor `deny`",
                rule.name
            ));
        }
        if rule.from.is_empty() {
            return Err(format!("rule `{}` has an empty `from`", rule.name));
        }
    }
    Ok(file)
}

pub fn load(path: &Path) -> Result<RulesFile, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    parse(&text).map_err(|e| format!("{}: {e}", path.display()))
}

/// `from` patterns that match no module in the model: a rule that constrains
/// nothing is a stale rule, reported as an error.
pub fn unmatched_from<'a>(file: &'a RulesFile, model: &Model) -> Vec<(&'a str, &'a str)> {
    let mut out = Vec::new();
    for rule in &file.rules {
        for pattern in &rule.from {
            if !model.modules.keys().any(|m| matches(pattern, m)) {
                out.push((rule.name.as_str(), pattern.as_str()));
            }
        }
    }
    out
}

pub fn check<'a>(file: &'a RulesFile, model: &'a Model) -> Vec<Violation<'a>> {
    let mut out = Vec::new();
    for edge in &model.edges {
        if edge.from == edge.to {
            continue;
        }
        for rule in &file.rules {
            if breaks(rule, edge, model) {
                out.push(Violation { rule, edge });
            }
        }
    }
    out
}

fn breaks(rule: &Rule, edge: &Edge, model: &Model) -> bool {
    if !rule.scope.covers(edge.scope) || !rule.from.iter().any(|p| matches(p, &edge.from)) {
        return false;
    }
    let hit = |p: &String| matches(&expand_self(p, &edge.from), &edge.to);
    if rule.deny.iter().any(hit) {
        return true;
    }
    match &rule.allow {
        Some(allow) => model.is_internal(&edge.to) && !allow.iter().any(hit),
        None => false,
    }
}

/// Replaces a leading `self` segment of `pattern` with the source module.
fn expand_self(pattern: &str, from: &str) -> String {
    match pattern.strip_prefix("self") {
        Some("") => from.to_string(),
        Some(rest) if rest.starts_with("::") => format!("{from}{rest}"),
        _ => pattern.to_string(),
    }
}

/// True iff `path` matches `pattern` (see the module docs for the syntax).
pub fn matches(pattern: &str, path: &str) -> bool {
    match (pattern.strip_prefix("path:"), path.strip_prefix("path:")) {
        (Some(p), Some(q)) => segments_match(&split(p, "/"), &split(q, "/")),
        (None, None) => segments_match(&split(pattern, "::"), &split(path, "::")),
        _ => false,
    }
}

fn split<'s>(s: &'s str, sep: &str) -> Vec<&'s str> {
    s.split(sep).filter(|seg| !seg.is_empty()).collect()
}

fn segments_match(pattern: &[&str], path: &[&str]) -> bool {
    match pattern.split_first() {
        None => path.is_empty(),
        Some((&"**", rest)) => (0..=path.len()).any(|i| segments_match(rest, &path[i..])),
        Some((seg, rest)) => match path.split_first() {
            Some((head, tail)) => segment_match(seg, head) && segments_match(rest, tail),
            None => false,
        },
    }
}

/// Wildcard match of one segment: `*` matches any run of characters.
fn segment_match(pattern: &str, seg: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern == seg,
        Some((prefix, rest)) => {
            let Some(after) = seg.strip_prefix(prefix) else {
                return false;
            };
            (0..=after.len())
                .filter(|&i| after.is_char_boundary(i))
                .any(|i| segment_match(rest, &after[i..]))
        }
    }
}

#[cfg(test)]
#[path = "rules.test.rs"]
mod rules_tests;
