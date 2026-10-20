//! Which stored snapshots may be used: the coordinator's snapshot rule
//! (types.proto, `Snapshot`, "Validity"; decisions 14.1, 14.3, 14.4, 14.9).
//!
//! A snapshot is offered only when
//! 1. its state type is the registered state type, and
//! 2. its `rules_version` passes the domain's rule against the registered
//!    version:
//!    - by default, after stripping ONE leading `v`/`V` from each, both parse
//!      as SemVer 2.0.0 and their major and minor are equal; if either does
//!      not parse, the versions as recorded must be equal;
//!    - a domain's configured override replaces that: the accept pattern
//!      must match the whole snapshot version and the reject pattern, when
//!      set, must not (a reject match wins).
//!
//! Patterns use RE2 syntax (Rust's `regex`, linear time, no lookaround or
//! backreferences); one outside it is refused when the configuration loads.

use std::collections::{BTreeMap, HashMap};

use regex::Regex;
use serde::Deserialize;

use crate::proto::Snapshot;
use crate::proto_ext::type_url;
use crate::registry::Registration;

/// One domain's override, as configured
/// (`snapshot_compatibility.<domain>.{accept,reject}`).
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct SnapshotRuleConfig {
    /// Required: the snapshot's version must match it, whole.
    pub accept: Option<String>,
    /// Optional: a whole match refuses the snapshot, whatever `accept` says.
    pub reject: Option<String>,
}

/// The configured overrides, by domain.
pub type SnapshotCompatibilityConfig = BTreeMap<String, SnapshotRuleConfig>;

/// A domain's compiled override.
#[derive(Debug, Clone)]
struct Override {
    accept: Regex,
    reject: Option<Regex>,
}

/// The snapshot rules of every domain: the default rule, or a domain's
/// configured override.
#[derive(Debug, Clone, Default)]
pub struct SnapshotRules {
    overrides: HashMap<String, Override>,
}

impl SnapshotRules {
    /// Compile the configured overrides. A missing `accept`, or a pattern
    /// outside the supported syntax, is refused with a message naming the
    /// configuration key.
    pub fn from_config(config: &SnapshotCompatibilityConfig) -> Result<Self, String> {
        let mut overrides = HashMap::new();
        for (domain, rule) in config {
            let accept = rule
                .accept
                .as_deref()
                .ok_or_else(|| format!("snapshot_compatibility.{domain}.accept is required"))?;
            let accept = compile(domain, "accept", accept)?;
            let reject = rule
                .reject
                .as_deref()
                .map(|p| compile(domain, "reject", p))
                .transpose()?;
            overrides.insert(domain.clone(), Override { accept, reject });
        }
        Ok(Self { overrides })
    }

    /// Whether `snapshot` may be offered for `domain` against
    /// `registration`.
    pub fn offerable(
        &self,
        domain: &str,
        snapshot: &Snapshot,
        registration: &Registration,
    ) -> bool {
        let same_state = match (&snapshot.state, registration.state()) {
            (Some(state), Some(registered)) => {
                type_url::same_message(&state.type_url, &registered.type_url)
            }
            _ => false,
        };
        same_state
            && self.compatible(
                domain,
                &snapshot.rules_version,
                registration.rules_version(),
            )
    }

    /// Whether a snapshot recorded under `snapshot_version` passes `domain`'s
    /// rule against `registered_version`.
    pub fn compatible(
        &self,
        domain: &str,
        snapshot_version: &str,
        registered_version: &str,
    ) -> bool {
        match self.overrides.get(domain) {
            Some(rule) => {
                rule.accept.is_match(snapshot_version)
                    && !rule
                        .reject
                        .as_ref()
                        .is_some_and(|r| r.is_match(snapshot_version))
            }
            None => default_compatible(snapshot_version, registered_version),
        }
    }
}

/// Compile `pattern` for whole-version matching.
fn compile(domain: &str, key: &str, pattern: &str) -> Result<Regex, String> {
    Regex::new(&format!("^(?:{pattern})$")).map_err(|e| {
        format!("snapshot_compatibility.{domain}.{key}: {pattern:?} is not a supported (RE2) pattern: {e}")
    })
}

/// The default rule (decisions 14.1, 14.3).
fn default_compatible(snapshot_version: &str, registered_version: &str) -> bool {
    match (semver_of(snapshot_version), semver_of(registered_version)) {
        (Some(a), Some(b)) => a.major == b.major && a.minor == b.minor,
        _ => snapshot_version == registered_version,
    }
}

/// SemVer 2.0.0 after stripping one leading `v` or `V`.
fn semver_of(version: &str) -> Option<semver::Version> {
    let stripped = version
        .strip_prefix('v')
        .or_else(|| version.strip_prefix('V'))
        .unwrap_or(version);
    semver::Version::parse(stripped).ok()
}

#[cfg(test)]
#[path = "rules_version.test.rs"]
mod tests;
