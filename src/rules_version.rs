//! Which stored snapshots may be used: the coordinator's snapshot rule
//! (types.proto, `Snapshot`, "Validity"; decisions 14.1, 14.3, 14.4, 14.9).

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::proto::Snapshot;
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

/// The snapshot rules of every domain: the default rule, or a domain's
/// configured override.
#[derive(Debug, Clone, Default)]
pub struct SnapshotRules {}

impl SnapshotRules {
    /// Compile the configured overrides.
    pub fn from_config(_config: &SnapshotCompatibilityConfig) -> Result<Self, String> {
        Ok(Self {})
    }

    /// Whether `snapshot` may be offered for `domain` against
    /// `registration`.
    pub fn offerable(
        &self,
        _domain: &str,
        _snapshot: &Snapshot,
        _registration: &Registration,
    ) -> bool {
        false
    }
}
