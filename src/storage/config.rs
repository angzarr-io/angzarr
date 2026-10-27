//! Storage configuration types.

use serde::Deserialize;

// ============================================================================
// Backend registry + role references
// ============================================================================
//
// Backends are defined ONCE in a named registry; the storage roles (event /
// snapshot / position store) each reference a registry entry by name, so one
// physical backend can serve several roles. Role capability is validated
// against the registry at load time via `validate`.
//
// Only SQLite is ported to the latest proto. The other backend types still
// parse (the configuration surface is unchanged) and are refused when the
// factory constructs them (`factory::not_ported`).

/// A single backend definition: a registry value, internally tagged on
/// `type`. Each variant carries only that backend's own config.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum BackendConfig {
    /// PostgreSQL (event / snapshot; feature `postgres`). Positions are not
    /// yet ported.
    Postgres(PostgresConfig),
    /// SQLite (event / snapshot / position).
    Sqlite(SqliteConfig),
    /// Redis (snapshot only). Not yet ported.
    Redis(RedisConfig),
    /// Composite event store: append-only `main` + mutable `editions`, each
    /// naming another registry entry. Event role only. Not implemented.
    Composite(CompositeBackendConfig),
}

/// Composite event-store backend: references two registry entries by name.
#[derive(Debug, Clone, Deserialize)]
pub struct CompositeBackendConfig {
    /// Registry name of the append-only main-chain event backend.
    pub main: String,
    /// Registry name of the mutable editions backend (must reclaim on delete).
    pub editions: String,
}

/// The storage role a backend is asked to fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageRole {
    /// Event store (the source of truth).
    Event,
    /// Snapshot store (a replay cache).
    Snapshot,
    /// Position store (handler checkpoints).
    Position,
}

impl BackendConfig {
    /// Human-readable backend type name (for error messages).
    pub fn type_name(&self) -> &'static str {
        match self {
            BackendConfig::Postgres(_) => "postgres",
            BackendConfig::Sqlite(_) => "sqlite",
            BackendConfig::Redis(_) => "redis",
            BackendConfig::Composite(_) => "composite",
        }
    }

    /// Whether this backend can serve the given role.
    pub fn supports_role(&self, role: StorageRole) -> bool {
        match role {
            StorageRole::Event => self.is_event_capable(),
            StorageRole::Snapshot => self.is_snapshot_capable(),
            StorageRole::Position => self.is_position_capable(),
        }
    }

    fn is_event_capable(&self) -> bool {
        !matches!(self, BackendConfig::Redis(_))
    }

    fn is_snapshot_capable(&self) -> bool {
        matches!(
            self,
            BackendConfig::Postgres(_) | BackendConfig::Sqlite(_) | BackendConfig::Redis(_)
        )
    }

    fn is_position_capable(&self) -> bool {
        matches!(self, BackendConfig::Postgres(_) | BackendConfig::Sqlite(_))
    }

    /// Whether this backend reclaims space on delete: required for the
    /// `editions` half of a composite.
    fn is_editions_capable(&self) -> bool {
        matches!(self, BackendConfig::Postgres(_) | BackendConfig::Sqlite(_))
    }
}

/// A storage role's reference into the backend registry (`{ use: <name> }`).
#[derive(Debug, Clone, Deserialize)]
pub struct BackendRef {
    /// Registry name of the backend that fills this role.
    #[serde(rename = "use")]
    pub backend: String,
}

/// Storage configuration: a named backend registry plus per-role references.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StorageRegistryConfig {
    /// Named backend definitions.
    pub backends: std::collections::BTreeMap<String, BackendConfig>,
    /// Event-store role reference.
    pub events: BackendRef,
    /// Snapshot-store role reference.
    pub snapshots: BackendRef,
    /// Position-store role reference.
    pub positions: BackendRef,
    /// Snapshot read/write enable flags.
    pub snapshots_enable: SnapshotsEnableConfig,
}

impl Default for StorageRegistryConfig {
    fn default() -> Self {
        let mut backends = std::collections::BTreeMap::new();
        backends.insert(
            "default".to_string(),
            BackendConfig::Postgres(PostgresConfig::default()),
        );
        Self {
            backends,
            events: BackendRef {
                backend: "default".to_string(),
            },
            snapshots: BackendRef {
                backend: "default".to_string(),
            },
            positions: BackendRef {
                backend: "default".to_string(),
            },
            snapshots_enable: SnapshotsEnableConfig::default(),
        }
    }
}

impl StorageRegistryConfig {
    /// Validate that every role reference resolves to a registry entry capable of
    /// that role, and that each `Composite` backend references a valid
    /// (event-capable) main and a valid (reclaim-capable) editions entry.
    /// Returns a human-readable message describing the first violation.
    pub fn validate(&self) -> std::result::Result<(), String> {
        let check =
            |role: StorageRole, name: &str, label: &str| -> std::result::Result<(), String> {
                let backend = self.backends.get(name).ok_or_else(|| {
                    format!("storage.{label} references unknown backend '{name}'")
                })?;
                if !backend.supports_role(role) {
                    return Err(format!(
                        "storage.{label} backend '{name}' (type {}) cannot serve the {label} role",
                        backend.type_name()
                    ));
                }
                Ok(())
            };
        check(StorageRole::Event, &self.events.backend, "events")?;
        check(StorageRole::Snapshot, &self.snapshots.backend, "snapshots")?;
        check(StorageRole::Position, &self.positions.backend, "positions")?;

        for (name, backend) in &self.backends {
            if let BackendConfig::Composite(c) = backend {
                let main = self.backends.get(&c.main).ok_or_else(|| {
                    format!(
                        "composite backend '{name}' references unknown main '{}'",
                        c.main
                    )
                })?;
                if matches!(main, BackendConfig::Composite(_)) || !main.is_event_capable() {
                    return Err(format!(
                        "composite backend '{name}' main '{}' (type {}) is not an event-capable backend",
                        c.main,
                        main.type_name()
                    ));
                }
                let editions = self.backends.get(&c.editions).ok_or_else(|| {
                    format!(
                        "composite backend '{name}' references unknown editions '{}'",
                        c.editions
                    )
                })?;
                if !editions.is_editions_capable() {
                    return Err(format!(
                        "composite backend '{name}' editions '{}' (type {}) cannot reclaim space on delete; editions require a mutable backend",
                        c.editions,
                        editions.type_name()
                    ));
                }
            }
        }
        Ok(())
    }

    /// Resolve a role to its referenced backend config, re-checking capability.
    /// The factory uses this to choose which backend to construct for a role.
    pub fn resolve(&self, role: StorageRole) -> std::result::Result<&BackendConfig, String> {
        let name = match role {
            StorageRole::Event => &self.events.backend,
            StorageRole::Snapshot => &self.snapshots.backend,
            StorageRole::Position => &self.positions.backend,
        };
        let backend = self
            .backends
            .get(name)
            .ok_or_else(|| format!("storage role references unknown backend '{name}'"))?;
        if !backend.supports_role(role) {
            return Err(format!(
                "backend '{name}' (type {}) cannot serve the requested role",
                backend.type_name()
            ));
        }
        Ok(backend)
    }
}

/// PostgreSQL-specific configuration.
#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct PostgresConfig {
    /// PostgreSQL connection URI.
    pub uri: String,
}

impl std::fmt::Debug for PostgresConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresConfig")
            .field("uri", &"<redacted>")
            .finish()
    }
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            uri: "postgres://localhost:5432/angzarr".to_string(),
        }
    }
}

/// SQLite-specific configuration.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct SqliteConfig {
    /// SQLite database path.
    /// If empty or not set, uses in-memory database (:memory:).
    pub path: Option<String>,
}

impl SqliteConfig {
    /// Get the connection URI for SQLite.
    /// Returns in-memory URI if path is not configured.
    pub fn uri(&self) -> String {
        match &self.path {
            Some(path) if !path.is_empty() => format!("sqlite:{}", path),
            _ => "sqlite::memory:".to_string(),
        }
    }
}

/// Redis-specific configuration.
#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct RedisConfig {
    /// Redis connection URI.
    pub uri: String,
}

impl std::fmt::Debug for RedisConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisConfig")
            .field("uri", &"<redacted>")
            .finish()
    }
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            uri: "redis://localhost:6379".to_string(),
        }
    }
}

/// Snapshot enable/disable configuration.
///
/// These flags are useful for debugging and troubleshooting snapshot-related issues.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SnapshotsEnableConfig {
    /// Enable reading snapshots when loading aggregate state.
    /// When false, always replays all events from the beginning.
    /// Useful for debugging to verify event replay produces correct state.
    /// Default: true
    pub read: bool,
    /// Enable writing snapshots after processing commands.
    /// When false, no snapshots are stored (pure event sourcing).
    /// Useful for troubleshooting snapshot persistence issues.
    /// Default: true
    pub write: bool,
}

impl Default for SnapshotsEnableConfig {
    fn default() -> Self {
        Self {
            read: true,
            write: true,
        }
    }
}

#[cfg(test)]
#[path = "config.test.rs"]
mod tests;
