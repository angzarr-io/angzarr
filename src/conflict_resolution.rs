//! Conflict-resolution configuration (backlog HD-2; types.proto,
//! `Handling`): the resolution a command is handled under is, highest
//! first,
//! 1. the command's `Handling.conflict_resolution`, when set and the
//!    configuration allows the override;
//! 2. the configured resolution for the command type;
//! 3. the configured resolution for the component;
//! 4. the built-in default: COMMUTATIVE where the registered state lists
//!    it, else STRICT.
//!
//! Names are checked when the configuration loads; support is checked when
//! the component's descriptor arrives (the state is known only then).

use serde::Deserialize;

use crate::proto::{ComponentDescriptor, ConflictResolution};
use crate::proto_ext::type_url;
use crate::registry::Registration;

/// One command type's entry (`conflict_resolution.commands[i]`).
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct CommandConflictConfig {
    /// The command's type URL (compared by full name).
    #[serde(rename = "type")]
    pub type_url: String,
    /// Its resolution, when configured.
    pub resolution: Option<String>,
    /// Whether Handling may name a resolution for it; default: the
    /// component's.
    pub allow_override: Option<bool>,
}

/// The `conflict_resolution` configuration section.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ConflictConfig {
    /// The component's resolution.
    pub default: Option<String>,
    /// Whether Handling may name a resolution; default true.
    pub allow_override: Option<bool>,
    /// Per command type.
    pub commands: Vec<CommandConflictConfig>,
}

#[derive(Debug, Clone)]
struct CommandPolicy {
    full_name: String,
    resolution: Option<ConflictResolution>,
    allow_override: bool,
}

/// The compiled configuration.
#[derive(Debug, Clone)]
pub struct ConflictPolicy {
    default: Option<ConflictResolution>,
    allow_override: bool,
    commands: Vec<CommandPolicy>,
}

impl Default for ConflictPolicy {
    fn default() -> Self {
        Self {
            default: None,
            allow_override: true,
            commands: Vec::new(),
        }
    }
}

/// A configured name: the enum name without its prefix.
fn parse(key: &str, name: &str) -> Result<ConflictResolution, String> {
    match ConflictResolution::from_str_name(&format!("CONFLICT_RESOLUTION_{name}")) {
        Some(ConflictResolution::Unspecified) | None => Err(format!(
            "{key}: {name:?} is not a conflict resolution (STRICT, AGGREGATE_HANDLES or COMMUTATIVE)"
        )),
        Some(resolution) => Ok(resolution),
    }
}

impl ConflictPolicy {
    /// Compile the configuration, refusing unknown names and duplicate
    /// command types with a message naming the key.
    pub fn from_config(config: &ConflictConfig) -> Result<Self, String> {
        let default = config
            .default
            .as_deref()
            .map(|n| parse("conflict_resolution.default", n))
            .transpose()?;
        let allow_override = config.allow_override.unwrap_or(true);
        let mut commands: Vec<CommandPolicy> = Vec::new();
        for (i, entry) in config.commands.iter().enumerate() {
            if entry.type_url.is_empty() {
                return Err(format!(
                    "conflict_resolution.commands[{i}].type is required"
                ));
            }
            let full_name = type_url::fqn(&entry.type_url).to_string();
            if commands.iter().any(|c| c.full_name == full_name) {
                return Err(format!(
                    "conflict_resolution.commands[{i}].type: {full_name} is listed twice"
                ));
            }
            let resolution = entry
                .resolution
                .as_deref()
                .map(|n| parse(&format!("conflict_resolution.commands[{i}].resolution"), n))
                .transpose()?;
            commands.push(CommandPolicy {
                full_name,
                resolution,
                allow_override: entry.allow_override.unwrap_or(allow_override),
            });
        }
        Ok(Self {
            default,
            allow_override,
            commands,
        })
    }

    fn command(&self, command_type_url: &str) -> Option<&CommandPolicy> {
        let name = type_url::fqn(command_type_url);
        self.commands.iter().find(|c| c.full_name == name)
    }

    /// Whether a command of this type may name its own resolution.
    pub fn allows_override(&self, command_type_url: &str) -> bool {
        self.command(command_type_url)
            .map(|c| c.allow_override)
            .unwrap_or(self.allow_override)
    }

    /// The resolution a command of this type is handled under, given the
    /// one its Handling names (`Unspecified` when none).
    pub fn resolve(
        &self,
        named: ConflictResolution,
        command_type_url: &str,
        registration: &Registration,
    ) -> ConflictResolution {
        if named != ConflictResolution::Unspecified {
            return named;
        }
        if let Some(configured) = self.command(command_type_url).and_then(|c| c.resolution) {
            return configured;
        }
        if let Some(configured) = self.default {
            return configured;
        }
        if registration.supports(ConflictResolution::Commutative) {
            ConflictResolution::Commutative
        } else {
            ConflictResolution::Strict
        }
    }

    /// Refuse a descriptor whose state does not support every configured
    /// resolution.
    pub fn check_supported(&self, descriptor: &ComponentDescriptor) -> Result<(), String> {
        let supported = |r: ConflictResolution| {
            descriptor
                .state
                .as_ref()
                .and_then(|s| s.options.as_ref())
                .is_some_and(|o| o.conflict_resolutions.contains(&(r as i32)))
        };
        let configured = self
            .default
            .map(|r| ("conflict_resolution.default".to_string(), r))
            .into_iter()
            .chain(self.commands.iter().filter_map(|c| {
                c.resolution
                    .map(|r| (format!("conflict_resolution for {}", c.full_name), r))
            }));
        for (key, resolution) in configured {
            if !supported(resolution) {
                return Err(format!(
                    "{key} is {}, which the state of component '{}' does not support",
                    resolution
                        .as_str_name()
                        .trim_start_matches("CONFLICT_RESOLUTION_"),
                    descriptor.name
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "conflict_resolution.test.rs"]
mod tests;
