//! Component registrations: the descriptors a coordinator holds from its
//! business process (types.proto, `ComponentDescriptor`; component.proto).
//!
//! A registration records the binary's full build version
//! (`rules_version`) and, for aggregates and process managers, its state
//! descriptor (the state's type URL and the conflict resolutions it
//! supports). The coordinator reads them to choose snapshots, to refuse a
//! response reporting another version, and to resolve a command's conflict
//! resolution.
//!
//! Registrations are kept per domain and per instance: during a rolling
//! deployment several instances of one domain's component may be
//! registered, with different versions (decision 14.9). An aggregate's
//! descriptor names no domain, so the coordinator registers it under the
//! domain it serves.
//!
//! The coordinator fills it from its business process's
//! `ComponentService.Describe` answers ([`crate::registration`]).

use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

use tonic::Status;

use crate::proto::{ComponentDescriptor, ConflictResolution, StateDescriptor};

/// One accepted registration.
#[derive(Debug, Clone, PartialEq)]
pub struct Registration {
    descriptor: ComponentDescriptor,
}

impl Registration {
    /// Accept a descriptor. One without a rules version is refused
    /// INVALID_ARGUMENT: the registry never holds a versionless component
    /// (types.proto, `ComponentDescriptor.rules_version`).
    pub fn new(descriptor: ComponentDescriptor) -> Result<Self, Status> {
        if descriptor.rules_version.is_empty() {
            return Err(Status::invalid_argument(format!(
                "component '{}' has no rules_version",
                descriptor.name
            )));
        }
        Ok(Self { descriptor })
    }

    /// The registered component name.
    pub fn name(&self) -> &str {
        &self.descriptor.name
    }

    /// The business binary's full build version.
    pub fn rules_version(&self) -> &str {
        &self.descriptor.rules_version
    }

    /// The registered state, for aggregates and process managers.
    pub fn state(&self) -> Option<&StateDescriptor> {
        self.descriptor.state.as_ref()
    }

    /// Whether the registered state supports `resolution`.
    pub fn supports(&self, resolution: ConflictResolution) -> bool {
        self.state().is_some_and(|s| {
            s.options
                .as_ref()
                .is_some_and(|o| o.conflict_resolutions.contains(&(resolution as i32)))
        })
    }

    /// The descriptor as registered.
    pub fn descriptor(&self) -> &ComponentDescriptor {
        &self.descriptor
    }
}

/// The registrations a coordinator holds, by domain and instance.
#[derive(Debug, Default)]
pub struct ComponentRegistry {
    by_domain: RwLock<HashMap<String, BTreeMap<String, Registration>>>,
}

impl ComponentRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `instance` of `domain`'s component with `descriptor`,
    /// replacing any earlier registration of that instance.
    pub fn register(
        &self,
        domain: &str,
        instance: &str,
        descriptor: ComponentDescriptor,
    ) -> Result<(), Status> {
        let registration = Registration::new(descriptor)?;
        self.by_domain
            .write()
            .expect("registry lock poisoned")
            .entry(domain.to_string())
            .or_default()
            .insert(instance.to_string(), registration);
        Ok(())
    }

    /// Forget `instance` of `domain`'s component.
    pub fn deregister(&self, domain: &str, instance: &str) {
        let mut by_domain = self.by_domain.write().expect("registry lock poisoned");
        if let Some(instances) = by_domain.get_mut(domain) {
            instances.remove(instance);
            if instances.is_empty() {
                by_domain.remove(domain);
            }
        }
    }

    /// The registration of `instance` of `domain`'s component.
    pub fn get(&self, domain: &str, instance: &str) -> Option<Registration> {
        self.by_domain
            .read()
            .expect("registry lock poisoned")
            .get(domain)
            .and_then(|instances| instances.get(instance))
            .cloned()
    }

    /// Every registration of `domain`'s component (empty when none).
    pub fn all(&self, domain: &str) -> Vec<Registration> {
        self.by_domain
            .read()
            .expect("registry lock poisoned")
            .get(domain)
            .map(|instances| instances.values().cloned().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
#[path = "registry.test.rs"]
mod tests;
