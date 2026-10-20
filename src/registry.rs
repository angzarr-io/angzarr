//! The component registration: the descriptor a coordinator holds from its
//! business process (types.proto, `ComponentDescriptor`; component.proto).
//! One coordinator serves one component, so it holds at most one.
//!
//! A registration records the binary's full build version
//! (`rules_version`) and, for aggregates and process managers, its state
//! descriptor (the state's type URL and the conflict resolutions it
//! supports). The coordinator reads them to choose snapshots, to refuse a
//! response reporting another version, and to resolve a command's conflict
//! resolution.
//!
//! The coordinator fills it from its business process's
//! `ComponentService.Describe` answers ([`crate::registration`]).

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

/// The registration a coordinator holds: one coordinator serves one
/// component of one business process, so there is at most one.
#[derive(Debug, Default)]
pub struct ComponentRegistry {
    current: RwLock<Option<Registration>>,
}

impl ComponentRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Hold `descriptor`, replacing any earlier one. A versionless
    /// descriptor is refused and the earlier one kept.
    pub fn register(&self, descriptor: ComponentDescriptor) -> Result<(), Status> {
        let registration = Registration::new(descriptor)?;
        *self.current.write().expect("registry lock poisoned") = Some(registration);
        Ok(())
    }

    /// Hold nothing.
    pub fn deregister(&self) {
        *self.current.write().expect("registry lock poisoned") = None;
    }

    /// The registration held, if any.
    pub fn get(&self) -> Option<Registration> {
        self.current.read().expect("registry lock poisoned").clone()
    }
}

#[cfg(test)]
#[path = "registry.test.rs"]
mod tests;
