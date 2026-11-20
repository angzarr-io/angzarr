//! The generator a scenario runs: the protos it declares, compiled, then
//! generated from (the descriptors) or linted (the declarations' rules and
//! warnings), and what came of it.

use angzarr_codegen::{
    CodegenError, Descriptor, Warning, adapters, descriptors, markup_domain, warnings,
};
use angzarr_proto::v1::ComponentDescriptor;
use prost::Message as _;

use crate::protos::{Protos, compile, full, package};

/// The version every scenario's build is at.
pub const VERSION: &str = "1.4.2+build.7";

#[derive(Debug, Default, cucumber::World)]
pub struct Generator {
    pub protos: Protos,
    /// Each component by the name a scenario gives it, its anchor's full name.
    pub names: std::collections::BTreeMap<String, String>,
    /// What linting or generating came to.
    pub outcome: Option<Result<Generated, CodegenError>>,
    /// The build's version, if the scenario names one.
    pub version: Option<String>,
    /// The component declared last, by the name the scenario gave it.
    pub last_component: Option<String>,
    /// The input declared last, by its full name.
    pub last_input: Option<String>,
    /// What linting the protos warned of.
    pub warnings: Vec<Warning>,
    /// The refusal a scenario had its handler refuse with, by full name.
    pub last_refusal: Option<String>,
    /// The Rust handler interfaces generated of the protos.
    pub layers: Option<Result<Layers, CodegenError>>,
}

/// The Rust layers a scenario's protos generate: the domain layer of every
/// package (the handler interfaces, one trait per component), the adapters
/// (each handler's attribute and signature).
#[derive(Debug, Default)]
pub struct Layers {
    pub domain: String,
    pub adapters: String,
}

/// What generation made.
#[derive(Debug, Default)]
pub struct Generated {
    pub descriptors: Vec<Descriptor>,
}

/// `order-to-inventory` as `OrderToInventory`, the anchor message a
/// scenario's component name stands for; a name that is a message's
/// already is one.
pub fn anchor_of(name: &str) -> String {
    name.split('-')
        .map(|word| {
            let mut chars = word.chars();
            chars
                .next()
                .map(|c| c.to_ascii_uppercase().to_string() + chars.as_str())
                .unwrap_or_default()
        })
        .collect()
}

/// `OrderState` as `order-state`, `order-to-inventory` as itself: the
/// `component_name` a scenario's name stands for.
pub fn component_name_of(name: &str) -> String {
    let mut out = String::new();
    for (i, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                out.push('-');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

impl Generator {
    /// A component `name` of `kind` in the package of `domain`, declared
    /// by its anchor message, named as the scenario's name stands for.
    pub fn component(
        &mut self,
        name: &str,
        kind: &str,
        domain: &str,
    ) -> &mut crate::protos::Component {
        let full = format!("{}.{}", package(domain), anchor_of(name));
        self.declare(name, &full, kind)
    }

    /// A component `name` of `kind` anchored on the message `full`.
    pub fn declare(&mut self, name: &str, full: &str, kind: &str) -> &mut crate::protos::Component {
        self.names.insert(name.to_string(), full.to_string());
        self.last_component = Some(name.to_string());
        let component_name = component_name_of(name);
        let message = self.protos.message(full);
        message
            .component
            .get_or_insert_with(|| crate::protos::Component {
                kind: kind.to_string(),
                component_name: Some(component_name),
                ..Default::default()
            })
    }

    /// The full name of the component a scenario names.
    pub fn named(&self, name: &str) -> String {
        self.names.get(name).cloned().unwrap_or_else(|| full(name))
    }

    /// The input `name` (`order.ShipOrder`) for the component `component`.
    pub fn input(&mut self, name: &str, component: &str) -> &mut crate::protos::Input {
        let component = self.named(component);
        self.last_input = Some(full(name));
        let message = self.protos.message(&full(name));
        message.input.get_or_insert_with(|| crate::protos::Input {
            component,
            ..Default::default()
        })
    }

    /// The event `name` (`order.OrderPlaced`) consumed by `component`.
    /// Its owner is the aggregate or process manager of its package's
    /// domain, which folds it unless a scenario said otherwise: the
    /// scenarios that name only the consumer leave the folding implied.
    pub fn consumed(&mut self, event: &str, component: &str) {
        let event = full(event);
        let consumer = self.named(component);
        let folded = self.protos.messages.get(&event).is_some_and(|m| {
            m.events.iter().any(|(named, _)| {
                self.protos
                    .messages
                    .get(named)
                    .and_then(|c| c.component.as_ref())
                    .is_some_and(|c| c.kind == "AGGREGATE" || c.kind == "PROCESS_MANAGER")
            })
        });
        let event_package = event.rsplit_once('.').map(|(p, _)| p.to_string());
        let owner = self.protos.messages.iter().find_map(|(name, m)| {
            let c = m.component.as_ref()?;
            let owns = matches!(c.kind.as_str(), "AGGREGATE" | "PROCESS_MANAGER")
                && c.domain.as_deref().map(package) == event_package;
            owns.then(|| name.clone())
        });
        let message = self.protos.message(&event);
        if !folded && let Some(owner) = owner {
            message.events.push((owner, None));
        }
        message.events.push((consumer, None));
    }

    /// The protos compiled and generated from: the descriptors.
    pub fn generate(&mut self) {
        let files = self.protos.files();
        let set = compile(&files);
        let names: Vec<&str> = files.keys().map(String::as_str).collect();
        let version = self.version.clone().unwrap_or_else(|| VERSION.to_string());
        self.outcome =
            Some(descriptors(&set, &names, &version).map(|descriptors| Generated { descriptors }));
    }

    /// The protos compiled and linted as `angzarr lint app` lints them: the
    /// declarations' rules, an error refusing them, and their warnings
    /// ([`warnings`]).
    pub fn lint_protos(&mut self) {
        let files = self.protos.files();
        let set = compile(&files);
        // Linted: the scenario's files and the framework's it imports.
        let names: Vec<&str> = files
            .keys()
            .map(String::as_str)
            .chain(self.protos.imports.iter().map(String::as_str))
            .collect();
        let version = self.version.clone().unwrap_or_else(|| VERSION.to_string());
        let outcome = warnings(&set, &names).map(|found| {
            self.warnings = found;
            // The descriptors the build would make of them, none where
            // generation refuses what a lint only warns of.
            Generated {
                descriptors: descriptors(&set, &names, &version).unwrap_or_default(),
            }
        });
        self.outcome = Some(outcome);
    }

    /// The handler interfaces generated of the protos, as `angzarr-cli`
    /// scaffolds them.
    pub fn generate_interfaces(&mut self) {
        let files = self.protos.files();
        let set = compile(&files);
        let names: Vec<&str> = files.keys().map(String::as_str).collect();
        let packages: Vec<String> = files
            .keys()
            .map(|f| f.trim_end_matches(".proto").to_string())
            .collect();
        let wire: Vec<(&str, &str)> = packages
            .iter()
            .map(|p| (p.as_str(), "crate::wire"))
            .collect();
        let made = || -> Result<Layers, CodegenError> {
            let domain = markup_domain(&set, &names, &wire)?
                .into_iter()
                .map(|g| g.source)
                .collect::<Vec<_>>()
                .join("\n");
            Ok(Layers {
                domain,
                adapters: adapters(&set, &names, &[])?,
            })
        };
        self.layers = Some(made());
    }

    /// The interfaces generated, generating them if no step did; panics if
    /// generation refused.
    pub fn interfaces(&mut self) -> &Layers {
        if self.layers.is_none() {
            self.generate_interfaces();
        }
        match self.layers.as_ref().expect("generated") {
            Ok(layers) => layers,
            Err(error) => panic!("generation refused: {error:?}"),
        }
    }

    /// What generation made; panics if it refused.
    pub fn generated(&self) -> &Generated {
        match self.outcome.as_ref().expect("generated") {
            Ok(generated) => generated,
            Err(error) => panic!("generation refused: {error:?}"),
        }
    }

    /// Why generation refused; panics if it did not.
    pub fn refused(&self) -> &CodegenError {
        match self.outcome.as_ref().expect("linted") {
            Err(error) => error,
            Ok(_) => panic!("generation did not refuse"),
        }
    }

    /// The descriptor of the component the scenario declared last, decoded.
    pub fn descriptor(&self) -> ComponentDescriptor {
        let subject = self.named(&self.last_component.clone().expect("a component"));
        let all = &self.generated().descriptors;
        let found = all
            .iter()
            .find(|d| d.component == subject)
            .unwrap_or_else(|| panic!("no descriptor of {subject} in {all:?}"));
        ComponentDescriptor::decode(found.encoded.as_slice()).expect("an encoded descriptor")
    }
}
