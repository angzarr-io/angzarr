//! What the descriptor set declares, as the emitters need it: components,
//! their inputs (commands and facts) and events, and every message and enum
//! they reach. Every refusal of the protos happens here, so an emitter only
//! writes; the scaffold's own checks (the user's markup) are the scaffold's.

use std::collections::{BTreeMap, BTreeSet};

use prost_reflect::{
    DescriptorPool, DynamicMessage, EnumDescriptor, ExtensionDescriptor, Kind, MessageDescriptor,
    Value,
};

mod resolve;

use crate::error::{CodegenError, MarkupError, ProtoError, UseError};
use crate::warnings::Warning;

const COMPONENT: &str = "io.angzarr.v1.component";
const COMMAND: &str = "io.angzarr.v1.command";
const FACT: &str = "io.angzarr.v1.fact";
const EVENT: &str = "io.angzarr.v1.event";
const REFUSAL_STATUS: &str = "io.angzarr.v1.refusal_status";
/// The well-known types the domain layer maps, each as its own type on
/// both sides: angzarr-embedded's `Timestamp`, and prost's `Duration`.
pub(crate) const TIMESTAMP: &str = "google.protobuf.Timestamp";
pub(crate) const DURATION: &str = "google.protobuf.Duration";
/// angzarr's `UUID`, angzarr-proto's own type on both sides.
pub(crate) const UUID: &str = "io.angzarr.v1.UUID";
const WELL_KNOWN: &str = "google.protobuf";

/// A component kind a generator emits.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ComponentKind {
    Aggregate,
    Policy,
    ProcessManager,
    /// Only the descriptor's: the markup is the write side.
    Projector,
}

impl ComponentKind {
    /// The kind a `ComponentOptions` declares, by the arm of its `kind`
    /// oneof that is set, with that arm's declaration (decision 14.78);
    /// `None` when no arm is.
    fn declared(options: &DynamicMessage) -> Option<(ComponentKind, DynamicMessage)> {
        const ARMS: [(&str, ComponentKind); 4] = [
            ("aggregate", ComponentKind::Aggregate),
            ("process_manager", ComponentKind::ProcessManager),
            ("policy", ComponentKind::Policy),
            ("projector", ComponentKind::Projector),
        ];
        ARMS.into_iter().find_map(|(arm, kind)| {
            if !options.has_field_by_name(arm) {
                return None;
            }
            match options.get_field_by_name(arm).as_deref() {
                Some(Value::Message(declaration)) => Some((kind, declaration.clone())),
                _ => None,
            }
        })
    }

    /// The kind its name's default reads.
    pub(crate) fn name_kind(self) -> crate::names::NameKind {
        use crate::names::NameKind;
        match self {
            ComponentKind::Aggregate => NameKind::Aggregate,
            ComponentKind::Policy => NameKind::Policy,
            ComponentKind::ProcessManager => NameKind::ProcessManager,
            ComponentKind::Projector => NameKind::Projector,
        }
    }

    /// An aggregate or a process manager: a domain, owning a stream domain,
    /// deciding its inputs and folding its events.
    pub(crate) fn is_domain(self) -> bool {
        matches!(
            self,
            ComponentKind::Aggregate | ComponentKind::ProcessManager
        )
    }
}

/// The write side's kinds: what owns a policy's domains.
pub(crate) const WRITE_KINDS: &[ComponentKind] = &[
    ComponentKind::Aggregate,
    ComponentKind::Policy,
    ComponentKind::ProcessManager,
];
/// Every kind: what the Rust markup (scaffold, adapters, register, domain
/// layer), the Python scaffold and the descriptors are generated for.
pub(crate) const ALL_KINDS: &[ComponentKind] = &[
    ComponentKind::Aggregate,
    ComponentKind::Policy,
    ComponentKind::ProcessManager,
    ComponentKind::Projector,
];

/// One input of an aggregate or a process manager: a command (`(command)`),
/// or a fact (`(fact)`, decision 14.91).
pub(crate) struct Input {
    pub message: MessageDescriptor,
    pub emits: Vec<MessageDescriptor>,
    /// Its typed refusals; always empty for a fact.
    pub refuses: Vec<MessageDescriptor>,
    /// Whether its component lists it among its `starting_types`
    /// (derived: the declaration is the component's, not the input's).
    pub starts: bool,
    pub fact: bool,
}

impl Input {
    /// The events a handling of this input, of `component`, returns a list
    /// of (decision 14.75): its emits, or, when it lists none, any of the
    /// component's events.
    pub(crate) fn decided<'a>(&'a self, component: &'a Component) -> &'a [MessageDescriptor] {
        if self.emits.is_empty() {
            &component.events
        } else {
            &self.emits
        }
    }
}

/// One component: its anchor message and what it handles.
pub(crate) struct Component {
    pub anchor: MessageDescriptor,
    pub kind: ComponentKind,
    /// The generated names' stem: the anchor message's name.
    pub name: String,
    /// The name of its interface, the domain layer's trait:
    /// `<name>Interface`, every kind alike, so an aggregate's or a process
    /// manager's anchor, its state, is a domain type of the same name.
    pub interface: String,
    /// Its one required name in an application
    /// (`ComponentOptions.component_name`, decision 14.87).
    pub component_name: String,
    /// The stream domain it owns (an aggregate's or a process manager's).
    pub domain: String,
    /// A policy's source domain, derived from the events it names.
    pub input_domain: String,
    /// A policy's target domain.
    pub output_domain: String,
    /// A process manager's `starting_types`: the full names of the inputs
    /// that may start an instance, as it declares them.
    pub starting_types: Vec<String>,
    pub inputs: Vec<Input>,
    /// An aggregate's or a process manager's events (folded), or the events
    /// a policy or projector reacts to.
    pub events: Vec<MessageDescriptor>,
    /// A policy's or projector's source domains, each once, in the order
    /// of the events it names: derived from their owners (decision 14.78).
    /// Empty for an aggregate or a process manager, which subscribes to
    /// nothing.
    pub sources: Vec<String>,
}

impl Component {
    /// A policy's target: the aggregate or process manager of `components`
    /// owning its output domain, refused when none does.
    pub fn target<'a>(&self, components: &'a [Component]) -> Result<&'a Component, MarkupError> {
        owner(components, &self.output_domain).ok_or_else(|| MarkupError::UnownedDomain {
            policy: self.anchor.full_name().to_string(),
            domain: self.output_domain.clone(),
        })
    }

    /// A policy both of whose domains an aggregate or process manager of
    /// `components` owns: one an application of them runs.
    pub fn is_connected(&self, components: &[Component]) -> bool {
        owner(components, &self.input_domain).is_some()
            && owner(components, &self.output_domain).is_some()
    }

    /// The input whose message is `full_name`.
    pub fn input(&self, full_name: &str) -> Option<&Input> {
        self.inputs
            .iter()
            .find(|i| i.message.full_name() == full_name)
    }
}

/// The aggregate or process manager of `components` owning `domain`.
pub(crate) fn owner<'a>(components: &'a [Component], domain: &str) -> Option<&'a Component> {
    components
        .iter()
        .find(|c| c.kind.is_domain() && c.domain == domain)
}

/// Everything generated into one package.
#[derive(Default)]
pub(crate) struct Package {
    pub components: Vec<Component>,
    /// Domain types, in the order they were reached.
    pub messages: Vec<MessageDescriptor>,
    pub enums: Vec<EnumDescriptor>,
    /// Each component's interface the package's layer declares, as source.
    pub interfaces: Vec<String>,
}

impl Package {
    /// Every name the package's layer declares a type under, with what
    /// takes it: each message and enum (its full name), each oneof's
    /// choice (the oneof's full name) and each id newtype (the first field
    /// declaring it), in that order.
    pub(crate) fn names(&self) -> Vec<(String, String)> {
        let mut names: Vec<(String, String)> = self
            .messages
            .iter()
            .map(|m| (m.full_name().to_string(), crate::common::message_name(m)))
            .chain(
                self.enums
                    .iter()
                    .map(|e| (e.full_name().to_string(), crate::common::enum_name(e))),
            )
            .collect();
        for message in &self.messages {
            for oneof in message.oneofs().filter(|o| !o.is_synthetic()) {
                names.push((
                    oneof.full_name().to_string(),
                    crate::common::choice_name(&oneof),
                ));
            }
        }
        let mut ids: Vec<String> = Vec::new();
        for message in &self.messages {
            for field in message.fields() {
                if let Some(id) = crate::options::id_type(&field)
                    && !ids.contains(&id)
                {
                    names.push((field.full_name().to_string(), id.clone()));
                    ids.push(id);
                }
            }
        }
        names
    }
}

impl Package {
    /// Each type of another package a message of this one holds, as the
    /// holding message's full name and the other package, in order.
    pub(crate) fn foreign(&self) -> Vec<(String, String)> {
        let mut foreign = Vec::new();
        for message in &self.messages {
            for field in message.fields() {
                let kind = match field.kind() {
                    Kind::Message(entry) if entry.is_map_entry() => {
                        entry.map_entry_value_field().kind()
                    }
                    kind => kind,
                };
                let package = match kind {
                    Kind::Message(m) if mapped_as_is(&m) => continue,
                    Kind::Message(m) => m.package_name().to_string(),
                    Kind::Enum(e) => e.package_name().to_string(),
                    _ => continue,
                };
                if package != message.package_name() {
                    foreign.push((message.full_name().to_string(), package));
                }
            }
        }
        foreign
    }
}

/// A message the domain layer holds as its own type on both sides
/// (`Timestamp`, `Duration`, angzarr's `UUID`): no domain type of its own.
pub(crate) fn mapped_as_is(m: &MessageDescriptor) -> bool {
    [TIMESTAMP, DURATION, UUID].contains(&m.full_name())
}

/// The first name of `names` (each what takes it and the name) taken
/// twice, refused, naming both.
fn refuse_taken(names: &[(String, String)]) -> Result<(), ProtoError> {
    for (i, (second, name)) in names.iter().enumerate() {
        if let Some((first, _)) = names[..i].iter().find(|(_, n)| n == name) {
            return Err(ProtoError::NameTaken {
                name: name.clone(),
                first: first.clone(),
                second: second.clone(),
            });
        }
    }
    Ok(())
}

/// The markup domain layer's packages by name: every type the anchor,
/// inputs, events and refusals of each aggregate and process manager reach
/// (its anchor is its state), and the events each projector projects, each
/// in its own package's layer (a package of types alone has one); a policy
/// holds no domain type.
pub(crate) fn build_markup(
    descriptor_set: &[u8],
    files: &[&str],
) -> Result<BTreeMap<String, Package>, CodegenError> {
    let components = components(descriptor_set, files, ALL_KINDS)?
        .into_iter()
        .filter(|c| c.kind != ComponentKind::Policy)
        .collect();
    let packages = packages(components, |c| {
        c.kind
            .is_domain()
            .then(|| c.anchor.clone())
            .into_iter()
            .chain(
                c.inputs
                    .iter()
                    .flat_map(|i| std::iter::once(&i.message).chain(&i.refuses))
                    .chain(&c.events)
                    .cloned(),
            )
            .collect()
    })?;
    for package in packages.values() {
        let names = package.names();
        refuse_taken(&names)?;
        let names = names
            .iter()
            .map(|(full, local)| (full.as_str(), local.clone()));
        refuse_reserved(names.clone())?;
        // A command refusing several types refuses with a choice the
        // domain layer names after it: no domain type may take that name.
        for component in &package.components {
            for input in component.inputs.iter().filter(|i| i.refuses.len() > 1) {
                let choice = crate::common::refusal_choice_name(&input.message);
                if let Some((full, _)) = names.clone().find(|(_, local)| *local == choice) {
                    return Err(ProtoError::RefusalChoiceName {
                        message: full.to_string(),
                        name: choice,
                        command: input.message.full_name().to_string(),
                    }
                    .into());
                }
            }
        }
    }
    Ok(packages)
}

/// The names the generated domain layers give the runtime's types, which no
/// domain type may take: a handler's `Context`, a policy reaction's
/// `PolicyContext`, a refused command's `CommandContext`, a projector's
/// `ProjectionContext`, the two classes
/// a handler fails with, the defect `Bug`, the typed `Refusal` and the refused answer
/// `Refused`, and Rust's `Failed` of a refusal or of `Unrefusable`, with its `BusinessStatus` and its
/// `RetryableStatus`.
pub(crate) const RESERVED: &[&str] = &[
    "Context",
    "PolicyContext",
    "CommandContext",
    "ProjectionContext",
    "Refused",
    "Refusal",
    "Unrefusable",
    "Bug",
    "Failed",
    "BusinessStatus",
    "RetryableStatus",
];

/// The first of `names` (each a type's full name and the name the domain
/// layer gives it) that is [`RESERVED`], refused: the generated code would
/// collide, and nothing is renamed silently.
pub(crate) fn refuse_reserved<'a>(
    names: impl IntoIterator<Item = (&'a str, String)>,
) -> Result<(), ProtoError> {
    for (full, local) in names {
        if RESERVED.contains(&local.as_str()) {
            return Err(ProtoError::ReservedName {
                message: full.to_string(),
                name: local,
            });
        }
    }
    Ok(())
}

/// `descriptor_set` decoded, refused when it is not a descriptor set.
pub(crate) fn decode(descriptor_set: &[u8]) -> Result<DescriptorPool, ProtoError> {
    DescriptorPool::decode(descriptor_set).map_err(|e| ProtoError::Descriptors {
        detail: e.to_string(),
    })
}

/// Every marked-up component of every file in the descriptor set, so a
/// scaffold finds the owners of a policy's domains declared outside the
/// files it scaffolds.
pub(crate) fn pool_components(descriptor_set: &[u8]) -> Result<Vec<Component>, CodegenError> {
    let pool = decode(descriptor_set)?;
    let names: Vec<String> = pool.files().map(|f| f.name().to_string()).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    components(descriptor_set, &names, WRITE_KINDS)
}

/// `components` grouped by their anchor's package, with every type `roots`
/// reaches in its own package.
fn packages(
    components: Vec<Component>,
    roots: impl Fn(&Component) -> Vec<MessageDescriptor>,
) -> Result<BTreeMap<String, Package>, CodegenError> {
    let mut packages: BTreeMap<String, Package> = BTreeMap::new();
    for component in components {
        let mut reach = Reach::default();
        for root in roots(&component) {
            reach.message(&root)?;
        }
        for message in reach.messages {
            let package = packages
                .entry(message.package_name().to_string())
                .or_default();
            if !package.messages.contains(&message) {
                package.messages.push(message);
            }
        }
        for e in reach.enums {
            let package = packages.entry(e.package_name().to_string()).or_default();
            if !package.enums.contains(&e) {
                package.enums.push(e);
            }
        }
        packages
            .entry(component.anchor.package_name().to_string())
            .or_default()
            .components
            .push(component);
    }
    Ok(packages)
}

/// Every component declared in `files`, in declaration order, refusing a
/// kind outside `kinds`, and a declaration the resolver refuses.
pub(crate) fn components(
    descriptor_set: &[u8],
    files: &[&str],
    kinds: &[ComponentKind],
) -> Result<Vec<Component>, CodegenError> {
    declare(descriptor_set, files, kinds, None)
}

/// [`components`] as a lint reads them: every rule of the declarations
/// held as generation holds it, but a command's `emits` naming an event its
/// component folds none of, which generation refuses
/// ([`MarkupError::UnknownEmit`]) and a lint warns of
/// ([`Warning::UnappliedEmit`]), that emit left out; and the consumers
/// skipped as outside the generation, warned of.
pub(crate) fn declarations(
    descriptor_set: &[u8],
    files: &[&str],
) -> Result<(Vec<Component>, Vec<Warning>), CodegenError> {
    let mut warnings = Vec::new();
    let components = declare(descriptor_set, files, ALL_KINDS, Some(&mut warnings))?;
    Ok((components, warnings))
}

/// The components declared in `files`, an unapplied emit refused, or kept
/// in `warnings` when it is given (with the consumers skipped).
fn declare(
    descriptor_set: &[u8],
    files: &[&str],
    kinds: &[ComponentKind],
    mut warnings: Option<&mut Vec<Warning>>,
) -> Result<Vec<Component>, CodegenError> {
    let pool = decode(descriptor_set)?;
    let mut messages = Vec::new();
    for file in files {
        let found = pool
            .get_file_by_name(file)
            .ok_or_else(|| UseError::FileNotInSet {
                file: file.to_string(),
            })?;
        messages.extend(found.messages());
    }
    // A component's inputs and events are wherever the set declares them:
    // the files' messages first, then every other file's.
    let mut declaring = messages.clone();
    for file in pool.files() {
        for message in file.messages() {
            if !declaring.contains(&message) {
                declaring.push(message);
            }
        }
    }
    // A set that never imported options.proto declares no component.
    let Some(markup) = Markup::of(&pool) else {
        return Ok(Vec::new());
    };
    let mut declared = Vec::new();
    for message in &messages {
        declared.extend(markup.declared(message, kinds)?);
    }
    resolve::one_option(&markup, &declaring)?;
    // Events first, so a command's `emits` can be checked against them.
    for message in &declaring {
        for consumer in markup.consumers(message) {
            let skipped = attach_event(&mut declared, message, &consumer);
            if let (Some(skipped), Some(warnings)) = (skipped, warnings.as_deref_mut()) {
                warnings.push(Warning::ConsumerSkipped {
                    consumer: skipped,
                    event: message.full_name().to_string(),
                });
            }
        }
    }
    for message in &declaring {
        if let Some((input, fact)) = markup.input(message) {
            markup.attach_input(
                &mut declared,
                message,
                &input,
                fact,
                warnings.as_deref_mut(),
            )?;
        }
    }
    let owners = markup.owners(&declaring);
    for component in &mut declared {
        component.derive_sources(&owners);
    }
    resolve::check(&markup, &declaring, &messages, &declared)?;
    Ok(declared.into_iter().map(|d| d.component).collect())
}

/// angzarr's declaration options as a descriptor set has them:
/// `(component)`, and `(command)`, `(fact)` and `(event)` where its
/// options.proto declares them.
struct Markup<'a> {
    pool: &'a DescriptorPool,
    component: ExtensionDescriptor,
    command: Option<ExtensionDescriptor>,
    fact: Option<ExtensionDescriptor>,
    event: Option<ExtensionDescriptor>,
    /// `(refusal_status)`, where the set's options.proto has one.
    refusal_status: Option<ExtensionDescriptor>,
}

impl<'a> Markup<'a> {
    /// `None` for a set that never imported options.proto.
    fn of(pool: &'a DescriptorPool) -> Option<Markup<'a>> {
        Some(Markup {
            pool,
            component: pool.get_extension_by_name(COMPONENT)?,
            command: pool.get_extension_by_name(COMMAND),
            fact: pool.get_extension_by_name(FACT),
            event: pool.get_extension_by_name(EVENT),
            refusal_status: pool.get_extension_by_name(REFUSAL_STATUS),
        })
    }

    /// The `(refusal_status)` `message` declares, as its `RefusalStatus`
    /// number (0 for `REFUSAL_STATUS_UNSPECIFIED`); `None` when it declares
    /// none.
    fn status(&self, message: &MessageDescriptor) -> Option<i32> {
        let ext = self.refusal_status.as_ref()?;
        let options = message.options();
        options
            .has_extension(ext)
            .then(|| options.get_extension(ext).as_enum_number().unwrap_or(0))
    }

    /// The `(component)` `message` declares.
    fn component(&self, message: &MessageDescriptor) -> Option<DynamicMessage> {
        values(message, Some(&self.component)).pop()
    }

    /// The `(command)` or `(fact)` `message` declares, and whether it is
    /// the fact: it is an input. A message declaring both is a command
    /// here; refusing it is the resolver's.
    fn input(&self, message: &MessageDescriptor) -> Option<(DynamicMessage, bool)> {
        values(message, self.command.as_ref())
            .pop()
            .map(|command| (command, false))
            .or_else(|| {
                values(message, self.fact.as_ref())
                    .pop()
                    .map(|fact| (fact, true))
            })
    }

    /// The `(event)` entries of `message`: each a component consuming it.
    fn consumers(&self, message: &MessageDescriptor) -> Vec<DynamicMessage> {
        values(message, self.event.as_ref())
    }

    /// The domain of every aggregate and process manager of the set by its
    /// anchor's full name.
    fn domains(&self) -> BTreeMap<String, String> {
        let mut domains = BTreeMap::new();
        for file in self.pool.files() {
            for message in file.messages() {
                let Some(options) = self.component(&message) else {
                    continue;
                };
                if let Some((kind, declaration)) = ComponentKind::declared(&options)
                    && kind.is_domain()
                {
                    domains.insert(
                        message.full_name().to_string(),
                        string_field(&declaration, "domain"),
                    );
                }
            }
        }
        domains
    }

    /// The domain of the owner of each event of `messages` the set has one
    /// for, by the event's full name: the one aggregate or process manager
    /// with an `(event)` entry folding it (decision 14.78).
    fn owners(&self, messages: &[MessageDescriptor]) -> BTreeMap<String, String> {
        let domains = self.domains();
        let mut owners = BTreeMap::new();
        for message in messages {
            for consumer in self.consumers(message) {
                if let Some(domain) = domains.get(&string_field(&consumer, "component")) {
                    owners
                        .entry(message.full_name().to_string())
                        .or_insert_with(|| domain.clone());
                }
            }
        }
        owners
    }

    /// The component `message` declares, refusing a kind outside `kinds`;
    /// `None` when it declares none, or is a projector and `kinds` takes
    /// none.
    fn declared(
        &self,
        message: &MessageDescriptor,
        kinds: &[ComponentKind],
    ) -> Result<Option<Declared>, CodegenError> {
        let Some(options) = self.component(message) else {
            return Ok(None);
        };
        let full_name = || message.full_name().to_string();
        let Some((kind, declaration)) = ComponentKind::declared(&options) else {
            // No arm of the kind oneof is set.
            return Err(MarkupError::UnsupportedKind {
                component: full_name(),
                kind: 0,
            }
            .into());
        };
        // The markup is the write side: a projector beside it is skipped.
        if !kinds.contains(&kind) {
            return Ok(None);
        }
        let output_domain = string_field(&declaration, "output_domain");
        if kind == ComponentKind::Policy && output_domain.is_empty() {
            return Err(MarkupError::NoOutputDomain {
                component: full_name(),
            }
            .into());
        }
        let component_name = string_field(&options, "component_name");
        if !crate::names::well_formed(&component_name) {
            return Err(MarkupError::ComponentName {
                component: full_name(),
                name: component_name,
            }
            .into());
        }
        Ok(Some(Declared {
            entries: Vec::new(),
            component: Component {
                anchor: message.clone(),
                kind,
                name: message.name().to_string(),
                interface: format!("{}Interface", message.name()),
                component_name,
                domain: string_field(&declaration, "domain"),
                input_domain: String::new(),
                output_domain,
                starting_types: strings_field(&declaration, "starting_types"),
                inputs: Vec::new(),
                events: Vec::new(),
                sources: Vec::new(),
            },
        }))
    }

    /// The input the `(command)` or `(fact)` `message` declares
    /// (`declared`) added to the component it names, when that is
    /// generated here; refused when nothing declares it.
    fn attach_input(
        &self,
        components: &mut [Declared],
        message: &MessageDescriptor,
        declared: &DynamicMessage,
        fact: bool,
        unapplied: Option<&mut Vec<Warning>>,
    ) -> Result<(), CodegenError> {
        let named = string_field(declared, "component");
        if let Some(owner) = Declared::named(components, &named) {
            let input = command_input(
                self.pool,
                &owner.component,
                message,
                declared,
                fact,
                unapplied,
            )?;
            owner.component.inputs.push(input);
            return Ok(());
        }
        // Declared, but outside the files: generated elsewhere.
        match self.pool.get_message_by_name(&named) {
            Some(m) if m.options().has_extension(&self.component) => Ok(()),
            _ => Err(MarkupError::UnknownComponent {
                message: message.full_name().to_string(),
                component: named,
            }
            .into()),
        }
    }
}

/// The values `message` sets of the message option `ext`: one for a
/// singular option, each entry of a repeated one; none when it is unset, or
/// the set lacks the option.
fn values(message: &MessageDescriptor, ext: Option<&ExtensionDescriptor>) -> Vec<DynamicMessage> {
    let options = message.options();
    let Some(ext) = ext.filter(|ext| options.has_extension(ext)) else {
        return Vec::new();
    };
    match options.get_extension(ext).into_owned() {
        Value::Message(m) => vec![m],
        Value::List(entries) => entries
            .into_iter()
            .filter_map(|v| match v {
                Value::Message(m) => Some(m),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// A component as it is declared, with the `(event)` entries naming it.
struct Declared {
    component: Component,
    /// Each event it names, with the domain its entry overrides (empty:
    /// none), in the order the set declares them.
    entries: Vec<(MessageDescriptor, String)>,
}

impl Declared {
    /// The one of `components` whose anchor is `full_name`.
    fn named<'d>(components: &'d mut [Declared], full_name: &str) -> Option<&'d mut Declared> {
        components
            .iter_mut()
            .find(|d| d.component.anchor.full_name() == full_name)
    }

    /// `event`, of an `(event)` entry for this component, added to its
    /// events; `domain` is the entry's override.
    fn attach(&mut self, event: &MessageDescriptor, domain: String) {
        if !self.component.events.contains(event) {
            self.component.events.push(event.clone());
        }
        self.entries.push((event.clone(), domain));
    }

    /// A policy's or projector's source domains, derived from the events it
    /// names (decision 14.78): each event's owner's domain, else its
    /// entry's override, each once, in order; a policy's `input_domain` is
    /// the first. A component of another kind subscribes to nothing.
    fn derive_sources(&mut self, owners: &BTreeMap<String, String>) {
        let component = &mut self.component;
        if component.kind.is_domain() {
            return;
        }
        for (event, domain) in &self.entries {
            let source = owners.get(event.full_name()).unwrap_or(domain);
            if !source.is_empty() && !component.sources.contains(source) {
                component.sources.push(source.clone());
            }
        }
        if component.kind == ComponentKind::Policy {
            component.input_domain = component.sources.first().cloned().unwrap_or_default();
        }
    }
}

/// The `(event)` entry `consumer` of `event` attached to the component it
/// names. A consumer outside the set is a reference to a component
/// generated elsewhere (decision 14.50), skipped: its name is returned.
fn attach_event(
    components: &mut [Declared],
    event: &MessageDescriptor,
    consumer: &DynamicMessage,
) -> Option<String> {
    let name = string_field(consumer, "component");
    match Declared::named(components, &name) {
        Some(component) => {
            component.attach(event, string_field(consumer, "domain"));
            None
        }
        None => Some(name),
    }
}

/// The input the `(command)` or `(fact)` `message` declares (`declared`;
/// `fact` when it is the `(fact)`) of `component`: its emits, held to the
/// component's events (one it folds none of refused, or kept in `unapplied`
/// and left out when that is given), and its refusals, each a message of
/// `pool` in the command's own package. A fact declares none.
fn command_input(
    pool: &DescriptorPool,
    component: &Component,
    message: &MessageDescriptor,
    declared: &DynamicMessage,
    fact: bool,
    mut unapplied: Option<&mut Vec<Warning>>,
) -> Result<Input, MarkupError> {
    let command = || message.full_name().to_string();
    let mut emits = Vec::new();
    for event in strings_field(declared, "emits") {
        match component.events.iter().find(|e| e.full_name() == event) {
            Some(found) => emits.push(found.clone()),
            None => match unapplied.as_deref_mut() {
                Some(warnings) => warnings.push(Warning::UnappliedEmit {
                    command: command(),
                    event,
                }),
                None => {
                    return Err(MarkupError::UnknownEmit {
                        command: command(),
                        event,
                    });
                }
            },
        }
    }
    let refuses = strings_field(declared, "refuses")
        .into_iter()
        .map(|refusal| {
            let found =
                pool.get_message_by_name(&refusal)
                    .ok_or_else(|| MarkupError::UnknownRefusal {
                        command: command(),
                        refusal: refusal.clone(),
                    })?;
            if found.package_name() != message.package_name() {
                return Err(MarkupError::ForeignRefusal {
                    command: command(),
                    refusal,
                });
            }
            Ok(found)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Input {
        message: message.clone(),
        emits,
        refuses,
        starts: component
            .starting_types
            .iter()
            .any(|t| t == message.full_name()),
        fact,
    })
}

/// The status a refusal message declares (`(refusal_status)`), its
/// `google.rpc.Code` number: 0 where it declares none, or where the set's
/// options.proto has no `refusal_status`.
pub(crate) fn message_status(message: &MessageDescriptor) -> i32 {
    let Some(ext) = message.parent_pool().get_extension_by_name(REFUSAL_STATUS) else {
        return 0;
    };
    let options = message.options();
    if !options.has_extension(&ext) {
        return 0;
    }
    options.get_extension(&ext).as_enum_number().unwrap_or(0)
}

/// The messages and enums reachable from a component's roots.
#[derive(Default)]
struct Reach {
    seen: BTreeSet<String>,
    messages: Vec<MessageDescriptor>,
    enums: Vec<EnumDescriptor>,
}

impl Reach {
    fn message(&mut self, message: &MessageDescriptor) -> Result<(), ProtoError> {
        if !self.seen.insert(message.full_name().to_string()) {
            return Ok(());
        }
        self.messages.push(message.clone());
        for field in message.fields() {
            let kind = match field.kind() {
                Kind::Message(entry) if entry.is_map_entry() => {
                    entry.map_entry_value_field().kind()
                }
                kind => kind,
            };
            match kind {
                Kind::Message(m) if mapped_as_is(&m) => {}
                Kind::Message(m) if m.package_name() == WELL_KNOWN => {
                    return Err(ProtoError::WellKnownType {
                        message: message.full_name().to_string(),
                        field: field.name().to_string(),
                        type_name: m.full_name().to_string(),
                    });
                }
                Kind::Message(m) => self.message(&m)?,
                Kind::Enum(e) => self.enumeration(&e)?,
                _ => {}
            }
        }
        Ok(())
    }

    fn enumeration(&mut self, e: &EnumDescriptor) -> Result<(), ProtoError> {
        if !self.seen.insert(e.full_name().to_string()) {
            return Ok(());
        }
        if let Some(zero) = e.values().find(|v| v.number() == 0)
            && !zero.name().ends_with("_UNSPECIFIED")
        {
            return Err(ProtoError::EnumZero {
                enum_name: e.full_name().to_string(),
                value: zero.name().to_string(),
            });
        }
        self.enums.push(e.clone());
        Ok(())
    }
}

fn string_field(message: &DynamicMessage, name: &str) -> String {
    match message.get_field_by_name(name).as_deref() {
        Some(Value::String(s)) => s.clone(),
        _ => String::new(),
    }
}

/// A repeated string field's values.
fn strings_field(message: &DynamicMessage, name: &str) -> Vec<String> {
    list_field(message, name)
        .into_iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s),
            _ => None,
        })
        .collect()
}

fn list_field(message: &DynamicMessage, name: &str) -> Vec<Value> {
    match message.get_field_by_name(name).as_deref() {
        Some(Value::List(values)) => values.clone(),
        _ => Vec::new(),
    }
}
