//! What a project's protos give an angzarr-embedded application besides the
//! domain types, split so the user never edits a generated file and a run
//! never rewrites the user's (decision item S):
//!
//! - the user's, written once into the source tree by `angzarr scaffold
//!   rust`: per component its implementation (`<component>_impl.rs`, every
//!   body `todo!()` but the `snapshot` and `restore` its configuration
//!   selects ([`ScaffoldConfig`]), which take snapshots of the state it
//!   holds) of the component's interface, importing nothing of
//!   angzarr-embedded directly: their context types, `Failed`,
//!   `BusinessStatus`, `RetryableStatus`, `Compensation` and `Bug` are the
//!   generated domain layer's re-exports and its own
//!   (`Failed::retryable_bug(status, &bug)` and `Failed::immediate(&bug)`
//!   included);
//! - the CLI's, regenerated into `OUT_DIR` every build: each component's
//!   interface, a trait over the domain types, in the domain layer
//!   ([`markup_domain`]), so an implementation in another crate implements
//!   it; the adapters ([`adapters`]), each component's angzarr-embedded
//!   markup delegating to its interface; and [`register`], which registers
//!   every adapter with angzarr-embedded's `Registry`, so an application
//!   calls one function.
//!
//! A change to the protos reaches the user as a compile error on their
//! implementation, never inside the generated layer. Pure, as the rest of
//! the crate: descriptors (and, for [`complete`], the user's current file)
//! in, source out. Reading and writing files is angzarr-build's.

use proc_macro2::TokenStream;
use prost_reflect::MessageDescriptor;
use quote::{format_ident, quote};

use crate::common::{Arm, output_name, refusal_choice_name, snake};
use crate::error::{CodegenError, UseError};
use crate::model::{self, ALL_KINDS, Component, ComponentKind, Input, owner};
use crate::rust::{RUNTIME, ident, root, runtime_macros};
use crate::{Generated, generate};

/// One file of a scaffold.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ScaffoldFile {
    /// The component's anchor message, e.g. `buyin.v1.WalletState`.
    pub component: String,
    /// Where it goes, relative to the crate's `src` (`wallet.rs`) or the
    /// project's root.
    pub path: String,
    /// The file as it is created: a user's implementation holds a stub of
    /// every required method and of the optional ones it is created with.
    pub source: String,
    pub owner: Owner,
    /// A user's implementation's optional methods, by name: the refusal
    /// callbacks its interface offers and never requires. Every other
    /// method of `source` is required.
    pub optional: Vec<String>,
    /// The optional methods selected for it, by name: each is in `source`, and is added to an
    /// existing file that lacks it.
    pub selected: Vec<String>,
}

impl ScaffoldFile {
    /// A file with no optional methods.
    pub(crate) fn new(component: String, path: String, source: String, owner: Owner) -> Self {
        ScaffoldFile {
            component,
            path,
            source,
            owner,
            optional: Vec::new(),
            selected: Vec::new(),
        }
    }
}

/// Who owns a scaffolded file.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Owner {
    /// The user's: written once when absent; an existing one is only ever
    /// completed with the stubs it lacks.
    User,
    /// The CLI's: rewritten on every run, opening with the generated
    /// marker ([`crate::GENERATED_MARKER`]), never edited by hand.
    Generated,
    /// A file an earlier scaffold wrote and this one no longer does (an
    /// interface now generated elsewhere): reported where it is, never
    /// written or deleted.
    Obsolete,
    /// A generated file another tool writes in the same run (buf's message
    /// types): never written here, and never swept as an orphan.
    External,
}

/// A user's existing file against its fresh scaffold ([`complete`]): the
/// file with what is to be added, and what is reported.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Completion {
    /// The file with the stubs added, when any is: `--missing`'s required
    /// methods, and the selected optional ones.
    pub completed: Option<String>,
    pub notes: Notes,
}

/// What a user's existing file lacks, and has beyond, against its fresh
/// scaffold, each method by name: reported, never acted on.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Notes {
    /// Required methods it lacks and that are not added.
    pub lacks: Vec<String>,
    /// Methods for a message the protos no longer declare there.
    pub obsolete: Vec<String>,
    /// Optional methods its interface offers that it does not define.
    pub optional: Vec<String>,
    /// A manifest's pins that are not this CLI's.
    pub drift: Vec<PinDrift>,
}

/// A dependency a user's manifest pins at another revision than this CLI's
/// scaffold would: reported, the manifest never edited (item S Q8).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PinDrift {
    /// The dependency, as the manifest names it (`angzarr-client`).
    pub dependency: String,
    /// The revision the manifest pins.
    pub pinned: String,
    /// The revision this CLI pins.
    pub cli: String,
}

/// The scaffold of every component declared in `files`: its
/// implementation (`<component>_impl.rs`), the user's, of its generated
/// interface; and the interface an earlier scaffold wrote beside it
/// (`<component>.rs`), now the domain layer's, [`Owner::Obsolete`]. Every
/// method is required, a policy's refusal callback too (decisions 14.89, 14.99),
/// so each is stubbed.
///
/// A policy is scaffolded when an aggregate or a process manager of the
/// descriptor set owns each of its `input_domain` and `output_domain`.
pub fn scaffold(descriptor_set: &[u8], files: &[&str]) -> Result<Vec<ScaffoldFile>, CodegenError> {
    scaffold_with(descriptor_set, files, &[], &[], &ScaffoldConfig::default())
}

/// What the scaffold writes beyond each component's required methods, per
/// component: the host config's (`angzarr.yaml`) `component` entries'
/// `scaffold` ([`host_scaffold`](crate::host_scaffold)).
///
/// ```yaml
/// component:
///   - name: orders
///     kind: domain
///     language: rust
///     scaffold:
///       snapshots: true
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScaffoldConfig {
    /// The aggregates and process managers, by component name
    /// (`ComponentOptions.component_name`), whose implementation the
    /// scaffold writes taking snapshots: overriding the interface's
    /// `snapshot` and `restore` (angzarr-embedded's `markup::Snapshots`),
    /// over the state it holds. Every other one keeps the interface's
    /// defaults and takes none; its implementation may override them
    /// later all the same, and `--missing` offers them.
    pub snapshots: Vec<String>,
}

/// [`scaffold`], each component's implementation importing its package's
/// domain layer at the Rust path `domain` names for it, and holding a wire
/// state at the path `wire` names (`crate::domain` and `crate::wire` for a
/// package neither names), as [`markup_domain_with`] places them, and
/// written as `config` says. A component `config` names that `files`
/// declare as a policy or a projector is refused
/// ([`UseError::ScaffoldNoComponent`]); one they do not declare is
/// another crate's, and left to it.
pub fn scaffold_with(
    descriptor_set: &[u8],
    files: &[&str],
    wire: &[(&str, &str)],
    domain: &[(&str, &str)],
    config: &ScaffoldConfig,
) -> Result<Vec<ScaffoldFile>, CodegenError> {
    let components = model::components(descriptor_set, files, ALL_KINDS)?;
    let pool = model::pool_components(descriptor_set)?;
    if let Some(stateless) = components
        .iter()
        .find(|c| !c.kind.is_domain() && config.snapshots.contains(&c.component_name))
    {
        return Err(UseError::ScaffoldNoComponent {
            component: stateless.component_name.clone(),
        }
        .into());
    }
    let mut out = Vec::new();
    for component in &components {
        if component.kind == ComponentKind::Policy && !component.is_connected(&pool) {
            continue;
        }
        let stem = snake(&component.name);
        let anchor = component.anchor.full_name().to_string();
        let methods = interface_methods(component, &pool)?;
        let selected =
            component.kind.is_domain() && config.snapshots.contains(&component.component_name);
        let stubs: Vec<&Method> = methods
            .iter()
            .filter(|m| m.default.is_none() || selected)
            .collect();
        out.push(ScaffoldFile::new(
            anchor.clone(),
            format!("{stem}.rs"),
            String::new(),
            Owner::Obsolete,
        ));
        let mut implementation = ScaffoldFile::new(
            anchor,
            format!("{stem}_impl.rs"),
            impl_source(component, &stubs, wire, domain),
            Owner::User,
        );
        if component.kind.is_domain() {
            implementation.optional = SNAPSHOT_METHODS.map(String::from).to_vec();
            if selected {
                implementation.selected = implementation.optional.clone();
            }
        }
        out.push(implementation);
    }
    Ok(crate::headed_files(out))
}

/// One method of a component's interface: its name, receiver,
/// parameters and what it returns.
struct Method {
    name: String,
    receiver: &'static str,
    /// `(param, type)`, after the receiver.
    params: Vec<(String, String)>,
    returns: Option<String>,
    /// An async method: the interface's returns the future of `returns`
    /// (`impl Future<Output = ..> + Send`), its implementation's is an
    /// `async fn`.
    asynchronous: bool,
    /// Whether this is a policy's one refusal callback. Every method is
    /// required.
    refused: bool,
    /// The implementation's body, when the scaffold can write it over the
    /// anchor's domain type it holds: `snapshot`'s and `restore`'s. Else
    /// `todo!()`.
    body: Option<&'static str>,
    /// The interface's default body, for a method it does not require:
    /// `snapshot`'s and `restore`'s, which take no snapshot. The scaffold
    /// writes such a method only where its configuration selects it.
    default: Option<String>,
}

impl Method {
    /// The signature as rustfmt lays it out at an indent of 4: on one line
    /// when it fits in 100 columns, else one parameter a line. `prefix`
    /// prefixes the parameters (`_` in an implementation, which writes an
    /// async method as an `async fn`: `implementation`).
    fn signature(&self, prefix: &str, end: &str, implementation: bool) -> String {
        let asyncness = if self.asynchronous && implementation {
            "async "
        } else {
            ""
        };
        let output = match (&self.returns, self.asynchronous && !implementation) {
            (Some(r), true) => format!(" -> impl std::future::Future<Output = {r}> + Send"),
            (Some(r), false) => format!(" -> {r}"),
            (None, _) => String::new(),
        };
        let params: Vec<String> = std::iter::once(self.receiver.to_string())
            .chain(self.params.iter().map(|(p, t)| format!("{prefix}{p}: {t}")))
            .collect();
        let line = format!(
            "    {asyncness}fn {}({}){output}{end}",
            self.name,
            params.join(", ")
        );
        if line.len() <= MAX_WIDTH {
            return line;
        }
        let params: String = params.iter().map(|p| format!("        {p},\n")).collect();
        format!(
            "    {asyncness}fn {}(\n{params}    ){output}{end}",
            self.name
        )
    }
}

/// rustfmt's default line width, which the scaffold is laid out to.
const MAX_WIDTH: usize = 100;

/// A method name for `message`: its snake case, raw where it is a keyword.
fn method(message: &str) -> String {
    ident(&snake(message)).to_string()
}

/// What a decision returns (decision 14.75): a list of any number of the
/// component's events, as its event enum.
fn decided(component: &Component) -> String {
    format!("Vec<{}Event>", component.name)
}

/// The typed refusal `input` refuses with, as the domain layer names it:
/// its one type, or the choice of several; `None` when it names none.
fn refusal_type(input: &Input) -> Option<String> {
    match input.refuses.as_slice() {
        [] => None,
        [one] => Some(one.name().to_string()),
        _ => Some(refusal_choice_name(&input.message)),
    }
}

/// A `(name, type)` parameter.
fn param(name: &str, ty: impl Into<String>) -> (String, String) {
    (name.to_string(), ty.into())
}

/// A component's interface, by kind, required methods first.
fn interface_methods(
    component: &Component,
    pool: &[Component],
) -> Result<Vec<Method>, CodegenError> {
    match component.kind {
        ComponentKind::Projector => Ok(projector_methods(component)),
        ComponentKind::Policy => policy_methods(component, pool),
        ComponentKind::Aggregate | ComponentKind::ProcessManager => Ok(domain_methods(component)),
    }
}

/// A projector's interface: a projection per event.
fn projector_methods(component: &Component) -> Vec<Method> {
    let projections = component.events.iter().map(|event| Method {
        name: method(event.name()),
        receiver: "&mut self",
        params: vec![
            param("event", format!("&{}", event.name())),
            param("ctx", "&ProjectionContext"),
        ],
        returns: Some("Result<(), Bug>".to_string()),
        asynchronous: true,
        refused: false,
        body: None,
        default: None,
    });
    projections.collect()
}

/// A policy's interface: a reaction per event, and, when its target has a
/// command that can refuse and its source takes facts, its one required
/// `refused` callback: the command refused (the target's command choice),
/// the refusal (the target's refusal choice, one arm per refusal message
/// its commands name, so a missing arm does not compile) and the refused
/// answer, returning one of the source's facts ([`fact_choice`]) or a
/// `Bug`; it cannot decline (decisions 14.89, 14.99). A target that cannot
/// refuse needs none.
fn policy_methods(component: &Component, pool: &[Component]) -> Result<Vec<Method>, CodegenError> {
    let target = component.target(pool)?;
    let reactions = component.events.iter().map(|event| Method {
        name: method(event.name()),
        receiver: "&self",
        params: vec![
            param("source", "&[u8]"),
            param("event", format!("&{}", event.name())),
            param("ctx", "&PolicyContext"),
        ],
        returns: Some(format!(
            "Result<Vec<(Vec<u8>, {})>, Bug>",
            output_name(target)
        )),
        asynchronous: false,
        refused: false,
        body: None,
        default: None,
    });
    let choice = owner(pool, &component.input_domain).and_then(fact_choice);
    let callback = choice
        .zip(refusal_choice(target))
        .map(|(fact, refusal)| Method {
            name: "refused".to_string(),
            receiver: "&self",
            params: vec![
                param("command", format!("{}Command", target.name)),
                param("refusal", refusal),
                param("ctx", "&Refused"),
            ],
            returns: Some(format!("Result<{fact}, Bug>")),
            asynchronous: false,
            refused: true,
            body: None,
            default: None,
        });
    Ok(reactions.chain(callback).collect())
}

/// The domain layer's choice of a domain's facts (`<Domain>Fact`), when it
/// takes any: what a translating callback answers its source with.
pub(crate) fn fact_choice(domain: &Component) -> Option<String> {
    (domain.kind.is_domain() && domain.inputs.iter().any(|i| i.fact))
        .then(|| format!("{}Fact", domain.name))
}

/// The domain layer's choice of a domain's typed refusals
/// (`<Component>Refusal`), when any of its commands names one: what a
/// policy targeting it is handed by its one refusal callback.
pub(crate) fn refusal_choice(domain: &Component) -> Option<String> {
    (domain.kind.is_domain()
        && domain
            .inputs
            .iter()
            .any(|i| !i.fact && !i.refuses.is_empty()))
    .then(|| format!("{}Refusal", domain.name))
}

/// An aggregate's or a process manager's interface: a decision per command,
/// a record per fact, and a fold per event; then its snapshots
/// ([`snapshot_methods`]).
fn domain_methods(component: &Component) -> Vec<Method> {
    let decisions = component.inputs.iter().map(|input| {
        let ty = input.message.name();
        let out = decided(component);
        let (arg, returns) = if input.fact {
            ("fact", format!("Result<{out}, Bug>"))
        } else {
            let error = refusal_type(input).unwrap_or_else(|| "Unrefusable".to_string());
            ("cmd", format!("Result<{out}, Failed<{error}>>"))
        };
        Method {
            name: method(ty),
            receiver: "&self",
            params: vec![param(arg, format!("&{ty}")), param("ctx", "&Context")],
            returns: Some(returns),
            asynchronous: false,
            refused: false,
            body: None,
            default: None,
        }
    });
    let folds = component.events.iter().map(|event| Method {
        name: format!("apply_{}", snake(event.name())),
        receiver: "&mut self",
        params: vec![param("event", format!("&{}", event.name()))],
        returns: None,
        asynchronous: false,
        refused: false,
        body: None,
        default: None,
    });
    decisions
        .chain(folds)
        .chain(snapshot_methods(component))
        .collect()
}

/// The names of an aggregate's or a process manager's snapshot methods:
/// what its interface does not require, and its scaffold's configuration
/// selects.
const SNAPSHOT_METHODS: [&str; 2] = ["snapshot", "restore"];

/// An aggregate's or a process manager's snapshots, over its anchor's
/// domain type (its state): `snapshot` and `restore`, which angzarr-embedded's
/// `markup::Snapshots` mirrors and its adapter delegates to. The interface's
/// defaults take none; an implementation overriding both opts in. The
/// scaffold, where its configuration selects them, writes them over the
/// state it holds.
fn snapshot_methods(component: &Component) -> Vec<Method> {
    let state = &component.name;
    let root = crate::rust::RUNTIME;
    vec![
        Method {
            name: "snapshot".to_string(),
            receiver: "&self",
            params: Vec::new(),
            returns: Some(format!("Option<{state}>")),
            asynchronous: false,
            refused: false,
            body: Some("Some(self.state.clone())"),
            default: Some("None".to_string()),
        },
        Method {
            name: "restore".to_string(),
            receiver: "&mut self",
            params: vec![param("state", state.as_str())],
            returns: Some("Result<(), Report>".to_string()),
            asynchronous: false,
            refused: false,
            body: Some("self.state = state;\n        Ok(())"),
            default: Some(format!(
                "let _ = state;\n        Err(Report::of(&::{root}::NotSnapshotted::of::<\n            \
                 <{state} as ::{root}::markup::DomainMessage>::Wire,\n        >()))"
            )),
        },
    ]
}

/// A component's interface, as the domain layer declares it: a trait over
/// the domain types, every method required.
fn interface_source(component: &Component, methods: &[Method]) -> String {
    let name = &component.name;
    let interface = &component.interface;
    let anchor = component.anchor.full_name();
    let what = match component.kind {
        ComponentKind::Projector => {
            "One async method per projected event, of any of its source domains \
             (`ctx.stream()` says which), returning its `Bug` when the projection fails."
        }
        ComponentKind::Policy => {
            "Each reaction takes the source stream's root, the event and its context, \
             and returns what it sends its target, each with its target root: a command, \
             or a fact the target declares, sent in the arm the target declares. Or its \
             `Bug`: a defect, since a policy reacts to events the application's own \
             domains recorded. Its one `refused` callback, required when its target has a \
             command that can refuse, takes the command refused and the refusal, a choice \
             of the target's refusal messages (a missing arm does not compile), and answers \
             one of the source's facts, or its `Bug`: it cannot decline."
        }
        ComponentKind::Aggregate | ComponentKind::ProcessManager => {
            "Each handler takes its input and its `Context`; each `apply_` folds an event."
        }
    };
    let head = format!(
        "{interface}: the interface of `{anchor}`, generated from its protos: its \
         implementation, `{name}Impl`, is yours (`angzarr scaffold rust` writes it \
         once), and the generated adapter (`adapters::{name}`) carries the markup and \
         calls it."
    );
    let state = "`snapshot` and `restore` are its snapshots, its state as the anchor's \
                 domain type, which angzarr-embedded stores as the anchor's wire type and \
                 resumes a fresh implementation from (`markup::Snapshots`). Their defaults \
                 take none; overriding both opts in.";
    let mut paragraphs = vec![head.as_str(), what];
    if component.kind.is_domain() {
        paragraphs.push(state);
    }
    let mut out = String::new();
    for (i, paragraph) in paragraphs.iter().enumerate() {
        if i > 0 {
            out.push_str("///\n");
        }
        for line in crate::common::wrap(paragraph, 76).lines() {
            out.push_str(&format!("/// {line}\n"));
        }
    }
    // A domain's handlers fail with the generated layer's `Failed`, whose
    // size is not the application's to choose.
    out.push_str(&format!(
        "#[allow(clippy::result_large_err)]\npub trait {interface} {{\n"
    ));
    let body: Vec<String> = methods
        .iter()
        .map(|m| match &m.default {
            Some(default) => format!(
                "{}\n        {default}\n    }}\n",
                m.signature("", " {", false)
            ),
            None => format!("{}\n", m.signature("", ";", false)),
        })
        .collect();
    out.push_str(&body.join("\n"));
    out.push_str("}\n");
    out
}

/// A component's implementation, as the scaffold writes it: `methods`, its
/// interface's required methods and the ones its configuration selects,
/// over the state it holds. An aggregate's or a process manager's state is
/// its anchor's domain type where that type has a `Default`, so a selected
/// `snapshot` and `restore` clone and assign it; else the anchor's wire
/// type, and their bodies, like every other, are `todo!()`.
fn impl_source(
    component: &Component,
    methods: &[&Method],
    wire: &[(&str, &str)],
    domain: &[(&str, &str)],
) -> String {
    let name = &component.name;
    let interface = &component.interface;
    let package = component.anchor.package_name();
    let (wire, domain) = (
        rust_path(wire, package, WIRE),
        rust_path(domain, package, DOMAIN),
    );
    let held = component.kind.is_domain() && crate::rust::has_default(&component.anchor);
    let written = |m: &Method| m.body.filter(|_| held);
    let stubs = if methods.iter().any(|m| written(m).is_some()) {
        "each `todo!()` but\n//! `snapshot` and `restore`, which take snapshots of `state`"
    } else {
        "each `todo!()`"
    };
    let mut out = format!(
        "//! {name}Impl: the implementation of `{interface}`, the generated domain layer's\n\
         //! interface, written once by `angzarr scaffold rust` and yours from then on:\n\
         //! `--missing` adds the required methods it lacks, {stubs}.\n\n"
    );
    out.push_str(&format!("use {domain}::*;\n\n"));
    if component.kind == ComponentKind::Policy {
        out.push_str(&format!(
            "#[derive(Clone, Debug, Default)]\npub struct {name}Impl;\n\n"
        ));
    } else if component.kind == ComponentKind::Projector {
        out.push_str(&format!(
            "/// The projection the methods update.\n\
             #[derive(Clone, Debug, Default)]\n\
             pub struct {name}Impl {{\n    pub state: {wire}::{},\n}}\n\n",
            component.anchor.name()
        ));
    } else if held {
        out.push_str(&format!(
            "/// The state the `apply_` methods fold: the anchor's domain type.\n\
             #[derive(Clone, Debug, Default)]\n\
             pub struct {name}Impl {{\n    pub state: {name},\n}}\n\n"
        ));
    } else {
        out.push_str(&format!(
            "/// The state the `apply_` methods fold: the anchor's wire type, as its\n\
             /// domain type has no `Default`.\n\
             #[derive(Clone, Debug, Default)]\n\
             pub struct {name}Impl {{\n    pub state: {wire}::{},\n}}\n\n",
            component.anchor.name()
        ));
    }
    out.push_str(&format!("impl {interface} for {name}Impl {{\n"));
    let body: Vec<String> = methods
        .iter()
        .map(|m| match written(m) {
            Some(body) => format!("{}\n        {body}\n    }}\n", m.signature("", " {", true)),
            None => format!(
                "{}\n        todo!()\n    }}\n",
                m.signature("_", " {", true)
            ),
        })
        .collect();
    out.push_str(&body.join("\n"));
    out.push_str("}\n");
    out
}

/// `tokens`, a whole file, formatted.
pub(crate) fn file(tokens: TokenStream) -> String {
    let file: syn::File = syn::parse2(tokens).expect("the emitter writes valid Rust");
    prettyplease::unparse(&file)
}

// ------------------------------------------------------------- --missing

/// What [`complete`] does with a user's file that exists.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Existing {
    /// Leave its required methods to the user, reporting the ones it lacks:
    /// the file is the user's.
    Keep,
    /// `--missing`: add the required methods it lacks, changing nothing
    /// else.
    Complete,
}

/// One method of a scaffold or of the user's file: its name, the message it
/// takes first and the parameter it takes it as.
pub(crate) struct Found {
    pub name: String,
    pub ty: String,
    pub param: String,
}

/// `existing`, the user's file for `fresh`'s component, against `fresh`: the
/// required methods it lacks (added with `Existing::Complete`, else
/// reported), the selected optional methods it lacks (added), the optional
/// ones it does not define and the obsolete ones it has (reported), and
/// nothing else changed. A method is present when the user's has one taking
/// the same message; one taking an input as `cmd` where the proto declares
/// a fact (`fact`), or the reverse, is refused, the file left as it is. A
/// file with nothing to complete (a manifest) is kept as it is.
///
/// The methods are inserted in one place, at the end of the trait impl or
/// class; one needing a type the file does not import yet works once the
/// user adds the import.
pub fn complete(
    fresh: &ScaffoldFile,
    existing: &str,
    mode: Existing,
) -> Result<Completion, CodegenError> {
    if fresh.path.ends_with(".py") {
        return Ok(crate::python::complete(fresh, existing, mode)?);
    }
    if fresh.path.ends_with(".toml") {
        return Ok(Completion {
            completed: None,
            notes: Notes {
                drift: pin_drift(&fresh.source, existing),
                ..Notes::default()
            },
        });
    }
    if !fresh.path.ends_with(".rs") {
        return Ok(Completion::default());
    }
    let not_found = || UseError::ScaffoldImplNotFound {
        path: fresh.path.clone(),
        component: fresh.component.clone(),
    };
    let fresh_file = syn::parse_file(&fresh.source).expect("a scaffold parses");
    // A file with neither has nothing to complete.
    let Some(fresh_block) = block(&fresh_file, None) else {
        return Ok(Completion::default());
    };
    let user_file = syn::parse_file(existing).map_err(|_| not_found())?;
    let user_block = block(&user_file, Some(&fresh_block)).ok_or_else(not_found)?;
    let found = |b: &Block| -> Vec<Found> {
        b.methods
            .iter()
            .map(|m| Found {
                name: m.name.clone(),
                ty: m.ty.clone(),
                param: m.param.clone(),
            })
            .collect()
    };
    let (adding, mut completion) = judge(
        fresh,
        &found(&fresh_block),
        &found(&user_block),
        mode,
        &|_| true,
    )?;
    if adding.is_empty() {
        return Ok(completion);
    }

    // The methods as the scaffold lays them out, a blank line before each.
    let mut stubs = String::new();
    for i in adding {
        let method = &fresh_block.methods[i];
        let start = line_start(&fresh.source, method.lines.0);
        let end = line_start(&fresh.source, method.lines.1 + 1);
        stubs.push('\n');
        stubs.push_str(&fresh.source[start..end]);
    }

    let close = user_block.close;
    let line = line_start(existing, close.line);
    let brace = line
        + existing[line..]
            .char_indices()
            .nth(close.column)
            .map_or(0, |(i, _)| i);
    let at = if existing[line..brace].trim().is_empty() {
        line
    } else {
        stubs.insert(0, '\n');
        brace
    };
    let mut completed = existing.to_string();
    completed.insert_str(at, &stubs);
    completion.completed = Some(completed);
    Ok(completion)
}

/// The judgement of a user's file against its fresh scaffold, the two read
/// as [`Found`] methods (`ours` saying which of the user's methods are the
/// scaffold's kind, a message's handler, and not the user's own helpers):
/// the indices of `fresh_methods` to add, and the [`Completion`]'s reports.
/// Refused when the user takes an input as the other kind.
pub(crate) fn judge(
    fresh: &ScaffoldFile,
    fresh_methods: &[Found],
    user_methods: &[Found],
    mode: Existing,
    ours: &dyn Fn(&Found) -> bool,
) -> Result<(Vec<usize>, Completion), UseError> {
    let package = fresh
        .component
        .rsplit_once('.')
        .map_or("", |(package, _)| package);
    let mut adding = Vec::new();
    let mut completion = Completion::default();
    for (i, method) in fresh_methods.iter().enumerate() {
        match user_methods.iter().find(|m| m.ty == method.ty) {
            Some(found)
                if is_input(&found.param)
                    && is_input(&method.param)
                    && found.param != method.param =>
            {
                return Err(UseError::InputMismatch {
                    path: fresh.path.clone(),
                    message: format!("{package}.{}", method.ty),
                    expected: method.param.clone(),
                    found: found.param.clone(),
                });
            }
            Some(_) => {}
            None if fresh.selected.contains(&method.name) => adding.push(i),
            None if fresh.optional.contains(&method.name) => {}
            None if mode == Existing::Complete => adding.push(i),
            None => completion.notes.lacks.push(method.name.clone()),
        }
    }
    let defined = |name: &str| user_methods.iter().any(|m| m.name == name);
    let added = |name: &str| adding.iter().any(|&i| fresh_methods[i].name == name);
    completion.notes.optional = fresh
        .optional
        .iter()
        .filter(|name| !defined(name) && !added(name))
        .cloned()
        .collect();
    completion.notes.obsolete = user_methods
        .iter()
        .filter(|m| ours(m))
        .filter(|m| !fresh_methods.iter().any(|f| f.ty == m.ty))
        .filter(|m| !fresh.optional.contains(&m.name))
        .map(|m| m.name.clone())
        .collect();
    Ok((adding, completion))
}

/// The dependencies `existing`, a user's manifest (TOML), pins at another
/// revision than `fresh`, the manifest as this CLI would write it: each
/// `<name> = { .., rev = "<rev>" .. }` line of `fresh` against the line of
/// the same name in `existing`. A dependency the manifest does not pin by
/// revision (a path, a branch) is the user's choice, and not drift.
pub(crate) fn pin_drift(fresh: &str, existing: &str) -> Vec<PinDrift> {
    let pins = |text: &str| -> Vec<(String, String)> {
        text.lines()
            .filter_map(|line| {
                let (name, value) = line.split_once('=')?;
                let rev = value.split("rev = \"").nth(1)?.split('"').next()?;
                Some((name.trim().to_string(), rev.to_string()))
            })
            .collect()
    };
    let mine = pins(existing);
    pins(fresh)
        .into_iter()
        .filter_map(|(dependency, cli)| {
            let (_, pinned) = mine.iter().find(|(name, _)| *name == dependency)?;
            (*pinned != cli).then(|| PinDrift {
                dependency,
                pinned: pinned.clone(),
                cli,
            })
        })
        .collect()
}

/// The byte offset where 1-based line `line` of `text` starts (the length
/// of `text` past its last line).
fn line_start(text: &str, line: usize) -> usize {
    text.split_inclusive('\n')
        .take(line.saturating_sub(1))
        .map(str::len)
        .sum()
}

/// The parameters that name an input.
fn is_input(param: &str) -> bool {
    param == "cmd" || param == "fact"
}

/// The trait of an interface, or the trait impl of an implementation.
struct Block {
    /// The trait's name, and for an impl its self type.
    key: (String, Option<String>),
    methods: Vec<BlockMethod>,
    close: proc_macro2::LineColumn,
}

/// One method of a [`Block`]: its name, the message it takes (the last
/// segment of its first parameter's type that is not the source root; for
/// a method taking none, its name and `()`) and the parameter it takes it as
/// (a leading `_` dropped), and its first and last lines.
struct BlockMethod {
    name: String,
    ty: String,
    param: String,
    lines: (usize, usize),
}

/// The first trait or trait impl of `file` (with `like`, the one of the
/// same trait and self type).
fn block(file: &syn::File, like: Option<&Block>) -> Option<Block> {
    file.items.iter().find_map(|item| {
        let (key, sigs, close): (_, Vec<(&syn::Signature, usize, usize)>, _) = match item {
            syn::Item::Trait(t) => (
                (t.ident.to_string(), None),
                t.items
                    .iter()
                    .filter_map(|i| match i {
                        syn::TraitItem::Fn(f) => Some((
                            &f.sig,
                            first_line(&f.attrs, f.sig.fn_token.span),
                            last_line(f.semi_token.map(|s| s.span), f.default.as_ref()),
                        )),
                        _ => None,
                    })
                    .collect(),
                t.brace_token.span.close().start(),
            ),
            syn::Item::Impl(imp) => {
                let (path, _) = imp.trait_.as_ref()?;
                let ty = &imp.self_ty;
                (
                    (
                        path.segments.last()?.ident.to_string(),
                        Some(quote!(#ty).to_string()),
                    ),
                    imp.items
                        .iter()
                        .filter_map(|i| match i {
                            syn::ImplItem::Fn(f) => Some((
                                &f.sig,
                                first_line(&f.attrs, f.sig.fn_token.span),
                                f.block.brace_token.span.close().end().line,
                            )),
                            _ => None,
                        })
                        .collect(),
                    imp.brace_token.span.close().start(),
                )
            }
            _ => return None,
        };
        if let Some(like) = like
            && like.key != key
        {
            return None;
        }
        let methods = sigs
            .into_iter()
            .map(|(sig, first, last)| {
                // A method taking no message (an aggregate's `snapshot`)
                // is keyed by its name, written so no message's name is it.
                let (param, ty) = first_message(sig)
                    .unwrap_or_else(|| (String::new(), format!("{}()", sig.ident)));
                BlockMethod {
                    name: sig.ident.to_string(),
                    ty,
                    param,
                    lines: (first, last),
                }
            })
            .collect();
        Some(Block {
            key,
            methods,
            close,
        })
    })
}

fn first_line(attrs: &[syn::Attribute], fn_span: proc_macro2::Span) -> usize {
    attrs
        .first()
        .map_or(fn_span, |a| a.pound_token.span)
        .start()
        .line
}

fn last_line(semi: Option<proc_macro2::Span>, body: Option<&syn::Block>) -> usize {
    match (semi, body) {
        (_, Some(body)) => body.brace_token.span.close().end().line,
        (Some(semi), None) => semi.end().line,
        (None, None) => 0,
    }
}

/// A signature's first parameter naming a message, as `(param, type)`:
/// the source root (`&[u8]`) is passed over.
fn first_message(sig: &syn::Signature) -> Option<(String, String)> {
    sig.inputs.iter().find_map(|input| {
        let syn::FnArg::Typed(typed) = input else {
            return None;
        };
        let syn::Pat::Ident(pat) = &*typed.pat else {
            return None;
        };
        let mut ty = &*typed.ty;
        if let syn::Type::Reference(r) = ty {
            ty = &r.elem;
        }
        let syn::Type::Path(path) = ty else {
            return None;
        };
        let last = path.path.segments.last()?;
        let name = last.ident.to_string();
        // `Option<Refusal>` and the like are not what a method takes first.
        if !matches!(last.arguments, syn::PathArguments::None) {
            return None;
        }
        Some((
            pat.ident.to_string().trim_start_matches('_').to_string(),
            name,
        ))
    })
}

// -------------------------------------------------------- generated layer

/// The components of an application in `files`, with `remote` the domains
/// served by a child process: every aggregate and process manager, and
/// every policy whose two domains are owned among them.
fn application(components: &[Component]) -> Vec<&Component> {
    components
        .iter()
        .filter(|c| c.kind != ComponentKind::Policy || c.is_connected(components))
        .collect()
}

/// The generated adapters of every component declared in `files`, as the
/// module `adapters` (carrying the allowances generated code needs, so no
/// include site does): each aggregate's, process manager's, policy's and
/// projector's angzarr-embedded markup, every handler delegating to the
/// component's interface (the domain layer's trait, [`markup_domain`]) on
/// its implementation (`crate::<component>_impl::<Component>Impl`). A
/// policy's adapter has its one `#[on_refused]` callback, reading the
/// target's refusal choice and translating it through its interface's
/// required `refused` (decisions 14.89, 14.99). An aggregate's or a process
/// manager's adapter implements angzarr-embedded's `markup::Snapshots` over
/// its anchor's domain type through its interface's `snapshot` and
/// `restore`, so it takes snapshots exactly when its implementation
/// overrides them. What a child process serves is in `remote`, a domain by its
/// stream domain, a policy or a projector by its name in the application.
/// A remote domain has no markup here: it is its command set
/// (`CommandTarget`) and its event set (`EventSource`), so a policy can name
/// it as its source or target; it is never registered. A remote policy or
/// projector has nothing here.
pub fn adapters(
    descriptor_set: &[u8],
    files: &[&str],
    remote: &[&str],
) -> Result<String, CodegenError> {
    adapters_with(descriptor_set, files, remote, &[])
}

/// [`adapters`], each component's adapter reading its package's domain
/// layer at the Rust path `domain` names for it (`crate::domain` for a
/// package it does not name), as [`markup_domain_with`] places them. Over
/// layers at more than one path, each package's adapters are a module of
/// their own, re-exported, so every adapter is still `adapters::<Component>`.
pub fn adapters_with(
    descriptor_set: &[u8],
    files: &[&str],
    remote: &[&str],
    domain: &[(&str, &str)],
) -> Result<String, CodegenError> {
    let components = model::components(descriptor_set, files, ALL_KINDS)?;
    let root = root();
    // Each package's adapters, in the order their components come.
    let mut packages: Vec<(String, TokenStream)> = Vec::new();
    let mut markup = vec!["Context", "Emit"];
    let mut macros = vec![];
    let mut has_policy = false;
    for component in application(&components) {
        if !component.kind.is_domain() && remote.contains(&component.component_name.as_str()) {
            continue;
        }
        let is_remote = remote.contains(&component.domain.as_str());
        let tokens = match component.kind {
            ComponentKind::Projector => {
                markup.push("Report");
                macros.extend(["projector", "projects"]);
                projector_adapter(component, domain)
            }
            ComponentKind::Policy => {
                has_policy = true;
                markup.extend(["Commands", "PolicyContext"]);
                macros.extend(["on_event", "policy"]);
                let methods = policy_methods(component, &components)?;
                let refused = methods.iter().any(|m| m.refused);
                if refused {
                    markup.push("Refused");
                    macros.push("on_refused");
                }
                policy_adapter(component, &components, refused, domain)
            }
            _ if is_remote => {
                markup.extend(["CommandTarget", "EventSource"]);
                if component.inputs.iter().any(|i| !i.fact) {
                    markup.push("Decides");
                }
                if component.inputs.iter().any(|i| i.fact) {
                    markup.push("Ingests");
                }
                remote_domain(component)
            }
            _ => {
                macros.extend(["applies", "handles_cmd", "handles_fact"]);
                markup.extend(["Fails", "FactEmit", "Refuses"]);
                macros.push(domain_markup(component.kind));
                domain_adapter(component, domain)
            }
        };
        let package = component.anchor.package_name();
        match packages.iter_mut().find(|(p, _)| p == package) {
            Some((_, items)) => items.extend(tokens),
            None => packages.push((package.to_string(), tokens)),
        }
    }
    markup.sort();
    markup.dedup();
    macros.sort();
    macros.dedup();
    let markup: Vec<syn::Ident> = markup.iter().map(|m| format_ident!("{m}")).collect();
    let macros: Vec<syn::Ident> = macros.iter().map(|m| format_ident!("{m}")).collect();
    let macros_crate = runtime_macros();
    // A remote domain alone takes no macro: no import of none.
    let macros = if macros.is_empty() {
        quote!()
    } else {
        quote! {
            #[allow(unused_imports)]
            use ::#macros_crate::{#(#macros),*};
        }
    };
    let uuid = if has_policy {
        quote!(use #root::v1::Uuid;)
    } else {
        quote!()
    };
    let layers = adapter_layers(packages, domain);
    let doc = " The adapter layer: each component's angzarr-embedded markup, every \
               handler delegating to the component's interface (the domain layer's \
               trait) on its implementation (the user's). A domain's handlers fail \
               with the generated layer's `Failed`, whose size is not the \
               application's to choose.";
    let source = file(quote! {
        #[doc = #doc]
        #[allow(clippy::result_large_err)]
        pub mod adapters {
            #[allow(unused_imports)]
            use #root::markup::{#(#markup),*};
            #uuid
            #macros
            #layers
        }
    });
    Ok(crate::headed(
        ".rs",
        format!(
            "// @generated by angzarr-codegen from {}. Do not edit.\n//\n\
             // Regenerated every build; the implementations are the user's and are\n\
             // never touched.\n\n{source}",
            files.join(", ")
        ),
    ))
}

/// The domain layer of a package no path is given for.
const DOMAIN: &str = "crate::domain";
/// The wire types of a package no path is given for.
const WIRE: &str = "crate::wire";

/// The Rust path `paths` names for `package`, when it parses as a path,
/// else `default`.
fn rust_path<'a>(paths: &[(&'a str, &'a str)], package: &str, default: &'a str) -> &'a str {
    paths
        .iter()
        .find(|(p, rust)| *p == package && syn::parse_str::<syn::Path>(rust).is_ok())
        .map_or(default, |(_, rust)| rust)
}

/// The adapters of each package, reading its domain layer: over one layer,
/// the adapters as they are, importing it; over several, each package's in
/// a module of its own importing its layer (and the module above's
/// imports), re-exported.
fn adapter_layers(packages: Vec<(String, TokenStream)>, domain: &[(&str, &str)]) -> TokenStream {
    let path = |package: &str| -> syn::Path {
        syn::parse_str(rust_path(domain, package, DOMAIN)).expect("a path that parses")
    };
    let mut layers: Vec<&str> = packages
        .iter()
        .map(|(p, _)| rust_path(domain, p, DOMAIN))
        .collect();
    layers.dedup();
    if let [] | [_] = layers.as_slice() {
        let layer = path(layers.first().copied().unwrap_or(DOMAIN));
        let items = packages.into_iter().map(|(_, items)| items);
        return quote! {
            use #layer::*;
            #(#items)*
        };
    }
    packages
        .into_iter()
        .map(|(package, items)| {
            let layer = path(&package);
            let module = format_ident!("{}", package.replace('.', "_"));
            let doc = format!(" The adapters of `{package}`'s components, reading its layer.");
            quote! {
                #[doc = #doc]
                mod #module {
                    #[allow(unused_imports)]
                    use super::*;
                    use #layer::*;
                    #items
                }
                pub use self::#module::*;
            }
        })
        .collect()
}

/// A component's adapter: its type, wrapping the user's implementation,
/// and the interface each of its handlers delegates to.
struct Adapter {
    name: syn::Ident,
    /// The component's interface, the domain layer's trait,
    /// `<its package's layer>::<Component>` (`crate::domain` by default).
    interface: TokenStream,
    /// The user's type it wraps, `crate::<stem>_impl::<Component>Impl`.
    implementation: TokenStream,
}

impl Adapter {
    fn of(component: &Component, domain: &[(&str, &str)]) -> Adapter {
        let layer: syn::Path =
            syn::parse_str(rust_path(domain, component.anchor.package_name(), DOMAIN))
                .expect("a path that parses");
        let stem_impl = format_ident!("{}_impl", snake(&component.name));
        let name = ident(&component.name);
        let name_impl = format_ident!("{}Impl", component.name);
        let interface = ident(&component.interface);
        Adapter {
            interface: quote!(#layer::#interface),
            implementation: quote!(crate::#stem_impl::#name_impl),
            name,
        }
    }

    /// The adapter's type, documented as `component`'s, delegating to the
    /// interface.
    fn declaration(&self, component: &Component) -> TokenStream {
        let Adapter {
            name,
            interface,
            implementation,
        } = self;
        let doc = format!(
            " `{}`, delegating to `{}`.",
            component.anchor.full_name(),
            interface.to_string().replace(' ', "")
        );
        quote! {
            #[doc = #doc]
            #[derive(Clone, Debug, Default)]
            pub struct #name(pub #implementation);
        }
    }
}

/// A domain adapter's helpers turning a handling's list of the domain's
/// events (decision 14.75) into what the markup takes: `emitted`, a
/// command's `Emit`, generic over the handler's failure `E`, and
/// `recorded`, a fact's `FactEmit` of the events packed, each command event one
/// continuation step (the markup takes no event enum as is). One of each per domain, generated
/// only where an input takes it, so no handler repeats the fold.
fn steps(component: &Component) -> TokenStream {
    let enum_name = format_ident!("{}Event", component.name);
    // The empty decision starts from an event type: the domain's first, or
    // with none its own event type, which the domain layer makes a
    // `DomainMessage` for the purpose.
    let first = component
        .events
        .first()
        .map(|e| ident(e.name()))
        .unwrap_or_else(|| enum_name.clone());
    let arms = |component: &Component| -> Vec<TokenStream> {
        component
            .events
            .iter()
            .map(|e| {
                let v = ident(e.name());
                quote!(#enum_name::#v(event) => emit.then(move |_: &Self, _: &Context| Ok::<_, E>(event)))
            })
            .collect()
    };
    let mut out = TokenStream::new();
    if component.inputs.iter().any(|i| !i.fact) {
        let arms = arms(component);
        out.extend(quote! {
            /// A decision's events, each a step of the markup's `Emit`,
            /// failing as its handler does (`E`).
            fn emitted<E: Fails<Self> + 'static>(events: Vec<#enum_name>) -> Emit<Self> {
                events.into_iter().fold(Emit::of(None::<#first>), |emit, event| match event {
                    #(#arms,)*
                })
            }
        });
    }
    if component.inputs.iter().any(|i| i.fact) {
        let enum_name = &enum_name;
        out.extend(quote! {
            /// A fact's events, as the router's `FactEmit`.
            fn recorded(events: Vec<#enum_name>) -> FactEmit<Self> {
                use ::angzarr_embedded::Payload as _;
                FactEmit::of(events.iter().map(|event| event.to_any()))
            }
        });
    }
    out
}

/// The markup macro of a domain of `kind`: `process_manager` or
/// `aggregate`.
fn domain_markup(kind: ComponentKind) -> &'static str {
    if kind == ComponentKind::ProcessManager {
        "process_manager"
    } else {
        "aggregate"
    }
}

/// A domain's adapter: its handler per input and fold per event, each
/// delegating to its interface.
fn domain_adapter(component: &Component, domain: &[(&str, &str)]) -> TokenStream {
    let adapter = Adapter::of(component, domain);
    let snapshots = snapshots(component, &adapter, domain);
    let declaration = adapter.declaration(component);
    let Adapter {
        name,
        interface,
        implementation,
    } = &adapter;
    let markup = format_ident!("{}", domain_markup(component.kind));
    let domain = &component.domain;
    // A process manager lists its starting inputs, which the macro
    // cross-checks against the handlers marked `starts`.
    let starting = (component.kind == ComponentKind::ProcessManager).then(|| {
        let types = component
            .inputs
            .iter()
            .filter(|i| i.starts)
            .map(|i| ident(i.message.name()));
        quote!(, starting_types = [#(#types),*])
    });
    // The domain layer's named enums, which carry their Payload: the
    // domain's payloads, so the markup writes none of its own.
    let commands = format_ident!("{}Command", component.name);
    let events = format_ident!("{}Event", component.name);
    let handlers = component.inputs.iter().map(|input| {
        if input.fact {
            fact_method(input, interface)
        } else {
            command_method(input, interface)
        }
    });
    let folds = component
        .events
        .iter()
        .map(|event| fold_method(event, interface));
    let methods: Vec<TokenStream> = handlers.chain(folds).collect();
    let steps = steps(component);
    quote! {
        #declaration

        impl std::ops::Deref for #name {
            type Target = #implementation;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        #[#markup(domain = #domain, command = #commands, event = #events #starting)]
        #[allow(clippy::result_large_err)]
        impl #name {
            #(#methods)*
        }

        impl #name {
            #steps
        }

        #snapshots
    }
}

/// A domain adapter's `markup::Snapshots`: its anchor's domain type (its
/// state), and the interface's `snapshot` and `restore`, which take none
/// unless the implementation overrides them. The markup writes the
/// aggregate's or process manager's `State` from it, the anchor's wire type
/// its registered state type and what a snapshot holds.
fn snapshots(component: &Component, adapter: &Adapter, domain: &[(&str, &str)]) -> TokenStream {
    let Adapter {
        name, interface, ..
    } = adapter;
    let layer: syn::Path =
        syn::parse_str(rust_path(domain, component.anchor.package_name(), DOMAIN))
            .expect("a path that parses");
    let root = root();
    quote! {
        /// Its snapshots: the implementation's, as its interface takes them.
        impl #root::markup::Snapshots for #name {
            type Anchor = #layer::#name;

            fn snapshot(&self) -> Option<#layer::#name> {
                #interface::snapshot(&self.0)
            }

            fn restore(&mut self, anchor: #layer::#name) -> Result<(), #root::markup::Report> {
                #interface::restore(&mut self.0, anchor)
            }
        }
    }
}

/// A domain adapter's fold of `event`, delegating to `interface`.
fn fold_method(event: &MessageDescriptor, interface: &TokenStream) -> TokenStream {
    let ty = ident(event.name());
    let m = format_ident!("{}", method(event.name()));
    let apply = format_ident!("apply_{}", snake(event.name()));
    quote! {
        #[applies(#ty)]
        fn #m(&mut self, event: #ty) {
            #interface::#apply(&mut self.0, &event);
        }
    }
}

/// A handler's `starts` marker: a process manager's starting input, a
/// command or a fact.
fn starts(input: &Input) -> TokenStream {
    match input.starts {
        true => quote!(, starts),
        false => quote!(),
    }
}

/// A domain adapter's handler of the fact `input`, delegating to
/// `interface`: the router's `FactEmit`, its one failure a `Bug`.
fn fact_method(input: &Input, interface: &TokenStream) -> TokenStream {
    let ty = ident(input.message.name());
    let starts = starts(input);
    let m = format_ident!("{}", method(input.message.name()));
    let root = root();
    quote! {
        #[handles_fact(#ty #starts)]
        fn #m(
            &self,
            fact: #ty,
            ctx: &Context,
        ) -> Result<FactEmit<Self>, #root::markup::Bug> {
            #interface::#m(&self.0, &fact, ctx)
                .map(Self::recorded)
                .map_err(#root::markup::Bug::from)
        }
    }
}

/// A domain adapter's handler of the command `input`, delegating to
/// `interface`, its refusals widened to the adapter's.
fn command_method(input: &Input, interface: &TokenStream) -> TokenStream {
    let ty = ident(input.message.name());
    let m = format_ident!("{}", method(input.message.name()));
    let starts = starts(input);
    match input.refuses.as_slice() {
        [] => {
            let emit = quote!(Self::emitted::<Failed<Unrefusable>>);
            quote! {
                #[handles_cmd(#ty #starts)]
                fn #m(&self, cmd: #ty, ctx: &Context) -> Result<Emit<Self>, Failed<Unrefusable>> {
                    #interface::#m(&self.0, &cmd, ctx).map(#emit)
                }
            }
        }
        refusals => {
            let emit = quote!(Self::emitted::<Failed<Refuses<Self, #ty>>>);
            // One type widens by its own `Into`; several, by their
            // domain-layer choice's `widen`. A refusal is refused under the
            // status its message declares first.
            let refusal = ident(&refusal_type(input).expect("it refuses with types"));
            let widen = match refusals {
                [_] => quote!(Failed::widen),
                _ => quote!(#refusal::widen),
            };
            let widen = quote!(map_err(#refusal::with_declared_status).map_err(#widen));
            let refusals: Vec<syn::Ident> = refusals.iter().map(|r| ident(r.name())).collect();
            quote! {
                #[handles_cmd(#ty #starts, refuses(#(#refusals),*))]
                fn #m(&self, cmd: #ty, ctx: &Context) -> Result<Emit<Self>, Failed<Refuses<Self, #ty>>> {
                    #interface::#m(&self.0, &cmd, ctx).map(#emit).#widen
                }
            }
        }
    }
}

/// The stand-in of every aggregate and process manager declared in
/// `files` served by a child process, generated from its proto: what a
/// Rust policy names it by, its command set (`CommandTarget`, `Decides` of
/// each command) and its event set (`EventSource`, `Ingests` of each fact),
/// for a crate that hand-writes its policies and generates no adapter of
/// the domain. Its domain types are `crate::domain`'s.
pub fn stand_ins(descriptor_set: &[u8], files: &[&str]) -> Result<String, CodegenError> {
    let components = model::components(descriptor_set, files, ALL_KINDS)?;
    let domains: Vec<&Component> = components.iter().filter(|c| c.kind.is_domain()).collect();
    let mut markup = vec!["CommandTarget", "EventSource"];
    if domains.iter().any(|d| d.inputs.iter().any(|i| !i.fact)) {
        markup.push("Decides");
    }
    if domains.iter().any(|d| d.inputs.iter().any(|i| i.fact)) {
        markup.push("Ingests");
    }
    markup.sort_unstable();
    let markup: Vec<syn::Ident> = markup.iter().map(|m| format_ident!("{m}")).collect();
    let root = root();
    let items = domains.into_iter().map(remote_domain);
    let source = file(quote! {
        #[allow(unused_imports)]
        use #root::markup::{#(#markup),*};
        #[allow(unused_imports)]
        use crate::domain::*;
        #(#items)*
    });
    Ok(crate::headed(
        ".rs",
        format!(
            "// @generated by angzarr-codegen from {}. Do not edit.\n//\n\
             // The stand-ins of the domains a child process serves: each one's command\n\
             // set and event set, as a Rust policy names it as its target or source.\n\
             // Regenerated every build; never registered.\n\n{source}",
            files.join(", ")
        ),
    ))
}

/// A remote domain (served by a child process) as a policy names it: its
/// command set (`CommandTarget`, `Decides` of each command) and its
/// event set (`EventSource`, `Ingests` of each fact), each type by its full
/// name. No aggregate stands in for it.
fn remote_domain(component: &Component) -> TokenStream {
    let name = ident(&component.name);
    let doc = format!(
        " `{}`, served by a child process: its commands and events, as a \
         policy names it; never registered.",
        component.anchor.full_name()
    );
    let domain = &component.domain;
    let full = |m: &prost_reflect::MessageDescriptor| m.full_name().to_string();
    let commands: Vec<String> = component
        .inputs
        .iter()
        .filter(|i| !i.fact)
        .map(|i| full(&i.message))
        .collect();
    let events: Vec<String> = component.events.iter().map(full).collect();
    let decides = component.inputs.iter().filter(|i| !i.fact).map(|i| {
        let ty = ident(i.message.name());
        quote!(impl Decides<#ty> for #name {})
    });
    let ingests = component.inputs.iter().filter(|i| i.fact).map(|i| {
        let ty = ident(i.message.name());
        quote!(impl Ingests<#ty> for #name {})
    });
    quote! {
        #[doc = #doc]
        #[derive(Clone, Debug, Default)]
        pub struct #name;

        impl CommandTarget for #name {
            const DOMAIN: &'static str = #domain;

            fn command_types() -> Vec<String> {
                vec![#(#commands.to_string()),*]
            }
        }

        #(#decides)*

        impl EventSource for #name {
            const DOMAIN: &'static str = #domain;

            fn event_types() -> Vec<String> {
                vec![#(#events.to_string()),*]
            }
        }

        #(#ingests)*
    }
}

/// A policy's adapter: its reaction per event, each output in the arm its
/// target declares, and, when its interface has one, its one
/// `#[on_refused]` callback delegating to the interface's `refused`.
fn policy_adapter(
    component: &Component,
    components: &[Component],
    refused: bool,
    domain: &[(&str, &str)],
) -> TokenStream {
    let adapter = Adapter::of(component, domain);
    let declaration = adapter.declaration(component);
    let Adapter {
        name, interface, ..
    } = &adapter;
    let (Some(source), Some(target)) = (
        owner(components, &component.input_domain),
        owner(components, &component.output_domain),
    ) else {
        return quote!();
    };
    let policy_name = &component.component_name;
    let source_ty = ident(&source.name);
    let target_ty = ident(&target.name);
    // Each output in the arm its target declares (decision 14.69).
    let output_enum = format_ident!("{}", output_name(target));
    let arms: Vec<TokenStream> = target
        .inputs
        .iter()
        .map(|i| {
            let v = ident(i.message.name());
            match Arm::of(i) {
                Arm::Command => quote!(#output_enum::#v(output) => commands.to(root, output)),
                Arm::Fact => quote!(#output_enum::#v(output) => commands.fact(root, output)),
            }
        })
        .collect();
    let methods = component.events.iter().map(|event| {
        let ty = ident(event.name());
        let m = format_ident!("{}", method(event.name()));
        quote! {
            #[on_event(#ty)]
            fn #m(
                &self,
                event: &#ty,
                ctx: &PolicyContext,
            ) -> ::core::result::Result<Commands<#target_ty>, Bug> {
                let orders = #interface::#m(&self.0, &ctx.source_root().value, event, ctx)?;
                ::core::result::Result::Ok(orders
                    .into_iter()
                    .fold(Commands::none(), |commands, (root, output)| {
                        let root = Uuid { value: root.into() };
                        match output {
                            #(#arms,)*
                        }
                    }))
            }
        }
    });
    // The one callback: the command refused and the refusal, read from the
    // refused answer as the target's choices, handed to the interface; it
    // answers one of the source's facts.
    let callback = refused
        .then(|| {
            let fact = format_ident!("{}", fact_choice(source)?);
            let refusal = format_ident!("{}", refusal_choice(target)?);
            Some(quote! {
                #[on_refused]
                fn refused(&self, refused: &Refused) -> ::core::result::Result<#fact, Bug> {
                    let (command, refusal) = #refusal::read(refused)?;
                    #interface::refused(&self.0, command, refusal, refused)
                }
            })
        })
        .flatten();
    quote! {
        #declaration

        #[policy(name = #policy_name, source = #source_ty, target = #target_ty)]
        impl #name {
            #(#methods)*
            #callback
        }
    }
}

fn projector_adapter(component: &Component, domain: &[(&str, &str)]) -> TokenStream {
    let adapter = Adapter::of(component, domain);
    let declaration = adapter.declaration(component);
    let Adapter {
        name, interface, ..
    } = &adapter;
    let projector_name = &component.component_name;
    let sources = &component.sources;
    let methods = component.events.iter().map(|event| {
        let ty = ident(event.name());
        let m = format_ident!("{}", method(event.name()));
        quote! {
            #[projects(#ty)]
            async fn #m(&mut self, event: &#ty, ctx: &ProjectionContext) -> Result<(), Report> {
                #interface::#m(&mut self.0, event, ctx)
                    .await
                    .map_err(|bug| Report::of(&bug))
            }
        }
    });
    quote! {
        #declaration

        #[projector(name = #projector_name, sources = [#(#sources),*])]
        impl #name {
            #(#methods)*
        }
    }
}

/// The generated `register(&mut Registry)`: every adapter of the
/// application declared in `files` registered with the runtime, the
/// `remote` domains (served by a child process) left to the host.
pub fn register(
    descriptor_set: &[u8],
    files: &[&str],
    remote: &[&str],
) -> Result<String, CodegenError> {
    registration(descriptor_set, files, &|c: &Component| {
        c.kind == ComponentKind::Policy || !remote.contains(&c.domain.as_str())
    })
}

/// The generated `register(&mut Registry)` of the components of `files`
/// named in `names` alone (by [`crate::component_name`]): what an
/// application's host config lists in process.
pub fn register_only(
    descriptor_set: &[u8],
    files: &[&str],
    names: &[&str],
) -> Result<String, CodegenError> {
    registration(descriptor_set, files, &|c: &Component| {
        names.contains(&c.component_name.as_str())
    })
}

fn registration(
    descriptor_set: &[u8],
    files: &[&str],
    keep: &dyn Fn(&Component) -> bool,
) -> Result<String, CodegenError> {
    let components = model::components(descriptor_set, files, ALL_KINDS)?;
    let registrations = application(&components)
        .into_iter()
        .filter(|c| keep(c))
        .map(|component| {
            let name = ident(&component.name);
            let ty = quote!(crate::adapters::#name);
            match component.kind {
                ComponentKind::Aggregate => quote!(registry.aggregate::<#ty>(#ty::default)?;),
                ComponentKind::ProcessManager => {
                    quote!(registry.process_manager::<#ty>(#ty::default)?;)
                }
                ComponentKind::Policy => quote!(registry.policy(#ty::default())?;),
                ComponentKind::Projector => quote!(registry.projector(#ty::default())?;),
            }
        })
        .collect::<Vec<_>>();
    // Every domain remote leaves the body empty: the registry is still
    // taken, for one signature, and bound so the build warns of nothing.
    let unused = registrations.is_empty().then(|| quote!(let _ = registry;));
    let root = root();
    let doc = [
        format!(" Registers every component of the project with {RUNTIME}, which dispatches"),
        " each through angzarr-router: an application calls this once, at startup.".to_string(),
        " Refused as the registry refuses a component: one registered twice, say.".to_string(),
    ];
    let source = file(quote! {
        #(#[doc = #doc])*
        pub fn register(registry: &mut #root::Registry) -> ::core::result::Result<(), #root::Misconfigured> {
            #unused
            #(#registrations)*
            ::core::result::Result::Ok(())
        }
    });
    Ok(crate::headed(
        ".rs",
        format!(
            "// @generated by angzarr-codegen from {}. Do not edit.\n\n{source}",
            files.join(", ")
        ),
    ))
}

/// The domain layer for marked-up components: the domain types, their wire
/// conversions and each one's `DomainMessage` impl, and each component's
/// `<Component>Command` and `<Component>Event` enums with their `Payload`
/// impls, the domain's payloads (the adapters name them to the markup);
/// nothing the markup generates itself (no `State` impl).
///
/// The wire types at each package's `wire` path are prost's, built with
/// angzarr's settings (`angzarr_build::wire`): a `bytes` field as
/// `Vec<u8>` (prost's default), a map as a `BTreeMap`
/// (`btree_map(["."])`), an enum field as its `i32`, a message field as an
/// `Option`, `prost::Name` on every message, and angzarr's own types
/// angzarr-proto's. The layer checks the first two where it is compiled: a
/// prost crate built otherwise fails with `angzarr-codegen-wire-bytes` or
/// `angzarr-codegen-wire-map` at the field.
///
/// Each component's interface ([`interface_source`]) is in its package's
/// layer, a policy's in its target's, beside the outputs it names: the
/// trait its adapter delegates to and the user implements, so an
/// implementation in another crate implements it. A domain type taking an
/// interface's name, or its facts' choice's, is refused
/// ([`ProtoError::InterfaceName`](crate::ProtoError::InterfaceName)).
pub fn markup_domain(
    descriptor_set: &[u8],
    files: &[&str],
    wire: &[(&str, &str)],
) -> Result<Vec<Generated>, CodegenError> {
    markup_domain_with(descriptor_set, files, wire, &[])
}

/// [`markup_domain`], each package's layer at the Rust path `domain` names
/// for it.
pub fn markup_domain_with(
    descriptor_set: &[u8],
    files: &[&str],
    wire: &[(&str, &str)],
    domain: &[(&str, &str)],
) -> Result<Vec<Generated>, CodegenError> {
    let mut packages = model::build_markup(descriptor_set, files)?;
    let components = model::components(descriptor_set, files, ALL_KINDS)?;
    for component in application(&components) {
        let methods = interface_methods(component, &components)?;
        let home = match component.kind {
            ComponentKind::Policy => component.target(&components)?,
            _ => component,
        };
        let layer = packages
            .entry(home.anchor.package_name().to_string())
            .or_default();
        let mut names = vec![component.interface.clone()];
        names.extend(fact_choice(component));
        names.extend(refusal_choice(component));
        let taken = layer
            .messages
            .iter()
            .map(|m| (m.full_name().to_string(), m.name().to_string()))
            .chain(
                layer
                    .enums
                    .iter()
                    .map(|e| (e.full_name().to_string(), crate::common::enum_name(e))),
            );
        for (message, local) in taken {
            if names.contains(&local) {
                return Err(crate::error::ProtoError::InterfaceName {
                    message,
                    name: local,
                    component: component.anchor.full_name().to_string(),
                }
                .into());
            }
        }
        layer.interfaces.push(interface_source(component, &methods));
    }
    generate(packages, wire, domain, crate::rust::markup_package)
}
