//! An angzarr-embedded application's startup, generated from its protos
//! and its host config (`angzarr scaffold app`):
//!
//! - the user's, written once and never rewritten: `angzarr.yaml`, the
//!   host config, seeded from the [`AppSeed`] (which component runs in
//!   which language, what is exposed, the retry and store settings) and the
//!   user's to edit from then on; `Cargo.toml`, the crate's manifest, with
//!   angzarr-embedded pinned at [`ANGZARR_EMBEDDED_REV`] (an existing one
//!   judged against it for drift); `src/main.rs` (the generated layer
//!   included from `OUT_DIR`, [`app_layer`], a module per in-process
//!   component's implementation, the driver's, and an async `main` running
//!   the host with the generated `register()`, which registers the
//!   in-process components the config lists); and `src/driver.rs`, what the
//!   application does once it is up;
//! - the CLI's, rewritten on every run from the protos, the host config
//!   and the manifest as they are on disk: `build.rs` (one call,
//!   `angzarr_build::app`, or `app_without_domain` when the manifest names
//!   a [`DomainCrate`]), which writes the generated layer every build.
//!
//! Every component is listed in the config, in-process policies and
//! projectors too, each by its name from the protos
//! ([`crate::component_name`]) and with its `kind`. The config is read
//! through angzarr-embedded-host-config, the schema angzarr-embedded-host
//! runs: stamped `schema_version`, read strictly, so an unknown key or
//! value, or a missing `kind`, is refused with the reader's error naming
//! its key path ([`AppError::Config`]).
//! [`lint_app`] checks it against the protos, and [`app`] refuses to
//! generate from one that has lints. Pure, as the rest of the crate:
//! descriptors and the config's text in, files out.

use std::fmt;

use angzarr_embedded_host_config::angzarr_yaml::{self, Value};
use angzarr_embedded_host_config::{
    ComponentConfig, ComponentKindConfig as Kind, Expose, FILE_NAME, HostConfig, KEY,
};
use coded_diagnostic::{Arg, Diagnostic, Report, code};
use serde::Deserialize;

use crate::common::snake;
use crate::error::{AppError, CodegenError, text, texts};
use crate::model::{self, Component, ComponentKind};
use crate::scaffold::{Owner, ScaffoldConfig, ScaffoldFile};
use crate::warnings::Warning;

/// The angzarr-embedded commit a generated application's `Cargo.toml` pins
/// (its kernel, which re-exports the diagnostics, its markup and its host): the one this generator's output is
/// written against.
pub const ANGZARR_EMBEDDED_REV: &str = "b9848330de43539a0a2d9ffbe960d59c2da0c67c";
/// Where [`ANGZARR_EMBEDDED_REV`] is fetched from.
pub const ANGZARR_EMBEDDED_GIT: &str = "https://github.com/angzarr-io/angzarr";
/// Where angzarr-build is fetched from when a seed names no other source.
pub const ANGZARR_BUILD_GIT: &str = "https://github.com/angzarr-io/angzarr";
/// The angzarr-build commit a generated application's `Cargo.toml` pins for
/// its build script: a full hash of a commit already on this repository's
/// main (a test holds it to that), since a commit cannot name itself. A
/// change to what the generated build script calls moves it, in a later
/// commit, to a pushed commit that has the change; a release moves it to
/// the release's commit.
pub const ANGZARR_BUILD_REV: &str = "b9848330de43539a0a2d9ffbe960d59c2da0c67c";

/// From how far a command may reach a domain through the host's listener
/// (the host config's `expose`); not exposed is the default and is not
/// written.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Exposure {
    /// `expose: loopback`: from this machine.
    Loopback,
    /// `expose: ip`: from the network.
    Ip,
}

/// The `retry` keys a seed sets; each unset one is left to the host's
/// default and not written.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct RetrySeed {
    /// `max_retries`.
    pub max_retries: Option<u32>,
    /// `max_race_runs`: 0 hands a lost race back as the store's conflict.
    pub max_race_runs: Option<u32>,
}

/// What `angzarr scaffold app` writes a new host config and manifest from.
/// Used only when they do not exist: an existing `angzarr.yaml` or
/// `Cargo.toml` is the user's, and the generated files follow it, not the
/// seed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AppSeed {
    /// The application's name, for the prose and, made a crate name, its
    /// package's.
    pub name: String,
    /// The protos' include directories, as the build script names them.
    pub includes: Vec<String>,
    /// The language of each component not in Rust, by its name in the
    /// application (its `component_name`), a domain's
    /// also by its domain: a Python one is served by a child process, the
    /// project of `angzarr scaffold python` (`dir` and its
    /// `angzarr-<project>` entry point). Every other component is Rust, in
    /// process.
    pub languages: Vec<(String, String)>,
    /// The components exposed through the host's listener, by name, and
    /// how far.
    pub expose: Vec<(String, Exposure)>,
    /// The `retry` keys to write.
    pub retry: RetrySeed,
    /// `store.in_memory: true`, else the default SQLite file.
    pub in_memory: bool,
    /// The manifest's angzarr-build build-dependency, a TOML value (`{ path
    /// = ".." }`); `None` is [`ANGZARR_BUILD_GIT`] at [`ANGZARR_BUILD_REV`].
    pub angzarr_build: Option<String>,
    /// The library crate whose domain layer the application uses, written
    /// into the manifest (`[package.metadata.angzarr.domain]`); `None`, the
    /// application generates its own.
    pub domain: Option<DomainCrate>,
}

/// A domain layer another crate generates (its build script running
/// angzarr-build's `wire` and `markup_domain`), which an application uses
/// instead of generating its own: the manifest's
/// `[package.metadata.angzarr.domain]`, `crate` and `wire`. Its generated
/// layer ([`app_layer`]) names `<path>::domain` as `crate::domain` and
/// `wire` as `crate::wire`,
/// and its build script generates the adapters, descriptors and
/// `register()` alone (`angzarr_build::app_without_domain`). The crate must
/// be a dependency of the application, and generated from the same protos.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DomainCrate {
    /// The crate's path, as Rust names it (`examples_proto`): its `domain`
    /// module is the domain layer.
    #[serde(rename = "crate")]
    pub path: String,
    /// Its wire types' module (`examples_proto::v1`), as the application's
    /// `crate::wire`; `None`, the application has no `crate::wire`.
    pub wire: Option<String>,
}

/// The startup of the application declared in `files`, paths relative to
/// the crate root: `angzarr.yaml`, `Cargo.toml`, `src/main.rs` and
/// `src/driver.rs` (the user's) and `build.rs` (the CLI's). `config` and
/// `manifest` are the host config and the manifest on disk, when there are
/// ones: the generated files are derived from them (the manifest's
/// [`DomainCrate`]), and the seed's are unused. Refused
/// ([`AppError::Refused`]) when the config, the seed's or the
/// user's, has lints ([`lint_app`]); a manifest that does not read, or
/// whose domain crate is malformed, is [`AppError::Manifest`], and a
/// domain crate's path that is no Rust path
/// [`AppError::DomainPath`].
pub fn app(
    descriptor_set: &[u8],
    files: &[&str],
    seed: &AppSeed,
    config: Option<&str>,
    manifest: Option<&str>,
) -> Result<Vec<ScaffoldFile>, CodegenError> {
    let components = model::components(descriptor_set, files, model::ALL_KINDS)?;
    let config_text = match config {
        Some(text) => text.to_string(),
        None => crate::headed(FILE_NAME, seeded_config(&components, seed)),
    };
    let lints = lint(&read(&config_text)?, &components);
    if !lints.is_empty() {
        return Err(AppError::Refused { lints }.into());
    }
    let host = read(&config_text)?;
    // The manifest as this CLI writes it: created when there is none, and
    // what an existing one's pins are judged against.
    let manifest_text = crate::headed("Cargo.toml", self::manifest(seed));
    let domain = match manifest {
        Some(text) => manifest_domain(text)?,
        None => seed.domain.clone(),
    };
    if let Some(domain) = &domain {
        for path in std::iter::once(&domain.path).chain(&domain.wire) {
            if syn::parse_str::<syn::Path>(path).is_err() {
                return Err(AppError::DomainPath { path: path.clone() }.into());
            }
        }
    }
    let user = |path: &str, source: String| {
        ScaffoldFile::new(String::new(), path.to_string(), source, Owner::User)
    };
    let generated = |path: &str, source: String| {
        ScaffoldFile::new(String::new(), path.to_string(), source, Owner::Generated)
    };
    // The host config on disk, the user's, passes through as it is; what
    // is generated here carries the licence header.
    Ok(vec![
        user(FILE_NAME, config_text),
        user("Cargo.toml", manifest_text),
        generated(
            "build.rs",
            crate::headed(".rs", build_script(files, &seed.includes, domain.as_ref())),
        ),
        user("src/driver.rs", crate::headed(".rs", driver(&seed.name))),
        user(
            "src/main.rs",
            crate::headed(".rs", main_rs(&seed.name, &components, &host)),
        ),
    ])
}

/// The domain layer an application's manifest (`Cargo.toml`) names
/// ([`DomainCrate`]), if it names one: what `angzarr_build`'s
/// `app_without_domain` reads to write the application's layer.
pub fn manifest_domain_crate(manifest: &str) -> Result<Option<DomainCrate>, CodegenError> {
    Ok(manifest_domain(manifest)?)
}

/// The application's generated layer (`angzarr-app.rs` in `OUT_DIR`, which
/// the user's `src/main.rs` includes): the wire types and the domain layer
/// of `packages`, or, over a domain crate, its domain layer and wire types
/// named in their place (neither when there are no packages and no domain
/// crate: the crate names its own); the adapters (their module carrying
/// its own allowances) and `register()`.
pub fn app_layer(packages: &[String], domain: Option<&DomainCrate>) -> String {
    let include = |suffix: &str| -> String {
        packages
            .iter()
            .map(|p| format!("    include!(concat!(env!(\"OUT_DIR\"), \"/{p}{suffix}\"));\n"))
            .collect()
    };
    let layer = match domain {
        None if packages.is_empty() => String::new(),
        None => format!(
            "pub mod wire {{\n{}}}\npub mod domain {{\n{}}}\n",
            include(".rs"),
            include(".domain.rs")
        ),
        Some(domain) => {
            let wire = domain
                .wire
                .as_ref()
                .map(|w| format!("pub use {w} as wire;\n"))
                .unwrap_or_default();
            format!("pub use {}::domain;\n{wire}", domain.path)
        }
    };
    crate::headed(
        ".rs",
        format!(
            "// @generated by angzarr-build from angzarr.yaml and the protos. Do not edit:\n\
             // every build rewrites it.\n\
             //\n\
             // The application's generated layer, which src/main.rs includes: the wire\n\
             // types, the domain layer, the adapters and `register()`.\n\
             \n\
             {layer}\
             include!(concat!(env!(\"OUT_DIR\"), \"/angzarr-adapters.rs\"));\n\
             include!(concat!(env!(\"OUT_DIR\"), \"/angzarr-register.rs\"));\n"
        ),
    )
}

/// The manifest's `[package.metadata.angzarr.domain]`, if it has one.
fn manifest_domain(text: &str) -> Result<Option<DomainCrate>, AppError> {
    Ok(manifest_angzarr(text)?.and_then(|a| a.domain))
}

/// The scaffold's configuration the host config `config` (`angzarr.yaml`)
/// gives ([`ScaffoldConfig`]): the components whose entry's
/// `scaffold.snapshots` is set, in the config's order. A config that does
/// not load is [`AppError::Config`].
pub fn host_scaffold(config: &str) -> Result<ScaffoldConfig, CodegenError> {
    let snapshots = read(config)?
        .components
        .into_iter()
        .filter(|c| c.scaffold.snapshots)
        .map(|c| c.name)
        .collect();
    Ok(ScaffoldConfig { snapshots })
}

/// The manifest's `[package.metadata.angzarr]`: angzarr-build's own
/// configuration, if it has any.
#[derive(Deserialize)]
struct Angzarr {
    domain: Option<DomainCrate>,
}

/// `text`, a manifest, read for its [`Angzarr`].
fn manifest_angzarr(text: &str) -> Result<Option<Angzarr>, AppError> {
    #[derive(Deserialize)]
    struct Manifest {
        package: Option<Package>,
    }
    #[derive(Deserialize)]
    struct Package {
        metadata: Option<Metadata>,
    }
    #[derive(Deserialize)]
    struct Metadata {
        angzarr: Option<Angzarr>,
    }
    let manifest: Manifest = toml::from_str(text).map_err(|e| AppError::Manifest {
        detail: e.message().to_string(),
    })?;
    Ok(manifest
        .package
        .and_then(|p| p.metadata)
        .and_then(|m| m.angzarr))
}

/// The domains a host config serves from a child process: every
/// `kind: domain` component not in Rust, or given a `launch`, by name.
/// What `angzarr_build::app` leaves out of the adapters and `register()`.
pub fn remote_domains(config: &str) -> Result<Vec<String>, CodegenError> {
    Ok(read(config)?
        .components
        .iter()
        .filter(|c| c.kind == Kind::Domain && !in_process(c))
        .map(|c| c.name.clone())
        .collect())
}

/// What a host config serves from a child process besides its domains:
/// every policy, projector or upcaster not in Rust, or given a `launch`, by
/// name.
pub fn child_components(config: &str) -> Result<Vec<String>, CodegenError> {
    Ok(read(config)?
        .components
        .iter()
        .filter(|c| c.kind != Kind::Domain && !in_process(c))
        .map(|c| c.name.clone())
        .collect())
}

/// The components a host config runs in process, by name: every one in
/// Rust with no `launch`. What `angzarr_build::app`'s `register()`
/// registers, those the protos declare.
pub fn in_process_components(config: &str) -> Result<Vec<String>, CodegenError> {
    Ok(read(config)?
        .components
        .iter()
        .filter(|c| in_process(c))
        .map(|c| c.name.clone())
        .collect())
}

/// What [`lint_app`] finds wrong with a host config against the protos.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum AppLint {
    /// A component no component of the protos is: a `domain` no aggregate
    /// or process manager is named, a `policy` no policy is.
    UnknownComponent { name: String, kind: String },
    /// Two components of one name, whatever their kinds.
    DuplicateComponent { name: String },
    /// A domain an aggregate or process manager of the protos owns, which
    /// the config does not list.
    UnlistedDomain { domain: String },
    /// A policy the config lists whose source domain no component of the
    /// application owns: nothing would ever trigger it.
    PolicySourceAbsent { policy: String, domain: String },
    /// A policy the config lists whose target domain no component of the
    /// application owns, in process or declared remote: its commands would
    /// go nowhere.
    PolicyTargetAbsent { policy: String, domain: String },
    /// An `expose` on what is not an aggregate: a policy, projector or
    /// upcaster (`kind` its config kind) or a process manager.
    NotExposable { name: String, kind: String },
    /// A component in a language the host has no launch for, and no
    /// `launch`.
    UnknownLanguage { name: String, language: String },
    /// A `launch` with no program.
    EmptyLaunch { name: String },
}

/// Its code and args (decision 14.37), like every error here.
impl fmt::Display for AppLint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        coded_diagnostic::display(self, f)
    }
}

impl Diagnostic for AppLint {
    fn code(&self) -> &'static str {
        match self {
            AppLint::UnknownComponent { .. } => code!("app-unknown-component"),
            AppLint::DuplicateComponent { .. } => code!("app-duplicate-component"),
            AppLint::UnlistedDomain { .. } => code!("app-unlisted-domain"),
            AppLint::PolicySourceAbsent { .. } => code!("app-policy-source-absent"),
            AppLint::PolicyTargetAbsent { .. } => code!("app-policy-target-absent"),
            AppLint::NotExposable { .. } => code!("app-not-exposable"),
            AppLint::UnknownLanguage { .. } => code!("app-unknown-language"),
            AppLint::EmptyLaunch { .. } => code!("app-empty-launch"),
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match self {
            AppLint::UnknownComponent { name, kind } | AppLint::NotExposable { name, kind } => {
                texts![name, kind]
            }
            AppLint::DuplicateComponent { name } | AppLint::EmptyLaunch { name } => texts![name],
            AppLint::UnlistedDomain { domain } => texts![domain],
            AppLint::PolicySourceAbsent { policy, domain }
            | AppLint::PolicyTargetAbsent { policy, domain } => texts![policy, domain],
            AppLint::UnknownLanguage { name, language } => texts![name, language],
        }
    }
}

/// One value of every lint, for the catalog test.
pub fn app_lint_samples() -> Vec<Box<dyn Diagnostic>> {
    vec![
        Box::new(AppLint::UnknownComponent {
            name: text("name"),
            kind: text("kind"),
        }),
        Box::new(AppLint::DuplicateComponent { name: text("name") }),
        Box::new(AppLint::UnlistedDomain {
            domain: text("domain"),
        }),
        Box::new(AppLint::PolicySourceAbsent {
            policy: text("policy"),
            domain: text("domain"),
        }),
        Box::new(AppLint::PolicyTargetAbsent {
            policy: text("policy"),
            domain: text("domain"),
        }),
        Box::new(AppLint::NotExposable {
            name: text("name"),
            kind: text("kind"),
        }),
        Box::new(AppLint::UnknownLanguage {
            name: text("name"),
            language: text("language"),
        }),
        Box::new(AppLint::EmptyLaunch { name: text("name") }),
    ]
}

/// What `angzarr lint app` found: the config's lints, each a failure, and
/// the protos' declaration warnings, which leave its exit status as it is.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Linted {
    pub lints: Vec<AppLint>,
    pub warnings: Vec<Warning>,
}

/// The lints of `config`, a host config's YAML, against the components the
/// protos declare in `files`, in the config's order and then the protos',
/// and the protos' own warnings ([`crate::warnings`]). A config that does
/// not read as a host config is refused ([`AppError::Config`]), as are
/// declarations the contract forbids.
pub fn lint_app(
    config: &str,
    descriptor_set: &[u8],
    files: &[&str],
) -> Result<Linted, CodegenError> {
    let (components, mut warnings) = model::declarations(descriptor_set, files)?;
    warnings.extend(crate::warnings::of_all(&components));
    Ok(Linted {
        lints: lint(&read(config)?, &components),
        warnings,
    })
}

// ------------------------------------------------------------ the config

/// `text` read as angzarr-embedded-host reads its config: through the
/// schema's loader, strictly, the reader's error kept as the cause.
pub(crate) fn read(text: &str) -> Result<HostConfig, AppError> {
    HostConfig::from_yaml(text, FILE_NAME).map_err(|e| AppError::Config {
        cause: Report::of(&e),
    })
}

/// In the application's process: in Rust, with no `launch`.
fn in_process(component: &ComponentConfig) -> bool {
    component.launch.is_none() && component.language == "rust"
}

/// A language the host launches with no `launch` given.
fn has_default_launch(language: &str) -> bool {
    language == "python"
}

/// The kind a component of the protos is listed as in a host config.
fn config_kind(kind: ComponentKind) -> Kind {
    match kind {
        ComponentKind::Aggregate | ComponentKind::ProcessManager => Kind::Domain,
        ComponentKind::Policy => Kind::Policy,
        ComponentKind::Projector => Kind::Projector,
    }
}

/// The component of `components` a config entry of `kind` named `name`
/// lists: none for an upcaster, which no proto declares.
fn listed<'a>(components: &'a [Component], kind: Kind, name: &str) -> Option<&'a Component> {
    components
        .iter()
        .find(|c| config_kind(c.kind) == kind && c.component_name == name)
}

fn lint(config: &HostConfig, components: &[Component]) -> Vec<AppLint> {
    let mut lints = Vec::new();
    // The domains the application has: each listed domain's, by the
    // protos' component of its name, or its name.
    let domains: Vec<&str> = config
        .components
        .iter()
        .filter(|c| c.kind == Kind::Domain)
        .map(|c| {
            listed(components, Kind::Domain, &c.name).map_or(c.name.as_str(), |d| d.domain.as_str())
        })
        .collect();
    for (i, listed) in config.components.iter().enumerate() {
        if config.components[..i].iter().any(|c| c.name == listed.name) {
            lints.push(AppLint::DuplicateComponent {
                name: listed.name.clone(),
            });
            continue;
        }
        let unknown = || AppLint::UnknownComponent {
            name: listed.name.clone(),
            kind: listed.kind.as_str().to_string(),
        };
        match listed.kind {
            Kind::Domain => match self::listed(components, Kind::Domain, &listed.name) {
                None => lints.push(unknown()),
                Some(pm)
                    if pm.kind == ComponentKind::ProcessManager
                        && listed.expose != Expose::None =>
                {
                    lints.push(AppLint::NotExposable {
                        name: listed.name.clone(),
                        kind: "process_manager".to_string(),
                    });
                }
                Some(_) => {}
            },
            Kind::Policy => match self::listed(components, Kind::Policy, &listed.name) {
                None => lints.push(unknown()),
                Some(policy) => {
                    if !domains.contains(&policy.input_domain.as_str()) {
                        lints.push(AppLint::PolicySourceAbsent {
                            policy: listed.name.clone(),
                            domain: policy.input_domain.clone(),
                        });
                    }
                    if !domains.contains(&policy.output_domain.as_str()) {
                        lints.push(AppLint::PolicyTargetAbsent {
                            policy: listed.name.clone(),
                            domain: policy.output_domain.clone(),
                        });
                    }
                }
            },
            // A projector or an upcaster may be written by hand: its name
            // is its own.
            Kind::Projector | Kind::Upcaster => {}
        }
        if listed.kind != Kind::Domain && listed.expose != Expose::None {
            lints.push(AppLint::NotExposable {
                name: listed.name.clone(),
                kind: listed.kind.as_str().to_string(),
            });
        }
        match &listed.launch {
            Some(launch) if launch.is_empty() => lints.push(AppLint::EmptyLaunch {
                name: listed.name.clone(),
            }),
            None if listed.language != "rust" && !has_default_launch(&listed.language) => lints
                .push(AppLint::UnknownLanguage {
                    name: listed.name.clone(),
                    language: listed.language.clone(),
                }),
            _ => {}
        }
    }
    for component in components.iter().filter(|c| c.kind.is_domain()) {
        let listed = config
            .components
            .iter()
            .any(|c| c.kind == Kind::Domain && c.name == component.component_name);
        if !listed {
            lints.push(AppLint::UnlistedDomain {
                domain: component.component_name.clone(),
            });
        }
    }
    lints
}

// ----------------------------------------------------------- the seed

/// One component's entry in a seeded host config.
struct Entry {
    name: String,
    kind: Kind,
    language: String,
    /// A Python child's project: `dir` and `angzarr-<project>`.
    project: Option<String>,
}

impl Entry {
    /// `component`'s entry in its seeded language: by its name, a domain's
    /// also by its domain; its Python project its domain's, else its name
    /// (as `scaffold python --domain` names it).
    fn of(component: &Component, seed: &AppSeed) -> Entry {
        let kind = config_kind(component.kind);
        let by = if kind == Kind::Domain {
            &component.domain
        } else {
            &component.component_name
        };
        let language = seed
            .languages
            .iter()
            .find(|(name, _)| name == &component.component_name || name == by)
            .map_or("rust", |(_, language)| language.as_str());
        Entry {
            name: component.component_name.clone(),
            kind,
            language: language.to_string(),
            project: (language == "python").then(|| by.clone()),
        }
    }

    /// A domain the seed names that the protos lack, listed for the lint to
    /// name.
    fn unknown(name: &str, language: &str) -> Entry {
        Entry {
            name: name.to_string(),
            kind: Kind::Domain,
            language: language.to_string(),
            project: None,
        }
    }

    /// The entry as the config's `component` item, exposed as the seed
    /// says.
    fn value(&self, seed: &AppSeed) -> Value {
        let mut fields = vec![
            pair("name", Value::scalar(&self.name)),
            pair("kind", Value::scalar(self.kind.as_str())),
            pair("language", Value::scalar(&self.language)),
        ];
        if let Some(project) = &self.project {
            fields.push(pair("dir", Value::scalar(project)));
            let entry = Value::scalar(format!("angzarr-{project}"));
            fields.push(pair("entry", Value::Seq(vec![entry])));
        }
        if let Some((_, exposure)) = seed.expose.iter().find(|(n, _)| *n == self.name) {
            let how = match exposure {
                Exposure::Loopback => Expose::Loopback,
                Exposure::Ip => Expose::Ip,
            };
            fields.push(pair("expose", Value::scalar(how.as_str())));
        }
        Value::Map(fields)
    }
}

/// The host config a seed makes for `components` ([`seeded_entries`]),
/// then `retry` with the keys seeded, and `store`; stamped at the current
/// version.
fn seeded_config(components: &[Component], seed: &AppSeed) -> String {
    let header = format!(
        "# The host config of {}: every component, its kind, the language it\n\
         # is written in and, for one served by a child process, how it is\n\
         # launched. Written once by `angzarr scaffold app` and yours from then\n\
         # on: each run regenerates build.rs and src/main.rs from it, main.rs\n\
         # registering the in-process components it lists, and `angzarr lint\n\
         # app` checks it against the protos. Its shape is versioned by\n\
         # `{KEY}`.\n",
        seed.name
    );
    let entries = seeded_entries(components, seed)
        .iter()
        .map(|entry| entry.value(seed))
        .collect();
    let current = angzarr_embedded_host_config::CURRENT.to_string();
    let mut root = vec![
        pair(KEY, Value::scalar(&current)),
        pair("component", Value::Seq(entries)),
    ];
    let retry: Vec<(String, Value)> = [
        ("max_retries", seed.retry.max_retries),
        ("max_race_runs", seed.retry.max_race_runs),
    ]
    .into_iter()
    .filter_map(|(key, value)| value.map(|v| pair(key, Value::scalar(v.to_string()))))
    .collect();
    if !retry.is_empty() {
        root.push(pair("retry", Value::Map(retry)));
    }
    let store = if seed.in_memory {
        pair("in_memory", Value::scalar("true"))
    } else {
        pair("path", Value::scalar("angzarr.db"))
    };
    root.push(pair("store", Value::Map(vec![store])));
    // Collections of scalars nested three deep (a component's `entry`) are
    // written on one line.
    header + &angzarr_yaml::write(&Value::Map(root), 3)
}

/// A key and its value, as a map's field.
fn pair(key: &str, value: Value) -> (String, Value) {
    (key.to_string(), value)
}

/// The entries a seed makes for `components`, in the config's order: every
/// aggregate and process manager (Rust, or a Python child for a domain the
/// seed names); every policy both of whose domains are among them; every
/// projector all of whose sources are (a Python child's too: the host
/// records its events), each in Rust; then each name the seed gives that
/// is none of them, and each exposure naming nothing listed, for the lint
/// to name.
fn seeded_entries(components: &[Component], seed: &AppSeed) -> Vec<Entry> {
    let runs = |c: &Component| match c.kind {
        ComponentKind::Aggregate | ComponentKind::ProcessManager => true,
        ComponentKind::Policy => c.is_connected(components),
        ComponentKind::Projector => {
            !c.sources.is_empty()
                && c.sources
                    .iter()
                    .all(|s| model::owner(components, s).is_some())
        }
    };
    let mut entries: Vec<Entry> = [Kind::Domain, Kind::Policy, Kind::Projector]
        .into_iter()
        .flat_map(|kind| {
            components
                .iter()
                .filter(move |c| config_kind(c.kind) == kind)
        })
        .filter(|c| runs(c))
        .map(|c| Entry::of(c, seed))
        .collect();
    for (name, language) in &seed.languages {
        let known =
            entries.iter().any(|e| &e.name == name) || model::owner(components, name).is_some();
        if !known {
            entries.push(Entry::unknown(name, language));
        }
    }
    for (name, _) in &seed.expose {
        if !entries.iter().any(|e| &e.name == name) {
            entries.push(Entry::unknown(name, "rust"));
        }
    }
    entries
}

/// `name` as a Cargo package name: ASCII letters and digits kept, lower
/// case, every other run of characters one `-`.
fn package_name(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if !out.is_empty() && !out.ends_with('-') {
            out.push('-');
        }
    }
    out.trim_end_matches('-').to_string()
}

/// The seed's manifest: angzarr-embedded's crates at
/// [`ANGZARR_EMBEDDED_REV`], prost and tokio, and angzarr-build for the
/// build script.
fn manifest(seed: &AppSeed) -> String {
    let embedded =
        format!("{{ git = \"{ANGZARR_EMBEDDED_GIT}\", rev = \"{ANGZARR_EMBEDDED_REV}\" }}");
    let build = seed.angzarr_build.clone().unwrap_or_else(|| {
        format!("{{ git = \"{ANGZARR_BUILD_GIT}\", rev = \"{ANGZARR_BUILD_REV}\" }}")
    });
    format!(
        "# The manifest of {name} on angzarr-embedded. Written once by `angzarr\n\
         # scaffold app` and yours from then on: add your logic's dependencies.\n\
         [package]\n\
         name = \"{package}\"\n\
         version = \"0.1.0\"\n\
         edition = \"2024\"\n\
         publish = false\n\
         {domain}\
         \n\
         [dependencies]\n\
         angzarr-embedded = {embedded}\n\
         angzarr-embedded-host = {embedded}\n\
         angzarr-embedded-macros = {embedded}\n\
         prost = \"0.14\"\n\
         # The App is async, on the caller's runtime: tokio's.\n\
         tokio = {{ version = \"1\", features = [\"macros\", \"rt-multi-thread\"] }}\n\
         \n\
         [build-dependencies]\n\
         angzarr-build = {build}\n",
        name = seed.name,
        package = package_name(&seed.name),
        domain = seed.domain.as_ref().map(domain_table).unwrap_or_default(),
    )
}

/// The manifest's table naming `domain`, a blank line before it.
fn domain_table(domain: &DomainCrate) -> String {
    let wire = domain
        .wire
        .as_ref()
        .map(|w| format!("wire = {w:?}\n"))
        .unwrap_or_default();
    format!(
        "\n# The domain layer is {path}'s (its `domain` module), not generated\n\
         # here: add {path} to [dependencies].\n\
         [package.metadata.angzarr.domain]\n\
         crate = {path:?}\n{wire}",
        path = domain.path
    )
}

/// The generated build script: one call, `angzarr_build::app`, or over a
/// domain crate `angzarr_build::app_without_domain`.
fn build_script(files: &[&str], includes: &[String], domain: Option<&DomainCrate>) -> String {
    let list = |items: &[String]| -> String {
        let one = format!(
            "&[{}]",
            items
                .iter()
                .map(|i| format!("{i:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        if one.len() <= 70 {
            return one;
        }
        let lines: String = items.iter().map(|i| format!("    {i:?},\n")).collect();
        format!("&[\n{lines}]")
    };
    let files: Vec<String> = files.iter().map(|f| f.to_string()).collect();
    let (doc, call) = match domain {
        None => (
            "//! The application's generated layer, into OUT_DIR (`angzarr-app.rs`,\n\
             //! which src/main.rs includes): the wire types, the markup domain layer,\n\
             //! the adapters, the descriptors and `register()`,\n"
                .to_string(),
            "app",
        ),
        Some(domain) => (
            format!(
                "//! The application's generated layer, into OUT_DIR (`angzarr-app.rs`,\n\
                 //! which src/main.rs includes), over {}'s wire types and domain layer\n\
                 //! (Cargo.toml's [package.metadata.angzarr.domain]): the adapters, the\n\
                 //! descriptors and `register()`,\n",
                domain.path
            ),
            "app_without_domain",
        ),
    };
    format!(
        "// @generated by `angzarr scaffold app`. Do not edit: every run rewrites it.\n\
         \n\
         {doc}\
         //! from the protos and angzarr.yaml; a domain angzarr.yaml serves from a\n\
         //! child process is its command and event sets, and `register()`\n\
         //! registers the in-process components angzarr.yaml lists.\n\
         \n\
         const PROTOS: &[&str] = {};\n\
         const INCLUDES: &[&str] = {};\n\
         \n\
         fn main() -> Result<(), angzarr_build::BuildError> {{\n    \
             angzarr_build::{call}(\"angzarr.yaml\", PROTOS, INCLUDES)\n\
         }}\n",
        list(&files),
        list(includes)
    )
}

/// The user's driver, a stub.
fn driver(name: &str) -> String {
    format!(
        "//! What {name} does once it is up. Written once by `angzarr scaffold app`\n\
         //! and yours: send commands with `app.handle(book, &sampled).await`, read\n\
         //! a stream with `app.events(&cover).await`.\n\
         \n\
         use angzarr_embedded_host::App;\n\
         \n\
         pub async fn drive(app: &App) -> Result<(), Box<dyn std::error::Error>> {{\n    \
             let _ = app;\n    \
             todo!(\"send the application's commands\")\n\
         }}\n"
    )
}

/// The user's main, written once: the application's generated layer
/// (`angzarr-app.rs`, [`app_layer`]), a module per in-process component's
/// implementation (each listed one the protos declare), the driver, and an
/// async `main`.
fn main_rs(name: &str, components: &[Component], config: &HostConfig) -> String {
    let mut modules: Vec<String> = vec!["driver".to_string()];
    for listed in config.components.iter().filter(|c| in_process(c)) {
        if let Some(component) = self::listed(components, listed.kind, &listed.name) {
            modules.push(format!("{}_impl", snake(&component.name)));
        }
    }
    modules.sort_unstable();
    modules.dedup();
    let modules: String = modules.iter().map(|m| format!("pub mod {m};\n")).collect();
    format!(
        "//! {name} on angzarr-embedded: the Rust components in process, every\n\
         //! component angzarr.yaml serves from a child process launched by the host,\n\
         //! the in-process ones it lists registered by the generated `register()`,\n\
         //! then the driver (`src/driver.rs`). Written once by `angzarr scaffold app`\n\
         //! and yours from then on: add your modules here. The generated layer (the\n\
         //! wire types, the domain layer, the adapters and `register()`) is\n\
         //! `angzarr-app.rs`, which build.rs writes into OUT_DIR every build.\n\
         \n\
         include!(concat!(env!(\"OUT_DIR\"), \"/angzarr-app.rs\"));\n\
         \n\
         {modules}\
         \n\
         /// Runs the application, then its driver, on tokio's runtime (the App is\n\
         /// async, on the caller's). Dropping the application stops the children.\n\
         #[tokio::main]\n\
         async fn main() -> Result<(), Box<dyn std::error::Error>> {{\n    \
             let app = angzarr_embedded_host::run(\"angzarr.yaml\", register).await?;\n    \
             driver::drive(&app).await\n\
         }}\n",
    )
}
