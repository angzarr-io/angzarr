//! Helpers for `build.rs`: angzarr's settings for prost types ([`wire`]),
//! and the generated domain layer for marked-up components
//! ([`markup_domain`]), adapters ([`adapters`]), descriptors
//! ([`descriptors`]) and component registration ([`register`]), written
//! to `OUT_DIR`, never into the source tree. And the one thing written into
//! the source tree, once, as the user's: the markup scaffold
//! ([`scaffold_into`], behind `angzarr scaffold rust`).
//!
//! These read the build environment themselves, so a calling build script
//! needs none of it: a crate whose lints forbid `std::env` in its own code
//! (a pure core) has a build script that is one call.
//!
//! Proto paths are import names, relative to one of `includes` (as a
//! `.proto` file's own `import` names them). angzarr's protos are always
//! importable: they come from angzarr-proto, never from a vendored copy.

use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

use angzarr_diagnostic::{Arg, Diagnostic, catalog, code};

pub use angzarr_codegen::{
    ANGZARR_BUILD_GIT, ANGZARR_BUILD_REV, ANGZARR_EMBEDDED_REV, AppLint, AppSeed, DomainCrate,
    Existing, Exposure, Linted, Notes, PinDrift, ProtoInput, PythonApp, RetrySeed, ScaffoldConfig,
    Warning,
};

/// This crate's en-US messages: its package name, and its catalog, each id
/// under that name (written by the build, `angzarr_diagnostic::build`).
pub const EN_US: (&str, &str) = (
    env!("CARGO_PKG_NAME"),
    include_str!(concat!(env!("OUT_DIR"), "/en-US.ftl")),
);

/// The prost types of `protos`, by package (`<package>.rs` in `OUT_DIR`),
/// with angzarr's settings: `prost::Name` on every message, ordered maps,
/// and angzarr's own types taken from angzarr-proto; and `angzarr-wire.rs`,
/// their modules nested as their packages are ([`wire_into`]).
pub fn wire(protos: &[&str], includes: &[&str]) -> Result<(), BuildError> {
    wire_into(&out_dir()?, protos, includes)
}

/// The domain layer for marked-up components (angzarr-embedded-macros), by package
/// (`<package>.domain.rs` in `OUT_DIR`): the domain types, their wire
/// conversions and `DomainMessage` impls, and each component's command and
/// event enums with their `Payload` impls (see
/// `angzarr_codegen::markup_domain`). `wire` names each package's prost
/// types, which must be built with [`wire`]'s settings (bytes as
/// `Vec<u8>`, maps as `BTreeMap`): the layer refuses others where it is
/// compiled, with `angzarr-codegen-wire-bytes` or `angzarr-codegen-wire-map`.
pub fn markup_domain(
    protos: &[&str],
    includes: &[&str],
    wire: &[(&str, &str)],
) -> Result<(), BuildError> {
    markup_domain_into(&out_dir()?, protos, includes, wire)
}

/// [`markup_domain`], into `out`.
pub fn markup_domain_into(
    out: &Path,
    protos: &[&str],
    includes: &[&str],
    wire: &[(&str, &str)],
) -> Result<(), BuildError> {
    markup_domain_with_into(out, protos, includes, wire, &[])
}

/// [`markup_domain`] across packages: `domain` names the Rust path each
/// package's layer is included at (`<package>.domain.rs`), so a type of
/// another package is that package's domain type, never a copy. A package
/// whose types another holds is generated too, a package of types alone
/// included, and refused without a path here
/// (`angzarr-codegen-no-domain-path`).
pub fn markup_domain_with(
    protos: &[&str],
    includes: &[&str],
    wire: &[(&str, &str)],
    domain: &[(&str, &str)],
) -> Result<(), BuildError> {
    markup_domain_with_into(&out_dir()?, protos, includes, wire, domain)
}

/// [`markup_domain_with`], into `out`.
pub fn markup_domain_with_into(
    out: &Path,
    protos: &[&str],
    includes: &[&str],
    wire: &[(&str, &str)],
    domain: &[(&str, &str)],
) -> Result<(), BuildError> {
    let bytes = build_descriptors(out, protos, includes)?;
    let generated = angzarr_codegen::markup_domain_with(&bytes, protos, wire, domain)
        .map_err(BuildError::Codegen)?;
    write_domain(out, generated)
}

/// The proto3 JSON of the prost types of `protos` (`angzarr-json.rs` in
/// `OUT_DIR`, see `angzarr_codegen::json`), for the wire crate to
/// `include!` in a module of its own: `wire` names each package's Rust
/// path, as [`markup_domain`]'s does. Opt-in: [`wire`] alone generates no
/// JSON. The wire crate depends on `serde`, and on `prost-types` and
/// `angzarr-proto` (the well-known types and angzarr's UUID).
pub fn json(protos: &[&str], includes: &[&str], wire: &[(&str, &str)]) -> Result<(), BuildError> {
    json_into(&out_dir()?, protos, includes, wire)
}

/// [`json`], into `out`.
pub fn json_into(
    out: &Path,
    protos: &[&str],
    includes: &[&str],
    wire: &[(&str, &str)],
) -> Result<(), BuildError> {
    let bytes = build_descriptors(out, protos, includes)?;
    let source = angzarr_codegen::json(&bytes, protos, wire).map_err(BuildError::Codegen)?;
    let path = out.join("angzarr-json.rs");
    std::fs::write(&path, source).map_err(|e| io(&path, &e))
}

/// The generated `register(&mut Registry)` for every component in
/// `protos` (`angzarr-register.rs` in `OUT_DIR`), for the crate root to
/// `include!`.
pub fn register(protos: &[&str], includes: &[&str], remote: &[&str]) -> Result<(), BuildError> {
    register_into(&out_dir()?, protos, includes, remote)
}

/// [`register`], into `out`, the `remote` domains (served by a child
/// process) left to the host.
pub fn register_into(
    out: &Path,
    protos: &[&str],
    includes: &[&str],
    remote: &[&str],
) -> Result<(), BuildError> {
    let bytes = build_descriptors(out, protos, includes)?;
    let source = angzarr_codegen::register(&bytes, protos, remote).map_err(BuildError::Codegen)?;
    let path = out.join("angzarr-register.rs");
    std::fs::write(&path, source).map_err(|e| io(&path, &e))
}

/// The stand-in of every domain in `protos` served by a child process
/// (`angzarr-stand-ins.rs` in `OUT_DIR`, see `angzarr_codegen::stand_ins`):
/// its command set and its event set as a hand-written Rust policy names
/// them, generated from its proto, for the crate root to `include!`.
pub fn stand_ins(protos: &[&str], includes: &[&str]) -> Result<(), BuildError> {
    stand_ins_into(&out_dir()?, protos, includes)
}

/// [`stand_ins`], into `out`.
pub fn stand_ins_into(out: &Path, protos: &[&str], includes: &[&str]) -> Result<(), BuildError> {
    let bytes = build_descriptors(out, protos, includes)?;
    let source = angzarr_codegen::stand_ins(&bytes, protos).map_err(BuildError::Codegen)?;
    let path = out.join("angzarr-stand-ins.rs");
    std::fs::write(&path, source).map_err(|e| io(&path, &e))
}

/// The generated adapters of every component in `protos`
/// (`angzarr-adapters.rs` in `OUT_DIR`), the module `adapters` with the
/// allowances it needs, for the crate root to `include!` (see
/// `angzarr_codegen::adapters`).
pub fn adapters(protos: &[&str], includes: &[&str], remote: &[&str]) -> Result<(), BuildError> {
    adapters_into(&out_dir()?, protos, includes, remote)
}

/// [`adapters`], into `out`.
pub fn adapters_into(
    out: &Path,
    protos: &[&str],
    includes: &[&str],
    remote: &[&str],
) -> Result<(), BuildError> {
    adapters_with_into(out, protos, includes, remote, &[])
}

/// [`adapters`], each component's adapter reading its package's domain
/// layer at the Rust path `domain` names for it, as [`markup_domain_with`]
/// places them (see `angzarr_codegen::adapters_with`).
pub fn adapters_with(
    protos: &[&str],
    includes: &[&str],
    remote: &[&str],
    domain: &[(&str, &str)],
) -> Result<(), BuildError> {
    adapters_with_into(&out_dir()?, protos, includes, remote, domain)
}

/// [`adapters_with`], into `out`.
pub fn adapters_with_into(
    out: &Path,
    protos: &[&str],
    includes: &[&str],
    remote: &[&str],
    domain: &[(&str, &str)],
) -> Result<(), BuildError> {
    let bytes = build_descriptors(out, protos, includes)?;
    let source = angzarr_codegen::adapters_with(&bytes, protos, remote, domain)
        .map_err(BuildError::Codegen)?;
    let path = out.join("angzarr-adapters.rs");
    std::fs::write(&path, source).map_err(|e| io(&path, &e))
}

/// A diagnostic in English where a build script shows it: its message in
/// the catalog `.0`, filled with its args ([`catalog::render`]).
struct English<'a>(&'static str, &'a dyn Diagnostic);

impl fmt::Display for English<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        catalog::render(self.0, self.1, f)
    }
}

/// Each component's generated `ComponentDescriptor` in `protos`
/// (`angzarr-descriptors.rs` in `OUT_DIR`, see
/// `angzarr_codegen::descriptors_rs`), the crate's version
/// (`CARGO_PKG_VERSION`) its rules version, the `remote` domains (served
/// by a child process) left to their process. Unversioned, the descriptors'
/// rules version is empty, which the coordinator refuses.
pub fn descriptors(protos: &[&str], includes: &[&str], remote: &[&str]) -> Result<(), BuildError> {
    descriptors_into(&out_dir()?, protos, includes, remote, &rules_version())
}

/// The build's version: the crate's.
fn rules_version() -> String {
    std::env::var("CARGO_PKG_VERSION").unwrap_or_default()
}

/// [`descriptors`], into `out`, with `rules_version`.
pub fn descriptors_into(
    out: &Path,
    protos: &[&str],
    includes: &[&str],
    remote: &[&str],
    rules_version: &str,
) -> Result<(), BuildError> {
    let bytes = build_descriptors(out, protos, includes)?;
    let source = angzarr_codegen::descriptors_rs(&bytes, protos, remote, rules_version)
        .map_err(BuildError::Codegen)?;
    let path = out.join("angzarr-descriptors.rs");
    std::fs::write(&path, source).map_err(|e| io(&path, &e))
}

/// Every component of `protos` with its name in an application (see
/// `angzarr_codegen::component_names`): what a build script writes for
/// hand-written code and its config to name components as the generator
/// does.
pub fn component_names(
    protos: &[&str],
    includes: &[&str],
) -> Result<Vec<angzarr_codegen::ComponentName>, BuildError> {
    let bytes = scaffold_descriptors(protos, includes)?;
    angzarr_codegen::component_names(&bytes, protos).map_err(BuildError::Codegen)
}

/// An angzarr-embedded application's whole generated layer, for the
/// build script `angzarr scaffold app` writes: from its host config
/// (`config`, relative to the crate) and its protos, the wire types, the
/// markup domain layer (each package's prost types at `crate::wire`), the
/// adapters, the components' descriptors and `register()`, which registers
/// the in-process components the config lists; a domain the config serves
/// from a child process is its command and event sets
/// (`CommandTarget`, `EventSource`). Cargo reruns it when the config
/// changes.
pub fn app(config: &str, protos: &[&str], includes: &[&str]) -> Result<(), BuildError> {
    app_into(&out_dir()?, &manifest_dir().join(config), protos, includes)
}

/// [`app`] over a domain crate's domain layer (the manifest's
/// [`DomainCrate`]): the adapters, the descriptors and `register()`, and
/// no wire types or domain layer of the application's own; the build
/// script `angzarr scaffold app` writes for an application whose manifest
/// names one.
pub fn app_without_domain(
    config: &str,
    protos: &[&str],
    includes: &[&str],
) -> Result<(), BuildError> {
    app_without_domain_into(&out_dir()?, &manifest_dir().join(config), protos, includes)
}

/// The crate's directory, as Cargo gives it to the build script.
fn manifest_dir() -> PathBuf {
    std::env::var_os("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_default()
}

/// [`app`], into `out`, the host config at `config`.
pub fn app_into(
    out: &Path,
    config: &Path,
    protos: &[&str],
    includes: &[&str],
) -> Result<(), BuildError> {
    app_layer_into(out, config, protos, includes, true)
}

/// [`app_without_domain`], into `out`, the host config at `config` and the
/// manifest beside it (its domain crate).
pub fn app_without_domain_into(
    out: &Path,
    config: &Path,
    protos: &[&str],
    includes: &[&str],
) -> Result<(), BuildError> {
    app_layer_into(out, config, protos, includes, false)
}

/// The application's layer into `out`; its own wire types and domain
/// layer too when `domain`.
fn app_layer_into(
    out: &Path,
    config: &Path,
    protos: &[&str],
    includes: &[&str],
    domain: bool,
) -> Result<(), BuildError> {
    println!("cargo:rerun-if-changed={}", config.display());
    let text = std::fs::read_to_string(config).map_err(|e| io(config, &e))?;
    let bytes = build_descriptors(out, protos, includes)?;
    let names = angzarr_codegen::component_names(&bytes, protos).map_err(BuildError::Codegen)?;
    // The config names components; a remote domain's command and event
    // sets are by its stream domain, and a policy or projector a child
    // serves is left to it by its name.
    let mut remote: Vec<String> = angzarr_codegen::remote_domains(&text)
        .map_err(BuildError::Codegen)?
        .into_iter()
        .map(|name| {
            names
                .iter()
                .find(|c| c.name == name && !c.domain.is_empty())
                .map_or(name, |c| c.domain.clone())
        })
        .collect();
    remote.extend(angzarr_codegen::child_components(&text).map_err(BuildError::Codegen)?);
    let remote: Vec<&str> = remote.iter().map(String::as_str).collect();
    let listed = angzarr_codegen::in_process_components(&text).map_err(BuildError::Codegen)?;
    let listed: Vec<&str> = listed.iter().map(String::as_str).collect();
    if domain {
        let packages = packages(&names);
        let wire: Vec<(&str, &str)> = packages
            .iter()
            .map(|p| (p.as_str(), "crate::wire"))
            .collect();
        wire_into(out, protos, includes)?;
        markup_domain_into(out, protos, includes, &wire)?;
    }
    adapters_into(out, protos, includes, &remote)?;
    descriptors_into(out, protos, includes, &remote, &rules_version())?;
    let source =
        angzarr_codegen::register_only(&bytes, protos, &listed).map_err(BuildError::Codegen)?;
    let path = out.join("angzarr-register.rs");
    std::fs::write(&path, source).map_err(|e| io(&path, &e))?;
    let crate_domain = if domain {
        None
    } else {
        let manifest = config.parent().unwrap_or(Path::new("")).join("Cargo.toml");
        println!("cargo:rerun-if-changed={}", manifest.display());
        let text = std::fs::read_to_string(&manifest).map_err(|e| io(&manifest, &e))?;
        angzarr_codegen::manifest_domain_crate(&text).map_err(BuildError::Codegen)?
    };
    let path = out.join("angzarr-app.rs");
    let own = if domain { packages(&names) } else { Vec::new() };
    let layer = angzarr_codegen::app_layer(&own, crate_domain.as_ref());
    std::fs::write(&path, layer).map_err(|e| io(&path, &e))
}

/// The proto packages of `names`' components, each once, sorted.
fn packages(names: &[angzarr_codegen::ComponentName]) -> Vec<String> {
    let mut packages: Vec<String> = names
        .iter()
        .filter_map(|c| c.component.rsplit_once('.').map(|(p, _)| p.to_string()))
        .collect();
    packages.sort();
    packages.dedup();
    packages
}

/// The startup of an angzarr-embedded application (see
/// `angzarr_codegen::app`) under `root`, the crate root: `angzarr.yaml`,
/// `Cargo.toml`, `src/main.rs` and `src/driver.rs` written once from `seed`
/// and the user's from then on; `build.rs` the CLI's, rewritten from the
/// protos and the `angzarr.yaml` and `Cargo.toml` on disk on every run. A
/// config with lints is refused and nothing is written.
pub fn app_scaffold_into(
    root: &Path,
    protos: &[&str],
    includes: &[&str],
    seed: &AppSeed,
) -> Result<Vec<Scaffolded>, BuildError> {
    let on_disk = |name: &str| -> Result<Option<String>, BuildError> {
        let path = root.join(name);
        if !path.exists() {
            return Ok(None);
        }
        std::fs::read_to_string(&path)
            .map(Some)
            .map_err(|e| io(&path, &e))
    };
    let config = on_disk("angzarr.yaml")?;
    let manifest = on_disk("Cargo.toml")?;
    let bytes = scaffold_descriptors(protos, includes)?;
    let files = angzarr_codegen::app(&bytes, protos, seed, config.as_deref(), manifest.as_deref())
        .map_err(BuildError::Codegen)?;
    write_scaffold(root, files, Existing::Keep)
}

/// The lints of the host config at `config` against the components
/// `protos` declare, and the protos' warnings (see
/// `angzarr_codegen::lint_app`).
pub fn lint_app(config: &Path, protos: &[&str], includes: &[&str]) -> Result<Linted, BuildError> {
    let text = std::fs::read_to_string(config).map_err(|e| io(config, &e))?;
    let bytes = scaffold_descriptors(protos, includes)?;
    angzarr_codegen::lint_app(&text, &bytes, protos).map_err(BuildError::Codegen)
}

/// What [`scaffold_into`] did with one component's file.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Scaffolded {
    /// It was absent, and is written.
    Created(PathBuf),
    /// The user's, it existed and lacked stubs, which are added; with what
    /// it still lacks, has beyond, and pins otherwise.
    Completed { path: PathBuf, notes: Notes },
    /// It existed and is unchanged; a user's with what it lacks, has
    /// beyond, and pins otherwise.
    Kept { path: PathBuf, notes: Notes },
    /// A generated file (the CLI's, never the user's) that existed with
    /// other content, rewritten.
    Regenerated(PathBuf),
    /// A file an earlier scaffold wrote and this one does not: left for the
    /// user to delete.
    Obsolete(PathBuf),
    /// A generated file under a `_gen` that this run did not write: an
    /// orphan, deleted (item S Q9).
    Deleted(PathBuf),
}

/// The markup scaffold of every component in `protos`, one file each
/// under `src` (see `angzarr_codegen::scaffold`; every Rust method is
/// required, a policy's refusal callback too). An absent file is
/// written; an existing one is the user's, kept, or completed with
/// `Existing::Complete`. Every file is decided before any is written, so a
/// refusal writes none. It is written as the host config beside `src`
/// configures it (`angzarr.yaml` in `src`'s parent, the crate's root:
/// [`host_scaffold`]), and with no config there, as the default, every
/// component keeping its interface's defaults.
pub fn scaffold_into(
    src: &Path,
    protos: &[&str],
    includes: &[&str],
    existing: Existing,
) -> Result<Vec<Scaffolded>, BuildError> {
    let beside = src
        .parent()
        .unwrap_or(Path::new(""))
        .join(angzarr_codegen::HOST_CONFIG);
    let config = match beside.exists() {
        true => host_scaffold(&beside)?,
        false => ScaffoldConfig::default(),
    };
    scaffold_with_into(src, protos, includes, existing, &[], &[], &config)
}

/// [`scaffold_into`], each component's implementation importing its
/// package's domain layer at the path `domain` names for it, and holding a
/// wire state at the path `wire` names, as [`markup_domain_with`] places
/// them, and written as `config` says, a host config's [`host_scaffold`]
/// (see `angzarr_codegen::scaffold_with`).
pub fn scaffold_with_into(
    src: &Path,
    protos: &[&str],
    includes: &[&str],
    existing: Existing,
    wire: &[(&str, &str)],
    domain: &[(&str, &str)],
    config: &ScaffoldConfig,
) -> Result<Vec<Scaffolded>, BuildError> {
    let bytes = scaffold_descriptors(protos, includes)?;
    let files = angzarr_codegen::scaffold_with(&bytes, protos, wire, domain, config)
        .map_err(BuildError::Codegen)?;
    write_scaffold(src, files, existing)
}

/// The scaffold's configuration the host config at `config` (an
/// application's `angzarr.yaml`) gives: the aggregates and process
/// managers whose `component` entry sets `scaffold.snapshots`, which the
/// scaffold writes taking snapshots (see `angzarr_codegen::host_scaffold`).
pub fn host_scaffold(config: &Path) -> Result<ScaffoldConfig, BuildError> {
    let text = std::fs::read_to_string(config).map_err(|e| io(config, &e))?;
    angzarr_codegen::host_scaffold(&text).map_err(BuildError::Codegen)
}

/// The uv project of one Python domain (see
/// `angzarr_codegen::scaffold_python`) under `root`, as [`scaffold_into`]
/// writes a Rust scaffold: a user's file once, a generated file every time.
pub fn scaffold_python_into(
    root: &Path,
    protos: &[&str],
    includes: &[&str],
    app: &PythonApp,
    existing: Existing,
) -> Result<Vec<Scaffolded>, BuildError> {
    let bytes = scaffold_descriptors(protos, includes)?;
    let files =
        angzarr_codegen::scaffold_python(&bytes, protos, app).map_err(BuildError::Codegen)?;
    write_scaffold(root, files, existing)
}

/// protoc's descriptor set for a scaffold: `protos`, and every other proto
/// in their directories, so a component a scaffolded message names (a
/// policy, a projector) is found without being scaffolded.
fn scaffold_descriptors(protos: &[&str], includes: &[&str]) -> Result<Vec<u8>, BuildError> {
    let work = scratch_dir()?;
    let inputs = with_siblings(protos, includes);
    let inputs: Vec<&str> = inputs.iter().map(String::as_str).collect();
    let bytes = protoc_set(&work, &inputs, includes);
    let _ = std::fs::remove_dir_all(&work);
    bytes
}

/// `protos`, then every other proto of their directories (under the first
/// include holding each), as import names: what protoc reads so a
/// component a message of `protos` names is declared.
fn with_siblings(protos: &[&str], includes: &[impl AsRef<Path>]) -> Vec<String> {
    let mut inputs: Vec<String> = protos.iter().map(|p| p.to_string()).collect();
    for proto in protos {
        let Some(include) = includes
            .iter()
            .map(AsRef::as_ref)
            .find(|i| i.join(proto).exists())
        else {
            continue;
        };
        let dir = Path::new(proto).parent().unwrap_or(Path::new(""));
        let Ok(entries) = std::fs::read_dir(include.join(dir)) else {
            continue;
        };
        let mut siblings: Vec<String> = entries
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|name| name.ends_with(".proto"))
            .map(|name| dir.join(name).to_string_lossy().replace('\\', "/"))
            .collect();
        siblings.sort();
        for sibling in siblings {
            if !inputs.contains(&sibling) {
                inputs.push(sibling);
            }
        }
    }
    inputs
}

/// Writes `files` under `root`: an absent file is created; an existing
/// user's file is kept, or completed with what it lacks
/// ([`angzarr_codegen::complete`]), and reported; an existing generated
/// file is rewritten when it differs, and only when it carries the
/// generated marker ([`angzarr_codegen::is_generated`]): one without it is
/// refused, so a hand edit is never overwritten. A file at a user's path
/// that carries the marker is an earlier CLI's generated file, and is
/// rewritten as the user's. An obsolete file is reported where it is. Every
/// file is decided before any is written, so a refusal writes none. Then
/// each `_gen` the files are under is swept ([`sweep`]).
fn write_scaffold(
    root: &Path,
    files: Vec<angzarr_codegen::ScaffoldFile>,
    existing: Existing,
) -> Result<Vec<Scaffolded>, BuildError> {
    use angzarr_codegen::Owner;
    let mut writes = Vec::new();
    let mut done = Vec::new();
    let ours: Vec<PathBuf> = files
        .iter()
        .filter(|f| f.owner != Owner::Obsolete)
        .map(|f| root.join(&f.path))
        .collect();
    for file in files {
        let path = root.join(&file.path);
        let present = path.exists();
        match file.owner {
            Owner::External => continue,
            Owner::Obsolete if present => {
                done.push(Scaffolded::Obsolete(path));
                continue;
            }
            Owner::Obsolete => continue,
            Owner::User | Owner::Generated if !present => {
                writes.push((path.clone(), file.source));
                done.push(Scaffolded::Created(path));
                continue;
            }
            Owner::User | Owner::Generated => {}
        }
        let mine = std::fs::read_to_string(&path).map_err(|e| io(&path, &e))?;
        let stale = angzarr_codegen::is_generated(&mine);
        if file.owner == Owner::Generated && mine == file.source {
            done.push(Scaffolded::Kept {
                path,
                notes: Notes::default(),
            });
        } else if file.owner == Owner::Generated && !stale {
            return Err(BuildError::GeneratedUnmarked {
                path: path.display().to_string(),
            });
        } else if stale {
            writes.push((path.clone(), file.source));
            done.push(Scaffolded::Regenerated(path));
        } else {
            let completion =
                angzarr_codegen::complete(&file, &mine, existing).map_err(BuildError::Codegen)?;
            let notes = completion.notes;
            match completion.completed {
                Some(completed) => {
                    writes.push((path.clone(), completed));
                    done.push(Scaffolded::Completed { path, notes });
                }
                None => done.push(Scaffolded::Kept { path, notes }),
            }
        }
    }
    for (path, source) in writes {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| io(parent, &e))?;
        }
        std::fs::write(&path, source).map_err(|e| io(&path, &e))?;
    }
    for dir in gen_dirs(root, &ours) {
        sweep(&dir, &ours, &mut done)?;
    }
    Ok(done)
}

/// The generated directory name ([`sweep`]): the one path every gate of a
/// project excludes.
const GEN: &str = "_gen";

/// Each `_gen` directory `files` are under (`root`-relative), once.
fn gen_dirs(root: &Path, files: &[PathBuf]) -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = Vec::new();
    for file in files {
        let Ok(relative) = file.strip_prefix(root) else {
            continue;
        };
        let mut dir = root.to_path_buf();
        for part in relative.components() {
            dir.push(part);
            if part.as_os_str() == GEN {
                if !dirs.contains(&dir) {
                    dirs.push(dir.clone());
                }
                break;
            }
        }
    }
    dirs
}

/// Deletes each file under `dir` that carries the generated marker and is
/// not one of `ours` (this run's): an orphan of a component, or a proto, no
/// longer scaffolded (item S Q9). A file without the marker is never
/// touched, nor is anything that is not text.
fn sweep(dir: &Path, ours: &[PathBuf], done: &mut Vec<Scaffolded>) -> Result<(), BuildError> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Ok(());
    };
    let mut entries: Vec<PathBuf> = entries.filter_map(|e| e.ok().map(|e| e.path())).collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            sweep(&path, ours, done)?;
            continue;
        }
        let orphan = !ours.contains(&path)
            && std::fs::read_to_string(&path)
                .is_ok_and(|text| angzarr_codegen::is_generated(&text));
        if orphan {
            std::fs::remove_file(&path).map_err(|e| io(&path, &e))?;
            done.push(Scaffolded::Deleted(path));
        }
    }
    Ok(())
}

/// The message types of a scaffolded Python project: `buf generate` run in
/// `root` (its `buf.gen.yaml`), then every generated module headed with the
/// generated marker and its imports rerooted
/// (`angzarr_codegen::reroot_python`): angzarr's own under
/// `angzarr_client.proto`, the project's relative, within its `_gen`.
pub fn proto_gen_python(root: &Path, app: &PythonApp) -> Result<(), BuildError> {
    run(root, "buf", &["generate"], |detail| BuildError::Buf {
        detail,
    })?;
    let gen_dir = root
        .join("src")
        .join(format!("angzarr_{}", app.domain.replace('-', "_")))
        .join("_gen");
    let mut modules = Vec::new();
    collect_python(&gen_dir, &mut modules)?;
    // The packages buf generated: the directories holding a `_pb2` module.
    let mut packages: Vec<String> = modules
        .iter()
        .filter(|m| {
            m.file_name()
                .is_some_and(|n| n.to_string_lossy().ends_with("_pb2.py"))
        })
        .filter_map(|m| m.parent()?.strip_prefix(&gen_dir).ok())
        .map(|dir| dir.to_string_lossy().replace(['/', '\\'], "."))
        .filter(|p| !p.is_empty())
        .collect();
    packages.sort();
    packages.dedup();
    let packages: Vec<&str> = packages.iter().map(String::as_str).collect();
    let rules = angzarr_codegen::python_reroots(&packages);
    for module in modules {
        let source = std::fs::read_to_string(&module).map_err(|e| io(&module, &e))?;
        let package = module
            .parent()
            .and_then(|dir| dir.strip_prefix(&gen_dir).ok())
            .map(|dir| dir.to_string_lossy().replace(['/', '\\'], "."))
            .unwrap_or_default();
        let rerooted = angzarr_codegen::reroot_python(&source, &rules, &package);
        if rerooted != source {
            std::fs::write(&module, rerooted).map_err(|e| io(&module, &e))?;
        }
    }
    Ok(())
}

/// `uv lock` in `root`: the `uv.lock` angzarr-embedded's `uv run --locked`
/// needs.
pub fn lock_python(root: &Path) -> Result<(), BuildError> {
    run(root, "uv", &["lock"], |detail| BuildError::Uv { detail })
}

/// Every `.py` and `.pyi` file under `dir`.
fn collect_python(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), BuildError> {
    let entries = std::fs::read_dir(dir).map_err(|e| io(dir, &e))?;
    for entry in entries {
        let path = entry.map_err(|e| io(dir, &e))?.path();
        if path.is_dir() {
            collect_python(&path, out)?;
        } else if path.extension().is_some_and(|e| e == "py" || e == "pyi") {
            out.push(path);
        }
    }
    Ok(())
}

/// `program args` in `dir`, its stderr the failure's detail.
fn run(
    dir: &Path,
    program: &str,
    args: &[&str],
    fail: impl Fn(String) -> BuildError,
) -> Result<(), BuildError> {
    let output = Command::new(program)
        .args(args)
        .current_dir(dir)
        .output()
        .map_err(|e| fail(e.to_string()))?;
    if !output.status.success() {
        return Err(fail(
            String::from_utf8_lossy(&output.stderr).trim().to_string(),
        ));
    }
    Ok(())
}

/// A fresh directory for protoc's output when there is no `OUT_DIR`.
fn scratch_dir() -> Result<PathBuf, BuildError> {
    static RUN: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "{}-{}-{}",
        env!("CARGO_PKG_NAME"),
        std::process::id(),
        RUN.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).map_err(|e| io(&dir, &e))?;
    Ok(dir)
}

/// [`wire`], into `out`; and `angzarr-wire.rs`, the modules nesting them as
/// their packages do, so prost's `super::` paths resolve, with the
/// allowances prost's output needs (see `angzarr_codegen::wire_modules`),
/// for the crate root to `include!`.
pub fn wire_into(out: &Path, protos: &[&str], includes: &[&str]) -> Result<(), BuildError> {
    let bytes = build_descriptors(out, protos, includes)?;
    let includes = with_angzarr(out, includes)?;
    prost_build::Config::new()
        .enable_type_names()
        .btree_map(["."])
        .extern_path(".io.angzarr.v1", "::angzarr_proto::v1")
        .out_dir(out)
        .compile_protos(protos, &includes)
        .map_err(|e| BuildError::Prost {
            detail: e.to_string(),
        })?;
    let modules = angzarr_codegen::wire_modules(&bytes, protos).map_err(BuildError::Codegen)?;
    let path = out.join("angzarr-wire.rs");
    std::fs::write(&path, modules).map_err(|e| io(&path, &e))
}

/// Each package's domain layer, as `<package>.domain.rs` in `out`.
fn write_domain(out: &Path, generated: Vec<angzarr_codegen::Generated>) -> Result<(), BuildError> {
    for generated in generated {
        let path = out.join(format!("{}.domain.rs", generated.package));
        std::fs::write(&path, generated.source).map_err(|e| io(&path, &e))?;
    }
    Ok(())
}

/// [`descriptors`] for a build script: Cargo reruns it when a proto
/// changes.
fn build_descriptors(
    out: &Path,
    protos: &[&str],
    includes: &[&str],
) -> Result<Vec<u8>, BuildError> {
    let includes = with_angzarr(out, includes)?;
    rerun_if_changed(protos, &includes);
    let inputs = with_siblings(protos, &includes);
    let inputs: Vec<&str> = inputs.iter().map(String::as_str).collect();
    protoc(out, &inputs, &includes)
}

/// protoc's descriptor set for `protos`, written under `work`.
fn protoc_set(work: &Path, protos: &[&str], includes: &[&str]) -> Result<Vec<u8>, BuildError> {
    protoc(work, protos, &with_angzarr(work, includes)?)
}

/// protoc's descriptor set for `protos` (`--include_imports`, so angzarr's
/// options survive), written under `work`.
fn protoc(work: &Path, protos: &[&str], includes: &[PathBuf]) -> Result<Vec<u8>, BuildError> {
    let set = work.join("angzarr.descriptors");
    let output = Command::new(prost_build::protoc_from_env())
        .arg("--include_imports")
        .arg(format!("--descriptor_set_out={}", set.display()))
        .args(includes.iter().map(|i| format!("-I{}", i.display())))
        .args(protos)
        .output()
        .map_err(|e| BuildError::Protoc {
            detail: e.to_string(),
        })?;
    if !output.status.success() {
        return Err(BuildError::Protoc {
            detail: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    std::fs::read(&set).map_err(|e| io(&set, &e))
}

fn out_dir() -> Result<PathBuf, BuildError> {
    std::env::var_os("OUT_DIR")
        .map(PathBuf::from)
        .ok_or(BuildError::NoOutDir)
}

/// `includes`, then angzarr's protos and the codegen options
/// (`angzarr_codegen::CODEGEN_PROTO`), written under `out`.
fn with_angzarr(out: &Path, includes: &[&str]) -> Result<Vec<PathBuf>, BuildError> {
    let dir = out.join("angzarr-proto");
    let angzarr = angzarr_proto::protos::write_include_dir(&dir).map_err(|e| io(&dir, &e))?;
    let (name, source) = angzarr_codegen::CODEGEN_PROTO;
    let codegen = angzarr.join(name);
    if let Some(parent) = codegen.parent() {
        std::fs::create_dir_all(parent).map_err(|e| io(parent, &e))?;
    }
    std::fs::write(&codegen, source).map_err(|e| io(&codegen, &e))?;
    Ok(includes
        .iter()
        .map(PathBuf::from)
        .chain(std::iter::once(angzarr))
        .collect())
}

/// Cargo reruns the build script when a proto changes: each proto, found
/// under the first include holding it.
fn rerun_if_changed(protos: &[&str], includes: &[PathBuf]) {
    for proto in protos {
        if let Some(path) = includes.iter().map(|i| i.join(proto)).find(|p| p.exists()) {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }
    println!("cargo:rerun-if-env-changed=PROTOC");
}

fn io(path: &Path, error: &std::io::Error) -> BuildError {
    BuildError::Io {
        path: path.display().to_string(),
        detail: error.to_string(),
    }
}

/// Why generation failed. A build script that returns it fails the build
/// with its English text (its `Debug`).
#[derive(Clone, PartialEq, Eq)]
pub enum BuildError {
    /// Not run by Cargo: `OUT_DIR` is unset.
    NoOutDir,
    /// Reading or writing a file under `OUT_DIR`.
    Io { path: String, detail: String },
    /// protoc could not run, or refused the protos.
    Protoc { detail: String },
    /// prost-build failed.
    Prost { detail: String },
    /// `buf generate` could not run, or failed.
    Buf { detail: String },
    /// `uv` could not run, or failed.
    Uv { detail: String },
    /// A generated file's path holds a file without the generated marker:
    /// edited by hand, or written by hand in its place. Never overwritten.
    GeneratedUnmarked { path: String },
    /// The generator refused the protos.
    Codegen(angzarr_codegen::CodegenError),
}

/// Its code and args (decision 14.37); a build script shows it in English
/// (its `Debug`).
impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

/// A build script's `main` returning `Result<(), BuildError>` prints the
/// error with `Debug`, where a person reads it: its English, its message in
/// this crate's catalog ([`EN_US`]), the generator's refusal its own message
/// in the generator's ([`angzarr_codegen::EN_US`]).
impl fmt::Debug for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BuildError::Codegen(error) => {
                fmt::Display::fmt(&English(angzarr_codegen::EN_US.1, error), f)
            }
            _ => fmt::Display::fmt(&English(EN_US.1, self), f),
        }
    }
}

impl std::error::Error for BuildError {}

impl Diagnostic for BuildError {
    fn code(&self) -> &'static str {
        match self {
            BuildError::NoOutDir => code!("no-out-dir"),
            BuildError::Io { .. } => code!("io"),
            BuildError::Protoc { .. } => code!("protoc"),
            BuildError::Prost { .. } => code!("prost"),
            BuildError::Buf { .. } => code!("buf"),
            BuildError::Uv { .. } => code!("uv"),
            BuildError::GeneratedUnmarked { .. } => code!("generated-unmarked"),
            BuildError::Codegen(_) => code!("codegen"),
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match self {
            BuildError::NoOutDir => Vec::new(),
            BuildError::Io { path, detail } => vec![
                ("path", path.as_str().into()),
                ("detail", detail.as_str().into()),
            ],
            BuildError::Protoc { detail }
            | BuildError::Prost { detail }
            | BuildError::Buf { detail }
            | BuildError::Uv { detail } => {
                vec![("detail", detail.as_str().into())]
            }
            BuildError::GeneratedUnmarked { path } => vec![("path", path.as_str().into())],
            BuildError::Codegen(error) => vec![("error", Arg::nested(error))],
        }
    }
}

impl BuildError {
    /// What the user can do about it, a coded message, when there is
    /// something: a generated path holding an unmarked file is restored or
    /// deleted, so the next run writes it.
    pub fn remedy(&self) -> Option<angzarr_diagnostic::Report> {
        match self {
            BuildError::GeneratedUnmarked { .. } => Some(angzarr_diagnostic::Report::new(code!(
                "generated-unmarked-remedy"
            ))),
            _ => None,
        }
    }
}

/// One value of every error this crate defines, for the catalog test.
pub fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    let mut samples: Vec<Box<dyn Diagnostic>> = error_samples()
        .into_iter()
        .map(|e| Box::new(e) as Box<dyn Diagnostic>)
        .collect();
    samples.extend(
        error_samples()
            .iter()
            .filter_map(BuildError::remedy)
            .map(|r| Box::new(r) as Box<dyn Diagnostic>),
    );
    samples
}

/// One of every [`BuildError`].
fn error_samples() -> Vec<BuildError> {
    let s = || "x".to_string();
    vec![
        BuildError::NoOutDir,
        BuildError::Io {
            path: s(),
            detail: s(),
        },
        BuildError::Protoc { detail: s() },
        BuildError::Prost { detail: s() },
        BuildError::Buf { detail: s() },
        BuildError::Uv { detail: s() },
        BuildError::GeneratedUnmarked { path: s() },
        BuildError::Codegen(angzarr_codegen::UseError::NoWirePath { package: s() }.into()),
    ]
}

#[cfg(test)]
mod tests {
    use angzarr_diagnostic::{Diagnostic, code};

    use super::{BuildError, EN_US, English, error_samples};

    /// Every crate's catalog a build script shows, in Fluent.
    fn fluent() -> presentation::Catalog {
        presentation::Catalog::en_us(&[angzarr_codegen::EN_US, EN_US]).expect("the catalogs load")
    }

    /// `d` as Fluent renders it, strictly: a code with no message, or a
    /// message that does not format, fails the test.
    fn rendered(catalog: &presentation::Catalog, d: &dyn Diagnostic) -> String {
        catalog.render(d).expect("the catalog renders it")
    }

    /// A build script's English is Fluent's rendering of the catalogs, for
    /// every error it can show: its own, and the generator's refusals (nested
    /// in [`BuildError::Codegen`]).
    #[test]
    fn a_build_scripts_english_is_the_catalogs_for_every_error() {
        let catalog = fluent();
        for error in error_samples() {
            assert_eq!(
                format!("{error:?}"),
                rendered(&catalog, &error),
                "{}",
                error.code()
            );
        }
        for refusal in angzarr_codegen::diagnostic_samples() {
            let shown = English(angzarr_codegen::EN_US.1, &*refusal).to_string();
            assert_eq!(shown, rendered(&catalog, &*refusal), "{}", refusal.code());
        }
    }

    #[test]
    fn an_error_displays_as_its_code_and_args() {
        let refusal = angzarr_codegen::UseError::NoWirePath {
            package: "shop.v1".to_string(),
        };
        let error = BuildError::Codegen(refusal.clone().into());
        assert_eq!(
            error.to_string(),
            format!(
                "{} error={} error.package=shop.v1",
                code!("codegen"),
                refusal.code()
            )
        );
    }
}
