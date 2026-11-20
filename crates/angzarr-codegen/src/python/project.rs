//! The project's harness files: `_descriptor.py`, `main.py`,
//! `pyproject.toml` and `buf.gen.yaml`, and the rerooting of the generated
//! pb2 modules' imports.

use std::collections::BTreeSet;

use crate::common::snake;
use crate::error::CodegenError;
use crate::model::ComponentKind;

use super::layout::{head, wrap};
use super::logic::logic_name;
use super::{
    CLIENT_PYTHON_GIT, CLIENT_PYTHON_REV, FRAMEWORK_PACKAGES, GENERATED, ProtoInput, PythonApp,
    Unit, capital, module_dir, roots,
};

/// `_descriptor.py`: the unit's generated `ComponentDescriptor`, named by
/// its name in an application, `app.rules_version` its rules version, as
/// the bytes its child answers `Describe` with, unchanged.
pub(super) fn descriptor_py(
    app: &PythonApp,
    served: &str,
    descriptor_set: &[u8],
    files: &[&str],
    unit: &Unit,
) -> Result<String, CodegenError> {
    let anchor = unit.component.anchor.full_name();
    let encoded = crate::descriptor::descriptors(descriptor_set, files, &app.rules_version)?
        .into_iter()
        .find(|d| d.component == anchor)
        .map(|d| d.encoded)
        .unwrap_or_default();
    let hex: String = encoded.iter().map(|b| format!("{b:02x}")).collect();
    let lines: String = hex
        .as_bytes()
        .chunks(64)
        .map(|chunk| format!("        \"{}\"\n", String::from_utf8_lossy(chunk)))
        .collect();
    Ok(format!(
        "{GENERATED}\"\"\"{}'s ComponentDescriptor, generated from its protos.\n\n\
         Its rules_version is ``_version``'s. The child answers ``Describe`` with it,\n\
         unchanged (decision 14.42).\n\
         \"\"\"\n\n\
         from __future__ import annotations\n\n\
         from angzarr_client.proto.io.angzarr.v1 import types_pb2 as _t\n\n\
         DESCRIPTOR = _t.ComponentDescriptor.FromString(\n    bytes.fromhex(\n{lines}    )\n)\n",
        capital(served),
    ))
}

/// `ComponentHost`'s registration of a kind: `add_<kind>`.
fn add(kind: ComponentKind) -> &'static str {
    match kind {
        ComponentKind::Aggregate => "add_aggregate",
        ComponentKind::ProcessManager => "add_process_manager",
        ComponentKind::Policy => "add_policy",
        ComponentKind::Projector => "add_projector",
    }
}

/// The logic parameter of `_gen/main.py`'s functions for `logic`, an
/// interface's name: `player_logic: PlayerLogic`.
fn logic_param(logic: &str) -> String {
    format!("{}: {logic}", snake(logic))
}

/// Each distinct logic interface of `units`, in order.
fn logics(app: &PythonApp, units: &[Unit]) -> Vec<String> {
    let mut logics: Vec<String> = Vec::new();
    for unit in units {
        let logic = logic_name(app, unit);
        if !logics.contains(&logic) {
            logics.push(logic);
        }
    }
    logics
}

/// `_gen/main.py`: the child's host, each unit's generated dispatch
/// registered over the logic it is handed, and `run`, which serves it. It
/// imports nothing of the user's: the user's `__main__.py` builds the logic
/// and hands it over.
pub(super) fn main_py(app: &PythonApp, served: &str, units: &[Unit]) -> String {
    let script = app.script();
    let mut imports = Vec::new();
    let mut chain = String::from("host");
    for unit in units {
        let component = &unit.component;
        let stem = snake(&component.name);
        let dir = module_dir(&component.anchor).replace('/', ".");
        let logic = snake(&logic_name(app, unit));
        imports.push(format!(
            "from .{dir}.{stem}_angzarr import (\n    new_{stem}_dispatch,\n)\n"
        ));
        chain = format!(
            "{chain}.{}(\n        new_{stem}_dispatch({logic}), DESCRIPTOR\n    )",
            add(component.kind)
        );
    }
    let logics = logics(app, units);
    imports.push("from ._descriptor import DESCRIPTOR\n".to_string());
    imports.push("from ._version import __version__\n".to_string());
    imports.push(format!("from .logic import {}\n", logics.join(", ")));
    imports.sort();
    let params: Vec<String> = logics.iter().map(|l| logic_param(l)).collect();
    let args: Vec<String> = logics.iter().map(|l| snake(l)).collect();
    let names: Vec<&str> = units.iter().map(|u| u.component.name.as_str()).collect();
    let (params, args) = (params.join(", "), args.join(", "));
    format!(
        "{GENERATED}\"\"\"{script}: {served} as an angzarr-embedded child process.\n\n\
         angzarr-embedded launches it (``uv run --locked {script}``), reads the\n\
         handshake it writes once it is serving, and calls it over gRPC on a Unix\n\
         socket. The logic is the user's, handed over by ``__main__.py``.\n\"\"\"\n\n\
         from __future__ import annotations\n\n\
         from angzarr_client import ComponentHost, configure_logging\n\
         from angzarr_client.child import run_child\n\n\
         {}\n\n\
         {}\n    \"\"\"The {} on ``host``, over the logic given.\"\"\"\n    return {chain}\n\n\n\
         {}\n    \"\"\"The child's host, reporting ``_version`` as its rules_version.\"\"\"\n    \
         return register(ComponentHost(rules_version=__version__), {args})\n\n\n\
         {}\n    \"\"\"Serves the logic as the child: what ``__main__.py`` calls.\"\"\"\n    \
         configure_logging()\n    run_child(host({args}))\n",
        imports.concat(),
        head(
            0,
            "def register",
            &[format!("host: ComponentHost, {params}")],
            "ComponentHost",
            ""
        ),
        names.join(", "),
        head(
            0,
            "def host",
            std::slice::from_ref(&params),
            "ComponentHost",
            ""
        ),
        head(0, "def run", std::slice::from_ref(&params), "None", ""),
    )
}

/// `__main__.py`, the user's composition root, written once: it builds the
/// logic (its dependencies its constructor's arguments) and runs it; the
/// project's script and `python -m` both start here.
pub(super) fn dunder_main(app: &PythonApp, served: &str, units: &[Unit]) -> String {
    let logics = logics(app, units);
    let impls: Vec<String> = logics.iter().map(|l| format!("{l}Impl()")).collect();
    let imports: String = logics
        .iter()
        .map(|l| format!("from .logic_impl import {l}Impl\n"))
        .collect();
    let doc = wrap(&format!(
        "\"\"\"{}: {served} as an angzarr-embedded child process. Written once by \
         ``angzarr scaffold python`` and yours from then on: build the logic here, its \
         dependencies its constructor's arguments, and hand it to ``run``.",
        app.script()
    ));
    format!(
        "{doc}\n\"\"\"\n\n\
         from __future__ import annotations\n\n\
         from ._gen.main import run\n\
         {imports}\n\n\
         def main() -> None:\n    run({})\n\n\n\
         if __name__ == \"__main__\":\n    main()\n",
        impls.join(", "),
    )
}

pub(super) fn pyproject(app: &PythonApp, served: &str, units: &[Unit]) -> String {
    let anchors: Vec<&str> = units
        .iter()
        .map(|u| u.component.anchor.full_name())
        .collect();
    format!(
        "# {Served} ({anchors}), served to\n\
         # angzarr-embedded as a child process. Written once by `angzarr scaffold\n\
         # python` and yours from then on.\n\n\
         [project]\n\
         name = \"{script}\"\n\
         version = \"0.1.0\"\n\
         description = \"{Served}, an angzarr-embedded child process\"\n\
         requires-python = \">=3.11\"\n\
         dependencies = [\n    \"angzarr-client\",\n    \"protobuf>=7.35.1,<8\",\n    \"grpcio>=1.84.1\",\n    \"grpcio-health-checking>=1.76.0\",\n    \"structlog>=24\",\n]\n\n\
         [project.scripts]\n\
         {script} = \"{package}.__main__:main\"\n\n\
         [dependency-groups]\n\
         dev = [\"pytest>=8,<9.2\", \"pytest-timeout>=2.3\"]\n\n\
         [tool.uv.sources]\n\
         # The angzarr Python package at the commit this angzarr CLI was built against.\n\
         angzarr-client = {{ git = \"{CLIENT_PYTHON_GIT}\", rev = \"{CLIENT_PYTHON_REV}\" }}\n\n\
         [build-system]\n\
         requires = [\"hatchling\"]\n\
         build-backend = \"hatchling.build\"\n\n\
         [tool.hatch.build.targets.wheel]\n\
         packages = [\"src/{package}\"]\n\n\
         [tool.pytest.ini_options]\n\
         pythonpath = [\"src\"]\n\
         testpaths = [\"tests\"]\n",
        Served = capital(served),
        anchors = anchors.join(", "),
        script = app.script(),
        package = app.package(),
    )
}

/// Every proto file declaring a unit's root, and every file those import
/// (a pb2 module imports each of its file's imports, used or not; a field's
/// type is always among them), angzarr's own and google's excepted.
pub(super) fn proto_paths(units: &[Unit]) -> BTreeSet<String> {
    let mut files = BTreeSet::new();
    let mut reached: Vec<_> = units
        .iter()
        .flat_map(roots)
        .map(|m| m.parent_file())
        .collect();
    while let Some(file) = reached.pop() {
        let package = file.package_name();
        let ours = !FRAMEWORK_PACKAGES.contains(&package) && !package.starts_with("google.");
        if ours && files.insert(file.name().to_string()) {
            reached.extend(file.dependencies());
        }
    }
    files
}

pub(super) fn buf_gen(app: &PythonApp, served: &str, units: &[Unit]) -> String {
    let package = app.package();
    // A git input's paths are under its `proto`; a directory's are buf's
    // `--path`s, relative to where buf runs (the project root), as the
    // directory is.
    let (input, under) = match &app.protos {
        ProtoInput::Git { url, rev } => (
            format!("  - git_repo: {url}\n    ref: {rev}\n    subdir: proto\n"),
            String::new(),
        ),
        ProtoInput::Directory(dir) => (format!("  - directory: {dir}\n"), format!("{dir}/")),
    };
    let paths: String = proto_paths(units)
        .iter()
        .map(|p| format!("      - {under}{p}\n"))
        .collect();
    format!(
        "{GENERATED}#\n\
         # {}'s message types (pb2 and pyi) into\n\
         # src/{package}/_gen: its protos and the ones they import, never\n\
         # angzarr's own (those are angzarr_client.proto's). The dispatch beside them\n\
         # is the angzarr CLI's, and so is the rerooting of their imports.\n\
         version: v2\n\
         inputs:\n{input}    paths:\n{paths}\
         plugins:\n\
         \x20 - remote: buf.build/protocolbuffers/python:v35.1\n    out: src/{package}/_gen\n\
         \x20 - remote: buf.build/protocolbuffers/pyi:v35.1\n    out: src/{package}/_gen\n",
        capital(served),
    )
}

/// The import rules for generated message types: angzarr's framework
/// packages from `angzarr_client.proto`, every other package's from the
/// project's `_gen`, relative: its target opens with `.` (`.shop.v1`), to
/// be taken from the importing module's own package ([`reroot_python`]).
pub fn python_reroots(packages: &[&str]) -> Vec<(String, String)> {
    let mut rules: Vec<(String, String)> = FRAMEWORK_PACKAGES
        .iter()
        .map(|p| (p.to_string(), format!("angzarr_client.proto.{p}")))
        .collect();
    for package in packages {
        if !FRAMEWORK_PACKAGES.contains(package) {
            rules.push((package.to_string(), format!(".{package}")));
        }
    }
    rules
}

/// `source`, a generated pb2 or pyi module of the package `module` (dotted,
/// under the project's `_gen`), headed with the generated marker
/// ([`GENERATED`], once) and each import of a module under a rule's package
/// rerooted to the rule's target: `from io.angzarr.v1 import x` and
/// `import io.angzarr.v1.x` alike, absolute; a relative target (`.pkg`)
/// as `from` the importing module's package (`from ..pkg import x`), an
/// `import` of one as the `from` that binds the same name's module.
/// Nothing else changes.
pub fn reroot_python(source: &str, rules: &[(String, String)], module: &str) -> String {
    // The longest package a line names wins: `io.angzarr.v1` is not `io`.
    let mut rules: Vec<&(String, String)> = rules.iter().collect();
    rules.sort_by_key(|(from, _)| std::cmp::Reverse(from.len()));
    let here: Vec<String> = module
        .split('.')
        .filter(|p| !p.is_empty())
        .map(str::to_string)
        .collect();
    let mut out = String::with_capacity(source.len() + GENERATED.len());
    if !crate::is_generated(source) {
        out.push_str(GENERATED);
    }
    for line in source.split_inclusive('\n') {
        let mut rewritten = None;
        for (from, to) in &rules {
            for keyword in ["from ", "import "] {
                let Some(rest) = line.strip_prefix(keyword) else {
                    continue;
                };
                let Some(after) = rest.strip_prefix(from.as_str()) else {
                    continue;
                };
                if rewritten.is_none() && after.starts_with([' ', '.']) {
                    rewritten = Some(match to.strip_prefix('.') {
                        None => format!("{keyword}{to}{after}"),
                        Some(package) => relative_line(keyword, package, after, &here),
                    });
                }
            }
        }
        out.push_str(rewritten.as_deref().unwrap_or(line));
    }
    out
}

/// One import line rerooted under `_gen`, relative to the package `here`:
/// `from <package><after>` (`after` the rest of the dotted path and the
/// line's tail) from `here`; `import <package>.<module><tail>` as `from
/// <its package> import <module><tail>`.
fn relative_line(keyword: &str, package: &str, after: &str, here: &[String]) -> String {
    let (path, tail) = after
        .find(' ')
        .map_or((after.trim_end(), &after[after.trim_end().len()..]), |at| {
            (&after[..at], &after[at..])
        });
    let mut parts: Vec<String> = package.split('.').map(str::to_string).collect();
    parts.extend(
        path.split('.')
            .filter(|p| !p.is_empty())
            .map(str::to_string),
    );
    if keyword == "from " {
        parts.push(String::new());
        let (clause, _) = super::relative(here, &parts);
        return format!("from {clause}{tail}");
    }
    let (clause, leaf) = super::relative(here, &parts);
    format!("from {clause} import {leaf}{tail}")
}
