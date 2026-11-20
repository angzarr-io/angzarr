//! Reads the workspace through `cargo metadata` and feeds every target and
//! every manifest dependency to the [`Scanner`].

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::Deserialize;

use crate::model::{Model, Scope};
use crate::scan::Scanner;

#[derive(Debug, Deserialize)]
pub struct Metadata {
    pub packages: Vec<Package>,
    pub workspace_root: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct Package {
    pub name: String,
    pub manifest_path: PathBuf,
    pub targets: Vec<Target>,
    pub dependencies: Vec<Dependency>,
}

#[derive(Debug, Deserialize)]
pub struct Target {
    pub name: String,
    pub kind: Vec<String>,
    pub src_path: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct Dependency {
    pub name: String,
    pub rename: Option<String>,
    pub kind: Option<String>,
    pub path: Option<PathBuf>,
}

const LIB_KINDS: [&str; 6] = ["lib", "rlib", "dylib", "cdylib", "staticlib", "proc-macro"];

impl Package {
    /// The name the package's library is referenced by in code.
    pub fn lib_name(&self) -> String {
        self.targets
            .iter()
            .find(|t| t.kind.iter().any(|k| LIB_KINDS.contains(&k.as_str())))
            .map_or_else(|| self.name.replace('-', "_"), |t| t.name.clone())
    }
}

impl Dependency {
    pub fn code_name(&self) -> String {
        self.rename
            .as_deref()
            .unwrap_or(&self.name)
            .replace('-', "_")
    }

    pub fn scope(&self) -> Scope {
        match self.kind.as_deref() {
            Some("dev") => Scope::Test,
            _ => Scope::Lib,
        }
    }
}

impl Target {
    /// The module path a target's root is known by: the library name for
    /// the library, `<lib>::<kind>#<name>` for every other target.
    pub fn root_module(&self, lib: &str) -> String {
        if self.is_lib() {
            return lib.to_string();
        }
        let kind = match self.kind.first().map(String::as_str) {
            Some("custom-build") => "build",
            Some(other) => other,
            None => "target",
        };
        format!("{lib}::{kind}#{}", self.name)
    }

    pub fn is_lib(&self) -> bool {
        self.kind.iter().any(|k| LIB_KINDS.contains(&k.as_str()))
    }

    pub fn scope(&self) -> Scope {
        let test_kinds = ["test", "bench", "example"];
        if self.kind.iter().any(|k| test_kinds.contains(&k.as_str())) {
            Scope::Test
        } else {
            Scope::Lib
        }
    }
}

pub fn metadata(manifest: &Path) -> Result<Metadata, String> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let output = Command::new(cargo)
        .args([
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
            "--manifest-path",
        ])
        .arg(manifest)
        .output()
        .map_err(|e| format!("cargo metadata: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "cargo metadata: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    serde_json::from_slice(&output.stdout).map_err(|e| format!("cargo metadata: {e}"))
}

/// Builds the model of every workspace package: each target's module tree
/// and the edges its manifest dependencies declare.
pub fn build_model(meta: &Metadata) -> Result<(Model, Vec<String>), String> {
    let mut scanner = Scanner::new(meta.workspace_root.clone());
    for package in &meta.packages {
        scanner.add_workspace_crate(&package.lib_name());
    }
    for package in &meta.packages {
        let lib = package.lib_name();
        let mut known: BTreeSet<String> = package
            .dependencies
            .iter()
            .map(Dependency::code_name)
            .collect();
        known.extend(["std", "core", "alloc"].map(String::from));
        known.insert(lib.clone());
        let mut seen = BTreeSet::new();
        for target in &package.targets {
            let root = target.root_module(&lib);
            if seen.insert(root.clone()) {
                scanner.add_target(&root, &target.src_path, target.scope(), &known)?;
            }
        }
        for dep in &package.dependencies {
            let table = match dep.kind.as_deref() {
                Some("dev") => "dev-dependencies",
                Some("build") => "build-dependencies",
                _ => "dependencies",
            };
            let via = format!("[{table}] {}", dep.name);
            scanner.add_manifest_edge(
                &lib,
                &dep.code_name(),
                dep.scope(),
                &package.manifest_path,
                &via,
            );
            let in_workspace = meta.packages.iter().any(|p| p.name == dep.name);
            if let Some(path) = dep.path.as_ref().filter(|_| !in_workspace) {
                let target = format!("path:{}", scanner.relative(path).display());
                scanner.add_manifest_edge(&lib, &target, dep.scope(), &package.manifest_path, &via);
            }
        }
    }
    Ok(scanner.finish())
}

#[cfg(test)]
#[path = "workspace.test.rs"]
mod workspace_tests;
