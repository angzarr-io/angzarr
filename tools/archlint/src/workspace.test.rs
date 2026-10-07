use std::path::PathBuf;

use super::*;

fn target(name: &str, kind: &str, src: &str) -> Target {
    Target {
        name: name.into(),
        kind: vec![kind.into()],
        src_path: src.into(),
    }
}

fn dep(name: &str, rename: Option<&str>, kind: Option<&str>, path: Option<&str>) -> Dependency {
    Dependency {
        name: name.into(),
        rename: rename.map(String::from),
        kind: kind.map(String::from),
        path: path.map(PathBuf::from),
    }
}

#[test]
fn lib_name_comes_from_the_library_target() {
    let p = Package {
        name: "my-crate".into(),
        manifest_path: "/ws/Cargo.toml".into(),
        targets: vec![
            target("tool", "bin", "src/main.rs"),
            target("my_lib", "cdylib", "src/lib.rs"),
        ],
        dependencies: vec![],
    };
    assert_eq!(p.lib_name(), "my_lib");
    let bin_only = Package {
        targets: vec![target("tool", "bin", "src/main.rs")],
        ..p
    };
    assert_eq!(bin_only.lib_name(), "my_crate");
}

#[test]
fn non_library_targets_get_kind_tagged_roots_and_scopes() {
    assert_eq!(target("x", "lib", "").root_module("app"), "app");
    assert_eq!(
        target("saga", "test", "").root_module("app"),
        "app::test#saga"
    );
    assert_eq!(
        target("build-script-build", "custom-build", "").root_module("app"),
        "app::build#build-script-build"
    );
    assert_eq!(
        Target {
            name: "t".into(),
            kind: vec![],
            src_path: "".into()
        }
        .root_module("app"),
        "app::target#t"
    );
    assert_eq!(target("x", "lib", "").scope(), Scope::Lib);
    assert_eq!(target("x", "bin", "").scope(), Scope::Lib);
    assert_eq!(target("x", "custom-build", "").scope(), Scope::Lib);
    for kind in ["test", "bench", "example"] {
        assert_eq!(target("x", kind, "").scope(), Scope::Test, "{kind}");
    }
}

#[test]
fn dependency_code_name_and_scope() {
    assert_eq!(
        dep("angzarr-router", None, None, None).code_name(),
        "angzarr_router"
    );
    assert_eq!(
        dep("angzarr-router", Some("core-x"), None, None).code_name(),
        "core_x"
    );
    assert_eq!(dep("a", None, Some("dev"), None).scope(), Scope::Test);
    assert_eq!(dep("a", None, Some("build"), None).scope(), Scope::Lib);
    assert_eq!(dep("a", None, None, None).scope(), Scope::Lib);
}

#[test]
fn build_model_scans_targets_and_manifest_edges() {
    let dir = std::env::temp_dir().join(format!("archlint-ws-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("core/src")).unwrap();
    std::fs::create_dir_all(dir.join("ffi/src")).unwrap();
    std::fs::create_dir_all(dir.join("ffi/tests")).unwrap();
    std::fs::write(dir.join("core/src/lib.rs"), "pub mod error;").unwrap();
    std::fs::write(dir.join("core/src/error.rs"), "").unwrap();
    std::fs::write(dir.join("ffi/src/lib.rs"), "use core_crate::error::E;").unwrap();
    std::fs::write(dir.join("ffi/tests/abi.rs"), "use ffi::X; use helper::Y;").unwrap();
    let meta = Metadata {
        workspace_root: dir.clone(),
        packages: vec![
            Package {
                name: "core-crate".into(),
                manifest_path: dir.join("core/Cargo.toml"),
                targets: vec![
                    target(
                        "core_crate",
                        "lib",
                        dir.join("core/src/lib.rs").to_str().unwrap(),
                    ),
                    target(
                        "core_crate",
                        "rlib",
                        dir.join("core/src/lib.rs").to_str().unwrap(),
                    ),
                ],
                dependencies: vec![],
            },
            Package {
                name: "ffi".into(),
                manifest_path: dir.join("ffi/Cargo.toml"),
                targets: vec![
                    target("ffi", "lib", dir.join("ffi/src/lib.rs").to_str().unwrap()),
                    target(
                        "abi",
                        "test",
                        dir.join("ffi/tests/abi.rs").to_str().unwrap(),
                    ),
                ],
                dependencies: vec![
                    dep(
                        "core-crate",
                        None,
                        None,
                        Some(dir.join("core").to_str().unwrap()),
                    ),
                    dep(
                        "vendored",
                        None,
                        None,
                        Some(dir.join("third_party/vendored").to_str().unwrap()),
                    ),
                    dep("helper", None, Some("dev"), None),
                    dep("gen", None, Some("build"), None),
                ],
            },
        ],
    };
    let (m, warnings) = build_model(&meta).unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    assert!(warnings.is_empty(), "{warnings:?}");
    let e: Vec<(String, String, Scope)> = m
        .edges
        .iter()
        .map(|e| (e.from.clone(), e.to.clone(), e.scope))
        .collect();
    for want in [
        ("ffi", "core_crate::error", Scope::Lib),
        ("ffi", "core_crate", Scope::Lib),
        ("ffi", "path:third_party/vendored", Scope::Lib),
        ("ffi", "vendored", Scope::Lib),
        ("ffi", "helper", Scope::Test),
        ("ffi::test#abi", "ffi", Scope::Test),
        ("ffi::test#abi", "helper::Y", Scope::Test),
    ] {
        let want = (want.0.to_string(), want.1.to_string(), want.2);
        assert!(e.contains(&want), "missing {want:?} in {e:?}");
    }
    assert!(
        !e.iter().any(|(_, t, _)| t == "path:core"),
        "workspace path deps are crate edges only"
    );
    let via = |to: &str| {
        m.edges
            .iter()
            .find(|e| e.from == "ffi" && e.to == to)
            .unwrap()
            .via
            .clone()
    };
    assert_eq!(via("helper"), "[dev-dependencies] helper");
    assert_eq!(via("vendored"), "[dependencies] vendored");
    assert_eq!(via("gen"), "[build-dependencies] gen");
    assert!(m.modules.contains_key("core_crate::error"));
    assert_eq!(m.workspace_crates.len(), 2);
}

#[test]
fn metadata_reports_cargo_failures() {
    let err = metadata(std::path::Path::new("/nonexistent/Cargo.toml")).unwrap_err();
    assert!(err.starts_with("cargo metadata:"), "{err}");
    assert!(
        err.contains("/nonexistent/Cargo.toml"),
        "carries cargo's stderr: {err}"
    );
}

#[test]
fn metadata_reads_the_workspace() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let meta = metadata(&manifest).unwrap();
    let me = meta
        .packages
        .iter()
        .find(|p| p.name == "archlint")
        .expect("archlint package");
    assert!(me.targets.iter().any(|t| t.kind == ["bin"]));
}
