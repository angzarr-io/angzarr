//! `proto_gen_python`: `buf generate` in the project, then every module it
//! wrote headed with the generated marker and its imports rerooted, the
//! project's relative within `_gen` (item S). A `buf` of the test's own,
//! first on the PATH, writes what buf would. Alone in its binary, as it
//! sets the PATH for the process.

use angzarr_build::{BuildError, ProtoInput, PythonApp, proto_gen_python};

use angzarr_test_dir::TestDir;

fn app() -> PythonApp {
    PythonApp {
        domain: "player".to_string(),
        protos: ProtoInput::Directory("proto".to_string()),
        rules_version: "0.1.0".to_string(),
    }
}

/// A `buf` in `bin` running `script`.
fn buf(bin: &std::path::Path, script: &str) {
    let path = bin.join("buf");
    std::fs::write(&path, format!("#!/bin/sh\n{script}\n")).unwrap();
    std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
}

#[test]
fn proto_gen_python_heads_and_reroots_what_buf_writes_and_fails_as_buf_does() {
    let bin = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-buf-bin"));
    let path = std::env::var_os("PATH").unwrap_or_default();
    let path = std::env::join_paths(
        std::iter::once(bin.to_path_buf()).chain(std::env::split_paths(&path)),
    )
    .unwrap();
    // SAFETY: this binary's one test; nothing else reads the environment.
    unsafe { std::env::set_var("PATH", path) };

    let root = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-proto-gen"));
    buf(
        &bin,
        "d=src/angzarr_player/_gen/io/angzarr/examples/v1\nmkdir -p $d\n\
         printf 'from io.angzarr.examples.v1 import cards_pb2 as c\\nfrom io.angzarr.v1 import options_pb2 as o\\n' > $d/player_pb2.py\n\
         : > $d/cards_pb2.py",
    );
    proto_gen_python(&root, &app()).unwrap();
    let pb2 = std::fs::read_to_string(
        root.join("src/angzarr_player/_gen/io/angzarr/examples/v1/player_pb2.py"),
    )
    .unwrap();
    let (header, rest) = pb2.split_once('\n').unwrap();
    assert!(angzarr_codegen::is_generated(header), "{pb2}");
    assert_eq!(
        rest,
        "from . import cards_pb2 as c\n\
         from angzarr_client.proto.io.angzarr.v1 import options_pb2 as o\n"
    );

    buf(&bin, "echo broken >&2\nexit 3");
    assert_eq!(
        proto_gen_python(&root, &app()),
        Err(BuildError::Buf {
            detail: "broken".to_string()
        })
    );
}
