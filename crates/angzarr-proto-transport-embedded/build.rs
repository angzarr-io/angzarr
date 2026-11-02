//! Generates angzarr's local-process transport,
//! `io.angzarr.transport.embedded.v1` (decision 14.64): every `.proto`
//! under `io/angzarr/transport/embedded/v1/` of angzarr-project's `proto/`
//! (the `angzarr-project` submodule), prost only, core's messages
//! extern-pathed onto angzarr-proto. It declares no service.
//!
//! From the encoded `FileDescriptorSet` (also the library's
//! `FILE_DESCRIPTOR_SET`):
//! - the package's reason keys' args, to `$OUT_DIR/reason_args.rs`, and its
//!   en-US catalog copied to `$OUT_DIR` (angzarr-proto's
//!   `build_reasons.rs`);
//! - the child protocol's constants, read from `child_protocol.proto`'s
//!   `(child_protocol)` file option (prost does not generate options), to
//!   `$OUT_DIR/child_protocol.rs`: the spec states them once, and no code
//!   types them again.
//!
//! The proto root is exported to the tests as `ANGZARR_PROTO_ROOT`.
//! Requires `protoc` on PATH (or `PROTOC` set).

use std::fmt::Write as _;
use std::path::PathBuf;

#[path = "../angzarr-proto/build_descriptor.rs"]
mod descriptor;
#[path = "../angzarr-proto/build_reasons.rs"]
mod reasons;
#[path = "../angzarr-proto/build_support.rs"]
mod support;

/// The local-process transport's package, under the proto root.
const PACKAGE: &str = "io/angzarr/transport/embedded/v1";

/// The file whose option declares the constants.
const CHILD_PROTOCOL: &str = "io/angzarr/transport/embedded/v1/child_protocol.proto";

/// The `(child_protocol)` extension's number on `FileOptions`.
const CHILD_PROTOCOL_OPTION: u64 = 50110;

fn main() {
    let root = support::proto_root();
    let files = support::package_files(&root, PACKAGE);
    println!("cargo:rustc-env=ANGZARR_PROTO_ROOT={}", root.display());
    support::rerun_if_changed(&files);
    let out = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));

    let mut config = prost_build::Config::new();
    config
        .enable_type_names()
        .file_descriptor_set_path(out.join("file_descriptor_set.bin"))
        .bytes(["."])
        .extern_path(".io.angzarr.v1", "::angzarr_proto::v1")
        .compile_protos(&files, std::slice::from_ref(&root))
        .expect("prost: compile angzarr's io.angzarr.transport.embedded.v1 protos");

    let set = std::fs::read(out.join("file_descriptor_set.bin")).expect("read the descriptor set");
    reasons::reason_keys(&root, &out, &set, PACKAGE, &["HostReason"]);
    std::fs::write(out.join("child_protocol.rs"), child_protocol(&set))
        .expect("write child_protocol.rs");
}

/// The constants module for the `(child_protocol)` option set on
/// `child_protocol.proto` in the encoded descriptor set `set`: one constant
/// per `ChildProtocol` field, each required.
fn child_protocol(set: &[u8]) -> String {
    let options = descriptor::messages(descriptor::file(set, CHILD_PROTOCOL), 8)
        .last()
        .unwrap_or_else(|| panic!("{CHILD_PROTOCOL} sets no file option"));
    let constants = descriptor::messages(options, CHILD_PROTOCOL_OPTION)
        .last()
        .unwrap_or_else(|| panic!("{CHILD_PROTOCOL} does not set (child_protocol)"));
    let text = |number: u64, field: &str| {
        let value = descriptor::text(constants, number);
        assert!(!value.is_empty(), "(child_protocol) sets no {field}");
        value
    };
    let max = descriptor::varints(constants, 5)
        .last()
        .expect("(child_protocol) sets no max_socket_path_bytes");
    let mut out = String::new();
    for (name, doc, value) in [
        (
            "HANDSHAKE_PREFIX",
            "The handshake line's first word.",
            text(1, "handshake_prefix"),
        ),
        (
            "PROTOCOL_VERSION",
            "The handshake line's format version.",
            text(2, "protocol_version"),
        ),
        (
            "ADDRESS_SCHEME",
            "The scheme before a handshake's socket path.",
            text(3, "address_scheme"),
        ),
        (
            "SOCKET_ENV",
            "The environment variable a host names the socket path in.",
            text(4, "socket_env"),
        ),
    ] {
        writeln!(out, "/// {doc}\npub const {name}: &str = {value:?};").expect("write to a String");
    }
    writeln!(
        out,
        "/// The longest socket path a host gives, in bytes: a Unix socket\n\
         /// address's `sun_path`, less its NUL.\npub const MAX_SOCKET_PATH_BYTES: usize = {max};"
    )
    .expect("write to a String");
    out
}
