//! Generates the `io.angzarr.sealed.v1` messages from this crate's own
//! proto, and writes the crate's catalogs (coded_diagnostic::build).
//! Requires `protoc` on PATH (or `PROTOC` set).

fn main() {
    coded_diagnostic::build::catalogs();
    let proto = "proto/io/angzarr/sealed/v1/sealed.proto";
    println!("cargo:rerun-if-changed={proto}");
    prost_build::Config::new()
        // Every `bytes` field is a `bytes::Bytes`, as angzarr-proto has it.
        .bytes(["."])
        .compile_protos(&[proto], &["proto"])
        .expect("prost: compile the sealed proto");
}
