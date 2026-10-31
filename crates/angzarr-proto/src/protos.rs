//! angzarr's `.proto` sources, embedded, for dependents that compile protos
//! of their own against angzarr's: a domain whose state message carries the
//! `(io.angzarr.v1.state)` option and `(io.angzarr.v1.conflict)` markers
//! must let protoc import `io/angzarr/v1/options.proto`.
//!
//! Use it from a build script, with this crate as a build-dependency:
//!
//! ```ignore
//! // build.rs
//! let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
//! let angzarr = angzarr_proto::protos::write_include_dir(&out.join("angzarr-proto"))?;
//! prost_build::Config::new().compile_protos(&["proto/my/v1/state.proto"], &["proto".into(), angzarr])?;
//! ```
//!
//! The files are the ones this crate's types were generated from, byte for
//! byte, under their import paths, with the `google/api` files meta.proto
//! imports for its HTTP bindings; `google/protobuf/*` comes from protoc's
//! own include directory, as usual. Generate only your own files: angzarr's
//! messages are this crate's (map them with prost-build's `extern_path` if
//! your messages embed any).

use std::io;
use std::path::{Path, PathBuf};

/// One embedded proto: its import path and its contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtoFile {
    /// As an `import` names it, e.g. `io/angzarr/v1/options.proto`.
    pub path: &'static str,
    pub contents: &'static str,
}

macro_rules! embedded {
    ($($path:literal),* $(,)?) => {
        &[$(ProtoFile {
            path: $path,
            contents: include_str!(concat!(env!("OUT_DIR"), "/proto/", $path)),
        }),*]
    };
}

/// Every proto this crate was generated from, and the vendored
/// `google/api` files meta.proto imports (embedded, not generated).
pub const FILES: &[ProtoFile] = embedded![
    "io/angzarr/v1/types.proto",
    "io/angzarr/v1/upcaster.proto",
    "io/angzarr/v1/options.proto",
    "io/angzarr/v1/meta.proto",
    "io/angzarr/v1/reasons.proto",
    "io/angzarr/v1/meta/events.proto",
    "sererr/v1/sererr.proto",
    "google/api/annotations.proto",
    "google/api/http.proto",
];

/// Write [`FILES`] under `dir` (created if needed), each at its import
/// path, and return `dir`: an include directory for protoc.
///
/// # Errors
/// Any I/O error creating the directories or writing a file.
pub fn write_include_dir(dir: &Path) -> io::Result<PathBuf> {
    for file in FILES {
        let target = dir.join(file.path);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&target, file.contents)?;
    }
    Ok(dir.to_path_buf())
}
