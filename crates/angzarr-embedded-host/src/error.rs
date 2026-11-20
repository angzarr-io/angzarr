//! The host's errors: why a remote component could not be started. Each is a
//! code and its args (`angzarr_diagnostic::Diagnostic`), and its
//! `Display` is those, never English. A failure once a domain is running is not
//! one of these: it is the router's [`CodedError`], from
//! [`RemoteCommandHandler`]'s dispatch.
//!
//! [`CodedError`]: angzarr_router::error::CodedError
//! [`RemoteCommandHandler`]: crate::RemoteCommandHandler

use std::fmt;

use angzarr_diagnostic::{code, Arg, Diagnostic, Report};
// A child's failures are the local-process transport's keys (decision
// 14.64).
use angzarr_proto_transport_embedded::v1::{reason_args, EmbeddedReason};

/// A key of the local-process transport (`EMBEDDED_REASON_*`), as the
/// kernel's `SpecKey` answers it: its full name, and how its
/// `(io.angzarr.v1.reason)` option says it surfaces.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Embedded(pub EmbeddedReason);

impl angzarr_embedded::SpecKey for Embedded {
    fn key(self) -> &'static str {
        self.0.as_str_name()
    }
    fn surface(self) -> Option<angzarr_proto::ReasonSurface> {
        self.0.surface()
    }
}

/// Why the host could not start: a config it refuses, or a remote
/// component that did not come up. Every variant about one component names
/// it as `domain`: its `component` entry name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostError {
    /// Two `component` entries with one name.
    DuplicateDomain { domain: String },
    /// A remote component whose language has no default launch command, and
    /// whose config gives none.
    NoLaunch { domain: String, language: String },
    /// A `launch` with no program in it.
    EmptyLaunch { domain: String },
    /// A component whose `language` is not one of the languages loaded
    /// (`config::Languages`): `key` its key path
    /// (`component[1].language`), `loaded` the loaded languages' names, in
    /// order, comma-separated.
    UnknownLanguage {
        key: String,
        language: String,
        loaded: String,
    },
    /// The launch command's program could not be started. `kind` is the I/O
    /// error's `std::io::ErrorKind`, by name.
    Spawn {
        domain: String,
        program: String,
        kind: String,
    },
    /// `ComponentService.Describe` failed; `grpc_code` is the status's wire
    /// code.
    Describe { domain: String, grpc_code: i32 },
    /// A child that did not report its server's health (service `""`)
    /// SERVING on the socket path the host chose within the start limit
    /// (`timeout_ms`), or exited first (docs/design/child-protocol.md §2;
    /// decision 14.80): the spec's `EMBEDDED_REASON_CHILD_NOT_SERVING`.
    /// `status` is the last health status it answered, by its name
    /// (`NOT_SERVING`), or `none` for a child that answered none.
    NotServing {
        component: String,
        status: String,
        timeout_ms: u64,
    },
    /// The child described itself as another kind than its `kind` says:
    /// `described` is its descriptor's `ComponentKind`, by name.
    KindMismatch {
        domain: String,
        configured: String,
        described: String,
    },
    /// A `kind = "upcaster"` component that names no domain to upcast
    /// (`upcasts`).
    UpcasterNoDomain { domain: String },
    /// A component given an `expose` other than `"none"` that is not an
    /// aggregate: `kind` is its `kind`, by its name in the config. Only an aggregate
    /// is served through the host's listener.
    NotExposable { domain: String, kind: String },
    /// The child's descriptor fails the coordinator's validation
    /// (decision 14.42, C-0687; `angzarr_embedded::validate_descriptor`).
    Descriptor {
        domain: String,
        cause: angzarr_embedded::Misconfigured,
    },
    /// A restarted child described another component than its first
    /// child did: another name or kind, or a policy or projector of other
    /// inputs or another output domain. It is not published; the host
    /// takes such a change only when it starts.
    DescriptorChanged { domain: String },
    /// A probed child exited on its stdin's end with a status other than
    /// 0 ([`crate::probe`]): the spec's `EMBEDDED_REASON_CHILD_EXIT_STATUS`.
    ExitStatus { component: String, exit_code: i32 },
    /// A probed child did not exit by itself on its stdin's end within the
    /// shutdown grace ([`crate::probe`]): the spec's
    /// `EMBEDDED_REASON_CHILD_NOT_EXITED`. `ended` is how it ended
    /// ([`ChildExit::ended`](crate::ChildExit::ended)).
    NotExited { component: String, ended: String },
    /// The probe was given an input a child of `kind` does not handle (a
    /// command for a policy, a book of events for a domain): the spec's
    /// `EMBEDDED_REASON_PROBE_INPUT`.
    ProbeInput { component: String, kind: String },
    /// The configured `socket_dir` is too long for a Unix socket path to
    /// hold the host's socket names in it: at most `max_bytes` bytes.
    SocketDirTooLong { dir: String, max_bytes: u64 },
    /// The config file could not be read. `kind` is the I/O error's
    /// `std::io::ErrorKind`, by name.
    ConfigUnreadable { path: String, kind: String },
    /// The config file does not load: YAML the reader does not take, a
    /// `schema_version` newer than this build reads or older than the
    /// oldest it reads (none is version 0), an unknown key, a missing one,
    /// a value outside its closed set or of the wrong type. `cause` is the
    /// reader's error, angzarr-yaml's `LoadError`, naming for a decode the
    /// key path where it goes wrong (`component[1].kind`).
    ConfigInvalid { path: String, cause: Report },
    /// An in-process component its `component` entry lists (`kind` its kind
    /// in the config) that the application's `register` did not register: no
    /// domain, policy or projector of that name, or, for an upcaster, no
    /// domain here of its `upcasts`.
    NotRegistered { domain: String, kind: String },
    /// A component the application's `register` registered in process
    /// (`kind` its kind in the config) that no `component` entry lists as an
    /// in-process component of that kind: every component is listed.
    NotListed { domain: String, kind: String },
    /// The registry refused a domain (the kernel's `Misconfigured`).
    Register { cause: Report },
    /// The store could not be opened (the kernel's store error).
    Store { cause: Report },
    /// The host's own runtime could not be built. `kind` is the I/O error's
    /// `std::io::ErrorKind`, by name.
    Runtime { kind: String },
}

impl fmt::Display for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for HostError {
    fn code(&self) -> &str {
        match self {
            HostError::DuplicateDomain { .. } => code!("duplicate-domain"),
            HostError::NoLaunch { .. } => code!("no-launch"),
            HostError::EmptyLaunch { .. } => code!("empty-launch"),
            HostError::UnknownLanguage { .. } => code!("unknown-language"),
            HostError::Spawn { .. } => code!("spawn"),
            HostError::Describe { .. } => code!("describe"),
            HostError::NotServing { .. } => EmbeddedReason::ChildNotServing.as_str_name(),
            HostError::KindMismatch { .. } => code!("kind-mismatch"),
            HostError::Descriptor { .. } => code!("descriptor"),
            HostError::DescriptorChanged { .. } => code!("descriptor-changed"),
            HostError::UpcasterNoDomain { .. } => code!("upcaster-no-domain"),
            HostError::NotExposable { .. } => code!("not-exposable"),
            HostError::ExitStatus { .. } => EmbeddedReason::ChildExitStatus.as_str_name(),
            HostError::NotExited { .. } => EmbeddedReason::ChildNotExited.as_str_name(),
            HostError::ProbeInput { .. } => EmbeddedReason::ProbeInput.as_str_name(),
            HostError::SocketDirTooLong { .. } => code!("socket-dir-too-long"),
            HostError::ConfigUnreadable { .. } => code!("config-unreadable"),
            HostError::ConfigInvalid { .. } => code!("config-invalid"),
            HostError::NotRegistered { .. } => code!("not-registered"),
            HostError::NotListed { .. } => code!("not-listed"),
            HostError::Register { .. } => code!("register"),
            HostError::Store { .. } => code!("store"),
            HostError::Runtime { .. } => code!("runtime"),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        let domain = |d: &str| ("domain", Arg::from(d));
        let component = |c: &str| (reason_args::COMPONENT, Arg::from(c));
        match self {
            HostError::DuplicateDomain { domain: d }
            | HostError::EmptyLaunch { domain: d }
            | HostError::DescriptorChanged { domain: d }
            | HostError::UpcasterNoDomain { domain: d } => {
                vec![domain(d)]
            }
            HostError::NoLaunch {
                domain: d,
                language,
            } => vec![domain(d), ("language", language.as_str().into())],
            HostError::Spawn {
                domain: d,
                program,
                kind,
            } => vec![
                domain(d),
                ("program", program.as_str().into()),
                ("kind", kind.as_str().into()),
            ],
            HostError::ExitStatus {
                component: c,
                exit_code,
            } => vec![component(c), (reason_args::EXIT_CODE, (*exit_code).into())],
            HostError::NotExited {
                component: c,
                ended,
            } => {
                vec![component(c), (reason_args::ENDED, ended.as_str().into())]
            }
            HostError::ProbeInput { component: c, kind } => {
                vec![component(c), (reason_args::KIND, kind.as_str().into())]
            }
            HostError::NotExposable { domain: d, kind }
            | HostError::NotRegistered { domain: d, kind }
            | HostError::NotListed { domain: d, kind } => {
                vec![domain(d), ("kind", kind.as_str().into())]
            }
            HostError::NotServing {
                component: c,
                status,
                timeout_ms,
            } => vec![
                component(c),
                (reason_args::STATUS, status.as_str().into()),
                (reason_args::TIMEOUT_MS, (*timeout_ms).into()),
            ],
            HostError::Describe {
                domain: d,
                grpc_code,
            } => vec![domain(d), ("grpc_code", (*grpc_code).into())],
            HostError::KindMismatch {
                domain: d,
                configured,
                described,
            } => vec![
                domain(d),
                ("configured", configured.as_str().into()),
                ("described", described.as_str().into()),
            ],
            HostError::Descriptor { domain: d, cause } => {
                vec![domain(d), ("cause", Arg::nested(cause))]
            }
            HostError::SocketDirTooLong { dir, max_bytes } => vec![
                ("dir", dir.as_str().into()),
                ("max_bytes", (*max_bytes).into()),
            ],
            HostError::UnknownLanguage {
                key,
                language,
                loaded,
            } => vec![
                ("key", key.as_str().into()),
                ("language", language.as_str().into()),
                ("loaded", loaded.as_str().into()),
            ],
            HostError::ConfigUnreadable { path, kind } => vec![
                ("path", path.as_str().into()),
                ("kind", kind.as_str().into()),
            ],
            HostError::ConfigInvalid { path, cause } => vec![
                ("path", path.as_str().into()),
                ("cause", Arg::nested(cause)),
            ],
            HostError::Register { cause } | HostError::Store { cause } => {
                vec![("cause", Arg::nested(cause))]
            }
            HostError::Runtime { kind } => vec![("kind", kind.as_str().into())],
        }
    }
}

impl std::error::Error for HostError {}

/// One value of every error this module defines, each variant once.
pub(crate) fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    let host = component_samples().into_iter().chain(host_samples());
    host.map(|e| Box::new(e) as _).collect()
}

/// Each [`HostError`] variant about one component, which it names.
fn component_samples() -> Vec<HostError> {
    let d = || "player".to_string();
    vec![
        HostError::DuplicateDomain { domain: d() },
        HostError::NoLaunch {
            domain: d(),
            language: "go".to_string(),
        },
        HostError::EmptyLaunch { domain: d() },
        HostError::UnknownLanguage {
            key: "component[1].language".to_string(),
            language: "go".to_string(),
            loaded: "python,rust".to_string(),
        },
        HostError::Spawn {
            domain: d(),
            program: "uv".to_string(),
            kind: "NotFound".to_string(),
        },
        HostError::Describe {
            domain: d(),
            grpc_code: 14,
        },
        HostError::NotServing {
            component: d(),
            status: "NOT_SERVING".to_string(),
            timeout_ms: 5000,
        },
        HostError::KindMismatch {
            domain: d(),
            configured: "policy".to_string(),
            described: "AGGREGATE".to_string(),
        },
        HostError::UpcasterNoDomain { domain: d() },
        HostError::NotExposable {
            domain: d(),
            kind: "policy".to_string(),
        },
        HostError::Descriptor {
            domain: d(),
            cause: angzarr_embedded::Misconfigured::PolicyNoOutputDomain {
                component: "TableWalletPolicy".to_string(),
            },
        },
        HostError::DescriptorChanged { domain: d() },
        HostError::ExitStatus {
            component: d(),
            exit_code: 3,
        },
        HostError::NotExited {
            component: d(),
            ended: "terminated".to_string(),
        },
        HostError::ProbeInput {
            component: d(),
            kind: "policy".to_string(),
        },
        HostError::NotRegistered {
            domain: d(),
            kind: "policy".to_string(),
        },
        HostError::NotListed {
            domain: d(),
            kind: "projector".to_string(),
        },
    ]
}

/// Each [`HostError`] variant of the host as a whole: its config, its
/// socket directory, its registry, its store and its runtime.
fn host_samples() -> Vec<HostError> {
    vec![
        HostError::SocketDirTooLong {
            dir: "/very/deep/directory".to_string(),
            max_bytes: 65,
        },
        HostError::ConfigUnreadable {
            path: "angzarr.yaml".to_string(),
            kind: "NotFound".to_string(),
        },
        HostError::ConfigInvalid {
            path: "angzarr.yaml".to_string(),
            cause: Report::of(
                &crate::config::HostConfig::from_yaml("schema_version: 99\n", "angzarr.yaml")
                    .expect_err("a newer config is refused"),
            ),
        },
        HostError::Register {
            cause: Report::new(code!("duplicate-domain")).arg("domain", "player"),
        },
        HostError::Store {
            cause: Report::new(code!("runtime")).arg("kind", "Other"),
        },
        HostError::Runtime {
            kind: "Other".to_string(),
        },
    ]
}
