//! angzarr-embedded's host for remote components: an application's domains,
//! policies and projectors written in another language (or another
//! process) run as child processes speaking gRPC on a Unix socket, in the
//! HashiCorp go-plugin model, and reach the kernel beside its in-process
//! ones: a domain as an angzarr-router [`CommandHandler`], a policy as a
//! [`PolicyComponent`], a projector as a [`ProjectorHandler`].
//!
//! For each remote `component` entry of a [`HostConfig`], [`Host::start`]:
//!
//! 1. launches the child with its launch command (the config's, or its
//!    language's default: [`Language::launched_by`]), `ANGZARR_SOCKET` set
//!    to the socket path the host chose (at most 103 bytes), its stdout
//!    drained to the host's stderr;
//! 2. waits for it to be ready: its gRPC health (`grpc.health.v1`, the
//!    server's) reporting SERVING on that socket within the start limit,
//!    there being no handshake line (decision 14.80), else
//!    `EMBEDDED_REASON_CHILD_NOT_SERVING`, a child exiting first included;
//! 3. watches its health, and calls `ComponentService.Describe`, refusing a child of another
//!    kind than its `kind` says, and one whose descriptor fails the
//!    kernel's one validation (`angzarr_embedded::validate_descriptor`,
//!    the rules a Rust registration meets too); an upcaster sidecar is not
//!    described, and may come up not serving;
//! 4. hands back, by its kind, a [`RemoteCommandHandler`] over
//!    `CommandHandlerService`, a [`RemotePolicyHandler`] over
//!    `PolicyService`, a [`RemoteProjectorHandler`] over
//!    `ProjectorService` or a [`RemoteUpcaster`] over `UpcasterService`
//!    (an upcaster sidecar), which
//!    [`Host::register`] registers with the kernel; a remote process
//!    manager's start rule is the kernel's, from its descriptor's starting
//!    types;
//! 5. supervises it: a child that exits is restarted with backoff; its
//!    health is watched, and checked every interval within a deadline, and
//!    a child unhealthy past its grace is restarted, up to a restart limit
//!    past which it stays down (the child protocol's supervision, decision
//!    14.63); while it is down (exited, unhealthy, or given up) its
//!    dispatch fails retryably ([`is_retryable`]).
//!
//! A child exits when its stdin reaches end of file, or on SIGTERM; the
//! host ends its health watch and closes its stdin to stop it. The launch
//! contract is angzarr-project's `docs/design/child-protocol.md`.
//!
//! [`probe`] takes one child through that contract a step at a time,
//! reporting each as passed or as the keyed failure the host would refuse
//! it with: the conformance check a client library runs against its own
//! child shim.
//!
//! [`CommandHandler`]: angzarr_router::router::CommandHandler
//! [`PolicyComponent`]: angzarr_router::router::PolicyComponent
//! [`ProjectorHandler`]: angzarr_router::router::ProjectorHandler

pub mod app;
pub mod config;
pub mod error;
pub mod exposure;
pub mod host;
pub mod probe;
pub mod remote;
pub mod rpc;
pub mod stores;
pub(crate) mod vitals;
#[cfg(not(unix))]
mod without_unix;

/// The host config's schema crate, whose types [`config`] re-exports.
pub use angzarr_embedded_host_config;
pub use app::{run, run_with, run_with_languages, App, AppBuilder};
pub use config::{
    ComponentConfig, ComponentKindConfig, Expose, HostConfig, Language, Languages, Launch,
    Placement, StoreConfig, Supervision,
};
pub use error::{Embedded, HostError};
pub use exposure::{Arrival, NotExposed, Unserved};
pub use host::{ChildExit, Host};
pub use probe::{probe, Input, Outcome, Probed, Step, PROBE_ROOT};
pub use remote::{
    is_retryable, RemoteCommandHandler, RemotePolicyHandler, RemoteProjectorHandler, RemoteUpcaster,
};
pub use stores::{OneStore, Stores};

/// This crate's en-US Fluent catalog, its name first: what a dependent
/// embeds to render this crate's diagnostics. The text of
/// `locales/en-US/messages.ftl`, its ids under the crate's package name
/// (`angzarr_diagnostic::build`).
pub const EN_US: (&str, &str) = (
    env!("CARGO_PKG_NAME"),
    include_str!(concat!(env!("OUT_DIR"), "/en-US.ftl")),
);

/// One value of every error this crate defines, each variant once, for the
/// catalog parity test.
pub fn diagnostic_samples() -> Vec<Box<dyn angzarr_diagnostic::Diagnostic>> {
    let mut samples = error::diagnostic_samples();
    samples.extend(exposure::diagnostic_samples());
    samples
}

#[cfg(test)]
#[path = "fake_child.test.rs"]
pub(crate) mod fake_child;

#[cfg(test)]
#[path = "clock.test.rs"]
pub(crate) mod clock;
