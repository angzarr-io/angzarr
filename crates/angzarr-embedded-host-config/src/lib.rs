//! The schema of an angzarr-embedded application's host config,
//! `angzarr.yaml` ([`FILE_NAME`]): EVERY component of the application,
//! in-process and remote, what kind each is, the language each is written
//! in, and how each remote one is launched. The CLI writes it once, beside
//! the thin `main.rs` it generates, and it is the user's from then on;
//! angzarr-embedded-host runs it, and the CLI's `lint app` holds the
//! generated code to it. Both read it here, through one [`loader`].
//!
//! ```yaml
//! schema_version: 4
//! component:
//!   - name: table
//!     kind: domain              # required: domain, policy, projector or upcaster
//!     language: rust            # in-process: registered by main.rs, not launched
//!     expose: loopback          # reachable through the host's listener from
//!                               # this machine; ip from the network; none (the
//!                               # default) from nowhere
//!     scaffold:
//!       snapshots: true         # `angzarr scaffold rust` writes its implementation
//!                               # taking snapshots (false, the default: none)
//!   - name: policy-table-player
//!     kind: policy              # in-process too: main.rs registers it
//!     language: rust
//!   - name: player
//!     kind: domain
//!     language: python          # launched as `uv run --locked -m player.serve`
//!     entry: [-m, player.serve]
//!     dir: domains/player
//!   - name: table-player
//!     kind: policy              # a PolicyService child
//!     language: python
//!     entry: [-m, table_player.serve]
//!   - name: table-upcaster
//!     kind: upcaster            # the table's inputs are upcast through it
//!     upcasts: table
//!     language: python
//!     entry: [-m, table_upcaster.serve]
//!   - name: dealer
//!     kind: domain
//!     language: go              # loaded by the application, with no default:
//!     launch: [./bin/dealer]    # the launch command is given
//! supervision:
//!   handshake_timeout_ms: 10000
//!   health_check_interval_ms: 5000  # a grpc.health.v1 Check of each child,
//!   health_check_deadline_ms: 2000  # answered SERVING within this, or it is
//!                                   # unhealthy;
//!   unhealthy_grace_ms: 15000       # unhealthy this long, it is restarted,
//!   unhealthy_restart_limit: 3      # at most this often, then left down,
//!   unhealthy_reset_ms: 600000      # the count clearing once a child is
//!                                   # healthy this long, unbroken
//! store:
//!   path: angzarr.db            # SQLite; `in_memory: true` for none on disk
//!   snapshots:                  # the snapshot role's storage policy
//!     every: 1                  # the events a stream gains between the
//!                               # snapshots stored of it: 1, every write, by
//!                               # default; the newest only is kept
//!     domain:
//!       table:
//!         every: 10             # a domain's own, in place of `every`
//! policy:
//!   on_bug: move_past           # a policy step's bug, dead-lettered, moves its
//!                               # position past the trigger; stay keeps it
//! retry:
//!   max_retries: 3              # a retryable failure is retried this often
//!                               # (0: never), then dead-lettered; waiting
//!   max_race_runs: 10           # a command that keeps losing its write to
//!                               # another write to its stream runs this often,
//!                               # then is dead-lettered (0: never again, the
//!                               # conflict back to the caller, retryable, not
//!                               # dead-lettered); waiting
//!   initial_backoff_ms: 100     # this long first,
//!   multiplier: 2.0             # this many times longer each retry,
//!   max_backoff_ms: 5000        # at most this long,
//!   jitter: 0.2                 # less up to this fraction of it at random
//! ```
//!
//! - **Versioned.** The file is stamped `schema_version` ([`KEY`]), an
//!   unsigned integer that only increases; this build reads version
//!   [`CURRENT`]. A newer file is refused (`angzarr-yaml-too-new`), never
//!   half-read. A file with no stamp (an empty or comment-only file
//!   among them) is the oldest generation, version 0, which has version
//!   1's shape, and loads through every step. A later version adds a step
//!   from the one before it to [`loader`], and a fixture of the old version
//!   to [`FIXTURES`].
//! - **Strict.** Every mapping refuses a key it does not know, a missing
//!   required key (a component's `name`, `language` or `kind`) is refused,
//!   and so is a value outside a closed set (`kind`, `expose`,
//!   `policy.on_bug`) or of the wrong type: each as an
//!   [`angzarr_yaml::versioned::LoadError`] whose problem names the key
//!   path where it goes wrong (`component[1].kind`, `retry.retries`).
//! - **`language`** is a closed set too, but of the languages the running
//!   application loads, which this schema cannot know: angzarr-embedded-host
//!   checks it.
//!
//! A component's `kind` is required, with no default: `domain` (an
//! aggregate or a process manager; a child serves
//! `CommandHandlerService`), `policy` (`PolicyService`), `projector`
//! (`ProjectorService`) or `upcaster` (an upcaster, naming the domain it
//! `upcasts`; a child is a sidecar, decision 14.39, serving
//! `UpcasterService` and gRPC health).

use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::path::PathBuf;

use angzarr_yaml::versioned::{Fixture, LoadError, Loader, Migrations};
use serde::Deserialize;

pub use angzarr_yaml;

/// The config's file name, in the application's root.
pub const FILE_NAME: &str = "angzarr.yaml";

/// The document kind the loader's errors name.
pub const KIND: &str = "angzarr-host-config";

/// The root key the file's version is stamped under.
pub const KEY: &str = angzarr_yaml::versioned::DEFAULT_KEY;

/// The version this build reads and writes.
pub const CURRENT: u32 = 4;

/// The config's loader: kind [`KIND`], stamped under [`KEY`], at version
/// [`CURRENT`], with a step from every older version it still reads:
///
/// - **0 to 1**: a file with no stamp, written before the stamp, which
///   already had version 1's shape: nothing but the stamp, which the loader
///   writes.
/// - **1 to 2**: `supervision` gains its health keys
///   (`health_check_interval_ms`, `health_check_deadline_ms`,
///   `unhealthy_grace_ms`, `unhealthy_restart_limit`; decision 14.63).
///   A key left out is its default, so the step writes none: a version-1
///   file is a version-2 file left at its defaults, and the stamp is what
///   tells a version-1 reader to refuse a file that may give them (as too
///   new, never as an unknown key).
/// - **2 to 3**: `supervision` gains `unhealthy_reset_ms`, the healthy
///   window after which a component's restart count clears (decision
///   14.67), its default left in place as version 2's health keys were.
/// - **3 to 4**: a `component` entry gains `scaffold` (whether `angzarr
///   scaffold rust` writes its implementation taking snapshots), and
///   `store` gains `snapshots` (how often a domain's snapshots are
///   stored), each left at its default.
pub fn loader() -> Loader {
    Migrations::new(KIND, CURRENT)
        .step(0, same_shape)
        .step(1, same_shape)
        .step(2, same_shape)
        .step(3, same_shape)
        .build()
        .expect("the host config's migration chain is whole")
}

/// A step that edits nothing: the next version reads the file as it is,
/// a key it adds left at its default.
fn same_shape(_: &mut angzarr_yaml::Value) -> Result<(), angzarr_yaml::versioned::MigrationError> {
    Ok(())
}

/// A config at every version the [`loader`] reads, and what each must
/// load to: `Loader::assert_fixtures` holds old files to the current
/// schema.
pub const FIXTURES: &[Fixture<'static>] = &[
    Fixture {
        name: "fixtures/v0-unstamped.yaml",
        input: include_str!("../fixtures/v0-unstamped.yaml"),
        expected: Some(include_str!("../fixtures/v0-unstamped.expected.yaml")),
    },
    Fixture {
        name: "fixtures/v1-documented.yaml",
        input: include_str!("../fixtures/v1-documented.yaml"),
        expected: Some(include_str!("../fixtures/v1-documented.expected.yaml")),
    },
    Fixture {
        name: "fixtures/v1-minimal.yaml",
        input: include_str!("../fixtures/v1-minimal.yaml"),
        expected: Some(include_str!("../fixtures/v1-minimal.expected.yaml")),
    },
    Fixture {
        name: "fixtures/v2-documented.yaml",
        input: include_str!("../fixtures/v2-documented.yaml"),
        expected: Some(include_str!("../fixtures/v2-documented.expected.yaml")),
    },
    Fixture {
        name: "fixtures/v2-minimal.yaml",
        input: include_str!("../fixtures/v2-minimal.yaml"),
        expected: Some(include_str!("../fixtures/v2-minimal.expected.yaml")),
    },
    Fixture {
        name: "fixtures/v3-documented.yaml",
        input: include_str!("../fixtures/v3-documented.yaml"),
        expected: Some(include_str!("../fixtures/v3-documented.expected.yaml")),
    },
    Fixture {
        name: "fixtures/v3-minimal.yaml",
        input: include_str!("../fixtures/v3-minimal.yaml"),
        expected: Some(include_str!("../fixtures/v3-minimal.expected.yaml")),
    },
    Fixture {
        name: "fixtures/v4-documented.yaml",
        input: include_str!("../fixtures/v4-documented.yaml"),
        expected: None,
    },
    Fixture {
        name: "fixtures/v4-minimal.yaml",
        input: include_str!("../fixtures/v4-minimal.yaml"),
        expected: None,
    },
];

/// The host's whole config. Every mapping refuses a key it does not know.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostConfig {
    /// Every component of the application, in-process and remote: the
    /// `component` entries.
    #[serde(rename = "component", default)]
    pub components: Vec<ComponentConfig>,
    /// How the host waits for, restarts and stops its children.
    #[serde(default)]
    pub supervision: Supervision,
    /// The store the host opens.
    #[serde(default)]
    pub store: StoreConfig,
    /// What the kernel does when a policy's step hits a bug.
    #[serde(default)]
    pub policy: PolicyConfig,
    /// How the kernel retries a step's retryable failure.
    #[serde(default)]
    pub retry: RetryConfig,
}

impl HostConfig {
    /// `text`, a config's YAML, read through the [`loader`] and decoded
    /// strictly. `source` (its path, say) is named in the error. Refused:
    /// YAML this reader does not take, a stamp that is not an unsigned
    /// integer, a version newer than [`CURRENT`] or older than the oldest
    /// the loader reads (no stamp is version 0), an unknown key, a missing
    /// required one, and a value outside its closed set or of the wrong
    /// type, each at its key path.
    pub fn from_yaml(text: &str, source: &str) -> Result<HostConfig, LoadError> {
        loader().load_as(Some(source), text, |doc| {
            let mut doc = doc.clone();
            doc.remove(KEY);
            angzarr_yaml::from_value::<HostConfig>(&doc)
        })
    }
}

/// The text of a config at the [`CURRENT`] version holding `body`, the
/// YAML of its mappings (`component:`, `store:` and the rest), which
/// must not stamp a version itself: the stamp, then `body`.
pub fn stamped(body: &str) -> String {
    format!("{KEY}: {CURRENT}\n{body}")
}

/// The `retry` mapping: how the kernel retries a step whose failure is
/// retryable (UNAVAILABLE, ABORTED, UNIMPLEMENTED, DEADLINE_EXCEEDED,
/// RESOURCE_EXHAUSTED and the like), with exponential backoff and jitter,
/// before it dead-letters it. Its defaults are the kernel's own
/// (angzarr-embedded's `OnRetryable`). Retrying is on with no config;
/// `max_retries: 0` turns it off. Every key has a default, so the mapping
/// may be left out.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RetryConfig {
    /// Retries after the first attempt, then the step is dead-lettered; 0
    /// turns retrying off.
    pub max_retries: u32,
    /// Runs of a command stamped with the head it loaded while it loses its
    /// write to another write to its stream, the first included: the race
    /// re-run cap (decision 14.46), then it is dead-lettered. 0 turns race
    /// re-runs off: the first lost race goes back to the caller as the
    /// store's conflict, retryable, and is not dead-lettered.
    pub max_race_runs: u32,
    /// The wait before the first retry.
    pub initial_backoff_ms: u64,
    /// What each retry's wait is the last one's times.
    pub multiplier: f64,
    /// The longest wait between retries.
    pub max_backoff_ms: u64,
    /// The most of each wait taken off at random, as a fraction of it (0
    /// to 1).
    pub jitter: f64,
}

impl Default for RetryConfig {
    /// The kernel's own defaults.
    fn default() -> Self {
        RetryConfig {
            max_retries: 3,
            max_race_runs: 10,
            initial_backoff_ms: 100,
            multiplier: 2.0,
            max_backoff_ms: 5_000,
            jitter: 0.2,
        }
    }
}

/// The `policy` mapping: how the kernel treats a policy whose step hits a
/// bug. Every key has a default, so the mapping may be left out.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct PolicyConfig {
    /// What a policy step that hits a bug (the policy's own error, or a
    /// handler's bug in the step) does with the policy's position once the
    /// step is dead-lettered: `move_past` the trigger (the default: the
    /// dead letter is the record to replay) or `stay` at it, so a resume
    /// runs the step again. The kernel's `OnPolicyBug`.
    pub on_bug: OnBug,
}

/// The `policy` mapping's `on_bug`: `move_past` or `stay`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnBug {
    /// Past the trigger: the dead letter is the record to replay.
    #[default]
    MovePast,
    /// At the trigger: a resume runs the step again.
    Stay,
}

/// The `store` mapping: the application's SQLite store, and its storage
/// policy for snapshots.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StoreConfig {
    /// The SQLite file, created when absent; relative to the working
    /// directory. `angzarr.db` by default.
    pub path: PathBuf,
    /// An in-memory SQLite store instead, gone when the application ends;
    /// `path` is then unused.
    pub in_memory: bool,
    /// How often each domain's snapshots are stored.
    pub snapshots: SnapshotsConfig,
}

impl Default for StoreConfig {
    fn default() -> Self {
        StoreConfig {
            path: PathBuf::from("angzarr.db"),
            in_memory: false,
            snapshots: SnapshotsConfig::default(),
        }
    }
}

/// The `store` mapping's `snapshots`: the snapshot role's storage policy,
/// per domain (angzarr's docs/design/snapshots.md section 1; angzarr core
/// names the role `storage.snapshots`). Snapshots are a framework
/// optimisation: a state offers one with every answer, and this says which
/// are stored. The newest only is kept. The kernel's `SnapshotPolicy`;
/// every key has a default, so the mapping may be left out.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SnapshotsConfig {
    /// The events a stream gains between the snapshots stored of it, at
    /// least 1: 1, every write, by default.
    pub every: NonZeroU64,
    /// A domain's own, by its name, in place of `every`.
    pub domain: BTreeMap<String, DomainSnapshots>,
}

impl Default for SnapshotsConfig {
    fn default() -> Self {
        SnapshotsConfig {
            every: NonZeroU64::MIN,
            domain: BTreeMap::new(),
        }
    }
}

/// One domain's entry of `store.snapshots.domain`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DomainSnapshots {
    /// The events its stream gains between the snapshots stored of it, at
    /// least 1.
    pub every: NonZeroU64,
}

/// One `component` entry.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentConfig {
    /// The component's name. An in-process domain's is its component name
    /// (decision 14.50): an aggregate's domain, a process manager's `pmg-`
    /// and its domain. A remote domain is registered as the domain this
    /// names.
    pub name: String,
    /// The language it is written in: one of the languages the application
    /// loads (`rust`, `python`, and any it adds), lowercase.
    pub language: String,
    /// What it is, required: `domain` (an aggregate or a process
    /// manager; a child serves `CommandHandlerService`), `policy`
    /// (`PolicyService`), `projector` (`ProjectorService`) or
    /// `upcaster` (a child is a sidecar, serving `UpcasterService` and
    /// gRPC health).
    pub kind: ComponentKindConfig,
    /// An upcaster's: the domain whose payloads it upcasts (decision
    /// 14.39); unset for every other kind.
    #[serde(default)]
    pub upcasts: Option<String>,
    /// The command that starts its child, program first. Overrides the
    /// language's default (and its `entry`) entirely.
    #[serde(default)]
    pub launch: Option<Vec<String>>,
    /// The arguments after the language's default launch command: the
    /// module or entry point (`[-m, player.serve]`, `[player-serve]`).
    /// Unused when `launch` is given.
    #[serde(default)]
    pub entry: Vec<String>,
    /// The child's working directory; the application's when absent.
    #[serde(default)]
    pub dir: Option<PathBuf>,
    /// Environment variables set for the child, beside the host's own and
    /// `ANGZARR_SOCKET`.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Who may send it commands from outside the application, through the
    /// host's one listener (`CommandHandlerCoordinatorService`, routed by
    /// `cover.domain`, this component's `name`): nobody (`none`, the
    /// default), this machine only (`loopback`) or the network (`ip`).
    /// Only an aggregate may be exposed.
    #[serde(default)]
    pub expose: Expose,
    /// How `angzarr scaffold rust` writes its implementation (a Rust
    /// aggregate's or process manager's): nothing beyond the required
    /// methods by default.
    #[serde(default)]
    pub scaffold: ComponentScaffold,
}

/// A `component` entry's `scaffold`: what `angzarr scaffold rust` writes
/// into its implementation beyond the required methods. Read by the
/// scaffold alone, never by the host.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ComponentScaffold {
    /// Its implementation takes snapshots: the scaffold writes its
    /// interface's `snapshot` and `restore` over the state it holds. An
    /// implementation may override them later all the same.
    pub snapshots: bool,
}

/// A `component` entry's `expose`: from how far a command may reach the
/// component through the host's listener. Ordered: an exposure admits
/// every request a narrower one does.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Expose {
    /// Not reachable from outside the application.
    #[default]
    None,
    /// Reachable from this machine: a request arriving on loopback.
    Loopback,
    /// Reachable from the network: a request arriving on any address.
    Ip,
}

impl Expose {
    /// Its form in the file: `none`, `loopback` or `ip`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Expose::None => "none",
            Expose::Loopback => "loopback",
            Expose::Ip => "ip",
        }
    }
}

/// A `component` entry's `kind`, required: what the component is, and for
/// a child, which of angzarr's component services it serves. A child's
/// descriptor's kind must agree.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComponentKindConfig {
    /// An aggregate or a process manager (`CommandHandlerService`); which,
    /// its descriptor says.
    Domain,
    /// A policy (`PolicyService`).
    Policy,
    /// A projector (`ProjectorService`).
    Projector,
    /// An upcaster sidecar (`UpcasterService` and gRPC health, decision
    /// 14.39) for the domain its component `upcasts`: no descriptor, no
    /// coordinator of its own.
    Upcaster,
}

impl ComponentKindConfig {
    /// Its form in the file: `domain`, `policy`, `projector` or
    /// `upcaster`.
    pub fn as_str(&self) -> &'static str {
        match self {
            ComponentKindConfig::Domain => "domain",
            ComponentKindConfig::Policy => "policy",
            ComponentKindConfig::Projector => "projector",
            ComponentKindConfig::Upcaster => "upcaster",
        }
    }
}

/// How the host supervises its children. Every key has a default, so the
/// `supervision` mapping, and any key in it, may be left out.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Supervision {
    /// How long a child has, from its start, to report SERVING on its
    /// socket's health service.
    pub handshake_timeout_ms: u64,
    /// The wait before the first restart of a child that exited; each
    /// restart that fails doubles it, up to `max_backoff_ms`, and a child
    /// that comes up resets it.
    pub initial_backoff_ms: u64,
    /// The longest wait between restarts.
    pub max_backoff_ms: u64,
    /// How long a child has, once its stdin is closed at shutdown, to exit
    /// before it is sent SIGTERM.
    pub shutdown_grace_ms: u64,
    /// How long a child has, once sent SIGTERM, to exit before it is
    /// killed (SIGKILL).
    pub terminate_grace_ms: u64,
    /// How long a dispatch waits for the child's answer; past it, the call
    /// fails retryably (`cause = timeout`).
    pub call_timeout_ms: u64,
    /// How often the host sends each child a grpc.health.v1 `Check` of
    /// its overall server, once it is serving (the child protocol's
    /// supervision, decision 14.63).
    pub health_check_interval_ms: u64,
    /// How long a `Check` has to be answered; past it, the child is
    /// unhealthy, as it is when the check answers other than SERVING.
    pub health_check_deadline_ms: u64,
    /// How long a child may stay unhealthy, unbroken, before it is
    /// restarted: judged each time its health is seen, so at the first
    /// check (or health watch report) at or after it.
    pub unhealthy_grace_ms: u64,
    /// How many times a component's child is restarted for being
    /// unhealthy past its grace; past it, the child is stopped and none
    /// started, the component down until the host restarts (0: never
    /// restarted for its health). A child that exits by itself is
    /// restarted outside this limit.
    pub unhealthy_restart_limit: u32,
    /// How long a component's child must be healthy, unbroken, for its
    /// restarts for its health to stop counting against
    /// `unhealthy_restart_limit` (the count clears; decision 14.67).
    pub unhealthy_reset_ms: u64,
    /// The directory the host puts the children's sockets in. When absent,
    /// `$XDG_RUNTIME_DIR` if it is set, absolute and short enough, else
    /// `/tmp`: never `$TMPDIR`, which may be deep. A Unix socket's path is
    /// at most 107 bytes, so the host refuses a directory too long to hold
    /// its socket names.
    pub socket_dir: Option<PathBuf>,
}

impl Default for Supervision {
    fn default() -> Self {
        Supervision {
            handshake_timeout_ms: 30_000,
            initial_backoff_ms: 100,
            max_backoff_ms: 5_000,
            shutdown_grace_ms: 5_000,
            terminate_grace_ms: 2_000,
            call_timeout_ms: 30_000,
            health_check_interval_ms: 5_000,
            health_check_deadline_ms: 2_000,
            unhealthy_grace_ms: 15_000,
            unhealthy_restart_limit: 3,
            unhealthy_reset_ms: 600_000,
            socket_dir: None,
        }
    }
}

#[cfg(test)]
#[path = "lib.test.rs"]
mod tests;
