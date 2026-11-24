//! The host's config: its schema, `angzarr.yaml`, is
//! angzarr-embedded-host-config's, re-exported here ([`HostConfig`] and its
//! children, the versioned [`loader`] and its [`FIXTURES`]), and read
//! strictly ([`parse`]): a config that does not load is refused as
//! [`HostError::ConfigInvalid`], its cause the reader's error naming the
//! key path where it goes wrong. What the schema cannot know is here: the
//! languages the running application loads.
//!
//! `language` is a closed set, populated by the languages loaded
//! ([`Languages`]): the built-in Rust (in process) and Python (a child,
//! `uv run --locked`), and any the application loads before it starts
//! ([`crate::run_with_languages`]); one not loaded is refused with its key
//! path and the languages loaded ([`HostError::UnknownLanguage`]). A loaded
//! language with no default launch command needs each component's
//! `launch`.
//!
//! A component's language picks where it runs and its default launch
//! command, and nothing else: the host speaks one protocol to every child
//! (readiness by gRPC health on a Unix socket, then gRPC there), so a language is loaded
//! with no more than its name ([`Language::child`]) and, if it has one, its
//! default launch ([`Language::launched_by`]).
//!
//! A child's descriptor must agree with its `kind`
//! ([`HostError::KindMismatch`]); a sidecar has none, and is up while its
//! health reports SERVING. An in-process component (Rust, no `launch`) is
//! registered by the application's `register`, and [`run`](crate::run)
//! holds the two to each other: one listed that `register` did not
//! register ([`HostError::NotRegistered`]), and one registered that no
//! `component` entry lists ([`HostError::NotListed`]), are refused, by name
//! and kind; an in-process upcaster's `upcasts` is a domain registered
//! here.

use std::collections::BTreeMap;
use std::path::PathBuf;

use angzarr_embedded::{OnPolicyBug, OnRetryable, SnapshotPolicy};
pub use angzarr_embedded_host_config::{
    loader, stamped, ComponentConfig, ComponentKindConfig, ComponentScaffold, DomainSnapshots,
    Expose, HostConfig, OnBug, PolicyConfig, RetryConfig, SnapshotsConfig, StoreConfig,
    Supervision, CURRENT, FILE_NAME, FIXTURES, KEY, KIND,
};
use coded_diagnostic::Report;

use crate::error::HostError;

/// `text`, a config's YAML, read strictly through the [`loader`]: one that
/// does not load is refused as [`HostError::ConfigInvalid`], naming `path`
/// and, as its cause, the reader's error (its code, the source, and for a
/// decode the key path: `component[1].kind`, `retry.retries`).
pub fn parse(text: &str, path: &str) -> Result<HostConfig, HostError> {
    HostConfig::from_yaml(text, path).map_err(|error| HostError::ConfigInvalid {
        path: path.to_string(),
        cause: Report::of(&error),
    })
}

/// The config file at `path`, read strictly ([`parse`]): refused as
/// [`HostError::ConfigUnreadable`] when it cannot be read.
pub fn read(path: &std::path::Path) -> Result<HostConfig, HostError> {
    let text = std::fs::read_to_string(path).map_err(|e| HostError::ConfigUnreadable {
        path: path.display().to_string(),
        kind: format!("{:?}", e.kind()),
    })?;
    parse(&text, &path.display().to_string())
}

/// `retry` as the kernel's [`OnRetryable`].
pub fn on_retryable(retry: &RetryConfig) -> OnRetryable {
    OnRetryable {
        max_retries: retry.max_retries,
        // At least one run (decision 14.84): a config's 0 is one.
        max_race_runs: std::num::NonZeroU32::new(retry.max_race_runs)
            .unwrap_or(std::num::NonZeroU32::MIN),
        initial_backoff: std::time::Duration::from_millis(retry.initial_backoff_ms),
        multiplier: retry.multiplier,
        max_backoff: std::time::Duration::from_millis(retry.max_backoff_ms),
        jitter: retry.jitter,
    }
}

/// `store.snapshots` as the kernel's [`SnapshotPolicy`]: its `every` for
/// every domain, and each domain's own in its place.
pub fn snapshot_policy(snapshots: &SnapshotsConfig) -> SnapshotPolicy {
    snapshots.domain.iter().fold(
        SnapshotPolicy::every(snapshots.every),
        |policy, (domain, own)| policy.domain(domain.clone(), own.every),
    )
}

/// `policy.on_bug` as the kernel's [`OnPolicyBug`].
pub fn on_policy_bug(on_bug: OnBug) -> OnPolicyBug {
    match on_bug {
        OnBug::MovePast => OnPolicyBug::MovePast,
        OnBug::Stay => OnPolicyBug::Stay,
    }
}

/// The built-in language whose components run in the application's own
/// process, as angzarr-embedded components: the host launches none of them
/// unless the config gives one a `launch`.
pub const IN_PROCESS_LANGUAGE: &str = "rust";

/// A language a component may be written in, as the host knows it: its
/// name (a `component` entry's `language`) and where a component in it runs
/// when the config gives no `launch`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Language {
    name: String,
    runs: Runs,
}

/// Where a component of a [`Language`] with no `launch` runs.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Runs {
    /// In the application's process, registered by its `register`.
    InProcess,
    /// As a child, launched by this command (program first) followed by the
    /// component's `entry`; with none, every component needs its `launch`.
    Child(Option<Vec<String>>),
}

impl Language {
    /// A language whose components with no `launch` run in process,
    /// registered by the application's `register`.
    pub fn in_process(name: impl Into<String>) -> Language {
        Language {
            name: name.into(),
            runs: Runs::InProcess,
        }
    }

    /// A language whose components run as children, each launched by its
    /// own `launch` until [`Language::launched_by`] gives a default.
    pub fn child(name: impl Into<String>) -> Language {
        Language {
            name: name.into(),
            runs: Runs::Child(None),
        }
    }

    /// This language with `command` (program first) as its default launch,
    /// before a component's `entry`. A child language.
    pub fn launched_by<W: Into<String>>(self, command: impl IntoIterator<Item = W>) -> Language {
        Language {
            name: self.name,
            runs: Runs::Child(Some(command.into_iter().map(Into::into).collect())),
        }
    }

    /// Its name, as a `component` entry's `language` names it.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// The languages the host knows: a closed set, the built-in ones
/// ([`Languages::builtin`]) and those the application loads before it
/// starts ([`Languages::with`]; [`crate::run_with_languages`]). A
/// `component` entry whose `language` is not among them is refused
/// ([`Languages::check`]). Its default is the built-in ones.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Languages(BTreeMap<String, Language>);

impl Languages {
    /// The host's own: Rust ([`IN_PROCESS_LANGUAGE`]), in process, and
    /// Python, a child launched by `uv run --locked` (the component's
    /// project with exactly its lock file's dependencies).
    pub fn builtin() -> Languages {
        Languages(BTreeMap::new())
            .with(Language::in_process(IN_PROCESS_LANGUAGE))
            .with(Language::child("python").launched_by(["uv", "run", "--locked"]))
    }

    /// These and `language`, which replaces one of its name.
    pub fn with(mut self, language: Language) -> Languages {
        self.0.insert(language.name.clone(), language);
        self
    }

    /// The language named `name`, if loaded.
    pub fn get(&self, name: &str) -> Option<&Language> {
        self.0.get(name)
    }

    /// The loaded languages' names, in order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }

    /// Whether every component of `config` is in one of these languages:
    /// the first that is not is refused as [`HostError::UnknownLanguage`],
    /// naming its key path (`component[1].language`) and the languages
    /// loaded.
    pub fn check(&self, config: &HostConfig) -> Result<(), HostError> {
        (0..config.components.len()).try_for_each(|index| self.of(config, index).map(|_| ()))
    }

    /// The language of `config`'s component `index`, or
    /// [`HostError::UnknownLanguage`] naming its key path.
    pub(crate) fn of(&self, config: &HostConfig, index: usize) -> Result<&Language, HostError> {
        let component = &config.components[index];
        self.get(&component.language)
            .ok_or_else(|| HostError::UnknownLanguage {
                key: format!("component[{index}].language"),
                language: component.language.clone(),
                loaded: self.names().collect::<Vec<_>>().join(","),
            })
    }
}

impl Default for Languages {
    fn default() -> Languages {
        Languages::builtin()
    }
}

/// Where a component runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Placement {
    /// In the application's process: main.rs registers it; the host does
    /// nothing with it.
    InProcess,
    /// In a child process the host launches.
    Remote(Launch),
}

/// A child's resolved launch: what [`Language::placement`] makes of the
/// config.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Launch {
    pub program: String,
    pub args: Vec<String>,
    pub dir: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
}

impl Language {
    /// Where `component`, written in this language, runs and, when
    /// remote, how it is launched: its `launch`, else, for an in-process
    /// language, in process, and for a child language its default followed
    /// by its `entry`. Refused when the launch is empty, or when no launch
    /// is given for a child language with no default.
    pub fn placement(&self, component: &ComponentConfig) -> Result<Placement, HostError> {
        let words: Vec<String> = match (&component.launch, &self.runs) {
            (Some(launch), _) => launch.clone(),
            (None, Runs::InProcess) => return Ok(Placement::InProcess),
            (None, Runs::Child(Some(default))) => default
                .iter()
                .cloned()
                .chain(component.entry.iter().cloned())
                .collect(),
            (None, Runs::Child(None)) => {
                return Err(HostError::NoLaunch {
                    domain: component.name.clone(),
                    language: component.language.clone(),
                })
            }
        };
        let mut words = words.into_iter();
        let Some(program) = words.next() else {
            return Err(HostError::EmptyLaunch {
                domain: component.name.clone(),
            });
        };
        Ok(Placement::Remote(Launch {
            program,
            args: words.collect(),
            dir: component.dir.clone(),
            env: component.env.clone(),
        }))
    }
}

#[cfg(test)]
#[path = "config.test.rs"]
mod tests;
