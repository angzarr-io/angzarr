use std::collections::BTreeMap;
use std::path::PathBuf;

use angzarr_diagnostic::Report;

use super::*;

fn domain(name: &str, language: &str) -> ComponentConfig {
    ComponentConfig {
        name: name.to_string(),
        language: language.to_string(),
        kind: ComponentKindConfig::Domain,
        upcasts: None,
        launch: None,
        entry: Vec::new(),
        dir: None,
        env: BTreeMap::new(),
        expose: Default::default(),
        scaffold: Default::default(),
    }
}

fn strings(words: &[&str]) -> Vec<String> {
    words.iter().map(|w| w.to_string()).collect()
}

/// `component`'s placement in its language, as the built-in languages
/// define it.
fn placed(component: &ComponentConfig) -> Result<Placement, HostError> {
    let languages = Languages::builtin();
    let language = languages
        .get(&component.language)
        .expect("a built-in language");
    language.placement(component)
}

/// The host knows two languages of its own: Rust, in process, and Python,
/// a child launched by `uv run --locked`.
#[test]
fn the_built_in_languages_are_rust_in_process_and_python_by_uv() {
    let languages = Languages::builtin();
    assert_eq!(languages.names().collect::<Vec<_>>(), ["python", "rust"]);
    assert_eq!(
        languages.get(IN_PROCESS_LANGUAGE),
        Some(&Language::in_process("rust"))
    );
    assert_eq!(
        languages.get("python"),
        Some(&Language::child("python").launched_by(["uv", "run", "--locked"]))
    );
    assert_eq!(languages.get("go"), None);
    assert_eq!(Languages::default(), languages);
    assert_eq!(Language::child("go").launched_by(["./go"]).name(), "go");
    assert_eq!(Language::in_process("rust").name(), "rust");
}

#[test]
fn python_defaults_to_uv_run_locked_then_its_entry() {
    let mut player = domain("player", "python");
    player.entry = strings(&["-m", "player.serve"]);
    player.dir = Some(PathBuf::from("domains/player"));
    player.env = BTreeMap::from([("K".to_string(), "v".to_string())]);
    assert_eq!(
        placed(&player),
        Ok(Placement::Remote(Launch {
            program: "uv".to_string(),
            args: strings(&["run", "--locked", "-m", "player.serve"]),
            dir: Some(PathBuf::from("domains/player")),
            env: BTreeMap::from([("K".to_string(), "v".to_string())]),
        }))
    );
}

/// `launch` replaces the default and the entry entirely.
#[test]
fn a_configured_launch_overrides_the_default() {
    let mut player = domain("player", "python");
    player.entry = strings(&["-m", "player.serve"]);
    player.launch = Some(strings(&["python3", "serve.py", "--fast"]));
    assert_eq!(
        placed(&player),
        Ok(Placement::Remote(Launch {
            program: "python3".to_string(),
            args: strings(&["serve.py", "--fast"]),
            dir: None,
            env: BTreeMap::new(),
        }))
    );
}

#[test]
fn a_rust_domain_without_a_launch_is_in_process() {
    assert_eq!(placed(&domain("table", "rust")), Ok(Placement::InProcess));
}

/// A Rust domain may be remote too: a `launch` makes it a child.
#[test]
fn a_rust_domain_with_a_launch_is_remote() {
    let mut table = domain("table", "rust");
    table.launch = Some(strings(&["./target/release/table-server"]));
    assert_eq!(
        placed(&table),
        Ok(Placement::Remote(Launch {
            program: "./target/release/table-server".to_string(),
            args: Vec::new(),
            dir: None,
            env: BTreeMap::new(),
        }))
    );
}

/// A language the application loads with no default launch needs the
/// component's `launch`, and without one it is refused by name, not
/// guessed at.
#[test]
fn a_loaded_language_with_no_default_needs_a_launch() {
    let go = Language::child("go");
    assert_eq!(
        go.placement(&domain("dealer", "go")),
        Err(HostError::NoLaunch {
            domain: "dealer".to_string(),
            language: "go".to_string(),
        })
    );
    let mut dealer = domain("dealer", "go");
    dealer.launch = Some(strings(&["./bin/dealer"]));
    assert!(matches!(go.placement(&dealer), Ok(Placement::Remote(_))));
}

/// A loaded language's default launch, then the component's entry.
#[test]
fn a_loaded_language_launches_by_its_default() {
    let node = Language::child("node").launched_by(["node"]);
    let mut dealer = domain("dealer", "node");
    dealer.entry = strings(&["dealer.js"]);
    assert_eq!(
        node.placement(&dealer),
        Ok(Placement::Remote(Launch {
            program: "node".to_string(),
            args: strings(&["dealer.js"]),
            dir: None,
            env: BTreeMap::new(),
        }))
    );
}

/// `language` is a closed set, the languages loaded: the built-in ones and
/// those the application loads before it starts (owner ruling, 2026-10-31).
/// One outside it is refused with its key path and the languages loaded,
/// in name order; loading it admits it, and a language loaded again under
/// a built-in's name replaces it.
#[test]
fn a_language_not_loaded_is_refused_with_its_key_path_and_the_loaded_languages() {
    let text = stamped(
        "component:\n  - name: table\n    kind: domain\n    language: rust\n  \
         - name: dealer\n    kind: domain\n    language: go\n    launch: [./bin/dealer]\n",
    );
    let config = parse(&text, "angzarr.yaml").unwrap();
    assert_eq!(
        Languages::builtin().check(&config),
        Err(HostError::UnknownLanguage {
            key: "component[1].language".to_string(),
            language: "go".to_string(),
            loaded: "python,rust".to_string(),
        })
    );
    let loaded = Languages::builtin().with(Language::child("go"));
    assert_eq!(loaded.names().collect::<Vec<_>>(), ["go", "python", "rust"]);
    assert_eq!(loaded.check(&config), Ok(()));
    let replaced = Languages::builtin().with(Language::child("python").launched_by(["python3"]));
    assert_eq!(
        replaced.get("python"),
        Some(&Language::child("python").launched_by(["python3"]))
    );
}

#[test]
fn an_empty_launch_is_refused() {
    let mut player = domain("player", "python");
    player.launch = Some(Vec::new());
    assert_eq!(
        placed(&player),
        Err(HostError::EmptyLaunch {
            domain: "player".to_string(),
        })
    );
}

/// Only an aggregate is served through the host's listener: a policy, a
/// projector or an upcaster given an `expose` is refused at start, before
/// any child starts, whatever its language. A domain's is accepted.
#[test]
fn exposing_anything_but_a_domain_is_refused_at_start() {
    for kind in [
        ComponentKindConfig::Policy,
        ComponentKindConfig::Projector,
        ComponentKindConfig::Upcaster,
    ] {
        for expose in [Expose::Loopback, Expose::Ip] {
            let mut component = domain("table-player", "rust");
            component.kind = kind;
            component.upcasts = Some("table".to_string());
            component.expose = expose;
            let config = HostConfig {
                components: vec![component],
                ..HostConfig::default()
            };
            assert_eq!(
                crate::Host::start(config).err(),
                Some(HostError::NotExposable {
                    domain: "table-player".to_string(),
                    kind: kind.as_str().to_string(),
                }),
                "{kind:?} exposed {expose:?}"
            );
        }
    }
    let mut table = domain("table", "rust");
    table.expose = Expose::Ip;
    let host = crate::Host::start(HostConfig {
        components: vec![table],
        ..HostConfig::default()
    });
    assert!(host.is_ok(), "{:?}", host.err());
}

/// A config is read through the schema's loader, strictly: one that does
/// not load is refused as invalid, naming the file and, as its cause, the
/// reader's error with the key path where it goes wrong.
mod strict {
    use angzarr_diagnostic::{Arg, Diagnostic};

    use super::*;

    /// The cause of `text`'s refusal.
    fn refused(text: &str) -> Report {
        match parse(text, "angzarr.yaml") {
            Err(HostError::ConfigInvalid { path, cause }) => {
                assert_eq!(path, "angzarr.yaml");
                cause
            }
            other => panic!("not refused as invalid: {other:?}"),
        }
    }

    /// The code of `cause`'s problem, and its key path if it has one.
    fn problem(cause: &Report) -> (String, Option<String>) {
        let args = cause.args();
        let Some(Arg::Nested(problem)) = args
            .iter()
            .find(|(name, _)| *name == "problem")
            .map(|(_, arg)| arg)
        else {
            panic!("no problem in {cause}");
        };
        let key_path = problem
            .args()
            .into_iter()
            .find_map(|(name, arg)| match (name, arg) {
                ("key_path", Arg::Str(path)) => Some(path),
                _ => None,
            });
        (problem.code().to_string(), key_path)
    }

    #[test]
    fn a_valid_config_reads() {
        let text =
            stamped("component:\n  - name: player\n    kind: domain\n    language: python\n");
        let config = parse(&text, "angzarr.yaml").unwrap();
        assert_eq!(config.components, [domain("player", "python")]);
    }

    /// An unknown key, at its path, under the reader's code, naming the
    /// file it was read from.
    #[test]
    fn an_unknown_key_is_refused_at_its_path() {
        let cause = refused(&stamped("store:\n  in_memory: true\n  lanuch: 1\n"));
        assert_eq!(cause.code(), "angzarr-yaml-load-failed-at");
        assert!(cause.to_string().contains("source=angzarr.yaml"), "{cause}");
        assert_eq!(
            problem(&cause),
            (
                "angzarr-yaml-decode-unknown-key".to_string(),
                Some("store.lanuch".to_string())
            )
        );
    }

    /// A newer file is refused with the reader's code; a file with no
    /// stamp reads, through every step.
    #[test]
    fn a_newer_file_is_refused_and_an_unstamped_one_reads() {
        assert_eq!(
            problem(&refused("schema_version: 5\n")).0,
            "angzarr-yaml-too-new"
        );
        assert_eq!(
            parse("store:\n  in_memory: true\n", "angzarr.yaml").map(|c| c.store.in_memory),
            Ok(true)
        );
    }

    /// The old TOML is not YAML this reader takes.
    #[test]
    fn the_old_toml_is_refused() {
        let cause = refused("[[component]]\nname = \"table\"\n");
        assert_eq!(cause.code(), "angzarr-yaml-load-failed-at");
    }
}

/// The config's retry and policy settings are the kernel's: its defaults
/// are the kernel's own, and each key carries over.
#[test]
fn the_retry_and_policy_settings_are_the_kernels() {
    use angzarr_embedded::{OnPolicyBug, OnRetryable};
    use std::time::Duration;
    assert_eq!(
        on_retryable(&RetryConfig::default()),
        OnRetryable::default()
    );
    assert_eq!(
        on_retryable(&RetryConfig {
            max_retries: 5,
            max_race_runs: 4,
            initial_backoff_ms: 50,
            multiplier: 3.0,
            max_backoff_ms: 1_000,
            jitter: 0.5,
        }),
        OnRetryable {
            max_retries: 5,
            max_race_runs: std::num::NonZeroU32::new(4).unwrap(),
            initial_backoff: Duration::from_millis(50),
            multiplier: 3.0,
            max_backoff: Duration::from_millis(1_000),
            jitter: 0.5,
        }
    );
    assert_eq!(on_policy_bug(OnBug::default()), OnPolicyBug::default());
    assert_eq!(on_policy_bug(OnBug::MovePast), OnPolicyBug::MovePast);
    assert_eq!(on_policy_bug(OnBug::Stay), OnPolicyBug::Stay);
}

/// The config's `store.snapshots` is the kernel's snapshot policy: one at
/// every write by default, its `every` for every domain, and a domain's own
/// in its place.
#[test]
fn the_store_snapshots_setting_is_the_kernels_snapshot_policy() {
    use angzarr_embedded::SnapshotPolicy;
    use std::num::NonZeroU64;
    let n = |events: u64| NonZeroU64::new(events).unwrap();
    assert_eq!(
        snapshot_policy(&SnapshotsConfig::default()),
        SnapshotPolicy::default()
    );
    let config = SnapshotsConfig {
        every: n(4),
        domain: BTreeMap::from([
            ("table".to_string(), DomainSnapshots { every: n(2) }),
            ("seat".to_string(), DomainSnapshots { every: n(9) }),
        ]),
    };
    assert_eq!(
        snapshot_policy(&config),
        SnapshotPolicy::every(n(4))
            .domain("table", n(2))
            .domain("seat", n(9))
    );
}
