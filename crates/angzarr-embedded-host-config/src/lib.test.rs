use std::collections::BTreeMap;
use std::path::PathBuf;

use angzarr_yaml::versioned::LoadProblem;
use coded_diagnostic::Diagnostic;

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
        expose: Expose::None,
        scaffold: ComponentScaffold::default(),
    }
}

fn strings(words: &[&str]) -> Vec<String> {
    words.iter().map(|w| w.to_string()).collect()
}

/// `body` stamped at the current version, read.
fn read(body: &str) -> Result<HostConfig, LoadError> {
    HostConfig::from_yaml(&stamped(body), FILE_NAME)
}

/// Why `text` is refused: its problem.
fn refused(text: &str) -> LoadProblem {
    match HostConfig::from_yaml(text, FILE_NAME) {
        Err(error) => {
            assert_eq!(error.kind, KIND);
            assert_eq!(error.source.as_deref(), Some(FILE_NAME));
            error.problem
        }
        Ok(config) => panic!("read, not refused: {config:?}"),
    }
}

/// The code and the key path of `body`'s refusal, stamped at the current
/// version: a decode's, under the loader's.
fn refused_at(body: &str) -> (String, String) {
    match refused(&stamped(body)) {
        LoadProblem::Decode(report) => {
            let key_path = report
                .args()
                .into_iter()
                .find_map(|(name, arg)| match (name, arg) {
                    ("key_path", coded_diagnostic::Arg::Str(path)) => Some(path),
                    _ => None,
                })
                .expect("a decode's refusal names its key path");
            (report.code().to_string(), key_path)
        }
        other => panic!("not a decode's refusal: {other:?}"),
    }
}

/// The file is `angzarr.yaml`, of kind `angzarr-host-config`, stamped
/// `schema_version`, at version 4 (owner rulings, 2026-10-31: one reader,
/// `schema_version` an unsigned integer; 2026-11-01: supervision's health
/// keys, then its reset window; then a component's scaffold and the
/// store's snapshots), reading every older version: 3, 2, 1, and 0, a file
/// with no stamp.
#[test]
fn the_file_is_angzarr_yaml_stamped_schema_version_at_version_4() {
    assert_eq!(FILE_NAME, "angzarr.yaml");
    let loader = loader();
    assert_eq!(loader.kind(), "angzarr-host-config");
    assert_eq!(loader.key(), "schema_version");
    assert_eq!((loader.oldest(), loader.current()), (0, 4));
    assert_eq!(CURRENT, 4);
    assert_eq!(loader.legacy_key(), None);
}

/// The fixture named `name`.
fn fixture(name: &str) -> &'static Fixture<'static> {
    FIXTURES
        .iter()
        .find(|f| f.name == name)
        .unwrap_or_else(|| panic!("no fixture {name}"))
}

/// Every fixture loads to what it must: the current ones to themselves,
/// an older one to its expected current text. There is one of every
/// version the loader reads.
#[test]
fn the_fixtures_load() {
    loader().assert_fixtures(FIXTURES);
    let versions: std::collections::BTreeSet<u32> = FIXTURES
        .iter()
        .map(|f| {
            let doc = angzarr_yaml::parse(f.input).unwrap();
            loader().version_of(&doc).unwrap()
        })
        .collect();
    assert_eq!(versions, [0, 1, 2, 3, 4].into());
}

/// A current file loads, every key read as its comment says.
#[test]
fn a_current_file_loads() {
    let documented = fixture("fixtures/v4-documented.yaml");
    let config = HostConfig::from_yaml(documented.input, documented.name).unwrap();
    let mut table = domain("table", "rust");
    table.expose = Expose::Loopback;
    table.scaffold = ComponentScaffold { snapshots: true };
    let mut policy = domain("policy-table-player", "rust");
    policy.kind = ComponentKindConfig::Policy;
    let mut player = domain("player", "python");
    player.entry = strings(&["-m", "player.serve"]);
    player.dir = Some(PathBuf::from("domains/player"));
    player.env = BTreeMap::from([("PLAYER_SEATS".to_string(), "6".to_string())]);
    let mut table_player = domain("table-player", "python");
    table_player.kind = ComponentKindConfig::Policy;
    table_player.entry = strings(&["-m", "table_player.serve"]);
    let mut upcaster = domain("table-upcaster", "python");
    upcaster.kind = ComponentKindConfig::Upcaster;
    upcaster.upcasts = Some("table".to_string());
    upcaster.entry = strings(&["-m", "table_upcaster.serve"]);
    let mut dealer = domain("dealer", "go");
    dealer.launch = Some(strings(&["./bin/dealer"]));
    assert_eq!(
        config,
        HostConfig {
            components: vec![table, policy, player, table_player, upcaster, dealer],
            supervision: Supervision {
                handshake_timeout_ms: 10_000,
                health_check_interval_ms: 1_000,
                health_check_deadline_ms: 500,
                unhealthy_grace_ms: 10_000,
                unhealthy_restart_limit: 5,
                unhealthy_reset_ms: 300_000,
                socket_dir: Some(PathBuf::from("/run/user/1000")),
                ..Supervision::default()
            },
            store: StoreConfig {
                snapshots: SnapshotsConfig {
                    every: NonZeroU64::MIN,
                    domain: BTreeMap::from([(
                        "table".to_string(),
                        DomainSnapshots {
                            every: NonZeroU64::new(10).unwrap()
                        }
                    )]),
                },
                ..StoreConfig::default()
            },
            policy: PolicyConfig {
                on_bug: OnBug::MovePast
            },
            retry: RetryConfig::default(),
        }
    );
}

/// The least a file holds is its stamp: every mapping has its defaults.
#[test]
fn a_file_of_only_its_stamp_is_every_default() {
    let minimal = fixture("fixtures/v4-minimal.yaml");
    assert_eq!(
        HostConfig::from_yaml(minimal.input, minimal.name),
        Ok(HostConfig::default())
    );
    assert_eq!(stamped(""), "schema_version: 4\n");
    assert_eq!(read(""), Ok(HostConfig::default()));
}

/// The defaults: those of the kernel's retry, a policy bug moving past its
/// trigger, `angzarr.db` on disk, and the host's supervision times.
#[test]
fn the_defaults_are_the_kernels_and_the_hosts() {
    assert_eq!(
        RetryConfig::default(),
        RetryConfig {
            max_retries: 3,
            max_race_runs: 10,
            initial_backoff_ms: 100,
            multiplier: 2.0,
            max_backoff_ms: 5_000,
            jitter: 0.2,
        }
    );
    assert_eq!(PolicyConfig::default().on_bug, OnBug::MovePast);
    assert_eq!(
        StoreConfig::default(),
        StoreConfig {
            path: PathBuf::from("angzarr.db"),
            in_memory: false,
            snapshots: SnapshotsConfig {
                every: NonZeroU64::MIN,
                domain: BTreeMap::new(),
            },
        }
    );
    assert_eq!(
        Supervision::default(),
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
    );
}

/// A file written by a newer build is refused, not half-read, with the
/// reader's code.
#[test]
fn a_newer_file_is_refused_with_the_readers_code() {
    let problem = refused("schema_version: 5\ncomponent: []\n");
    assert_eq!(
        problem,
        LoadProblem::TooNew {
            found: 5,
            current: 4
        }
    );
    assert_eq!(problem.code(), "angzarr-yaml-too-new");
}

/// A file with no stamp is the oldest generation, version 0 (the
/// config-versioning rule), which has version 1's shape: it loads through
/// every step, as the same file stamped does, every key it leaves out its
/// default, the supervision's health keys among them. An empty or
/// comment-only file is this case: every default.
#[test]
fn a_file_with_no_stamp_loads_through_every_step() {
    let body = "component:\n  - name: table\n    kind: domain\n    language: rust\n";
    let doc = angzarr_yaml::parse(body).unwrap();
    assert_eq!(loader().version_of(&doc), Ok(0));
    let stamped_config = read(body).unwrap();
    assert_eq!(stamped_config.components, [domain("table", "rust")]);
    assert_eq!(HostConfig::from_yaml(body, FILE_NAME), Ok(stamped_config));
    for text in ["", "# nothing yet\n"] {
        assert_eq!(
            HostConfig::from_yaml(text, FILE_NAME),
            Ok(HostConfig::default()),
            "{text:?}"
        );
    }
}

/// A stamp that is not an unsigned integer is refused as such.
#[test]
fn a_stamp_that_is_not_an_unsigned_integer_is_refused() {
    for text in [
        "schema_version: one\n",
        "schema_version: -1\n",
        "schema_version: 1.0\n",
    ] {
        assert_eq!(
            refused(text).code(),
            "angzarr-yaml-version-not-an-integer",
            "{text:?}"
        );
    }
}

/// YAML this reader does not take is refused as the reader's own error.
#[test]
fn malformed_yaml_is_refused() {
    assert!(matches!(
        refused("schema_version: 1\ncomponent: [a, b\n"),
        LoadProblem::Yaml(_)
    ));
}

/// A misspelt key is refused at its key path, not ignored: `lanuch` would
/// otherwise run the default launch silently. So is one in a mapping, and
/// one at the root.
#[test]
fn an_unknown_key_is_refused_with_its_key_path() {
    let player = "component:\n  - name: player\n    kind: domain\n    language: python\n";
    let unknown = "angzarr-yaml-decode-unknown-key".to_string();
    assert_eq!(
        refused_at(&format!("{player}    lanuch: [x]\n")),
        (unknown.clone(), "component[0].lanuch".to_string())
    );
    assert_eq!(
        refused_at("retry:\n  retries: 5\n"),
        (unknown.clone(), "retry.retries".to_string())
    );
    assert_eq!(
        refused_at("domain:\n  - name: x\n"),
        (unknown.clone(), "domain".to_string()),
        "the old [[domain]] tables are refused, not read as none"
    );
    assert_eq!(
        refused_at("policy:\n  on_bugs: stay\n"),
        (unknown, "policy.on_bugs".to_string())
    );
}

/// A component with no `kind` (or `name`, or `language`) is refused at
/// the key it lacks.
#[test]
fn a_missing_required_key_is_refused_at_its_path() {
    let missing = "angzarr-yaml-decode-missing-key".to_string();
    assert_eq!(
        refused_at(
            "component:\n  - name: player\n    kind: domain\n    language: python\n  \
             - name: table\n    language: rust\n"
        ),
        (missing.clone(), "component[1].kind".to_string())
    );
    assert_eq!(
        refused_at("component:\n  - kind: domain\n    language: rust\n"),
        (missing.clone(), "component[0].name".to_string())
    );
    assert_eq!(
        refused_at("component:\n  - name: table\n    kind: domain\n"),
        (missing, "component[0].language".to_string())
    );
}

/// A value of the wrong type is refused at its key.
#[test]
fn a_value_of_the_wrong_type_is_refused_at_its_key() {
    assert_eq!(
        refused_at("supervision:\n  call_timeout_ms: soon\n"),
        (
            "angzarr-yaml-decode-scalar".to_string(),
            "supervision.call_timeout_ms".to_string()
        )
    );
    assert_eq!(
        refused_at("retry:\n  max_retries: -1\n").1,
        "retry.max_retries".to_string()
    );
    assert_eq!(
        refused_at("store:\n  in_memory: yes\n").1,
        "store.in_memory".to_string()
    );
}

/// A component's `kind` names what it is: `domain`, `policy`, `projector`
/// or `upcaster`, with no default; anything else is refused at its key.
#[test]
fn a_components_kind_is_domain_policy_projector_or_upcaster() {
    let kind = |kind: &str| {
        read(&format!(
            "component:\n  - name: x\n    language: python\n    kind: {kind}\n"
        ))
        .map(|c| c.components[0].kind)
    };
    assert_eq!(kind("domain"), Ok(ComponentKindConfig::Domain));
    assert_eq!(kind("policy"), Ok(ComponentKindConfig::Policy));
    assert_eq!(kind("projector"), Ok(ComponentKindConfig::Projector));
    assert_eq!(kind("upcaster"), Ok(ComponentKindConfig::Upcaster));
    for bad in ["saga", "Policy"] {
        assert_eq!(
            refused_at(&format!(
                "component:\n  - name: x\n    language: python\n    kind: {bad}\n"
            )),
            (
                "angzarr-yaml-decode-unknown-variant".to_string(),
                "component[0].kind".to_string()
            ),
            "{bad}"
        );
    }
    let names: Vec<_> = [
        ComponentKindConfig::Domain,
        ComponentKindConfig::Policy,
        ComponentKindConfig::Projector,
        ComponentKindConfig::Upcaster,
    ]
    .iter()
    .map(|k| k.as_str())
    .collect();
    assert_eq!(names, ["domain", "policy", "projector", "upcaster"]);
}

/// A component's `expose`: `none` when left out, `loopback` or `ip` when
/// given; anything else is refused at its key.
#[test]
fn a_components_expose_is_none_by_default_else_loopback_or_ip() {
    let component = "component:\n  - name: x\n    kind: domain\n    language: rust\n";
    let expose = |line: &str| read(&format!("{component}{line}")).map(|c| c.components[0].expose);
    assert_eq!(expose(""), Ok(Expose::None));
    assert_eq!(expose("    expose: none\n"), Ok(Expose::None));
    assert_eq!(expose("    expose: loopback\n"), Ok(Expose::Loopback));
    assert_eq!(expose("    expose: ip\n"), Ok(Expose::Ip));
    for bad in ["Loopback", "public", "true"] {
        assert_eq!(
            refused_at(&format!("{component}    expose: {bad}\n")).1,
            "component[0].expose".to_string(),
            "{bad}"
        );
    }
    let names: Vec<_> = [Expose::None, Expose::Loopback, Expose::Ip]
        .iter()
        .map(|e| e.as_str())
        .collect();
    assert_eq!(names, ["none", "loopback", "ip"]);
}

/// An exposure admits every arrival a narrower one does: none, then
/// loopback, then ip.
#[test]
fn exposures_widen_from_none_to_loopback_to_ip() {
    assert!(Expose::None < Expose::Loopback);
    assert!(Expose::Loopback < Expose::Ip);
}

/// `policy.on_bug` is `move_past` (the default, when the key or the
/// mapping is left out) or `stay`; anything else is refused at its key.
#[test]
fn the_policy_mapping_sets_what_a_policy_bug_does_with_the_position() {
    let on_bug = |body: &str| read(body).map(|c| c.policy.on_bug);
    assert_eq!(on_bug("policy:\n  on_bug: stay\n"), Ok(OnBug::Stay));
    assert_eq!(
        on_bug("policy:\n  on_bug: move_past\n"),
        Ok(OnBug::MovePast)
    );
    assert_eq!(on_bug("policy: {}\n"), Ok(OnBug::MovePast));
    assert_eq!(on_bug(""), Ok(OnBug::MovePast));
    for bad in ["sometimes", "MovePast"] {
        assert_eq!(
            refused_at(&format!("policy:\n  on_bug: {bad}\n")).1,
            "policy.on_bug".to_string(),
            "{bad}"
        );
    }
}

/// `retry` sets the cap, the backoff and the jitter, each with the
/// kernel's default when left out.
#[test]
fn the_retry_mapping_sets_the_cap_the_backoff_and_the_jitter() {
    let retry = |body: &str| read(body).map(|c| c.retry);
    assert_eq!(
        retry(
            "retry:\n  max_retries: 5\n  max_race_runs: 4\n  initial_backoff_ms: 50\n  \
             multiplier: 3.0\n  max_backoff_ms: 1000\n  jitter: 0.5\n"
        ),
        Ok(RetryConfig {
            max_retries: 5,
            max_race_runs: 4,
            initial_backoff_ms: 50,
            multiplier: 3.0,
            max_backoff_ms: 1_000,
            jitter: 0.5,
        })
    );
    assert_eq!(
        retry("retry:\n  max_retries: 0\n"),
        Ok(RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        })
    );
    assert_eq!(retry(""), Ok(RetryConfig::default()));
}

/// `store` names the SQLite file, or none on disk.
#[test]
fn the_store_mapping_names_the_file_or_memory() {
    assert_eq!(
        read("store:\n  in_memory: true\n").map(|c| c.store),
        Ok(StoreConfig {
            path: PathBuf::from("angzarr.db"),
            in_memory: true,
            snapshots: SnapshotsConfig::default(),
        })
    );
    assert_eq!(
        read("store:\n  path: data/app.db\n").map(|c| c.store.path),
        Ok(PathBuf::from("data/app.db"))
    );
}

/// A version-1 file loads as version 2: the health keys version 2 adds
/// are left out, so each is its default, and every other key reads as it
/// did.
#[test]
fn a_version_1_file_loads_with_the_health_keys_defaulted() {
    let v1 = fixture("fixtures/v1-documented.yaml");
    let v2 = fixture("fixtures/v2-documented.yaml");
    let old = HostConfig::from_yaml(v1.input, v1.name).unwrap();
    let new = HostConfig::from_yaml(v2.input, v2.name).unwrap();
    assert_eq!(new.supervision.unhealthy_reset_ms, 600_000);
    assert_eq!(
        old.supervision,
        Supervision {
            health_check_interval_ms: 5_000,
            health_check_deadline_ms: 2_000,
            unhealthy_grace_ms: 15_000,
            unhealthy_restart_limit: 3,
            ..new.supervision.clone()
        }
    );
    assert_eq!(
        (old.components, old.store, old.policy, old.retry),
        (new.components, new.store, new.policy, new.retry)
    );
}

/// A version-2 file loads as version 3: the reset window version 3 adds is
/// left out, so it is its default (decision 14.67), and every other key
/// reads as it did.
#[test]
fn a_version_2_file_loads_with_the_reset_window_defaulted() {
    let v2 = fixture("fixtures/v2-documented.yaml");
    let v3 = fixture("fixtures/v3-documented.yaml");
    let old = HostConfig::from_yaml(v2.input, v2.name).unwrap();
    let new = HostConfig::from_yaml(v3.input, v3.name).unwrap();
    assert_eq!(
        old.supervision,
        Supervision {
            unhealthy_reset_ms: 600_000,
            ..new.supervision.clone()
        }
    );
    assert_eq!(
        (old.components, old.store, old.policy, old.retry),
        (new.components, new.store, new.policy, new.retry)
    );
    assert_eq!(
        HostConfig::from_yaml("schema_version: 2\n", "v2.yaml")
            .map(|c| c.supervision.unhealthy_reset_ms),
        Ok(600_000)
    );
}

/// `store.snapshots` is the snapshot role's storage policy: a snapshot at
/// every write by default, every N events for every domain, or a domain's
/// own N; N is at least 1, and a key it does not know is refused.
#[test]
fn the_store_snapshots_mapping_sets_each_domains_frequency() {
    assert_eq!(
        read("").map(|c| c.store.snapshots),
        Ok(SnapshotsConfig {
            every: NonZeroU64::MIN,
            domain: BTreeMap::new(),
        })
    );
    let config =
        read("store:\n  snapshots:\n    every: 5\n    domain:\n      table:\n        every: 2\n")
            .unwrap();
    assert_eq!(config.store.snapshots.every.get(), 5);
    assert_eq!(
        config
            .store
            .snapshots
            .domain
            .get("table")
            .map(|d| d.every.get()),
        Some(2)
    );
    for (body, at) in [
        (
            "store:\n  snapshots:\n    every: 0\n",
            "store.snapshots.every",
        ),
        (
            "store:\n  snapshots:\n    domain:\n      table:\n        every: 0\n",
            "store.snapshots.domain.table.every",
        ),
        (
            "store:\n  snapshots:\n    domain:\n      table: {}\n",
            "store.snapshots.domain.table",
        ),
        ("store:\n  snapshots:\n    keep: 2\n", "store.snapshots"),
    ] {
        let (_, path) = refused_at(body);
        assert!(path.starts_with(at), "{body}: refused at {path}, not {at}");
    }
}

/// A component's `scaffold.snapshots` says whether the scaffold writes its
/// implementation taking snapshots: false by default; a key it does not
/// know is refused.
#[test]
fn a_components_scaffold_says_whether_it_takes_snapshots() {
    let entry = "component:\n  - name: table\n    kind: domain\n    language: rust\n";
    assert_eq!(
        read(entry).map(|c| c.components[0].scaffold),
        Ok(ComponentScaffold { snapshots: false })
    );
    assert_eq!(
        read(&format!("{entry}    scaffold:\n      snapshots: true\n"))
            .map(|c| c.components[0].scaffold),
        Ok(ComponentScaffold { snapshots: true })
    );
    let (_, path) = refused_at(&format!("{entry}    scaffold:\n      snapshot: true\n"));
    assert!(path.starts_with("component[0].scaffold"), "{path}");
}

/// A version-3 file loads as version 4: the scaffold and the snapshots
/// version 4 adds are left out, so each is its default, and every other
/// key reads as it did.
#[test]
fn a_version_3_file_loads_with_the_scaffold_and_the_snapshots_defaulted() {
    let v3 = fixture("fixtures/v3-documented.yaml");
    let v4 = fixture("fixtures/v4-documented.yaml");
    let old = HostConfig::from_yaml(v3.input, v3.name).unwrap();
    let new = HostConfig::from_yaml(v4.input, v4.name).unwrap();
    assert_eq!(old.store.snapshots, SnapshotsConfig::default());
    assert!(old.components.iter().all(|c| !c.scaffold.snapshots));
    let mut components = new.components.clone();
    components[0].scaffold = ComponentScaffold::default();
    assert_eq!(old.components, components);
    assert_eq!(
        (old.supervision, old.policy, old.retry, old.store.path),
        (new.supervision, new.policy, new.retry, new.store.path)
    );
}
