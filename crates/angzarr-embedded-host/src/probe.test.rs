//! The conformance probe, against the fake child in each of its ways of
//! conforming or not, and against client-python's child shim (ignored by
//! default: it needs uv and the Python player's checkout).

use std::collections::BTreeMap;
use std::path::PathBuf;

use angzarr_proto::v1 as pb;
use coded_diagnostic::{code, Diagnostic};
use prost_types::Any;

use super::*;
use crate::config::{ComponentConfig, ComponentKindConfig, Languages};
use crate::fake_child::{
    config, domain, health, kind, mode, HEALTH_ENV, INPUT_ENV, KIND_ENV, TARGET_ENV,
};

fn any(type_url: &str) -> Any {
    Any {
        type_url: type_url.to_string(),
        value: Vec::new(),
    }
}

/// A command to `domain`, its payload `payload`.
fn command(domain: &str, payload: Any) -> Input {
    Input::command(domain, &payload.type_url, payload.value)
}

/// One event of `domain`, its payload `payload`.
fn events(domain: &str, payload: Any) -> Input {
    Input::event(domain, &payload.type_url, payload.value)
}

fn with(mut component: ComponentConfig, env: &[(&str, &str)]) -> ComponentConfig {
    component.env.extend(
        env.iter()
            .map(|(name, value)| (name.to_string(), value.to_string())),
    );
    component
}

/// `component` probed with `input`, under the fake's short time limits:
/// a start limit of [`START_LIMIT_MS`], which a child that never serves
/// waits out (the probe runs on tokio's clock, as an application does).
fn probed(component: ComponentConfig, input: Input) -> Probed {
    let mut supervision = config(vec![]).supervision;
    supervision.handshake_timeout_ms = START_LIMIT_MS;
    supervision.shutdown_grace_ms = 500;
    supervision.terminate_grace_ms = 500;
    probe(&component, &Languages::builtin(), &supervision, input)
}

/// The probe's start limit, in milliseconds.
const START_LIMIT_MS: u64 = 300;

/// Each step's outcome, a failure by its code alone.
fn outcomes(probed: &Probed) -> Vec<(Step, Option<String>)> {
    (probed.steps.iter())
        .map(|(step, outcome)| {
            let outcome = match outcome {
                Outcome::Passed => Some("passed".to_string()),
                Outcome::Failed(report) => Some(report.code().to_string()),
                Outcome::NotRun => None,
            };
            (*step, outcome)
        })
        .collect()
}

/// The five steps' outcomes: `passed` for the first `passed`, then
/// `failed` (a code), then not run.
fn expect(passed: usize, failed: Option<&str>) -> Vec<(Step, Option<String>)> {
    let steps = [
        Step::Launch,
        Step::Serving,
        Step::Describe,
        Step::Handle,
        Step::Shutdown,
    ];
    (steps.into_iter().enumerate())
        .map(|(n, step)| match n {
            n if n < passed => (step, Some("passed".to_string())),
            n if n == passed => (step, failed.map(str::to_string)),
            _ => (step, None),
        })
        .collect()
}

/// A child that keeps the protocol passes every step: launched,
/// SERVING, described, its command answered, and exited by
/// itself with status 0 on its stdin's end.
#[test]
fn a_conforming_child_passes_every_step() {
    let probed = probed(
        domain("player", "player", mode::SERVE),
        command("player", any("/examples.blackjack.SitDown")),
    );
    assert_eq!(outcomes(&probed), expect(6, None));
    assert!(probed.passed());
}

/// A policy and a projector are handed a book of events, and pass the
/// same way.
#[test]
fn a_conforming_policy_and_projector_pass_every_step() {
    let mut policy = with(
        domain("table-wallet", "TableWalletPolicy", mode::SERVE),
        &[
            (KIND_ENV, kind::POLICY),
            (INPUT_ENV, "table"),
            (TARGET_ENV, "wallet"),
        ],
    );
    policy.kind = ComponentKindConfig::Policy;
    let probed_policy = probed(policy, events("table", any("/test.table.v1.Seated")));
    assert_eq!(outcomes(&probed_policy), expect(6, None));
    let mut projector = with(
        domain("ledger", "Ledger", mode::SERVE),
        &[(KIND_ENV, kind::PROJECTOR), (INPUT_ENV, "table")],
    );
    projector.kind = ComponentKindConfig::Projector;
    let probed_projector = probed(projector, events("table", any("/test.table.v1.Seated")));
    assert_eq!(outcomes(&probed_projector), expect(6, None));
}

/// A program that cannot be started fails the launch, and nothing after
/// it runs.
#[test]
fn a_program_that_cannot_start_fails_the_launch() {
    let mut player = domain("player", "player", mode::SERVE);
    player.launch = Some(vec!["/nonexistent/angzarr-player".to_string()]);
    let probed = probed(
        player,
        command("player", any("/examples.blackjack.SitDown")),
    );
    assert_eq!(outcomes(&probed), expect(0, Some(code!("spawn"))));
    assert!(!probed.passed());
}

/// A component that runs in process has no launch to probe.
#[test]
fn an_in_process_component_fails_the_launch() {
    let mut table = domain("table", "table", mode::SERVE);
    table.language = "rust".to_string();
    table.launch = None;
    let in_process = probed(table, command("table", any("/examples.blackjack.SitDown")));
    assert_eq!(outcomes(&in_process), expect(0, Some(code!("no-launch"))));
    let mut cobol = domain("table", "table", mode::SERVE);
    cobol.language = "cobol".to_string();
    let unknown = probed(cobol, command("table", any("/examples.blackjack.SitDown")));
    assert_eq!(
        outcomes(&unknown),
        expect(0, Some(code!("unknown-language")))
    );
}

/// A child not SERVING on the host's path within the start limit, one
/// serving no health or never listening included, fails the serving step
/// under the spec's key; so does one exiting first.
#[test]
fn a_child_not_serving_fails_the_serving_step() {
    for child in [
        with(
            domain("player", "player", mode::SERVE),
            &[(HEALTH_ENV, health::NOT_SERVING)],
        ),
        with(
            domain("player", "player", mode::SERVE),
            &[(HEALTH_ENV, health::NONE)],
        ),
        domain("player", "player", mode::SILENT),
        domain("player", "player", mode::EXITS),
    ] {
        let probed = probed(child, command("player", any("/examples.blackjack.SitDown")));
        assert_eq!(
            outcomes(&probed),
            expect(1, Some("EMBEDDED_REASON_CHILD_NOT_SERVING"))
        );
    }
}

/// A child describing itself as another kind than its entry says fails
/// the describe step.
#[test]
fn a_child_of_another_kind_fails_the_describe_step() {
    let player = with(
        domain("player", "player", mode::SERVE),
        &[
            (KIND_ENV, kind::POLICY),
            (INPUT_ENV, "table"),
            (TARGET_ENV, "wallet"),
        ],
    );
    let probed = probed(
        player,
        command("player", any("/examples.blackjack.SitDown")),
    );
    assert_eq!(outcomes(&probed), expect(2, Some(code!("kind-mismatch"))));
}

/// An input the child does not answer (it exits mid-call) fails the
/// handle step, keyed as the kernel would carry it; one of another kind
/// than the child's is not handed at all. The child is stopped either way,
/// and the shutdown step is not run.
#[test]
fn an_input_not_answered_fails_the_handle_step() {
    let died = probed(
        domain("player", "player", mode::SERVE),
        command("player", any("/examples.blackjack.Die")),
    );
    assert_eq!(
        outcomes(&died),
        expect(3, Some("ANGZARR_REASON_HANDLER_UNAVAILABLE"))
    );
    assert!(!died.passed(), "the steps before it passed; not the probe");
    let other_kind = probed(
        domain("player", "player", mode::SERVE),
        events("player", any("/examples.blackjack.SitDown")),
    );
    assert_eq!(
        outcomes(&other_kind),
        expect(3, Some("EMBEDDED_REASON_PROBE_INPUT"))
    );
}

/// A child that ignores its stdin's end fails the shutdown step: stopped
/// by SIGTERM once its grace passed, not by itself.
#[test]
fn a_child_ignoring_its_stdins_end_fails_the_shutdown_step() {
    let probed = probed(
        domain("player", "player", mode::STUBBORN),
        command("player", any("/examples.blackjack.SitDown")),
    );
    assert_eq!(
        outcomes(&probed),
        expect(4, Some("EMBEDDED_REASON_CHILD_NOT_EXITED"))
    );
    let Outcome::Failed(report) = &probed.steps[4].1 else {
        panic!("{probed:?}");
    };
    assert_eq!(
        report.to_string(),
        "EMBEDDED_REASON_CHILD_NOT_EXITED component=player ended=terminated"
    );
}

/// An upcaster sidecar is neither described nor handed an input: those
/// steps are not run, and it passes the rest.
#[test]
fn an_upcaster_sidecar_is_not_described_or_handed_an_input() {
    let mut upcaster = with(
        domain("table-upcaster", "TableUpcaster", mode::SERVE),
        &[(KIND_ENV, kind::UPCASTER)],
    );
    upcaster.kind = ComponentKindConfig::Upcaster;
    upcaster.upcasts = Some("table".to_string());
    let probed = probed(upcaster, events("table", any("/test.table.v0.Sit")));
    let passed = || Some("passed".to_string());
    assert_eq!(
        outcomes(&probed),
        [
            (Step::Launch, passed()),
            (Step::Serving, passed()),
            (Step::Describe, None),
            (Step::Handle, None),
            (Step::Shutdown, passed()),
        ]
    );
    assert!(probed.passed());
}

/// client-python's child shim (`run_child`), as the real Python player
/// runs it by its default launch (`uv run --locked angzarr-player`),
/// passes every step, its `DecideMove` answered. Ignored by default: it
/// needs uv, the player's checkout and its locked dependencies.
/// `ANGZARR_PLAYER_DIR` names the checkout's `player` directory.
#[test]
#[ignore = "needs uv and the Python player's checkout"]
fn client_pythons_child_shim_passes_every_step() {
    let dir = std::env::var_os("ANGZARR_PLAYER_DIR")
        .map(PathBuf::from)
        .expect("ANGZARR_PLAYER_DIR names the Python player");
    let player = ComponentConfig {
        name: "player".to_string(),
        language: "python".to_string(),
        kind: ComponentKindConfig::Domain,
        upcasts: None,
        launch: None,
        entry: vec!["angzarr-player".to_string()],
        dir: Some(dir),
        env: BTreeMap::new(),
        expose: Default::default(),
        scaffold: Default::default(),
    };
    let mut supervision = config(vec![]).supervision;
    supervision.handshake_timeout_ms = 120_000;
    let probed = probe(
        &player,
        &Languages::builtin(),
        &supervision,
        command(
            "player",
            any("type.googleapis.com/io.angzarr.examples.v1.DecideMove"),
        ),
    );
    assert_eq!(outcomes(&probed), expect(5, None), "{probed:?}");
}

/// The probe's shutdown and input failures are the spec's keys (decision
/// 14.62), naming the child as `component`.
#[test]
fn the_probes_failures_are_spec_keys() {
    let exit_status = HostError::ExitStatus {
        component: "player".to_string(),
        exit_code: 3,
    };
    assert_eq!(
        exit_status.to_string(),
        "EMBEDDED_REASON_CHILD_EXIT_STATUS component=player exit_code=3"
    );
    let probe_input = HostError::ProbeInput {
        component: "player".to_string(),
        kind: "policy".to_string(),
    };
    assert_eq!(
        probe_input.to_string(),
        "EMBEDDED_REASON_PROBE_INPUT component=player kind=policy"
    );
}

/// A probe's command and event are on the probe's root, their payload
/// of the type and bytes given.
#[test]
fn a_probes_input_is_on_its_root_of_the_type_given() {
    let root = Some(pb::Uuid::from_bytes(PROBE_ROOT));
    let Input::Command(command) = Input::command("player", "/x.SitDown", vec![8, 1]) else {
        panic!("a command");
    };
    let book = command.input.unwrap();
    let cover = book.cover.unwrap();
    assert_eq!(
        (cover.domain.as_str(), cover.root),
        ("player", root.clone())
    );
    let Some(pb::command_page::Body::Command(pb::Command {
        payload: Some(payload),
        handling: None,
    })) = book.page.unwrap().body
    else {
        panic!("a command page");
    };
    assert_eq!(
        (payload.type_url.as_str(), payload.value),
        ("/x.SitDown", vec![8, 1])
    );
    let Input::Events(book) = Input::event("table", "/x.Seated", vec![2]) else {
        panic!("events");
    };
    let cover = book.cover.unwrap();
    assert_eq!((cover.domain.as_str(), cover.root), ("table", root));
    assert_eq!(book.next_sequence, 1);
    let page = &book.pages[0];
    assert_eq!(page.sequence, 0);
    let payload = angzarr_router::page_event(page).unwrap();
    assert_eq!(
        (payload.type_url.as_str(), payload.value.as_slice()),
        ("/x.Seated", &[2u8][..])
    );
}
