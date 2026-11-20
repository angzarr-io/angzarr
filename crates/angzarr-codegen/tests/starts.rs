//! A process manager's starting input marked on its adapter's handler
//! (`starts`), a fact's as a command's: decision 14.69's ruling 9 made the
//! buy-in's starting StartBuyIn a fact, and a starting fact the markup
//! does not mark `starts` can never start an instance (the coordinator
//! answers COORDINATOR_REASON_PM_NO_INSTANCE). The fixture is
//! `intake/v1/intake.proto`.

mod common;

use angzarr_codegen::adapters;
use common::*;

const INTAKE: &str = "intake/v1/intake.proto";

fn adapter() -> String {
    adapters(&descriptors(&[INTAKE]), &[INTAKE], &[])
        .unwrap()
        .replace([' ', '\n'], "")
}

#[test]
fn test_a_starting_fact_is_marked_starts() {
    let source = adapter();
    assert!(
        source.contains("#[handles_fact(StartIntake,starts)]"),
        "{source}"
    );
}

#[test]
fn test_a_fact_that_does_not_start_is_not_marked_starts() {
    let source = adapter();
    assert!(
        source.contains("#[handles_fact(RecordArrival)]"),
        "{source}"
    );
}

/// The process manager lists its starting types as the macro's
/// `starting_types`, which cross-checks them against the handlers marked
/// `starts` (an input listed and unmarked, or marked and unlisted, does not
/// compile); an aggregate has no such argument.
#[test]
fn test_a_process_manager_lists_its_starting_types() {
    let source = adapter();
    assert!(
        source.contains("#[process_manager(domain=\"intake\",command=IntakeStateCommand,event=IntakeStateEvent,starting_types=[StartIntake])]"),
        "{source}"
    );
    let buyin = adapters(&descriptors(BUYIN), BUYIN, &[])
        .unwrap()
        .replace([' ', '\n'], "");
    assert!(buyin.contains("starting_types=[StartBuyIn])]"), "{buyin}");
    assert!(
        buyin.contains(
            "#[aggregate(domain=\"wallet\",command=WalletStateCommand,event=WalletStateEvent)]"
        ),
        "{buyin}"
    );
    assert_eq!(buyin.matches("starting_types").count(), 1, "{buyin}");
}
