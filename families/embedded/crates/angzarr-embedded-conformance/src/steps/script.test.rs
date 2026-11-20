//! Givens scripting the business process and the coordinator's caps: what
//! the business process answers each delivery, what it reports with it,
//! what lands on the stream while it decides, and how often the
//! coordinator tries again.

use cucumber::given;

use angzarr_embedded::OnRetryable;

use crate::tests::business::{rejection, Answer, Race};
use crate::tests::world::package;
use crate::tests::world::Coordinator;

/// The delivery a scenario names, from 1.
fn nth(ordinal: &str) -> usize {
    match ordinal {
        "first" => 1,
        "second" => 2,
        "third" => 3,
        other => panic!("no ordinal {other}"),
    }
}

#[given(
    regex = r"^business code (?:refuses|rejects) the (?:command|reaction) with an? (\w+) refusal$"
)]
fn refuses(w: &mut Coordinator, refusal: String) {
    w.business("order")
        .every(|a| a.refuse = Some(rejection(&package("order"), &refusal)));
}

#[given(
    regex = r"^business code (?:refuses|rejects) the (first|second) delivery with an? (\w+) refusal$"
)]
fn refuses_nth(w: &mut Coordinator, ordinal: String, refusal: String) {
    w.business("order").nth(nth(&ordinal), |a| {
        a.refuse = Some(rejection(&package("order"), &refusal))
    });
}

#[given(regex = r"^the business process answers every (?:delivery|run) with an ItemAdded event$")]
fn answers_every(w: &mut Coordinator) {
    w.business("order").every(|a| a.refuse = None);
}

#[given(
    regex = r"^the (?:ingest )?handler answers (?:every run )?with a ShipmentDispatched event$"
)]
fn ingests(w: &mut Coordinator) {
    w.business("order").every(|a: &mut Answer| a.refuse = None);
}

#[given(
    regex = r#"^another write to the "order" stream lands (?:at sequence \d+ )?after (the command is loaded|the step's command is loaded|the fact is loaded|each run's load) and before (?:the command|the fact|the input|the reaction|it) is written$"#
)]
fn another_write(w: &mut Coordinator, when: String) {
    let business = w.business("order");
    match when.starts_with("each") {
        true => business.every(|a| a.race = Some(Race::SameStream)),
        false => business.nth(1, |a| a.race = Some(Race::SameStream)),
    }
}

#[given(r#"another write lands on a different "order" root's stream after the command is loaded and before it is written"#)]
fn another_root_write(w: &mut Coordinator) {
    w.business("order")
        .nth(1, |a| a.race = Some(Race::OtherRoot));
}

#[given(regex = r"^the retry cap is reached after (\d+) (?:deliveries|attempts)$")]
fn retry_cap(w: &mut Coordinator, deliveries: u32) {
    w.retry = OnRetryable {
        max_retries: deliveries - 1,
        ..w.retry
    };
}

#[given(regex = r"^the race re-run cap is (?:reached after )?(\d+)(?: runs)?$")]
fn race_cap(w: &mut Coordinator, runs: u32) {
    w.retry = OnRetryable {
        max_race_runs: std::num::NonZeroU32::new(runs).expect("a cap of at least one run"),
        ..w.retry
    };
}

#[given(
    regex = r"^the (\w+) aggregate declares (?:facts? )?(\w+)(?: and (\w+))?(?: (?:an? )?facts?)?(?:,? (?:each )?with the \(fact\) option)?$"
)]
fn declares_facts(w: &mut Coordinator, domain: String, first: String, second: String) {
    let business = w.business(&domain);
    business.declare_fact(&first);
    if !second.is_empty() {
        business.declare_fact(&second);
    }
}
