//! Thens about what business code was delivered (`HandleRequest`, decision
//! 14.98): how often, the history and its head, the snapshot, and the
//! input's expected sequence as sent.

use cucumber::then;

use angzarr_embedded::angzarr_router::hpb::HandleRequest;
use angzarr_embedded::v1::command_page;

use crate::tests::world::Coordinator;

/// A delivery's head: its history's next sequence.
pub(crate) fn head(delivered: &HandleRequest) -> u64 {
    delivered
        .history
        .as_ref()
        .map_or(0, |events| events.next_sequence)
}

/// A delivery's input's expected sequence, as its client sent it
/// (`Command.handling`; decision 14.79).
pub(crate) fn expected(delivered: &HandleRequest) -> Option<u64> {
    match delivered
        .input
        .as_ref()
        .and_then(|book| book.page.as_ref())
        .and_then(|page| page.body.as_ref())
    {
        Some(command_page::Body::Command(command)) => {
            command.handling.and_then(|h| h.expected_sequence)
        }
        _ => None,
    }
}

/// The order domain's deliveries.
fn seen(w: &mut Coordinator) -> Vec<HandleRequest> {
    w.business("order").delivered()
}

/// The last delivery.
fn last(w: &mut Coordinator) -> HandleRequest {
    seen(w).pop().expect("business code was invoked")
}

/// A count as a scenario says it.
fn count(text: &str) -> usize {
    match text {
        "once" => 1,
        "twice" => 2,
        n => n.trim_end_matches(" times").parse().expect("a count"),
    }
}

#[then(
    regex = r"^(?:business code is not invoked|the command is not delivered to business code|the refusal is the coordinator's: business code is not invoked)$"
)]
fn not_invoked(w: &mut Coordinator) {
    assert!(seen(w).is_empty(), "{:?}", seen(w));
}

#[then("business code is invoked")]
fn invoked(w: &mut Coordinator) {
    assert!(!seen(w).is_empty());
}

#[then(
    regex = r"^(?:business code|the ingest handler|the handler) is invoked (once|twice|\d+ times)$"
)]
fn invoked_times(w: &mut Coordinator, times: String) {
    assert_eq!(seen(w).len(), count(&times));
}

#[then(regex = r"^the coordinator (?:does not re-run|re-runs) the command(?: on the new head)?$")]
fn reruns(w: &mut Coordinator) {
    // A re-run that the check refuses never reaches business code again:
    // the one delivery is the first run's.
    assert_eq!(seen(w).len(), 1, "{:?}", seen(w));
}

#[then(
    regex = r"^business code is invoked twice, the second time against the head at sequence (\d+)$"
)]
fn second_against(w: &mut Coordinator, at: u64) {
    let seen = seen(w);
    assert_eq!(seen.len(), 2);
    assert_eq!(head(&seen[1]), at);
}

#[then(
    regex = r"^(?:business code|the ingest handler|the handler) is invoked (\d+) times, each time against a newer head$"
)]
fn newer_heads(w: &mut Coordinator, times: usize) {
    let heads: Vec<u64> = seen(w).iter().map(head).collect();
    assert_eq!(heads.len(), times);
    assert!(
        heads.windows(2).all(|p| p[0] < p[1]),
        "each newer: {heads:?}"
    );
}

#[then(
    regex = r"^business code is invoked with the stream's history up to its head at sequence (\d+)$"
)]
fn history_to_head(w: &mut Coordinator, at: u64) {
    let delivered = last(w);
    assert_eq!(head(&delivered), at);
    let pages = delivered.history.map(|h| h.pages).unwrap_or_default();
    assert_eq!(
        pages.iter().map(|p| p.sequence).collect::<Vec<_>>(),
        (0..at).collect::<Vec<_>>()
    );
}

#[then("business code receives the command with no expected sequence")]
fn no_expected(w: &mut Coordinator) {
    assert_eq!(expected(&last(w)), None, "the coordinator stamps none");
}

#[then(
    regex = r"^business code receives the command with expected sequence (\d+), equal to the history's next_sequence$"
)]
fn expected_at_head(w: &mut Coordinator, sequence: u64) {
    let delivered = last(w);
    assert_eq!(expected(&delivered), Some(sequence));
    assert_eq!(head(&delivered), sequence);
}

#[then("business code receives no snapshot")]
fn no_snapshot(w: &mut Coordinator) {
    assert!(last(w).history.and_then(|e| e.snapshot).is_none());
}
