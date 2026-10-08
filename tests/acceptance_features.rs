//! Acceptance harness for `features/acceptance/end_to_end.feature` (T14).
//!
//! Drives a DEPLOYED angzarr system — kind cluster with the blackjack
//! examples (`just kind up` + skaffold) — through its public gRPC surface, using the
//! `tests/common` fixture layer. This is the "direct (deployed cluster)"
//! acceptance mode from CLAUDE.md: client → gateway → coordinator → storage.
//!
//! Cluster-bound: the harness exits 0 with a notice unless
//! `ANGZARR_ACCEPTANCE=1` is set, so `cargo test` stays green without a
//! cluster. Endpoint comes from ANGZARR_ENDPOINT / ANGZARR_HOST+ANGZARR_PORT
//! (default localhost:9084).
//!
//! `features/acceptance/compensation_emit.feature` and
//! `compensation_handle.feature` (36 scenarios) still need harness coverage —
//! they require the saga/compensation example topology; extend this binary's
//! steps when that deployment is scripted.

mod common;

use common::*;
use cucumber::{given, then, when, World};
use futures::StreamExt;
use uuid::Uuid;

/// The blackjack player-domain command this feature exercises. Field tags
/// match `angzarr-project/proto/io/angzarr/examples/blackjack/v1/player.proto`
/// — the examples protos are not compiled into the framework crate, so the
/// harness carries its own minimal mirror (display_name=1, email=2).
#[derive(Clone, PartialEq, prost::Message)]
struct RegisterPlayer {
    #[prost(string, tag = "1")]
    display_name: String,
    #[prost(string, tag = "2")]
    email: String,
}

const PLAYER_DOMAIN: &str = "player";
const REGISTER_PLAYER_TYPE: &str = "io.angzarr.examples.blackjack.v1.RegisterPlayer";

#[derive(Debug, Default, World)]
struct AcceptanceWorld {
    root: Option<Uuid>,
    response: Option<CommandResponse>,
    events: Option<EventBook>,
}

impl AcceptanceWorld {
    fn root(&self) -> Uuid {
        self.root.expect("scenario has no aggregate root yet")
    }

    async fn send_register_player(&mut self, name: &str, email: &str) {
        let book = build_command_book(
            PLAYER_DOMAIN,
            self.root(),
            RegisterPlayer {
                display_name: name.to_string(),
                email: email.to_string(),
            },
            REGISTER_PLAYER_TYPE,
        );
        // SIMPLE: synchronous accept/reject + synchronous projector results,
        // so the response carries both the events and any projections.
        let request = build_command_request(book, SyncMode::Simple);
        let response = create_gateway_client()
            .await
            .handle_command(request)
            .await
            .expect("HandleCommand failed")
            .into_inner();
        self.response = Some(response);
    }

    async fn query_events(&mut self) {
        let mut stream = create_query_client()
            .await
            .get_events(build_query(PLAYER_DOMAIN, self.root()))
            .await
            .expect("GetEvents failed")
            .into_inner();
        let mut merged: Option<EventBook> = None;
        while let Some(book) = stream.next().await {
            let book = book.expect("GetEvents stream errored");
            match &mut merged {
                None => merged = Some(book),
                Some(m) => m.pages.extend(book.pages),
            }
        }
        self.events = Some(merged.unwrap_or_default());
    }
}

#[given(expr = "the angzarr system is deployed and reachable at {string}")]
async fn system_reachable(_world: &mut AcceptanceWorld, _documented_endpoint: String) {
    // The endpoint in the feature text is documentation; the live endpoint
    // comes from the environment (see module docs). Connecting at all is the
    // assertion — create_gateway_client panics with a clear message if the
    // deployment is unreachable.
    let _ = create_gateway_client().await;
}

#[given(regex = r"^a new player aggregate.*$")]
async fn new_player_aggregate(world: &mut AcceptanceWorld) {
    world.root = Some(Uuid::new_v4());
}

#[given(expr = "a player aggregate that has processed a RegisterPlayer command")]
async fn player_with_registration(world: &mut AcceptanceWorld) {
    world.root = Some(Uuid::new_v4());
    world
        .send_register_player("Query Test", "query@test.com")
        .await;
}

#[when(expr = "a RegisterPlayer command is sent with name {string} and email {string}")]
async fn send_register_with(world: &mut AcceptanceWorld, name: String, email: String) {
    world.send_register_player(&name, &email).await;
}

#[when(expr = "a RegisterPlayer command is processed")]
async fn send_register_default(world: &mut AcceptanceWorld) {
    world
        .send_register_player("Projection Test", "projection@test.com")
        .await;
}

#[when(expr = "we query that aggregate's event history")]
async fn query_history(world: &mut AcceptanceWorld) {
    world.query_events().await;
}

#[then(expr = "the command succeeds (aggregate processed it)")]
async fn command_succeeded(world: &mut AcceptanceWorld) {
    assert!(
        world.response.is_some(),
        "no CommandResponse captured — the command did not round-trip"
    );
}

#[then(expr = "a PlayerRegistered event was persisted")]
async fn player_registered_persisted(world: &mut AcceptanceWorld) {
    world.query_events().await;
    let events = world.events.as_ref().unwrap();
    let found = events.pages.iter().any(|p| {
        p.payload
            .as_ref()
            .and_then(|payload| match payload {
                angzarr::proto::event_page::Payload::Event(any) => {
                    Some(extract_event_type(any) == "PlayerRegistered")
                }
                _ => None,
            })
            .unwrap_or(false)
    });
    assert!(
        found,
        "PlayerRegistered not found in persisted events: {:?}",
        events
            .pages
            .iter()
            .filter_map(|p| p.payload.as_ref())
            .collect::<Vec<_>>()
    );
}

#[then(expr = "the aggregate's event count is {int}")]
async fn event_count_is(world: &mut AcceptanceWorld, count: usize) {
    let events = world.events.as_ref().expect("query events first");
    assert_eq!(
        events.pages.len(),
        count,
        "expected {count} persisted events"
    );
}

#[then(expr = "we receive the PlayerRegistered event at sequence {int}")]
async fn received_at_sequence(world: &mut AcceptanceWorld, seq: u32) {
    let events = world.events.as_ref().expect("query events first");
    let page = events
        .pages
        .iter()
        .find(|p| extract_sequence(p) == seq)
        .unwrap_or_else(|| panic!("no event at sequence {seq}"));
    let any = match page.payload.as_ref() {
        Some(angzarr::proto::event_page::Payload::Event(any)) => any,
        other => panic!("page at sequence {seq} is not an event: {other:?}"),
    };
    assert_eq!(
        extract_event_type(any),
        "PlayerRegistered",
        "wrong event type at sequence {seq}"
    );
}

#[then(expr = "the response includes any synchronous projections")]
async fn response_includes_projections(world: &mut AcceptanceWorld) {
    // "any": projections appear when synchronous projector coordinators are
    // deployed; the contract here is that the field round-trips without
    // error, not that a specific projector exists in the topology.
    let response = world.response.as_ref().expect("no CommandResponse");
    println!(
        "  response carried {} synchronous projection(s)",
        response.projections.len()
    );
}

#[tokio::main]
async fn main() {
    if std::env::var("ANGZARR_ACCEPTANCE").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping acceptance harness: set ANGZARR_ACCEPTANCE=1 with a deployed \
             angzarr system (just kind up + skaffold) reachable at ANGZARR_ENDPOINT \
             / ANGZARR_HOST:ANGZARR_PORT (default localhost:9084)"
        );
        return;
    }

    AcceptanceWorld::cucumber()
        .fail_on_skipped()
        .run_and_exit("features/acceptance/end_to_end.feature")
        .await;
}
