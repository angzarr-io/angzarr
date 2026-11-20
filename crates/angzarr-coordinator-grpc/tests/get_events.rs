//! `GetEvents` over a real Unix socket: what a poller that has only a
//! socket path and a token sees, selection by selection, and what a caller
//! beyond a domain's reach sees (nothing, as of a stream that is not there).

mod counter;
mod support;

use angzarr_coordinator::{Arrival, Reach};
use angzarr_coordinator_grpc::error_info;
use angzarr_ports::EventRead;
use angzarr_proto::v1::EventBook;
use angzarr_proto_transport_distributed::v1::{
    query, temporal_query, Query, SequenceSet, TemporalQuery,
};
use angzarr_test_dir::TestDir;
use counter::{
    bearing, commands, counted, cover, events, from_sequence, serve, serve_declared, Harness,
    DOMAIN, WRITER,
};
use tonic::transport::Channel;
use tonic::Code;

async fn seeded(name: &str, facts: usize) -> (TestDir, Harness, Channel) {
    let dir = TestDir::new(name);
    let harness = serve(support::socket_in(&dir, "s.sock")).await;
    let channel = support::uds(&harness.path).await;
    for n in 0..facts {
        commands(&channel)
            .handle(bearing(counted(1, &format!("fact-{n}")), WRITER))
            .await
            .expect("applied");
    }
    (dir, harness, channel)
}

fn sequences(books: &[EventBook]) -> Vec<u64> {
    books
        .iter()
        .flat_map(|b| b.pages.iter().map(|p| p.sequence))
        .collect()
}

/// Why: a feed polls "everything after what I have seen" as from = last+1,
/// and must get exactly those pages, in order, and no others (not the
/// pages of another stream).
#[tokio::test]
async fn events_from_a_sequence_return_pages_in_order_from_it() {
    let (_dir, harness, channel) = seeded("events-from", 4).await;
    commands(&channel)
        .handle(bearing(counted(2, "elsewhere"), WRITER))
        .await
        .unwrap();

    let got = events(&channel, WRITER, from_sequence(1, 2)).await.unwrap();

    assert_eq!(sequences(&got), vec![2, 3]);
    assert!(got.iter().all(|b| b.next_sequence == 4));
    drop(harness);
}

/// Why: a poll that has caught up is not an error and not silence: it is one
/// empty book that still says where the stream ends, so the next poll asks
/// from there.
#[tokio::test]
async fn reading_past_the_head_is_one_empty_book_at_the_head() {
    let (_dir, _harness, channel) = seeded("events-head", 3).await;

    let got = events(&channel, WRITER, from_sequence(1, 3)).await.unwrap();

    assert_eq!(got.len(), 1);
    assert!(got[0].pages.is_empty());
    assert_eq!(got[0].next_sequence, 3);
}

/// Why: the other selections of a query reach the engine as the caller
/// asked: a set of sequences, a point in time as of a sequence, and no
/// selection at all (every page).
#[tokio::test]
async fn the_selections_reach_the_engine_as_asked() {
    let (_dir, _harness, channel) = seeded("events-select", 4).await;
    let with = |selection| Query {
        cover: Some(cover(1)),
        selection,
        correlation_id: None,
    };

    let set = with(Some(query::Selection::Sequences(SequenceSet {
        values: vec![0, 3],
    })));
    let as_of = with(Some(query::Selection::Temporal(TemporalQuery {
        point_in_time: Some(temporal_query::PointInTime::AsOfSequence(1)),
    })));
    let all = with(None);

    assert_eq!(
        sequences(&events(&channel, WRITER, set).await.unwrap()),
        vec![0, 3]
    );
    assert_eq!(
        sequences(&events(&channel, WRITER, as_of).await.unwrap()),
        vec![0, 1]
    );
    assert_eq!(
        sequences(&events(&channel, WRITER, all).await.unwrap()),
        vec![0, 1, 2, 3]
    );
}

/// Why: a domain the caller may not reach is indistinguishable from a
/// stream that has no events: the same empty book at 0, not an error to
/// probe; and Handle answers it exactly as it answers a domain not served.
/// Here the listener declares `ip` for a domain served to loopback only.
#[tokio::test]
async fn a_domain_beyond_the_arrival_reach_is_hidden_exactly_as_an_absent_one() {
    let dir = TestDir::new("events-hidden");
    let hidden = serve_declared(
        support::socket_in(&dir, "h.sock"),
        Reach::Loopback,
        Some(Arrival::Ip),
    )
    .await;
    let missing = serve_declared(support::socket_in(&dir, "m.sock"), Reach::None, None).await;
    let (hidden_channel, missing_channel) = (
        support::uds(&hidden.path).await,
        support::uds(&missing.path).await,
    );

    let read = events(&hidden_channel, WRITER, from_sequence(1, 0))
        .await
        .unwrap();
    assert_eq!(read.len(), 1);
    assert!(read[0].pages.is_empty());
    assert_eq!(read[0].next_sequence, 0);

    let from_hidden = commands(&hidden_channel)
        .handle(bearing(counted(1, "a"), WRITER))
        .await
        .unwrap_err();
    let from_missing = commands(&missing_channel)
        .handle(bearing(counted(1, "a"), WRITER))
        .await
        .unwrap_err();
    assert_eq!(from_hidden.code(), Code::NotFound);
    assert_eq!(from_hidden.message(), from_missing.message());
    assert_eq!(
        from_hidden.details(),
        from_missing.details(),
        "the ErrorInfo too"
    );
    assert_eq!(
        error_info(&from_hidden)
            .unwrap()
            .metadata
            .get("domain")
            .map(String::as_str),
        Some(DOMAIN)
    );
    // And nothing was written behind either.
    assert!(EventRead::covers(&*hidden.store, DOMAIN)
        .await
        .unwrap()
        .is_empty());
    assert!(EventRead::covers(&*missing.store, DOMAIN)
        .await
        .unwrap()
        .is_empty());
}

/// Why: a malformed query is the caller's mistake, said plainly and keyed.
#[tokio::test]
async fn a_malformed_root_is_invalid_argument_with_its_key() {
    let (_dir, _harness, channel) = seeded("events-bad-root", 0).await;
    let mut bad = from_sequence(1, 0);
    bad.cover.as_mut().unwrap().root = Some(angzarr_proto::v1::Uuid {
        value: vec![1; 3].into(),
    });

    let status = events(&channel, WRITER, bad).await.unwrap_err();

    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(
        error_info(&status).unwrap().reason,
        "angzarr-reads-bad-root"
    );
}
