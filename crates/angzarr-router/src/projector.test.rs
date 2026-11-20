//! ProjectorDispatch contracts, transliterated from client-go's projector
//! dispatch (engine.go ProjectorDispatch.Dispatch + features/projector.go):
//! one instance folds every declared page (C-0086), undeclared domains fold
//! nothing but still finish (C-0032), unhandled event types are skipped
//! (C-0031). The answer is the projection payload alone (decision 14.78).

use std::sync::{Arc, Mutex};

use prost::Message;
use prost_types::Any;

use crate::error::{AngzarrReason, HandlerError};
use crate::hpb;
use crate::pb;
use crate::projector::{PageContext, ProjectorDispatch};
use crate::test_support::*;
use crate::type_url;

/// The fold-observing projection the Go bank calls the write model: records
/// each folded event's domain, in order.
#[derive(Default)]
struct Folded {
    entries: Vec<String>,
}

/// Folds a Cover event into the write log by domain.
fn fold_cover(p: &mut Folded, any: &Any, _ctx: &PageContext<'_>) -> Result<(), HandlerError> {
    let c =
        pb::Cover::decode(any.value.as_slice()).map_err(|e| HandlerError::Other(e.to_string()))?;
    p.entries.push(c.domain);
    Ok(())
}

/// An EventBook whose cover carries `domain`, over the given pages.
fn run(domain: &str, pages: Vec<pb::EventPage>) -> pb::EventBook {
    pb::EventBook {
        cover: Some(pb::Cover {
            domain: domain.to_string(),
            ..Default::default()
        }),
        pages,
        ..Default::default()
    }
}

/// Project's request for `events`.
fn project(events: pb::EventBook) -> hpb::ProjectRequest {
    hpb::ProjectRequest {
        events: Some(events),
    }
}

/// Project's request for a run of `domain` over `pages`.
fn book(domain: &str, pages: Vec<pb::EventPage>) -> hpb::ProjectRequest {
    project(run(domain, pages))
}

/// A projection payload carrying `count`, as the counting finishers answer.
fn counted(count: usize) -> Any {
    Any {
        type_url: type_url("test.Count"),
        value: count.to_string().into_bytes(),
    }
}

/// The count a counting finisher answered.
fn count(answer: &hpb::ProjectResponse) -> u64 {
    let payload = answer.projection.as_ref().expect("a projection");
    String::from_utf8(payload.value.clone())
        .unwrap()
        .parse()
        .unwrap()
}

/// A projector whose Finish reports the fold count — so the public dispatch
/// result observes how many folds ran. `domains == None` consumes every
/// domain.
fn counting_projector(domains: Option<&[&str]>) -> ProjectorDispatch<Folded> {
    let mut d = ProjectorDispatch::new("write-model", Folded::default)
        .on_event(&cover_full_name(), fold_cover);
    if let Some(ds) = domains {
        d = d.for_domains(ds.iter().copied());
    }
    d.finish(|p, _events| Ok(counted(p.entries.len())))
}

fn cover_pages(domain: &str, n: usize) -> Vec<pb::EventPage> {
    (0..n).map(|_| event_page(cover_any(domain))).collect()
}

#[test]
fn every_page_folds_into_one_projection() {
    let proj = counting_projector(None)
        .dispatch(&book("order", cover_pages("order", 3)))
        .expect("dispatch");
    assert_eq!(count(&proj), 3, "all three pages fold into one projection");
}

#[test]
fn single_instance_reused_across_delivery() {
    // A fresh instance per page would leave the fold count at most 1; five
    // folds into one count pins C-0086 (one instance per delivery).
    let proj = counting_projector(None)
        .dispatch(&book("order", cover_pages("order", 5)))
        .expect("dispatch");
    assert_eq!(count(&proj), 5);
}

#[test]
fn unhandled_event_type_is_skipped() {
    // Two declared Cover events plus one undeclared type: only the declared
    // pair folds (C-0031), the unknown is skipped without error.
    let mut pages = cover_pages("order", 2);
    pages.insert(
        1,
        event_page(Any {
            type_url: type_url("test.Unregistered"),
            value: Vec::new(),
        }),
    );
    let proj = counting_projector(None)
        .dispatch(&book("order", pages))
        .expect("dispatch");
    assert_eq!(count(&proj), 2, "unknown type skipped, declared pair folds");
}

// A meta-event in a run is the framework's, never folded (decision 14.106).
#[test]
fn a_meta_event_page_is_skipped() {
    let mut pages = cover_pages("order", 1);
    pages.push(pb::EventPage {
        body: Some(pb::event_page::Body::MetaEvent(pb::MetaEvent::default())),
        ..Default::default()
    });
    let proj = counting_projector(None)
        .dispatch(&book("order", pages))
        .expect("dispatch");
    assert_eq!(count(&proj), 1);
}

#[test]
fn undeclared_domain_folds_nothing_but_finishes() {
    // for_domains("order") with a run in "inventory": no page folds
    // (C-0032), but Finish still runs and answers its projection.
    let proj = counting_projector(Some(&["order"]))
        .dispatch(&book("inventory", cover_pages("inventory", 3)))
        .expect("dispatch");
    assert_eq!(count(&proj), 0, "undeclared domain folds nothing");
}

#[test]
fn declared_domain_folds() {
    let proj = counting_projector(Some(&["order"]))
        .dispatch(&book("order", cover_pages("order", 2)))
        .expect("dispatch");
    assert_eq!(count(&proj), 2);
}

// There is no wildcard domain (decision 14.53): "*" is a domain like any
// other, consuming only itself.
#[test]
fn a_star_domain_is_no_wildcard() {
    let proj = counting_projector(Some(&["order", "*"]))
        .dispatch(&book("inventory", cover_pages("inventory", 3)))
        .expect("dispatch");
    assert_eq!(count(&proj), 0, "\"*\" consumes no other domain");
}

#[test]
fn empty_run_finishes_with_zero_folds() {
    let proj = counting_projector(None)
        .dispatch(&book("order", vec![]))
        .expect("dispatch");
    assert_eq!(count(&proj), 0);
}

// Decision 14.42: the router guards no envelope; a request with no run is
// an empty one, and a run with no cover is read with an empty cover, of the
// domain "".
#[test]
fn a_run_with_no_cover_is_read_with_an_empty_cover() {
    let no_cover = project(pb::EventBook {
        pages: cover_pages("order", 2),
        ..Default::default()
    });
    let proj = counting_projector(None)
        .dispatch(&no_cover)
        .expect("folded");
    assert_eq!(count(&proj), 2, "every domain is consumed");
    let proj = counting_projector(Some(&["order"]))
        .dispatch(&no_cover)
        .expect("finished");
    assert_eq!(count(&proj), 0, "the empty domain is not order");
    let proj = counting_projector(None)
        .dispatch(&hpb::ProjectRequest::default())
        .expect("finished");
    assert_eq!(count(&proj), 0, "no run");
}

// Decision 14.78: without a finisher there is no payload to answer; the
// coordinator builds the Projection record itself.
#[test]
fn no_finish_answers_no_projection() {
    let proj = ProjectorDispatch::new("write-model", Folded::default)
        .on_event(&cover_full_name(), fold_cover)
        .dispatch(&book("order", cover_pages("order", 3)))
        .expect("dispatch");
    assert_eq!(proj, hpb::ProjectResponse { projection: None });
}

#[test]
fn finish_answers_its_payload_seeing_the_run() {
    let proj = ProjectorDispatch::new("write-model", Folded::default)
        .on_event(&cover_full_name(), fold_cover)
        .finish(|p, events| {
            assert_eq!(
                events.cover.as_ref().map(|c| c.domain.as_str()),
                Some("order")
            );
            Ok(cover_any(&p.entries.join(",")))
        })
        .dispatch(&book("order", cover_pages("order", 2)))
        .expect("dispatch");
    assert_eq!(proj.projection, Some(cover_any("order,order")));
}

#[test]
fn accessors_report_name_and_registered_types() {
    let d = counting_projector(None);
    assert_eq!(d.name(), "write-model");
    assert_eq!(d.event_types(), vec![cover_full_name()]);
}

// Owner ruling 2026-10-19: any fold of accepted events that fails is a
// handler bug, named by its page and the projection type. Decision 14.37:
// it is ANGZARR_REASON_FOLD_FAILED, the fold's own failure its cause as a
// key and args (none for an unclassified error, whose text stays in the
// handler's log), and no text crosses.
#[test]
fn a_failing_fold_is_a_handler_bug_naming_its_page() {
    for (failure, cause) in [
        (HandlerError::Other("boom".to_string()), vec![]),
        (
            HandlerError::Coded(crate::error::CodedError::immediate(
                "SOME_CODE",
                [("x".to_string(), "1".to_string())],
            )),
            vec![("cause", "SOME_CODE"), ("cause.x", "1")],
        ),
    ] {
        let failure = std::sync::Mutex::new(Some(failure));
        let d = ProjectorDispatch::new("write-model", Folded::default)
            .with_projection_type("test.Board")
            .on_event(&cover_full_name(), move |_p, _any, ctx| {
                if ctx.sequence == 1 {
                    return Err(failure.lock().unwrap().take().expect("one failure"));
                }
                Ok(())
            });
        let mut events = run("order", cover_pages("order", 2));
        events.pages[1].sequence = 1;
        let err = d
            .dispatch(&project(events))
            .expect_err("handler error must fail dispatch");
        let type_url = crate::type_url(&cover_full_name());
        assert_fold_failed(
            &err,
            "ANGZARR_REASON_FOLD_FAILED",
            &[
                (angzarr_proto::v1::reason_args::SEQUENCE, "1"),
                (angzarr_proto::v1::reason_args::TYPE_URL, type_url.as_str()),
            ],
            "/test.Board",
        );
        let caused: Vec<(&str, &str)> = err
            .extras
            .iter()
            .filter(|(k, _)| k.as_str() == "cause" || k.starts_with("cause."))
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(caused, cause, "the fold's own failure, as its cause");
    }
}

// A projector handler that panics is caught: a failing fold whose cause is
// ANGZARR_REASON_HANDLER_PANICKED (decision 14.37).
#[test]
fn a_panicking_fold_handler_is_a_fold_failure_caused_by_handler_panicked() {
    let err = ProjectorDispatch::new("write-model", Folded::default)
        .with_projection_type("test.Board")
        .on_event(&cover_full_name(), |_p, _any, _ctx| {
            panic!("index out of bounds")
        })
        .dispatch(&book("order", cover_pages("order", 1)))
        .expect_err("the handler panicked");
    assert_eq!(err.code, "ANGZARR_REASON_FOLD_FAILED");
    assert_eq!(
        err.extras.get("cause").map(String::as_str),
        Some("ANGZARR_REASON_HANDLER_PANICKED")
    );
}

#[test]
fn a_finish_failure_is_not_a_fold_failure() {
    let err = ProjectorDispatch::new("write-model", Folded::default)
        .finish(|_p, _events| Err(HandlerError::Other("boom".to_string())))
        .dispatch(&book("order", cover_pages("order", 1)))
        .expect_err("finish failed");
    assert_eq!(err.code, AngzarrReason::UnhandledHandlerError.as_str_name());
}

// Each fold sees its page's place: the run's cover (root) and the page's
// sequence, for idempotent per-page handling, and the page's metadata.
#[test]
fn folds_observe_the_run_cover_and_page_sequence() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let captured = seen.clone();
    let d = ProjectorDispatch::new("write-model", Folded::default).on_event(
        &cover_full_name(),
        move |_p, _any, ctx| {
            captured.lock().unwrap().push((
                ctx.cover
                    .and_then(|c| c.root.clone())
                    .map(|r| r.value.to_vec()),
                ctx.sequence,
                ctx.meta.and_then(|m| m.recorded_at),
            ));
            Ok(())
        },
    );
    let mut events = run("order", cover_pages("order", 2));
    events.cover.as_mut().unwrap().root = Some(uuid_of(9));
    events.pages[0].sequence = 4;
    let recorded_at = prost_types::Timestamp {
        seconds: 1_700_000_000,
        nanos: 0,
    };
    events.pages[0].meta = Some(pb::EventMeta {
        recorded_at: Some(recorded_at),
        ..Default::default()
    });
    d.dispatch(&project(events)).expect("dispatch");
    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            (Some(vec![9; 16]), 4, Some(recorded_at)),
            (Some(vec![9; 16]), 0, None)
        ]
    );
}

// --- the batched fold (router-owned projections across the C ABI) -----------

/// Each batched fold call: its page sequences and cover domain.
type FoldCalls = Arc<Mutex<Vec<(Vec<u64>, String)>>>;

/// A projector whose batched fold records each call's page sequences and
/// cover domain, and counts the pages folded.
fn batched(calls: FoldCalls) -> ProjectorDispatch<Folded> {
    ProjectorDispatch::new("write-model", Folded::default)
        .for_domains(["order"])
        .with_fold(move |p: &mut Folded, pages, cover| {
            calls.lock().unwrap().push((
                pages.iter().map(|pg| pg.sequence).collect(),
                cover.domain.clone(),
            ));
            p.entries.extend(pages.iter().map(|_| "folded".to_string()));
            Ok(())
        })
        .finish(|p, _events| Ok(counted(p.entries.len())))
}

#[test]
fn a_batched_fold_sees_every_page_of_a_consumed_run_in_one_call() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut events = run("order", cover_pages("order", 3));
    for (seq, page) in events.pages.iter_mut().enumerate() {
        page.sequence = seq as u64 + 4;
    }
    let proj = batched(calls.clone())
        .dispatch(&project(events))
        .expect("dispatch");
    assert_eq!(count(&proj), 3, "all three pages folded");
    assert_eq!(
        *calls.lock().unwrap(),
        vec![(vec![4, 5, 6], "order".to_string())]
    );
}

#[test]
fn a_batched_fold_is_not_called_for_an_undeclared_domain_or_no_pages() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let d = batched(calls.clone());
    let proj = d
        .dispatch(&book("billing", cover_pages("billing", 2)))
        .expect("dispatch");
    assert_eq!(count(&proj), 0, "finish still runs");
    d.dispatch(&book("order", vec![])).expect("dispatch");
    assert!(calls.lock().unwrap().is_empty());
}

#[test]
fn a_batched_fold_failure_names_its_page_or_the_batchs_range() {
    let mut events = run("order", cover_pages("order", 3));
    for (page, seq) in events.pages.iter_mut().zip(4..) {
        page.sequence = seq;
    }
    let events = project(events);
    let failing = |page_index: Option<usize>| {
        ProjectorDispatch::new("write-model", Folded::default)
            .with_projection_type("test.Board")
            .with_fold(move |_p, _pages, _c| {
                Err(crate::rebuild::FoldFailure {
                    page_index,
                    cause: None,
                })
            })
    };
    let err = failing(Some(2))
        .dispatch(&events)
        .expect_err("the fold failed");
    assert_fold_failed(
        &err,
        "ANGZARR_REASON_FOLD_FAILED",
        &[(angzarr_proto::v1::reason_args::SEQUENCE, "6")],
        "/test.Board",
    );
    let err = failing(Some(9))
        .dispatch(&events)
        .expect_err("the fold failed");
    assert_fold_failed(
        &err,
        "ANGZARR_REASON_FOLD_FAILED",
        &[
            (angzarr_proto::v1::reason_args::FIRST_SEQUENCE, "4"),
            (angzarr_proto::v1::reason_args::LAST_SEQUENCE, "6"),
        ],
        "/test.Board",
    );
}
