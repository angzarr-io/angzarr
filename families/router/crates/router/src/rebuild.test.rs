//! Rebuilder contracts, transliterated from client-go's engine_test.go +
//! engine_boundaries_test.go (mutation-hardened bank, kill 0.972). The
//! boundary knowledge these encode — HadPriorEvents shapes, gap pages never
//! terminal, every given page folded onto the starting state (decision
//! 14.40) — is the contract the shared core must satisfy byte-for-byte.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use prost::Message;
use prost_types::Any;

use crate::error::AngzarrReason;
use crate::pb;
use crate::rebuild::{FoldFailure, PageOrigin, Rebuilder};
use crate::test_support::*;
use angzarr_proto::v1::reason_args;

/// `r` with a state codec whose decode answers a state that has applied
/// only `label`, and whose encode answers an empty `Any`.
fn starting_at(r: Rebuilder<TestState>, label: &'static str) -> Rebuilder<TestState> {
    r.with_state_codec(
        move |_| {
            Ok(TestState {
                applied: vec![label.to_string()],
            })
        },
        |_| Any::default(),
    )
}

#[test]
fn applies_all_pages_in_order() {
    let r = cover_applier(fresh_rebuilder());

    let (state, info) = r
        .rebuild(Some(&book_of_covers(&["a", "b", "c"])))
        .expect("rebuild");
    assert_eq!(state.applied, vec!["a", "b", "c"]);
    assert!(
        info.had_prior_events,
        "had_prior_events = false with 3 pages (the Exists() bug)"
    );
    assert_eq!(info.applied_count, 3);
}

#[test]
fn absent_book_normalizes_to_fresh_state() {
    let r = fresh_rebuilder();
    let (state, info) = r.rebuild(None).expect("rebuild(None)");
    assert!(state.applied.is_empty(), "state must be fresh");
    assert!(
        !info.had_prior_events,
        "had_prior_events for an absent book"
    );
}

// Owner ruling 2026-10-19: the events were accepted by the same apply
// logic, so an applier failing on one is a handler bug (INTERNAL), named by
// the failing page, the state type and the applier's own text.
#[test]
fn an_applier_failing_on_a_persisted_event_is_a_handler_bug_naming_it() {
    let r = cover_applier(fresh_rebuilder().with_state_type("test.Ledger"));
    let book = pb::EventBook {
        pages: vec![
            sequenced_event_page(4, cover_any("a")),
            sequenced_event_page(5, corrupt_cover_any()),
        ],
        ..Default::default()
    };

    let err = r
        .rebuild(Some(&book))
        .expect_err("a failing applier must fail the rebuild, not yield factory-default state");
    let type_url = crate::type_url(&cover_full_name());
    assert_fold_failed(
        &err,
        "ANGZARR_REASON_FOLD_FAILED",
        &[
            (reason_args::SEQUENCE, "5"),
            (reason_args::TYPE_URL, type_url.as_str()),
        ],
        "/test.Ledger",
    );
    // Decision 14.37: the applier's own text stays in its log; none crosses,
    // and an unclassified failure has no cause.
    assert!(
        !err.extras.contains_key("fold_message"),
        "no fold text crosses: {:?}",
        err.extras
    );
    assert!(
        !err.extras.contains_key(reason_args::CAUSE),
        "an unclassified failure has no cause: {:?}",
        err.extras
    );
}

// An applier that fails with a CodedError hands the fold its key and args:
// the failing fold's cause (decision 14.37).
#[test]
fn an_applier_failing_with_a_coded_error_gives_the_fold_its_cause() {
    let r = fresh_rebuilder().apply(&cover_full_name(), |_state: &mut TestState, _event| {
        Err(Box::new(crate::error::CodedError::immediate(
            "BAD_COVER",
            [("field".to_string(), "domain".to_string())],
        )))
    });
    let err = r
        .rebuild(Some(&pb::EventBook {
            pages: vec![sequenced_event_page(2, cover_any("a"))],
            ..Default::default()
        }))
        .expect_err("the applier failed");
    assert_eq!(err.code, "ANGZARR_REASON_FOLD_FAILED");
    assert_eq!(
        err.extras.get("cause").map(String::as_str),
        Some("BAD_COVER")
    );
    assert_eq!(
        err.extras.get("cause.field").map(String::as_str),
        Some("domain")
    );
}

#[test]
fn a_rebuilder_declares_no_state_type_unless_told() {
    assert_eq!(fresh_rebuilder().state_type_url(), "");
    assert_eq!(fresh_rebuilder().with_state_type("").state_type_url(), "");
    let err = cover_applier(fresh_rebuilder())
        .rebuild(Some(&pb::EventBook {
            pages: vec![event_page(corrupt_cover_any())],
            ..Default::default()
        }))
        .expect_err("failed");
    assert_eq!(err.extras[reason_args::STATE_TYPE_URL], "");
}

#[test]
fn of_message_takes_the_state_type_from_the_prost_name() {
    let r: Rebuilder<LedgerState> = Rebuilder::of_message();
    assert_eq!(r.state_type_url(), "/test.ledger.LedgerState");
    let (state, _) = r.rebuild(None).expect("fresh");
    assert_eq!(state, LedgerState::default());
}

#[test]
fn a_starting_state_the_codec_cannot_decode_is_persisted_event_corrupt() {
    let r = fresh_rebuilder().with_state_codec(|_any| Err("bad state".into()), |_| Any::default());
    let book = pb::EventBook {
        snapshot: Some(pb::Snapshot {
            state: Some(cover_any("snap")),
            ..Default::default()
        }),
        ..Default::default()
    };
    let err = r.rebuild(Some(&book)).expect_err("unloadable");
    assert_eq!(err.code, AngzarrReason::PersistedEventCorrupt.as_str_name());
}

#[test]
fn unknown_event_type_skipped() {
    let r = cover_applier(fresh_rebuilder());
    let book = pb::EventBook {
        pages: vec![
            // no applier registered — not all events fold into state
            event_page(any_of(&pb::Snapshot::default())),
            event_page(cover_any("a")),
        ],
        ..Default::default()
    };

    let (state, info) = r.rebuild(Some(&book)).expect("rebuild");
    assert_eq!(state.applied, vec!["a"], "want exactly the Cover");
    assert!(
        info.had_prior_events,
        "had_prior_events must reflect page presence, not applier hits"
    );
    assert_eq!(
        info.applied_count, 1,
        "applied_count counts applier hits only"
    );
}

#[test]
fn the_starting_state_is_decoded_before_the_pages_fold() {
    let r = cover_applier(fresh_rebuilder().with_state_codec(
        |start| {
            let c = pb::Cover::decode(start.value.as_slice())?;
            Ok(TestState {
                applied: vec![format!("snap:{}", c.domain)],
            })
        },
        |_| Any::default(),
    ));

    let book = pb::EventBook {
        snapshot: Some(pb::Snapshot {
            sequence: 0,
            state: Some(cover_any("base")),
            ..Default::default()
        }),
        pages: vec![sequenced_event_page(1, cover_any("after"))],
        ..Default::default()
    };

    let (state, info) = r.rebuild(Some(&book)).expect("rebuild");
    assert_eq!(state.applied, vec!["snap:base", "after"]);
    assert!(info.had_prior_events, "a snapshot alone is prior history");
}

// Decision 14.40: the coordinator drops the pages a snapshot covers, and the
// router folds every page it is given onto the starting state, without
// comparing sequences with the snapshot's: per-type appliers and a batched
// fold alike.
#[test]
fn every_given_page_is_folded_even_at_or_below_the_starting_states_sequence() {
    let book = pb::EventBook {
        snapshot: Some(pb::Snapshot {
            sequence: 2,
            state: Some(cover_any("start")),
            ..Default::default()
        }),
        pages: vec![
            sequenced_event_page(1, cover_any("one")),
            sequenced_event_page(2, cover_any("two")),
            sequenced_event_page(3, cover_any("three")),
        ],
        ..Default::default()
    };
    let (state, info) = cover_applier(starting_at(fresh_rebuilder(), "start"))
        .rebuild(Some(&book))
        .expect("rebuild");
    assert_eq!(state.applied, vec!["start", "one", "two", "three"]);
    assert_eq!(info.applied_count, 3);

    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (state, info) = batch_rebuilder(calls.clone())
        .rebuild(Some(&book))
        .expect("rebuild");
    assert_eq!(state.applied, vec!["snap", "fold3"]);
    assert_eq!(*calls.lock().unwrap(), vec![(vec![1, 2, 3], String::new())]);
    assert_eq!(info.applied_count, 3);
}

#[test]
fn empty_book_no_prior_history() {
    let r = fresh_rebuilder();
    let (_, info) = r.rebuild(Some(&pb::EventBook::default())).expect("rebuild");
    assert!(
        !info.had_prior_events,
        "pageless, snapshotless history is fresh state"
    );
}

#[test]
fn snapshot_only_is_prior_history() {
    let r = fresh_rebuilder();
    let book = pb::EventBook {
        snapshot: Some(pb::Snapshot {
            sequence: 5,
            state: Some(cover_any("")),
            ..Default::default()
        }),
        ..Default::default()
    };
    let (_, info) = r.rebuild(Some(&book)).expect("rebuild");
    assert!(info.had_prior_events, "a snapshot alone is prior history");
}

#[test]
fn no_starting_state_in_book_decode_not_invoked() {
    let calls = Arc::new(AtomicUsize::new(0));
    let r = counting_decode(calls.clone());
    r.rebuild(Some(&book_of_covers(&["a"]))).expect("rebuild");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "the codec decoded with no starting state in the book"
    );
}

#[test]
fn snapshot_without_state_decode_not_invoked() {
    let calls = Arc::new(AtomicUsize::new(0));
    let r = counting_decode(calls.clone());
    let book = pb::EventBook {
        snapshot: Some(pb::Snapshot {
            sequence: 3, // no state payload
            ..Default::default()
        }),
        ..Default::default()
    };
    r.rebuild(Some(&book)).expect("rebuild");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "the codec decoded a stateless snapshot"
    );
}

/// A rebuilder whose codec counts its decodes.
fn counting_decode(calls: Arc<AtomicUsize>) -> Rebuilder<TestState> {
    fresh_rebuilder().with_state_codec(
        move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(TestState::default())
        },
        |_| Any::default(),
    )
}

#[test]
fn a_starting_state_without_a_codec_is_ignored_safely() {
    let r = cover_applier(fresh_rebuilder());
    let mut book = book_of_covers(&["after"]);
    book.snapshot = Some(pb::Snapshot {
        state: Some(cover_any("base")),
        ..Default::default()
    });

    let (state, _) = r.rebuild(Some(&book)).expect("rebuild");
    assert_eq!(
        state.applied,
        vec!["after"],
        "no codec declared — the starting state cannot decode"
    );
}

// A pageless entry (no event payload) is skipped, never ends the fold.
#[test]
fn pageless_entries_do_not_end_the_fold() {
    let r = cover_applier(fresh_rebuilder());
    let book = pb::EventBook {
        pages: vec![
            pb::EventPage::default(), // no payload
            event_page(cover_any("after-gap")),
        ],
        ..Default::default()
    };
    let (state, _) = r.rebuild(Some(&book)).expect("rebuild");
    assert_eq!(
        state.applied,
        vec!["after-gap"],
        "the gap page is skipped, not terminal"
    );
}

// Without a codec the starting state never decodes, and every page folds
// onto fresh state.
#[test]
fn without_a_codec_every_page_folds_onto_fresh_state() {
    let r = cover_applier(fresh_rebuilder());
    let book = pb::EventBook {
        snapshot: Some(pb::Snapshot {
            sequence: 2,
            state: Some(cover_any("snap")),
            ..Default::default()
        }),
        pages: vec![
            sequenced_event_page(1, cover_any("one")),
            sequenced_event_page(2, cover_any("two")),
            sequenced_event_page(3, cover_any("three")),
        ],
        ..Default::default()
    };
    let (state, _) = r.rebuild(Some(&book)).expect("rebuild");
    assert_eq!(state.applied, vec!["one", "two", "three"]);
}

// A snapshot with no state decodes nothing: every page folds onto fresh
// state.
#[test]
fn a_stateless_snapshot_folds_every_page_onto_fresh_state() {
    let r = cover_applier(starting_at(fresh_rebuilder(), "snapshot"));
    let book = pb::EventBook {
        snapshot: Some(pb::Snapshot {
            sequence: 2,
            ..Default::default()
        }),
        pages: vec![
            sequenced_event_page(1, cover_any("one")),
            sequenced_event_page(2, cover_any("two")),
        ],
        ..Default::default()
    };
    let (state, _) = r.rebuild(Some(&book)).expect("rebuild");
    assert_eq!(state.applied, vec!["one", "two"]);
}

// Appliers registered with context see the book's cover, each page's
// sequence and its metadata (here, its correlation's first byte; 0: none).
#[test]
fn context_appliers_see_the_book_cover_and_page_sequence() {
    let r = fresh_rebuilder().apply_with_context(&cover_full_name(), |s, _any, ctx| {
        s.applied.push(format!(
            "{}@{}#{}",
            ctx.cover.map_or("", |c| c.domain.as_str()),
            ctx.sequence,
            ctx.meta
                .and_then(|m| m.correlation_id.as_ref())
                .map_or(0, |u| u.value[0])
        ));
        Ok(())
    });
    let mut book = pb::EventBook {
        cover: Some(pb::Cover {
            domain: "ledger".to_string(),
            ..Default::default()
        }),
        pages: vec![
            sequenced_event_page(4, cover_any("a")),
            event_page(cover_any("b")),
        ],
        ..Default::default()
    };
    book.pages[0].meta = Some(pb::EventMeta {
        correlation_id: Some(uuid_of(7)),
        ..Default::default()
    });
    let (state, _) = r.rebuild(Some(&book)).expect("rebuild");
    assert_eq!(state.applied, vec!["ledger@4#7", "ledger@0#0"]);

    book.cover = None;
    let mut state = TestState::default();
    assert!(r
        .apply_page(
            &mut state,
            &book.pages[0],
            Some(&pb::Cover {
                domain: "facts".to_string(),
                ..Default::default()
            })
        )
        .expect("apply"));
    assert_eq!(state.applied, vec!["facts@4#7"]);
    assert!(!r
        .apply_page(&mut state, &pb::EventPage::default(), None)
        .expect("no event"));
}

// --- the batched fold (router-owned state across the C ABI) ----------------

/// Each batched fold call: its page sequences and cover domain.
type FoldCalls = Arc<std::sync::Mutex<Vec<(Vec<u64>, String)>>>;

/// A fold recording each call's page sequences and the cover domain.
fn batch_rebuilder(calls: FoldCalls) -> crate::rebuild::Rebuilder<TestState> {
    starting_at(fresh_rebuilder(), "snap").with_fold(move |s: &mut TestState, pages, cover| {
        calls.lock().unwrap().push((
            pages.iter().map(|p| p.sequence).collect(),
            cover.map(|c| c.domain.clone()).unwrap_or_default(),
        ));
        s.applied.push(format!("fold{}", pages.len()));
        Ok(())
    })
}

#[test]
fn a_batched_fold_sees_every_page_in_one_call() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let r = batch_rebuilder(calls.clone());
    let book = pb::EventBook {
        cover: Some(pb::Cover {
            domain: "ledger".to_string(),
            ..Default::default()
        }),
        snapshot: Some(pb::Snapshot {
            sequence: 1,
            state: Some(cover_any("snap")),
            ..Default::default()
        }),
        pages: vec![
            sequenced_event_page(1, cover_any("at-the-snapshot")),
            sequenced_event_page(2, cover_any("a")),
            sequenced_event_page(3, prost_types::Any::default()),
        ],
        ..Default::default()
    };
    let (state, info) = r.rebuild(Some(&book)).expect("rebuild");
    assert_eq!(state.applied, vec!["snap", "fold3"]);
    assert_eq!(
        *calls.lock().unwrap(),
        vec![(vec![1, 2, 3], "ledger".to_string())]
    );
    assert_eq!(info.applied_count, 3, "every folded page counts");
}

#[test]
fn a_batched_fold_is_not_called_without_pages() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let r = batch_rebuilder(calls.clone());
    r.rebuild(None).expect("absent");
    r.rebuild(Some(&pb::EventBook::default())).expect("empty");
    assert!(calls.lock().unwrap().is_empty());
}

/// A rebuilder over "/test.State" whose batched fold fails with `failure`,
/// counting its calls.
fn failing_fold(failure: FoldFailure, calls: Arc<AtomicUsize>) -> Rebuilder<TestState> {
    fresh_rebuilder().with_state_type("test.State").with_fold(
        move |_s: &mut TestState, _pages, _cover| {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(failure.clone())
        },
    )
}

/// Three Cover pages at sequences 4, 5 and 6.
fn three_pages() -> pb::EventBook {
    let mut book = book_of_covers(&["a", "b", "c"]);
    for (page, seq) in book.pages.iter_mut().zip(4..) {
        page.sequence = seq;
    }
    book
}

#[test]
fn a_batched_fold_failure_names_the_page_it_indexes() {
    let calls = Arc::new(AtomicUsize::new(0));
    let r = failing_fold(
        FoldFailure {
            page_index: Some(1),
            cause: Some(pb::Diagnostic {
                code: "NO".to_string(),
                ..Default::default()
            }),
        },
        calls.clone(),
    );
    let err = r
        .rebuild(Some(&three_pages()))
        .expect_err("the fold failed");
    let type_url = crate::type_url(&cover_full_name());
    assert_fold_failed(
        &err,
        "ANGZARR_REASON_FOLD_FAILED",
        &[
            (reason_args::SEQUENCE, "5"),
            (reason_args::TYPE_URL, type_url.as_str()),
        ],
        "/test.State",
    );
    assert!(
        !err.extras.contains_key("fold_message"),
        "no fold text crosses (decision 14.37): {:?}",
        err.extras
    );
    assert_eq!(
        err.extras.get(reason_args::CAUSE).map(String::as_str),
        Some("NO"),
        "the fold's own failure is its cause"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "the fold is never re-run");
}

#[test]
fn an_unlocated_or_out_of_range_fold_failure_names_the_batchs_range() {
    for page_index in [None, Some(3)] {
        let calls = Arc::new(AtomicUsize::new(0));
        let r = failing_fold(
            FoldFailure {
                page_index,
                cause: None,
            },
            calls.clone(),
        );
        let err = r
            .rebuild(Some(&three_pages()))
            .expect_err("the fold failed");
        assert_fold_failed(
            &err,
            "ANGZARR_REASON_FOLD_FAILED",
            &[
                (reason_args::FIRST_SEQUENCE, "4"),
                (reason_args::LAST_SEQUENCE, "6"),
            ],
            "/test.State",
        );
        assert!(!err.extras.contains_key(reason_args::SEQUENCE));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "the fold is never re-run");
    }
}

#[test]
fn folding_emitted_pages_says_the_command_emitted_them() {
    let r = failing_fold(
        FoldFailure {
            page_index: Some(0),
            cause: None,
        },
        Arc::default(),
    );
    let mut state = TestState::default();
    let book = three_pages();
    let err = r
        .apply_pages(&mut state, &book.pages, None, PageOrigin::Emitted)
        .expect_err("failed");
    assert_fold_failed(
        &err,
        "ANGZARR_REASON_FOLD_FAILED_EMITTED",
        &[(reason_args::SEQUENCE, "4")],
        "/test.State",
    );
    let err = r
        .apply_pages(&mut state, &book.pages, None, PageOrigin::Persisted)
        .expect_err("failed");
    assert_eq!(err.code, "ANGZARR_REASON_FOLD_FAILED");

    // Per-page appliers say the same.
    let r = cover_applier(fresh_rebuilder());
    let err = r
        .apply_pages(
            &mut state,
            &[event_page(corrupt_cover_any())],
            None,
            PageOrigin::Emitted,
        )
        .expect_err("failed");
    assert_eq!(err.code, "ANGZARR_REASON_FOLD_FAILED_EMITTED");
}

#[test]
fn apply_pages_folds_a_batch_in_one_call() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let r = batch_rebuilder(calls.clone());
    let mut state = TestState::default();
    let book = book_of_covers(&["a", "b"]);
    r.apply_pages(&mut state, &book.pages, None, PageOrigin::Persisted)
        .expect("folded");
    r.apply_pages(&mut state, &[], None, PageOrigin::Persisted)
        .expect("nothing to fold");
    assert_eq!(*calls.lock().unwrap(), vec![(vec![0, 1], String::new())]);

    // Without a batched fold, apply_pages applies each page through its applier.
    let r = cover_applier(fresh_rebuilder());
    let mut state = TestState::default();
    r.apply_pages(&mut state, &book.pages, None, PageOrigin::Persisted)
        .expect("applied");
    assert_eq!(state.applied, vec!["a", "b"]);
}

#[test]
fn apply_page_goes_through_a_batched_fold_when_registered() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let r = batch_rebuilder(calls.clone());
    let mut state = TestState::default();
    assert!(r
        .apply_page(&mut state, &sequenced_event_page(4, cover_any("a")), None)
        .expect("folded"));
    assert_eq!(*calls.lock().unwrap(), vec![(vec![4], String::new())]);
}

// --- a panicking applier or batched fold (decision 14.37) -------------------
//
// A panic in a fold is a handler bug like an error is: the fold fails as
// ANGZARR_REASON_FOLD_FAILED (ANGZARR_REASON_FOLD_FAILED_EMITTED for the command's own events) with cause
// ANGZARR_REASON_HANDLER_PANICKED, and the router answers rather than unwinding into its
// host. The panic's message stays in the handler's own log.

/// A rebuilder over "/test.State" whose Cover applier panics.
fn panicking_applier() -> Rebuilder<TestState> {
    fresh_rebuilder()
        .with_state_type("test.State")
        .apply(&cover_full_name(), |_state: &mut TestState, _event| {
            panic!("index out of bounds")
        })
}

/// A rebuilder over "/test.State" whose batched fold panics.
fn panicking_fold() -> Rebuilder<TestState> {
    fresh_rebuilder()
        .with_state_type("test.State")
        .with_fold(|_s: &mut TestState, _pages, _cover| panic!("index out of bounds"))
}

/// The fold failed with cause ANGZARR_REASON_HANDLER_PANICKED and no other cause args.
fn assert_panicked_cause(err: &crate::error::CodedError) {
    assert_eq!(
        err.extras.get(reason_args::CAUSE).map(String::as_str),
        Some(AngzarrReason::HandlerPanicked.as_str_name()),
        "a panicking fold's cause: {:?}",
        err.extras
    );
    assert!(
        !err.extras.keys().any(|k| k.starts_with("cause.")),
        "a panic's message stays in the handler's log: {:?}",
        err.extras
    );
}

/// Runs `fold`, failing the test if its panic escapes.
fn answered<T>(fold: impl FnOnce() -> T) -> T {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(fold))
        .unwrap_or_else(|_| panic!("the fold's panic escaped the router"))
}

#[test]
fn a_panicking_applier_fails_the_rebuild_naming_the_page() {
    let r = panicking_applier();
    let err = answered(|| r.rebuild(Some(&three_pages()))).expect_err("the applier panicked");
    let type_url = crate::type_url(&cover_full_name());
    assert_fold_failed(
        &err,
        "ANGZARR_REASON_FOLD_FAILED",
        &[
            (reason_args::SEQUENCE, "4"),
            (reason_args::TYPE_URL, type_url.as_str()),
        ],
        "/test.State",
    );
    assert_panicked_cause(&err);
}

#[test]
fn a_panicking_batched_fold_fails_the_rebuild_naming_the_batchs_range() {
    let r = panicking_fold();
    let err = answered(|| r.rebuild(Some(&three_pages()))).expect_err("the fold panicked");
    assert_fold_failed(
        &err,
        "ANGZARR_REASON_FOLD_FAILED",
        &[
            (reason_args::FIRST_SEQUENCE, "4"),
            (reason_args::LAST_SEQUENCE, "6"),
        ],
        "/test.State",
    );
    assert!(!err.extras.contains_key(reason_args::SEQUENCE));
    assert_panicked_cause(&err);
}

#[test]
fn a_panicking_batched_fold_of_emitted_pages_is_fold_failed_emitted() {
    let r = panicking_fold();
    let mut state = TestState::default();
    let book = three_pages();
    let err = answered(|| r.apply_pages(&mut state, &book.pages, None, PageOrigin::Emitted))
        .expect_err("the fold panicked");
    assert_fold_failed(
        &err,
        "ANGZARR_REASON_FOLD_FAILED_EMITTED",
        &[
            (reason_args::FIRST_SEQUENCE, "4"),
            (reason_args::LAST_SEQUENCE, "6"),
        ],
        "/test.State",
    );
    assert_panicked_cause(&err);
}

#[test]
fn a_panicking_applier_of_emitted_pages_is_fold_failed_emitted() {
    let r = panicking_applier();
    let mut state = TestState::default();
    let err = answered(|| {
        r.apply_emitted(
            &mut state,
            &[cover_any("a")],
            None,
            7,
            &pb::EventMeta::default(),
        )
    })
    .expect_err("the applier panicked");
    assert_fold_failed(
        &err,
        "ANGZARR_REASON_FOLD_FAILED_EMITTED",
        &[(reason_args::SEQUENCE, "7")],
        "/test.State",
    );
    assert_panicked_cause(&err);
}

#[test]
fn a_panicking_batched_fold_of_one_page_names_it() {
    let r = panicking_fold();
    let mut state = TestState::default();
    let err = answered(|| r.apply_page(&mut state, &sequenced_event_page(9, cover_any("a")), None))
        .expect_err("the fold panicked");
    assert_fold_failed(
        &err,
        "ANGZARR_REASON_FOLD_FAILED",
        &[
            (reason_args::FIRST_SEQUENCE, "9"),
            (reason_args::LAST_SEQUENCE, "9"),
        ],
        "/test.State",
    );
    assert_panicked_cause(&err);
}

// A panicking codec is a defect in user code, not corrupt data (corrupt
// bytes are a decode error): ANGZARR_REASON_FOLD_FAILED with cause ANGZARR_REASON_HANDLER_PANICKED, naming
// the snapshot by its sequence and its state's type URL.
#[test]
fn a_panicking_codec_fails_the_rebuild_naming_the_snapshot() {
    let r = cover_applier(fresh_rebuilder().with_state_type("test.State"))
        .with_state_codec(|_any| panic!("index out of bounds"), |_| Any::default());
    let snap_state = cover_any("snap");
    let book = pb::EventBook {
        snapshot: Some(pb::Snapshot {
            sequence: 3,
            state: Some(snap_state.clone()),
            ..Default::default()
        }),
        pages: vec![sequenced_event_page(4, cover_any("a"))],
        ..Default::default()
    };
    let err = answered(|| r.rebuild(Some(&book))).expect_err("the codec panicked");
    assert_fold_failed(
        &err,
        "ANGZARR_REASON_FOLD_FAILED",
        &[
            (reason_args::SEQUENCE, "3"),
            (reason_args::TYPE_URL, snap_state.type_url.as_str()),
        ],
        "/test.State",
    );
    assert_panicked_cause(&err);
}
