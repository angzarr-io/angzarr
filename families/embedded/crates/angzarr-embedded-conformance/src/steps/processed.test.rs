//! processed_state.feature: the record a projector's or a policy's driver
//! keeps of what its handler processed (angzarr-ports' `Processed`, its
//! positions), which a purge never touches (decision 14.68). A projector
//! here is a recording one, caught up stored; a policy is caught up by the
//! kernel's resume, which first tells it of the streams gone.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use angzarr_embedded::angzarr_ports::{ListStreams as _, Processed as _};
use angzarr_embedded::angzarr_router::error::{CodedError, RetryableStatus};
use angzarr_embedded::v1::{Cover, EventBook};
use angzarr_embedded::{CatchUp, EventStore as _, PositionKey, Purge as _, StreamRef, Write};
use cucumber::{given, then, when};

use crate::tests::policy::locked;
use crate::tests::world::Coordinator;

/// A catch-up projector a scenario declares: its source domain, and every
/// book it was handed.
#[derive(Clone, Default)]
pub(crate) struct Projected {
    pub(crate) domains: Vec<String>,
    pub(crate) books: Arc<Mutex<Vec<EventBook>>>,
    /// What it answers its next run with instead of a projection.
    pub(crate) fails: Arc<Mutex<Option<CodedError>>>,
}

/// The projector handler that records what it is handed.
struct Recording {
    name: String,
    books: Arc<Mutex<Vec<EventBook>>>,
    fails: Arc<Mutex<Option<CodedError>>>,
}

impl angzarr_embedded::ProjectorHandler for Recording {
    fn name(&self) -> &str {
        &self.name
    }

    async fn dispatch(
        &self,
        request: &angzarr_embedded::angzarr_router::hpb::ProjectRequest,
    ) -> Result<
        angzarr_embedded::angzarr_router::hpb::ProjectResponse,
        angzarr_embedded::angzarr_router::error::CodedError,
    > {
        let book = request.events.clone().unwrap_or_default();
        self.books.lock().unwrap().push(book);
        if let Some(error) = self.fails.lock().unwrap().take() {
            return Err(error);
        }
        Ok(angzarr_embedded::angzarr_router::hpb::ProjectResponse::default())
    }
}

/// Order `name`'s stream, `n` events long, with `handler`'s position at its
/// end: what the handler has processed.
fn processed_to(w: &mut Coordinator, handler: &str, name: &str, n: usize) -> Cover {
    let cover = w.cover("order", name);
    let names = vec!["Happened"; n];
    w.write(&cover, &names);
    let stream = StreamRef::try_from(&cover).unwrap();
    let key = PositionKey { handler, stream };
    futures_ready(w.store.write(Write::positions(&[(key, n as u64)])));
    cover
}

/// A future the in-memory store answers at once.
fn futures_ready<T>(
    future: impl std::future::Future<Output = Result<T, angzarr_embedded::StoreError>>,
) -> T {
    angzarr_embedded::testing::block_on_ready(future).expect("the in-memory store")
}

#[given(
    regex = r#"^a projector "(\w+)" of domain "([\w-]+)" that has processed order "([\w-]+)" to sequence (\d+)$"#
)]
fn projector_processed(w: &mut Coordinator, name: String, domain: String, order: String, n: usize) {
    w.projectors.insert(
        name.clone(),
        Projected {
            domains: vec![domain],
            ..Projected::default()
        },
    );
    processed_to(w, &name, &order, n);
}

#[given(
    regex = r#"^a policy "(\w+)" translating from "([\w-]+)" to "([\w-]+)" that has processed order "([\w-]+)" to sequence (\d+)$"#
)]
fn policy_processed(
    w: &mut Coordinator,
    name: String,
    input: String,
    output: String,
    order: String,
    n: usize,
) {
    w.policy(&name, &input, &output);
    processed_to(w, &name, &order, n);
}

#[when(regex = r#"^order "([\w-]+)" is purged(?: while "\w+" is not running)?$"#)]
fn purged(w: &mut Coordinator, order: String) {
    let cover = w.cover("order", &order);
    let stream = StreamRef::try_from(&cover).unwrap();
    let head = futures_ready(w.store.head(stream)).next_sequence;
    futures_ready(w.store.purge(stream, head));
}

#[given(regex = r#"^order "([\w-]+)" is purged while "\w+" is not running$"#)]
fn purged_given(w: &mut Coordinator, order: String) {
    purged(w, order);
}

#[when(regex = r#"^"(\w+)" catches up$"#)]
async fn catches_up(w: &mut Coordinator, name: String) {
    match w.projectors.get(&name).cloned() {
        Some(projected) => {
            let recording = Recording {
                name: name.clone(),
                books: Arc::clone(&projected.books),
                fails: Arc::clone(&projected.fails),
            };
            let registry = w.registry();
            let sources: Vec<&str> = projected.domains.iter().map(String::as_str).collect();
            let mut catch_up = CatchUp::stored(&sources, recording).unwrap();
            let store = w.store.clone();
            catch_up
                .step(&store, registry.catalog())
                .await
                .expect("the catch-up steps");
        }
        None => {
            let resumed = w.resume().await;
            w.resumed.push(resumed);
        }
    }
}

/// The books `name`, a projector or a policy, was handed of `cover`.
fn handed(w: &Coordinator, name: &str, cover: &Cover) -> Vec<EventBook> {
    let books = match w.projectors.get(name) {
        Some(projected) => projected.books.lock().unwrap().clone(),
        None => {
            let def = w
                .policies
                .iter()
                .find(|p| p.name == name)
                .expect("a policy");
            locked(&def.script).sources.clone()
        }
    };
    books
        .into_iter()
        .filter(|b| b.cover.as_ref() == Some(cover))
        .collect()
}

#[then(regex = r#"^"(\w+)" is handed order "([\w-]+)" as gone(, and answers nothing)?$"#)]
fn handed_gone(w: &mut Coordinator, name: String, order: String, nothing: String) {
    let cover = w.cover("order", &order);
    let books = handed(w, &name, &cover);
    assert_eq!(
        books,
        [EventBook {
            cover: Some(cover),
            ..EventBook::default()
        }],
        "its gone book alone"
    );
    if !nothing.is_empty() {
        let others: Vec<String> = (w.domains.keys())
            .filter(|d| d.as_str() != "order")
            .cloned()
            .collect();
        for domain in others {
            assert!(w.business(&domain).delivered().is_empty(), "{domain}");
        }
    }
}

#[then(regex = r#"^"(\w+)" is handed nothing of order "([\w-]+)"$"#)]
fn handed_nothing(w: &mut Coordinator, name: String, order: String) {
    let cover = w.cover("order", &order);
    assert_eq!(handed(w, &name, &cover), []);
}

#[then(
    regex = r#"^(?:the|after its affirmative answer the) record of "(\w+)" (?:still )?holds order "([\w-]+)" at (?:next_)?sequence (\d+)$"#
)]
fn still_holds(w: &mut Coordinator, name: String, order: String, n: u64) {
    let cover = w.cover("order", &order);
    let record = futures_ready(w.store.processed(&name));
    assert!(record.contains(&(cover, n)), "{record:?}");
}

/// The coordinator builds no `Projection` value in process: the run's
/// cover, the projector's name and the position after it are the record's
/// own key and value, which the projector's record holds once it answered.
#[then(
    regex = r#"^the coordinator builds the Projection with cover order "([\w-]+)", projector "(\w+)" and next_sequence (\d+)$"#
)]
fn builds_projection(w: &mut Coordinator, order: String, name: String, n: u64) {
    still_holds(w, name, order, n);
}

#[then(regex = r#"^the record of "(\w+)" no longer holds order "([\w-]+)"$"#)]
fn no_longer_holds(w: &mut Coordinator, name: String, order: String) {
    let cover = w.cover("order", &order);
    let record = futures_ready(w.store.processed(&name));
    assert!(record.iter().all(|(c, _)| *c != cover), "{record:?}");
    assert!(!futures_ready(w.store.list_streams()).contains(&cover));
}

#[then(regex = r#"^"(\w+)"'s React is not called for order "([\w-]+)"$"#)]
fn react_not_called(w: &mut Coordinator, name: String, order: String) {
    let cover = w.cover("order", &order);
    let def = w
        .policies
        .iter()
        .find(|p| p.name == name)
        .expect("the policy");
    let sources = crate::tests::policy::locked(&def.script).sources.clone();
    assert!(
        sources.iter().all(|s| s.cover.as_ref() != Some(&cover)),
        "{sources:?}"
    );
}

#[given(
    regex = r#"^a projector "(\w+)" of (?:domain "([\w-]+)"|domains "([\w-]+)" and "([\w-]+)")$"#
)]
fn projector_of(w: &mut Coordinator, name: String, one: String, first: String, second: String) {
    let domains = match one.is_empty() {
        true => vec![first, second],
        false => vec![one],
    };
    w.projectors.insert(
        name,
        Projected {
            domains,
            ..Projected::default()
        },
    );
}

#[given(regex = r#"^order "([\w-]+)" holds events at sequences 0 to (\d+)$"#)]
fn order_holds(w: &mut Coordinator, order: String, last: usize) {
    let cover = w.cover("order", &order);
    w.write(&cover, &vec!["Happened"; last + 1]);
}

#[given(regex = r#"^order "([\w-]+)" and payment "([\w-]+)" each record events$"#)]
fn each_record(w: &mut Coordinator, order: String, payment: String) {
    for cover in [w.cover("order", &order), w.cover("payment", &payment)] {
        w.write(&cover, &["Happened", "Happened"]);
    }
}

#[given(regex = r#"^"(\w+)" has processed order "([\w-]+)" to next_sequence (\d+)$"#)]
fn has_processed(w: &mut Coordinator, name: String, order: String, n: u64) {
    let cover = w.cover("order", &order);
    let stream = StreamRef::try_from(&cover).unwrap();
    let key = PositionKey {
        handler: &name,
        stream,
    };
    futures_ready(w.store.write(Write::positions(&[(key, n)])));
}

#[given(
    regex = r#"^"(\w+)" (answers UNAVAILABLE|answers INTERNAL|does not answer before its deadline) for its next run$"#
)]
fn fails_next_run(w: &mut Coordinator, name: String, failure: String) {
    // A deadline missed reaches the kernel as the port's DEADLINE_EXCEEDED.
    let error = match failure.as_str() {
        "answers UNAVAILABLE" => {
            CodedError::retryable_bug(RetryableStatus::Unavailable, "VIEW_STORE_UNAVAILABLE", [])
        }
        "answers INTERNAL" => CodedError::bug("VIEW_STORE_CORRUPT", []),
        _ => CodedError::retryable_bug(RetryableStatus::DeadlineExceeded, "PROJECTOR_DEADLINE", []),
    };
    let projected = w.projectors.get(&name).expect("the projector");
    *projected.fails.lock().unwrap() = Some(error);
}

#[when(
    regex = r#"^the coordinator delivers (?:the run of sequences (\d+) to (\d+) )?to "(\w+)"(?: and it answers a projection)?$"#
)]
async fn delivers_run(w: &mut Coordinator, first: String, last: String, name: String) {
    catches_up(w, name.clone()).await;
    if let (Ok(first), Ok(last)) = (first.parse::<u64>(), last.parse::<u64>()) {
        assert_eq!(last_run(w, &name), (first..=last).collect::<Vec<_>>());
    }
}

/// The sequences of the last book `name` was handed.
fn last_run(w: &Coordinator, name: &str) -> Vec<u64> {
    let books = w.projectors[name].books.lock().unwrap().clone();
    let book = books.last().expect("a run delivered");
    book.pages.iter().map(|p| p.sequence).collect()
}

#[then(regex = r#"^the run of sequences (\d+) to (\d+) is delivered to "(\w+)" again$"#)]
async fn delivered_again(w: &mut Coordinator, first: u64, last: u64, name: String) {
    catches_up(w, name.clone()).await;
    let run: Vec<u64> = (first..=last).collect();
    let books = w.projectors[&name].books.lock().unwrap().clone();
    let runs = books
        .iter()
        .filter(|b| b.pages.iter().map(|p| p.sequence).collect::<Vec<_>>() == run)
        .count();
    assert_eq!(runs, 2, "{books:?}");
}

#[then("each Project call holds pages of one stream only, in sequence order")]
fn one_stream_each(w: &mut Coordinator) {
    let books: Vec<EventBook> = w
        .projectors
        .values()
        .flat_map(|p| p.books.lock().unwrap().clone())
        .collect();
    let covers: BTreeSet<String> = books.iter().map(|b| format!("{:?}", b.cover)).collect();
    assert_eq!(covers.len(), 2, "each stream's run: {books:?}");
    for book in &books {
        let sequences: Vec<u64> = book.pages.iter().map(|p| p.sequence).collect();
        assert!(sequences.windows(2).all(|p| p[0] < p[1]), "{book:?}");
    }
}

#[then("no order is promised between the order stream's runs and the payment stream's runs")]
fn no_order_promised(_w: &mut Coordinator) {
    // Nothing to observe: a projector may be handed either stream first.
}
