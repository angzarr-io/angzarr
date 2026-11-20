//! Rebuilder — state reconstruction from an EventBook: the starting state
//! the coordinator delivered (decoded with the state's codec, when it has
//! one), then every event page it is given, in order (decision 14.40). The
//! coordinator chose the snapshot and dropped the pages it covers; the
//! router compares no sequence with it.

use std::collections::HashMap;

use prost_types::Any;

use crate::error::{AngzarrReason, CodedError, FoldAt};
use crate::pb;
use crate::PageContext;

/// Folds one event payload into state, seeing where the event sits.
/// Generated/binding thunks unmarshal to the typed event and call the pure
/// typed applier; decode errors surface here so the engine can classify them.
pub type ApplierFn<S> = Box<
    dyn for<'a> Fn(
            &mut S,
            &Any,
            &PageContext<'a>,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
        + Send
        + Sync,
>;

/// Decodes a delivered starting state (an `Any` of the state message) into
/// state: the inverse of [`EncodeFn`].
pub type DecodeFn<S> =
    Box<dyn Fn(&Any) -> Result<S, Box<dyn std::error::Error + Send + Sync>> + Send + Sync>;

/// Encodes state as an `Any` of the state message: what every answer
/// carries as the state after it (decision 14.12).
pub type EncodeFn<S> = Box<dyn Fn(&S) -> Any + Send + Sync>;

/// The state's one codec (decision 14.40): decodes the starting state it is
/// delivered and encodes the state it answers.
struct StateCodec<S> {
    decode: DecodeFn<S>,
    encode: EncodeFn<S>,
}

/// What the rebuild consumed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RebuildInfo {
    /// True when the book carried any history (pages or a starting state)
    /// — the "does this aggregate exist" signal. Reflects page presence, not
    /// applier hits.
    pub had_prior_events: bool,
    /// Number of pages whose type had a registered applier.
    pub applied_count: usize,
}

/// Reconstructs state of type S from an EventBook.
///
/// Error semantics: a page that fails to fold FAILS the rebuild —
/// commands must never validate against silently-truncated state. The
/// events were accepted by the same apply logic, so the failure is a handler
/// bug (INTERNAL; [`fold_error`]), and so is a page whose applier or batched
/// fold panics, its cause ANGZARR_REASON_HANDLER_PANICKED (decision 14.37). A starting
/// state the codec cannot decode is ANGZARR_REASON_PERSISTED_EVENT_CORRUPT; one whose decode
/// panics is ANGZARR_REASON_FOLD_FAILED with cause ANGZARR_REASON_HANDLER_PANICKED, naming the delivered
/// snapshot's sequence and its state's type URL. Event types with no
/// registered applier are skipped: not every event folds into state.
pub struct Rebuilder<S> {
    factory: Box<dyn Fn() -> S + Send + Sync>,
    codec: Option<StateCodec<S>>,
    appliers: HashMap<String, ApplierFn<S>>,
    fold: Option<FoldFn<S>>,
    /// The state message's type URL ("" when undeclared).
    state_type_url: String,
}

/// How a batched fold failed: the index of the failing page within the
/// batch it was handed, when it knows, and its own failure as a key and
/// args, when it has one (decision 14.37: never text).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FoldFailure {
    pub page_index: Option<usize>,
    pub cause: Option<pb::Diagnostic>,
}

/// Where the pages a fold failed on came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageOrigin {
    /// Persisted events: a rebuild's history, a projector's delivery.
    Persisted,
    /// The events the input being handled emitted (any step's), not yet
    /// persisted.
    Emitted,
}

impl PageOrigin {
    /// The key a fold failing on pages from here answers with: ANGZARR_REASON_FOLD_FAILED
    /// for persisted events, ANGZARR_REASON_FOLD_FAILED_EMITTED for emitted ones
    /// (decision 14.37).
    pub fn reason(self) -> AngzarrReason {
        match self {
            PageOrigin::Persisted => AngzarrReason::FoldFailed,
            PageOrigin::Emitted => AngzarrReason::FoldFailedEmitted,
        }
    }
}

/// The error for a fold of `pages` (from `origin`) into state of
/// `state_type_url` that failed with `failure`: names the page
/// `failure.page_index` indexes, or the batch's sequence range when the
/// index is missing or outside the batch.
pub fn fold_error(
    origin: PageOrigin,
    pages: &[pb::EventPage],
    failure: &FoldFailure,
    state_type_url: &str,
) -> CodedError {
    let at = match failure.page_index.and_then(|index| pages.get(index)) {
        Some(page) => return page_fold_error(origin, page, state_type_url, failure.cause.as_ref()),
        None => FoldAt::Range {
            first: pages.first().map_or(0, |p| p.sequence),
            last: pages.last().map_or(0, |p| p.sequence),
        },
    };
    CodedError::fold_failed(origin.reason(), at, state_type_url, failure.cause.as_ref())
}

/// An applier's failure as a fold's cause: its key and args when it failed
/// with a [`CodedError`], else none (its text stays in its own log;
/// decision 14.37).
fn applier_cause(err: &(dyn std::error::Error + Send + Sync + 'static)) -> Option<pb::Diagnostic> {
    match err.downcast_ref::<CodedError>() {
        Some(coded) => Some(coded.diagnostic()),
        None => {
            tracing::debug!(error = %err, "an applier's unclassified failure");
            None
        }
    }
}

/// A panicking fold's cause: ANGZARR_REASON_HANDLER_PANICKED, with no args (the panic's
/// message stays in the handler's own log; decision 14.37).
fn panicked_cause() -> pb::Diagnostic {
    pb::Diagnostic {
        code: AngzarrReason::HandlerPanicked.as_str_name().to_string(),
        ..Default::default()
    }
}

/// Runs an applier on one event: its failure, or its panic, as the fold's
/// cause ([`applier_cause`], [`panicked_cause`]). A panic is answered, never
/// unwound into the host.
fn run_applier<S>(
    thunk: &ApplierFn<S>,
    state: &mut S,
    event: &Any,
    ctx: &PageContext<'_>,
) -> Result<(), Option<pb::Diagnostic>> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| thunk(state, event, ctx))) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(applier_cause(&*err)),
        Err(_) => Err(Some(panicked_cause())),
    }
}

/// Runs a batched fold: its panic is a failure at no known page, caused by
/// ANGZARR_REASON_HANDLER_PANICKED ([`panicked_cause`]), answered rather than unwound into
/// the host.
fn run_fold<S>(
    fold: &FoldFn<S>,
    state: &mut S,
    pages: &[pb::EventPage],
    cover: Option<&pb::Cover>,
) -> Result<(), FoldFailure> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fold(state, pages, cover)))
        .unwrap_or_else(|_| {
            Err(FoldFailure {
                page_index: None,
                cause: Some(panicked_cause()),
            })
        })
}

/// The type URL a component declares for the message `full_name`: "" (none
/// declared) for an empty name.
pub fn declared_type_url(full_name: &str) -> String {
    if full_name.is_empty() {
        String::new()
    } else {
        crate::type_url(full_name)
    }
}

/// The error for a fold that failed on `page`.
pub fn page_fold_error(
    origin: PageOrigin,
    page: &pb::EventPage,
    state_type_url: &str,
    cause: Option<&pb::Diagnostic>,
) -> CodedError {
    let type_url = crate::page_event(page).map_or("", |e| e.type_url.as_str());
    CodedError::fold_failed(
        origin.reason(),
        FoldAt::Page {
            sequence: page.sequence,
            type_url,
        },
        state_type_url,
        cause,
    )
}

/// Folds a batch of event pages (with their book's cover) into state in one
/// call: the batched alternative to per-type appliers, for state the router
/// holds on a host's behalf (router-owned state across the C ABI).
pub type FoldFn<S> = Box<
    dyn Fn(&mut S, &[pb::EventPage], Option<&pb::Cover>) -> Result<(), FoldFailure> + Send + Sync,
>;

impl<S> Rebuilder<S> {
    /// A Rebuilder whose fresh state comes from `factory`.
    pub fn new(factory: impl Fn() -> S + Send + Sync + 'static) -> Self {
        Rebuilder {
            factory: Box::new(factory),
            codec: None,
            appliers: HashMap::new(),
            fold: None,
            state_type_url: String::new(),
        }
    }

    /// A Rebuilder starting from `S::default()` whose state type comes from
    /// `S`'s prost name: nobody declares the string by hand. Its codec is
    /// the state message's own protobuf encoding.
    pub fn of_message() -> Self
    where
        S: prost::Name + Default + 'static,
    {
        Rebuilder::new(S::default)
            .with_state_type(&S::full_name())
            .with_state_codec(
                |start: &Any| Ok(S::decode(start.value.as_slice())?),
                |state: &S| Any {
                    type_url: crate::type_url(&S::full_name()),
                    value: prost::Message::encode_to_vec(state),
                },
            )
    }

    /// Declares the state's codec (decision 14.40), for state held as
    /// something other than its own message: `decode` turns a delivered
    /// starting state (an `Any` of the state message) into state, and
    /// `encode` is its inverse, the state every answer carries. Without
    /// one, a delivered starting state is ignored and no answer carries a
    /// state.
    pub fn with_state_codec(
        mut self,
        decode: impl Fn(&Any) -> Result<S, Box<dyn std::error::Error + Send + Sync>>
            + Send
            + Sync
            + 'static,
        encode: impl Fn(&S) -> Any + Send + Sync + 'static,
    ) -> Self {
        self.codec = Some(StateCodec {
            decode: Box::new(decode),
            encode: Box::new(encode),
        });
        self
    }

    /// `state` encoded by the state's codec, when the rebuilder has one.
    pub fn encode_state(&self, state: &S) -> Option<Any> {
        self.codec.as_ref().map(|codec| (codec.encode)(state))
    }

    /// Declares the state message's fully-qualified name, for state held as
    /// something other than its own message (an `Any` across the C ABI).
    /// An empty name declares none.
    pub fn with_state_type(mut self, full_name: &str) -> Self {
        self.state_type_url = declared_type_url(full_name);
        self
    }

    /// The declared state type URL ("" when undeclared).
    pub fn state_type_url(&self) -> &str {
        &self.state_type_url
    }

    /// Registers the applier for a fully-qualified event type name.
    pub fn apply(
        mut self,
        full_name: &str,
        thunk: impl Fn(&mut S, &Any) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.appliers.insert(
            full_name.to_string(),
            Box::new(move |state, any, _ctx| thunk(state, any)),
        );
        self
    }

    /// Registers an applier that also sees the page's context (the book's
    /// cover and the page's sequence).
    pub fn apply_with_context(
        mut self,
        full_name: &str,
        thunk: impl for<'a> Fn(
                &mut S,
                &Any,
                &PageContext<'a>,
            ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.appliers.insert(full_name.to_string(), Box::new(thunk));
        self
    }

    /// Folds pages through one batched `fold` instead of the per-type
    /// appliers: a rebuild hands it every page (and the book's
    /// cover) in a single call, and none when there are none. Its failure
    /// fails the rebuild as a handler bug ([`fold_error`]); it is never
    /// called again for the same pages.
    pub fn with_fold(
        mut self,
        fold: impl Fn(&mut S, &[pb::EventPage], Option<&pb::Cover>) -> Result<(), FoldFailure>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.fold = Some(Box::new(fold));
        self
    }

    /// Folds the book into state: the starting state it carries (decoded by
    /// the codec), else fresh state, then every page in order. An absent
    /// book normalizes to an empty one (fresh state, no prior events) —
    /// callers never need their own guard.
    pub fn rebuild(&self, book: Option<&pb::EventBook>) -> Result<(S, RebuildInfo), CodedError> {
        let mut state = (self.factory)();
        let mut info = RebuildInfo::default();
        let Some(book) = book else {
            return Ok((state, info));
        };
        info.had_prior_events = !book.pages.is_empty() || book.snapshot.is_some();

        if let (Some(snapshot), Some(codec)) = (book.snapshot.as_ref(), self.codec.as_ref()) {
            if let Some(start) = snapshot.state.as_ref() {
                let decoded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    (codec.decode)(start)
                }));
                state = match decoded {
                    Ok(Ok(decoded)) => decoded,
                    Ok(Err(_)) => return Err(CodedError::persisted_corrupt(&start.type_url)),
                    // A panic is a defect in the codec, not corrupt data:
                    // the fold fails, naming the snapshot (decision 14.37).
                    Err(_) => {
                        return Err(CodedError::fold_failed(
                            AngzarrReason::FoldFailed,
                            FoldAt::Page {
                                sequence: snapshot.sequence,
                                type_url: &start.type_url,
                            },
                            &self.state_type_url,
                            Some(&panicked_cause()),
                        ));
                    }
                };
            }
        }

        if let Some(fold) = self.fold.as_ref() {
            if !book.pages.is_empty() {
                run_fold(fold, &mut state, &book.pages, book.cover.as_ref()).map_err(
                    |failure| {
                        fold_error(
                            PageOrigin::Persisted,
                            &book.pages,
                            &failure,
                            &self.state_type_url,
                        )
                    },
                )?;
                info.applied_count = book.pages.len();
            }
            return Ok((state, info));
        }

        for page in &book.pages {
            let Some(event) = crate::page_event(page) else {
                continue;
            };
            let Some(thunk) = self
                .appliers
                .get(crate::type_name_from_url(&event.type_url))
            else {
                continue;
            };
            let ctx = PageContext {
                cover: book.cover.as_ref(),
                sequence: page.sequence,
                meta: page.meta.as_ref(),
            };
            if let Err(cause) = run_applier(thunk, &mut state, event, &ctx) {
                return Err(page_fold_error(
                    PageOrigin::Persisted,
                    page,
                    &self.state_type_url,
                    cause.as_ref(),
                ));
            }
            info.applied_count += 1;
        }
        Ok((state, info))
    }
}

impl<S> Rebuilder<S> {
    /// Folds `pages` (from `origin`) of a book with `cover` into `state`:
    /// through the batched fold in one call when one is registered (none for
    /// no pages), otherwise page by page through the appliers. A failure is
    /// a handler bug ([`fold_error`]) whose message says where the pages
    /// came from.
    pub fn apply_pages(
        &self,
        state: &mut S,
        pages: &[pb::EventPage],
        cover: Option<&pb::Cover>,
        origin: PageOrigin,
    ) -> Result<(), CodedError> {
        if let Some(fold) = self.fold.as_ref() {
            if pages.is_empty() {
                return Ok(());
            }
            return run_fold(fold, state, pages, cover)
                .map_err(|failure| fold_error(origin, pages, &failure, &self.state_type_url));
        }
        for page in pages {
            self.fold_page(state, page, cover, origin)?;
        }
        Ok(())
    }

    /// Folds `events` a handler emitted (payloads, not yet persisted) into
    /// `state`, each seen as it will be persisted: wrapped as an EventPage
    /// at the sequence it will be given (`next_sequence` onward, in order)
    /// with the `meta` its coordinator will stamp
    /// ([`crate::context::CommandContext::event_meta`]), so an applier sees
    /// at decide time what replay hands it (the answered events stay
    /// payloads only). A failure is a handler bug naming the input's own
    /// event ([`PageOrigin::Emitted`]).
    pub fn apply_emitted(
        &self,
        state: &mut S,
        events: &[Any],
        cover: Option<&pb::Cover>,
        next_sequence: u64,
        meta: &pb::EventMeta,
    ) -> Result<(), CodedError> {
        let numbered: Vec<pb::EventPage> = events
            .iter()
            .zip(next_sequence..)
            .map(|(event, sequence)| pb::EventPage {
                body: Some(pb::event_page::Body::Event(event.clone())),
                meta: Some(meta.clone()),
                sequence,
            })
            .collect();
        self.apply_pages(state, &numbered, cover, PageOrigin::Emitted)
    }

    /// Folds one persisted page of a book with `cover` into `state` through
    /// the batched fold when one is registered, else its applier, returning
    /// whether anything ran. A page with no event or an unapplied type is
    /// skipped; a failing fold is a handler bug ([`page_fold_error`]).
    pub fn apply_page(
        &self,
        state: &mut S,
        page: &pb::EventPage,
        cover: Option<&pb::Cover>,
    ) -> Result<bool, CodedError> {
        self.fold_page(state, page, cover, PageOrigin::Persisted)
    }

    fn fold_page(
        &self,
        state: &mut S,
        page: &pb::EventPage,
        cover: Option<&pb::Cover>,
        origin: PageOrigin,
    ) -> Result<bool, CodedError> {
        if let Some(fold) = self.fold.as_ref() {
            let pages = std::slice::from_ref(page);
            run_fold(fold, state, pages, cover)
                .map_err(|failure| fold_error(origin, pages, &failure, &self.state_type_url))?;
            return Ok(true);
        }
        let Some(event) = crate::page_event(page) else {
            return Ok(false);
        };
        let Some(thunk) = self
            .appliers
            .get(crate::type_name_from_url(&event.type_url))
        else {
            return Ok(false);
        };
        let ctx = PageContext {
            cover,
            sequence: page.sequence,
            meta: page.meta.as_ref(),
        };
        run_applier(thunk, state, event, &ctx)
            .map_err(|cause| page_fold_error(origin, page, &self.state_type_url, cause.as_ref()))?;
        Ok(true)
    }
}

#[cfg(test)]
#[path = "rebuild.test.rs"]
mod rebuild_tests;
