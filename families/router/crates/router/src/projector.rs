//! ProjectorDispatch — the events → projection table.
//!
//! `ProjectorService.Project` (decision 14.78): one projection instance
//! per delivery folds every declared event page of one stream's run in
//! order (the instance is reused across pages — spec C-0086), then an
//! optional Finish packs it into the answer's projection, payload only
//! (`ProjectResponse.projection`): the coordinator builds the Projection
//! record (its cover, projector and position) and marks the run only on
//! this affirmative answer (decision 14.97). Without a Finish the answer
//! carries no projection. Unlike the aggregate there is no prior-state
//! rebuild, no snapshot, and no refusal path: an event with no registered
//! handler is skipped with a warn; a run whose cover carries an undeclared
//! domain never folds, though Finish still runs. There is no wildcard
//! domain (decision 14.53): a projector lists every domain it reads.

use std::collections::{HashMap, HashSet};

use prost_types::Any;

use crate::error::{call_handler, map_handler_error, AngzarrReason, CodedError, HandlerError};
use crate::hpb;
use crate::pb;
use crate::rebuild::{fold_error, page_fold_error, FoldFailure, PageOrigin};

pub use crate::PageContext;

/// Folds one delivered event page into the rebuilding projection. Generated
/// thunks unmarshal to the typed event and call the typed business method.
pub type EventFn<P> =
    Box<dyn for<'a> Fn(&mut P, &Any, &PageContext<'a>) -> Result<(), HandlerError> + Send + Sync>;

/// Packs the folded projection instance into the answer's projection
/// payload. When absent, the answer carries none.
pub type FinishFn<P> =
    Box<dyn Fn(&mut P, &pb::EventBook) -> Result<prost_types::Any, HandlerError> + Send + Sync>;

/// Folds every page of a consumed book into the projection in one call: the
/// batched alternative to per-type folds, for a projection the router holds
/// on a host's behalf (router-owned projections across the C ABI).
pub type BatchFoldFn<P> =
    Box<dyn Fn(&mut P, &[pb::EventPage], &pb::Cover) -> Result<(), FoldFailure> + Send + Sync>;

/// The dispatch table for one projector component.
pub struct ProjectorDispatch<P> {
    name: String,
    factory: Box<dyn Fn() -> P + Send + Sync>,
    domains: Option<HashSet<String>>,
    handlers: HashMap<String, EventFn<P>>,
    fold: Option<BatchFoldFn<P>>,
    finish: Option<FinishFn<P>>,
    /// The projection message's type URL ("" when undeclared).
    projection_type_url: String,
}

impl<P> ProjectorDispatch<P> {
    /// An empty projector table over a fresh-projection factory.
    pub fn new(name: impl Into<String>, factory: impl Fn() -> P + Send + Sync + 'static) -> Self {
        ProjectorDispatch {
            name: name.into(),
            factory: Box::new(factory),
            domains: None,
            handlers: HashMap::new(),
            fold: None,
            finish: None,
            projection_type_url: String::new(),
        }
    }

    /// Folds through one batched `fold` instead of the per-type folds: a
    /// delivery whose domain the projector consumes hands it every page (and
    /// the book's cover) in a single call, and none when there are none.
    pub fn with_fold(
        mut self,
        fold: impl Fn(&mut P, &[pb::EventPage], &pb::Cover) -> Result<(), FoldFailure>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.fold = Some(Box::new(fold));
        self
    }

    /// Declares the projection message's fully-qualified name, named by a
    /// failing fold's error (`state_type_url`). An empty name declares none.
    pub fn with_projection_type(mut self, full_name: &str) -> Self {
        self.projection_type_url = crate::rebuild::declared_type_url(full_name);
        self
    }

    /// Restricts folding to books whose cover carries one of these domains,
    /// each named (no wildcard, decision 14.53). Unset (the default)
    /// consumes every domain.
    pub fn for_domains(mut self, domains: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.domains = Some(domains.into_iter().map(Into::into).collect());
        self
    }

    /// Registers the fold thunk for a fully-qualified event type name.
    pub fn on_event(
        mut self,
        full_name: &str,
        thunk: impl for<'a> Fn(&mut P, &Any, &PageContext<'a>) -> Result<(), HandlerError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.handlers.insert(full_name.to_string(), Box::new(thunk));
        self
    }

    /// Registers the finisher that packs the folded instance into the
    /// answer's projection payload. Without it the answer carries none.
    pub fn finish(
        mut self,
        thunk: impl Fn(&mut P, &pb::EventBook) -> Result<prost_types::Any, HandlerError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.finish = Some(Box::new(thunk));
        self
    }

    /// The component name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The registered fully-qualified event type names.
    pub fn event_types(&self) -> Vec<String> {
        self.handlers.keys().cloned().collect()
    }

    /// `ProjectorService.Project`: folds every page of the request's run
    /// into one projection instance, then runs the finisher, answering its
    /// payload (none without one). An undeclared domain folds nothing but
    /// still finishes; an event type with no thunk, and a meta-event, are
    /// skipped. It guards no envelope (decision 14.42): a request with no
    /// run is an empty one, and a run with no cover is read as one with an
    /// empty cover.
    pub fn dispatch(&self, req: &hpb::ProjectRequest) -> Result<hpb::ProjectResponse, CodedError> {
        let unsent = pb::EventBook::default();
        let events = req.events.as_ref().unwrap_or(&unsent);
        let uncovered = pb::Cover::default();
        let cover = events.cover.as_ref().unwrap_or(&uncovered);

        let mut projection = (self.factory)();
        let consumed = match self.domains.as_ref() {
            Some(set) => set.contains(&cover.domain),
            None => true,
        };

        if let (true, Some(fold)) = (consumed, self.fold.as_ref()) {
            if !events.pages.is_empty() {
                fold(&mut projection, &events.pages, cover).map_err(|failure| {
                    fold_error(
                        PageOrigin::Persisted,
                        &events.pages,
                        &failure,
                        &self.projection_type_url,
                    )
                })?;
            }
        } else if consumed {
            for page in &events.pages {
                let Some(event_any) = crate::page_event(page) else {
                    continue;
                };
                let Some(thunk) = self
                    .handlers
                    .get(crate::type_name_from_url(&event_any.type_url))
                else {
                    // Parity with the Go engine's projectorLogger.Warn.
                    tracing::warn!(
                        projector = %self.name,
                        type_url = %event_any.type_url,
                        "projector received event with no matching handler"
                    );
                    continue;
                };
                let ctx = PageContext {
                    cover: Some(cover),
                    sequence: page.sequence,
                    meta: page.meta.as_ref(),
                };
                fold_handler(|| thunk(&mut projection, event_any, &ctx)).map_err(|cause| {
                    page_fold_error(
                        PageOrigin::Persisted,
                        page,
                        &self.projection_type_url,
                        cause.as_ref(),
                    )
                })?;
            }
        }

        let projection = match self.finish.as_ref() {
            Some(finish) => Some(call_handler(|| finish(&mut projection, events))?),
            None => None,
        };
        Ok(hpb::ProjectResponse { projection })
    }
}

/// Runs a projector's fold handler: its failure is the fold's cause, a key
/// and args (decision 14.37): a coded error's own, a panic's
/// ANGZARR_REASON_HANDLER_PANICKED, and none for an unclassified error, whose text stays in
/// its own log.
fn fold_handler(
    handler: impl FnOnce() -> Result<(), HandlerError>,
) -> Result<(), Option<pb::Diagnostic>> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(handler)) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(HandlerError::Coded(coded))) => Err(Some(coded.diagnostic())),
        Ok(Err(other)) => {
            map_handler_error(other);
            Err(None)
        }
        Err(_) => Err(Some(pb::Diagnostic {
            code: AngzarrReason::HandlerPanicked.as_str_name().to_string(),
            ..Default::default()
        })),
    }
}

#[cfg(test)]
#[path = "projector.test.rs"]
mod projector_tests;
