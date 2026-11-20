//! DomainDispatch — a domain's input → events table: an aggregate's, or a
//! process manager's, since a process manager is a domain (decision 14.31),
//! served by the same table ([`DomainDispatch::process_manager`]). There is
//! no trigger-fed process manager:
//!
//! ```compile_fail
//! use angzarr_router::process_manager::ProcessManagerDispatch;
//! ```
//!
//! One call per input, pure: [`DomainDispatch::dispatch`] runs the handler.
//! The runtime (the angzarr-embedded kernel, or core's coordinator) owns the
//! store reads and writes around it, and admits the input first.
//!
//! The router never sees a meta-event and has no edition-lifecycle outcome
//! (owner ruling, 2026-10-28): forking, refusing an input to an unforked
//! stream and deleting are the runtime's, decided before it calls the
//! router.
//!
//! `CommandHandlerService.Handle` is the one input path, for commands and
//! facts alike (decision 14.98): the body arm of the input book's page
//! picks the handler table (Command → a command handler, Fact → a fact
//! handler) and the payload's type URL picks the handler. The arm and the
//! type are checked BEFORE state is rebuilt (an unknown command must report
//! ANGZARR_REASON_UNKNOWN_COMMAND_TYPE, not whatever the rebuild would
//! surface). There is no compensation input and no pass-through refusal
//! fact: a refusal fact is an ordinary fact of the domain (decision 46 #5).
//!
//! A command handler returns an [`Emit`] (decision 14.104): events, and
//! optionally a continuation run on the state with them folded in, all of
//! it one decision. There is no step cap. A fact handler returns a
//! [`FactEmit`], the same but for its failures: its handler and steps fail
//! only with a [`Bug`] (a classified bug, never a refusal or a re-run),
//! lowered to the same `Emit` core.
//!
//! The router admits nothing (decision 14.42): deduplication, a process
//! manager's start rule and the expected-sequence check are the
//! coordinator's, done before it calls the router. No admit rules,
//!
//! ```compile_fail
//! use angzarr_router::domain::admit_rules;
//! ```
//!
//! and no conflict resolution (decision 14.92):
//!
//! ```compile_fail
//! use angzarr_router::domain::Conflict;
//! ```

use std::collections::HashMap;

use prost_types::Any;

use crate::error::{call_handler, AngzarrReason, Bug, CodedError, HandlerError};
use crate::hpb;
use crate::pb;
use crate::rebuild::Rebuilder;

pub use crate::context::{command_context, CommandContext, InputArm};

/// A continuation step: run on the state with every earlier event of the
/// decision folded in, it answers an [`Emit`] of its own.
pub type StepFn<S> = Box<dyn FnOnce(&S, &CommandContext) -> Result<Emit<S>, HandlerError>>;

/// What every command and fact handler returns (decision 14.104;
/// docs/design/command-handling.md §9): events, applied in order, and
/// optionally a continuation run on the state with them folded in, which
/// returns an `Emit` of its own, and so on.
///
/// All of it is **one decision**: a refusal or a bug from the handler or
/// any step fails it all, and what is answered is every step's events, in
/// order, as one list (the wire's `HandleResponse.events`). There is no
/// step cap: a step that always continues never terminates, a handler bug
/// with no framework guard; a step must be idempotent on its own result,
/// which [`DomainDispatch::check_step_idempotent`] checks.
pub struct Emit<S> {
    events: Vec<Any>,
    next: Option<StepFn<S>>,
}

impl<S> Emit<S> {
    /// These events, in order, and no continuation.
    pub fn of(events: impl IntoIterator<Item = Any>) -> Self {
        Emit {
            events: events.into_iter().collect(),
            next: None,
        }
    }

    /// No events and no continuation: a decision that records nothing.
    pub fn none() -> Self {
        Emit::of([])
    }

    /// Run `step` on the state once these events (and any continuation
    /// already added) are folded in. The step may itself return an `Emit`
    /// with a continuation.
    pub fn then(
        self,
        step: impl FnOnce(&S, &CommandContext) -> Result<Emit<S>, HandlerError> + 'static,
    ) -> Self
    where
        S: 'static,
    {
        self.push(Box::new(step))
    }

    /// `step` after this emit's continuation, if it has one, else as it.
    fn push(self, step: StepFn<S>) -> Self
    where
        S: 'static,
    {
        let next: StepFn<S> = match self.next {
            None => step,
            Some(first) => {
                Box::new(move |state, ctx| first(state, ctx).map(|emit| emit.push(step)))
            }
        };
        Emit {
            events: self.events,
            next: Some(next),
        }
    }

    /// This step's own events, in order.
    pub fn events(&self) -> &[Any] {
        &self.events
    }

    /// Whether a continuation follows: [`Continues::Somewhere`] when one
    /// does.
    pub fn continues(&self) -> Continues {
        match self.next {
            Some(_) => Continues::Somewhere,
            None => Continues::Nowhere,
        }
    }
}

impl<S> std::fmt::Debug for Emit<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Emit")
            .field("events", &self.events)
            .field("continues", &self.continues())
            .finish()
    }
}

/// What every fact handler returns: events and optionally a continuation,
/// as an [`Emit`], one decision all the same, but its steps fail only with a
/// [`Bug`]: a fact never refuses and never asks for a re-run (decisions
/// 14.72 and 14.98, amending 14.104 for facts). It lowers to an [`Emit`]
/// (`From<FactEmit<S>> for Emit<S>`), so a fact runs through the one
/// step-running core a command's does.
///
/// A fact step cannot fail with a refusal or any [`HandlerError`]:
///
/// ```compile_fail
/// use angzarr_router::domain::FactEmit;
/// use angzarr_router::error::{BusinessStatus, CodedError, HandlerError};
/// let _ = FactEmit::<()>::none().then(|_, _| {
///     Err(HandlerError::from(CodedError::refused(
///         BusinessStatus::FailedPrecondition,
///         prost_types::Any::default(),
///     )))
/// });
/// ```
///
/// nor with a [`CodedError`] (a refusal included):
///
/// ```compile_fail
/// use angzarr_router::domain::FactEmit;
/// use angzarr_router::error::{BusinessStatus, CodedError};
/// let _ = FactEmit::<()>::none().then(|_, _| {
///     Err(CodedError::refused(
///         BusinessStatus::FailedPrecondition,
///         prost_types::Any::default(),
///     ))
/// });
/// ```
///
/// and a step cannot return a command's [`Emit`]:
///
/// ```compile_fail
/// use angzarr_router::domain::{Emit, FactEmit};
/// let _ = FactEmit::<()>::none().then(|_, _| Ok(Emit::none()));
/// ```
pub struct FactEmit<S>(Emit<S>);

impl<S> FactEmit<S> {
    /// These events, in order, and no continuation.
    pub fn of(events: impl IntoIterator<Item = Any>) -> Self {
        FactEmit(Emit::of(events))
    }

    /// No events and no continuation.
    pub fn none() -> Self {
        FactEmit(Emit::none())
    }

    /// Run `step` on the state once these events are folded in; a step
    /// fails only with a [`Bug`].
    pub fn then(
        self,
        step: impl FnOnce(&S, &CommandContext) -> Result<FactEmit<S>, Bug> + 'static,
    ) -> Self
    where
        S: 'static,
    {
        FactEmit(self.0.then(move |state, ctx| lower(step(state, ctx))))
    }

    /// This step's own events, in order.
    pub fn events(&self) -> &[Any] {
        self.0.events()
    }

    /// Whether a continuation follows.
    pub fn continues(&self) -> Continues {
        self.0.continues()
    }
}

/// A fact step's answer as the shared step-running core takes it: its
/// [`Emit`], or its [`Bug`] as the handler error it lowers to.
fn lower<S>(answer: Result<FactEmit<S>, Bug>) -> Result<Emit<S>, HandlerError> {
    answer.map(Emit::from).map_err(HandlerError::from)
}

impl<S> From<FactEmit<S>> for Emit<S> {
    fn from(fact: FactEmit<S>) -> Self {
        fact.0
    }
}

impl<S> std::fmt::Debug for FactEmit<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("FactEmit").field(&self.0).finish()
    }
}

/// Whether an [`Emit`] continues.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Continues {
    Nowhere,
    Somewhere,
}

/// Why a step is not idempotent on its own result
/// ([`DomainDispatch::check_step_idempotent`]).
#[derive(Debug, Clone, PartialEq)]
pub enum NotIdempotent {
    /// A run of the step failed.
    Failed(CodedError),
    /// Run again on the state its own events produce, the step emitted
    /// these events.
    EmittedAgain(Vec<Any>),
    /// Run again, the step emitted nothing but continued.
    ContinuedAgain,
}

/// Handles one command (or records one fact) against the rebuilt state.
/// Generated thunks unmarshal to the typed input and call the typed business
/// method.
pub type HandlerFn<S> =
    Box<dyn Fn(&Any, &S, &CommandContext) -> Result<Emit<S>, HandlerError> + Send + Sync>;

/// The input's kind, by its book's body arm, and the handler that runs it.
enum Dispatched<'a, S> {
    Command(&'a HandlerFn<S>),
    Fact(&'a HandlerFn<S>),
}

/// The dispatch table for one domain: an aggregate or a process manager.
pub struct DomainDispatch<S> {
    name: String,
    domain: String,
    kind: pb::ComponentKind,
    rebuilder: Rebuilder<S>,
    handlers: HashMap<String, HandlerFn<S>>,
    facts: HashMap<String, HandlerFn<S>>,
    rules_version: String,
}

impl<S> DomainDispatch<S> {
    /// An empty aggregate table over a Rebuilder.
    pub fn new(
        name: impl Into<String>,
        domain: impl Into<String>,
        rebuilder: Rebuilder<S>,
    ) -> Self {
        DomainDispatch {
            name: name.into(),
            domain: domain.into(),
            kind: pb::ComponentKind::Aggregate,
            rebuilder,
            handlers: HashMap::new(),
            facts: HashMap::new(),
            rules_version: String::new(),
        }
    }

    /// An empty process-manager table over a Rebuilder: a domain (decision
    /// 14.31) whose inputs are commands and facts, whose only output is its
    /// own events, and whose stream root is the one its input's cover names
    /// (decision 14.36). Its kind is PROCESS_MANAGER; its starting types are
    /// its generated descriptor's, which the coordinator's start rule reads
    /// (decision 14.42).
    pub fn process_manager(
        name: impl Into<String>,
        domain: impl Into<String>,
        rebuilder: Rebuilder<S>,
    ) -> Self {
        DomainDispatch {
            kind: pb::ComponentKind::ProcessManager,
            ..Self::new(name, domain, rebuilder)
        }
    }

    /// AGGREGATE or PROCESS_MANAGER.
    pub fn kind(&self) -> pb::ComponentKind {
        self.kind
    }

    /// The rules version reported on every answer
    /// (`HandleResponse.rules_version`; decision 14.56): the business
    /// binary's full version string, baked in at build. Empty until
    /// declared.
    pub fn with_rules_version(mut self, rules_version: impl Into<String>) -> Self {
        self.rules_version = rules_version.into();
        self
    }

    /// Registers the handler for a fully-qualified command type name: a
    /// page whose Command arm carries it runs this handler.
    pub fn on_command(
        mut self,
        full_name: &str,
        thunk: impl Fn(&Any, &S, &CommandContext) -> Result<Emit<S>, HandlerError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.handlers.insert(full_name.to_string(), Box::new(thunk));
        self
    }

    /// Registers the fact handler for a fully-qualified fact type name: a
    /// page whose Fact arm carries it runs this handler, which records the
    /// fact (typically as its first event, possibly with events that flag
    /// it). A fact's handling never refuses: it returns a [`FactEmit`], and
    /// fails only with a [`Bug`] (decision 14.72), lowered to the one
    /// step-running core a command's [`Emit`] runs through.
    pub fn on_fact(
        mut self,
        full_name: &str,
        thunk: impl Fn(&Any, &S, &CommandContext) -> Result<FactEmit<S>, Bug> + Send + Sync + 'static,
    ) -> Self {
        self.facts.insert(
            full_name.to_string(),
            Box::new(move |any, state, ctx| lower(thunk(any, state, ctx))),
        );
        self
    }

    /// The component name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The aggregate's domain.
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// The registered fully-qualified command type names.
    pub fn command_types(&self) -> Vec<String> {
        self.handlers.keys().cloned().collect()
    }

    /// The registered fully-qualified fact type names.
    pub fn fact_types(&self) -> Vec<String> {
        self.facts.keys().cloned().collect()
    }

    /// The handler the input's arm and payload type pick, with the
    /// payload: a Command runs its command handler (none:
    /// ANGZARR_REASON_UNKNOWN_COMMAND_TYPE), a Fact its fact handler (none:
    /// ANGZARR_REASON_NO_FACT_HANDLER, so a fact never reaches a command
    /// handler or the reverse). A refusal names the type URL it was given:
    /// the coordinator upcast it, and relays the type as sent beside it
    /// (decision 14.39). A page with no arm or payload is a command of no
    /// type: the router guards no envelope (decision 14.42).
    fn route<'a>(
        &'a self,
        page: Option<&'a pb::CommandPage>,
        untyped: &'a Any,
    ) -> Result<(Dispatched<'a, S>, &'a Any), CodedError> {
        if let Some(pb::command_page::Body::Fact(fact)) = page.and_then(|p| p.body.as_ref()) {
            let any = fact.payload.as_ref().unwrap_or(untyped);
            return match self.facts.get(crate::type_name_from_url(&any.type_url)) {
                Some(thunk) => Ok((Dispatched::Fact(thunk), any)),
                None => Err(CodedError::unknown_type(
                    AngzarrReason::NoFactHandler,
                    &any.type_url,
                )),
            };
        }
        let any = page.and_then(crate::command_payload).unwrap_or(untyped);
        match self.handlers.get(crate::type_name_from_url(&any.type_url)) {
            Some(thunk) => Ok((Dispatched::Command(thunk), any)),
            None => Err(CodedError::unknown_type(
                AngzarrReason::UnknownCommandType,
                &any.type_url,
            )),
        }
    }

    /// `CommandHandlerService.Handle` (decision 14.98): routes the input
    /// ([`Self::route`]) before rebuild, rebuilds the state from the
    /// history (a corrupt persisted event fails the input), runs the
    /// handler's [`Emit`] to its end ([`Self::decide`]), and answers success
    /// only (decision 14.95): the events, payloads only, in order (the
    /// coordinator wraps and stamps them); the state after them, on every
    /// answer, when the state has a codec (decision 14.12); and the rules
    /// version. Every other outcome is the error: a command's typed
    /// refusal, a re-run (ABORTED) or a bug; for a fact, a bug only.
    pub fn dispatch(&self, req: &hpb::HandleRequest) -> Result<hpb::HandleResponse, CodedError> {
        let page = req.input.as_ref().and_then(|b| b.page.as_ref());
        let untyped = Any::default();
        let (dispatched, any) = self.route(page, &untyped)?;
        let (mut state, _) = self.rebuilder.rebuild(req.history.as_ref())?;
        let ctx = command_context(req);
        let cover = req
            .history
            .as_ref()
            .and_then(|b| b.cover.as_ref())
            .or(ctx.cover.as_ref());
        let events = match dispatched {
            Dispatched::Command(thunk) => self.decide(thunk, any, &mut state, &ctx, cover),
            Dispatched::Fact(thunk) => self
                .decide(thunk, any, &mut state, &ctx, cover)
                .map_err(CodedError::as_fact_failure),
        }?;
        Ok(hpb::HandleResponse {
            events,
            state: self.rebuilder.encode_state(&state),
            rules_version: self.rules_version.clone(),
        })
    }

    /// Runs `thunk` on `any` and its [`Emit`] to its end, as one decision:
    /// each step's events are folded into `state`, seen as they will be
    /// persisted (at their sequences on from the history's head, with the
    /// meta the coordinator will stamp), before the next step runs on it.
    /// Answers every step's events, in order, leaving `state` at the head
    /// with all of them folded in.
    fn decide(
        &self,
        thunk: &HandlerFn<S>,
        any: &Any,
        state: &mut S,
        ctx: &CommandContext,
        cover: Option<&pb::Cover>,
    ) -> Result<Vec<Any>, CodedError> {
        let meta = ctx.event_meta(&self.rules_version);
        let mut answered = Vec::new();
        let mut emit = call_handler(|| thunk(any, state, ctx))?;
        loop {
            let at = ctx.next_sequence + answered.len() as u64;
            self.rebuilder
                .apply_emitted(state, &emit.events, cover, at, &meta)?;
            answered.extend(emit.events);
            let Some(step) = emit.next else {
                return Ok(answered);
            };
            emit = call_handler(|| step(state, ctx))?;
        }
    }

    /// [`Self::check_step_idempotent`] for a fact's step.
    pub fn check_fact_step_idempotent(
        &self,
        state: &S,
        ctx: &CommandContext,
        step: impl Fn(&S, &CommandContext) -> Result<FactEmit<S>, Bug>,
    ) -> Result<(), NotIdempotent>
    where
        S: Clone,
    {
        self.check_step_idempotent(state, ctx, |state, ctx| lower(step(state, ctx)))
    }

    /// The client library's step-idempotence test helper (decision 14.104):
    /// runs `step` on `state`, folds its own events into a copy, and runs it
    /// again there. Idempotent when the second run emits nothing and
    /// continues nowhere; otherwise the events the second run emitted
    /// ([`NotIdempotent::EmittedAgain`]), or that it continued
    /// ([`NotIdempotent::ContinuedAgain`]), or a run's failure.
    pub fn check_step_idempotent(
        &self,
        state: &S,
        ctx: &CommandContext,
        step: impl Fn(&S, &CommandContext) -> Result<Emit<S>, HandlerError>,
    ) -> Result<(), NotIdempotent>
    where
        S: Clone,
    {
        let first = call_handler(|| step(state, ctx)).map_err(NotIdempotent::Failed)?;
        let mut after = state.clone();
        let meta = ctx.event_meta(&self.rules_version);
        self.rebuilder
            .apply_emitted(
                &mut after,
                &first.events,
                ctx.cover.as_ref(),
                ctx.next_sequence,
                &meta,
            )
            .map_err(NotIdempotent::Failed)?;
        let again = call_handler(|| step(&after, ctx)).map_err(NotIdempotent::Failed)?;
        match (again.events.is_empty(), again.continues()) {
            (false, _) => Err(NotIdempotent::EmittedAgain(again.events)),
            (true, Continues::Somewhere) => Err(NotIdempotent::ContinuedAgain),
            (true, Continues::Nowhere) => Ok(()),
        }
    }
}

#[cfg(test)]
#[path = "domain.test.rs"]
mod domain_tests;
