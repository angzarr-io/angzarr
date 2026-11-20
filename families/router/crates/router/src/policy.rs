//! PolicyDispatch — a stateless policy: React to one source event,
//! Translate one refusal of a command it sent (decision 14.78;
//! docs/design/policies.md §6).
//!
//! A policy holds NO state and rebuilds nothing. React is handed ONE
//! trigger event (decision 46, Q3: a policy is a function of one event and
//! is called once per trigger): a declared event type answers its outputs,
//! an undeclared one nothing (C-0051, C-0710). An output is the destination
//! stream's root and the payload in the arm of its kind, a command or a
//! fact (decisions 14.78 and 14.91): everything else (its domain, edition,
//! identity, correlation and causation) is the coordinator's, which also
//! checks every output (decision 14.42). The router passes the outputs
//! through as business code returned them, in order.
//!
//! Refusals take ONE path (decision 46 #5) through ONE callback per policy
//! (decision 14.89, [`PolicyDispatch::on_refused`]): it takes the refused
//! command and its target's typed refusal, and answers a domestic fact's
//! payload, or a bug for a refusal it does not handle. There is no
//! pass-through and no policy that declines; a policy written by hand with
//! no callback answers ANGZARR_REASON_NO_REFUSAL_HANDLER.

use std::collections::HashMap;

use prost_types::Any;

use crate::error::{call_handler, AngzarrReason, CodedError, HandlerError};
use crate::hpb;
use crate::pb;
use crate::PageContext;
use angzarr_proto::v1::reason_args;

/// Reacts to one source event. Generated thunks unmarshal to the typed
/// event and call the typed business method; the event's place and
/// metadata arrive as a [`PageContext`]. It answers the outputs, built with
/// [`command`] and [`fact`].
pub type EventFn = Box<
    dyn for<'a> Fn(&Any, &PageContext<'a>) -> Result<Vec<hpb::Output>, HandlerError> + Send + Sync,
>;

/// A command for the policy's destination stream rooted at `root`: an
/// Output in the command arm (decision 14.91), what generated code builds
/// for a type its destination declares `(command)`.
pub fn command(root: Option<pb::Uuid>, payload: Any) -> hpb::Output {
    hpb::Output {
        root,
        body: Some(hpb::output::Body::Command(payload)),
    }
}

/// A fact for the policy's destination stream rooted at `root`: an Output
/// in the fact arm (decision 14.91), what generated code builds for a type
/// its destination declares `(fact)`.
pub fn fact(root: Option<pb::Uuid>, payload: Any) -> hpb::Output {
    hpb::Output {
        root,
        body: Some(hpb::output::Body::Fact(payload)),
    }
}

/// What the policy's one refusal callback is handed beside the refused
/// command and its refusal: the full context, read-only (decision 46, Q1).
#[derive(Debug, Clone, Copy)]
pub struct RefusalContext<'a> {
    /// The triggering event the policy reacted to: its cover, sequence and
    /// metadata (the refused command's causation).
    pub trigger: PageContext<'a>,
    /// The refused command exactly as it was delivered (decision 14.79):
    /// its cover (the destination and root) and the identity the
    /// coordinator stamped.
    pub delivery: &'a pb::ReactionDelivery,
}

/// The policy's ONE refusal callback (decision 14.89): the refused
/// command's payload (its target's command choice), the target's typed
/// refusal with the status it travelled as, and the context; answers a fact
/// of the policy's own source domain, packed, or a bug for a refusal it does
/// not handle.
pub type RefusalFn = Box<
    dyn for<'a> Fn(&Any, &hpb::Refusal, &RefusalContext<'a>) -> Result<Any, HandlerError>
        + Send
        + Sync,
>;

/// The dispatch table for one policy component.
pub struct PolicyDispatch {
    name: String,
    input_domain: String,
    output_domain: String,
    handlers: HashMap<String, EventFn>,
    refused: Option<RefusalFn>,
}

impl PolicyDispatch {
    /// An empty policy table reacting to events of its one source domain,
    /// `input_domain`, with commands and facts for its one destination
    /// domain, `output_domain` (decision 14.24).
    pub fn new(
        name: impl Into<String>,
        input_domain: impl Into<String>,
        output_domain: impl Into<String>,
    ) -> Self {
        PolicyDispatch {
            name: name.into(),
            input_domain: input_domain.into(),
            output_domain: output_domain.into(),
            handlers: HashMap::new(),
            refused: None,
        }
    }

    /// Registers the reaction to a fully-qualified event type; it receives
    /// the event's [`PageContext`]: the source book's cover, the event's
    /// sequence and its metadata.
    pub fn on_event(
        mut self,
        full_name: &str,
        thunk: impl for<'a> Fn(&Any, &PageContext<'a>) -> Result<Vec<hpb::Output>, HandlerError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.handlers.insert(full_name.to_string(), Box::new(thunk));
        self
    }

    /// Registers the policy's ONE refusal callback (decision 14.89),
    /// replacing any earlier one: every refusal of a command the policy
    /// sent reaches it.
    pub fn on_refused(
        mut self,
        thunk: impl for<'a> Fn(&Any, &hpb::Refusal, &RefusalContext<'a>) -> Result<Any, HandlerError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.refused = Some(Box::new(thunk));
        self
    }

    /// The component name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The domain whose events this policy consumes.
    pub fn input_domain(&self) -> &str {
        &self.input_domain
    }

    /// The one domain its commands and facts go to: the descriptor's
    /// output domain, which the coordinator addresses them to and checks
    /// them against (decision 14.42).
    pub fn output_domain(&self) -> &str {
        &self.output_domain
    }

    /// The registered fully-qualified event type names.
    pub fn event_types(&self) -> Vec<String> {
        self.handlers.keys().cloned().collect()
    }

    /// `PolicyService.React`: the outputs for the trigger book's one event
    /// (decision 46, Q3), as business code returned them, commands and facts
    /// in one index space and in order (decision 14.69); none for an
    /// undeclared event type, or a book with no page. The router guards no
    /// envelope (decision 14.42): the coordinator hands one page.
    pub fn react(&self, req: &hpb::ReactRequest) -> Result<hpb::ReactResponse, CodedError> {
        let Some(trigger) = req.trigger.as_ref() else {
            return Ok(hpb::ReactResponse::default());
        };
        let Some((page, event)) = trigger
            .pages
            .first()
            .and_then(|page| crate::page_event(page).map(|event| (page, event)))
        else {
            return Ok(hpb::ReactResponse::default());
        };
        let Some(thunk) = self
            .handlers
            .get(crate::type_name_from_url(&event.type_url))
        else {
            return Ok(hpb::ReactResponse::default()); // a policy reacts to its declared types only (C-0051)
        };
        let context = PageContext {
            cover: trigger.cover.as_ref(),
            sequence: page.sequence,
            meta: page.meta.as_ref(),
        };
        let outputs = call_handler(|| thunk(event, &context))?;
        Ok(hpb::ReactResponse { outputs })
    }

    /// `PolicyService.Translate`: the policy's one refusal callback run on
    /// the refused command's payload and the target's refusal, its answer
    /// the domestic fact's payload alone (C-0691): the coordinator
    /// addresses, identifies and delivers it. A failing callback, or one
    /// answering a bug for a refusal it does not handle, is a policy bug
    /// (dead-lettered and alerted; C-0854). With no callback (a policy
    /// written by hand), ANGZARR_REASON_NO_REFUSAL_HANDLER naming the
    /// refused command's type URL (C-0670).
    pub fn translate(
        &self,
        req: &hpb::TranslateRequest,
    ) -> Result<hpb::TranslateResponse, CodedError> {
        let untyped = Any::default();
        let delivered = pb::ReactionDelivery::default();
        let delivery = req.command.as_ref().unwrap_or(&delivered);
        let command = match &delivery.body {
            Some(
                pb::reaction_delivery::Body::Command(any) | pb::reaction_delivery::Body::Fact(any),
            ) => any,
            None => &untyped,
        };
        let Some(refused) = self.refused.as_ref() else {
            return Err(CodedError::keyed(
                AngzarrReason::NoRefusalHandler,
                [(
                    reason_args::COMMAND_TYPE.to_string(),
                    command.type_url.clone(),
                )],
            ));
        };
        let unrefused = hpb::Refusal::default();
        let refusal = req.refusal.as_ref().unwrap_or(&unrefused);
        let trigger = req.trigger.as_ref();
        let page = trigger.and_then(|t| t.pages.first());
        let context = RefusalContext {
            trigger: PageContext {
                cover: trigger.and_then(|t| t.cover.as_ref()),
                sequence: page.map_or(0, |p| p.sequence),
                meta: page.and_then(|p| p.meta.as_ref()),
            },
            delivery,
        };
        let fact = call_handler(|| refused(command, refusal, &context))?;
        Ok(hpb::TranslateResponse { fact: Some(fact) })
    }
}

#[cfg(test)]
#[path = "policy.test.rs"]
mod policy_tests;

#[cfg(test)]
#[path = "refusal.test.rs"]
mod refusal_tests;
