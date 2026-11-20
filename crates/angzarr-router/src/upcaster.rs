//! UpcasterDispatch — an upcaster's rules: the client library's side of the
//! upcaster sidecar's `UpcasterService.Upcast` (decisions 14.39, 14.96 and
//! 14.97; docs/design/upcasting.md §6). An upcaster is part of the
//! component whose streams it upcasts, never a component of its own: it has
//! no kind and no descriptor, and a business process hosting it hosts
//! nothing else (ANGZARR_REASON_UPCASTER_WITH_COMPONENT is the host's).
//!
//! A rule brings one payload type to a newer shape. Each payload is
//! brought through every rule its type reaches, in turn, each rule at most
//! once (a v1 to v2 rule, then v2 to v3; a rule re-encoding its own type
//! runs once), and is answered as is when no rule applies. The answer is
//! keyed, never positional: a stream page by its sequence, a lone command
//! or fact payload by `NoSequence` (decision 14.97). A page's sequence and
//! EventMeta are the coordinator's and stay as stored, so a rule sees and
//! answers the payload alone. A rule that fails is a rule bug (decision
//! 14.37), with no key of its own.

use std::collections::{HashMap, HashSet};

use prost_types::Any;

use crate::error::{call_handler, CodedError, HandlerError};
use crate::hpb;

/// One rule: an old payload, its type named by the rule's key, to a newer
/// shape (another type, or the same type re-encoded).
pub type RuleFn = Box<dyn Fn(&Any) -> Result<Any, HandlerError> + Send + Sync>;

/// The rules of one domain's upcaster.
pub struct UpcasterDispatch {
    name: String,
    domain: String,
    rules: HashMap<String, RuleFn>,
}

impl UpcasterDispatch {
    /// An upcaster with no rules for `domain`'s streams, commands and facts.
    pub fn new(name: impl Into<String>, domain: impl Into<String>) -> Self {
        UpcasterDispatch {
            name: name.into(),
            domain: domain.into(),
            rules: HashMap::new(),
        }
    }

    /// Registers the rule for payloads of the fully-qualified type
    /// `full_name`.
    pub fn rule(
        mut self,
        full_name: &str,
        thunk: impl Fn(&Any) -> Result<Any, HandlerError> + Send + Sync + 'static,
    ) -> Self {
        self.rules.insert(full_name.to_string(), Box::new(thunk));
        self
    }

    /// The upcaster's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The domain it upcasts.
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// `payload` brought through every rule its type reaches, in turn,
    /// each rule at most once: as is when none applies.
    pub fn upcast(&self, payload: &Any) -> Result<Any, CodedError> {
        let mut current = payload.clone();
        let mut applied = HashSet::new();
        while let Some((name, rule)) = self
            .rules
            .get_key_value(crate::type_name_from_url(&current.type_url))
        {
            if !applied.insert(name.as_str()) {
                break;
            }
            current = call_handler(|| rule(&current))?;
        }
        Ok(current)
    }

    /// `UpcasterService.Upcast`: each requested payload in its current
    /// shape, keyed by its page's sequence, in the request's order, or by
    /// `NoSequence` for a lone payload (decisions 14.96 and 14.97). A page
    /// whose body is no domain event (a framework meta-event, which is never
    /// upcast) is answered keyed by its sequence with no payload, so every
    /// requested sequence has its entry. A request with no input answers
    /// none. The first failing rule fails the request.
    pub fn dispatch(&self, req: &hpb::UpcastRequest) -> Result<hpb::UpcastResponse, CodedError> {
        let pages = match &req.input {
            None => Vec::new(),
            Some(hpb::upcast_request::Input::LonePayload(payload)) => vec![hpb::Upcasted {
                key: Some(hpb::upcasted::Key::NoSequence(hpb::NoSequence {})),
                payload: Some(self.upcast(payload)?),
            }],
            Some(hpb::upcast_request::Input::Pages(pages)) => pages
                .events
                .iter()
                .map(|page| {
                    Ok(hpb::Upcasted {
                        key: Some(hpb::upcasted::Key::Sequence(page.sequence)),
                        payload: crate::page_event(page)
                            .map(|event| self.upcast(event))
                            .transpose()?,
                    })
                })
                .collect::<Result<_, CodedError>>()?,
        };
        Ok(hpb::UpcastResponse { pages })
    }
}

#[cfg(test)]
#[path = "upcaster.test.rs"]
mod upcaster_tests;
