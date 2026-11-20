//! A policy's business process, as its coordinator sees it: a scripted
//! angzarr-router `PolicyComponent`, registered with
//! `Registry::policy_component` as a child's would be. It answers each
//! trigger with the outputs its reactions name, as business code returns
//! them (a root and a payload in the arm of its kind, decision 14.78), and
//! answers a refusal with the fact its Translate callback makes (decision
//! 14.89), or, with no callback, ANGZARR_REASON_NO_REFUSAL_HANDLER.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use angzarr_embedded::angzarr_router::error::{AngzarrReason, CodedError, GrpcCode};
use angzarr_embedded::angzarr_router::hpb::{
    self, ReactRequest, ReactResponse, TranslateRequest, TranslateResponse,
};
use angzarr_embedded::angzarr_router::router::PolicyComponent;
use angzarr_embedded::v1::{reaction_delivery, EventBook};
use angzarr_embedded::{type_name, Any};

use crate::tests::world::{package, root as root_of};

/// Where a reaction's command is addressed.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Root {
    /// The root a scenario names.
    Named(String),
    /// The trigger's correlation, as a policy to a process manager may.
    TriggerCorrelation,
    /// Another root than the trigger's, a new one for each trigger: its
    /// root's text with `+`, named. A policy never addresses its trigger's
    /// own stream (C-0784).
    Another,
    /// None at all: a policy bug.
    Unnamed,
}

/// An output's arm (decisions 14.78 and 14.91): a command, which its
/// destination may refuse, or a fact, which it records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Arm {
    Command,
    Fact,
}

/// One reaction: on an event of type `on`, an output of `arm` of type
/// `command` for the policy's destination, at `root`. `domain` names the
/// destination's package the type is in.
#[derive(Clone, Debug)]
pub(crate) struct Reaction {
    pub(crate) on: String,
    pub(crate) domain: String,
    pub(crate) command: String,
    pub(crate) arm: Arm,
    pub(crate) root: Root,
}

/// What the policy's Translate callback answers.
#[derive(Clone, Debug)]
pub(crate) enum Translation {
    /// The fact of this type in the source domain, payload only.
    Fact(String),
    /// A fact with no payload (C-0834).
    Empty,
    /// A failure.
    Fails(CodedError),
}

/// A policy's script, and what it was handed.
#[derive(Default)]
pub(crate) struct Script {
    pub(crate) reactions: Vec<Reaction>,
    /// Its Translate callback; none: a policy with no callback.
    pub(crate) translation: Option<Translation>,
    /// Every handled source failing so.
    pub(crate) fails: Option<CodedError>,
    /// Every trigger it was handed, one event each.
    pub(crate) sources: Vec<EventBook>,
    /// Every refusal it was handed.
    pub(crate) refusals: Vec<TranslateRequest>,
}

/// One policy.
pub(crate) struct Policy {
    pub(crate) name: String,
    pub(crate) input: String,
    pub(crate) script: Arc<Mutex<Script>>,
}

pub(crate) fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The bare type name of a payload: `OrderPlaced` for
/// `/orders.v1.OrderPlaced`.
pub(crate) fn bare(any: &Any) -> String {
    type_name(&any.type_url)
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .to_string()
}

/// `name`'s payload in `domain`'s package, empty.
pub(crate) fn payload(domain: &str, name: &str) -> Any {
    Any {
        type_url: format!("/{}{name}", package(domain)),
        value: Vec::new(),
    }
}

impl Reaction {
    /// The output business code returns for `page` of `source`.
    fn output(&self, source: &EventBook, page: &angzarr_embedded::v1::EventPage) -> hpb::Output {
        let meta = page.meta.clone().unwrap_or_default();
        let root = match &self.root {
            Root::Named(name) => Some(root_of(name)),
            Root::TriggerCorrelation => meta.correlation_id.clone(),
            Root::Another => (source.cover.as_ref())
                .and_then(|c| c.root.as_ref())
                .map(|trigger| root_of(&format!("{trigger}+"))),
            Root::Unnamed => None,
        };
        let payload = payload(&self.domain, &self.command);
        hpb::Output {
            root,
            body: Some(match self.arm {
                Arm::Fact => hpb::output::Body::Fact(payload),
                Arm::Command => hpb::output::Body::Command(payload),
            }),
        }
    }
}

impl PolicyComponent for Policy {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_domain(&self) -> &str {
        &self.input
    }

    async fn react(&self, request: &ReactRequest) -> Result<ReactResponse, CodedError> {
        let mut script = locked(&self.script);
        let source = request.trigger.clone().unwrap_or_default();
        script.sources.push(source.clone());
        if let Some(fails) = &script.fails {
            return Err(fails.clone());
        }
        let outputs = source
            .pages
            .iter()
            .filter_map(|page| {
                let name = bare(angzarr_embedded::angzarr_router::page_event(page)?);
                let outputs: Vec<hpb::Output> = (script.reactions.iter())
                    .filter(|r| r.on == name)
                    .map(|r| r.output(&source, page))
                    .collect();
                Some(outputs)
            })
            .flatten()
            .collect();
        Ok(ReactResponse { outputs })
    }

    async fn translate(&self, request: &TranslateRequest) -> Result<TranslateResponse, CodedError> {
        let mut script = locked(&self.script);
        script.refusals.push(request.clone());
        match &script.translation {
            None => {
                let command_type = match request.command.as_ref().and_then(|c| c.body.as_ref()) {
                    Some(
                        reaction_delivery::Body::Command(any) | reaction_delivery::Body::Fact(any),
                    ) => any.type_url.clone(),
                    None => String::new(),
                };
                Err(angzarr_embedded::spec_coded(
                    AngzarrReason::NoRefusalHandler,
                    [("command_type".to_string(), command_type)],
                ))
            }
            Some(Translation::Fact(name)) => Ok(TranslateResponse {
                fact: Some(payload(&self.input, name)),
            }),
            Some(Translation::Empty) => Ok(TranslateResponse {
                fact: Some(Any::default()),
            }),
            Some(Translation::Fails(error)) => Err(error.clone()),
        }
    }
}

/// A policy bug of `code`, INTERNAL.
pub(crate) fn internal(code: &str) -> CodedError {
    CodedError {
        code: code.to_string(),
        extras: BTreeMap::new(),
        grpc: GrpcCode::Internal,
        refusal: None,
    }
}
