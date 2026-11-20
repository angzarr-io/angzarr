//! The business process behind a domain, as the coordinator sees it: a
//! scripted angzarr-router `CommandHandler`, registered with
//! `Registry::command_handler` as a child's would be. It keeps every
//! `HandleRequest` it is delivered and answers each as its script says: by
//! default as a well-behaved client library would, one event per input,
//! the state returned with it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use angzarr_embedded::angzarr_router::error::{BusinessStatus, CodedError, GrpcCode};
use angzarr_embedded::angzarr_router::hpb::{HandleRequest, HandleResponse};
use angzarr_embedded::angzarr_router::router::CommandHandler;
use angzarr_embedded::testing::MemoryStore;
use angzarr_embedded::v1::{command_page, event_page, ComponentKind, Cover, EventPage};
use angzarr_embedded::{Any, EventStore, StreamRef, Write};

use crate::tests::policy::bare;

/// Another write that lands while a delivery is decided: after the
/// stream was loaded, before the command is written.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Race {
    /// On the delivery's own stream, at its head.
    SameStream,
    /// On another root of the same domain.
    OtherRoot,
}

/// How one delivery is answered.
#[derive(Clone, Debug, Default)]
pub(crate) struct Answer {
    /// Refused, with this error, instead of answered.
    pub(crate) refuse: Option<CodedError>,
    pub(crate) race: Option<Race>,
    /// The rules version reported, if not the registered one.
    pub(crate) rules_version: Option<String>,
    /// The state type answered, if not the registered one.
    pub(crate) state_type: Option<String>,
    /// The types of the events recorded, if not the one named for the
    /// input.
    pub(crate) events: Option<Vec<String>>,
}

/// A typed refusal (decision 14.95): the refusal message `name` of
/// `package` (`/orders.v1.`), refused under `status`.
pub(crate) fn typed_refusal(package: &str, name: &str, status: BusinessStatus) -> CodedError {
    CodedError::refused(
        status,
        Any {
            type_url: format!("/{package}{name}"),
            value: Vec::new(),
        },
    )
}

/// The router's refusal of a command of a type no handler takes:
/// `ANGZARR_REASON_UNKNOWN_COMMAND_TYPE`, INTERNAL, naming `type_url`.
pub(crate) fn unknown_command_type(type_url: &str) -> CodedError {
    CodedError {
        code: "ANGZARR_REASON_UNKNOWN_COMMAND_TYPE".to_string(),
        extras: BTreeMap::from([("type_url".to_string(), type_url.to_string())]),
        grpc: GrpcCode::Internal,
        refusal: None,
    }
}

/// A typed refusal `name` of `package`, FAILED_PRECONDITION.
pub(crate) fn rejection(package: &str, name: &str) -> CodedError {
    typed_refusal(package, name, BusinessStatus::FailedPrecondition)
}

/// A refusal of every input of a type, or only once the stream holds an
/// event of a type.
#[derive(Clone, Debug)]
pub(crate) struct Rule {
    /// The input's type, by bare name.
    pub(crate) input: String,
    /// The event the stream must hold for the rule to apply.
    pub(crate) once: Option<String>,
    pub(crate) error: CodedError,
}

/// The script: an answer for every delivery, and for some by number.
#[derive(Default)]
struct Script {
    every: Answer,
    nth: BTreeMap<usize, Answer>,
}

/// One domain's business process.
pub(crate) struct Business {
    /// Its domain's type URL package: `/orders.v1.` for `order`.
    package: String,
    /// The version it reports: the one it was built at.
    rules_version: Mutex<String>,
    state_type: String,
    /// The stream it lands races on.
    store: MemoryStore,
    /// The fact types it has a handler for.
    facts: Mutex<BTreeSet<String>>,
    /// Whether it has a handler for every fact, as a process manager here
    /// does.
    every_fact: Mutex<bool>,
    script: Mutex<Script>,
    seen: Mutex<Vec<HandleRequest>>,
    /// The deliveries made before the scenario's first when.
    before: Mutex<usize>,
    /// The state returned with each answer, in order.
    returned: Mutex<Vec<(usize, Any)>>,
    rules: Mutex<Vec<Rule>>,
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Business {
    pub(crate) fn new(
        package: &str,
        rules_version: &str,
        state_type: &str,
        store: &MemoryStore,
    ) -> Business {
        Business {
            package: package.to_string(),
            rules_version: Mutex::new(rules_version.to_string()),
            state_type: state_type.to_string(),
            store: store.clone(),
            facts: Mutex::default(),
            every_fact: Mutex::default(),
            script: Mutex::default(),
            seen: Mutex::default(),
            before: Mutex::default(),
            returned: Mutex::default(),
            rules: Mutex::default(),
        }
    }

    /// Inputs refused as `rule` says.
    pub(crate) fn rule(&self, rule: Rule) {
        locked(&self.rules).push(rule);
    }

    /// Inputs of `rule`'s type refused as `rule` says alone: the rules
    /// they had are gone.
    pub(crate) fn replace_rule(&self, rule: Rule) {
        let mut rules = locked(&self.rules);
        rules.retain(|r| r.input != rule.input);
        rules.push(rule);
    }

    /// The error a rule refuses `delivered`'s input of type `name` with.
    fn ruled(&self, name: &str, delivered: &HandleRequest) -> Option<CodedError> {
        let held: Vec<String> = delivered.history.as_ref().map_or(Vec::new(), |e| {
            e.pages
                .iter()
                .filter_map(angzarr_embedded::angzarr_router::page_event)
                .map(bare)
                .collect()
        });
        locked(&self.rules)
            .iter()
            .find(|r| r.input == name && r.once.as_ref().is_none_or(|e| held.contains(e)))
            .map(|r| r.error.clone())
    }

    /// It is built at `version` from now on: what it reports.
    pub(crate) fn built_at(&self, version: &str) {
        *locked(&self.rules_version) = version.to_string();
    }

    /// Its handler for the fact `name` (a bare name: `ShipmentDispatched`).
    pub(crate) fn declare_fact(&self, name: &str) {
        locked(&self.facts).insert(name.to_string());
    }

    /// Its handler for every fact.
    pub(crate) fn takes_every_fact(&self) {
        *locked(&self.every_fact) = true;
    }

    /// Every delivery answered as `edit` makes `answer`.
    pub(crate) fn every(&self, edit: impl FnOnce(&mut Answer)) {
        edit(&mut locked(&self.script).every);
    }

    /// The `n`th delivery (from 1) answered as `edit` makes it, from the
    /// answer for every delivery.
    pub(crate) fn nth(&self, n: usize, edit: impl FnOnce(&mut Answer)) {
        let mut script = locked(&self.script);
        let mut answer = script
            .nth
            .get(&n)
            .cloned()
            .unwrap_or_else(|| script.every.clone());
        edit(&mut answer);
        script.nth.insert(n, answer);
    }

    /// The scenario's first when begins: what was delivered before it was
    /// the givens'.
    pub(crate) fn begin(&self) {
        *locked(&self.before) = locked(&self.seen).len();
    }

    /// Every delivery since the first when, in order.
    pub(crate) fn delivered(&self) -> Vec<HandleRequest> {
        let before = *locked(&self.before);
        locked(&self.seen)[before..].to_vec()
    }

    /// The state returned with each answer since the first when, in order.
    pub(crate) fn returned(&self) -> Vec<Any> {
        let before = *locked(&self.before);
        let returned = locked(&self.returned);
        returned
            .iter()
            .filter(|(n, _)| *n > before)
            .map(|(_, s)| s.clone())
            .collect()
    }

    /// The answer for the delivery just kept as the `n`th.
    fn answer(&self, n: usize) -> Answer {
        let script = locked(&self.script);
        script
            .nth
            .get(&n)
            .cloned()
            .unwrap_or_else(|| script.every.clone())
    }

    /// The type URL of `name` in this domain's package.
    pub(crate) fn type_url(&self, name: &str) -> String {
        format!("/{}{name}", self.package)
    }

    /// Another write on `stream`'s domain while a delivery is decided.
    async fn land(&self, stream: &Cover, race: Race) {
        let stream = match race {
            Race::SameStream => stream.clone(),
            Race::OtherRoot => Cover {
                root: Some(angzarr_embedded::v1::Uuid::from_bytes([0xee; 16])),
                ..stream.clone()
            },
        };
        let at = StreamRef::try_from(&stream).expect("a stream");
        let head = self
            .store
            .read(at, 0, None)
            .await
            .expect("read")
            .next_sequence;
        let page = EventPage {
            sequence: head,
            body: Some(event_page::Body::Event(Any {
                type_url: self.type_url("OrderNoteAdded"),
                value: Vec::new(),
            })),
            meta: None,
        };
        self.store
            .write(Write::append(at, head, &[page]))
            .await
            .expect("another write lands");
    }

    /// What `delivered`'s handler records: the events `answer` names, else
    /// one named for its input.
    fn events(&self, delivered: &HandleRequest, answer: &Answer) -> Result<Vec<Any>, CodedError> {
        let page = delivered.input.as_ref().and_then(|book| book.page.as_ref());
        let (is_fact, payload) = match page.and_then(|page| page.body.as_ref()) {
            Some(command_page::Body::Fact(fact)) => (true, fact.payload.clone()),
            Some(command_page::Body::Command(command)) => (false, command.payload.clone()),
            None => (false, None),
        };
        let input = payload.unwrap_or_default();
        let name = bare(&input);
        if let Some(error) = self.ruled(&name, delivered) {
            return Err(error);
        }
        let declared = locked(&self.facts).contains(&name);
        if !is_fact && declared {
            // Its handler for the type is a fact handler: the router finds
            // no command handler for it.
            return Err(CodedError {
                code: "ANGZARR_REASON_UNKNOWN_COMMAND_TYPE".to_string(),
                extras: BTreeMap::from([("type_url".to_string(), input.type_url.clone())]),
                grpc: GrpcCode::Internal,
                refusal: None,
            });
        }
        // A payload with no type URL is of no type a handler is for.
        let handled = !input.type_url.is_empty() && (*locked(&self.every_fact) || declared);
        if is_fact && !handled {
            return Err(CodedError {
                code: "ANGZARR_REASON_NO_FACT_HANDLER".to_string(),
                extras: BTreeMap::from([("type_url".to_string(), input.type_url.clone())]),
                grpc: GrpcCode::Internal,
                refusal: None,
            });
        }
        let names = match &answer.events {
            Some(names) => names.clone(),
            None => vec![recorded(&name).to_string()],
        };
        Ok(names
            .iter()
            .map(|name| Any {
                type_url: self.type_url(name),
                value: Vec::new(),
            })
            .collect())
    }
}

/// The event an input of `name` records: `Record<Event>` records its
/// event, `AddItem` records `ItemAdded`; a fact records itself.
pub(crate) fn recorded(name: &str) -> &str {
    if let Some(event) = name.strip_prefix("Record") {
        return event;
    }
    match name {
        "AddItem" => "ItemAdded",
        "CreateOrder" => "OrderCreated",
        "ChangeShippingAddress" => "ShippingAddressChanged",
        other => other,
    }
}

/// A [`Business`] as the router's `CommandHandler`, of its kind.
#[derive(Clone)]
pub(crate) struct Process {
    pub(crate) business: Arc<Business>,
    pub(crate) kind: ComponentKind,
}

impl CommandHandler for Process {
    fn name(&self) -> &str {
        "business"
    }

    fn kind(&self) -> ComponentKind {
        self.kind
    }

    fn command_types(&self) -> Vec<String> {
        Vec::new()
    }

    async fn dispatch(&self, delivered: &HandleRequest) -> Result<HandleResponse, CodedError> {
        let business = &self.business;
        let n = {
            let mut seen = locked(&business.seen);
            seen.push(delivered.clone());
            seen.len()
        };
        let answer = business.answer(n);
        let stream = delivered
            .history
            .as_ref()
            .and_then(|events| events.cover.clone())
            .unwrap_or_default();
        if let Some(race) = answer.race {
            business.land(&stream, race).await;
        }
        if let Some(refusal) = answer.refuse {
            return Err(refusal);
        }
        let events = business.events(delivered, &answer)?;
        let state = Any {
            type_url: answer
                .state_type
                .clone()
                .unwrap_or_else(|| business.state_type.clone()),
            value: n.to_be_bytes().to_vec(),
        };
        locked(&business.returned).push((n, state.clone()));
        Ok(HandleResponse {
            events,
            state: Some(state),
            rules_version: answer
                .rules_version
                .unwrap_or_else(|| locked(&business.rules_version).clone()),
        })
    }
}
