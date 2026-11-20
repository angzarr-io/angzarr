//! Binds angzarr-project's handler-contract tier
//! (features/handler-contract/handling.feature; the spec is the source, read
//! from the submodule) to the Rust core: the order aggregate, the
//! fulfillment process manager (a domain, decision 14.31) and the
//! order-fulfillment policy, built by hand over the tier's orders.v1
//! fixture messages, on the handler layer's messages (decision 14.78).

mod common;

use angzarr_router::domain::{Emit, FactEmit, NotIdempotent};
use angzarr_router::error::{CodedError, GrpcCode, HandlerError, RetryableStatus};
use angzarr_router::hpb;
use angzarr_router::pb;
use angzarr_router::policy::{self, PolicyDispatch};
use angzarr_router::router::{CommandHandler, RouterBuilder};
use angzarr_router_conformance as conf;
use cucumber::writer::Stats as _;
use cucumber::{given, then, when, World};
use futures::FutureExt as _;
use prost::Message;
use std::sync::{Arc, Mutex};

/// The business process's registered component.
struct Registered(angzarr_router::router::Router);

impl std::fmt::Debug for Registered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Registered(Router)")
    }
}

/// How the order-fulfillment policy's one refusal callback answers.
#[derive(Debug, Clone, Default)]
enum Callback {
    /// No callback: a policy written by hand without one.
    #[default]
    None,
    /// A StockShort is translated into the "order" fact StockUnavailable;
    /// every other refusal answers a Bug (C-0691, C-0854).
    StockShortToStockUnavailable,
}

/// The step C-0853's helper checks.
#[derive(Debug, Clone, Copy)]
enum StepUnderCheck {
    /// Records OrderNoteAdded "restock apple" only while no such note is in
    /// the state.
    OnlyWhileAbsent,
    /// Records OrderNoteAdded "restock apple" whenever it runs.
    Always,
}

#[derive(Debug, Default, World)]
struct HandlingWorld {
    rules_version: String,
    pm_rules_version: String,
    behaviour: conf::OrderBehaviour,
    seen: conf::OrderSink,
    /// What a continuation step saw (C-0849).
    step_seen: conf::OrderSink,
    snapshot: Option<pb::Snapshot>,
    /// The pages of a delivery after its snapshot (C-0688).
    delivery_pages: Vec<pb::EventPage>,
    result: Option<Result<hpb::HandleResponse, CodedError>>,
    pm_result: Option<Result<hpb::HandleResponse, CodedError>>,
    pm_writes: Option<conf::Writes>,
    policy: Option<(String, String, String)>,
    /// What the policy's business code answers each OrderPlaced with.
    policy_outputs: Vec<hpb::Output>,
    react_result: Option<Result<hpb::ReactResponse, CodedError>>,
    callback: Callback,
    /// The refused commands and refusals the callback was called with.
    translated: Arc<Mutex<Vec<(prost_types::Any, hpb::Refusal)>>>,
    translate_result: Option<Result<hpb::TranslateResponse, CodedError>>,
    registered: Option<Registered>,
    second_refusal: Option<CodedError>,
    /// When the delivery says its input was accepted (C-0671).
    recorded_at: Option<prost_types::Timestamp>,
    /// A handler's panic that escaped the router, with its text.
    escaped: Option<String>,
    /// The answer to one of C-0670's conditions: `Ok` when it was not
    /// refused.
    outcome: Option<Result<(), CodedError>>,
    step_under_check: Option<StepUnderCheck>,
    /// Whether the step under check is a fact's (C-0878), not a command's.
    step_is_fact: bool,
    step_check: Option<Result<(), NotIdempotent>>,
}

impl HandlingWorld {
    fn order(&self) -> angzarr_router::domain::DomainDispatch<conf::orders::v1::OrderState> {
        conf::order_aggregate(
            conf::OrderBehaviour {
                rules_version: self.rules_version.clone(),
                ..self.behaviour.clone()
            },
            self.seen.clone(),
        )
    }

    /// Dispatches through the kernel-facing [`CommandHandler`], awaiting
    /// its answer.
    async fn dispatch(&mut self, mut req: hpb::HandleRequest) {
        if self.recorded_at.is_some() {
            req.recorded_at = self.recorded_at;
        }
        let order = self.order();
        // A handler's panic is the router's to answer (decision 14.37);
        // one that escapes is recorded, for the Then steps to fail on.
        let answer = std::panic::AssertUnwindSafe(CommandHandler::dispatch(&order, &req))
            .catch_unwind()
            .await;
        match answer {
            Ok(answer) => self.result = Some(answer),
            Err(panic) => {
                self.escaped = Some(
                    panic
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| panic.downcast_ref::<&str>().map(|t| t.to_string()))
                        .unwrap_or_default(),
                )
            }
        }
    }

    /// The error the scenario's call failed with: one of C-0670's
    /// conditions, a Translate's, or the handled input's.
    fn the_error(&self) -> &CodedError {
        if let Some(outcome) = &self.outcome {
            return outcome
                .as_ref()
                .expect_err("expected the condition refused, it was not");
        }
        if let Some(translated) = &self.translate_result {
            return translated.as_ref().expect_err("expected Translate to fail");
        }
        self.refusal()
    }

    fn accepted(&self) -> &hpb::HandleResponse {
        self.result
            .as_ref()
            .expect("an input was handled")
            .as_ref()
            .expect("expected the input accepted")
    }

    fn refusal(&self) -> &CodedError {
        if let Some(text) = &self.escaped {
            panic!(
                "the handler's panic escaped the router instead of being answered \
                 ANGZARR_REASON_UNHANDLED_HANDLER_ERROR with cause \
                 ANGZARR_REASON_HANDLER_PANICKED: {text:?}"
            );
        }
        self.result
            .as_ref()
            .expect("an input was handled")
            .as_ref()
            .expect_err("expected a refusal")
    }

    fn observed(&self) -> conf::OrderObserved {
        self.seen
            .lock()
            .unwrap()
            .last()
            .cloned()
            .expect("business code ran")
    }
}

/// The quoted strings of `text`, in order.
fn quoted(text: &str) -> Vec<String> {
    text.split('"')
        .skip(1)
        .step_by(2)
        .map(str::to_string)
        .collect()
}

/// The type name and decoded string field 1 of an orders.v1 payload.
fn order_payload(any: &prost_types::Any) -> (String, String) {
    let name = angzarr_router::type_name_from_url(&any.type_url).to_string();
    // Every orders.v1 event the scenarios name carries its text in field 1.
    let text = conf::orders::v1::ItemAdded::decode(any.value.as_slice())
        .map(|m| m.item)
        .unwrap_or_default();
    (name, text)
}

fn grpc_name(code: GrpcCode) -> &'static str {
    match code {
        GrpcCode::Unknown => "UNKNOWN",
        GrpcCode::InvalidArgument => "INVALID_ARGUMENT",
        GrpcCode::DeadlineExceeded => "DEADLINE_EXCEEDED",
        GrpcCode::ResourceExhausted => "RESOURCE_EXHAUSTED",
        GrpcCode::NotFound => "NOT_FOUND",
        GrpcCode::AlreadyExists => "ALREADY_EXISTS",
        GrpcCode::PermissionDenied => "PERMISSION_DENIED",
        GrpcCode::OutOfRange => "OUT_OF_RANGE",
        GrpcCode::Unavailable => "UNAVAILABLE",
        GrpcCode::FailedPrecondition => "FAILED_PRECONDITION",
        GrpcCode::Aborted => "ABORTED",
        GrpcCode::Unimplemented => "UNIMPLEMENTED",
        GrpcCode::Internal => "INTERNAL",
        GrpcCode::DataLoss => "DATA_LOSS",
    }
}

/// `name "value"` and bare `name` (present, any value) pairs of
/// `sequence "3" and state_type_url`.
fn expected_metadata(text: &str) -> Vec<(String, Option<String>)> {
    text.split(" and ")
        .map(|part| {
            let part = part.trim();
            match part.split_once(' ') {
                Some((name, value)) => (
                    name.to_string(),
                    Some(value.trim().trim_matches('"').to_string()),
                ),
                None => (part.to_string(), None),
            }
        })
        .collect()
}

// --- the version -------------------------------------------------------------

#[given(regex = r#"^an "order" business binary built as version "([^"]*)"$"#)]
async fn order_binary(w: &mut HandlingWorld, version: String) {
    w.rules_version = version;
}

#[given(regex = r#"^a "fulfillment" process manager built as version "([^"]*)"$"#)]
async fn fulfillment(w: &mut HandlingWorld, version: String) {
    w.pm_rules_version = version;
}

async fn pm_handle(w: &mut HandlingWorld, writes: conf::Writes) {
    w.pm_writes = Some(writes);
    let pm = conf::fulfillment_pm(&w.pm_rules_version, writes);
    w.pm_result = Some(CommandHandler::dispatch(&pm, &conf::start_fulfillment()).await);
}

#[when("the process manager handles a StartFulfillment command")]
async fn pm_handles(w: &mut HandlingWorld) {
    pm_handle(w, conf::Writes::Nothing).await;
}

#[when(
    regex = r"^the process manager handles a StartFulfillment command (and writes a FulfillmentStarted event|without writing a process event)$"
)]
async fn pm_handles_writing(w: &mut HandlingWorld, writing: String) {
    let writes = match writing.starts_with("and writes") {
        true => conf::Writes::FulfillmentStarted,
        false => conf::Writes::Nothing,
    };
    pm_handle(w, writes).await;
}

fn pm_answer(w: &HandlingWorld) -> &hpb::HandleResponse {
    w.pm_result
        .as_ref()
        .expect("the PM handled")
        .as_ref()
        .expect("the PM answered")
}

#[then(regex = r#"^the answer reports rules version "([^"]*)"$"#)]
async fn reports(w: &mut HandlingWorld, version: String) {
    let reported = match (&w.result, &w.pm_result) {
        (_, Some(_)) => pm_answer(w).rules_version.clone(),
        (Some(_), None) => w.accepted().rules_version.clone(),
        (None, None) => panic!("nothing was handled"),
    };
    assert_eq!(reported, version);
}

#[then("the answer's state is the process manager's state after the command")]
async fn pm_state(w: &mut HandlingWorld) {
    let placed = match w.pm_writes.expect("the PM handled") {
        conf::Writes::FulfillmentStarted => 1,
        conf::Writes::Nothing => 0,
    };
    assert_eq!(pm_answer(w).state, Some(conf::fulfillment_state(placed)));
}

#[then(regex = r"^the answer's events are (one FulfillmentStarted payload, nothing else|empty)$")]
async fn pm_events(w: &mut HandlingWorld, events: String) {
    let answered: Vec<String> = pm_answer(w)
        .events
        .iter()
        .map(|e| angzarr_router::type_name_from_url(&e.type_url).to_string())
        .collect();
    let expected: Vec<String> = match events.as_str() {
        "empty" => vec![],
        _ => vec!["orders.v1.FulfillmentStarted".to_string()],
    };
    assert_eq!(answered, expected);
}

#[given(
    regex = r#"^a delivered snapshot of the order state recorded under rules version "([^"]*)"$"#
)]
async fn delivered_snapshot(w: &mut HandlingWorld, version: String) {
    w.snapshot = Some(conf::order_snapshot(&["fig"], &version));
}

#[when(regex = r#"^AddItem "([^"]*)" is handled over that snapshot$"#)]
async fn over_snapshot(w: &mut HandlingWorld, item: String) {
    let snapshot = w.snapshot.clone();
    w.dispatch(conf::order_command("AddItem", &item, &[], snapshot))
        .await;
}

#[then("business code decides on the snapshot's state")]
async fn decided_on_snapshot(w: &mut HandlingWorld) {
    assert_eq!(w.observed().items, vec!["fig".to_string()]);
}

#[then("the command is accepted")]
async fn command_accepted(w: &mut HandlingWorld) {
    w.accepted();
}

// --- registration (C-0580) ----------------------------------------------------

#[given(r#"the "order" business binary has its "order" aggregate registered"#)]
async fn order_registered(w: &mut HandlingWorld) {
    // The descriptor angzarr-cli generates for it (decision 14.42).
    let descriptor = pb::ComponentDescriptor {
        name: "order".to_string(),
        rules_version: w.rules_version.clone(),
        kind: Some(pb::component_descriptor::Kind::Aggregate(
            pb::AggregateDescriptor {
                domain: "order".to_string(),
                state: Some(pb::StateDescriptor {
                    type_name: "orders.v1.OrderState".to_string(),
                }),
            },
        )),
    };
    w.registered = Some(Registered(
        RouterBuilder::new()
            .aggregate(w.order())
            .descriptor(descriptor)
            .build()
            .expect("the order aggregate"),
    ));
}

// The native builder takes a process's component at once: registering a
// second beside the order aggregate refuses the whole configuration, and the
// process keeps the one it has.
#[when(
    regex = r#"^an? (aggregate|policy|process manager|projector) component "([^"]*)" is registered in the same business process$"#
)]
async fn second_registered(w: &mut HandlingWorld, kind: String, name: String) {
    let builder = RouterBuilder::new().aggregate(w.order());
    let builder = match kind.as_str() {
        "aggregate" => builder.aggregate(angzarr_router::domain::DomainDispatch::new(
            name.clone(),
            name,
            angzarr_router::rebuild::Rebuilder::new(|| ()),
        )),
        "policy" => builder.policy(PolicyDispatch::new(name, "order", "inventory")),
        "process manager" => {
            builder.process_manager(angzarr_router::domain::DomainDispatch::process_manager(
                name.clone(),
                name,
                angzarr_router::rebuild::Rebuilder::new(|| ()),
            ))
        }
        _ => builder.projector(angzarr_router::projector::ProjectorDispatch::new(
            name,
            || (),
        )),
    };
    w.second_refusal = builder.build().err();
}

#[then(regex = r"^the registration is refused INTERNAL with reason (\w+)$")]
async fn registration_refused(w: &mut HandlingWorld, reason: String) {
    let err = w.second_refusal.as_ref().expect("refused");
    assert_eq!(err.code, reason);
    assert_eq!(err.grpc, GrpcCode::Internal);
}

async fn registered_and_refused(w: &mut HandlingWorld, registered: String, refused: String) {
    let err = match &w.second_refusal {
        Some(err) => err,
        None => w.the_error(),
    };
    assert_eq!(err.extras.get("registered"), Some(&registered));
    assert_eq!(err.extras.get("refused"), Some(&refused));
}

#[then(r#"the business process describes only the "order" aggregate"#)]
async fn describes_only_order(w: &mut HandlingWorld) {
    let described = w
        .registered
        .as_ref()
        .expect("registered")
        .0
        .component_descriptor()
        .expect("its description");
    assert_eq!(described.name, "order");
    assert!(matches!(
        described.kind,
        Some(pb::component_descriptor::Kind::Aggregate(_))
    ));
}

// --- the state on every answer (C-0570; decisions 14.12, 14.14) ---------------

#[given("business code that never mentions snapshots")]
async fn never_mentions_snapshots(_w: &mut HandlingWorld) {}

#[when(regex = r#"^(.+) is handled over ItemAdded "([^"]*)"$"#)]
async fn delivery_over(w: &mut HandlingWorld, delivery: String, prior: String) {
    let history = [("ItemAdded".to_string(), prior)];
    let req = if delivery == "a command that business code answers with no events" {
        conf::order_command("Acknowledge", "", &history, None)
    } else if let Some(fact) = delivery.strip_prefix("a fact ") {
        let name = fact.split(' ').next().unwrap();
        conf::order_fact(name, &quoted(fact)[0], &history)
    } else {
        let command = delivery.split(' ').next().unwrap().to_string();
        conf::order_command(&command, &quoted(&delivery)[0], &history, None)
    };
    w.dispatch(req).await;
}

/// The answer's state, read back.
fn answered_state(w: &HandlingWorld) -> (String, Vec<(String, u32)>, Vec<String>, String) {
    let state = w.accepted().state.as_ref().expect("the answer's state");
    conf::read_order_state(state)
}

#[then(regex = r#"^the answer carries the order state with (.+) as its state$"#)]
async fn answer_carries_state(w: &mut HandlingWorld, described: String) {
    if described == "both folded in" {
        // C-0848: ItemAdded "apple" and OrderNoteAdded "restock apple".
        let (_, items, _, notes) = answered_state(w);
        assert_eq!(items, vec![("apple".to_string(), 1)]);
        assert_eq!(notes, "restock apple");
        return;
    }
    // `items "pear" and "apple"`, `item "pear"`, `item "apple"`, or `item
    // "pear" and "apple" delivered` (the last quoted item delivered).
    let quoted = quoted(&described);
    let (items, delivered) = if described.ends_with("delivered") {
        quoted.split_at(quoted.len() - 1)
    } else {
        (quoted.as_slice(), &[][..])
    };
    let mut expected: Vec<(String, u32)> = items.iter().map(|i| (i.to_string(), 1)).collect();
    expected.sort();
    let (type_url, items, delivered_items, _) = answered_state(w);
    assert_eq!(
        (type_url, items, delivered_items),
        (
            "/orders.v1.OrderState".to_string(),
            expected,
            delivered.to_vec()
        )
    );
}

// --- inputs --------------------------------------------------------------------

#[when(regex = r#"^(\w+) "([^"]*)" is handled over no prior events$"#)]
async fn handled(w: &mut HandlingWorld, command: String, item: String) {
    w.dispatch(conf::order_command(&command, &item, &[], None))
        .await;
}

#[when(regex = r#"^a fact (\w+) "([^"]*)" is handled over no prior events$"#)]
async fn fact_handled(w: &mut HandlingWorld, fact: String, item: String) {
    w.dispatch(conf::order_fact(&fact, &item, &[])).await;
}

#[then("business code is not invoked")]
async fn not_invoked(w: &mut HandlingWorld) {
    assert!(w.seen.lock().unwrap().is_empty());
}

// --- outcomes (decision 14.95) ------------------------------------------------

/// A failure as a scenario names it, after "business code for X ".
fn failure(text: &str) -> conf::AddItemFails {
    let args = |text: &str| -> Vec<(String, String)> {
        let quoted = quoted(text);
        let name = text
            .split(" and arg ")
            .nth(1)
            .and_then(|rest| rest.split(' ').next())
            .expect("an arg name");
        vec![(name.to_string(), quoted[0].clone())]
    };
    let code = |text: &str| -> String {
        text.split("code ")
            .nth(1)
            .and_then(|rest| rest.split([' ', ',']).next())
            .expect("a code")
            .to_string()
    };
    if text.starts_with("refuses with the refusal object ItemOutOfStock") {
        let quoted = quoted(text);
        let on_hand = text.rsplit(' ').next().unwrap().parse().unwrap();
        return conf::AddItemFails::Refuses {
            item: quoted[0].clone(),
            on_hand,
        };
    }
    if text.starts_with("asks for a re-run") {
        return conf::AddItemFails::Immediate {
            code: code(text),
            args: args(text),
            logs: String::new(),
        };
    }
    if let Some(rest) = text.strip_prefix("fails with a retryable bug under ") {
        let status = match rest.split(',').next().unwrap() {
            "UNAVAILABLE" => RetryableStatus::Unavailable,
            "DEADLINE_EXCEEDED" => RetryableStatus::DeadlineExceeded,
            "RESOURCE_EXHAUSTED" => RetryableStatus::ResourceExhausted,
            other => panic!("no retryable status {other}"),
        };
        return conf::AddItemFails::BugRetryable {
            status,
            code: code(text),
            args: args(text),
            logs: String::new(),
        };
    }
    if text.starts_with("fails with code ") {
        return conf::AddItemFails::Bug {
            code: code(text),
            args: args(text),
            logs: String::new(),
        };
    }
    if text.starts_with("fails with an unclassified error") {
        let text = quoted(text).first().cloned().unwrap_or_default();
        return conf::AddItemFails::Errs { text };
    }
    if text.starts_with("panics with ") {
        return conf::AddItemFails::Panics {
            text: quoted(text)[0].clone(),
        };
    }
    panic!("no failure {text}")
}

#[given(
    regex = r#"^business code for AddItem (refuses with .*|asks for a re-run .*|fails with .*|panics with .*)$"#
)]
async fn add_item_fails(w: &mut HandlingWorld, fails: String) {
    w.behaviour.fails = Some(failure(&fails));
}

#[given(regex = r#"^business code for the fact ItemDelivered (fails with .*)$"#)]
async fn fact_fails(w: &mut HandlingWorld, fails: String) {
    w.behaviour.fact = conf::FactReturns::Fails(failure(&fails));
}

// A fact's handler fails only with a Bug (decision 14.110): a refusal object
// reaches the answer only through the explicit `Bug::from_error`, which
// `fail_as_bug` applies to a refusal.
#[given(
    regex = r#"^business code for the fact ItemDelivered fails by passing the refusal object ItemOutOfStock with item "([^"]*)" and on_hand (\d+) through Bug::from_error$"#
)]
async fn fact_fails_through_from_error(w: &mut HandlingWorld, item: String, on_hand: u32) {
    w.behaviour.fact = conf::FactReturns::Fails(conf::AddItemFails::Refuses { item, on_hand });
}

#[then(regex = r"^the call fails with status (\w+) and reason (\w+)$")]
async fn call_fails(w: &mut HandlingWorld, status: String, reason: String) {
    let err = w.the_error();
    assert_eq!(err.code, reason, "the reason key ({err:?})");
    assert_eq!(grpc_name(err.grpc), status, "the gRPC status");
}

#[then("no HandleResponse is answered")]
async fn no_handle_response(w: &mut HandlingWorld) {
    assert!(w.result.as_ref().expect("handled").is_err());
}

#[then(regex = r#"^the answer's events are one ItemAdded "([^"]*)" payload$"#)]
async fn one_item_added(w: &mut HandlingWorld, item: String) {
    let answered: Vec<_> = w.accepted().events.iter().map(order_payload).collect();
    assert_eq!(answered, vec![("orders.v1.ItemAdded".to_string(), item)]);
}

#[then("the answer carries no cover, no sequence, no command id and no event metadata")]
async fn no_coordinator_fields(w: &mut HandlingWorld) {
    // HandleResponse is the events as payloads, the state and the rules
    // version, and nothing the coordinator stamps (decision 14.78).
    let hpb::HandleResponse {
        events: _,
        state: _,
        rules_version: _,
    } = w.accepted();
}

#[then("the answer carries no refusal_type and no status details")]
async fn no_refusal_parts(w: &mut HandlingWorld) {
    let err = w.the_error();
    assert!(!err.extras.contains_key("refusal_type"), "{err:?}");
    assert!(err.refusal.is_none(), "{err:?}");
}

// --- typed refusals (decision 14.95) -------------------------------------------

#[given(
    regex = r#"^business code refuses AddItem with the refusal object ItemOutOfStock with item "([^"]*)" and on_hand (\d+)$"#
)]
async fn refuses_out_of_stock(w: &mut HandlingWorld, item: String, on_hand: u32) {
    w.behaviour.fails = Some(conf::AddItemFails::Refuses { item, on_hand });
}

#[given("AddItem declares no refuses types")]
async fn no_refuses(_w: &mut HandlingWorld) {}

// A command whose refuses names nothing cannot refuse (decision 14.95): the
// router has no untyped rejection, so business code's own key is a bug's
// cause.
#[given(
    regex = r#"^business code rejects AddItem with code (\w+) and arg (\w+) "([^"]*)", and logs "([^"]*)" for itself$"#
)]
async fn rejects_untyped(
    w: &mut HandlingWorld,
    code: String,
    name: String,
    value: String,
    logs: String,
) {
    w.behaviour.fails = Some(conf::AddItemFails::Bug {
        code,
        args: vec![(name, value)],
        logs,
    });
}

#[given(
    regex = r"^business code refuses the counter's IncreaseBy with the refusal object ValueNotPositive with n (\d+)$"
)]
async fn counter_refuses(_w: &mut HandlingWorld, _n: u32) {
    // The CounterAggregate refuses IncreaseBy 0 so (fixture/counter.proto).
}

#[when(regex = r"^IncreaseBy (\d+) is handled over no prior events$")]
async fn increase_handled(w: &mut HandlingWorld, n: u32) {
    let counter = conf::counter_aggregate(conf::ObservedSink::default());
    w.result = Some(CommandHandler::dispatch(&counter, &conf::increase_command(n)).await);
}

#[given(
    regex = r#"^business code rejects AddItem for immediate return with code (\w+) and arg (\w+) "([^"]*)", and logs "([^"]*)" for itself$"#
)]
async fn immediate(w: &mut HandlingWorld, code: String, name: String, value: String, logs: String) {
    w.behaviour.fails = Some(conf::AddItemFails::Immediate {
        code,
        args: vec![(name, value)],
        logs,
    });
}

#[then(
    regex = r"^(?:the command|it) is refused (\w+) with reason (\w+), which is also its status message$"
)]
async fn refused_with_reason(w: &mut HandlingWorld, status: String, reason: String) {
    let err = w.the_error();
    assert_eq!(err.code, reason, "the reason key ({err:?})");
    assert_eq!(grpc_name(err.grpc), status, "the gRPC status");
    assert_eq!(
        err.status_message(),
        reason,
        "the status message IS the reason key (decision 14.37)"
    );
}

#[then(regex = r"^the refusal's error info carries (.+)$")]
async fn error_info_carries(w: &mut HandlingWorld, metadata: String) {
    if metadata.starts_with("registered ") {
        return registered_and_refused(
            w,
            quoted(&metadata)[0].clone(),
            quoted(&metadata)[1].clone(),
        )
        .await;
    }
    let err = w.the_error();
    // A trailing gloss ("..., the refusal's key") is the spec's prose.
    let metadata = metadata
        .split_once(", the ")
        .map_or(metadata.as_str(), |(m, _)| m);
    match metadata {
        "no other metadata" => assert!(
            err.extras.is_empty(),
            "expected no metadata, got {:?}",
            err.extras
        ),
        "no cause" => assert!(
            !err.extras
                .keys()
                .any(|k| k == "cause" || k.starts_with("cause.")),
            "expected no cause, got {:?}",
            err.extras
        ),
        _ if metadata.starts_with("no ") => {
            let name = metadata.trim_start_matches("no ");
            assert!(
                !err.extras.contains_key(name),
                "expected no {name}, got {:?}",
                err.extras
            );
        }
        _ => {
            for (name, value) in expected_metadata(metadata) {
                let carried = err.extras.get(&name);
                match value {
                    Some(value) => assert_eq!(
                        carried,
                        Some(&value),
                        "error info {name} (all: {:?})",
                        err.extras
                    ),
                    None => assert!(
                        carried.is_some_and(|v| !v.is_empty()),
                        "error info carries {name} (all: {:?})",
                        err.extras
                    ),
                }
            }
        }
    }
}

#[then(
    regex = r#"^the refusal's status details carry the ItemOutOfStock with item "([^"]*)" and on_hand (\d+)$"#
)]
async fn details_carry_out_of_stock(w: &mut HandlingWorld, item: String, on_hand: u32) {
    let refusal = w
        .the_error()
        .refusal
        .as_deref()
        .expect("the typed refusal in the status details");
    assert_eq!(refusal.type_url, "/orders.v1.ItemOutOfStock");
    assert_eq!(
        conf::orders::v1::ItemOutOfStock::decode(refusal.value.as_slice()).expect("ItemOutOfStock"),
        conf::orders::v1::ItemOutOfStock { item, on_hand }
    );
}

#[then(regex = r"^the refusal's status details carry the ValueNotPositive with n (\d+)$")]
async fn details_carry_value(w: &mut HandlingWorld, n: u32) {
    let refusal = w
        .the_error()
        .refusal
        .as_deref()
        .expect("the typed refusal in the status details");
    assert_eq!(
        conf::counter::ValueNotPositive::decode(refusal.value.as_slice()).unwrap(),
        conf::counter::ValueNotPositive { n }
    );
}

#[then(regex = r#"^no part of the answer carries the text "([^"]*)"$"#)]
async fn no_text(w: &mut HandlingWorld, text: String) {
    let err = w.the_error();
    for (part, shown) in [
        ("status message", err.status_message().to_string()),
        ("display", err.to_string()),
        ("error", format!("{err:?}")),
    ] {
        assert!(
            !shown.contains(&text),
            "the {part} carries {text:?}: {shown}"
        );
    }
}

// --- C-0670's conditions, each driven through the router's public API ----------

fn condition(w: &mut HandlingWorld, outcome: Result<(), CodedError>) {
    w.outcome = Some(outcome);
}

fn items(n: u64) -> Vec<(String, String)> {
    (0..n)
        .map(|i| ("ItemAdded".to_string(), format!("item-{i}")))
        .collect()
}

async fn order_outcome(w: &HandlingWorld, req: hpb::HandleRequest) -> Result<(), CodedError> {
    CommandHandler::dispatch(&w.order(), &req).await.map(|_| ())
}

#[when(regex = r"^the state fold fails on a persisted event at sequence (\d+)$")]
async fn fold_fails_persisted(w: &mut HandlingWorld, sequence: u64) {
    let mut history = items(sequence);
    history.push(("ItemAdded".to_string(), conf::ROTTEN_ITEM.to_string()));
    let outcome = order_outcome(w, conf::order_command("AddItem", "apple", &history, None)).await;
    condition(w, outcome);
}

#[when(regex = r"^the state fold fails on an event the command emitted at sequence (\d+)$")]
async fn fold_fails_emitted(w: &mut HandlingWorld, sequence: u64) {
    let history = items(sequence);
    let outcome = order_outcome(
        w,
        conf::order_command("AddItem", conf::ROTTEN_ITEM, &history, None),
    )
    .await;
    condition(w, outcome);
}

fn with_payload_type(mut req: hpb::HandleRequest, type_url: &str) -> hpb::HandleRequest {
    let page = req
        .input
        .as_mut()
        .and_then(|b| b.page.as_mut())
        .expect("a page");
    let payload = match page.body.as_mut() {
        Some(pb::command_page::Body::Command(c)) => c.payload.as_mut(),
        Some(pb::command_page::Body::Fact(f)) => f.payload.as_mut(),
        None => None,
    };
    payload.expect("a payload").type_url = type_url.to_string();
    req
}

#[when(regex = r#"^a command of type "([^"]*)" arrives$"#)]
async fn command_of_type(w: &mut HandlingWorld, type_url: String) {
    let req = with_payload_type(conf::order_command("AddItem", "x", &[], None), &type_url);
    let outcome = order_outcome(w, req).await;
    condition(w, outcome);
}

#[when(regex = r#"^a fact of type "([^"]*)" arrives$"#)]
async fn fact_of_type(w: &mut HandlingWorld, type_url: String) {
    let req = with_payload_type(conf::order_fact("ItemDelivered", "x", &[]), &type_url);
    let outcome = order_outcome(w, req).await;
    condition(w, outcome);
}

#[when("a fact ItemDelivered arrives whose payload does not decode")]
async fn undecodable_fact(w: &mut HandlingWorld) {
    let mut req = conf::order_fact("ItemDelivered", "x", &[]);
    let page = req
        .input
        .as_mut()
        .and_then(|b| b.page.as_mut())
        .expect("a page");
    if let Some(pb::command_page::Body::Fact(fact)) = page.body.as_mut() {
        fact.payload.as_mut().expect("a payload").value = vec![0xff, 0xff, 0xff];
    }
    let outcome = order_outcome(w, req).await;
    condition(w, outcome);
}

/// A ReserveStock as the order-fulfillment policy's coordinator delivered
/// it: to the "inventory" stream of "inv-7", in the command arm.
fn delivered_reserve_stock() -> pb::ReactionDelivery {
    pb::ReactionDelivery {
        cover: Some(conf::cover_of("inventory", "inv-7")),
        body: Some(pb::reaction_delivery::Body::Command(conf::any_of(
            &conf::orders::v1::ReserveStock {
                item: "apple".to_string(),
            },
        ))),
        command_id: Some(conf::uuid_of("reserve-1")),
        correlation_id: Some(conf::uuid_of("order-1")),
        causation: Some(pb::EventRef {
            stream: Some(conf::cover_of("order", "order-1")),
            sequence: 3,
        }),
        component: "order-fulfillment".to_string(),
    }
}

/// Translate's request: the trigger, the delivered ReserveStock, and the
/// refusal `typed` under FAILED_PRECONDITION.
fn translate_request(typed: prost_types::Any) -> hpb::TranslateRequest {
    hpb::TranslateRequest {
        trigger: Some(conf::order_placed_trigger(3)),
        command: Some(delivered_reserve_stock()),
        refusal: Some(hpb::Refusal {
            status: pb::RefusalStatus::FailedPrecondition as i32,
            typed: Some(typed),
        }),
    }
}

fn stock_short(item: &str, on_hand: u32) -> prost_types::Any {
    conf::any_of(&conf::orders::v1::StockShort {
        item: item.to_string(),
        on_hand,
    })
}

#[when(
    "a policy written by hand with no refusal callback is given a refusal of a ReserveStock it sent"
)]
async fn no_refusal_callback(w: &mut HandlingWorld) {
    let router = RouterBuilder::new()
        .policy(PolicyDispatch::new(
            "order-fulfillment",
            "order",
            "inventory",
        ))
        .build()
        .expect("one policy");
    let outcome = router
        .dispatch_translate(&translate_request(stock_short("apple", 0)))
        .await
        .map(|_| ());
    condition(w, outcome);
}

#[when(r#"a policy is dispatched to the business process hosting the "order" aggregate"#)]
async fn policy_to_aggregate(w: &mut HandlingWorld) {
    let router = RouterBuilder::new()
        .aggregate(w.order())
        .build()
        .expect("the order aggregate");
    let outcome = router
        .dispatch_react(&hpb::ReactRequest {
            trigger: Some(conf::order_placed_trigger(0)),
        })
        .await
        .map(|_| ());
    condition(w, outcome);
}

#[when(r#"the router is built with the "order" aggregate and then a "payment" aggregate"#)]
async fn two_components(w: &mut HandlingWorld) {
    let outcome = RouterBuilder::new()
        .aggregate(w.order())
        .aggregate(angzarr_router::domain::DomainDispatch::new(
            "payment",
            "payment",
            angzarr_router::rebuild::Rebuilder::new(|| ()),
        ))
        .build()
        .map(|_| ());
    condition(w, outcome);
}

// --- Emit (decision 14.104) ----------------------------------------------------

#[given(
    regex = r#"^business code for AddItem returns Emit of ItemAdded "([^"]*)", then a step that records OrderNoteAdded "([^"]*)" when an item's quantity reaches 1 and continues nowhere$"#
)]
async fn emit_restock(w: &mut HandlingWorld, _item: String, _note: String) {
    w.behaviour.returns = conf::AddItemReturns::ThenRestockNote;
}

#[given(regex = r#"^business code for AddItem returns Emit of ItemAdded "([^"]*)", then a step$"#)]
async fn emit_observing(w: &mut HandlingWorld, _item: String) {
    w.behaviour.returns = conf::AddItemReturns::ThenObserve(w.step_seen.clone());
}

#[given(
    regex = r#"^business code for AddItem returns Emit of ItemAdded "([^"]*)", then a step that (refuses with .*|fails with .*)$"#
)]
async fn emit_failing(w: &mut HandlingWorld, _item: String, fails: String) {
    w.behaviour.returns = conf::AddItemReturns::ThenFails(failure(&fails));
}

#[given(
    regex = r#"^business code for AddItem returns Emit of ItemAdded "([^"]*)", then (\d+) steps, each recording one OrderNoteAdded and continuing to the next$"#
)]
async fn emit_chain(w: &mut HandlingWorld, _item: String, steps: u32) {
    w.behaviour.returns = conf::AddItemReturns::ThenChain(steps);
}

#[given(
    regex = r#"^business code for the fact ItemDelivered returns FactEmit of the fact's own event, then a step that records OrderNoteAdded "([^"]*)"$"#
)]
async fn fact_emit(w: &mut HandlingWorld, _note: String) {
    w.behaviour.fact = conf::FactReturns::ThenDeliveredNote;
}

/// The answer's events, as `(type name, text)` pairs.
fn answered_events(w: &HandlingWorld) -> Vec<(String, String)> {
    w.accepted().events.iter().map(order_payload).collect()
}

#[then(
    regex = r#"^the answer's events are ItemAdded "([^"]*)" and OrderNoteAdded "([^"]*)", in that order$"#
)]
async fn item_then_note(w: &mut HandlingWorld, item: String, note: String) {
    assert_eq!(
        answered_events(w),
        vec![
            ("orders.v1.ItemAdded".to_string(), item),
            ("orders.v1.OrderNoteAdded".to_string(), note),
        ]
    );
}

#[then(regex = r#"^the step saw items (.+) in its state$"#)]
async fn step_saw(w: &mut HandlingWorld, list: String) {
    let mut expected = quoted(&list);
    expected.sort();
    let seen = w
        .step_seen
        .lock()
        .unwrap()
        .last()
        .cloned()
        .expect("the step ran");
    assert_eq!(seen.items, expected);
}

#[then("no events are answered")]
async fn no_events_answered(w: &mut HandlingWorld) {
    assert!(
        w.result.as_ref().expect("handled").is_err(),
        "an error answers no events"
    );
}

#[then(
    regex = r#"^the answer's events are the fact's own event and OrderNoteAdded "([^"]*)", in that order$"#
)]
async fn fact_then_note(w: &mut HandlingWorld, note: String) {
    assert_eq!(
        answered_events(w),
        vec![
            ("orders.v1.ItemDelivered".to_string(), "apple".to_string()),
            ("orders.v1.OrderNoteAdded".to_string(), note),
        ]
    );
}

#[then(
    regex = r#"^the answer's events are ItemAdded "([^"]*)" and (\d+) OrderNoteAdded events, in order$"#
)]
async fn item_then_notes(w: &mut HandlingWorld, item: String, n: u32) {
    let mut expected = vec![("orders.v1.ItemAdded".to_string(), item)];
    expected.extend((1..=n).map(|k| ("orders.v1.OrderNoteAdded".to_string(), format!("note {k}"))));
    assert_eq!(answered_events(w), expected);
}

#[then("the call is not refused")]
async fn not_refused(w: &mut HandlingWorld) {
    w.accepted();
}

#[given(
    regex = r#"^a step that records OrderNoteAdded "([^"]*)" (only while no such note is in the state|whenever it runs)$"#
)]
async fn step_under_check(w: &mut HandlingWorld, _note: String, when: String) {
    w.step_is_fact = false;
    w.step_under_check = Some(match when.as_str() {
        "whenever it runs" => StepUnderCheck::Always,
        _ => StepUnderCheck::OnlyWhileAbsent,
    });
}

#[given(
    regex = r#"^a fact step that records OrderNoteAdded "([^"]*)" (only while no such note is in the state|whenever it runs)$"#
)]
async fn fact_step_under_check(w: &mut HandlingWorld, _note: String, when: String) {
    step_under_check(w, _note, when).await;
    w.step_is_fact = true;
}

#[when("the client library's fact step-idempotence test helper runs it")]
async fn fact_helper_runs(w: &mut HandlingWorld) {
    let state = conf::orders::v1::OrderState {
        items: [("apple".to_string(), 1)].into(),
        ..Default::default()
    };
    let ctx = angzarr_router::context::CommandContext::default();
    let order = w.order();
    w.step_check = Some(match w.step_under_check.expect("a step") {
        StepUnderCheck::OnlyWhileAbsent => {
            order.check_fact_step_idempotent(&state, &ctx, |state, _ctx| {
                if state.notes == "delivered apple" {
                    return Ok(FactEmit::none());
                }
                Ok(FactEmit::of([conf::note("delivered apple")]))
            })
        }
        StepUnderCheck::Always => order.check_fact_step_idempotent(&state, &ctx, |_state, _ctx| {
            Ok(FactEmit::of([conf::note("delivered apple")]))
        }),
    });
}

#[when("the client library's step-idempotence test helper runs it")]
async fn helper_runs(w: &mut HandlingWorld) {
    let state = conf::orders::v1::OrderState {
        items: [("apple".to_string(), 1)].into(),
        ..Default::default()
    };
    let ctx = angzarr_router::context::CommandContext::default();
    let order = w.order();
    let restock = conf::restock_step("apple".to_string());
    w.step_check = Some(match w.step_under_check.expect("a step") {
        StepUnderCheck::OnlyWhileAbsent => order.check_step_idempotent(&state, &ctx, restock),
        StepUnderCheck::Always => order.check_step_idempotent(&state, &ctx, |_state, _ctx| {
            Ok(Emit::of([conf::note("restock apple")]))
        }),
    });
}

#[then(regex = r"^the helper (passes|fails, naming the events the second run emitted)$")]
async fn helper_verdict(w: &mut HandlingWorld, verdict: String) {
    let check = w.step_check.as_ref().expect("the helper ran");
    let again = if w.step_is_fact {
        "delivered apple"
    } else {
        "restock apple"
    };
    match verdict.as_str() {
        "passes" => assert_eq!(check, &Ok(())),
        _ => assert_eq!(
            check,
            &Err(NotIdempotent::EmittedAgain(vec![conf::note(again)]))
        ),
    }
}

// --- folding what it is given (C-0688; decision 14.40) -------------------------

#[given(
    regex = r#"^a delivered snapshot at sequence (\d+) of the order state with item "([^"]*)"$"#
)]
async fn snapshot_at(w: &mut HandlingWorld, sequence: u64, item: String) {
    let mut snapshot = conf::order_snapshot(&[&item], "1.0.0");
    snapshot.sequence = sequence;
    w.snapshot = Some(snapshot);
}

#[given(regex = r"^the delivery's pages are (.+)$")]
async fn delivery_pages(w: &mut HandlingWorld, list: String) {
    // `ItemAdded "plum" at sequence 5 and ItemAdded "fig" at sequence 6`.
    w.delivery_pages = list
        .split(" and ")
        .map(|part| {
            let item = quoted(part)[0].clone();
            let sequence = part.rsplit(' ').next().unwrap().parse().unwrap();
            conf::event_page(
                sequence,
                conf::any_of(&conf::orders::v1::ItemAdded { item }),
            )
        })
        .collect();
}

#[when(regex = r#"^AddItem "([^"]*)" is handled over that delivery$"#)]
async fn over_delivery(w: &mut HandlingWorld, item: String) {
    let mut req = conf::order_command("AddItem", &item, &[], w.snapshot.clone());
    let history = req.history.as_mut().expect("a history");
    history.pages = w.delivery_pages.clone();
    history.next_sequence = w.delivery_pages.last().map_or(0, |p| p.sequence + 1);
    w.dispatch(req).await;
}

#[then(regex = r#"^business code saw items (.+) in its state$"#)]
async fn saw_items(w: &mut HandlingWorld, list: String) {
    let mut expected = quoted(&list);
    expected.sort();
    assert_eq!(w.observed().items, expected);
}

// --- policies: React and Translate (decisions 46 Q3, 14.69, 14.89) ------------

#[given(regex = r#"^an? "([^"]*)" policy translating "([^"]*)" events into "([^"]*)" inputs$"#)]
async fn policy_translating(w: &mut HandlingWorld, name: String, input: String, output: String) {
    w.policy = Some((name, input, output));
}

fn reserve_stock(root: Option<pb::Uuid>) -> hpb::Output {
    policy::command(
        root,
        conf::any_of(&conf::orders::v1::ReserveStock {
            item: "apple".to_string(),
        }),
    )
}

fn stock_hold_requested() -> hpb::Output {
    policy::fact(
        Some(conf::uuid_of("inv-7")),
        conf::any_of(&conf::orders::v1::StockHoldRequested {
            item: "apple".to_string(),
        }),
    )
}

#[given("its business code answers each OrderPlaced event with one ReserveStock command")]
async fn answers_reserve_stock(w: &mut HandlingWorld) {
    w.policy_outputs = vec![reserve_stock(Some(conf::uuid_of("inv-7")))];
}

#[given(
    "its business code answers an OrderPlaced event with a ReserveStock command that names no root"
)]
async fn answers_rootless(w: &mut HandlingWorld) {
    w.policy_outputs = vec![reserve_stock(None)];
}

#[given(
    "its business code answers an OrderPlaced event with a ReserveStock command and then a StockHoldRequested fact"
)]
async fn answers_command_then_fact(w: &mut HandlingWorld) {
    w.policy_outputs = vec![
        reserve_stock(Some(conf::uuid_of("inv-7"))),
        stock_hold_requested(),
    ];
}

async fn react(w: &mut HandlingWorld, trigger: pb::EventBook) {
    let (name, input, output) = w.policy.clone().expect("a policy");
    let outputs = w.policy_outputs.clone();
    let policy = PolicyDispatch::new(name, input, output)
        .on_event("orders.v1.OrderPlaced", move |_e, _c| Ok(outputs.clone()));
    let router = RouterBuilder::new()
        .policy(policy)
        .build()
        .expect("one policy");
    w.react_result = Some(
        router
            .dispatch_react(&hpb::ReactRequest {
                trigger: Some(trigger),
            })
            .await,
    );
}

#[when(regex = r"^React is handed a trigger book holding only OrderPlaced at sequence (\d+)$")]
async fn react_at(w: &mut HandlingWorld, sequence: u64) {
    react(w, conf::order_placed_trigger(sequence)).await;
}

#[when("React is handed a trigger book holding only that OrderPlaced event")]
async fn react_that(w: &mut HandlingWorld) {
    react(w, conf::order_placed_trigger(1)).await;
}

fn reacted(w: &HandlingWorld) -> &[hpb::Output] {
    &w.react_result
        .as_ref()
        .expect("the policy reacted")
        .as_ref()
        .expect("the policy answered")
        .outputs
}

#[then("the answer holds one output, a ReserveStock in the command arm")]
async fn one_reserve_output(w: &mut HandlingWorld) {
    let outputs = reacted(w);
    assert_eq!(outputs.len(), 1);
    assert!(matches!(
        &outputs[0].body,
        Some(hpb::output::Body::Command(any)) if any.type_url == "/orders.v1.ReserveStock"
    ));
}

#[then("no output names a trigger sequence")]
async fn no_trigger_sequence(w: &mut HandlingWorld) {
    // An Output is a root and a payload in its arm (decision 14.78); the
    // answer has no trigger sequence to group outputs by.
    let hpb::ReactResponse { outputs: _ } = w
        .react_result
        .as_ref()
        .expect("reacted")
        .as_ref()
        .expect("answered");
}

#[then("the answer's one output carries that ReserveStock in the command arm, with no root")]
async fn rootless_output(w: &mut HandlingWorld) {
    assert_eq!(reacted(w), [reserve_stock(None)]);
}

#[then("the output carries no domain, no edition, no command id and no correlation")]
async fn output_carries_nothing_else(w: &mut HandlingWorld) {
    let hpb::Output { root: _, body: _ } = &reacted(w)[0];
}

#[then("the answer is not refused")]
async fn react_not_refused(w: &mut HandlingWorld) {
    reacted(w);
}

#[then(
    "the answer's outputs are the ReserveStock in the command arm and then the StockHoldRequested in the fact arm, unchanged"
)]
async fn command_then_fact(w: &mut HandlingWorld) {
    assert_eq!(
        reacted(w),
        [
            reserve_stock(Some(conf::uuid_of("inv-7"))),
            stock_hold_requested()
        ]
    );
}

#[given(
    regex = r#"^an "([^"]*)" policy whose refusal callback translates a ReserveStock refused StockShort into the "order" fact StockUnavailable$"#
)]
async fn callback_translates(w: &mut HandlingWorld, name: String) {
    w.policy = Some((name, "order".to_string(), "inventory".to_string()));
    w.callback = Callback::StockShortToStockUnavailable;
}

#[given(
    regex = r#"^an "([^"]*)" policy whose refusal callback answers a Bug for every refusal but StockShort$"#
)]
async fn callback_bugs(w: &mut HandlingWorld, name: String) {
    w.policy = Some((name, "order".to_string(), "inventory".to_string()));
    w.callback = Callback::StockShortToStockUnavailable;
}

async fn translate(w: &mut HandlingWorld, typed: prost_types::Any) {
    let (name, input, output) = w.policy.clone().expect("a policy");
    let translated = w.translated.clone();
    let policy = match w.callback {
        Callback::None => PolicyDispatch::new(name, input, output),
        Callback::StockShortToStockUnavailable => PolicyDispatch::new(name, input, output)
            .on_refused(move |command, refusal, _ctx| {
                translated
                    .lock()
                    .unwrap()
                    .push((command.clone(), refusal.clone()));
                let typed = refusal
                    .typed
                    .as_ref()
                    .ok_or_else(|| HandlerError::Coded(CodedError::bug("NO_REFUSAL_OBJECT", [])))?;
                if angzarr_router::type_name_from_url(&typed.type_url) != "orders.v1.StockShort" {
                    // Every other arm answers a Bug (decision 14.89).
                    return Err(CodedError::bug("UNHANDLED_REFUSAL", []).into());
                }
                let short = conf::orders::v1::StockShort::decode(typed.value.as_slice())
                    .map_err(|e| HandlerError::Other(e.to_string()))?;
                Ok(conf::any_of(&conf::orders::v1::StockUnavailable {
                    item: short.item,
                }))
            }),
    };
    let router = RouterBuilder::new()
        .policy(policy)
        .build()
        .expect("one policy");
    w.translate_result = Some(router.dispatch_translate(&translate_request(typed)).await);
}

#[when(
    regex = r#"^Translate is handed the refusal StockShort with item "([^"]*)" and on_hand (\d+), under FAILED_PRECONDITION, of a ReserveStock it sent$"#
)]
async fn translate_stock_short(w: &mut HandlingWorld, item: String, on_hand: u32) {
    translate(w, stock_short(&item, on_hand)).await;
}

#[when("Translate is handed a refusal of another type of a ReserveStock it sent")]
async fn translate_other(w: &mut HandlingWorld) {
    translate(
        w,
        conf::any_of(&conf::orders::v1::ItemOutOfStock {
            item: "apple".to_string(),
            on_hand: 0,
        }),
    )
    .await;
}

#[then("the callback is called with that ReserveStock and that StockShort")]
async fn callback_called(w: &mut HandlingWorld) {
    let translated = w.translated.lock().unwrap();
    assert_eq!(translated.len(), 1, "called once");
    let (command, refusal) = &translated[0];
    let Some(pb::reaction_delivery::Body::Command(delivered)) = delivered_reserve_stock().body
    else {
        panic!("the delivery's command arm");
    };
    assert_eq!(command, &delivered);
    assert_eq!(refusal.status(), pb::RefusalStatus::FailedPrecondition);
    assert_eq!(refusal.typed, Some(stock_short("apple", 0)));
}

#[then("the answer is the fact StockUnavailable as its only content")]
async fn answer_is_stock_unavailable(w: &mut HandlingWorld) {
    let answer = w
        .translate_result
        .as_ref()
        .expect("translated")
        .as_ref()
        .expect("answered");
    assert_eq!(
        answer.fact,
        Some(conf::any_of(&conf::orders::v1::StockUnavailable {
            item: "apple".to_string()
        }))
    );
}

#[then("the answer sets no cover, no handling and no identity")]
async fn answer_sets_nothing_else(w: &mut HandlingWorld) {
    // TranslateResponse is the fact's payload alone (decision 14.78).
    let hpb::TranslateResponse { fact: _ } = w
        .translate_result
        .as_ref()
        .expect("translated")
        .as_ref()
        .expect("answered");
}

#[then("no fact is answered")]
async fn no_fact(w: &mut HandlingWorld) {
    assert!(w.translate_result.as_ref().expect("translated").is_err());
}

// --- recorded_at (C-0671; decision 14.37) ---------------------------------------

#[given(regex = r#"^the delivery says its input was accepted at "([^"]*)"$"#)]
async fn accepted_at(w: &mut HandlingWorld, at: String) {
    w.recorded_at = Some(at.parse().expect("an RFC 3339 timestamp"));
}

#[then(regex = r#"^business code read "([^"]*)" as when its input was accepted$"#)]
async fn read_accepted_at(w: &mut HandlingWorld, at: String) {
    let expected: prost_types::Timestamp = at.parse().expect("an RFC 3339 timestamp");
    assert_eq!(
        w.observed().recorded_at,
        Some(expected),
        "CommandContext.recorded_at is the delivery's HandleRequest.recorded_at"
    );
}

// The order fixtures read no clock: what business code read is the
// delivery's acceptance time, exactly.
#[then("business code read no clock of its own for it")]
async fn read_no_clock(w: &mut HandlingWorld) {
    assert_eq!(
        w.observed().recorded_at,
        w.recorded_at,
        "business code read the delivered acceptance time, not a clock"
    );
}

#[tokio::main]
async fn main() {
    let tier = HandlingWorld::cucumber()
        .fail_on_skipped()
        .run(common::tier("handling.feature"))
        .await;
    common::exit(tier.execution_has_failed());
}
