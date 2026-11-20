//! RouterBuilder / Router: one router hosts ONE component (owner ruling: one
//! coordinator, one component, one business process). Build-time validation
//! (C-0060, C-0580) and dispatch to that component, on the handler layer's
//! messages (decision 14.78).

use prost_types::Any;

use crate::domain::{DomainDispatch, Emit, FactEmit};
use crate::error::{AngzarrReason, CodedError};
use crate::hpb;
use crate::pb;
use crate::policy::{self, PolicyDispatch};
use crate::projector::ProjectorDispatch;
use crate::rebuild::Rebuilder;
use crate::router::{
    CommandHandler, DynCommandHandler, DynPolicyComponent, DynProjectorHandler, PolicyComponent,
    ProjectorHandler, Router, RouterBuilder,
};
use crate::test_support::{
    command_page, event_page, fact_page, fresh_rebuilder, now, uuid_of, LedgerState, TestState,
};
use crate::type_url;

fn any_of_type(fq: &str) -> Any {
    Any {
        type_url: type_url(fq),
        value: Vec::new(),
    }
}

/// An aggregate in `domain` handling `commands`, each emitting one event
/// tagged (type URL) with `label`.
fn aggregate(label: &'static str, domain: &str, commands: &[&str]) -> DomainDispatch<TestState> {
    let mut agg = DomainDispatch::new(label, domain, fresh_rebuilder());
    for c in commands {
        agg = agg.on_command(c, move |_, _, _| Ok(Emit::of([any_of_type(label)])));
    }
    agg
}

fn command(domain: &str, fq: &str) -> hpb::HandleRequest {
    hpb::HandleRequest {
        input: Some(pb::CommandBook {
            cover: Some(pb::Cover {
                domain: domain.to_string(),
                ..Default::default()
            }),
            page: Some(command_page(any_of_type(fq))),
        }),
        ..Default::default()
    }
}

/// A Fact-arm delivery of `fq` to `domain`.
fn fact(domain: &str, fq: &str) -> hpb::HandleRequest {
    let mut req = command(domain, fq);
    req.input.as_mut().unwrap().page = Some(fact_page(any_of_type(fq)));
    req
}

fn emitted_label(resp: &hpb::HandleResponse) -> String {
    resp.events
        .first()
        .map(|any| crate::type_name_from_url(&any.type_url).to_string())
        .unwrap_or_default()
}

fn build_err(builder: RouterBuilder) -> CodedError {
    match builder.build() {
        Ok(_) => panic!("expected the build to fail"),
        Err(err) => err,
    }
}

#[test]
fn an_empty_configuration_fails_to_build() {
    assert_eq!(
        build_err(RouterBuilder::new()).code,
        AngzarrReason::NoHandlersRegistered.as_str_name(),
        "C-0060"
    );
}

/// A builder holding one component of `kind`, named `name`.
fn with_one(builder: RouterBuilder, kind: &str, name: &'static str) -> RouterBuilder {
    match kind {
        "aggregate" => builder.aggregate(aggregate(name, "order", &["test.Create"])),
        "policy" => builder.policy(PolicyDispatch::new(name, "order", "inventory")),
        "process manager" => builder.process_manager(
            DomainDispatch::process_manager(name, "pm", fresh_rebuilder())
                .on_command("test.Start", |_, _, _| Ok(Emit::none())),
        ),
        _ => builder.projector(ProjectorDispatch::new(name, || ())),
    }
}

const KINDS: [&str; 4] = ["aggregate", "policy", "process manager", "projector"];

// C-0580: one owner, the router, and one arg set, each a component name
// (decisions 14.87 and 14.88): `registered` and `refused`.
#[test]
fn a_router_hosts_one_component_and_refuses_a_second_of_any_kind() {
    for first in KINDS {
        assert!(
            with_one(RouterBuilder::new(), first, "first")
                .build()
                .is_ok(),
            "one {first} builds"
        );
        for second in KINDS {
            let err = build_err(with_one(
                with_one(RouterBuilder::new(), first, "first"),
                second,
                "second",
            ));
            assert_eq!(err.code, "ANGZARR_REASON_DUPLICATE_COMPONENT");
            assert_eq!(err.grpc, crate::error::GrpcCode::Internal);
            let extra = |key: &str| err.extras.get(key).map(String::as_str);
            assert_eq!(
                extra(angzarr_proto::v1::reason_args::REGISTERED),
                Some("first")
            );
            assert_eq!(
                extra(angzarr_proto::v1::reason_args::REFUSED),
                Some("second")
            );
            assert_eq!(err.extras.len(), 2, "{:?}", err.extras);
        }
    }
}

#[test]
fn a_router_dispatches_commands_and_facts_to_its_aggregate() {
    let router = RouterBuilder::new()
        .aggregate(
            aggregate("created", "order", &["test.Create"]).on_fact("test.Paid", |_, _, _| {
                Ok(FactEmit::of([any_of_type("paid")]))
            }),
        )
        .build()
        .expect("build");
    let resp = now(router.dispatch_command(&command("order", "test.Create"))).expect("command");
    assert_eq!(emitted_label(&resp), "created");
    let resp = now(router.dispatch_command(&fact("order", "test.Paid"))).expect("fact");
    assert_eq!(emitted_label(&resp), "paid");
    let err = now(router.dispatch_command(&command("order", "test.Unknown"))).unwrap_err();
    assert_eq!(err.code, "ANGZARR_REASON_UNKNOWN_COMMAND_TYPE");
    crate::test_support::assert_bug(&err);
    let err = now(router.dispatch_command(&fact("order", "test.Unrecorded"))).unwrap_err();
    assert_eq!(err.code, AngzarrReason::NoFactHandler.as_str_name());
    crate::test_support::assert_bug(&err);
}

fn trigger(domain: &str) -> hpb::ReactRequest {
    hpb::ReactRequest {
        trigger: Some(pb::EventBook {
            cover: Some(pb::Cover {
                domain: domain.to_string(),
                ..Default::default()
            }),
            pages: vec![event_page(any_of_type("test.Created"))],
            ..Default::default()
        }),
    }
}

fn reserve() -> hpb::Output {
    policy::command(Some(uuid_of(1)), any_of_type("test.Reserve"))
}

#[test]
fn a_router_runs_its_policy() {
    let router = RouterBuilder::new()
        .policy(
            PolicyDispatch::new("s", "order", "inventory")
                .on_event("test.Created", |_, _| Ok(vec![reserve()])),
        )
        .build()
        .expect("build");
    assert_eq!(
        now(router.dispatch_react(&trigger("order")))
            .expect("policy")
            .outputs,
        [reserve()]
    );
    // Decision 14.42: the coordinator delivers only the input domain's
    // events, and the router guards no envelope: a trigger from another
    // domain is handled all the same.
    assert_eq!(
        now(router.dispatch_react(&trigger("billing")))
            .expect("policy")
            .outputs
            .len(),
        1
    );
}

// A process manager is a domain (decision 14.31): the router hosts it as it
// hosts an aggregate, and a command to it answers its own events only.
#[test]
fn a_router_runs_its_process_manager_as_a_domain() {
    let router = RouterBuilder::new()
        .process_manager(
            DomainDispatch::process_manager("pm", "pm", Rebuilder::<LedgerState>::of_message())
                .with_rules_version("9")
                .on_command("test.Start", |_, _, _| {
                    Ok(Emit::of([any_of_type("test.Started")]))
                }),
        )
        .build()
        .expect("build");
    let resp = now(router.dispatch_command(&command("pm", "test.Start"))).expect("pm");
    assert_eq!(emitted_label(&resp), "test.Started");
    assert_eq!(resp.rules_version, "9");
}

fn run(domain: &str) -> hpb::ProjectRequest {
    hpb::ProjectRequest {
        events: trigger(domain).trigger,
    }
}

#[test]
fn a_router_answers_its_projectors_projection() {
    let router = RouterBuilder::new()
        .projector(
            ProjectorDispatch::new("p1", || ()).finish(|_, _| Ok(any_of_type("test.Projected"))),
        )
        .build()
        .expect("build");
    let projection = now(router.dispatch_project(&run("order"))).expect("projection");
    assert_eq!(projection.projection, Some(any_of_type("test.Projected")));
}

#[test]
fn a_router_refuses_dispatch_of_another_kind() {
    let no_kind = AngzarrReason::NoComponentOfKind.as_str_name();
    let router: Router = with_one(RouterBuilder::new(), "policy", "s")
        .build()
        .expect("build");
    assert_eq!(
        now(router.dispatch_command(&command("order", "test.Create")))
            .unwrap_err()
            .code,
        no_kind
    );
    assert_eq!(
        now(router.dispatch_project(&run("order")))
            .unwrap_err()
            .code,
        no_kind
    );
    let aggregate_router = with_one(RouterBuilder::new(), "aggregate", "a")
        .build()
        .expect("build");
    let err = now(aggregate_router.dispatch_react(&trigger("order"))).unwrap_err();
    assert_eq!(err.code, no_kind);
    assert_eq!(err.grpc, crate::error::GrpcCode::Unimplemented);
    assert_eq!(
        now(aggregate_router.dispatch_translate(&hpb::TranslateRequest::default()))
            .unwrap_err()
            .code,
        no_kind
    );
}

// --- boxed and hand-written components ---------------------------------------

#[test]
fn boxed_components_build_routers() {
    let boxed: Box<dyn DynCommandHandler> = Box::new(aggregate("boxed", "order", &["test.Create"]));
    assert_eq!(
        CommandHandler::command_types(&boxed),
        vec!["test.Create".to_string()]
    );
    let router = RouterBuilder::new()
        .aggregate(boxed)
        .build()
        .expect("build");
    let resp = now(router.dispatch_command(&command("order", "test.Create"))).expect("dispatch");
    assert_eq!(emitted_label(&resp), "boxed");

    let projector: Box<dyn DynProjectorHandler> =
        Box::new(ProjectorDispatch::new("p", || ()).finish(|_, _| Ok(any_of_type("boxed"))));
    let router = RouterBuilder::new()
        .projector(projector)
        .build()
        .expect("build");
    let projection =
        now(router.dispatch_project(&hpb::ProjectRequest::default())).expect("dispatch");
    assert_eq!(projection.projection, Some(any_of_type("boxed")));
}

/// A hand-written aggregate.
struct Plain;

impl CommandHandler for Plain {
    fn name(&self) -> &str {
        "plain"
    }
    fn command_types(&self) -> Vec<String> {
        vec!["test.Plain".to_string()]
    }
    async fn dispatch(&self, _: &hpb::HandleRequest) -> Result<hpb::HandleResponse, CodedError> {
        Ok(hpb::HandleResponse::default())
    }
}

#[test]
fn a_hand_written_aggregate_builds_a_router() {
    let router = RouterBuilder::new()
        .aggregate(Plain)
        .build()
        .expect("build");
    assert_eq!(
        now(router.dispatch_command(&command("plain", "test.Plain"))).expect("dispatch"),
        hpb::HandleResponse::default()
    );
    assert_eq!(CommandHandler::kind(&Plain), pb::ComponentKind::Aggregate);
}

// A host registering components one at a time (the C ABI) refuses another
// beside the one it has, naming both (C-0580).
#[test]
fn a_router_refuses_another_component_naming_both() {
    let router = RouterBuilder::new()
        .aggregate(Plain)
        .build()
        .expect("build");
    let err = router.refuse_another_component("payment");
    assert_eq!(err.code, AngzarrReason::DuplicateComponent.as_str_name());
    assert_eq!(
        err.extras,
        [
            ("refused".to_string(), "payment".to_string()),
            ("registered".to_string(), "plain".to_string()),
        ]
        .into()
    );
}

// The object-safe twins (owner ruling 2026-10-30): every component is one,
// and a `dyn` twin is the component again, answering what it answers.
#[test]
fn a_dyn_twin_is_its_component_again() {
    let pm: Box<dyn DynCommandHandler> = Box::new(
        DomainDispatch::process_manager("pm", "pm", Rebuilder::<LedgerState>::of_message())
            .with_rules_version("3")
            .on_command("test.Start", |_, _, _| Ok(Emit::none())),
    );
    let held: &dyn DynCommandHandler = &*pm;
    assert_eq!(CommandHandler::name(held), "pm");
    assert_eq!(
        CommandHandler::kind(held),
        pb::ComponentKind::ProcessManager
    );
    assert_eq!(CommandHandler::command_types(held), ["test.Start"]);
    assert_eq!(
        now(CommandHandler::dispatch(held, &command("pm", "test.Start")))
            .expect("dispatch")
            .rules_version,
        "3"
    );
    assert_eq!(CommandHandler::name(&pm), "pm");
    assert_eq!(CommandHandler::kind(&pm), pb::ComponentKind::ProcessManager);
    assert_eq!(CommandHandler::command_types(&pm), ["test.Start"]);

    let projector: Box<dyn DynProjectorHandler> =
        Box::new(ProjectorDispatch::new("p", || ()).finish(|_, _| Ok(any_of_type("p"))));
    let held: &dyn DynProjectorHandler = &*projector;
    assert_eq!(ProjectorHandler::name(held), "p");
    assert_eq!(ProjectorHandler::name(&projector), "p");
    assert_eq!(
        now(ProjectorHandler::dispatch(held, &run("order")))
            .expect("dispatch")
            .projection,
        Some(any_of_type("p"))
    );
}

// --- the registration descriptor (types.proto ComponentDescriptor) -----------

fn ledger(version: &str) -> DomainDispatch<LedgerState> {
    DomainDispatch::new("ledger", "ledger", Rebuilder::of_message())
        .with_rules_version(version)
        .on_command("test.Ledger", |_, _, _| Ok(Emit::none()))
}

// Decision 14.42: angzarr-cli generates the descriptor at build time; the
// business process answers Describe with it unchanged, and the router builds
// and checks none. One the coordinator would refuse is answered as given,
// and a router given none answers none.
#[test]
fn describe_answers_the_descriptor_it_was_given_unchanged() {
    let given = pb::ComponentDescriptor {
        name: "ledger".to_string(),
        rules_version: String::new(),
        kind: Some(pb::component_descriptor::Kind::Policy(
            pb::PolicyDescriptor {
                input_domain: String::new(),
                output_domain: "inventory".to_string(),
            },
        )),
    };
    let router = RouterBuilder::new()
        .aggregate(ledger(""))
        .descriptor(given.clone())
        .build()
        .expect("build");
    assert_eq!(router.component_descriptor(), Some(&given));
    let undescribed = RouterBuilder::new()
        .aggregate(ledger("1"))
        .build()
        .expect("build");
    assert_eq!(undescribed.component_descriptor(), None);
}

/// A hand-written policy (a policy in another process, say): from `order`,
/// answering one output per trigger and translating every refusal into a
/// fact named for the refused command's type.
struct Relay;

impl PolicyComponent for Relay {
    fn name(&self) -> &str {
        "relay"
    }
    fn input_domain(&self) -> &str {
        "order"
    }
    async fn react(&self, _: &hpb::ReactRequest) -> Result<hpb::ReactResponse, CodedError> {
        Ok(hpb::ReactResponse {
            outputs: vec![reserve()],
        })
    }
    async fn translate(
        &self,
        req: &hpb::TranslateRequest,
    ) -> Result<hpb::TranslateResponse, CodedError> {
        let refused = match req.command.as_ref().and_then(|d| d.body.as_ref()) {
            Some(pb::reaction_delivery::Body::Command(a)) => a.type_url.clone(),
            _ => String::new(),
        };
        Ok(hpb::TranslateResponse {
            fact: Some(any_of_type(&format!("relayed{refused}"))),
        })
    }
}

/// A refusal of a `test.Reserve` command, triggered by an `order` event.
fn reserve_refused() -> hpb::TranslateRequest {
    hpb::TranslateRequest {
        trigger: trigger("order").trigger,
        command: Some(pb::ReactionDelivery {
            body: Some(pb::reaction_delivery::Body::Command(any_of_type(
                "test.Reserve",
            ))),
            ..Default::default()
        }),
        refusal: Some(hpb::Refusal::default()),
    }
}

// A policy as a runtime holds it, whatever it is (decision 14.41): a
// hand-written one, boxed, answers through the trait.
#[test]
fn a_hand_written_policy_answers_through_policy_component() {
    let boxed: Box<dyn DynPolicyComponent> = Box::new(Relay);
    assert_eq!(PolicyComponent::name(&boxed), "relay");
    assert_eq!(PolicyComponent::input_domain(&boxed), "order");
    assert_eq!(
        now(PolicyComponent::react(&boxed, &trigger("order")))
            .expect("policy")
            .outputs,
        [reserve()]
    );
    assert_eq!(
        now(PolicyComponent::translate(&boxed, &reserve_refused()))
            .expect("answered")
            .fact,
        Some(any_of_type("relayed/test.Reserve"))
    );
}

// A PolicyDispatch through the trait: its outputs, and its one refusal
// callback's payload.
#[test]
fn a_policy_dispatch_answers_through_policy_component() {
    let dispatch = PolicyDispatch::new("s", "order", "inventory")
        .on_event("test.Created", |_, _| Ok(vec![reserve()]))
        .on_refused(|_, _, _| Ok(any_of_type("test.ReserveFailed")));
    let policy: &dyn DynPolicyComponent = &dispatch;
    assert_eq!(PolicyComponent::name(policy), "s");
    assert_eq!(PolicyComponent::input_domain(policy), "order");
    assert_eq!(PolicyComponent::name(&dispatch), "s");
    assert_eq!(PolicyComponent::input_domain(&dispatch), "order");
    assert_eq!(
        now(PolicyComponent::react(policy, &trigger("order")))
            .expect("policy")
            .outputs,
        [reserve()]
    );
    assert_eq!(
        now(PolicyComponent::translate(policy, &reserve_refused()))
            .expect("translated")
            .fact,
        Some(any_of_type("test.ReserveFailed"))
    );
    let boxed: Box<dyn DynPolicyComponent> = Box::new(dispatch);
    assert_eq!(
        now(PolicyComponent::translate(&boxed, &reserve_refused()))
            .expect("translated")
            .fact,
        Some(any_of_type("test.ReserveFailed"))
    );
}
