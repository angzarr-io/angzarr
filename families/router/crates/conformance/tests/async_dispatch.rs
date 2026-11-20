//! Kernel-facing dispatch is async (owner ruling 2026-10-30); business code
//! stays sync.
//!
//! The coordinator awaits [`CommandHandler::dispatch`],
//! [`PolicyComponent::react`] and [`PolicyComponent::translate`], and
//! [`ProjectorHandler::dispatch`]. The router's own components
//! ([`DomainDispatch`], [`PolicyDispatch`], [`ProjectorDispatch`]) wrap the
//! business code's synchronous functions in a ready future: they answer on
//! the first poll, exactly what their synchronous dispatch answers. A
//! hand-written component (a remote one, in the coordinator) awaits real
//! I/O: the router's dispatch is pending until it answers. Every future is
//! `Send` off wasm32, so a server spawns dispatch on a multi-threaded
//! runtime.

#![cfg(not(target_arch = "wasm32"))]

use std::future::Future;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;

use angzarr_router::domain::DomainDispatch;
use angzarr_router::error::CodedError;
use angzarr_router::hpb;
use angzarr_router::pb;
use angzarr_router::policy::PolicyDispatch;
use angzarr_router::projector::ProjectorDispatch;
use angzarr_router::router::{
    CommandHandler, DynCommandHandler, DynPolicyComponent, DynProjectorHandler, PolicyComponent,
    ProjectorHandler, RouterBuilder,
};
use angzarr_router_conformance as conf;
use futures::channel::oneshot;
use futures::poll;

/// The answer a remote component is waiting on: taken by its one call.
type Pending<T> = Mutex<Option<oneshot::Receiver<T>>>;

fn waiting<T>() -> (oneshot::Sender<T>, Pending<T>) {
    let (answer, answered) = oneshot::channel();
    (answer, Mutex::new(Some(answered)))
}

async fn answer_of<T>(pending: &Pending<T>) -> T {
    let answered = pending.lock().unwrap().take().expect("one call");
    answered.await.expect("the test answers")
}

/// A domain behind I/O: its answer arrives when the test sends it.
struct RemoteDomain(Pending<hpb::HandleResponse>);

impl CommandHandler for RemoteDomain {
    fn name(&self) -> &str {
        "remote"
    }
    fn command_types(&self) -> Vec<String> {
        vec!["test.counter.IncreaseBy".to_string()]
    }
    async fn dispatch(&self, _req: &hpb::HandleRequest) -> Result<hpb::HandleResponse, CodedError> {
        Ok(answer_of(&self.0).await)
    }
}

/// A policy behind I/O.
struct RemotePolicy {
    reactions: Pending<hpb::ReactResponse>,
    translation: Pending<hpb::TranslateResponse>,
}

impl PolicyComponent for RemotePolicy {
    fn name(&self) -> &str {
        "remote-policy"
    }
    fn input_domain(&self) -> &str {
        "order"
    }
    async fn react(&self, _req: &hpb::ReactRequest) -> Result<hpb::ReactResponse, CodedError> {
        Ok(answer_of(&self.reactions).await)
    }
    async fn translate(
        &self,
        _req: &hpb::TranslateRequest,
    ) -> Result<hpb::TranslateResponse, CodedError> {
        Ok(answer_of(&self.translation).await)
    }
}

/// A projector behind I/O.
struct RemoteProjector(Pending<hpb::ProjectResponse>);

impl ProjectorHandler for RemoteProjector {
    fn name(&self) -> &str {
        "remote-projector"
    }
    async fn dispatch(
        &self,
        _req: &hpb::ProjectRequest,
    ) -> Result<hpb::ProjectResponse, CodedError> {
        Ok(answer_of(&self.0).await)
    }
}

fn answered_with(label: &str) -> hpb::HandleResponse {
    hpb::HandleResponse {
        rules_version: label.to_string(),
        ..Default::default()
    }
}

fn projected(label: &str) -> hpb::ProjectResponse {
    hpb::ProjectResponse {
        projection: Some(prost_types::Any {
            type_url: angzarr_router::type_url(label),
            value: vec![],
        }),
    }
}

fn reacted() -> hpb::ReactResponse {
    hpb::ReactResponse {
        outputs: vec![angzarr_router::policy::command(
            Some(conf::uuid_of("inv-7")),
            conf::any_of(&conf::orders::v1::ReserveStock {
                item: "apple".to_string(),
            }),
        )],
    }
}

fn stock_unreserved() -> prost_types::Any {
    prost_types::Any {
        type_url: angzarr_router::type_url("orders.v1.StockUnreserved"),
        value: vec![],
    }
}

fn trigger() -> hpb::ReactRequest {
    hpb::ReactRequest {
        trigger: Some(conf::order_placed_trigger(1)),
    }
}

// The race this is about: the router's dispatch must not answer before the
// component does. The test holds the answer back, polls (pending), sends
// it, polls again (ready, with that answer): the interleaving is forced,
// not hoped for.
#[tokio::test]
async fn a_router_awaits_a_domain_that_answers_later() {
    let (answer, pending) = waiting();
    let router = RouterBuilder::new()
        .aggregate(RemoteDomain(pending))
        .build()
        .expect("one domain");
    let req = conf::increase_command(1);
    let mut dispatched = pin!(router.dispatch_command(&req));
    assert!(poll!(dispatched.as_mut()).is_pending());
    answer.send(answered_with("later")).unwrap();
    assert_eq!(
        poll!(dispatched.as_mut()),
        Poll::Ready(Ok(answered_with("later")))
    );
}

#[tokio::test]
async fn a_router_awaits_a_projector_that_answers_later() {
    let (answer, pending) = waiting();
    let router = RouterBuilder::new()
        .projector(RemoteProjector(pending))
        .build()
        .expect("one projector");
    let run = conf::delivery("counter", 1);
    let mut dispatched = pin!(router.dispatch_project(&run));
    assert!(poll!(dispatched.as_mut()).is_pending());
    answer.send(projected("later")).unwrap();
    assert_eq!(
        poll!(dispatched.as_mut()),
        Poll::Ready(Ok(projected("later")))
    );
}

// A remote policy is held boxed, through its object-safe twin, as the
// coordinator holds every hand-written component.
#[tokio::test]
async fn a_boxed_policy_is_awaited_until_it_answers() {
    let (reactions, pending_reactions) = waiting();
    let (translation, pending_translation) = waiting();
    let policy: Box<dyn DynPolicyComponent> = Box::new(RemotePolicy {
        reactions: pending_reactions,
        translation: pending_translation,
    });
    assert_eq!(PolicyComponent::name(&policy), "remote-policy");
    assert_eq!(PolicyComponent::input_domain(&policy), "order");

    let req = trigger();
    let mut handled = pin!(PolicyComponent::react(&policy, &req));
    assert!(poll!(handled.as_mut()).is_pending());
    reactions.send(reacted()).unwrap();
    assert_eq!(poll!(handled.as_mut()), Poll::Ready(Ok(reacted())));

    let req = hpb::TranslateRequest::default();
    let mut translated = pin!(PolicyComponent::translate(&policy, &req));
    assert!(poll!(translated.as_mut()).is_pending());
    let fact = hpb::TranslateResponse {
        fact: Some(stock_unreserved()),
    };
    translation.send(fact.clone()).unwrap();
    assert_eq!(poll!(translated.as_mut()), Poll::Ready(Ok(fact)));
}

#[tokio::test]
async fn boxed_domains_and_projectors_dispatch_through_their_twins() {
    let (answer, pending) = waiting();
    let domain: Box<dyn DynCommandHandler> = Box::new(RemoteDomain(pending));
    assert_eq!(CommandHandler::name(&domain), "remote");
    assert_eq!(CommandHandler::kind(&domain), pb::ComponentKind::Aggregate);
    assert_eq!(
        CommandHandler::command_types(&domain),
        ["test.counter.IncreaseBy"]
    );
    answer.send(answered_with("boxed")).unwrap();
    let router = RouterBuilder::new()
        .aggregate(domain)
        .build()
        .expect("one boxed domain");
    assert_eq!(
        router.dispatch_command(&conf::increase_command(1)).await,
        Ok(answered_with("boxed"))
    );

    let (answer, pending) = waiting();
    let projector: Box<dyn DynProjectorHandler> = Box::new(RemoteProjector(pending));
    assert_eq!(ProjectorHandler::name(&projector), "remote-projector");
    answer.send(projected("boxed")).unwrap();
    assert_eq!(
        ProjectorHandler::dispatch(&projector, &conf::delivery("counter", 1)).await,
        Ok(projected("boxed"))
    );
}

/// `future`'s first poll, which must be its answer.
async fn first_poll<F: Future>(future: F) -> F::Output {
    match poll!(pin!(future)) {
        Poll::Ready(answer) => answer,
        Poll::Pending => panic!("business code is synchronous: no I/O to wait on"),
    }
}

/// A policy whose one refusal callback answers StockUnreserved.
fn translating() -> PolicyDispatch {
    PolicyDispatch::new("p", "order", "inventory")
        .on_refused(|_command, _refusal, _ctx| Ok(stock_unreserved()))
}

// No behaviour change: each of the router's own components answers on the
// first poll, and answers what its synchronous dispatch answers.
#[tokio::test]
async fn the_routers_own_components_answer_on_the_first_poll() {
    let counter: DomainDispatch<_> = conf::counter_aggregate(Default::default());
    let req = conf::increase_command(2);
    assert_eq!(
        first_poll(CommandHandler::dispatch(&counter, &req)).await,
        counter.dispatch(&req)
    );
    let unknown = conf::unhandled_command();
    assert_eq!(
        first_poll(CommandHandler::dispatch(&counter, &unknown)).await,
        counter.dispatch(&unknown)
    );

    let policy: PolicyDispatch = conf::order_policy(Default::default());
    let source = conf::policy_event_source("test.counter.Increased", 3);
    assert_eq!(
        first_poll(PolicyComponent::react(&policy, &source)).await,
        policy.react(&source)
    );
    let refusal = hpb::TranslateRequest::default();
    let translating = translating();
    assert_eq!(
        first_poll(PolicyComponent::translate(&translating, &refusal)).await,
        translating.translate(&refusal)
    );
    assert_eq!(
        translating.translate(&refusal).map(|t| t.fact),
        Ok(Some(stock_unreserved())),
        "the translation is answered"
    );

    let projector: ProjectorDispatch<_> = conf::counter_projector(&["counter"]);
    let run = conf::delivery("counter", 2);
    assert_eq!(
        first_poll(ProjectorHandler::dispatch(&projector, &run)).await,
        projector.dispatch(&run)
    );
}

#[tokio::test]
async fn a_router_hosting_its_own_components_answers_on_the_first_poll() {
    let router = RouterBuilder::new()
        .aggregate(conf::counter_aggregate(Default::default()))
        .build()
        .expect("one domain");
    let req = conf::increase_command(1);
    let answer = first_poll(router.dispatch_command(&req)).await;
    assert_eq!(
        answer,
        conf::counter_aggregate(Default::default()).dispatch(&req)
    );
    assert!(answer.is_ok());

    let router = RouterBuilder::new()
        .policy(translating())
        .build()
        .expect("one policy");
    let source = trigger();
    assert_eq!(
        first_poll(router.dispatch_react(&source)).await,
        translating().react(&source)
    );
    let refusal = hpb::TranslateRequest::default();
    assert_eq!(
        first_poll(router.dispatch_translate(&refusal)).await,
        translating().translate(&refusal)
    );

    let router = RouterBuilder::new()
        .projector(conf::counter_projector(&["counter"]))
        .build()
        .expect("one projector");
    let run = conf::delivery("counter", 1);
    assert_eq!(
        first_poll(router.dispatch_project(&run)).await,
        conf::counter_projector(&["counter"]).dispatch(&run)
    );
}

/// Compiles only when `future` is `Send`.
fn sent<F: Future + Send>(future: F) -> F {
    future
}

// A server spawns dispatch onto a multi-threaded runtime: the router and its
// boxed components are shared across tasks, and every dispatch future is
// `Send`. The spawn compiling is half the test; its answer the other.
#[tokio::test(flavor = "multi_thread")]
async fn dispatch_spawns_on_a_multi_threaded_runtime() {
    let (answer, pending) = waiting();
    let router = Arc::new(
        RouterBuilder::new()
            .aggregate(RemoteDomain(pending))
            .build()
            .expect("one domain"),
    );
    let shared = Arc::clone(&router);
    let task =
        tokio::spawn(
            async move { sent(shared.dispatch_command(&conf::increase_command(1))).await },
        );
    answer.send(answered_with("spawned")).unwrap();
    assert_eq!(task.await.unwrap(), Ok(answered_with("spawned")));

    let policy: Arc<dyn DynPolicyComponent> = Arc::new(conf::order_policy(Default::default()));
    let task = tokio::spawn(async move {
        let source = conf::policy_event_source("test.counter.Increased", 1);
        sent(PolicyComponent::react(&*policy, &source)).await
    });
    assert!(task.await.unwrap().is_ok());
}
