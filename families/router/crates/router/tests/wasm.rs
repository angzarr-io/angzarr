//! On wasm32 the kernel-facing dispatch traits ask no `Send` (owner rulings
//! 2026-10-27 and 2026-10-30): a browser coordinator's components, whose
//! handles are not `Send`, implement the same one trait set, and the
//! router hosts them. `RcDomain`, `RcPolicy` and `RcProjector` stand for
//! such components: each holds an `Rc`, so neither it nor its dispatch
//! future (which holds the `Rc` across an await) is `Send`.
//!
//! This file is compiled only for wasm32 (the native build of the same
//! traits asks `Send`, and these impls would not compile there); `just wasm`
//! runs it under node with wasm-bindgen-test-runner.

#![cfg(target_arch = "wasm32")]

use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use angzarr_router::error::CodedError;
use angzarr_router::hpb;
use angzarr_router::router::{
    CommandHandler, DynCommandHandler, DynPolicyComponent, DynProjectorHandler, PolicyComponent,
    ProjectorHandler, RouterBuilder,
};
use wasm_bindgen_test::wasm_bindgen_test;

/// An await point holding `held` across it: makes the future not `Send`.
async fn across_an_await(held: Rc<str>) -> String {
    std::future::ready(()).await;
    held.to_string()
}

struct RcDomain(Rc<str>);

impl CommandHandler for RcDomain {
    fn name(&self) -> &str {
        &self.0
    }
    fn command_types(&self) -> Vec<String> {
        Vec::new()
    }
    async fn dispatch(&self, _req: &hpb::HandleRequest) -> Result<hpb::HandleResponse, CodedError> {
        Ok(hpb::HandleResponse {
            rules_version: across_an_await(Rc::clone(&self.0)).await,
            ..Default::default()
        })
    }
}

struct RcPolicy(Rc<str>);

impl PolicyComponent for RcPolicy {
    fn name(&self) -> &str {
        &self.0
    }
    fn input_domain(&self) -> &str {
        &self.0
    }
    async fn react(&self, _req: &hpb::ReactRequest) -> Result<hpb::ReactResponse, CodedError> {
        across_an_await(Rc::clone(&self.0)).await;
        Ok(hpb::ReactResponse::default())
    }
    async fn translate(
        &self,
        _req: &hpb::TranslateRequest,
    ) -> Result<hpb::TranslateResponse, CodedError> {
        across_an_await(Rc::clone(&self.0)).await;
        Ok(hpb::TranslateResponse::default())
    }
}

struct RcProjector(Rc<str>);

impl ProjectorHandler for RcProjector {
    fn name(&self) -> &str {
        &self.0
    }
    async fn dispatch(
        &self,
        _req: &hpb::ProjectRequest,
    ) -> Result<hpb::ProjectResponse, CodedError> {
        Ok(hpb::ProjectResponse {
            projection: Some(prost_types::Any {
                type_url: across_an_await(Rc::clone(&self.0)).await,
                value: Vec::new(),
            }),
        })
    }
}

fn now<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(answer) => answer,
        Poll::Pending => panic!("nothing here waits"),
    }
}

#[wasm_bindgen_test]
fn a_router_hosts_components_that_are_not_send() {
    let router = RouterBuilder::new()
        .aggregate(RcDomain(Rc::from("rc")))
        .build()
        .expect("one domain");
    let answer = now(router.dispatch_command(&hpb::HandleRequest::default()));
    assert_eq!(answer.expect("answered").rules_version, "rc");

    let router = RouterBuilder::new()
        .projector(RcProjector(Rc::from("rc")))
        .build()
        .expect("one projector");
    let answer = now(router.dispatch_project(&hpb::ProjectRequest::default()));
    assert_eq!(
        answer.expect("answered").projection.map(|p| p.type_url),
        Some("rc".to_string())
    );
}

#[wasm_bindgen_test]
fn components_that_are_not_send_box_through_their_twins() {
    let domain: Box<dyn DynCommandHandler> = Box::new(RcDomain(Rc::from("rc")));
    let policy: Box<dyn DynPolicyComponent> = Box::new(RcPolicy(Rc::from("rc")));
    let projector: Box<dyn DynProjectorHandler> = Box::new(RcProjector(Rc::from("rc")));
    assert!(now(CommandHandler::dispatch(&domain, &Default::default())).is_ok());
    assert!(now(PolicyComponent::react(&policy, &Default::default())).is_ok());
    assert!(now(PolicyComponent::translate(&policy, &Default::default())).is_ok());
    assert!(now(ProjectorHandler::dispatch(&projector, &Default::default())).is_ok());
}
