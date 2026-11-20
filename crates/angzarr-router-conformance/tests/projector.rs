//! Cucumber harness for the handler-contract tier's
//! `projector_dispatch.feature`: drives it against the Rust router's
//! ProjectorDispatch, through Project (one stream's run, decision 14.78).
//! Every router binds the same feature; only this step-definition layer
//! differs.

mod common;

use angzarr_router::error::CodedError;
use angzarr_router::hpb;
use angzarr_router::router::ProjectorHandler;
use angzarr_router_conformance as conf;
use cucumber::writer::Stats as _;
use cucumber::{given, then, when, World};

#[derive(Debug, Default, World)]
struct ProjectorWorld {
    /// Outcome of the dispatched run.
    result: Option<Result<hpb::ProjectResponse, CodedError>>,
    /// The projector's declared domains.
    domains: Vec<&'static str>,
}

impl ProjectorWorld {
    /// Projects through the kernel-facing [`ProjectorHandler`], awaiting
    /// its answer.
    async fn project(&mut self, req: hpb::ProjectRequest) {
        let projector = conf::counter_projector(&self.domains);
        self.result = Some(ProjectorHandler::dispatch(&projector, &req).await);
    }

    fn count(&self) -> u32 {
        conf::projected_count(
            self.result
                .as_ref()
                .expect("a run was dispatched")
                .as_ref()
                .expect("expected a successful projection"),
        )
    }
}

#[given("a counter projection")]
async fn a_counter_projection(w: &mut ProjectorWorld) {
    w.domains = vec!["counter"];
}

#[when(regex = r#"^(\d+) events are delivered in domain "([^"]*)"$"#)]
async fn events_delivered(w: &mut ProjectorWorld, count: u32, domain: String) {
    w.project(conf::delivery(&domain, count)).await;
}

#[then(regex = r"^the projection records (\d+) events?$")]
async fn records_count(w: &mut ProjectorWorld, count: u32) {
    assert_eq!(
        w.count(),
        count,
        "every delivered event folds into one projection"
    );
}

#[then("the projection records nothing")]
async fn records_nothing(w: &mut ProjectorWorld) {
    assert_eq!(w.count(), 0, "an undeclared domain folds nothing");
}

#[tokio::main]
async fn main() {
    let tier = ProjectorWorld::cucumber()
        .fail_on_skipped()
        .run(common::tier("projector_dispatch.feature"))
        .await;
    common::exit(tier.execution_has_failed());
}
