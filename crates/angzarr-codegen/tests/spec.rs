//! The spec's codegen tier (`angzarr-project/features/codegen`, the
//! submodule's), run by cucumber against angzarr-codegen: every scenario
//! not tagged ([`tags`]) runs; one with a step that matches no binding, or
//! a step that fails, fails the run. A tagged scenario is not run, and the
//! run lists it with its tag; with `SPEC_RUN_TAGGED=1` in the environment
//! every scenario runs, to see which tags no longer hold.

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use cucumber::World as _;
use cucumber::writer::Stats as _;

#[path = "spec/declarations.rs"]
mod declarations;
#[path = "spec/generation.rs"]
mod generation;
#[path = "spec/interfaces.rs"]
mod interfaces;
#[path = "spec/protos.rs"]
mod protos;
#[path = "spec/tags.rs"]
mod tags;
#[path = "spec/world.rs"]
mod world;

/// Where the tier is: the angzarr-project submodule's.
const FEATURES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../angzarr-project/features/codegen"
);

/// Wakes the thread blocked in [`block_on`].
struct Unpark(Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Run `future` to its end on this thread, parking it while the future
/// waits: cucumber's runner needs no runtime of its own.
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        thread::park();
    }
}

/// The tier, run: every untagged scenario passes.
#[test]
fn codegen_tier() {
    let run_tagged = std::env::var_os("SPEC_RUN_TAGGED").is_some();
    let writer = block_on(
        world::Generator::cucumber()
            .with_default_cli()
            .fail_on_skipped()
            .filter_run(FEATURES, move |_, _, scenario| {
                run_tagged || tags::of(scenario).is_none()
            }),
    );
    print!("{}", tags::listing(FEATURES));
    assert!(
        !writer.execution_has_failed(),
        "{} scenarios failed ({} steps failed, {} skipped), {} parsing errors: see the output above",
        writer.scenarios_stats().failed,
        writer.steps_stats().failed,
        writer.steps_stats().skipped,
        writer.parsing_errors(),
    );
}

/// Every scenario carries one ID, and every tag names one of them.
#[test]
fn every_tag_names_a_scenario_of_the_tier() {
    tags::check(FEATURES);
}
