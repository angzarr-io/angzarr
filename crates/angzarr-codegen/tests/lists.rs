//! Every handling returns a list (decision 14.75; C-0797): a command's or
//! a fact's handler returns any number of its component's events, none
//! included, whatever its emits. The kinds fixture (`kinds/v1/kinds.proto`)
//! has commands and facts of one, two and four emits types, and one of
//! none; its domain layer (its interfaces in it), scaffold and adapters are
//! pinned by `tests/refusals.rs`'s golden files.

mod common;

use angzarr_codegen::{adapters, markup_domain};
use common::*;

const KINDS: &str = "kinds/v1/kinds.proto";

/// The kinds fixture's domain layer, the ledger's interface in it.
fn interface() -> String {
    markup_domain(
        &descriptors(&[KINDS]),
        &[KINDS],
        &[("kinds.v1", "crate::wire")],
    )
    .unwrap()
    .remove(0)
    .source
}

#[test]
fn test_every_handler_returns_a_list_of_its_components_events() {
    let source = interface();
    for expected in [
        "fn open(&self, cmd: &Open, ctx: &Context) -> Result<Vec<LedgerStateEvent>, Failed<Unrefusable>>;",
        "fn four(&self, cmd: &Four, ctx: &Context) -> Result<Vec<LedgerStateEvent>, Failed<Unrefusable>>;",
        "fn maybe(&self, cmd: &Maybe, ctx: &Context) -> Result<Vec<LedgerStateEvent>, Failed<Unrefusable>>;",
        "    ) -> Result<Vec<LedgerStateEvent>, Failed<PostRefused>>;",
        "fn raw(&self, cmd: &Raw, ctx: &Context) -> Result<Vec<LedgerStateEvent>, Failed<Unrefusable>>;",
        "fn arrived(&self, fact: &Arrived, ctx: &Context) -> Result<Vec<LedgerStateEvent>, Bug>;",
        "fn swept(&self, fact: &Swept, ctx: &Context) -> Result<Vec<LedgerStateEvent>, Bug>;",
    ] {
        let flat = |text: &str| text.replace([' ', '\n'], "").replace(",)", ")");
        assert!(
            flat(&source).contains(&flat(expected)),
            "{expected}:\n{source}"
        );
    }
    // Their snapshots are no handler: `snapshot` alone returns an `Option`.
    let handlers = source.replace("fn snapshot(&self) -> Option<", "");
    for shape in [
        "-> Option<",
        "-> Result<Option<",
        "-> Result<(Posted",
        "-> (",
        "Result<Posted,",
    ] {
        assert!(!handlers.contains(shape), "{shape} in:\n{source}");
    }
}

#[test]
fn test_the_adapter_hands_the_markup_a_step_per_event() {
    let source = adapters(&descriptors(&[KINDS]), &[KINDS], &[]).unwrap();
    let flat = source.replace([' ', '\n'], "");
    for expected in [
        // One helper per domain, not a fold per handler: each event a step
        // of the markup, a command's returning the handler's own failure.
        "fnemitted<E:Fails<Self>+'static>(events:Vec<LedgerStateEvent>",
        "fnrecorded(events:Vec<LedgerStateEvent>)->FactEmit<Self>{",
        "Ok::<_,E>(event)",
        "FactEmit::of(events.iter().map(|event|event.to_any()))",
        // An empty list is no event.
        "events.into_iter().fold(Emit::of(None::<Posted>),",
        // Each handler names its failure, or records.
        ".map(Self::emitted::<Failed<Refuses<Self,Batch>>>)",
        ".map(Self::emitted::<Failed<Unrefusable>>)",
        "crate::domain::LedgerStateInterface::arrived(&self.0,&fact,ctx).map(Self::recorded)",
    ] {
        assert!(flat.contains(expected), "{expected}:\n{source}");
    }
    assert_eq!(
        flat.matches(".fold(").count(),
        3,
        "one fold per helper (a command's), and the policy's two:\n{source}"
    );
}
