//! Projectors (decision 14.50): one projector of many source domains, its
//! sources derived from the events it names (decision 14.78; the
//! descriptor's are `tests/descriptor.rs`'s); an async user interface; one
//! `#[projector]` over every source; registered with the rest. The kinds fixture's LedgerView is
//! pinned by `tests/refusals.rs`'s golden files and run on
//! angzarr-embedded by angzarr-build's kinds-app.

mod common;

use angzarr_codegen::{adapters, register, scaffold};

const KINDS: &str = "kinds/v1/kinds.proto";

#[test]
fn test_the_projector_adapter_projects_every_source_under_its_name() {
    let source = adapters(&common::descriptors(&[KINDS]), &[KINDS], &[]).unwrap();
    assert!(
        source.contains(
            "#[projector(name = \"projector-ledger-view\", sources = [\"ledger\", \"audit\"])]"
        ),
        "{source}"
    );
    assert!(source.contains("#[projects(Audited)]"), "{source}");
    let register = register(&common::descriptors(&[KINDS]), &[KINDS], &[]).unwrap();
    assert!(
        register.contains("registry.projector(crate::adapters::LedgerView::default())?;"),
        "{register}"
    );
}

#[test]
fn test_one_input_domain_is_the_one_entry_list() {
    const ONE: &str = "kinds/v1/one_source.proto";
    let source = adapters(&common::descriptors(&[ONE]), &[ONE], &[]).unwrap();
    assert!(
        source.contains("#[projector(name = \"projector-thing-view\", sources = [\"things\"])]"),
        "{source}"
    );
    let files = scaffold(&common::descriptors(&[ONE]), &[ONE]).unwrap();
    assert!(files.iter().any(|f| f.path == "thing_view_impl.rs"));
}
