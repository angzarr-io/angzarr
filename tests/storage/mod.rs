//! Shared storage contract test suites.
//!
//! Defines reusable test suites for EventStore, SnapshotStore, and PositionStore
//! trait contracts. Each backend test module imports and runs these via macros.

pub mod event_store_tests;
pub mod position_store_tests;
pub mod snapshot_store_tests;

/// T12: assert every `pub async fn test_*` in a contract module is actually
/// wired into a runner/generator macro or invoked by a backend binary.
///
/// The contract modules carry `#![allow(dead_code)]` because each backend
/// binary compiles only the subset it runs — which means a contract fn that
/// loses its last caller vanishes SILENTLY (the T1 phantom-helper rot
/// class). This inventory check closes that gap: a fn must appear, by
/// name, in its module's macro region (`macro_region`) or in one of the
/// backend binary sources (`binaries`), or be listed in `skips` with a
/// written justification.
///
/// Known imprecision: matching is by name occurrence, so a same-named fn
/// in another suite (e.g. `test_aggregate_isolation` exists in both the
/// event and snapshot suites) can mask an unwiring if the OTHER suite's
/// usage remains. The rot vector this exists to catch — a fn referenced
/// by nothing anywhere — is still caught.
#[allow(dead_code)] // each binary compiles this; only some run the inventory tests
pub fn assert_contract_inventory(
    module_src: &str,
    macro_region_marker: &str,
    binaries: &[&str],
    skips: &[(&str, &str)],
) {
    let names: Vec<&str> = module_src
        .match_indices("pub async fn test_")
        .map(|(i, _)| {
            let start = i + "pub async fn ".len();
            let rest = &module_src[start..];
            let end = rest
                .find(|c: char| !(c.is_alphanumeric() || c == '_'))
                .unwrap_or(rest.len());
            &rest[..end]
        })
        .collect();
    assert!(
        !names.is_empty(),
        "inventory found no contract fns — marker or parsing broke"
    );

    let macro_region = module_src
        .split(macro_region_marker)
        .nth(1)
        .unwrap_or_else(|| panic!("macro region marker {macro_region_marker:?} not found"));

    let mut unwired: Vec<&str> = Vec::new();
    for name in names {
        if skips.iter().any(|(skip, _why)| *skip == name) {
            continue;
        }
        let in_macros = macro_region.contains(name);
        let in_binaries = binaries.iter().any(|src| src.contains(name));
        if !(in_macros || in_binaries) {
            unwired.push(name);
        }
    }
    assert!(
        unwired.is_empty(),
        "contract fns defined but wired into NOTHING (no runner macro entry, \
         no backend binary call) — either wire them or add them to the skip \
         list with a justification: {unwired:?}"
    );
}
