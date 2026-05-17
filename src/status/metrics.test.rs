//! Tests for the status-console metric stems.
//!
//! WHY: a metric name typo is silent — the metric still records, but
//! against the wrong stem, so dashboards / alerts looking for the
//! documented name find nothing. Pinning the names + scope in tests
//! catches the rename-without-grep failure mode.

#[cfg(feature = "otel")]
#[test]
fn meter_name_matches_documented_convention() {
    // Plan and operator runbooks key off this constant. Treat any
    // change as a breaking change worth noticing.
    assert_eq!(super::METER_NAME, "angzarr-status");
}

#[cfg(feature = "otel")]
#[test]
fn rpc_duration_handle_initialises() {
    // Force the LazyLock to materialize. Even with no metric exporter
    // wired up (the default in unit tests), construction must not
    // panic — handlers will rely on calling `.record()` from cold.
    let _h = &*super::RPC_DURATION;
}
