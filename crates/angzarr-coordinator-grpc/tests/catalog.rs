//! The catalog and its errors agree: a message for every code, using
//! exactly its args, and no message without an error.

use std::collections::BTreeSet;

use angzarr_coordinator_grpc::angzarr_diagnostic::catalog;

#[test]
fn its_locales_are_every_catalog_under_locales_en_us_alone() {
    let (name, table) = angzarr_coordinator_grpc::LOCALES;
    assert_eq!(name, angzarr_coordinator_grpc::EN_US.0);
    assert_eq!(table, &[("en-US", angzarr_coordinator_grpc::EN_US.1)]);
}

#[test]
fn grpc_errors_have_catalog_messages() {
    let (name, text) = angzarr_coordinator_grpc::EN_US;
    assert_eq!(name, env!("CARGO_PKG_NAME"));
    let samples = angzarr_coordinator_grpc::diagnostic_samples();
    let sampled: BTreeSet<String> = samples.iter().map(|d| d.code().to_string()).collect();
    let expected: BTreeSet<String> = [
        "authorization-repeated",
        "authorization-not-text",
        "authorization-not-bearer",
        "authorization-empty",
        "listen-loopback-on-wide-bind",
        "listen-socket-in-use",
        "listen-not-a-socket",
        "listen-directory-not-private",
        "listen-no-directory",
        "listen-io",
    ]
    .into_iter()
    .map(|suffix| format!("angzarr-coordinator-grpc-{suffix}"))
    .collect();
    assert_eq!(sampled, expected, "one sample per error variant");
    assert_eq!(catalog::check(name, text, &samples), []);
}
