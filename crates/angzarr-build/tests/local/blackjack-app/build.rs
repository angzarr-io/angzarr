//! The wire types, the markup domain layer, the adapters and register(),
//! generated from the blackjack example's protos (the spec's) into OUT_DIR.

const PROTOS: &[&str] = &[
    "io/angzarr/examples/v1/cards.proto",
    "io/angzarr/examples/v1/wallet.proto",
    "io/angzarr/examples/v1/buy_in.proto",
    "io/angzarr/examples/v1/table.proto",
    "io/angzarr/examples/v1/player.proto",
    "io/angzarr/examples/v1/policies.proto",
    "io/angzarr/examples/v1/ledger.proto",
];
/// The spec's protos: the directory the `angzarr-project` package names,
/// which tests/local.rs hands every build here (one pin: the workspace's).
fn spec_proto() -> String {
    println!("cargo:rerun-if-env-changed=ANGZARR_SPEC_PROTO");
    std::env::var("ANGZARR_SPEC_PROTO").expect("ANGZARR_SPEC_PROTO (set by tests/local.rs)")
}

fn main() -> Result<(), angzarr_build::BuildError> {
    let spec = spec_proto();
    let includes = &[spec.as_str()];
    angzarr_build::wire(PROTOS, includes)?;
    angzarr_build::markup_domain(
        PROTOS,
        includes,
        &[("io.angzarr.examples.v1", "crate::wire")],
    )?;
    angzarr_build::adapters(PROTOS, includes, &[])?;
    angzarr_build::register(PROTOS, includes, &[])
}
