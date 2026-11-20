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
const INCLUDES: &[&str] = &["../../../../../angzarr-project/proto"];

fn main() -> Result<(), angzarr_build::BuildError> {
    angzarr_build::wire(PROTOS, INCLUDES)?;
    angzarr_build::markup_domain(
        PROTOS,
        INCLUDES,
        &[("io.angzarr.examples.v1", "crate::wire")],
    )?;
    angzarr_build::adapters(PROTOS, INCLUDES, &[])?;
    angzarr_build::register(PROTOS, INCLUDES, &[])
}
