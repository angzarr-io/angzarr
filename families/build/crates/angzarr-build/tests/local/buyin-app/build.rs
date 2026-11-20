//! The wire types, the markup domain layer, the adapters, the descriptors
//! and register(), generated from the buy-in fixture protos into OUT_DIR.

const PROTOS: &[&str] = &[
    "buyin/v1/wallet.proto",
    "buyin/v1/buy_in.proto",
    "buyin/v1/policies.proto",
];
const INCLUDES: &[&str] = &["../../../../angzarr-codegen/tests/fixtures"];

fn main() -> Result<(), angzarr_build::BuildError> {
    angzarr_build::wire(PROTOS, INCLUDES)?;
    angzarr_build::markup_domain(PROTOS, INCLUDES, &[("buyin.v1", "crate::wire")])?;
    angzarr_build::adapters(PROTOS, INCLUDES, &[])?;
    angzarr_build::descriptors(PROTOS, INCLUDES, &[])?;
    angzarr_build::register(PROTOS, INCLUDES, &[])
}
