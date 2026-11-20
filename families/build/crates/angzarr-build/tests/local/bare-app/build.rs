//! The wire types, the markup domain layer, the adapters and register(),
//! generated from the bare fixture proto (a domain with no events) into
//! OUT_DIR.

const PROTOS: &[&str] = &["bare/v1/bare.proto"];
const INCLUDES: &[&str] = &[
    "../../../../angzarr-codegen/tests/fixtures",
    "../../../../../angzarr-project/proto",
];

fn main() -> Result<(), angzarr_build::BuildError> {
    angzarr_build::wire(PROTOS, INCLUDES)?;
    angzarr_build::markup_domain(PROTOS, INCLUDES, &[("bare.v1", "crate::wire")])?;
    angzarr_build::adapters(PROTOS, INCLUDES, &[])?;
    angzarr_build::register(PROTOS, INCLUDES, &[])
}
