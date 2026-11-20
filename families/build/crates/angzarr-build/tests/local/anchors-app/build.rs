//! The shapes fixtures' wire types, each package's domain layer at its own
//! path, the adapters (each package's reading its own layer) and
//! register(), generated into OUT_DIR.

const PROTOS: &[&str] = &[
    "shapes/common/v1/common.proto",
    "shapes/catalog/v1/catalog.proto",
    "shapes/order/v1/order.proto",
];
const INCLUDES: &[&str] = &["../../../../angzarr-codegen/tests/fixtures"];
/// Each package's layer at its own path, as the adapters read them.
const DOMAIN: &[(&str, &str)] = &[
    ("shapes.common.v1", "crate::domain::common"),
    ("shapes.catalog.v1", "crate::domain::catalog"),
    ("shapes.order.v1", "crate::domain::order"),
];

fn main() -> Result<(), angzarr_build::BuildError> {
    angzarr_build::wire(PROTOS, INCLUDES)?;
    angzarr_build::markup_domain_with(
        PROTOS,
        INCLUDES,
        &[
            ("shapes.common.v1", "crate::wire::shapes::common::v1"),
            ("shapes.catalog.v1", "crate::wire::shapes::catalog::v1"),
            ("shapes.order.v1", "crate::wire::shapes::order::v1"),
        ],
        DOMAIN,
    )?;
    angzarr_build::adapters_with(PROTOS, INCLUDES, &[], DOMAIN)?;
    angzarr_build::register(PROTOS, INCLUDES, &[])
}
