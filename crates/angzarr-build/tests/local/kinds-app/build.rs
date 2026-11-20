//! The wire types, the markup domain layer, the adapters, register() and
//! the components' names, generated from the kinds fixture proto into
//! OUT_DIR.

const PROTOS: &[&str] = &["kinds/v1/kinds.proto"];
/// The fixtures, then the spec's protos, whose options.proto
/// angzarr-proto's copy can trail.
const INCLUDES: &[&str] = &[
    "../../../../angzarr-codegen/tests/fixtures",
    "../../../../../angzarr-project/proto",
];

fn main() -> Result<(), angzarr_build::BuildError> {
    // The components' names, for the code written by hand: the policy it
    // writes in place of the generated one takes the generated one's.
    let names: String = angzarr_build::component_names(PROTOS, INCLUDES)?
        .into_iter()
        .map(|c| {
            let constant = c.component.rsplit('.').next().unwrap_or_default().to_uppercase();
            format!("pub const {constant}: &str = {:?};\n", c.name)
        })
        .collect();
    let out = std::env::var("OUT_DIR").expect("OUT_DIR");
    std::fs::write(format!("{out}/names.rs"), names).expect("names.rs is written");
    angzarr_build::wire(PROTOS, INCLUDES)?;
    angzarr_build::markup_domain(PROTOS, INCLUDES, &[("kinds.v1", "crate::wire")])?;
    angzarr_build::adapters(PROTOS, INCLUDES, &[])?;
    angzarr_build::register(PROTOS, INCLUDES, &[])
}
