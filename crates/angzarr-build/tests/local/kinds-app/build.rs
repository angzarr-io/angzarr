//! The wire types, the markup domain layer, the adapters, register() and
//! the components' names, generated from the kinds fixture proto into
//! OUT_DIR.

const PROTOS: &[&str] = &["kinds/v1/kinds.proto"];
/// The spec's protos: the directory the `angzarr-project` package names,
/// which tests/local.rs hands every build here (one pin: the workspace's).
fn spec_proto() -> String {
    println!("cargo:rerun-if-env-changed=ANGZARR_SPEC_PROTO");
    std::env::var("ANGZARR_SPEC_PROTO").expect("ANGZARR_SPEC_PROTO (set by tests/local.rs)")
}

fn main() -> Result<(), angzarr_build::BuildError> {
    let spec = spec_proto();
    // The fixtures, then the spec's protos, whose options.proto
    // angzarr-proto's copy can trail.
    let includes = &["../../../../angzarr-codegen/tests/fixtures", spec.as_str()];
    // The components' names, for the code written by hand: the policy it
    // writes in place of the generated one takes the generated one's.
    let names: String = angzarr_build::component_names(PROTOS, includes)?
        .into_iter()
        .map(|c| {
            let constant = c
                .component
                .rsplit('.')
                .next()
                .unwrap_or_default()
                .to_uppercase();
            format!("pub const {constant}: &str = {:?};\n", c.name)
        })
        .collect();
    let out = std::env::var("OUT_DIR").expect("OUT_DIR");
    std::fs::write(format!("{out}/names.rs"), names).expect("names.rs is written");
    angzarr_build::wire(PROTOS, includes)?;
    angzarr_build::markup_domain(PROTOS, includes, &[("kinds.v1", "crate::wire")])?;
    angzarr_build::adapters(PROTOS, includes, &[])?;
    angzarr_build::register(PROTOS, includes, &[])
}
