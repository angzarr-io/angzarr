//! The wire types, the markup domain layer, the adapters and register(),
//! generated from the bare fixture proto (a domain with no events) into
//! OUT_DIR.

const PROTOS: &[&str] = &["bare/v1/bare.proto"];
/// The spec's protos: the directory the `angzarr-project` package names,
/// which tests/local.rs hands every build here (one pin: the workspace's).
fn spec_proto() -> String {
    println!("cargo:rerun-if-env-changed=ANGZARR_SPEC_PROTO");
    std::env::var("ANGZARR_SPEC_PROTO").expect("ANGZARR_SPEC_PROTO (set by tests/local.rs)")
}

fn main() -> Result<(), angzarr_build::BuildError> {
    let spec = spec_proto();
    let includes = &["../../../../angzarr-codegen/tests/fixtures", spec.as_str()];
    angzarr_build::wire(PROTOS, includes)?;
    angzarr_build::markup_domain(PROTOS, includes, &[("bare.v1", "crate::wire")])?;
    angzarr_build::adapters(PROTOS, includes, &[])?;
    angzarr_build::register(PROTOS, includes, &[])
}
