//! Running a harness's features: angzarr-project's handler-contract tier
//! (`features/handler-contract/`, the router's complete contract, decision
//! 14.42), against the tier's fixtures (`fixture/`). It runs whole: every
//! scenario, none held out.

use std::path::PathBuf;

/// A handler-contract tier feature, by file name.
pub fn tier(feature: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../angzarr-project/features/handler-contract")
        .join(feature)
}

/// Fails the harness when any run failed.
pub fn exit(failed: bool) {
    if failed {
        eprintln!("conformance: a feature failed");
        std::process::exit(1);
    }
}
