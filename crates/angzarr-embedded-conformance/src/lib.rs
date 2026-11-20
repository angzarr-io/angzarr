//! Test-only: angzarr's coordinator-contract tier run against
//! angzarr-embedded's kernel. The tier's `.feature` files are the spec's,
//! vendored in `features/` at the revision `SPEC_REV` names (`just
//! spec-vendor` refreshes them); the runner is `lib.test.rs`, and there is
//! nothing to use here.
//!
//! Every scenario is either bound, its steps run against the kernel, or
//! tagged ([`tags`](lib.test.rs)): a divergence (angzarr-embedded's
//! execution model differs from the spec here, deliberately), an
//! unsupported surface (angzarr-embedded has nothing of the kind), or
//! unbound (no binding yet). Tagged scenarios are not run; an untagged
//! scenario that fails, or has a step with no binding, fails the run.

#[cfg(test)]
#[path = "lib.test.rs"]
mod tests;
