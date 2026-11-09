//! A probe for `tests/features.rs`: it names the read half and the routing,
//! and builds with the `reads` feature alone. Not an example of use.

fn main() {
    let _ = angzarr_coordinator::NoReads;
    let _ = angzarr_coordinator::Reach::Ip;
}
