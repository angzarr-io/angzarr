//! A probe for `tests/features.rs`: it names the command half, so it builds
//! only where the `commands` feature is on. Not an example of use.

fn main() {
    let _ = angzarr_coordinator::Caller::Client;
    let _ = angzarr_coordinator::NoCommands;
}
