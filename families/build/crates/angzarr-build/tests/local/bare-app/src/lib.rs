//! A domain with no events on angzarr-embedded: its wire types, domain
//! layer, adapters and register() generated from the bare fixture proto, its
//! component the scaffold of the day (`todo!()` bodies), written into this
//! directory by tests/local.rs before it builds, never committed.

pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/bare.v1.rs"));
}
pub mod domain {
    include!(concat!(env!("OUT_DIR"), "/bare.v1.domain.rs"));
}
include!(concat!(env!("OUT_DIR"), "/angzarr-adapters.rs"));
include!(concat!(env!("OUT_DIR"), "/angzarr-register.rs"));

pub mod bare_state_impl;
