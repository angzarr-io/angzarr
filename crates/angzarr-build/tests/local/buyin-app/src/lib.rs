//! The buy-in on angzarr-embedded: its wire types, domain layer, adapters,
//! descriptors and register() generated from the buy-in fixture protos, and
//! its components' interfaces the domain layer's and their implementations
//! the scaffold's, bodies
//! written.

pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/buyin.v1.rs"));
}
pub mod domain {
    include!(concat!(env!("OUT_DIR"), "/buyin.v1.domain.rs"));
}
include!(concat!(env!("OUT_DIR"), "/angzarr-adapters.rs"));
/// Each component's generated ComponentDescriptor (decision 14.42).
pub mod descriptors {
    include!(concat!(env!("OUT_DIR"), "/angzarr-descriptors.rs"));
}
include!(concat!(env!("OUT_DIR"), "/angzarr-register.rs"));

pub mod buy_in_state_impl;
pub mod buy_in_wallet_policy_impl;
pub mod wallet_buy_in_policy_impl;
pub mod wallet_state_impl;
