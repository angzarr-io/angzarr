//! The blackjack example on angzarr-embedded: its wire types, domain layer,
//! adapters and register() generated from the example's protos, and its
//! components' implementations the scaffold of the day, `todo!()` bodies
//! and all: written into this directory by tests/local.rs before it builds,
//! never committed. The point is that what is generated and what is
//! scaffolded compile against angzarr-embedded and angzarr-router.

pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/io.angzarr.examples.v1.rs"));
}
pub mod domain {
    include!(concat!(env!("OUT_DIR"), "/io.angzarr.examples.v1.domain.rs"));
}
include!(concat!(env!("OUT_DIR"), "/angzarr-adapters.rs"));
include!(concat!(env!("OUT_DIR"), "/angzarr-register.rs"));

pub mod buy_in_state_impl;
pub mod buy_in_table_policy_impl;
pub mod buy_in_wallet_policy_impl;
pub mod player_state_impl;
pub mod player_table_policy_impl;
pub mod table_buy_in_policy_impl;
pub mod table_ledger_projection_impl;
pub mod table_player_policy_impl;
pub mod table_state_impl;
pub mod table_wallet_policy_impl;
pub mod wallet_buy_in_policy_impl;
pub mod wallet_ledger_projection_impl;
pub mod wallet_state_impl;
