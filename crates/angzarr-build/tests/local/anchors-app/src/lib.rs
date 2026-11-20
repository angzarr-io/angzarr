//! Aggregates on angzarr-embedded, in a crate whose components are in
//! two packages: each package's domain layer at its own path, the
//! generated adapters and register() over the scaffold of the day, the
//! orders taking snapshots as their anchor's wire type and the catalog none.

pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/angzarr-wire.rs"));
}

pub mod domain {
    pub mod common {
        include!(concat!(env!("OUT_DIR"), "/shapes.common.v1.domain.rs"));
    }
    pub mod catalog {
        include!(concat!(env!("OUT_DIR"), "/shapes.catalog.v1.domain.rs"));
    }
    pub mod order {
        include!(concat!(env!("OUT_DIR"), "/shapes.order.v1.domain.rs"));
    }
}
include!(concat!(env!("OUT_DIR"), "/angzarr-adapters.rs"));
include!(concat!(env!("OUT_DIR"), "/angzarr-register.rs"));

pub mod catalog_state_impl;
pub mod order_state_impl;
mod snapshots;
