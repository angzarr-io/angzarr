//! The shapes fixtures' wire types and their domain layers, a module per
//! package, each referencing the others' types.

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

#[cfg(test)]
mod roundtrip;
