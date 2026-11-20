//! The json fixture's wire types, and their proto3 JSON.

// prost's choice of Choice.pick holds a Plain inline, much larger than its
// other arms.
#[allow(clippy::large_enum_variant)]
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/angzarr-wire.rs"));
}

/// The generated JSON: the runtime, and every wire type's impls.
pub mod json {
    include!(concat!(env!("OUT_DIR"), "/angzarr-json.rs"));
}

#[cfg(test)]
mod tests;
