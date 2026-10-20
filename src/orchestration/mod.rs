//! Orchestration: the coordinators' processing pipelines.
//!
//! - [`aggregate`]: the command pipeline of the aggregate coordinator
//!   (validate, assign ids, load and deduplicate, resolve handling, business
//!   code, append, publish).
//! - [`projector`]: the projector handler abstraction shared by the bus
//!   consumer and the synchronous projector path.

pub mod aggregate;
pub mod projector;
