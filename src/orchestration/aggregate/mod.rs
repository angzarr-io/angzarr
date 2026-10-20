//! The aggregate coordinator's command pipeline.
//!
//! One pipeline, normative in every mode (types.proto, `Handling`, "The
//! pipeline"):
//!
//! 1. **validate**: refuse a malformed command before anything is minted,
//!    looked up or stored ([`validate`]);
//! 2. **assign ids**: mint or derive `command_id`, set an absent
//!    `correlation_id`;
//! 3. **load, then deduplicate**: load the stream (snapshot and pages), then
//!    look up `(stream, command_id)`; a hit is answered DUPLICATE;
//! 4. **resolve**: stamp or check `expected_sequence` and resolve the
//!    conflict resolution;
//! 5. **business code**: `CommandHandlerService.Handle`, then the append.
//!    An append that loses the optimistic race re-runs from step 3.
//!
//! Which RPC the caller used (Handle, HandleAsync, HandleSync) decides only
//! what is waited for and who is answered; see
//! [`crate::services::aggregate`].

mod business;
mod error;
mod pipeline;
pub mod validate;

pub use business::{BusinessLogic, GrpcBusinessLogic};
pub use error::PipelineError;
pub use pipeline::{AggregatePipeline, Clock, Decided, Delivery, SystemClock};

#[cfg(test)]
mod tests;
