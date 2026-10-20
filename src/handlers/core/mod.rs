//! Bus-driven handlers: events from the bus, forwarded to business code.

pub mod projector;

pub use projector::ProjectorEventHandler;
