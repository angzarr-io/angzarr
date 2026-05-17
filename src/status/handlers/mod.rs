//! gRPC handler implementations for the `angzarr-status` operations
//! console.
//!
//! Each handler is a thin façade over a domain-specific trait
//! (`DeadLetterReader`, future `ClusterHealth`, `EventBrowser`, etc.):
//! the handler maps proto types in/out and stamps the
//! `Health<T>`-style envelope onto every response. Domain logic
//! lives behind the trait so unit tests don't need a tonic server
//! and so the backend (Postgres, SQLite, mock) can be swapped per
//! deployment without touching the gRPC surface.
//!
//! Plan reference: P1.1+ in `plans/virtual-spinning-flute.md`.

pub mod dlq;
