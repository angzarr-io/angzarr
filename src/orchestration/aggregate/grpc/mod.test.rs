//! Tests for the gRPC aggregate context.
//!
//! The `should_skip_post_persist` predicate that used to live here moved to
//! `super::super::sync_policy` (C-06), so its tests are now in
//! `sync_policy.test.rs` — they are the single source of truth that drives
//! both the local and gRPC `post_persist` short-circuit. This file is kept
//! around for future gRPC-context-specific tests.
