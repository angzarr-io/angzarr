//! Tests for the replay trait + ReplayMode conversions.
//!
//! WHY: ReplayMode lives in two places — the proto wire enum and a
//! Rust enum used inside the handler. Drift between them would
//! silently mis-replay (operator picks FRESH_SEQUENCE and gets AS_IS,
//! or vice versa). UNSPECIFIED handling also matters: per the plan's
//! safe-default contract, an operator who doesn't specify a mode
//! should get the mode most likely to succeed, NOT the diagnostic
//! reproduce-the-failure mode.

use super::*;

#[test]
fn proto_as_is_maps_to_rust_as_is() {
    use crate::proto::status::ReplayMode as Proto;
    assert_eq!(ReplayMode::from_proto(Proto::AsIs as i32), ReplayMode::AsIs);
}

#[test]
fn proto_fresh_sequence_maps_to_rust_fresh_sequence() {
    use crate::proto::status::ReplayMode as Proto;
    assert_eq!(
        ReplayMode::from_proto(Proto::FreshSequence as i32),
        ReplayMode::FreshSequence
    );
}

#[test]
fn proto_unspecified_defaults_to_fresh_sequence_safe_default() {
    // Plan contract: UNSPECIFIED → FRESH_SEQUENCE. The operator
    // who left the field blank almost certainly wants the command
    // to succeed, not the diagnostic mode that re-reproduces the
    // original failure.
    use crate::proto::status::ReplayMode as Proto;
    assert_eq!(
        ReplayMode::from_proto(Proto::Unspecified as i32),
        ReplayMode::FreshSequence
    );
}

#[test]
fn proto_out_of_range_value_falls_back_to_fresh_sequence() {
    // Tolerance: a client speaking a newer proto we don't know yet
    // (or a corrupted wire value) must not panic. The safe default
    // applies.
    assert_eq!(ReplayMode::from_proto(999_999), ReplayMode::FreshSequence);
}

#[test]
fn rust_as_is_maps_back_to_proto_as_is() {
    use crate::proto::status::ReplayMode as Proto;
    assert_eq!(ReplayMode::AsIs.to_proto(), Proto::AsIs);
}

#[test]
fn rust_fresh_sequence_maps_back_to_proto_fresh_sequence() {
    use crate::proto::status::ReplayMode as Proto;
    assert_eq!(ReplayMode::FreshSequence.to_proto(), Proto::FreshSequence);
}

#[tokio::test]
async fn noop_replay_publisher_is_not_configured() {
    let p = NoopReplayPublisher;
    assert!(!p.is_configured());
}

#[tokio::test]
async fn noop_replay_publisher_source_id_is_noop() {
    // Handler keys the Health<T> envelope's `source` field off this.
    // Pinning catches a silent rename.
    let p = NoopReplayPublisher;
    assert_eq!(p.source_id(), "noop");
}

#[tokio::test]
async fn noop_replay_publisher_returns_not_configured() {
    // The discriminator that the handler maps to a 503-class
    // ProblemDetails. Any other error variant would land in a
    // different bucket and surface incorrectly.
    let p = NoopReplayPublisher;
    let err = p
        .replay(crate::proto::CommandBook::default())
        .await
        .unwrap_err();
    assert!(matches!(err, DlqError::NotConfigured));
}

#[test]
fn trait_default_is_configured_returns_true() {
    // Same pattern as DeadLetterReader: a real impl that doesn't
    // override should report configured. Catches the mutation that
    // flips the default body to false (silently breaks every real
    // publisher).
    struct DefaultsPublisher;
    #[async_trait::async_trait]
    impl ReplayPublisher for DefaultsPublisher {
        async fn replay(&self, _: crate::proto::CommandBook) -> Result<(), DlqError> {
            Ok(())
        }
    }
    let p = DefaultsPublisher;
    assert!(p.is_configured());
    assert_eq!(p.source_id(), "unknown");
}
