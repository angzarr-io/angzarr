//! Tests for fill_correlation_id.
//!
//! Correlation IDs enable cross-domain tracing in saga and process manager
//! flows. When a saga produces commands, the framework must ensure each
//! command carries the correlation ID from the triggering event — otherwise
//! observability breaks and PMs cannot correlate related events.

use super::*;
use crate::proto::Cover;

fn make_command_with_correlation(domain: &str, correlation_id: &str) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            correlation_id: correlation_id.to_string(),
            ..Default::default()
        }),
        pages: vec![],
    }
}

/// Empty command list should not panic or produce side effects.
#[test]
fn test_fill_correlation_id_empty_commands() {
    let mut commands: Vec<CommandBook> = vec![];
    fill_correlation_id(&mut commands, "corr-123");
    assert!(commands.is_empty());
}

/// Commands with empty correlation_id should receive the propagated value.
///
/// This is the primary use case: saga/PM produces commands without setting
/// correlation_id, and the framework fills it in from the triggering event.
#[test]
fn test_fill_correlation_id_fills_empty() {
    let mut commands = vec![make_command_with_correlation("orders", "")];
    fill_correlation_id(&mut commands, "corr-123");

    assert_eq!(
        commands[0].cover.as_ref().unwrap().correlation_id,
        "corr-123"
    );
}

/// Commands that already have a correlation_id should not be overwritten.
///
/// Sagas may explicitly set correlation_id when routing to a different
/// workflow context. The framework must respect explicit values.
#[test]
fn test_fill_correlation_id_preserves_existing() {
    let mut commands = vec![make_command_with_correlation("orders", "existing-corr")];
    fill_correlation_id(&mut commands, "new-corr");

    assert_eq!(
        commands[0].cover.as_ref().unwrap().correlation_id,
        "existing-corr"
    );
}

/// Mixed batch: fill empty, preserve existing.
///
/// Process managers may emit multiple commands to different domains.
/// Some may have explicit correlation_ids (e.g., spawning a new workflow),
/// while others should inherit the current workflow's correlation.
#[test]
fn test_fill_correlation_id_mixed() {
    let mut commands = vec![
        make_command_with_correlation("orders", ""),
        make_command_with_correlation("inventory", "existing"),
        make_command_with_correlation("fulfillment", ""),
    ];
    fill_correlation_id(&mut commands, "new-corr");

    assert_eq!(
        commands[0].cover.as_ref().unwrap().correlation_id,
        "new-corr"
    );
    assert_eq!(
        commands[1].cover.as_ref().unwrap().correlation_id,
        "existing"
    );
    assert_eq!(
        commands[2].cover.as_ref().unwrap().correlation_id,
        "new-corr"
    );
}

/// Commands without a cover should be skipped gracefully.
///
/// Defensive: malformed commands shouldn't crash the framework.
/// The router will reject them later with a proper error.
#[test]
fn test_fill_correlation_id_no_cover_skipped() {
    let mut commands = vec![
        make_command_with_correlation("orders", ""),
        CommandBook {
            cover: None,
            pages: vec![],
        },
    ];
    fill_correlation_id(&mut commands, "corr-123");

    assert_eq!(
        commands[0].cover.as_ref().unwrap().correlation_id,
        "corr-123"
    );
    assert!(commands[1].cover.is_none());
}
