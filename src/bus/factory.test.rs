//! Tests for the self-registering bus factory.
//!
//! C02: two sidecar binaries (`angzarr-aggregate`, `angzarr-process-manager`)
//! used to hand-roll their own `match messaging_type { "amqp" => ..., _ =>
//! MockEventBus }` instead of calling `init_event_bus`. Because
//! `MockEventBus::publish` always returns `Ok`, an unrecognized or
//! unconfigured messaging type silently swallowed every published event —
//! no error, no log beyond a single `warn!`, no DLQ/retry trigger. Both
//! binaries now call `init_event_bus` directly (see
//! `src/bin/angzarr_aggregate.rs` and `src/bin/angzarr_process_manager.rs`),
//! so this is the one place left that decides whether an unresolvable
//! messaging type is a hard failure. These tests pin that: no backend
//! module is registered here for "channel" (`MessagingConfig`'s default
//! type) or for any made-up type, so both must surface as a startup error
//! instead of resolving to something that quietly discards events.
//!
//! These tests intentionally use messaging types with NO registered
//! backend (rather than "amqp"/"kafka"/etc.) so they are deterministic and
//! broker-independent regardless of which optional transport features are
//! compiled in for a given `cargo test` invocation.

use super::*;

/// `MessagingConfig::default()` sets `messaging_type` to "channel" (see
/// `src/bus/config.rs`), but no backend module in `src/bus/*` registers a
/// `BusBackend` for "channel" -- there is no channel/in-memory bus
/// implementation left in production code. An unconfigured deployment
/// must therefore hard-fail at startup rather than resolve to a
/// silently-succeeding fallback.
#[tokio::test]
async fn init_event_bus_rejects_default_channel_type() {
    let config = MessagingConfig::default();
    assert_eq!(
        config.messaging_type, "channel",
        "test assumes MessagingConfig::default() is still \"channel\"; if this \
         assumption changes, re-point the test at whatever the new default is"
    );

    let result = init_event_bus(&config, EventBusMode::Publisher).await;

    // `Arc<dyn EventBus>` isn't `Debug`, so `Result::expect_err` (which
    // requires `T: Debug`) doesn't type-check here; match explicitly.
    let err = match result {
        Ok(_) => panic!(
            "no backend is registered for \"channel\" -- init_event_bus must return \
             Err(UnknownType), not a fallback bus that silently swallows publishes"
        ),
        Err(e) => e,
    };
    let message = err.to_string();
    assert!(
        message.contains("channel"),
        "error must name the unresolved messaging type so operators can diagnose \
         a misconfiguration; got: {message}"
    );
}

/// Same hard-fail contract for an arbitrary unrecognized type string, in
/// every `EventBusMode` a caller might request. Guards against a fix that
/// only special-cases "channel" instead of genuinely falling through every
/// registered backend and rejecting non-matches.
#[tokio::test]
async fn init_event_bus_rejects_arbitrary_unknown_type() {
    let config = MessagingConfig {
        messaging_type: "definitely-not-a-registered-backend".to_string(),
        ..Default::default()
    };

    for mode in [
        EventBusMode::Publisher,
        EventBusMode::SubscriberAll {
            queue: "q".to_string(),
        },
        EventBusMode::Subscriber {
            queue: "q".to_string(),
            domain: "orders".to_string(),
        },
    ] {
        let result = init_event_bus(&config, mode.clone()).await;
        assert!(
            result.is_err(),
            "mode {mode:?}: unregistered messaging type must fail startup, not \
             silently succeed"
        );
    }
}

/// The error variant itself must be `BusError::UnknownType` carrying the
/// configured type -- not merely "some error" -- so callers (and DLQ /
/// operator tooling) can distinguish "nothing registered for this type"
/// from a backend-specific connection failure.
#[tokio::test]
async fn init_event_bus_unknown_type_error_names_the_configured_type() {
    let config = MessagingConfig {
        messaging_type: "carrier-pigeon".to_string(),
        ..Default::default()
    };

    let result = init_event_bus(&config, EventBusMode::Publisher).await;
    let err = match result {
        Ok(_) => panic!("carrier-pigeon has no registered backend"),
        Err(e) => e,
    };

    // init_event_bus erases to Box<dyn Error + Send + Sync>; downcast back
    // to BusError to assert the concrete variant, not just the message text.
    let bus_err = err
        .downcast_ref::<BusError>()
        .expect("error must be a BusError, not some other error type");
    assert!(
        matches!(bus_err, BusError::UnknownType(t) if t == "carrier-pigeon"),
        "expected BusError::UnknownType(\"carrier-pigeon\"), got: {bus_err:?}"
    );
}
