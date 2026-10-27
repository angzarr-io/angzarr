# handlers/core

Core angzarr sidecar event handlers.

## Purpose

These handlers receive events from the AMQP message bus and forward them to client logic coordinators via gRPC. They are the bridge between the event bus infrastructure and user-defined client logic.

## Architecture

```
[AMQP Event Bus] --> [core handlers] --> [client logic Coordinators]
                          |
                          v
                    (gRPC calls)
```

## Modules

- **projector.rs** - `ProjectorEventHandler`: Receives events from AMQP, forwards to ProjectorCoordinator services. Handles EventBook repair (fetching missing history) and publishes projector output back to AMQP for streaming.

## Used By

- `angzarr-projector` sidecar binary

The saga handler and the `angzarr-saga` binary are not yet ported to the
latest proto; restore them from 16db7169 as they are ported.

## See Also

- `services/command_handler.rs` - Command handler coordinator (receives commands, not events)
