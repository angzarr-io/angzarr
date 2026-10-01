# handlers/core

Core angzarr sidecar event handlers.

## Purpose

These handlers receive events from the event bus and forward them to client logic via gRPC. They are the bridge between the event bus infrastructure and user-defined client logic.

## Architecture

```
[Event Bus] --> [core handlers] --> [client logic]
                          |
                          v
                    (gRPC calls)
```

## Modules

- **projector.rs** - `ProjectorEventHandler`: Receives events from the bus and calls the client's ProjectorService.

- **saga.rs** - `SagaEventHandler`: Receives events from the bus and runs `orchestrate_saga`: calls the client's SagaService, delivers its commands to their aggregates' coordinators, and routes rejections back for compensation.

- **process_manager.rs** - `ProcessManagerEventHandler`: Receives correlated events from the bus and runs `orchestrate_pm`.

## Used By

- `angzarr-projector` sidecar binary
- `angzarr-saga` sidecar binary
- `angzarr-process-manager` sidecar binary

## See Also

- `services/aggregate.rs` - Command handler coordinator (receives commands, not events)
