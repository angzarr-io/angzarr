# Angzarr CLI Implementation Plan

## Overview

A unified `angzarr` CLI for interacting with running angzarr systems (distributed or standalone) and scaffolding new projects/components.

**Language**: Rust (direct proto type reuse, existing gRPC clients)

**Primary mode**: Distributed (gRPC to coordinators)

**Secondary mode**: Standalone (direct library access for local dev)

## Architecture

**Library-first design**: All functionality lives in `angzarr::ctl` library module. The CLI binary is a thin wrapper. This enables future graphical/web UIs to reuse the same logic.

```
┌─────────────────────────────────────────────────────────────────┐
│                       UI Layer (thin)                            │
├──────────────────┬──────────────────┬───────────────────────────┤
│  angzarr CLI     │  Web UI (future) │  Desktop GUI (future)     │
│  (clap + output) │  (axum/REST)     │  (tauri/egui)             │
└────────┬─────────┴────────┬─────────┴─────────────┬─────────────┘
         │                  │                       │
         ▼                  ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    angzarr::ctl (library)                        │
├─────────────────────────────────────────────────────────────────┤
│  Operations       │  Client          │  Types                   │
│  ─────────────    │  ──────────────  │  ────────────────────    │
│  events::query()  │  CtlClient       │  QueryResult             │
│  events::inject() │  - endpoint      │  InspectResult           │
│  command::send()  │  - config        │  EditionInfo             │
│  edition::list()  │  - gRPC clients  │  SnapshotInfo            │
│  inspect::state() │                  │  SystemStatus            │
│  snapshot::*()    │                  │  ... (UI-agnostic)       │
│  compensate()     │                  │                          │
│  status()         │                  │                          │
└─────────────────────────────────────────────────────────────────┘
         │                    │
         │ gRPC               │ gRPC
         ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ CommandHandler  │  │ EventQuery      │  │ MetaService     │
│ Coordinator     │  │ Service         │  │ (new)           │
└─────────────────┘  └─────────────────┘  └─────────────────┘
```

### Library Design Principles

1. **No terminal dependencies in library** - `angzarr::ctl` has no `colored`, `comfy-table`, or `dialoguer` dependencies
2. **Return structured types** - Operations return `Result<T, CtlError>` with rich typed results
3. **UI-agnostic** - No formatting, no prompts, no progress bars in library
4. **Async-first** - All operations are async for UI responsiveness
5. **Cancellable** - Operations accept `CancellationToken` for user interruption

## Command Tree

```
angzarr
├── new                                    # Code generation
│   ├── project <name>                     # Scaffold full project
│   ├── aggregate <domain> --lang <L>      # Generate aggregate
│   ├── saga <source> <target> --lang <L>  # Generate saga
│   ├── process-manager <name> --lang <L>  # Generate PM
│   └── projector <name> --lang <L>        # Generate projector
│
├── dev [--watch]                          # Run standalone mode
├── test [--domain <d>]                    # Run acceptance tests
│
├── command                                # Command operations
│   └── send <domain> <root-id> <file>     # Send command
│       [--edition <name>]
│       [--sync]
│
├── events                                 # Event operations
│   ├── query                              # Query events
│   │   --domain <d> [--root-id <id>]
│   │   [--correlation-id <id>]
│   │   [--edition <name>]
│   │   [--from-seq <n>] [--to-seq <n>]
│   ├── replay                             # Replay to component
│   │   --domain <d> --root-id <id>
│   │   --to <component>
│   ├── inject <domain> <root-id> <file>   # Inject fact
│   │   [--edition <name>]
│   ├── export --domain <d>                # Export events
│   │   [--format jsonl|parquet]
│   └── import <file>                      # Import events
│
├── compensate                             # Compensation
│   <domain> <root-id> <sequence>
│   [--correlation-id <id>]                # Compensate flow
│   [--dry-run]
│
├── edition                                # Timeline branching
│   ├── list [--domain <d>]                # List existing editions
│   ├── show <name>                        # Show edition metadata
│   ├── diff <name> --domain <d> --root-id <id>
│   ├── delete <name> [--domain <d>]
│   └── adopt <name> --confirm
│   # NOTE: No "create" - editions are created implicitly on first write
│
├── inspect <domain> <root-id>             # View aggregate state
│   [--edition <name>]
│   [--at-sequence <n>]
│
├── diff <domain> <root-id>                # Compare states
│   --seq <a> --seq <b>
│   [--format text|json|patch]
│
├── trace <correlation-id>                 # Follow correlation
│   [--live]                               # Stream mode
│
├── snapshot                               # Snapshot operations
│   ├── view <domain> <root-id>
│   ├── create <domain> <root-id>
│   ├── compact [--domain <d>] [--older-than <duration>]
│   ├── list <domain>
│   └── delete <domain> <root-id> <sequence>
│
├── schema                                 # Schema management
│   ├── register <proto-file>
│   ├── check-compat <old> <new>
│   └── upcaster run --domain <d> --from <v> --to <v>
│
├── status [bus|storage|components]        # Health overview
├── metrics                                # Growth/throughput
│   [--events|--throughput|--storage|--lag]
├── topology [--format text|dot]           # Component graph
│
├── dlq                                    # Dead letter queue
│   ├── list
│   ├── replay <id>
│   └── purge [--older-than <duration>]
│
├── repl [--domain <d>]                    # Interactive mode
└── version                                # Version info
```

## Phases

### Phase 1: Foundation + Existing Services

**Goal**: CLI scaffolding and commands that work with existing gRPC services.

**Duration**: ~2-3 weeks equivalent effort

#### 1.1 Library Scaffolding (angzarr::ctl)

- [ ] Create `src/ctl/` module structure
- [ ] `CtlClient` - gRPC connection management with endpoint discovery
- [ ] `CtlError` - error enum covering all failure modes
- [ ] Result types in `src/ctl/types.rs` (QueryResult, InspectResult, etc.)
- [ ] Proto-to-JSON decoder in `src/ctl/proto_decode.rs`
- [ ] Config loading in `src/ctl/config.rs`
- [ ] Create `src/ctl/ops/` with stub modules for each operation group

```rust
// src/ctl/mod.rs
pub mod client;
pub mod config;
pub mod error;
pub mod types;
pub mod proto_decode;
pub mod ops;
pub mod codegen;

pub use client::CtlClient;
pub use error::CtlError;
```

```rust
// src/ctl/client.rs
pub struct CtlClient {
    endpoint: String,
    event_query: EventQueryServiceClient<Channel>,
    command_handler: CommandHandlerCoordinatorServiceClient<Channel>,
    event_stream: EventStreamServiceClient<Channel>,
    // meta: MetaServiceClient<Channel>,  // Phase 2
}

impl CtlClient {
    pub async fn connect(endpoint: &str) -> Result<Self, CtlError>;
    pub async fn from_config(config_path: Option<&Path>) -> Result<Self, CtlError>;
}
```

#### 1.2 CLI Scaffolding (thin wrapper)

- [ ] Add `cli` feature flag to Cargo.toml
- [ ] Create `src/cli/` module structure
- [ ] Add clap dependency (feature-gated)
- [ ] Create `src/bin/angzarr.rs` entry point
- [ ] Output formatting module (table, json, colored) - CLI only
- [ ] Global flags: `--endpoint`, `--config`, `--output`

```rust
// src/bin/angzarr.rs
use angzarr::ctl::CtlClient;
use angzarr::cli::{output, Cli, Commands};
use clap::Parser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let client = CtlClient::from_args(&cli).await?;

    let result = match cli.command {
        Commands::Events(cmd) => cmd.execute(&client).await?,
        Commands::Inspect(cmd) => cmd.execute(&client).await?,
        // ...
    };

    output::render(&result, cli.output);
    Ok(())
}
```

#### 1.2 Commands Using Existing Services

**EventQueryService** (exists):
- [ ] `angzarr events query` - GetEventBook / GetEvents
- [ ] `angzarr inspect` - GetEventBook + client-side state reconstruction
- [ ] `angzarr trace` - Subscribe by correlation_id (uses EventStreamService)

**CommandHandlerCoordinatorService** (exists):
- [ ] `angzarr command send` - HandleCommand
- [ ] `angzarr events inject` - HandleEvent (fact injection)
- [ ] `angzarr compensate` - HandleCompensation

#### 1.3 Version Command

- [ ] `angzarr version` - CLI version + server version (health check)

#### 1.5 Tests

**Library tests (angzarr::ctl):**
- [ ] Unit tests for `CtlClient` connection handling
- [ ] Unit tests for config/endpoint discovery
- [ ] Unit tests for proto decoding (Any → JSON)
- [ ] Unit tests for result types serialization
- [ ] Integration tests against mock gRPC server

**CLI tests (feature-gated):**
- [ ] Unit tests for output formatting (table, json)
- [ ] Unit tests for clap argument parsing
- [ ] Snapshot tests for CLI output

---

### Phase 2: MetaService + New Commands

**Goal**: Server-side support for edition, snapshot, and status operations.

**Duration**: ~3-4 weeks equivalent effort

#### 2.1 MetaService Proto Definition

```protobuf
// proto/angzarr/meta.proto (extend existing)

service MetaService {
  // Edition management
  // NOTE: No CreateEdition - editions are created implicitly on first write
  //       (like git branches created on first commit)
  rpc ListEditions (ListEditionsRequest) returns (ListEditionsResponse);
  rpc GetEdition (GetEditionRequest) returns (EditionMetadata);
  rpc DeleteEdition (DeleteEditionRequest) returns (google.protobuf.Empty);
  rpc AdoptEdition (AdoptEditionRequest) returns (AdoptEditionResponse);

  // Snapshot operations
  rpc GetSnapshot (GetSnapshotRequest) returns (Snapshot);
  rpc ListSnapshots (ListSnapshotsRequest) returns (ListSnapshotsResponse);
  rpc CreateSnapshot (CreateSnapshotRequest) returns (Snapshot);
  rpc CompactSnapshots (CompactSnapshotsRequest) returns (CompactSnapshotsResponse);
  rpc DeleteSnapshot (DeleteSnapshotRequest) returns (google.protobuf.Empty);

  // System status
  rpc GetStatus (GetStatusRequest) returns (SystemStatus);
  rpc GetMetrics (GetMetricsRequest) returns (SystemMetrics);

  // DLQ operations
  rpc ListDlqItems (ListDlqRequest) returns (ListDlqResponse);
  rpc ReplayDlqItem (ReplayDlqRequest) returns (ReplayDlqResponse);
  rpc PurgeDlq (PurgeDlqRequest) returns (PurgeDlqResponse);
}

message EditionMetadata {
  string name = 1;
  string divergence_type = 2;      // "sequence" or "timestamp"
  string divergence_value = 3;     // sequence number or RFC3339 timestamp
  string created_at = 4;
  string description = 5;
  repeated string domains = 6;     // domains with events in this edition
}

message SystemStatus {
  string version = 1;
  repeated ComponentStatus components = 2;
  StorageStatus storage = 3;
  BusStatus bus = 4;
}

message ComponentStatus {
  string name = 1;
  string type = 2;                 // aggregate, saga, projector, pm
  string status = 3;               // healthy, degraded, unhealthy
  string last_event_at = 4;
  uint64 event_count = 5;
}

message SystemMetrics {
  uint64 total_events = 1;
  map<string, uint64> events_by_domain = 2;
  double events_per_second = 3;
  uint64 storage_bytes = 4;
  map<string, int64> consumer_lag = 5;
}
```

#### 2.2 MetaService Implementation

- [ ] Create `src/services/meta/mod.rs`
- [ ] Implement edition listing (scan storage for edition prefixes)
- [ ] Implement edition deletion (uses existing DeleteEditionEvents logic)
- [ ] Implement edition adoption (swap main timeline)
- [ ] Implement snapshot operations:
  - [ ] Add `list()` method to SnapshotStore trait (enumerate roots with snapshots)
  - [ ] Wrap SnapshotStore.get() for view
  - [ ] Wrap SnapshotStore.delete() for delete
  - [ ] Force snapshot creation (trigger via synthetic command? or direct put?)
- [ ] Implement status aggregation (health checks, component registry)
- [ ] Implement metrics collection (counters, storage stats)
- [ ] Implement DLQ operations (wrap DLQ publisher)

NOTE: Editions are implicit - no "create" needed. They come into existence on first write.

#### 2.3 Wire MetaService into Coordinators

- [ ] Add MetaService to angzarr-standalone
- [ ] Add MetaService to distributed coordinators
- [ ] Add MetaServiceClient to CLI

#### 2.4 CLI Commands Using MetaService

- [ ] `angzarr edition list`
- [ ] `angzarr edition show`
- [ ] `angzarr edition delete`
- [ ] `angzarr edition adopt`
- [ ] `angzarr edition diff` (client-side: query both timelines, diff states)
  # NOTE: No "edition create" - editions are implicit (created on first write)
- [ ] `angzarr snapshot view`
- [ ] `angzarr snapshot list`
- [ ] `angzarr snapshot create`
- [ ] `angzarr snapshot compact`
- [ ] `angzarr snapshot delete`
- [ ] `angzarr status`
- [ ] `angzarr metrics`
- [ ] `angzarr dlq list`
- [ ] `angzarr dlq replay`
- [ ] `angzarr dlq purge`

#### 2.5 Topology Command

- [ ] `angzarr topology` - Query topology service REST API
- [ ] Text format (ASCII graph)
- [ ] Dot format (GraphViz)

---

### Phase 3: Code Generation

**Goal**: Scaffold new projects and components in all supported languages.

**Duration**: ~2-3 weeks equivalent effort

#### 3.1 Template Engine

- [ ] Choose template engine (tera, handlebars, or simple string interpolation)
- [ ] Define template variables (domain, language, component type, etc.)
- [ ] Create template directory structure

```
src/cli/templates/
├── project/
│   ├── angzarr.yaml.tera
│   ├── skaffold.yaml.tera
│   └── helm/
├── rust/
│   ├── aggregate/
│   │   ├── Cargo.toml.tera
│   │   ├── src/
│   │   │   ├── main.rs.tera
│   │   │   ├── handler.rs.tera
│   │   │   └── state.rs.tera
│   │   └── proto/
│   │       └── {{domain}}.proto.tera
│   ├── saga/
│   ├── projector/
│   └── process_manager/
├── go/
│   ├── aggregate/
│   ├── saga/
│   └── ...
├── python/
├── java/
├── csharp/
└── cpp/
```

#### 3.2 Project Scaffolding

- [ ] `angzarr new project <name>`
  - Creates directory structure
  - Generates angzarr.yaml with placeholders
  - Generates skaffold.yaml
  - Generates helm chart skeleton
  - Initializes git repo

#### 3.3 Component Generation

- [ ] `angzarr new aggregate <domain> --lang rust`
- [ ] `angzarr new aggregate <domain> --lang go`
- [ ] `angzarr new aggregate <domain> --lang python`
- [ ] `angzarr new aggregate <domain> --lang java`
- [ ] `angzarr new aggregate <domain> --lang csharp`
- [ ] `angzarr new aggregate <domain> --lang cpp`

Same for saga, projector, process-manager.

Each generates:
- Proto file with placeholder commands/events
- Boilerplate handler code
- Build configuration (Cargo.toml, go.mod, pyproject.toml, etc.)
- Dockerfile
- Test scaffolding

#### 3.4 Proto Generation Hook

- [ ] After generating proto, optionally run proto compilation
- [ ] `--compile-proto` flag to auto-generate bindings

---

### Phase 4: Dev Experience + Polish

**Goal**: REPL, dev mode, and quality-of-life improvements.

**Duration**: ~2 weeks equivalent effort

#### 4.1 REPL

- [ ] Choose readline library (rustyline)
- [ ] Implement command parsing in REPL context
- [ ] Tab completion for:
  - Commands
  - Domain names (from discovery)
  - Event types (from descriptors)
  - Root IDs (recent history)
- [ ] Command history with persistence
- [ ] `.help`, `.exit`, `.clear` meta-commands

```
$ angzarr repl --domain order
angzarr:order> send CreateOrder {"customer_id": "123"}
✓ Command accepted, sequence: 1

angzarr:order> query 123
EventBook: order/123
  [1] OrderCreated { customer_id: "123", ... }

angzarr:order> state 123
OrderState {
  id: "123",
  status: "created",
  items: []
}

angzarr:order> .exit
```

#### 4.2 Dev Mode

- [ ] `angzarr dev` - wrapper around angzarr-standalone
- [ ] `angzarr dev --watch` - file watching + restart
- [ ] Hot reload detection (inotify/fsevents)
- [ ] Colored log output aggregation

#### 4.3 Test Runner

- [ ] `angzarr test` - discover and run Gherkin tests
- [ ] `angzarr test --domain order` - filter by domain
- [ ] Proper exit codes for CI

#### 4.4 Diff Command

- [ ] `angzarr diff <domain> <root-id> --seq 10 --seq 50`
- [ ] Compare states at two sequences
- [ ] Text diff output (similar to git diff)
- [ ] JSON patch format option

#### 4.5 Events Export/Import

- [ ] `angzarr events export --domain order --format jsonl`
- [ ] `angzarr events export --format parquet` (optional, adds dependency)
- [ ] `angzarr events import <file>` - replay events into system

#### 4.6 Schema Commands

- [ ] `angzarr schema register` - store proto descriptors
- [ ] `angzarr schema check-compat` - wire format compatibility check
- [ ] `angzarr schema upcaster run` - trigger upcaster pipeline

---

### Phase 5: Plugin Architecture (Design Only)

**Goal**: Plan extensibility without implementing.

#### 5.1 Design Document

- [ ] Plugin discovery mechanism (~/.angzarr/plugins/)
- [ ] Plugin manifest format (plugin.yaml)
- [ ] IPC protocol (JSON-RPC over stdin/stdout)
- [ ] Hook points (pre-command, post-command, custom subcommands)
- [ ] Security considerations

Document only - implementation deferred.

---

## File Structure

```
src/
├── ctl/                           # LIBRARY: UI-agnostic operations
│   ├── mod.rs                     # pub mod + CtlClient
│   ├── client.rs                  # CtlClient: gRPC connection management
│   ├── config.rs                  # Endpoint discovery, config loading
│   ├── error.rs                   # CtlError enum
│   ├── types.rs                   # Result types (QueryResult, EditionInfo, etc.)
│   ├── proto_decode.rs            # Any → serde_json::Value decoding
│   │
│   ├── ops/                       # Operations (one module per command group)
│   │   ├── mod.rs
│   │   ├── events.rs              # query, replay, inject, export, import
│   │   ├── command.rs             # send
│   │   ├── compensate.rs          # compensate
│   │   ├── edition.rs             # list, show, delete, adopt, diff
│   │   ├── inspect.rs             # state inspection
│   │   ├── trace.rs               # correlation tracing
│   │   ├── snapshot.rs            # view, list, create, compact, delete
│   │   ├── status.rs              # system status
│   │   ├── metrics.rs             # system metrics
│   │   ├── topology.rs            # component graph
│   │   ├── dlq.rs                 # list, replay, purge
│   │   └── schema.rs              # register, check-compat, upcaster
│   │
│   └── codegen/                   # Code generation (also library)
│       ├── mod.rs
│       ├── project.rs             # Project scaffolding
│       ├── aggregate.rs           # Aggregate generation
│       ├── saga.rs                # Saga generation
│       ├── projector.rs           # Projector generation
│       ├── process_manager.rs     # PM generation
│       └── templates/             # Embedded templates
│           ├── project/
│           ├── rust/
│           ├── go/
│           ├── python/
│           ├── java/
│           ├── csharp/
│           └── cpp/
│
├── bin/
│   └── angzarr.rs                 # CLI BINARY: thin wrapper
│
├── cli/                           # CLI-specific (terminal UI)
│   ├── mod.rs
│   ├── commands/                  # clap command definitions
│   │   ├── mod.rs
│   │   ├── new.rs
│   │   ├── events.rs
│   │   ├── command.rs
│   │   ├── edition.rs
│   │   ├── inspect.rs
│   │   ├── snapshot.rs
│   │   ├── compensate.rs
│   │   ├── status.rs
│   │   ├── topology.rs
│   │   ├── dlq.rs
│   │   ├── repl.rs
│   │   ├── dev.rs
│   │   ├── test.rs
│   │   └── version.rs
│   ├── output.rs                  # Table/JSON/colored formatting
│   ├── progress.rs                # Progress bars (indicatif)
│   └── prompt.rs                  # Interactive prompts (dialoguer)
│
├── services/
│   └── meta/                      # New MetaService (server-side)
│       ├── mod.rs
│       ├── edition.rs
│       ├── snapshot.rs
│       ├── status.rs
│       └── dlq.rs
└── ...
```

### Library API Example

```rust
// angzarr::ctl - library usage (from any UI)
use angzarr::ctl::{CtlClient, ops, types::QueryResult};

let client = CtlClient::connect("localhost:1310").await?;

// Query events - returns structured data, not formatted strings
let result: QueryResult = ops::events::query(&client, QueryParams {
    domain: "order".into(),
    root_id: Some(uuid),
    edition: None,
    from_seq: None,
    to_seq: None,
}).await?;

// Caller decides how to display
println!("{:?}", result.events);           // Debug
serde_json::to_string(&result)?;           // JSON API
render_table(&result);                      // CLI table
```

```rust
// CLI binary - thin wrapper
// src/bin/angzarr.rs
use angzarr::ctl::{CtlClient, ops};
use angzarr::cli::{output, commands};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let client = CtlClient::from_config(&cli).await?;

    match cli.command {
        Commands::Events(EventsCmd::Query(args)) => {
            let result = ops::events::query(&client, args.into()).await?;
            output::render(&result, cli.output_format);  // CLI formatting here
        }
        // ...
    }
}
```

## Dependencies

### Library Dependencies (angzarr::ctl)

These are part of the main angzarr crate, usable by any UI:

```toml
# Already in angzarr
tonic = "..."               # gRPC client
prost = "..."               # Proto types
prost-reflect = "..."       # Proto reflection for Any decoding
tokio = "..."               # Async runtime
tokio-util = "..."          # CancellationToken
serde = "..."               # Serialization
serde_json = "..."          # JSON for decoded protos
thiserror = "..."           # Error types

# New for ctl module
tera = "1"                  # Template engine (codegen)
```

### CLI-Only Dependencies

These are feature-gated behind `cli` feature, only needed for terminal binary:

```toml
[dependencies]
clap = { version = "4", features = ["derive", "env"], optional = true }
comfy-table = { version = "7", optional = true }
colored = { version = "2", optional = true }
dialoguer = { version = "0.11", optional = true }
indicatif = { version = "0.17", optional = true }
rustyline = { version = "14", optional = true }
directories = { version = "5", optional = true }

[features]
cli = ["clap", "comfy-table", "colored", "dialoguer", "indicatif", "rustyline", "directories"]

[[bin]]
name = "angzarr"
required-features = ["cli"]
```

### Future UI Dependencies (separate crates)

```toml
# angzarr-web (future)
axum = "..."
tower = "..."

# angzarr-gui (future)
tauri = "..."
# or egui = "..."
```

## Configuration

### CLI Config File (~/.angzarr/config.yaml)

```yaml
# Default endpoint for distributed mode
endpoint: "localhost:1310"

# Or discover from angzarr.yaml
config: "./angzarr.yaml"

# Output preferences
output:
  format: text              # text, json
  color: auto               # auto, always, never

# REPL settings
repl:
  history_file: ~/.angzarr/history
  history_size: 1000

# Proto descriptors for JSON decoding
descriptors:
  - ./proto/descriptor.bin
  - ~/.angzarr/descriptors/
```

### Endpoint Discovery Priority

1. `--endpoint` flag
2. `ANGZARR_ENDPOINT` env var
3. `~/.angzarr/config.yaml` endpoint
4. `--config` / `ANGZARR_CONFIG` → parse angzarr.yaml for coordinator addresses
5. Default: `localhost:1310`

## Testing Strategy

### Unit Tests

- Output formatting
- Proto decoding
- Template rendering
- Config parsing

### Integration Tests

- Commands against mock gRPC server
- REPL command parsing
- Code generation output validation

### E2E Tests

- Full CLI against angzarr-standalone
- Code generation → compile → deploy cycle

## Open Questions

1. **Descriptor distribution**: How do users get proto descriptors for JSON decoding?
   - Ship common descriptors with CLI?
   - Download from running system?
   - `angzarr schema pull` command?

2. **Multi-cluster**: Should CLI support multiple named clusters/contexts (like kubectl)?

3. **Authentication**: Any auth needed for distributed mode? mTLS? API keys?

4. **Edition storage**: Where is edition metadata stored?
   - Editions are implicit (created on first write to `{edition}.{domain}`)
   - Metadata (description, created_at) could be stored as:
     - Special event in a `_meta` domain?
     - Separate metadata table in storage?
     - Derived from scanning existing events?

5. **Adopt semantics**: What exactly happens on `edition adopt`? Rename? Copy? Tombstone old?

6. **Snapshot access**: No snapshot gRPC service, but partial coverage exists:
   - `SnapshotStore` trait is internal to coordinators
   - `EventQueryService` can return snapshots when `enable_snapshots=true`
   - Existing trait methods: `get()`, `get_at_seq()`, `put()`, `delete()`
   - **Missing**: `list(domain)` to enumerate aggregates with snapshots
   - Compact is implicit (transient snapshots cleaned on `put()`)

   CLI snapshot commands:
   | Command | Mechanism |
   |---------|-----------|
   | `snapshot view` | EventQueryService (enable_snapshots) or extend MetaService |
   | `snapshot list` | **Needs SnapshotStore.list()** + MetaService exposure |
   | `snapshot create` | Force snapshot via command? Or MetaService.CreateSnapshot |
   | `snapshot compact` | Implicit via put(); explicit compact = delete transient |
   | `snapshot delete` | MetaService wrapping SnapshotStore.delete() |

## Success Criteria

- [ ] Single `angzarr` binary handles all operations
- [ ] Works against distributed and standalone deployments
- [ ] Code generation produces working, tested boilerplate
- [ ] REPL provides productive interactive experience
- [ ] Proto events decode to readable JSON
- [ ] < 100ms latency for simple queries
- [ ] Comprehensive `--help` for all commands
