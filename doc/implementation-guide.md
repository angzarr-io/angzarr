# Angzarr Implementation Guide

A comprehensive guide for mapping a problem space into aggregates and the full Angzarr CQRS/Event Sourcing flow.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Domain Modeling](#domain-modeling)
3. [Component Types](#component-types)
4. [Implementing Aggregates](#implementing-aggregates)
5. [Implementing Sagas](#implementing-sagas)
6. [Implementing Process Managers](#implementing-process-managers)
7. [Implementing Projectors](#implementing-projectors)
8. [Proto Schema Design](#proto-schema-design)
9. [Sequence Management](#sequence-management)
10. [Commands vs Facts](#commands-vs-facts)
11. [Cross-Domain Patterns](#cross-domain-patterns)
12. [Testing Patterns](#testing-patterns)
13. [Naming Conventions](#naming-conventions)

---

## Architecture Overview

Angzarr separates concerns using a sidecar pattern:

```
┌─────────────────────────────────────────────────────────────┐
│                      Your Business Logic                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │  Aggregate   │  │    Saga      │  │  Projector   │       │
│  │  (commands   │  │  (domain     │  │  (read       │       │
│  │   → events)  │  │  translator) │  │   models)    │       │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘       │
│         │ gRPC            │ gRPC            │ gRPC          │
└─────────┼─────────────────┼─────────────────┼───────────────┘
          ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────┐
│                   Angzarr Coordinator                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ Event Store  │  │ Message Bus  │  │ Orchestrator │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
└─────────────────────────────────────────────────────────────┘
```

**Key principle:** Business logic cares only about commands/events. The framework handles persistence, messaging, sequencing, and retries.

---

## Domain Modeling

### Step 1: Identify Bounded Contexts

Each bounded context becomes a **domain**. A domain has:
- One aggregate type (the source of truth)
- Its own event store partition
- Clear boundaries with other domains

**Example: Poker System**

| Domain | Aggregate Root | Responsibility |
|--------|---------------|----------------|
| `player` | Player ID (from email) | Identity, bankroll, table reservations |
| `table` | Table ID | Table state, player seating, hand lifecycle |
| `hand` | Hand ID | Cards, betting rounds, pot management |

### Step 2: Map Data Flow

Draw the event flow between domains:

```
                ┌──────────────────┐
                │     player       │
                │  (RegisterPlayer │
                │  DepositFunds)   │
                └────────┬─────────┘
                         │ FundsReserved
                         ▼
┌──────────────────┐    ┌──────────────────┐
│      table       │───▶│      hand        │
│  (StartHand)     │    │  (DealCards,     │
└────────┬─────────┘    │   TakeAction)    │
         │              └────────┬─────────┘
         │ HandEnded             │ HandComplete
         ▼                       ▼
     saga-table-player      saga-hand-table
         │                       │
         ▼                       ▼
   ReleaseFunds →           EndHand →
   player domain            table domain
```

### Step 3: Define Aggregate Boundaries

Each aggregate should:
- Own exactly one consistency boundary
- Accept commands, emit events
- Be the single source of truth for its state
- Never directly call other aggregates

**Questions to ask:**
1. "What is the invariant this aggregate protects?"
2. "Can this be eventually consistent with other data?"
3. "Who decides if this operation succeeds or fails?"

---

## Component Types

### Aggregate (Command Handler)
- **Purpose:** Accept commands, emit events
- **State:** Event-sourced, rebuilt from events
- **Naming:** `agg-{domain}`
- **Trait:** `CommandHandlerDomainHandler`

### Saga
- **Purpose:** Translate events from one domain to commands for another
- **State:** Stateless (pure translator)
- **Naming:** `saga-{source}-{target}`
- **Trait:** `SagaDomainHandler`

### Process Manager
- **Purpose:** Orchestrate workflows across multiple domains
- **State:** Stateful (its own event-sourced aggregate)
- **Naming:** `pmg-{workflow-name}`
- **Trait:** `ProcessManagerDomainHandler<S>`

### Projector
- **Purpose:** Build read models from events
- **State:** External (database, cache, etc.)
- **Naming:** `prj-{source}-{purpose}`
- **Trait:** `ProjectorDomainHandler`

---

## Implementing Aggregates

### Directory Structure

```
{domain}/agg/src/
├── lib.rs              # Module exports
├── main.rs             # Server entry point
├── handler.rs          # CommandHandlerDomainHandler impl
├── state.rs            # State struct + StateRouter
└── handlers/
    ├── mod.rs          # Handler exports
    ├── register.rs     # guard/validate/compute per command
    ├── deposit.rs
    └── ...
```

### State Definition

```rust
// state.rs
use std::sync::LazyLock;
use angzarr_client::StateRouter;

#[derive(Debug, Default, Clone)]
pub struct PlayerState {
    pub player_id: String,
    pub display_name: String,
    pub bankroll: i64,
    pub reserved_funds: i64,
    // Derived helper
}

impl PlayerState {
    pub fn exists(&self) -> bool {
        !self.player_id.is_empty()
    }

    pub fn available_balance(&self) -> i64 {
        self.bankroll - self.reserved_funds
    }
}

// Event appliers - pure functions
fn apply_registered(state: &mut PlayerState, event: PlayerRegistered) {
    state.player_id = format!("player_{}", event.email);
    state.display_name = event.display_name;
}

fn apply_deposited(state: &mut PlayerState, event: FundsDeposited) {
    if let Some(balance) = event.new_balance {
        state.bankroll = balance.amount;
    }
}

// StateRouter: register once, use for all replays
pub static STATE_ROUTER: LazyLock<StateRouter<PlayerState>> = LazyLock::new(|| {
    StateRouter::new()
        .on::<PlayerRegistered>(apply_registered)
        .on::<FundsDeposited>(apply_deposited)
        // ... more event types
});
```

### Command Handler Pattern

Each command handler follows **guard → validate → compute**:

```rust
// handlers/deposit.rs

/// Guard: Check preconditions against current state
fn guard(state: &PlayerState) -> CommandResult<()> {
    if !state.exists() {
        return Err(CommandRejectedError::new("Player does not exist"));
    }
    Ok(())
}

/// Validate: Check command input validity, extract/transform data
fn validate(cmd: &DepositFunds) -> CommandResult<i64> {
    let amount = cmd.amount.as_ref().map(|c| c.amount).unwrap_or(0);
    if amount <= 0 {
        return Err(CommandRejectedError::new("amount must be positive"));
    }
    Ok(amount)
}

/// Compute: Pure business logic, produces event
fn compute(cmd: &DepositFunds, state: &PlayerState, amount: i64) -> FundsDeposited {
    let new_balance = state.bankroll + amount;
    FundsDeposited {
        amount: cmd.amount.clone(),
        new_balance: Some(Currency { amount: new_balance, currency_code: "CHIPS".into() }),
        deposited_at: Some(angzarr_client::now()),
    }
}

/// Public handler: orchestrates guard → validate → compute
pub fn handle_deposit_funds(
    command_book: &CommandBook,
    command_any: &Any,
    state: &PlayerState,
    seq: u32,
) -> CommandResult<EventBook> {
    let cmd: DepositFunds = command_any.unpack()
        .map_err(|e| CommandRejectedError::new(format!("Decode error: {}", e)))?;

    guard(state)?;
    let amount = validate(&cmd)?;
    let event = compute(&cmd, state, amount);

    let event_any = pack_event(&event, "examples.FundsDeposited");
    Ok(new_event_book(command_book, seq, event_any))
}
```

### Handler Trait Implementation

```rust
// handler.rs
use angzarr_client::{
    dispatch_command, CommandHandlerDomainHandler, CommandResult,
    RejectionHandlerResponse, StateRouter,
};

#[derive(Clone)]
pub struct PlayerHandler;

impl CommandHandlerDomainHandler for PlayerHandler {
    type State = PlayerState;

    fn command_types(&self) -> Vec<String> {
        vec![
            "RegisterPlayer".into(),
            "DepositFunds".into(),
            "WithdrawFunds".into(),
        ]
    }

    fn state_router(&self) -> &StateRouter<Self::State> {
        &STATE_ROUTER
    }

    fn handle(
        &self,
        cmd: &CommandBook,
        payload: &Any,
        state: &Self::State,
        seq: u32,
    ) -> CommandResult<EventBook> {
        dispatch_command!(payload, cmd, state, seq, {
            "RegisterPlayer" => handlers::handle_register_player,
            "DepositFunds" => handlers::handle_deposit_funds,
            "WithdrawFunds" => handlers::handle_withdraw_funds,
        })
    }

    fn on_rejected(
        &self,
        notification: &Notification,
        state: &Self::State,
        target_domain: &str,
        target_command: &str,
    ) -> CommandResult<RejectionHandlerResponse> {
        // Handle rejections from commands we issued via sagas
        if target_domain == "table" && target_command.ends_with("JoinTable") {
            return handlers::handle_join_rejected(notification, state);
        }
        // Default: framework handles
        Ok(RejectionHandlerResponse::default())
    }
}
```

### Server Entry Point

```rust
// main.rs
use angzarr_client::{run_command_handler_server, CommandHandlerRouter};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let router = CommandHandlerRouter::new("player", "player", PlayerHandler::new());

    run_command_handler_server("player", 50001, router)
        .await
        .expect("Server failed");
}
```

---

## Implementing Sagas

### Purpose

Sagas are **stateless translators**: they receive events from a source domain and produce commands for target domains.

**Key characteristics:**
- No access to destination state
- Framework handles sequence assignment
- Framework handles delivery retries
- Preserves correlation ID for tracing

### Implementation

```rust
// saga-table-player/src/main.rs
use angzarr_client::{
    run_saga_server, CommandRejectedError, CommandResult,
    SagaDomainHandler, SagaHandlerResponse, SagaRouter, UnpackAny,
};

#[derive(Clone)]
struct TablePlayerSagaHandler;

impl SagaDomainHandler for TablePlayerSagaHandler {
    fn event_types(&self) -> Vec<String> {
        vec!["HandEnded".into()]
    }

    fn handle(&self, source: &EventBook, event: &Any) -> CommandResult<SagaHandlerResponse> {
        if event.type_url.ends_with("HandEnded") {
            return Self::handle_hand_ended(source, event);
        }
        Ok(SagaHandlerResponse::default())
    }
}

impl TablePlayerSagaHandler {
    fn handle_hand_ended(
        source: &EventBook,
        event_any: &Any,
    ) -> CommandResult<SagaHandlerResponse> {
        let event: HandEnded = event_any.unpack()?;

        // Preserve correlation ID from source
        let correlation_id = source
            .cover.as_ref()
            .map(|c| c.correlation_id.clone())
            .unwrap_or_default();

        // Create commands for each player in the hand
        let commands: Vec<CommandBook> = event.stack_changes.keys()
            .filter_map(|player_hex| {
                let player_root = hex::decode(player_hex).ok()?;

                let release_funds = ReleaseFunds {
                    table_root: event.hand_root.clone(),
                };

                Some(CommandBook {
                    cover: Some(Cover {
                        domain: "player".to_string(),
                        root: Some(Uuid { value: player_root }),
                        correlation_id: correlation_id.clone(),
                        ..Default::default()
                    }),
                    // NO explicit sequence - framework stamps via angzarr_deferred
                    pages: vec![CommandPage {
                        payload: Some(command_page::Payload::Command(
                            Any::from_msg(&release_funds).unwrap()
                        )),
                        ..Default::default()
                    }],
                })
            })
            .collect();

        Ok(SagaHandlerResponse { commands, events: vec![] })
    }
}

#[tokio::main]
async fn main() {
    let router = SagaRouter::new("saga-table-player", "table", TablePlayerSagaHandler);
    run_saga_server("saga-table-player", 50013, router).await.unwrap();
}
```

### Saga Decision Tree

```
When to use a Saga?
│
├─ Event in Domain A should trigger action in Domain B?
│   └─ YES → Use a saga
│
├─ Need to maintain state across the translation?
│   └─ YES → Use a Process Manager instead
│
├─ Need to query Domain B's state before translating?
│   └─ YES → Consider Process Manager or enriching source events
│
└─ Pure field mapping from event to command?
    └─ YES → Perfect saga use case
```

---

## Implementing Process Managers

### Purpose

Process Managers are **stateful coordinators** for complex workflows spanning multiple domains.

**Key characteristics:**
- Own aggregate state (correlation ID = aggregate root)
- Receive events from multiple domains
- Can query destination aggregate state
- Two-phase execution: Prepare → Handle

### When to Use

Use Process Manager when:
- Workflow state is NOT derivable from existing aggregates
- Need to query workflow status independently
- Complex timeout/scheduling logic
- Must react to events from MULTIPLE domains

### Implementation

```rust
// pmg-hand-flow/src/main.rs

#[derive(Default, Clone)]
pub struct HandFlowState {
    hand_root: Vec<u8>,
    phase: HandPhase,
    blinds_posted: u32,
}

#[derive(Default, PartialEq, Clone, Copy)]
pub enum HandPhase {
    #[default]
    AwaitingDeal,
    Dealing,
    Blinds,
    Betting,
    Complete,
}

struct HandFlowPmHandler;

impl ProcessManagerDomainHandler<HandFlowState> for HandFlowPmHandler {
    fn event_types(&self) -> Vec<String> {
        vec![
            "HandStarted".into(),
            "CardsDealt".into(),
            "BlindPosted".into(),
            "HandComplete".into(),
        ]
    }

    /// Declare which additional aggregates we need
    fn prepare(&self, _trigger: &EventBook, _state: &HandFlowState, event: &Any) -> Vec<Cover> {
        if event.type_url.ends_with("HandStarted") {
            if let Ok(evt) = event.unpack::<HandStarted>() {
                return vec![Cover {
                    domain: "hand".to_string(),
                    root: Some(Uuid { value: evt.hand_root }),
                    ..Default::default()
                }];
            }
        }
        vec![]
    }

    /// Execute with full context
    fn handle(
        &self,
        _trigger: &EventBook,
        state: &HandFlowState,
        event: &Any,
        _destinations: &[EventBook],
    ) -> CommandResult<ProcessManagerResponse> {
        let mut local_state = state.clone();

        if event.type_url.ends_with("HandStarted") {
            return self.handle_hand_started(&mut local_state, event);
        } else if event.type_url.ends_with("CardsDealt") {
            local_state.phase = HandPhase::Blinds;
        } else if event.type_url.ends_with("BlindPosted") {
            local_state.blinds_posted += 1;
            if local_state.blinds_posted >= 2 {
                local_state.phase = HandPhase::Betting;
            }
        } else if event.type_url.ends_with("HandComplete") {
            local_state.phase = HandPhase::Complete;
        }

        Ok(ProcessManagerResponse::default())
    }
}

#[tokio::main]
async fn main() {
    let router = ProcessManagerRouter::new(
        "pmg-hand-flow",
        "hand-flow",  // PM's own domain
        |_| HandFlowState::default(),
    )
    .domain("table", HandFlowPmHandler)   // Listen to table events
    .domain("hand", HandFlowPmHandler);   // Listen to hand events

    run_process_manager_server("pmg-hand-flow", 50391, router).await.unwrap();
}
```

---

## Implementing Projectors

### Purpose

Projectors build **read models** from events for query optimization.

### Implementation

```rust
// prj-output/src/main.rs

fn handle_events(events: &EventBook) -> Result<Projection, Status> {
    let cover = events.cover.as_ref();
    let domain = cover.map(|c| c.domain.as_str()).unwrap_or("");

    for page in &events.pages {
        let event_any = match &page.payload {
            Some(event_page::Payload::Event(e)) => e,
            _ => continue,
        };

        let type_name = event_any.type_url.rsplit('.').next().unwrap_or("");

        // Write to your read model (database, cache, file, etc.)
        match type_name {
            "PlayerRegistered" => {
                if let Ok(e) = PlayerRegistered::decode(&event_any.value[..]) {
                    // Insert into read model
                    db.insert_player(e.email, e.display_name)?;
                }
            }
            "FundsDeposited" => {
                if let Ok(e) = FundsDeposited::decode(&event_any.value[..]) {
                    // Update read model
                    let new_balance = e.new_balance.map(|b| b.amount).unwrap_or(0);
                    db.update_balance(player_id, new_balance)?;
                }
            }
            _ => {}
        }
    }

    Ok(Projection {
        cover: cover.cloned(),
        projector: "output".to_string(),
        sequence: events.next_sequence,
        projection: None,
    })
}
```

---

## Proto Schema Design

### Commands (Imperative, Present Tense)

```protobuf
// What should happen
message RegisterPlayer {
  string display_name = 1;
  string email = 2;
  PlayerType player_type = 3;
}

message DepositFunds {
  Currency amount = 1;
}
```

### Events (Past Tense, Self-Contained)

```protobuf
// What happened - include ALL derived data
message PlayerRegistered {
  string display_name = 1;
  string email = 2;
  PlayerType player_type = 3;
  google.protobuf.Timestamp registered_at = 5;
}

message FundsDeposited {
  Currency amount = 1;
  Currency new_balance = 2;  // Include derived values!
  google.protobuf.Timestamp deposited_at = 3;
}
```

### State (For Snapshots)

```protobuf
// Full aggregate state for replay optimization
message PlayerState {
  string player_id = 1;
  string display_name = 2;
  Currency bankroll = 6;
  Currency reserved_funds = 7;
  map<string, int64> table_reservations = 8;
}
```

### Design Rules

| Rule | Rationale |
|------|-----------|
| Events include absolute values | Projectors don't need to query |
| Use `bytes` for aggregate root IDs | Binary efficiency, opaque handling |
| Include timestamps in events | Audit trail, temporal queries |
| Share value types (`Currency`, `Card`) | Consistency across domains |
| Commands are minimal | Aggregates compute derived values |
| Events are denormalized | Future-proof, no schema joins |

---

## Sequence Management

### Optimistic Concurrency

Commands include expected sequence number:

```rust
CommandPage {
    header: Some(PageHeader {
        sequence_type: Some(Sequence(expected_seq)),
    }),
    // ...
}
```

### Merge Strategies

| Strategy | Behavior | Use When |
|----------|----------|----------|
| `MERGE_STRICT` | Fail on mismatch | Default, simple consistency |
| `MERGE_COMMUTATIVE` | Allow if fields don't overlap | High-contention aggregates |
| `MERGE_AGGREGATE_HANDLES` | Aggregate decides | Custom conflict resolution |
| `MERGE_MANUAL` | Send to DLQ | Human review required |

### Deferred Sequences

Sagas produce commands WITHOUT explicit sequences:

```rust
// Saga produces command
CommandPage {
    header: Some(PageHeader {
        sequence_type: Some(AngzarrDeferred(AngzarrDeferredSequence {
            source: Some(source_cover),  // Where to route rejections
            source_seq: triggering_event_seq,
        })),
    }),
    // ...
}
```

Framework stamps real sequence on delivery.

---

## Commands vs Facts

| Aspect | Command | Fact |
|--------|---------|------|
| **Sequence** | `sequence: u32` | `external_deferred` |
| **Validation** | Full business rules | Idempotency only |
| **Rejection** | Can be rejected | Cannot be rejected |
| **Origin** | Internal system | External system (Stripe, etc.) |

### Fact Injection

```rust
// External system sends fact via HandleEvent RPC
EventPage {
    header: Some(PageHeader {
        sequence_type: Some(ExternalDeferred(ExternalDeferredSequence {
            external_id: "pi_1234567890",  // Stripe payment ID
            description: "Stripe webhook",
        })),
    }),
    payload: Some(Payload::Event(payment_completed_any)),
    // ...
}
```

---

## Cross-Domain Patterns

### Saga Pattern (Event → Command)

```
Table Domain                    Player Domain
     │                               │
     │ HandEnded event               │
     │──────────────────────────────▶│
     │         saga-table-player     │
     │                               │ ReleaseFunds command
     │                               │◀──────────────────
     │                               │ FundsReleased event
```

### Process Manager Pattern (Multi-Domain Orchestration)

```
Table Domain         PM Domain           Hand Domain
     │                   │                    │
     │ HandStarted       │                    │
     │──────────────────▶│                    │
     │                   │ WorkflowStarted    │
     │                   │                    │
     │                   │ DealCards command  │
     │                   │───────────────────▶│
     │                   │                    │ CardsDealt
     │                   │◀───────────────────│
     │                   │ PhaseDeal complete │
```

### Enrichment at Source

**Anti-pattern:** Saga queries destination before translating.

**Correct pattern:** Source aggregate includes all needed data in events.

```protobuf
// BAD: Saga needs to query player state
message HandEnded {
  repeated string player_roots = 1;  // Just IDs
}

// GOOD: Source enriches event
message HandEnded {
  map<string, int64> stack_changes = 1;  // player_hex -> balance change
  bytes table_root = 2;
}
```

---

## Testing Patterns

### Unit Test (guard/validate/compute)

```rust
#[test]
fn test_deposit_increases_bankroll() {
    let state = PlayerState {
        player_id: "player_1".to_string(),
        bankroll: 1000,
        ..Default::default()
    };
    let cmd = DepositFunds {
        amount: Some(Currency { amount: 500, .. }),
    };

    let event = compute(&cmd, &state, 500);

    assert_eq!(event.new_balance.unwrap().amount, 1500);
}

#[test]
fn test_deposit_rejects_non_existent_player() {
    let state = PlayerState::default();
    let result = guard(&state);
    assert!(result.is_err());
}
```

### Integration Test (Full Flow)

```rust
#[tokio::test]
async fn test_register_then_deposit() {
    let runtime = RuntimeBuilder::builder()
        .with_sqlite_memory()
        .register_command_handler("player", PlayerHandler::new())
        .build()
        .await?;

    // Register player
    let response = runtime.execute_command(
        "player",
        player_root,
        RegisterPlayer { display_name: "Alice".into(), .. },
    ).await?;

    assert_eq!(response.events.pages.len(), 1);

    // Deposit funds
    let response = runtime.execute_command(
        "player",
        player_root,
        DepositFunds { amount: Some(Currency { amount: 1000, .. }) },
    ).await?;

    assert_eq!(response.events.pages.len(), 1);
}
```

### Mutation Testing

```bash
cargo mutants --in-place --timeout 120 -f src/handlers/deposit.rs -- --features "sqlite test-utils"
```

Target kill rates:
- Pure utilities: 70%+
- Validation/guard: 60-70%
- gRPC handlers: 30-40%

---

## Naming Conventions

| Component | Pattern | Example |
|-----------|---------|---------|
| Domain | lowercase, singular | `player`, `table`, `hand` |
| Aggregate | `agg-{domain}` | `agg-player` |
| Saga | `saga-{source}-{target}` | `saga-table-player` |
| Process Manager | `pmg-{workflow}` | `pmg-hand-flow` |
| Projector | `prj-{source}-{purpose}` | `prj-player-leaderboard` |

### Directory Layout

```
{lang}-examples/
├── {domain}/
│   ├── agg/              # Aggregate
│   ├── saga-{target}/    # Outbound saga
│   └── ...
├── pmg-{name}/           # Process managers (domain peers)
├── prj-{name}/           # Projectors (domain peers)
├── proto/                # Domain proto definitions
└── tests/
```

---

## Quick Reference

### Component Selection

```
Need to accept commands and emit events?
    → Aggregate

Need to translate events from A to commands for B?
    → Saga (stateless)

Need to coordinate workflow across multiple domains?
    → Process Manager (stateful)

Need to build a read model or trigger side effects?
    → Projector
```

### Handler Signature Quick Reference

```rust
// Aggregate
fn handle(&self, cmd: &CommandBook, payload: &Any, state: &S, seq: u32) -> CommandResult<EventBook>

// Saga
fn handle(&self, source: &EventBook, event: &Any) -> CommandResult<SagaHandlerResponse>

// Process Manager
fn prepare(&self, trigger: &EventBook, state: &S, event: &Any) -> Vec<Cover>
fn handle(&self, trigger: &EventBook, state: &S, event: &Any, destinations: &[EventBook]) -> CommandResult<ProcessManagerResponse>

// Projector
fn project(&self, events: &EventBook) -> Result<Projection, Box<dyn Error>>
```

### Response Types

```rust
// Saga
SagaHandlerResponse {
    commands: Vec<CommandBook>,  // Commands to other domains
    events: Vec<EventBook>,      // Facts to inject
}

// Process Manager
ProcessManagerResponse {
    commands: Vec<CommandBook>,      // Commands to other aggregates
    process_events: Option<EventBook>, // PM's own events
    facts: Vec<EventBook>,           // Facts to inject
}
```
