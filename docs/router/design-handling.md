# Design: rules version, conflict resolution, command upcasting and router-owned state

> **Historical.** Superseded whole by the proto rewrite (decisions 14.78
> to 14.109): the handler layer, one Handle, success-only answers, typed
> refusals only, Emit, and no conflict concept (14.92). See
> [architecture.md](architecture.md) for the router as it is.
>
> **Earlier: superseded in most of its detail by decisions 14.39, 14.40 and 14.42
> (the router keeps dispatch; everything else is the coordinator's):** the
> router no longer upcasts (no `UpcasterDispatch`, no command chains, no
> claims), skips no page a snapshot covers (one state codec), admits
> nothing, builds no descriptor (`component_descriptor()` and the
> registration hello below are gone; `Router::component_descriptor`
> answers the one it was given), checks no policy command, builds no
> refusal fact, guards no envelope, and judges no conflict: it reports
> the nonce or the unit sets, and the coordinator refuses
> `COORDINATOR_REASON_CONFLICT_UNRESOLVED` and `COORDINATOR_REASON_CONFLICT_OVERLAP`. router-ffi, its C ABI and
> its router-owned state are deleted. The current contract is
> docs/architecture.md; this note is the record of how it got there.

> **Superseded in part by decision 14.37 (errors are a key and an argument
> map):** every error code, status and message below is historical. A
> failing fold is `ROUTER_REASON_FOLD_FAILED` / `ROUTER_REASON_FOLD_FAILED_EMITTED` with the fold's own
> failure as `cause` (no `fold_message`; `FoldFailure` carries a `cause`, not
> a `message`); `DUPLICATE_REGISTRATION` is `ROUTER_REASON_DUPLICATE_COMPONENT` or
> `DUPLICATE_UPCASTER_CHAIN`; an unknown type is `ROUTER_REASON_UNKNOWN_COMMAND_TYPE`; the
> router's own malformed-input and misconfiguration errors are INTERNAL. The
> current contract is docs/architecture.md's Errors row.

> **Superseded in part on the POC line (spec 636b313, decision 14.31):** a
> process manager is a domain, so every passage below about process-manager
> triggers, `ProcessManagerDispatch`, `ProcessManagerHandler`,
> `ProcessManagerHandleResponse`/`process_state` and their C-ABI
> descriptors describes code deleted on `poc/dispatch`. Kept as the record
> of what the distributed side (router-ffi, the bindings) still implements.

**Status:**
- **Implemented:** conflict resolution, command upcasting, the rules version
  on every response, and claims for upcaster source types at registration.
- **Implemented:** router-owned state across the C ABI for aggregates,
  process managers and projectors, the retirement of `host_ctx`, and the
  registration hello (`component_descriptor()`), per the owner rulings
  recorded under "Registration and projectors" below; C-0535 is bound.
- **Implemented:** fold failures everywhere are INTERNAL and typed, and the
  native `Router` gathers registration hellos, per the owner rulings recorded
  under "Fold failures and the native Router" below.
- **Implemented:** one component per router (owner ruling, 2026-10-20;
  spec 825311f, decisions 14.19–14.23), recorded under "One component per
  router" below. It supersedes every multi-component passage here.
- **Open:** how the rules version is sourced (the owner intends the binary's
  full build version, emitted by versionator).

Snapshot selection is the coordinator's job (decision 14.1). The router treats
a delivered snapshot as the initial state and never judges it. ABI_VERSION stays
at 4.

## Spec

Source: angzarr-project `spec/20261019-draft` (9a84005).

- **Protos:**
  - `command_handler.proto`: `CommandHandlerService.Handle`,
    `CommandHandlerResponse`.
  - `types.proto`: `Handling`, `ConflictResolution`, `ContextualCommand`,
    `ComponentDescriptor.rules_version`.
  - `options.proto`: the `conflict_units` region.
  - `upcaster.proto`.
- **Decisions:** 12.6, 12.7, 12.11, 14.1, 14.2.
- **Backlog:** RV-2, VC-1, NC-1, CR-1.
- **Behaviour:** the handler-contract tier,
  `features/handler-contract/handling.feature` (C-0533..C-0550). This repo's
  conformance suite binds those scenarios by id.

## What `Handle` does, in order

1. **Envelope guards** run first.
2. **Upcast.** The payload of either arm (command or fact) runs through the
   aggregate's upcaster chains, by type URL (VC-1).
3. **Dispatch.** The body arm and the upcast type URL pick the handler. If no
   handler takes the type, the aggregate answers INVALID_ARGUMENT. The message
   names the type URL as it was sent; the extras carry `upcast_type_url`.
4. **Conflicts** (Command arm only):
   - **STRICT:** no extra step.
   - **AGGREGATE_HANDLES:** the handler receives the window and nonce. If it
     does not call `resolve_conflicts` with exactly that nonce, the command
     fails `COORDINATOR_REASON_CONFLICT_UNRESOLVED` (ABORTED). An empty `resolved_nonce` counts
     as unresolved.
   - **COMMUTATIVE:** the router runs these steps:
     1. Fold to the expected sequence and capture.
     2. Fold the window and capture the head.
     3. Decide on the head, then restore the head.
     4. Fold the command's events and capture.
     5. If `changed(expected, head)` and `changed(head, after)` overlap, fail
        with `COORDINATOR_REASON_CONFLICT_OVERLAP` (ABORTED).

     If no `changed` is registered, the command fails
     `ROUTER_REASON_CONFLICT_RESOLUTION_UNSUPPORTED` (INVALID_ARGUMENT). If an emitted event
     fails to fold, the error is INTERNAL.
5. **Rules version.** The router always sets the binary's declared version on
   every aggregate and PM response, overwriting whatever a handler set, even
   when the PM did not consume the trigger.

### Claims

A router hosts one aggregate, so there is nothing to claim across
aggregates. At build (`AggregateDispatch::check_claims`), an old type its
upcaster chains accept (`PayloadUpcaster::source_types`) that is also one of
its current command types is `DUPLICATE_COMMAND_HANDLER`.

## Rust API

Native aggregates keep typed state.

| Symbol | Purpose |
|---|---|
| `with_rules_version` (aggregate and PM); `ProcessManagerDispatch::rules_version` | The binary's declared version, reported on every response. |
| `with_commutative(changed)` (`S: Clone`); `with_commutative_via(capture, restore, changed)` | Turns on COMMUTATIVE resolution. `_via` stays public by owner ruling. |
| `with_command_upcaster(impl PayloadUpcaster)` | Adds an upcaster chain. Its `source_types` must not be current command types (checked at build). |
| `CommandContext.{expected_sequence, conflict_resolution, conflict}`, `aggregate::Conflict` | The resolved handling, plus the window and nonce under AGGREGATE_HANDLES. |
| `CommandHandler::upcast_types`, `CommandHandler::check_claims` | The old types an aggregate accepts, and the build-time check above. Defaults: empty, and nothing to check. |
| `Rebuilder::with_fold(fold)` | A batched fold, `fold(&mut S, &[EventPage], cover)`, called once over every uncovered page. It replaces per-type appliers. The C ABI uses it over `prost_types::Any` state. |
| `conflict::overlap` | The overlap rule. |
| Error codes | `COORDINATOR_REASON_CONFLICT_UNRESOLVED` and `COORDINATOR_REASON_CONFLICT_OVERLAP` (both ABORTED); `ROUTER_REASON_CONFLICT_RESOLUTION_UNSUPPORTED` (INVALID_ARGUMENT). |

## C ABI: router-owned state (ABI 4, language-neutral)

The router holds the state of each C-ABI aggregate and process manager as a
serialized `google.protobuf.Any`. The spec requires state to be a protobuf
message, and its type URL is registered. Every host callback is stateless.

- **Initial state:** the delivered snapshot's `state`. Pages at or below the
  snapshot's sequence count as covered. With no snapshot, the initial state is
  `Any { type_url: "/" + state_type_url, value: [] }`, which is the message's
  default.
- **fold:**
  - Input: `FoldCall { Any state = 1; repeated EventPage pages = 2; Cover cover = 3; }`.
  - Output: the new state, as a serialized `Any`.
  - The router sends one call per rebuild, carrying every uncovered page.
  - `STATUS_OK_EMPTY` means the state is unchanged.
  - A failure answers a typed `FoldFailure` (see "Fold failures and the
    native Router" below); the router never calls the fold again.
  - An undecodable answer is INTERNAL too.
- **decide** (one callback per command type): the payload is the command; the
  aux is `CommandContextAux`, which now carries `state = 8`. The callback
  returns `CommandOutput { events, resolved_nonce }` and no state.
- **fact** (one callback per fact type): the payload is the fact; the aux is
  `CommandContextAux`, including its `state`. The callback returns
  `FactRecord`.
- **changed:** takes `ChangedCall { before, after }` (two state Anys) and
  returns `ChangedUnits { units }`. COMMUTATIVE copies state bytes, so it needs
  neither a state packer nor a snapshot loader.
- **PM event** (one callback per (input domain, type)): the aux is
  `PmEventAux { trigger_cover = 2; state = 3 }`. The callback returns
  `ProcessManagerHandleResponse`.

### `abi.proto` changes

| Message | Added | Removed and reserved |
|---|---|---|
| `AggregateDescriptor` | `state_type_url = 11`, `fold_callback_id = 12` | `appliers = 4`, `snapshot_callback_id = 6` (both were published in ABI 3) |
| `ProcessManagerDescriptor` | `state_type_url = 10`, `fold_callback_id = 11` | `appliers = 3`, `snapshot_callback_id = 4` |
| `CommandContextAux` | `state = 8` | none |
| `PmEventAux` | `state = 3` | none |
| `FoldCall` | new message | none |

Some removals carry no reservation because those fields were never published:

- `pack_state_callback_id = 12` was added and dropped within this ABI 4 round,
  so field 12 now holds `fold_callback_id`.
- Field 11 was never used before `state_type_url`.
- `INCOMPLETE_COMMUTATIVE` is gone.

`ProjectorEventAux` is still the aux for projector folds.

**What a binding does:** it registers a fold, its deciders, its fact handlers
and `changed` against the state message. It keeps no session state for
aggregates or PMs. Projectors are unchanged; see Open.

## Rejected alternatives

- **A re-entrant `resolve_conflicts` entry point.** The acknowledgement travels
  as data in `CommandOutput` instead.
- **A COMMUTATIVE resolver in each binding.** The overlap rule is implemented
  once, in Rust.
- **Per-event fold callbacks across the C ABI.** The router makes one batched
  call per rebuild.
- **decide returning a state.** decide stays pure; the router only folds
  events.
- **Trial upcasting at dispatch.** Replaced by claims at registration.

## Rulings applied

- **Stale results:** two codes, both ABORTED.
- **COMMUTATIVE without `changed`:** `ROUTER_REASON_CONFLICT_RESOLUTION_UNSUPPORTED`.
- **Upcasting:** applies to both arms. A refusal names the sent type, and the
  type a chain produced goes in the extras.
- **Old types:** claimed at registration.
- **Rules version:** always the declared one, never the handler's. PMs on one
  host share it.
- **Accepted:**
  - envelope guards run before upcasting;
  - an empty `resolved_nonce` means unresolved;
  - a COMMUTATIVE fold failure is INTERNAL;
  - any host fold failure is INTERNAL, naming the failing event (above).
- **State across the C ABI:** router-owned.
- **Snapshot selection:** the coordinator's (decision 14.1).

## Registration and projectors

These follow the owner rulings on the two open items, against spec b412af8 (now pinned at 619933c, which changes nothing the
router does).

### Registration hello: `component_descriptor()`

The router builds the spec's `ComponentDescriptor` once, in Rust, for every
language. Hosts only send it.

- **Fields.**
  - `name`.
  - `kind`: AGGREGATE, SAGA, PROCESS_MANAGER or PROJECTOR.
  - `inputs`: the domains whose events the component consumes, one
    `Target` per domain, deduplicated and sorted:
    - a saga: its input domain;
    - a process manager: its source domains;
    - a projector: its declared domains, or `*` when it declares none;
    - an aggregate: none, because it consumes commands and no events.
  - `rules_version`.
  - `state`: set for aggregates and process managers only.
    - `type_url` is the state type the component's `Rebuilder` declares (see
      "Fold failures and the native Router": the rebuilder now owns it).
    - `options.conflict_resolutions` is the full resolved list in enum-number
      order. The base is STRICT and AGGREGATE_HANDLES; COMMUTATIVE is added
      first when a `changed()` is registered (`with_commutative`).
- **Refusal.** A descriptor with an empty `rules_version` is refused
  `INVALID_ARGUMENT` (`COORDINATOR_REASON_MISSING_RULES_VERSION`), mirroring the coordinator.
  The ErrorInfo metadata names the `component`.
  Every kind needs one, so sagas and projectors gain `with_rules_version`
  too.
- **Rust API.** `component_descriptor()` on every dispatch table (superseded
  in part below: the state type now comes from the rebuilder, so the
  `S: prost::Name` bound and the `_with_state` variants are gone).
- **C ABI.**
  - Entry point: `angzarr_router_component_descriptor(r, out)` (singular
    since round 10). It fills `out` with a serialized
    `io.angzarr.v1.ComponentDescriptor` (the spec's
    `ComponentService.Describe` answer, spec 825311f), or with a status: the
    component's refusal, or `ROUTER_REASON_NO_HANDLERS_REGISTERED` before one registers.
  - The saga and projector descriptors gain `rules_version`.
- **Binding.** C-0535 and C-0565 (describing the process gives its one
  component, spec 825311f) bind against a native `Router` hosting the order
  aggregate.

### Router-owned projectors

Projectors now follow the aggregate and process-manager model. Each delivery
starts from the projection message's default, `Any { type_url: "/" +
projection_type_url, value: [] }`.

- For a declared domain, one stateless host fold, `FoldCall { state:
  projection, pages, cover }`, returns the new projection `Any`. An undeclared
  domain folds nothing.
- The router answers the `Projection` itself, so the host needs no finish
  step:
  - `cover`: the book's cover;
  - `projector`: the projector's name;
  - `projection`: the folded `Any`;
  - `next_sequence`: the last projected page's sequence + 1, or the book's
    `next_sequence` when it has no pages (types.proto, `Projection`).
- **Core.**
  - `ProjectorDispatch::with_fold(fold)` is the batched fold, used in place of
    the per-type folds.
  - `ProjectorDispatch::finish` stays for native projectors.

### `host_ctx` retired

With aggregates, process managers and projectors all router-owned, bindings
keep no state of any kind.

- **Removed:**
  - the per-dispatch `host_ctx` parameter, from every
    `angzarr_router_dispatch*` entry point and from the host callback
    `AngzarrCb`;
  - the thread-local session pointer behind it.
- **Unchanged:** a callback is still selected by its `callback_id`.

### `abi.proto` changes in this step

| Message | Added | Removed |
|---|---|---|
| `ProjectorDescriptor` | `projection_type_url = 6`, `fold_callback_id = 7`, `rules_version = 8` | `events = 3` and `finish_callback_id = 4`, both reserved because ABI 3 published them |
| `SagaDescriptor` | `rules_version = 6` | none |
| `ComponentDescriptors` | new message (round 5; replaced in round 7 by the spec's `io.angzarr.v1.ComponentDescriptors`, the same wire shape, so abi.proto no longer defines it) | none |
| `ProjectorEventAux` | none | the whole message, which nothing uses any more |

## Fold failures and the native Router

Owner rulings of 2026-10-19 (round 6). They replace the refold-per-page
search of round 5, which is deleted.

### Every failing fold is a handler bug

Any fold of events that fails answers `ROUTER_REASON_UNHANDLED_HANDLER_ERROR` (INTERNAL):
the native per-event appliers, the C-ABI aggregate and process-manager folds,
and the C-ABI projector fold (which no longer passes the host's status
through), and the native projector's per-event folds.

- **Message.** `the state fold failed on an event it had accepted` for
  persisted events (a rebuild, a COMMUTATIVE window). COMMUTATIVE's fold of
  the command's own new events, not yet persisted, says `the state fold
  failed on an event this command emitted`.
- **ErrorInfo metadata.**
  - When the failing page is known: `sequence` and `type_url` of that page.
  - When it is not (no failure output, or an index outside the batch):
    `first_sequence` and `last_sequence` of the batch.
  - Always `state_type_url` (for a projector, its projection type), empty
    when the component declared none.
  - `fold_message`, when the fold gave one (a native applier's error text,
    or `FoldFailure.message`).
- **`ROUTER_REASON_PERSISTED_EVENT_CORRUPT`** keeps one producer: a snapshot whose state
  the native snapshot loader cannot load. That is not a fold of events, so
  the code stays (DATA_LOSS) for it alone.

### The typed failure across the C ABI

`abi.proto` gains `FoldFailure { optional uint32 page_index = 1; string
message = 2; }`. A fold callback (aggregate, process manager, projector)
that fails fills `out` with it: `page_index` is the failing page's index
within the batch the router sent, filled by the host's fold loop from the
one batched call. `page_index` is `optional` so that index 0 is
distinguishable from an empty output. The router maps the index to that
page; missing output, undecodable output or an index outside the batch
reports the batch's sequence range. The router never calls the fold again.

### The state type lives on the rebuilder

The fold metadata needs the state type at rebuild time, and the
registration hello needs it too, so the `Rebuilder` owns it:

- `Rebuilder::of_message()` (for `S: prost::Name + Default`) starts from
  `S::default()` and takes the type from the prost name, so nobody declares
  the string by hand;
- `Rebuilder::with_state_type(full_name)` declares it for state held as an
  `Any` (the C ABI);
- `Rebuilder::new(factory)` declares none.

A batched fold (`Rebuilder::with_fold`, `ProjectorDispatch::with_fold`)
returns `Result<(), FoldFailure>` (core type: `page_index: Option<usize>`,
`message`), and the rebuilder or projector turns it into the coded error.
`ProjectorDispatch::with_projection_type(full_name)` declares a projector's
projection type.

`component_descriptor()` on aggregates and process managers takes the state
type from the rebuilder; a component that declared none is refused
`COORDINATOR_REASON_MISSING_STATE_TYPE` (INVALID_ARGUMENT, metadata `component`), since the
spec sets `StateDescriptor` for both kinds.

### `Router::component_descriptor()`

`CommandHandler`, `ProcessManagerHandler` and `ProjectorHandler` carry
`component_descriptor()`, implemented by each dispatch table (sagas are held
as `SagaDispatch` and use its own). `Router::component_descriptor()` (singular
since round 10) answers the one component's hello, as the C ABI's
`angzarr_router_component_descriptor` does.

### `abi.proto` changes in round 6

| Message | Added | Removed |
|---|---|---|
| `FoldFailure` | new message: `optional uint32 page_index = 1`, `string message = 2` | none |

## The post-command snapshot (round 7)

Owner ruling: the router answers the component's state with EVERY response.
Business code never decides when, and there is no request flag.

- **Aggregates.** `CommandHandlerResponse.events.snapshot.state` is the state
  after the answer's events: the head with the emitted events (or a fact's
  record) folded in. A command emitting nothing still answers the current
  state. A rejection is a gRPC error status with no response body
  (command_handler.proto), so it carries none.
- **Process managers.** On every trigger the PM handles, its state after its
  process events is `ProcessManagerHandleResponse.process_state.state`
  (field 5; decision 14.14, C-0571, spec eb09fbd); the process-event books
  carry payloads only. A trigger the PM does not handle (outside its sources,
  or an undeclared type) answers nothing, as before.
- **Co-resident PMs** do not exist: a router hosts one component (round 10),
  so a PM's answer is its own.
- **Unset:** `snapshot.sequence` and `snapshot.rules_version`, which the
  coordinator stamps.
- **The image.** Native state: the prost-encoded message
  (`Rebuilder::of_message`); `Rebuilder::with_state_image` declares another.
  Across the C ABI: the state bytes the router holds. A rebuilder with no
  image (`Rebuilder::new` alone) answers no snapshot.
- **Emitted events fold as they will be persisted.** The fold sees each
  emitted page at the head's next sequence onward and with the meta the
  coordinator will stamp (`CommandContext::event_meta`, built from the
  `ContextualCommand`), so the snapshot equals what a rebuild folds once the
  coordinator stamps them, and an applier reads the same meta at decide time
  and on replay; the answered pages stay unstamped. Across the C ABI this is one more host fold call per
  answer. A failing fold of an emitted event is INTERNAL with the "emitted"
  message, as COMMUTATIVE's already was.

## One component per router (round 10)

Owner ruling, 2026-10-20 (spec 825311f, decisions 14.19–14.23): one
coordinator, one component, one business process. A component is one
aggregate, saga, process manager or projector; its upcaster chain is part of
it, not a component of its own. Nothing hosts several. (A saga or PM emitting
commands to several domains, or a PM or projector with several input
domains, stays.)

- **Native `Router`.** `RouterBuilder` takes one component (and, optionally,
  its upcaster chain); a second component of any kind, or a second chain, is
  `DUPLICATE_REGISTRATION`; none is `ROUTER_REASON_NO_HANDLERS_REGISTERED`. The router
  dispatches to that component: commands and facts to the aggregate, a saga
  source (from the saga's input domain) to the saga, a PM request to the PM,
  an EventBook to the projector (`dispatch_projector`, one `Projection`), and
  `upcast` through the chain for its domain (otherwise pass-through).
  `Router::component_descriptor()` is singular.
- **C ABI.** `FfiRouter` holds one core `Router`; a second `register_*` of any
  kind is `DUPLICATE_REGISTRATION`. `angzarr_router_component_descriptor`
  replaces the plural entry point and answers one `ComponentDescriptor`.
  ABI_VERSION stays 4 (unreleased).
- **Deleted:** `CommandHandlers` (the cross-aggregate claims table, `sole`,
  `serves`, first-handler-of-the-domain fallbacks), `MIXED_HANDLER_KINDS`,
  saga fan-out and merge, chaining across several upcaster components,
  `dispatch_projectors`, `ProcessManagerRoute`, `select_process_managers`,
  `merge_responses`, `ProcessManagerHandler::rules_version`,
  `CommandHandler::{domain, fact_types}`, and the co-resident
  AuditProcessManager fixture. `DUPLICATE_COMMAND_HANDLER` keeps only its
  within-aggregate meaning.

## One destination per saga (round 11)

Decision 14.24, SG-1, spec 9a84005: a saga translates between exactly two
domains, its one source (`input_domain`) and its one destination
(`output_domain`); `output_domains` is PROCESS_MANAGER only.

- **Core.** `SagaDispatch::new(name, input_domain, output_domain)` takes the
  one destination (it took a list of target domains), read back with
  `output_domain()`. An answer holding a command whose cover names any other
  domain (an absent or empty cover included) is refused
  `SAGA_COMMAND_OUTSIDE_DESTINATION` (INTERNAL), with no commands, naming
  `expected_domain` and `actual_domain` in the ErrorInfo metadata (C-0583).
- **C ABI.** `SagaDescriptor.output_domain = 7`; `target_domains = 3` is
  reserved (ABI 3 published it). ABI_VERSION stays 4.
- **Registration refusals name the component already hosted** (C-0580):
  `DUPLICATE_REGISTRATION` carries metadata `component` and
  `component_kind` (e.g. `AGGREGATE`). The component traits gained `name()`
  for it.

## Open

Choices no ruling covers, reported for sign-off:

- **`fold_message`.** The fold's own text rides in ErrorInfo metadata
  `fold_message`; the coded message stays the fixed cross-language text.
- **Range keys.** `first_sequence` and `last_sequence`.
- **`COORDINATOR_REASON_MISSING_STATE_TYPE`.** A native aggregate or process manager whose
  rebuilder declares no state type cannot build its hello.
- **Snapshot loads.** A snapshot the native loader cannot load stays
  `ROUTER_REASON_PERSISTED_EVENT_CORRUPT` (DATA_LOSS); it is not a fold of events.
