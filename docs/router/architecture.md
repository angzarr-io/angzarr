# angzarr-router architecture

angzarr-router is the Rust client library's router, and the reference
router: the one part of a client library that calls business code
(decision 14.42). It routes an input to the business code registered for
its arm and type, folds the state it is given, runs the handler's `Emit`,
and answers what business code decided, on the handler layer's messages
(`io.angzarr.handler.v1`, decision 14.78): success only, every other
outcome a status (decision 14.95). Python has a native router of its own;
angzarr-project's handler-contract tier (`features/handler-contract/`,
with its fixtures in `fixture/`) is the complete contract of both, run in
CI against every router.

The router has no transport and no store. The host process serves the
component contract's gRPC services (`CommandHandlerService`,
`PolicyService`, `ProjectorService`, `UpcasterService`,
`ComponentService`) and hands their messages to the router; the
coordinator (core's, or the angzarr-embedded kernel) owns every read and
write around each call.

## What the router does, and what it leaves to the coordinator

| The router (this crate) | The coordinator |
|---|---|
| routes by arm and type URL, one `Handle` for commands and facts (14.98); refuses a type nothing handles | upcasts every payload and event first, through the component's upcaster sidecar (14.39), and relays the type as sent beside an unknown type (`upcast_type_url`) |
| folds the starting state it is delivered and every page, in order | loads the snapshot, drops the pages it covers, decides when to write one (14.40) |
| runs the handler's `Emit` to its end as one decision, with no step cap (14.104) | appends the decision's events at the head it loaded, re-running a lost race on the new head (14.92) |
| answers the state after every input, with the build's rules version | stamps the events' meta and the snapshot's sequence and rules version |
| — | admits an input: deduplication, a process manager's start rule by `starting_types`; refuses a stale expected sequence `ExpectedSequenceMismatch` (14.94) |
| answers `Describe` with the descriptor it was given, unchanged | validates the descriptor angzarr-cli generated |
| answers a status and a reason key with its args; a typed refusal under its declared status | owns its own keys, and classifies every status (14.37's four classes) |
| answers a policy's outputs for its one trigger, as business code returned them | addresses, stamps and checks each output, and delivers it (`ReactionDelivery`) |
| runs the policy's one refusal callback, or answers `NO_REFUSAL_HANDLER` | reads the refusal off the target's status, calls Translate, delivers the fact to the trigger's stream |
| answers an upcaster's rules keyed by sequence or `NoSequence` | calls the sidecar on every read path |
| — | builds every delivery well formed (no envelope guards in the router) |

## The API

`crates/router` is a public contract, documented on each item. Business
payloads are opaque `Any`s; the router calls thunks keyed by
fully-qualified type name.

- `Rebuilder<S>`: a state factory, per-type appliers (`apply`,
  `apply_with_context`) or one batched fold (`with_fold`), the state type
  (`with_state_type`), and the state's one codec (`with_state_codec`:
  decode the starting state, encode the state answered).
  `Rebuilder::of_message` takes all of them from a prost message.
- `DomainDispatch<S>`: an aggregate (`new`) or a process manager
  (`process_manager`; a PM is a domain, decision 14.31): command handlers
  (`on_command`) returning an `Emit<S>` (`Emit::of(events)`, `Emit::none()`,
  `.then(step)`; a step fails with a `HandlerError`, a refusal included),
  fact handlers (`on_fact`) returning a `FactEmit<S>` (the same shape, but
  its handler and steps fail only with a `Bug`: a classified bug, never a
  refusal or a re-run; `Bug::new`, `Bug::coded`, `Bug::retryable`, and the
  deliberate `Bug::from_error` / `.bug()` for a std error), the rules
  version (`with_rules_version`), and the step-idempotence test helpers
  (`check_step_idempotent`, `check_fact_step_idempotent`).
- `PolicyDispatch`: one input domain and one output domain, reactions
  (`on_event`, answering `policy::command` and `policy::fact` outputs)
  and the one refusal callback (`on_refused`).
- `ProjectorDispatch<P>`: the domains it consumes (`for_domains`, each
  named; no wildcard, 14.53), event folds (`on_event`) or one batched
  fold (`with_fold`), and a finish step answering the projection payload
  (`finish`).
- `UpcasterDispatch`: an upcaster's rules (`rule`), each payload brought
  through every rule its type reaches, answered keyed (`dispatch`).
- `RouterBuilder`/`Router`: one component per router (owner ruling: one
  coordinator, one component, one business process), and the descriptor
  it answers `Describe` with (`RouterBuilder::descriptor`,
  `Router::component_descriptor`). The traits `CommandHandler`,
  `PolicyComponent` and `ProjectorHandler` let a runtime hold a component
  of its own (one in another process, say). They are kernel-facing and
  async (owner ruling 2026-10-30): their dispatch returns a future, `Send`
  on every target but wasm32 (`send::MaybeSend`, angzarr-ports' own,
  re-exported), and so do the `Router`'s `dispatch_*`. The router's own
  components answer at once, a ready future
  around the business code's synchronous functions; business code is never
  async. Each trait has an object-safe twin (`DynCommandHandler`,
  `DynPolicyComponent`, `DynProjectorHandler`, boxed futures) that every
  implementor is, and whose `dyn` is the trait again: components are held
  and registered boxed as `Box<dyn Dyn…>`.
- `CodedError`, `HandlerError` and the reason keys (`error::AngzarrReason`,
  re-exported from angzarr-proto), and the wire types re-exported as `pb`
  (core) and `hpb` (the handler layer).

## Semantics

| Semantic | Contract |
|---|---|
| Folding | A delivered starting state (`EventBook.snapshot.state`) is decoded by the state's codec (`with_state_codec`; `of_message` decodes the state message); without one it is ignored. Then every page the book holds folds onto it, in order, whatever its sequence (14.40); pageless or unknown-type entries are skipped, never terminal. A starting state the codec cannot decode is `ANGZARR_REASON_PERSISTED_EVENT_CORRUPT` (DATA_LOSS); a codec that panics is `ANGZARR_REASON_FOLD_FAILED` naming the snapshot's `sequence` and `type_url`. A page that fails to fold is a handler bug (INTERNAL, `ANGZARR_REASON_FOLD_FAILED`, or `ANGZARR_REASON_FOLD_FAILED_EMITTED` for a fold of the command's own events), whose metadata names the page's `sequence` and `type_url` (or the batch's `first_sequence` and `last_sequence`), the `state_type_url`, and the fold's own failure, when it is a key and args, as `cause` and `cause.<arg>` |
| Domain dispatch | `CommandHandlerService.Handle` is the one input, for commands and facts (14.98): the input book's body arm picks the table (a Command its command handler, a Fact its fact handler), the payload's type the handler, before any fold. A type nothing handles is `ANGZARR_REASON_UNKNOWN_COMMAND_TYPE` or `ANGZARR_REASON_NO_FACT_HANDLER` (INTERNAL), naming `type_url`, the type it was given; a delivery with no book, page, arm or payload is a command of no type. Handlers receive a `CommandContext`: the next sequence, had-prior-events, the cover, the input's arm (a command's expected sequence as sent, a fact's origin), its identity, `recorded_at` and, for a policy's input, its trigger (`causation`) |
| Emit | Every handler returns an `Emit` (14.104): its events are folded at the sequences they will get, with the meta the coordinator will stamp, before its continuation runs on the state; every step's events are answered in order as one list. A refusal or a bug from any step fails it all. There is no step cap; `check_step_idempotent` runs a step twice and names what the second run emitted |
| Answers | Success only (14.95): `HandleResponse { events, state, rules_version }`, the events payloads only, the state the codec's encoding of the state after them (none without a codec). Every other outcome is a `CodedError`: a command's typed refusal (`refused(status, object)`, `ANGZARR_REASON_COMMAND_REFUSED`, `refusal_type`, under the status its message declares), a re-run (`immediate`, ABORTED), or a bug. A fact's handling never refuses: a refusal or ABORTED from a fact handler is answered as the bug it is (`as_fact_failure`) |
| Policy | React takes one trigger event (Q3): its first page; an undeclared type, a meta-event or no page answers nothing (C-0051). Its outputs are a root and a payload in the arm of their kind (14.91), as business code returned them (C-0690, C-0783). A handler sees the trigger's cover, sequence and metadata. Translate runs the one refusal callback with the refused command's payload, the typed refusal and its status, and the trigger and delivery as context, answering the fact's payload alone (C-0691); a refusal it does not handle is a bug (C-0854); with no callback, `ANGZARR_REASON_NO_REFUSAL_HANDLER` naming the refused command's type URL |
| Projector | Folds every page of one stream's run into one projection instance, each fold seeing the cover and the page's sequence; undeclared domains fold nothing (C-0032); unmatched types and meta-events are skipped (a WARN for an unmatched type); the answer is the finish step's projection payload, none without one. A failing fold is named as a state fold's is, with the projection type as `state_type_url` |
| Upcaster | Each payload brought through every rule its type reaches, each rule at most once; a stream's pages answered keyed by sequence (a meta-event page with no payload), a lone payload keyed `NoSequence` (14.96, 14.97); a failing rule fails the request as a bug |
| Describe | `Router::component_descriptor` answers the `ComponentDescriptor` the router was given (angzarr-cli generates it at build time), unchanged, and none when given none |
| One component | A router hosts one component: none is `ANGZARR_REASON_NO_HANDLERS_REGISTERED`, a second of any kind `ANGZARR_REASON_DUPLICATE_COMPONENT` (both INTERNAL; the latter naming the hosted one `registered` and the refused one `refused`); dispatching a kind it does not host is `ANGZARR_REASON_NO_COMPONENT_OF_KIND` (UNIMPLEMENTED) |
| Type URLs | Emitted type URLs use the bare `/` prefix; incoming ones match by the fully-qualified name after the last `/`, whatever the prefix |
| Errors | Decision 14.37: every error is a key and an argument map, never English. The status message IS the key; the `google.rpc.ErrorInfo` (domain `angzarr.io`) carries the same key and the args as metadata; a wrapped cause flattens to `cause` / `cause.<arg>`; `Display` is the key then ` name=value` args. The router's keys are core's `AngzarrReason` values raised by the router (14.104), answered as the full value name (`as_str_name()`) under the `google.rpc.Code` their option declares. A handler refuses with a typed `refused(status, object)` only (14.95: a key of its own is a bug's cause), returns at once with `immediate(key, args)` (ABORTED), or fails as a `bug(key, args)` or a `retryable_bug(status, key, args)` (`ANGZARR_REASON_UNHANDLED_HANDLER_ERROR`, its key the `cause`); an unclassified error is `ANGZARR_REASON_UNHANDLED_HANDLER_ERROR` with no cause, and a panic is caught (`cause` `ANGZARR_REASON_HANDLER_PANICKED`), their text left to the log. The router never classifies: what a status means is the coordinator's |

Not provided here: gRPC serving, health and readiness, transport
configuration, CloudEvents, and every duty the table above gives the
coordinator.

## Testing

- `just test`: the unit bank (`crates/router/src/*.test.rs`) and the
  handler-contract tier, run whole from the submodule by cucumber-rs
  (`crates/conformance`, one harness per tier feature).
- `just mutation-test`: cargo-mutants over the router's modules and the
  architecture linter, every missed mutant a failure; weekly in CI.
  `just mutation-push`, by hand, is the same over only what a push adds.
  No git hook runs mutation.
- `just push-check` (pre-push): every test, in each pushed commit's tree
  checked out alone. Red tests pushed to a branch pass with a warning;
  pushed to the default branch they are refused.
- `just archlint`: the layering as checked rules (`archlint.toml`: the
  router below the harness, its components independent of each other and
  of the `Router` that composes them, the harness on the public surface),
  enforced by `tools/archlint` over every module's references and every
  manifest dependency. Runs in pre-commit and CI.
