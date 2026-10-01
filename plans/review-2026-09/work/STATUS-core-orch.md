# core-orch remediation status

X-### | status | commit | note
---|---|---|---
X-016 | fixed | de08f2ee | pre-validate STRICT-only; COMMUTATIVE merge / MANUAL DLQ reachable for client commands; wire-tag field diff when state type not in pool
X-083 | fixed | de08f2ee | no in-place re-run of identical book on merge-gate rejection; overlap message retryable ("Sequence mismatch:" prefix) with EventBook details
X-088 | fixed | de08f2ee | overlap window base rebuilt from history (AsOfSequence) when snapshot covers expected
X-002 | already-fixed | 1929e9bd | proto edits committed in angzarr-project b993c65 with original enum numbering; core decodes via SyncModeExt/MergeStrategyExt
X-020 | already-fixed | 5f8b5b4e,49baa0c5 | TemporalQuery::AsOfTimestamp carries prost Timestamp; services/aggregate.test.rs AsOfTime test
X-011 | fixed | 30796c8d | post_persist split: publish (in-place retry + DLQ) vs sync_fanout (once, errors to caller); no republish on fan-out failure
X-012 | fixed | 30796c8d | CascadeErrorMode honoured in aggregate fan-out, SagaCoord, PmCoord (DeliveryPolicy); undeliverable commands -> ABORTED; COMPENSATE = source compensation of the failed command (downstream 2PC revoke needs cascade_id propagation, see X-078)
X-014 | fixed | 30796c8d | UNIMPLEMENTED projector endpoints skipped; fan-out errors no longer feed post_persist DLQ. Deploy/bin side (no bin serves ProjectorCoordinator; chart points projector Services at client) flagged to infra
X-080 | fixed | 30796c8d | speculative/fact/compensation share load_prior (upcast + 2PC view); speculative honours divergence
X-081 | fixed | 30796c8d | fact NoOp not published; publish failure retried/DLQ'd; correlation validated
X-112 | fixed | 30796c8d | AggregateService::with_domain check on all four RPCs; bin must call .with_domain(target.domain) (flagged, src/bin)
X-113 | fixed | 30796c8d | persist_target_cover: coordinator (domain, root, correlation); foreign response cover refused
X-154 | fixed | 30796c8d | ChannelCache per endpoint + per-call deadline (DEFAULT_DOWNSTREAM_TIMEOUT 30s)
X-157 | fixed | 30796c8d | fact pages never carry producer cascade_id
X-013 | already-fixed | 580db9e0 | C04 outbox: non-Decision Retryable -> outbox or DLQ capture (process_manager/mod.rs enqueue_or_dead_letter)
X-079 | fixed | 0da581c5 | locks = unresolved other-cascade pages only; gate replays the 2PC view; Conflict path tested
X-021 | fixed | 5278d862 | Instrumented::get_with_divergence forwards (+OP_EVENT_GET_WITH_DIVERGENCE metric)
X-071 | fixed | 8242515e | clone-then-release at all 8 sites; concurrency test (red under old client)
X-012 | fixed | 6f622431 | follow-up: CONTINUE succeeds per cascade_error_mode.feature (C-0438)
X-082 | fixed | a34181ac | PM command provenance = trigger cover + trigger seq (spec AngzarrDeferredSequence doc); distinct triggers -> distinct keys
X-161 | fixed | a34181ac | provenance source carries the trigger edition
X-162 | fixed | a34181ac | fetch_pm_state: correlation_root + trigger edition, no HashMap pick, no "main" sentinel
X-066 | fixed | a34181ac | PM publish retried x3 then DLQ'd (transient). PM snapshots/upcasting not added (ProcessManagerHandleResponse has no snapshot; PM bin has no upcaster client) - noted, not fixed
X-095 | fixed | 6918972d | GetEvents -> dispatch_selection + validation
X-165 | fixed | 6918972d | Synchronize validates domain/edition; GetAggregateRoots surfaces per-domain error (error path untested: MockEventStore has no list_roots failure knob). Main-timeline-only listing kept (AggregateRoot has no edition field)
X-035 | fixed | 3c512794 | core half: GrpcProjectorHandler + ProjectorCoord call HandleSpeculative (prj-event/client-rust halves belong to prj/client agents)
X-179 | fixed | 823168ab | upcaster reply must preserve page count, sequences, no_commit, cascade_id
X-012 | fixed | 9f9dc491 | CONTINUE -> CommandResponse.reaction_errors (proto 5c41df4); COMPENSATE -> Compensate markers for delivered commands within a saga/PM coordinator. Cross-saga markers (ChargeSaga failure compensating ReserveSaga's commands, C-0439) need the aggregate to learn other coordinators' delivered commands - not in proto; open
X-016 | fixed | 9f9dc491 | follow-up per user decision: deferred commands bypass all merge/sequence checks; explicit saga sequences validated as client commands
X-155 | fixed | 9f9dc491 | FactExecutor::inject(fact, FactDelivery{sync_mode, skip_handler}); saga/PM facts inherit flow sync mode
X-078 | wontfix | - | 2PC being removed (user decision 2026-09-30)
X-022 | fixed (core-orch half) | 6e54eaaf | orchestration keys all main-timeline spellings as ""; storage backends' own sentinels are core-storage's
X-092 | fixed (core-orch half) | 6bff7b04 | handler retention persisted unchanged; store-side pruning of DEFAULT/TRANSIENT is core-storage's
X-169 | fixed | da2c33e4 | dotted types compared by fqn; tokens trimmed
X-160 | fixed | 07de9f22 | single deferred_source_info (bad root -> INVALID_ARGUMENT); local calculate_set_next_seq removed (empty book was next_sequence 1). Also fixed racy transport env-var tests
X-065 | fixed | e5817c6d | PM trigger dedup via trigger provenance on PM events; saga duplicates absorbed by command idempotency / fact external_id (bus copy is not suppressed - no proto marker for 'handled synchronously')
X-195 | fixed (core-orch scope) | 73f353ad | deleted StreamService, AggregateCommandHandler, client_traits, AggregateContextFactory, fetch_by_root. Kept: src/process (projector bin embedded mode uses it), ProjectorCoord (needed server for X-014). Bus/storage/registration items are siblings'
X-073 | fixed (core half) | 73f353ad | core StreamService deleted (dead); prj-cloudevents copy is prj's
X-194 | fixed (core-orch files) | e582f202 | ticket-history commentary rewritten in saga/PM/hybrid/pipeline (saga D-7 comments left for basis_seq removal; 2PC comments left for 2PC removal); protos/features are angzarr-project's
X-196 | fixed (core-orch docs) | e582f202 | aggregate/orchestration/saga/PM/handlers docs corrected; src/bin, bus README, storage README are siblings'
X-187 | fixed (core saga half) | f3f01e48 | GrpcSagaContext handle/on_command_rejected tested via in-process servers; Mock-store contract macros / prj / router halves are others'
X-017 | wontfix (superseded) | - | user decision 2026-09-30: deferred commands carry no expected version and skip every merge check (implemented in 9f9dc491); basis_seq / destination-sequence fetch are being removed by the coordinator after merge
X-026 | fixed (core half) | 9f9dc491,580db9e0 | a PM command with an explicit sequence is validated like a client command (COMMUTATIVE merge applies); a transient failure goes to the outbox, not dropped. examples-rust half skipped (poker replaced)
X-023 | partial | 30796c8d (validation), f3f01e48 (test) | orchestration side: GrpcSagaContext routes rejections to the source aggregate's HandleCompensation (tested); HandleCompensation now validated and run through the pipeline. OPEN: saga bin passes compensation_handler None (one-line src/bin change); PM rejection handling has no client RPC (ProcessManagerService has only Handle) -> needs spec/proto
MUTATION | merge.rs (at de08f2ee): 41 caught / 2 missed / 11 unviable (95%); missed = wire_fields varint-length (test added 1f in later commit) + check_cascade_conflict (2PC, being removed). Diff-wide run (325 mutants) aborted: /home disk full (ENOSPC) - not re-run
