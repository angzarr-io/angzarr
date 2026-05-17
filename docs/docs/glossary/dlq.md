---
id: dlq
title: Dead Letter Queue (DLQ)
hoverText: Destination for messages that can't be processed automatically, requiring manual review.
---

# Dead Letter Queue (DLQ)

A destination for messages that cannot be processed automatically. Messages are routed to the DLQ when:

1. **Sequence mismatch** with `MERGE_MANUAL` strategy
2. **Processing failures** after retry exhaustion
3. **Payload retrieval failures** (external storage unavailable)
4. **Unrecoverable errors** in handlers

## DLQ Entry Types

### SequenceMismatchDetails
```protobuf
message SequenceMismatchDetails {
  uint32 expected_sequence = 1;
  uint32 actual_sequence = 2;
  MergeStrategy merge_strategy = 3;
}
```

### EventProcessingFailedDetails
```protobuf
import "sererr/sererr.proto";

message EventProcessingFailedDetails {
  string error = 1;
  uint32 retry_count = 2;
  bool is_transient = 3;
  // Flat cause chain, most-causal-first; the originating caught error
  // is the LAST element. CapturedError is defined in the sererr
  // schema (sererr.fyi/spec/proto).
  repeated sererr.v1.CapturedError stack_trace = 4;
}
```

`stack_trace` carries a Sentry-compatible structured capture: per-frame
function/file/line + optional source context, plus an `ExceptionMechanism`
that links chain entries. The schema and Rust producer library live in
the [sererr](https://sererr.fyi) project; angzarr's `src/dlq/mod.rs`
consumes `sererr::CapturedError` (plain Rust types) and converts to
`sererr_proto::ProtoCapturedError` for wire serialization. Operators
read it from the status console DLQ detail view. Producers populate it
at the originating failure site (not at the DLQ-publish layer). Full
shape and producer/consumer guidance:
[`reference/stack-trace-proto`](../reference/stack-trace-proto) +
[sererr.fyi](https://sererr.fyi).

### PayloadRetrievalFailedDetails
For claim-check pattern failures when external payload cannot be retrieved.

## Topic Structure

Per-domain DLQ topics: `angzarr.dlq.{domain}`

## Resolution

DLQ messages require manual intervention:
1. Inspect the failed message and context
2. Fix the underlying issue
3. Resubmit or discard the message
4. Update monitoring/alerting as needed
