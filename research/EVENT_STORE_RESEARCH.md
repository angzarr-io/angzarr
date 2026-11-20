# angzarr Architecture Research

## Multi-Language client logic

| Language | Integration | Feature Flag | Latency | Deployment |
|----------|-------------|--------------|---------|------------|
| **Rust** | Native | - | ~ns | Single binary |
| **Python** | PyO3 (in-process) | `python` | ~μs | Single binary |
| **Go** | FFI via C-shared | `go-ffi` | ~μs | Single binary + .so |
| **Any** | gRPC (out-of-process) | default | ~ms | Multiple containers |

### Build Commands

```bash
# Default (gRPC only)
cargo build --release

# With Python support
cargo build --release --features python

# With Go FFI support
cargo build --release --features go-ffi

# All features
cargo build --release --features "python,go-ffi"
```

### Go client logic (FFI)

```go
// business/main.go
package main

import "C"
import "unsafe"

//export Handle
func Handle(cmdPtr *C.char, cmdLen C.int) (*C.char, C.int) {
    cmdBytes := C.GoBytes(unsafe.Pointer(cmdPtr), cmdLen)
    // Unmarshal protobuf, execute client logic
    resultBytes := executeBusinessLogic(cmdBytes)
    return (*C.char)(C.CBytes(resultBytes)), C.int(len(resultBytes))
}

func main() {}
```

```bash
# Build Go shared library
go build -buildmode=c-shared -o libbusiness.so business/main.go
```

---

# Event Store Database Research

Research on optimal database storage for event sourcing on AWS and GCP.

## Executive Summary

| Cloud | Primary Recommendation | Alternative |
|-------|----------------------|-------------|
| **AWS** | DynamoDB | Aurora PostgreSQL |
| **GCP** | Cloud Spanner | AlloyDB |

---

## AWS Comparison

### Candidates Evaluated

| Criteria | Aurora PostgreSQL | DynamoDB | Timestream | QLDB |
|----------|------------------|----------|------------|------|
| Append writes | Good | Excellent | Good | Moderate |
| Sequential reads | Excellent | Good | Excellent | Moderate |
| Ordering | Strong | Per-partition | Built-in | Strong |
| Horizontal scale | Limited | Excellent | Good | Limited |
| Write-heavy cost | High | Moderate | High | High |
| CDC/Streams | Good | Excellent | Limited | Good |
| Consistency | Strong ACID | Eventually* | Eventually | Strong |
| Ops complexity | Moderate | Low | Low | Low |

*Strong consistency optional per-request

### AWS Recommendation: DynamoDB

**Rationale:**
1. Native event sourcing patterns - partition key = aggregate, sort key = sequence
2. DynamoDB Streams - built-in CDC for projections without additional infrastructure
3. Unlimited horizontal scaling, handles traffic spikes
4. On-demand pricing aligns with write-heavy, variable workloads
5. Fully managed, no connection pooling or sharding decisions

**Schema Design:**
```
Events Table:
  PK: domain#root (String)
  SK: sequence (Number)
  Attributes: event_data (Binary), created_at (String), synchronous (Boolean)

  GSI1 (for list_roots):
    PK: domain
    SK: root
    Projection: KEYS_ONLY

Snapshots Table:
  PK: domain#root (String)
  Attributes: sequence (Number), state_data (Binary), created_at (String)
```

**Choose Aurora PostgreSQL instead if:**
- Cross-aggregate queries needed (reporting, analytics)
- Team has strong PostgreSQL expertise
- Complex transactions spanning multiple aggregates required
- Ad-hoc SQL querying needed

**Avoid:**
- **Timestream** - wrong paradigm for event sourcing (time-series, not domain events)
- **QLDB** - **DISCONTINUED July 31, 2025**. Even before discontinuation: 200 TPS limit, no ORDER BY, no LIMIT, 5 index max, 30-sec timeout

### AWS Cost Estimate (10M events/month, 1KB avg)

| Service | Monthly Cost |
|---------|-------------|
| DynamoDB | ~$150 |
| Aurora | ~$260 |
| QLDB | ~$500 |
| Timestream | ~$630 |

---

## GCP Comparison

### Candidates Evaluated

| Criteria | Cloud Spanner | Cloud SQL/AlloyDB | Firestore | Bigtable |
|----------|---------------|-------------------|-----------|----------|
| Append writes | Good* | Good | Good | Excellent |
| Sequential reads | Excellent | Good | Poor | Excellent |
| Ordering | Excellent | Good | Poor | Good |
| Horizontal scale | Excellent | Limited | Excellent | Excellent |
| Write-heavy cost | Medium-High | Low-Medium | High | Medium-High |
| CDC/Streams | Excellent | Good | Limited | Limited |
| Consistency | Excellent | Good | Limited | Limited |
| Ops complexity | Low | Low-Medium | Very Low | Medium |

*With proper key design

### GCP Recommendation: Cloud Spanner

**Rationale:**
1. Strong consistency - TrueTime guarantees global ordering (critical for event sourcing)
2. Native CDC - Change Streams provide first-class projection support
3. Horizontal scaling without losing ACID guarantees
4. Schema maps directly with minor key reordering

**Schema Design:**
```sql
CREATE TABLE events (
    domain STRING(255) NOT NULL,
    root STRING(36) NOT NULL,
    sequence INT64 NOT NULL,
    created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    event_data BYTES(MAX) NOT NULL,
    synchronous BOOL NOT NULL DEFAULT (false),
) PRIMARY KEY (domain, root, sequence);

CREATE TABLE snapshots (
    domain STRING(255) NOT NULL,
    root STRING(36) NOT NULL,
    sequence INT64 NOT NULL,
    state_data BYTES(MAX) NOT NULL,
    created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (domain, root);
```

**Choose AlloyDB instead if:**
- Cost is primary concern (~$650/month minimum for Spanner)
- Single-region deployment acceptable
- Team prefers PostgreSQL compatibility
- Smaller scale application

**Alternative: Bigtable (for high-throughput)**

Bigtable is viable for event sourcing when:
- Write throughput is critical (14K writes/node, proven at 25B events/hour by FIS)
- Aggregates are independent (single-row atomicity via `CheckAndMutateRow` is sufficient)
- Cost-sensitive at scale (~30% cheaper than Spanner)
- Can accept eventual consistency for multi-cluster or use single-cluster routing

Trade-offs vs Spanner:
- No SQL queries, no cross-row transactions
- Change Streams less mature (no old value capture, no Dataflow windowing)
- Higher minimum cost ($468/mo vs $65/mo for Spanner 100 PUs)

**Avoid:**
- **Firestore** - ordering guarantees too weak for event sourcing, per-operation pricing expensive for writes

### GCP Cost Estimate

| Service | Minimum Monthly |
|---------|----------------|
| Cloud SQL | ~$50 |
| AlloyDB | ~$200 |
| Spanner | ~$650 |
| Bigtable | ~$500 |

---

## Decision Matrix

| Priority | AWS | GCP | Multi-Cloud |
|----------|-----|-----|-------------|
| Free tier / minimal cost | DynamoDB | Firestore | MongoDB Atlas |
| Scale (high throughput) | DynamoDB | Bigtable | MongoDB Atlas |
| Multi-cloud portability | - | - | **MongoDB Atlas** |
| Massive throughput (25B+/hr) | DynamoDB | Bigtable | - |

## Multi-Cloud Option: MongoDB Atlas

| Feature | MongoDB Atlas |
|---------|---------------|
| **Clouds** | AWS, GCP, Azure |
| **Free tier** | 512 MB storage, shared cluster |
| **Event sourcing fit** | Good - document model, Change Streams for CDC |
| **Consistency** | Configurable (eventual to strong) |
| **Scaling** | Vertical + horizontal (sharding) |

Advantages:
- Single implementation works across clouds
- Change Streams provide CDC for projections
- Flexible document model fits event payloads
- No vendor lock-in

Trade-offs:
- Not as performant as cloud-native options at extreme scale
- Additional operational layer (Atlas control plane)

## Redis as Event Store (Small-Medium Scale)

Redis with persistence can serve as both event store AND cache in a single service.

| Service | AWS | GCP | Multi-Cloud |
|---------|-----|-----|-------------|
| **Recommended** | **MemoryDB for Redis** | Memorystore for Redis | Redis Cloud |
| **Durability** | Multi-AZ transaction log (built-in) | Configurable AOF/RDB | AOF + replication |
| **Free tier** | No | No | 30 MB |
| **Min cost** | ~$65/mo (db.t4g.small) | ~$35/mo (1GB basic) | Free-$7/mo |

**AWS MemoryDB** is the preferred choice:
- Purpose-built as durable primary database (not cache with persistence)
- Multi-AZ transaction log - data survives node failures
- Redis-compatible API - same code works
- Microsecond read latency, single-digit ms writes

### Why Redis Works for Event Sourcing

1. **Sorted Sets** - Natural fit for ordered event streams
   - Key: `{domain}:{root}:events`
   - Score: sequence number
   - Value: serialized event

2. **Atomic operations** - `ZADD` with `NX` flag for optimistic concurrency

3. **Range queries** - `ZRANGEBYSCORE` for replay from sequence N

4. **Pub/Sub** - Built-in event notifications for projections

5. **Lua scripting** - Atomic multi-step operations (check sequence + append)

### Data Model

```
# Events (sorted set)
ZADD {domain}:{root}:events NX {sequence} {event_data}

# Replay all
ZRANGE {domain}:{root}:events 0 -1

# Replay from sequence 5
ZRANGEBYSCORE {domain}:{root}:events 5 +inf

# Snapshot (hash)
HSET {domain}:{root}:snapshot sequence {n} state {data}
```

### When to Use Redis vs DynamoDB/Bigtable

| Criteria | Redis | DynamoDB/Bigtable |
|----------|-------|-------------------|
| Events/day | < 1M | > 1M |
| Total storage | < 100 GB | > 100 GB |
| Latency requirement | Sub-ms | Single-digit ms |
| Simplicity priority | High | Lower |
| Cost sensitivity | High | Lower |

### Recommended Architecture by Scale

| Scale | Event Store | Cache | Notes |
|-------|-------------|-------|-------|
| **Dev/Test** | SQLite | None | Free, local |
| **Small** (< 100K events/day) | Redis (Memorystore/MemoryDB) | Same Redis | Single service |
| **Medium** (< 1M events/day) | Redis Cluster | Same Redis | Scale horizontally |
| **Large** (> 1M events/day) | DynamoDB/Bigtable | Redis | Separate concerns |

## Final Architecture Decision

### AWS: MemoryDB for Redis
- **Event store + cache in single service**
- Purpose-built durable Redis (multi-AZ transaction log)
- Sorted sets for events, hashes for snapshots
- Sub-millisecond reads, single-digit ms writes
- Min cost: ~$65/mo

### GCP: Bigtable
- **Bigtable for event store + cache** (single service)
- Single-digit ms latency sufficient for most caching needs
- Bigtable proven at 25B events/hour (FIS case study)
- Min cost: ~$468/mo

### Architecture Comparison

| Component | AWS | GCP |
|-----------|-----|-----|
| Event Store | MemoryDB | Bigtable |
| Cache | MemoryDB (same) | Bigtable (same) |
| Services | 1 | 1 |
| Min cost | ~$65/mo | ~$468/mo |
| Latency | Sub-ms | Single-digit ms |

---

## Key Design (All Stores)

| Store | Events Key | Snapshots Key | Projections Key |
|-------|-----------|---------------|-----------------|
| SQLite | `(domain, root, sequence)` PK | `(domain, root)` PK | TBD |
| Redis | `evt:{domain}:{root}` sorted set | `snap:{domain}:{root}` hash | `proj:{domain}:{root}:{projector}` |
| Bigtable | `{domain}#{root}#{seq:010d}` row | `{domain}#{root}#_snapshot` row | `{domain}#{root}#_proj#{projector}` |

All stores use the same logical key structure: `(domain, root, sequence)` for events, `(domain, root)` for snapshots.

---

## Implementation Notes

### RedisEventStore (MemoryDB)

```rust
// Data model using sorted sets
// Key: {domain}:{root}:events
// Score: sequence number
// Value: protobuf-encoded EventPage

impl EventStore for RedisEventStore {
    async fn add(&self, domain: &str, root: Uuid, events: Vec<EventPage>) -> Result<()> {
        // ZADD {domain}:{root}:events NX {seq} {event_data}
        // NX = only add if score doesn't exist (optimistic concurrency)
    }

    async fn get_from(&self, domain: &str, root: Uuid, from: u32) -> Result<Vec<EventPage>> {
        // ZRANGEBYSCORE {domain}:{root}:events {from} +inf
    }

    async fn get_next_sequence(&self, domain: &str, root: Uuid) -> Result<u32> {
        // ZREVRANGE {domain}:{root}:events 0 0 WITHSCORES
        // Returns highest score + 1
    }
}

// Snapshots using hashes
// HSET {domain}:{root}:snapshot sequence {n} state {data}
```

### BigtableEventStore (GCP)

```rust
// Row key design: {domain}#{root}#{sequence:010d}
// Padded sequence for lexicographic ordering

impl EventStore for BigtableEventStore {
    async fn add(&self, domain: &str, root: Uuid, events: Vec<EventPage>) -> Result<()> {
        // CheckAndMutateRow for optimistic concurrency
        // Row key: {domain}#{root}#{seq:010d}
        // Column family: "event"
        // Column: "data" -> protobuf bytes
    }

    async fn get_from(&self, domain: &str, root: Uuid, from: u32) -> Result<Vec<EventPage>> {
        // ReadRows with RowRange
        // Start: {domain}#{root}#{from:010d}
        // End: {domain}#{root}#9999999999
    }

    async fn list_roots(&self, domain: &str) -> Result<Vec<Uuid>> {
        // ReadRows with RowRange prefix {domain}#
        // Extract unique roots from row keys
    }
}

// Snapshots: separate row
// Row key: {domain}#{root}#snapshot
// Column family: "snapshot", Column: "state"
```

### DynamoDB (AWS) Migration

```rust
// Partition key construction
fn make_pk(domain: &str, root: Uuid) -> String {
    format!("{}#{}", domain, root)
}

// EventStore trait mapping:
// add() -> BatchWriteItem with PutRequest
// get() -> Query(PK=domain#root, ScanIndexForward=true)
// get_from() -> Query(PK=domain#root, SK>=from)
// list_roots() -> Query on GSI1(PK=domain)
// get_next_sequence() -> Query(PK, ScanIndexForward=false, Limit=1)
```

### Cloud Spanner (GCP) Migration

- Use `OPTIONS (allow_commit_timestamp=true)` for automatic ordering
- Key design `(domain, root, sequence)` distributes writes across aggregates
- Change Streams integrate with Dataflow or Kafka via Debezium connector

---

## Sources

### AWS
- [DynamoDB Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html)
- [The Mill Adventure - DynamoDB Event Sourcing at Scale](https://aws.amazon.com/blogs/architecture/how-the-mill-adventure-implemented-event-sourcing-at-scale-using-dynamodb/)
- [Build a CQRS Event Store with DynamoDB](https://aws.amazon.com/blogs/database/build-a-cqrs-event-store-with-amazon-dynamodb/)
- [Aurora PostgreSQL Documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.AuroraPostgreSQL.html)

### AWS QLDB (Discontinued)
- [QLDB Discontinuation Announcement - InfoQ](https://www.infoq.com/news/2024/07/aws-kill-qldb/)
- [Why we didn't choose QLDB - theburningmonk](https://theburningmonk.com/2020/07/why-we-didnt-choose-qldb-for-a-healthcare-app/)
- [QLDB Guide - Data Design Limitations](https://qldbguide.com/guide/data-design/)
- [QLDB vs DynamoDB Streams Comparison](https://qldbguide.com/blog/stream-processing-with-qldb-and-dynamodb/)

### GCP
- [Deploying event-sourced systems with Cloud Spanner](https://cloud.google.com/solutions/deploying-event-sourced-systems-with-cloud-spanner)
- [Cloud Spanner Change Streams](https://cloud.google.com/spanner/docs/change-streams)
- [Cloud Spanner Schema Design Best Practices](https://cloud.google.com/spanner/docs/schema-design)
- [AlloyDB vs Cloud SQL Engineering Guide](https://www.bytebase.com/blog/alloydb-vs-cloudsql/)
- [AlloyDB Performance Comparison](https://cloud.google.com/blog/products/databases/alloydb-vs-self-managed-postgresql-a-price-performance-comparison)
- [CDC from Cloud SQL for PostgreSQL](https://cloud.google.com/blog/products/databases/you-can-now-use-cdc-from-cloudsql-for-postgresql)
- [Firestore Quotas and Limits](https://firebase.google.com/docs/firestore/quotas)

### GCP Bigtable
- [FIS: 25 Billion Events/Hour Case Study](https://cloud.google.com/blog/products/gcp/financial-services-firm-processes-25-billion-stock-market-events-per-hour-with-google-cloud-bigtable)
- [Bigtable Performance Documentation](https://cloud.google.com/bigtable/docs/performance)
- [Bigtable Schema Design for Time Series](https://cloud.google.com/bigtable/docs/schema-design-time-series)
- [Bigtable Change Streams Overview](https://cloud.google.com/bigtable/docs/change-streams-overview)
- [Bigtable Writes and CheckAndMutateRow](https://cloud.google.com/bigtable/docs/writes)
- [Fitbit: Evaluating Spanner vs Bigtable](https://medium.com/fitbit-tech-blog/evaluating-google-cloud-spanner-and-bigtable-engineering-fitness-a4c2135b1c70)
- [Smart Parking Event-Driven Architecture](https://cloud.google.com/blog/products/gcp/implementing-an-event-driven-architecture-on-serverless-the-smart-parking-story)
