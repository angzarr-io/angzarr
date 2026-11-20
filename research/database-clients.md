# Database Client Libraries Research

## Google Bigtable

### Recommended: bigtable_rs

- **Crate**: https://crates.io/crates/bigtable_rs
- **GitHub**: https://github.com/liufuyang/bigtable_rs
- **Docs**: https://docs.rs/bigtable_rs

Uses proper gRPC over HTTP/2 via tonic. Supports Google Bigtable Data API V2.

**Installation:**
```toml
bigtable_rs = "0.2.18"
tokio = { version = "1.0", features = ["rt-multi-thread"] }
```

**Authentication:**
- Service account key: `GOOGLE_APPLICATION_CREDENTIALS=path/to/key.json`
- GCE metadata server (default service account)

### Alternative: rust-bigtable

- **GitHub**: https://github.com/durch/rust-bigtable

Older library that uses JSON-based HTTP instead of native gRPC. Less performant but simpler setup.

### Notes

Google has no official Rust client on their roadmap. Community libraries are the only option.

---

## AWS Redis (ElastiCache / MemoryDB)

### Recommended: redis-rs

- **Crate**: https://crates.io/crates/redis
- **GitHub**: https://github.com/redis-rs/redis-rs
- **Docs**: https://docs.rs/redis/latest/redis/

The standard Rust Redis client with 45M+ downloads. AWS uses it as the core of their Valkey GLIDE project.

**Installation:**
```toml
redis = { version = "0.27", features = ["tokio-comp", "cluster-async", "tls-rustls"] }
```

**Key Features:**
- Async support (tokio or smol runtimes)
- Cluster support (merged from redis-cluster-async)
- TLS via rustls or native-tls
- Connection pooling with bb8
- RESP2 and RESP3 protocol support
- Client-side caching (experimental)

**Feature Flags:**
| Feature | Purpose |
|---------|---------|
| `tokio-comp` | Tokio async runtime |
| `smol-comp` | Smol async runtime |
| `cluster-async` | Async cluster connections |
| `tls-rustls` | TLS via rustls |
| `tls-native-tls` | TLS via OpenSSL |
| `bb8` | Connection pooling |
| `cache-aio` | Client-side caching |

### Alternative: fred

- **Crate**: https://lib.rs/crates/fred

Async-first Redis/Valkey client with RESP2/RESP3 support. Handles clustered, centralized, and sentinel deployments.

---

## Comparison

| Feature | bigtable_rs | redis-rs |
|---------|-------------|----------|
| Maturity | Community-maintained | Very mature |
| Downloads | ~100k | 45M+ |
| Async | Yes (tonic) | Yes (tokio/smol) |
| Cluster | N/A | Yes |
| TLS | Yes | Yes |
| Connection pooling | Manual | Built-in (bb8) |
| Official support | No | AWS-endorsed |

---

## References

- [Google Bigtable client libraries](https://cloud.google.com/bigtable/docs/reference/libraries)
- [redis-rs official guide](https://redis.io/docs/latest/develop/clients/rust/)
- [AWS Valkey GLIDE announcement](https://aws.amazon.com/blogs/database/introducing-valkey-glide-an-open-source-client-library-for-valkey-and-redis-open-source/)
- [AWS ElastiCache best practices](https://aws.amazon.com/blogs/database/best-practices-valkey-redis-oss-clients-and-amazon-elasticache/)
