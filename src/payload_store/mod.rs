//! External payload storage for claim check pattern.
//!
//! DOC: This file is referenced in docs/docs/operations/payload-offloading.md
//!      Update documentation when making changes to payload store patterns.
//!
//! When event/command payloads exceed message bus size limits, they are stored
//! externally and replaced with a `PayloadReference` marker. This module provides
//! the storage backends and related infrastructure.
//!
//! ## Storage Backends
//!
//! - `FilesystemPayloadStore` - Local filesystem storage
//! - `GcsPayloadStore` (feature: gcs) - Google Cloud Storage
//! - `S3PayloadStore` (feature: s3) - Amazon S3
//!
//! ## Content-Addressable Storage
//!
//! All backends use SHA-256 content hashing for:
//! - Deduplication: identical payloads share storage
//! - Integrity verification: corruption detection on retrieval
//!
//! ## TTL Cleanup
//!
//! Use `TtlReaper` to periodically clean up expired payloads.

mod config;
mod filesystem;
#[cfg(feature = "gcs")]
mod gcs;
mod reaper;
#[cfg(feature = "s3")]
mod s3;

#[cfg(feature = "gcs")]
pub use config::GcsStoreConfig;
#[cfg(feature = "s3")]
pub use config::S3StoreConfig;
pub use config::{FilesystemStoreConfig, PayloadOffloadConfig, PayloadStoreType};
pub use filesystem::FilesystemPayloadStore;
#[cfg(feature = "gcs")]
pub use gcs::GcsPayloadStore;
pub use reaper::TtlReaper;
#[cfg(feature = "s3")]
pub use s3::S3PayloadStore;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::bus::EventBus;
use crate::proto::{PayloadReference, PayloadStorageType};

/// Error message constants for payload store operations.
pub mod errmsg {
    pub const STORE_FAILED: &str = "Failed to store payload: ";
    pub const RETRIEVE_FAILED: &str = "Failed to retrieve payload: ";
    pub const NOT_FOUND: &str = "Payload not found: ";
    pub const INTEGRITY_FAILED: &str = "Integrity check failed: ";
    pub const IO_ERROR: &str = "IO error: ";
    pub const INVALID_URI: &str = "Invalid URI: ";
}

/// Errors that can occur during payload store operations.
#[derive(Debug, Error)]
pub enum PayloadStoreError {
    #[error("{}{}", errmsg::STORE_FAILED, .0)]
    StoreFailed(String),

    #[error("{}{}", errmsg::RETRIEVE_FAILED, .0)]
    RetrieveFailed(String),

    #[error("{}{}", errmsg::NOT_FOUND, .0)]
    NotFound(String),

    #[error("{}expected {expected}, got {actual}", errmsg::INTEGRITY_FAILED)]
    IntegrityFailed { expected: String, actual: String },

    #[error("{}{}", errmsg::IO_ERROR, .0)]
    Io(#[from] std::io::Error),

    #[error("{}{}", errmsg::INVALID_URI, .0)]
    InvalidUri(String),
}

/// Result type for payload store operations.
pub type Result<T> = std::result::Result<T, PayloadStoreError>;

/// External payload storage backend for claim check pattern.
///
/// Implementations store large payloads externally and return references
/// that can be embedded in EventPage/CommandPage messages.
#[async_trait]
pub trait PayloadStore: Send + Sync {
    /// Store payload and return a reference.
    ///
    /// Uses content-addressable storage (hash-based naming) for deduplication.
    /// If a payload with the same hash already exists, returns the existing reference.
    async fn put(&self, payload: &[u8]) -> Result<PayloadReference>;

    /// Retrieve payload by reference.
    ///
    /// Verifies content hash matches the reference for integrity.
    async fn get(&self, reference: &PayloadReference) -> Result<Vec<u8>>;

    /// Delete payloads older than the given duration.
    ///
    /// Called by TTL reaper background task.
    /// Returns the number of payloads deleted.
    async fn delete_older_than(&self, age: Duration) -> Result<usize>;

    /// Storage type for this backend.
    fn storage_type(&self) -> PayloadStorageType;
}

/// Compute SHA-256 hash of payload.
pub fn compute_hash(payload: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    hasher.finalize().to_vec()
}

/// Format hash as hex string for filenames.
pub fn hash_to_hex(hash: &[u8]) -> String {
    hex::encode(hash)
}

/// Object key for a payload in an object store: `[{prefix}/]{hh}/{hash}`,
/// sharded by the first two hex chars of the content hash.
pub fn payload_object_key(prefix: Option<&str>, hash: &[u8]) -> String {
    let hex = hash_to_hex(hash);
    let subdir = &hex[0..2];
    match prefix {
        Some(prefix) => format!("{}/{}/{}", prefix, subdir, hex),
        None => format!("{}/{}", subdir, hex),
    }
}

/// Whether `key` has the content-addressed layout every payload store
/// writes: `[{prefix}/]{hh}/{hash}` where `hash` is 64 lowercase hex chars
/// and `hh` its first two. Object-store reapers delete only such keys, so a
/// bucket shared with other data never loses foreign objects.
pub fn is_payload_object_key(key: &str, prefix: Option<&str>) -> bool {
    let rest = match prefix {
        Some(p) => match key.strip_prefix(p).and_then(|r| r.strip_prefix('/')) {
            Some(r) => r,
            None => return false,
        },
        None => key,
    };
    let Some((subdir, hash)) = rest.split_once('/') else {
        return false;
    };
    hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        && hash.starts_with(subdir)
        && subdir.len() == 2
}

/// Parse hex string back to hash bytes.
pub fn hex_to_hash(hex_str: &str) -> Result<Vec<u8>> {
    hex::decode(hex_str).map_err(|e| PayloadStoreError::InvalidUri(e.to_string()))
}

// ============================================================================
// Factory
// ============================================================================

/// Initialize a payload store based on configuration.
///
/// Returns `None` if payload offloading is disabled.
///
/// # Errors
///
/// Returns error if the configured store type requires an unavailable feature.
pub async fn init_payload_store(
    config: &PayloadOffloadConfig,
) -> std::result::Result<Option<Arc<dyn PayloadStore>>, Box<dyn std::error::Error>> {
    use tracing::info;

    if !config.enabled {
        return Ok(None);
    }

    match config.store_type {
        PayloadStoreType::Filesystem => {
            info!(
                path = %config.filesystem.base_path.display(),
                "PayloadStore: filesystem"
            );
            let store = FilesystemPayloadStore::new(&config.filesystem.base_path).await?;
            Ok(Some(Arc::new(store)))
        }
        #[cfg(feature = "gcs")]
        PayloadStoreType::Gcs => {
            info!(
                bucket = %config.gcs.bucket,
                prefix = ?config.gcs.prefix,
                "PayloadStore: gcs"
            );
            let store = GcsPayloadStore::new(&config.gcs.bucket, config.gcs.prefix.clone()).await?;
            Ok(Some(Arc::new(store)))
        }
        #[cfg(feature = "s3")]
        PayloadStoreType::S3 => {
            info!(
                bucket = %config.s3.bucket,
                prefix = ?config.s3.prefix,
                region = ?config.s3.region,
                endpoint = ?config.s3.endpoint,
                "PayloadStore: s3"
            );
            let store = match &config.s3.endpoint {
                Some(endpoint) => {
                    S3PayloadStore::with_endpoint(
                        &config.s3.bucket,
                        config.s3.prefix.clone(),
                        endpoint,
                        config.s3.region.as_deref(),
                    )
                    .await?
                }
                None => S3PayloadStore::new(&config.s3.bucket, config.s3.prefix.clone()).await?,
            };
            Ok(Some(Arc::new(store)))
        }
    }
}

/// Claim-check offload resolved at boot: the payload store plus the size
/// above which event payloads are moved to it.
#[derive(Clone)]
pub struct PayloadOffload {
    store: Arc<dyn PayloadStore>,
    threshold: Option<usize>,
}

impl PayloadOffload {
    /// Offload through `store`; `threshold` `None` uses each bus's limit.
    pub fn new(store: Arc<dyn PayloadStore>, threshold: Option<usize>) -> Self {
        Self { store, threshold }
    }

    /// Wrap `bus` so publishes offload oversized payloads and subscribers
    /// resolve references back to full events.
    pub fn wrap(&self, bus: Arc<dyn EventBus>) -> Arc<dyn EventBus> {
        crate::bus::wrap_with_offloading(bus, Some(Arc::clone(&self.store)), self.threshold)
    }
}

/// Wrap `bus` with `offload` when offloading is enabled.
pub fn with_offload(bus: Arc<dyn EventBus>, offload: Option<&PayloadOffload>) -> Arc<dyn EventBus> {
    match offload {
        Some(o) => o.wrap(bus),
        None => bus,
    }
}

/// Initialize claim-check offload from `payload_offload` config.
///
/// Returns `None` when offloading is disabled. When enabled, builds the
/// store and spawns its [`TtlReaper`] (retention `retention_hours`,
/// interval `cleanup_interval_secs`).
pub async fn init_payload_offload(
    config: &PayloadOffloadConfig,
) -> std::result::Result<Option<PayloadOffload>, Box<dyn std::error::Error>> {
    let Some(store) = init_payload_store(config).await? else {
        return Ok(None);
    };
    TtlReaper::new(Arc::clone(&store), config.retention())
        .with_interval(config.cleanup_interval())
        .spawn();
    Ok(Some(PayloadOffload::new(store, config.threshold())))
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
