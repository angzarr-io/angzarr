//! Google Cloud Storage payload store.
//!
//! Stores payloads as objects in a GCS bucket:
//! ```text
//! gs://{bucket}/{prefix}/{hash[0:2]}/{hash}
//! ```

use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use gcloud_storage::client::{Client, ClientConfig};
use gcloud_storage::http::objects::delete::DeleteObjectRequest;
use gcloud_storage::http::objects::download::Range;
use gcloud_storage::http::objects::get::GetObjectRequest;
use gcloud_storage::http::objects::list::ListObjectsRequest;
use gcloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use gcloud_storage::http::Error as GcsError;
use prost_types::Timestamp;
use tracing::{debug, warn};

use super::{
    compute_hash, hash_to_hex, is_payload_object_key, payload_object_key, PayloadStore,
    PayloadStoreError, Result,
};
use crate::proto::{PayloadReference, PayloadStorageType};

/// GCS-based payload store.
///
/// Stores payloads in content-addressable objects under a GCS bucket.
pub struct GcsPayloadStore {
    client: Client,
    bucket: String,
    prefix: Option<String>,
}

impl GcsPayloadStore {
    /// Create a new GCS payload store.
    ///
    /// Uses default credentials from the environment (GOOGLE_APPLICATION_CREDENTIALS
    /// or workload identity).
    pub async fn new(bucket: impl Into<String>, prefix: Option<String>) -> Result<Self> {
        let config = ClientConfig::default()
            .with_auth()
            .await
            .map_err(|e| PayloadStoreError::StoreFailed(format!("GCS auth failed: {}", e)))?;

        let client = Client::new(config);

        Ok(Self {
            client,
            bucket: bucket.into(),
            prefix,
        })
    }

    /// Create with explicit client config (for testing).
    pub fn with_client(client: Client, bucket: impl Into<String>, prefix: Option<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            prefix,
        }
    }

    /// Get the object name for a given hash.
    fn object_name(&self, hash: &[u8]) -> String {
        payload_object_key(self.prefix.as_deref(), hash)
    }

    /// Build a URI for an object.
    fn uri_for_object(&self, object_name: &str) -> String {
        format!("gs://{}/{}", self.bucket, object_name)
    }

    /// Extract object name from a gs:// URI.
    fn object_from_uri(&self, uri: &str) -> Result<String> {
        let expected_prefix = format!("gs://{}/", self.bucket);
        uri.strip_prefix(&expected_prefix)
            .map(|s| s.to_string())
            .ok_or_else(|| {
                PayloadStoreError::InvalidUri(format!(
                    "URI doesn't match bucket {}: {}",
                    self.bucket, uri
                ))
            })
    }
}

#[async_trait]
impl PayloadStore for GcsPayloadStore {
    async fn put(&self, payload: &[u8]) -> Result<PayloadReference> {
        let hash = compute_hash(payload);
        let object_name = self.object_name(&hash);

        // Always upload: content addressing makes the write idempotent, and
        // rewriting refreshes the object's creation time so the TTL reaper
        // never deletes a payload a just-published event still references.
        let upload_type = UploadType::Simple(Media::new(object_name.clone()));
        self.client
            .upload_object(
                &UploadObjectRequest {
                    bucket: self.bucket.clone(),
                    ..Default::default()
                },
                payload.to_vec(),
                &upload_type,
            )
            .await
            .map_err(|e| PayloadStoreError::StoreFailed(format!("GCS upload failed: {}", e)))?;

        debug!(
            hash = %hash_to_hex(&hash),
            size = payload.len(),
            bucket = %self.bucket,
            "Stored payload in GCS"
        );

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();

        Ok(PayloadReference {
            storage_type: PayloadStorageType::Gcs as i32,
            uri: self.uri_for_object(&object_name),
            content_hash: hash,
            original_size: payload.len() as u64,
            stored_at: Some(Timestamp {
                seconds: now.as_secs() as i64,
                nanos: now.subsec_nanos() as i32,
            }),
        })
    }

    async fn get(&self, reference: &PayloadReference) -> Result<Vec<u8>> {
        let object_name = self.object_from_uri(&reference.uri)?;
        if object_name != self.object_name(&reference.content_hash) {
            return Err(PayloadStoreError::InvalidUri(format!(
                "{} is not this store's object for its content hash",
                reference.uri
            )));
        }

        let payload = self
            .client
            .download_object(
                &GetObjectRequest {
                    bucket: self.bucket.clone(),
                    object: object_name.clone(),
                    ..Default::default()
                },
                &Range::default(),
            )
            .await
            .map_err(|e| match e {
                GcsError::Response(ref err_response) if err_response.code == 404 => {
                    PayloadStoreError::NotFound(reference.uri.clone())
                }
                err => PayloadStoreError::RetrieveFailed(format!("GCS download failed: {}", err)),
            })?;

        // Verify integrity
        let actual_hash = compute_hash(&payload);
        if actual_hash != reference.content_hash {
            return Err(PayloadStoreError::IntegrityFailed {
                expected: hash_to_hex(&reference.content_hash),
                actual: hash_to_hex(&actual_hash),
            });
        }

        Ok(payload)
    }

    async fn delete_older_than(&self, age: Duration) -> Result<usize> {
        let cutoff = SystemTime::now() - age;
        let cutoff_secs = cutoff
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut deleted = 0;
        let mut page_token: Option<String> = None;

        loop {
            let list_request = ListObjectsRequest {
                bucket: self.bucket.clone(),
                prefix: self.prefix.clone(),
                page_token: page_token.clone(),
                ..Default::default()
            };

            let response =
                self.client.list_objects(&list_request).await.map_err(|e| {
                    PayloadStoreError::StoreFailed(format!("GCS list failed: {}", e))
                })?;

            for object in response.items.into_iter().flatten() {
                if !is_payload_object_key(&object.name, self.prefix.as_deref()) {
                    continue;
                }
                // Check object creation time
                if let Some(time_created) = object.time_created {
                    let created_secs = time_created.unix_timestamp() as u64;
                    if created_secs < cutoff_secs {
                        if let Err(e) = self
                            .client
                            .delete_object(&DeleteObjectRequest {
                                bucket: self.bucket.clone(),
                                object: object.name.clone(),
                                ..Default::default()
                            })
                            .await
                        {
                            warn!(
                                object = %object.name,
                                error = %e,
                                "Failed to delete expired payload from GCS"
                            );
                        } else {
                            deleted += 1;
                        }
                    }
                }
            }

            page_token = response.next_page_token;
            if page_token.is_none() {
                break;
            }
        }

        Ok(deleted)
    }

    fn storage_type(&self) -> PayloadStorageType {
        PayloadStorageType::Gcs
    }
}
