//! DynamoDB SnapshotStore implementation.
//!
//! Table schema:
//! - PK: `{domain}#{edition}#{root}` (String)
//! - SK: sequence number (Number)
//! - snapshot: serialized Snapshot (Binary)
//! - retention: retention type (String)

use async_trait::async_trait;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use prost::Message;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::proto::{Snapshot, SnapshotRetention};
use crate::storage::{Result, SnapshotStore, StorageError};

/// DynamoDB implementation of SnapshotStore.
pub struct DynamoSnapshotStore {
    client: Client,
    table_name: String,
}

impl DynamoSnapshotStore {
    /// Create a new DynamoDB snapshot store.
    pub async fn new(table_name: impl Into<String>, endpoint_url: Option<&str>) -> Result<Self> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;

        let client = if let Some(endpoint) = endpoint_url {
            let dynamo_config = aws_sdk_dynamodb::config::Builder::from(&config)
                .endpoint_url(endpoint)
                .build();
            Client::from_conf(dynamo_config)
        } else {
            Client::new(&config)
        };

        let table_name = table_name.into();
        info!(table = %table_name, "Connected to DynamoDB for snapshots");

        Ok(Self { client, table_name })
    }

    /// Build the partition key.
    ///
    /// H-26: percent-encode `domain` and `edition` so `#`-containing
    /// values survive the round-trip; UUIDs are safe as-is.
    fn pk(domain: &str, edition: &str, root: Uuid) -> String {
        format!(
            "{}#{}#{}",
            crate::storage::helpers::pct_encode_component(domain),
            crate::storage::helpers::pct_encode_component(edition),
            root
        )
    }
}

#[async_trait]
impl SnapshotStore for DynamoSnapshotStore {
    async fn get(&self, domain: &str, edition: &str, root: Uuid) -> Result<Option<Snapshot>> {
        let pk = Self::pk(domain, edition, root);

        // Query for latest snapshot (highest sequence)
        let result = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(":pk", AttributeValue::S(pk))
            .scan_index_forward(false) // Descending order
            .limit(1)
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("DynamoDB query failed: {}", e)))?;

        if let Some(items) = result.items {
            if let Some(item) = items.first() {
                if let Some(AttributeValue::B(blob)) = item.get("snapshot") {
                    let snapshot =
                        Snapshot::decode(blob.as_ref()).map_err(StorageError::ProtobufDecode)?;
                    debug!(domain = %domain, root = %root, "Retrieved snapshot from DynamoDB");
                    return Ok(Some(snapshot));
                }
            }
        }

        Ok(None)
    }

    async fn get_at_seq(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        seq: u32,
    ) -> Result<Option<Snapshot>> {
        let pk = Self::pk(domain, edition, root);

        // Query for snapshot with sequence <= seq
        let result = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("pk = :pk AND seq <= :seq")
            .expression_attribute_values(":pk", AttributeValue::S(pk))
            .expression_attribute_values(":seq", AttributeValue::N(seq.to_string()))
            .scan_index_forward(false) // Descending order to get highest <= seq
            .limit(1)
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("DynamoDB query failed: {}", e)))?;

        if let Some(items) = result.items {
            if let Some(item) = items.first() {
                if let Some(AttributeValue::B(blob)) = item.get("snapshot") {
                    let snapshot =
                        Snapshot::decode(blob.as_ref()).map_err(StorageError::ProtobufDecode)?;
                    return Ok(Some(snapshot));
                }
            }
        }

        Ok(None)
    }

    async fn put(&self, domain: &str, edition: &str, root: Uuid, snapshot: Snapshot) -> Result<()> {
        let pk = Self::pk(domain, edition, root);
        let seq = snapshot.sequence;
        let retention = snapshot.retention;
        let snapshot_bytes = snapshot.encode_to_vec();

        // Store the new snapshot
        let mut item = std::collections::HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S(pk.clone()));
        item.insert("seq".to_string(), AttributeValue::N(seq.to_string()));
        item.insert(
            "snapshot".to_string(),
            AttributeValue::B(snapshot_bytes.into()),
        );
        item.insert(
            "retention".to_string(),
            AttributeValue::N(retention.to_string()),
        );

        self.client
            .put_item()
            .table_name(&self.table_name)
            .set_item(Some(item))
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("DynamoDB put_item failed: {}", e)))?;

        // Clean up old transient snapshots
        let result = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("pk = :pk AND seq < :seq")
            .expression_attribute_values(":pk", AttributeValue::S(pk.clone()))
            .expression_attribute_values(":seq", AttributeValue::N(seq.to_string()))
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("DynamoDB query failed: {}", e)))?;

        if let Some(items) = result.items {
            for item in items {
                // Only delete TRANSIENT snapshots
                if let Some(AttributeValue::N(ret_str)) = item.get("retention") {
                    if let Ok(ret) = ret_str.parse::<i32>() {
                        if ret == SnapshotRetention::RetentionTransient as i32 {
                            if let Some(old_seq) = item.get("seq") {
                                if let Err(e) = self
                                    .client
                                    .delete_item()
                                    .table_name(&self.table_name)
                                    .key("pk", AttributeValue::S(pk.clone()))
                                    .key("seq", old_seq.clone())
                                    .send()
                                    .await
                                {
                                    warn!(error = %e, "Failed to delete old transient snapshot");
                                }
                            }
                        }
                    }
                }
            }
        }

        debug!(domain = %domain, root = %root, seq = seq, "Stored snapshot in DynamoDB");
        Ok(())
    }

    async fn delete(&self, domain: &str, edition: &str, root: Uuid) -> Result<()> {
        let pk = Self::pk(domain, edition, root);

        // Query all snapshots for this root
        let result = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(":pk", AttributeValue::S(pk.clone()))
            .projection_expression("pk, seq")
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("DynamoDB query failed: {}", e)))?;

        if let Some(items) = result.items {
            for item in items {
                if let Some(seq) = item.get("seq") {
                    if let Err(e) = self
                        .client
                        .delete_item()
                        .table_name(&self.table_name)
                        .key("pk", AttributeValue::S(pk.clone()))
                        .key("seq", seq.clone())
                        .send()
                        .await
                    {
                        warn!(error = %e, "Failed to delete snapshot from DynamoDB");
                    }
                }
            }
        }

        debug!(domain = %domain, root = %root, "Deleted snapshots from DynamoDB");
        Ok(())
    }
}
