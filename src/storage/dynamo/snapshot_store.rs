//! DynamoDB SnapshotStore implementation.
//!
//! Table schema:
//! - PK: `{domain}#{edition}#{root}` (String)
//! - SK: sequence number (Number)
//! - snapshot: serialized Snapshot (Binary)
//! - retention: retention type (Number)

use async_trait::async_trait;
use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;
use aws_sdk_dynamodb::types::{AttributeValue, Delete, Put, TransactWriteItem};
use aws_sdk_dynamodb::Client;
use prost::Message;
use tracing::{debug, info};
use uuid::Uuid;

use super::event_store::{Item, MAX_TRANSACTION_ITEMS};
use crate::proto::Snapshot;
use crate::storage::{is_superseded, Result, SnapshotStore, StorageError};

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
            crate::storage::helpers::pct_encode_component(
                crate::storage::timeline::storage_edition(edition),
            ),
            root
        )
    }

    /// All items of a Query, following `LastEvaluatedKey`.
    async fn query_all(&self, query: QueryFluentBuilder) -> Result<Vec<Item>> {
        query
            .into_paginator()
            .items()
            .send()
            .try_collect()
            .await
            .map_err(|e| StorageError::Backend(format!("DynamoDB query failed: {}", e)))
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

        let mut item = std::collections::HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S(pk.clone()));
        item.insert("seq".to_string(), AttributeValue::N(seq.to_string()));
        item.insert(
            "retention".to_string(),
            AttributeValue::N(snapshot.retention.to_string()),
        );
        item.insert(
            "snapshot".to_string(),
            AttributeValue::B(snapshot.encode_to_vec().into()),
        );

        // Older snapshots this one supersedes.
        let older = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("pk = :pk AND seq < :seq")
            .expression_attribute_values(":pk", AttributeValue::S(pk.clone()))
            .expression_attribute_values(":seq", AttributeValue::N(seq.to_string()))
            .projection_expression("seq, #retention")
            .expression_attribute_names("#retention", "retention");
        let superseded: Vec<AttributeValue> = self
            .query_all(older)
            .await?
            .into_iter()
            .filter_map(|old| {
                let old_seq = match old.get("seq") {
                    Some(AttributeValue::N(n)) => n.parse::<u32>().ok()?,
                    _ => return None,
                };
                let old_retention = match old.get("retention") {
                    Some(AttributeValue::N(n)) => n.parse::<i32>().ok()?,
                    _ => return None,
                };
                is_superseded(old_seq, old_retention, seq)
                    .then(|| AttributeValue::N(old_seq.to_string()))
            })
            .collect();

        // The new snapshot and the deletes it implies commit together (one
        // transaction of at most MAX_TRANSACTION_ITEMS actions; any further
        // deletes follow in additional transactions).
        let put = Put::builder()
            .table_name(&self.table_name)
            .set_item(Some(item))
            .build()
            .map_err(|e| StorageError::Backend(format!("DynamoDB put build failed: {}", e)))?;
        let mut actions = vec![TransactWriteItem::builder().put(put).build()];
        for old_seq in superseded {
            let delete = Delete::builder()
                .table_name(&self.table_name)
                .key("pk", AttributeValue::S(pk.clone()))
                .key("seq", old_seq)
                .build()
                .map_err(|e| {
                    StorageError::Backend(format!("DynamoDB delete build failed: {}", e))
                })?;
            actions.push(TransactWriteItem::builder().delete(delete).build());
        }
        for chunk in actions.chunks(MAX_TRANSACTION_ITEMS) {
            self.client
                .transact_write_items()
                .set_transact_items(Some(chunk.to_vec()))
                .send()
                .await
                .map_err(|e| {
                    StorageError::Backend(format!("DynamoDB snapshot write failed: {}", e))
                })?;
        }

        debug!(domain = %domain, root = %root, seq = seq, "Stored snapshot in DynamoDB");
        Ok(())
    }

    async fn delete(&self, domain: &str, edition: &str, root: Uuid) -> Result<()> {
        let pk = Self::pk(domain, edition, root);

        let all = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(":pk", AttributeValue::S(pk.clone()))
            .projection_expression("seq");
        for item in self.query_all(all).await? {
            if let Some(seq) = item.get("seq") {
                self.client
                    .delete_item()
                    .table_name(&self.table_name)
                    .key("pk", AttributeValue::S(pk.clone()))
                    .key("seq", seq.clone())
                    .send()
                    .await
                    .map_err(|e| {
                        StorageError::Backend(format!("DynamoDB delete_item failed: {}", e))
                    })?;
            }
        }

        debug!(domain = %domain, root = %root, "Deleted snapshots from DynamoDB");
        Ok(())
    }
}
