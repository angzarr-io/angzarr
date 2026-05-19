//! DynamoDB PositionStore implementation.
//!
//! Table schema:
//! - PK: `{handler}#{domain}#{edition}#{root_hex}` (String)
//! - sequence: last processed sequence number (Number)

use async_trait::async_trait;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use tracing::{debug, info};

use crate::storage::{PositionStore, Result, StorageError};

/// DynamoDB implementation of PositionStore.
pub struct DynamoPositionStore {
    client: Client,
    table_name: String,
}

impl DynamoPositionStore {
    /// Create a new DynamoDB position store.
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
        info!(table = %table_name, "Connected to DynamoDB for positions");

        Ok(Self { client, table_name })
    }

    /// Build the partition key.
    ///
    /// H-26: percent-encode `handler`, `domain`, and `edition` so any
    /// `#` in any of them survives a round-trip. `root_hex` is bare hex
    /// (no separator characters) so it's safe as-is.
    fn pk(handler: &str, domain: &str, edition: &str, root: &[u8]) -> String {
        let root_hex = hex::encode(root);
        format!(
            "{}#{}#{}#{}",
            crate::storage::helpers::pct_encode_component(handler),
            crate::storage::helpers::pct_encode_component(domain),
            crate::storage::helpers::pct_encode_component(edition),
            root_hex
        )
    }
}

#[async_trait]
impl PositionStore for DynamoPositionStore {
    async fn get(
        &self,
        handler: &str,
        domain: &str,
        edition: &str,
        root: &[u8],
    ) -> Result<Option<u32>> {
        let pk = Self::pk(handler, domain, edition, root);

        let result = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key("pk", AttributeValue::S(pk))
            .send()
            .await
            .map_err(|e| {
                StorageError::NotImplemented(format!("DynamoDB get_item failed: {}", e))
            })?;

        if let Some(item) = result.item {
            if let Some(AttributeValue::N(seq_str)) = item.get("sequence") {
                if let Ok(seq) = seq_str.parse::<u32>() {
                    debug!(
                        handler = %handler,
                        domain = %domain,
                        edition = %edition,
                        sequence = seq,
                        "Retrieved position from DynamoDB"
                    );
                    return Ok(Some(seq));
                }
            }
        }

        Ok(None)
    }

    async fn put(
        &self,
        handler: &str,
        domain: &str,
        edition: &str,
        root: &[u8],
        sequence: u32,
    ) -> Result<()> {
        let pk = Self::pk(handler, domain, edition, root);

        let mut item = std::collections::HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S(pk));
        item.insert(
            "sequence".to_string(),
            AttributeValue::N(sequence.to_string()),
        );

        self.client
            .put_item()
            .table_name(&self.table_name)
            .set_item(Some(item))
            .send()
            .await
            .map_err(|e| {
                StorageError::NotImplemented(format!("DynamoDB put_item failed: {}", e))
            })?;

        debug!(
            handler = %handler,
            domain = %domain,
            edition = %edition,
            sequence = sequence,
            "Stored position in DynamoDB"
        );

        Ok(())
    }
}
