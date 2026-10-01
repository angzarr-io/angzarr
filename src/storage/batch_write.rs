//! All-or-nothing batch writes for backends without multi-row transactions.
//!
//! Bigtable mutates one row atomically and DynamoDB commits at most 100
//! items per transaction. An `add` batch that spans several such units is
//! written unit by unit; if a later unit fails, the units already written
//! are removed again so a failed `add` leaves no partial command behind.
//!
//! The removal is best effort: an undo that itself fails is logged and the
//! original error is still returned. Readers may observe the partial batch
//! between its write and its undo.

use async_trait::async_trait;
use tracing::warn;

use crate::storage::Result;

/// A backend's write and undo of one atomic unit of an `add` batch.
#[async_trait]
pub trait UnitWriter: Send + Sync {
    /// One atomically written unit (a row, or a transaction's items).
    type Unit: Send + Sync;

    /// Write `unit` atomically; fail without side effects if any part of
    /// it conflicts or errors.
    async fn write(&self, unit: &Self::Unit) -> Result<()>;

    /// Remove a previously written `unit`.
    async fn undo(&self, unit: &Self::Unit) -> Result<()>;
}

/// Write `units` in order. On the first failure, undo every unit already
/// written (most recent first) and return the failure.
pub async fn write_all_or_undo<W: UnitWriter>(writer: &W, units: &[W::Unit]) -> Result<()> {
    for (index, unit) in units.iter().enumerate() {
        if let Err(error) = writer.write(unit).await {
            for written in units[..index].iter().rev() {
                if let Err(undo_error) = writer.undo(written).await {
                    warn!(
                        error = %undo_error,
                        "failed to undo a partially written batch unit"
                    );
                }
            }
            return Err(error);
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "batch_write.test.rs"]
mod tests;
