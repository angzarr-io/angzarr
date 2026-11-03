//! The export: a store's [`Export`] rows, checked and written as JSONL.

use std::io::Write;

use angzarr_ports::{AppliedMigration, Export, Migration, Migrations};
use prost_reflect::DescriptorPool;

use crate::error::ExportError;
use crate::record::End;

/// A pool of angzarr's descriptors (`angzarr_proto::FILE_DESCRIPTOR_SET`)
/// and the app's `FileDescriptorSet`s, encoded: what an export expands
/// payloads through. A file in two sets is taken once. Refused
/// ([`ExportError::DescriptorSet`]): a set that does not decode or does
/// not join the pool, by its index (0 is angzarr's own; the app's from 1).
pub fn descriptor_pool(app_sets: &[&[u8]]) -> Result<DescriptorPool, ExportError> {
    let mut pool = DescriptorPool::new();
    let sets = std::iter::once(angzarr_ports::angzarr_proto::FILE_DESCRIPTOR_SET)
        .chain(app_sets.iter().copied());
    for (index, set) in (0u64..).zip(sets) {
        pool.decode_file_descriptor_set(set)
            .map_err(|_| ExportError::DescriptorSet { index })?;
    }
    Ok(pool)
}

/// That `ledger` is exactly `build`'s migration set: every version of it,
/// each a success with the build's checksum, and nothing more. Answers
/// its versions, in order; refuses ([`ExportError::LedgerNotTheBuilds`])
/// a store older or newer than the build, dirty, or changed. For a caller
/// that judges a store's ledger before opening it (an open migrates).
pub fn check_ledger(
    build: &[Migration],
    ledger: &[AppliedMigration],
) -> Result<Vec<u64>, ExportError> {
    let _ = (build, ledger);
    todo!("the ledger check")
}

/// Export `store` to `out`: the header, every row of its [`Export`] as a
/// record, then the end. Every row is checked first, and nothing is
/// written unless all pass ([`ExportError`]). Answers the end record
/// written.
pub async fn export<S, W>(store: &S, pool: &DescriptorPool, out: &mut W) -> Result<End, ExportError>
where
    S: Export + Migrations,
    W: Write,
{
    let _ = (store, pool, out);
    todo!("the export")
}
