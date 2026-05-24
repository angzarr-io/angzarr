//! Shared orchestration helper used by saga and process manager flows.

use crate::proto::CommandBook;

/// Ensure correlation_id is set on all command covers.
///
/// Fills in the correlation_id on any command whose cover has an empty one.
pub fn fill_correlation_id(commands: &mut [CommandBook], correlation_id: &str) {
    for command in commands.iter_mut() {
        if let Some(ref mut cover) = command.cover {
            if cover.correlation_id.is_empty() {
                cover.correlation_id = correlation_id.to_string();
            }
        }
    }
}

#[cfg(test)]
#[path = "shared.test.rs"]
mod tests;
