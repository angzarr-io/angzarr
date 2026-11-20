//! Addressing a stream and wrapping a command for it, as every caller of
//! the kernel does: angzarr's wire types, built in one place.
//!
//! - [`CoverExt::natural`]: the stream of the entity a natural key names,
//!   its root the key's UUIDv5 under its domain ([`ids::compute_root`]),
//!   in the main edition.
//! - [`CommandBookExt::command`]: a client's command to a stream, with no
//!   ids and no expected sequence, its payload packed (or
//!   [`CommandBookExt::command_of`] a domain's payload, packing it); [`CommandBookExt::at`] sends it at the head its
//!   sender read, [`CommandBookExt::with_id`] under the sender's command
//!   id.
//!
//! Both are extension traits on the wire types, so a book is still a
//! [`CommandBook`]: nothing parallel to angzarr's types.

use angzarr_proto::ids;
use angzarr_proto::v1::{command_page, Command, CommandBook, CommandPage, Cover, Handling, Uuid};
use prost_types::Any;

use super::payload::Payload;

/// A [`Cover`] addressed by a natural key.
pub trait CoverExt: Sized {
    /// The main-edition stream of `domain` whose root is `key`'s
    /// ([`ids::compute_root`]): the stream of the one entity `key` names.
    /// A domain holding `':'` derives no root, so its cover has none, and
    /// the kernel's check refuses it ([`Malformed::NoRoot`](super::Malformed::NoRoot)).
    fn natural(domain: &str, key: &str) -> Self;
}

impl CoverExt for Cover {
    fn natural(domain: &str, key: &str) -> Cover {
        Cover {
            domain: domain.to_string(),
            root: ids::compute_root(domain, key).ok(),
            edition: None,
        }
    }
}

/// A [`CommandBook`] built for one command.
pub trait CommandBookExt: Sized {
    /// A client's command carrying `payload` (packed: a domain's
    /// [`Payload::to_any`](super::Payload::to_any), or [`pack`](super::pack)
    /// of a message) to `cover`: no command id (the kernel takes the
    /// [`Sampled`](super::Sampled) one), no correlation, no expected
    /// sequence (it is decided on the stream as it is when it is loaded).
    fn command(cover: Cover, payload: Any) -> Self;

    /// [`CommandBookExt::command`] of a domain's command payload, packed
    /// by its [`Payload::to_any`](super::Payload::to_any): the generated
    /// command enum's value, or one of its variants'.
    fn command_of(cover: Cover, payload: &impl Payload) -> Self;

    /// The same book, sent at `head`: its command's expected sequence, the
    /// head its sender read (`Command.handling`; decision 14.79), refused
    /// `ExpectedSequenceMismatch` unless it is the head when it is loaded
    /// (decision 14.94). A fact has no expected sequence: its book is
    /// unchanged.
    fn at(self, head: u64) -> Self;

    /// The same book, under the client's command id `command_id`.
    fn with_id(self, command_id: Uuid) -> Self;
}

impl CommandBookExt for CommandBook {
    fn command(cover: Cover, payload: Any) -> CommandBook {
        CommandBook {
            cover: Some(cover),
            page: Some(CommandPage {
                body: Some(command_page::Body::Command(Command {
                    payload: Some(payload),
                    handling: None,
                })),
                ..CommandPage::default()
            }),
        }
    }

    fn command_of(cover: Cover, payload: &impl Payload) -> CommandBook {
        CommandBook::command(cover, payload.to_any())
    }

    fn at(mut self, head: u64) -> CommandBook {
        if let Some(command_page::Body::Command(command)) =
            self.page.as_mut().and_then(|page| page.body.as_mut())
        {
            command.handling = Some(Handling {
                expected_sequence: Some(head),
            });
        }
        self
    }

    fn with_id(mut self, command_id: Uuid) -> CommandBook {
        self.page
            .get_or_insert_with(CommandPage::default)
            .command_id = Some(command_id);
        self
    }
}

#[cfg(test)]
#[path = "book.test.rs"]
mod tests;
