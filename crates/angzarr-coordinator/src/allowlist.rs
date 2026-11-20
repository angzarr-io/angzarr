//! [`Allowlist`]: the closed set of what a principal may send and which
//! reads it may use, as DATA the platform enforces. An application's code
//! cannot over-grant: what is not listed is refused, whatever its `owns`
//! or `identify` does.

use std::collections::BTreeSet;

use angzarr_proto::v1::{command_page, reaction_delivery, CommandBook, ReactionDelivery};

use crate::Denied;

/// What a sent body is: a command (asks the aggregate to decide) or a fact
/// (records what already happened). Distinct kinds: a principal that may
/// send one may not, by that, send the other.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BodyKind {
    Command,
    Fact,
}

impl BodyKind {
    /// As an arg: `"command"` or `"fact"`.
    pub fn as_str(&self) -> &'static str {
        match self {
            BodyKind::Command => "command",
            BodyKind::Fact => "fact",
        }
    }
}

/// A read operation a principal may use.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReadOp {
    Events,
    Correlated,
    ListStreams,
    Follow,
}

impl ReadOp {
    /// As an arg: `"events"`, `"correlated"`, `"list_streams"` or `"follow"`.
    pub fn as_str(&self) -> &'static str {
        match self {
            ReadOp::Events => "events",
            ReadOp::Correlated => "correlated",
            ReadOp::ListStreams => "list_streams",
            ReadOp::Follow => "follow",
        }
    }
}

/// The closed set: (body kind, type URL) pairs a principal may send, and
/// the read operations it may use. Empty by default (nothing is allowed);
/// a type URL is matched exactly, with no pattern, prefix or case folding.
/// Built by the application's `identify` for the principal it returns, and
/// read from nowhere else: a credential's metadata cannot widen it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Allowlist {
    /// Open for everything: only [`Trusted`](crate::Trusted) mints one.
    unrestricted: bool,
    send: BTreeSet<(BodyKind, String)>,
    reads: BTreeSet<ReadOp>,
}

impl Allowlist {
    /// Nothing allowed.
    pub fn none() -> Allowlist {
        Allowlist::default()
    }

    /// Everything allowed: no list at all. Crate-private: the one holder is
    /// [`Trusted`](crate::Trusted), whose point is that it identifies nobody.
    pub(crate) fn unrestricted() -> Allowlist {
        Allowlist {
            unrestricted: true,
            ..Allowlist::default()
        }
    }

    /// Also allow sending a `kind` body of type `type_url`.
    pub fn send(mut self, kind: BodyKind, type_url: impl Into<String>) -> Allowlist {
        self.send.insert((kind, type_url.into()));
        self
    }

    /// Also allow the read operation `op`.
    pub fn read(mut self, op: ReadOp) -> Allowlist {
        self.reads.insert(op);
        self
    }

    /// Whether a `kind` body of type `type_url` may be sent, or the
    /// [`Denied`] that says why not: no body of that kind is allowed at all
    /// ([`Denied::BodyKindNotAllowed`]), or that type is not
    /// ([`Denied::TypeNotAllowed`]). An empty type URL is never allowed.
    pub fn permits_body(&self, kind: BodyKind, type_url: &str) -> Result<(), Denied> {
        if self.unrestricted {
            return Ok(());
        }
        if !self.send.iter().any(|(listed, _)| *listed == kind) {
            return Err(Denied::BodyKindNotAllowed {
                kind: kind.as_str(),
            });
        }
        if !type_url.is_empty() && self.send.contains(&(kind, type_url.to_string())) {
            Ok(())
        } else {
            Err(Denied::TypeNotAllowed {
                type_url: type_url.to_string(),
            })
        }
    }

    /// Whether `book`'s body may be sent: its kind and its type URL
    /// ([`permits_body`](Allowlist::permits_body)). A book with no body has
    /// no kind, and is refused as one.
    pub fn permits(&self, book: &CommandBook) -> Result<(), Denied> {
        match book.page.as_ref().and_then(|page| page.body.as_ref()) {
            Some(command_page::Body::Command(command)) => self.permits_body(
                BodyKind::Command,
                url(command.payload.as_ref().map(|any| any.type_url.as_str())),
            ),
            Some(command_page::Body::Fact(fact)) => self.permits_body(
                BodyKind::Fact,
                url(fact.payload.as_ref().map(|any| any.type_url.as_str())),
            ),
            None if self.unrestricted => Ok(()),
            None => Err(Denied::BodyKindNotAllowed { kind: "none" }),
        }
    }

    /// Whether `delivery`'s body may be sent, as `permits` does a book's.
    pub fn permits_delivery(&self, delivery: &ReactionDelivery) -> Result<(), Denied> {
        match delivery.body.as_ref() {
            Some(reaction_delivery::Body::Command(any)) => {
                self.permits_body(BodyKind::Command, &any.type_url)
            }
            Some(reaction_delivery::Body::Fact(any)) => {
                self.permits_body(BodyKind::Fact, &any.type_url)
            }
            None if self.unrestricted => Ok(()),
            None => Err(Denied::BodyKindNotAllowed { kind: "none" }),
        }
    }

    /// Whether the read operation `op` may be used.
    pub fn permits_read(&self, op: ReadOp) -> Result<(), Denied> {
        if self.unrestricted || self.reads.contains(&op) {
            Ok(())
        } else {
            Err(Denied::SelectionNotAllowed {
                operation: op.as_str(),
            })
        }
    }
}

/// A payload's type URL; none is the empty URL, which is never allowed.
fn url(type_url: Option<&str>) -> &str {
    type_url.unwrap_or("")
}

#[cfg(test)]
#[path = "allowlist.test.rs"]
mod tests;
