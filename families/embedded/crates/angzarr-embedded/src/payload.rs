//! Payloads are `google.protobuf.Any`, packed from a domain's prost
//! messages by their [`prost::Name`]. The type URL is the version: a new
//! version of an event (or a command) is a new message in a new package
//! (`pkg.v1.Foo`, then `pkg.v2.Foo`), and the old one is upcast by type URL
//! before it is decoded ([`Upcaster`](super::Upcaster)).
//!
//! A domain's events, commands and facts are each one Rust type, usually
//! an enum with one variant per message; [`Payload`] packs and unpacks it,
//! and [`Decoder`] is the table of type URLs unpacking reads from. A
//! domain's state is a single prost message, packed with [`pack`]
//! ([`State`](super::State)).

use std::collections::BTreeMap;

use prost::{Message, Name};
use prost_types::Any;

use angzarr_diagnostic::Report;

use super::error::PayloadError;
use super::wire::WireError;

/// A domain's events, commands or facts, as `Any`.
pub trait Payload: Sized {
    /// Pack this value as the message for its current type: [`pack`].
    fn to_any(&self) -> Any;

    /// Unpack a payload of one of the current types. Any other type URL is
    /// [`PayloadError::UnknownType`], never skipped: an older type must be
    /// upcast first.
    fn from_any(any: &Any) -> Result<Self, PayloadError>;

    /// The type URLs [`Payload::from_any`] reads: the current types. What
    /// an upcast chain must end at ([`Registry::check`](super::Registry::check)).
    fn type_urls() -> Vec<String>;
}

/// A domain that takes no facts (`type Fact = Infallible`): nothing to pack,
/// and every payload refused as [`PayloadError::UnknownType`].
impl Payload for std::convert::Infallible {
    fn to_any(&self) -> Any {
        match *self {}
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        Err(PayloadError::UnknownType {
            type_url: any.type_url.clone(),
        })
    }

    fn type_urls() -> Vec<String> {
        Vec::new()
    }
}

/// `message` as an `Any` whose type URL is its [`Name::type_url`]
/// (`/pkg.v1.Foo`, as angzarr writes them).
pub fn pack<M: Message + Name>(message: &M) -> Any {
    Any {
        type_url: M::type_url(),
        value: message.encode_to_vec(),
    }
}

/// `any` decoded as the wire message `W`, then converted to its domain type
/// `D`: what a generated payload enum's `from_any` does for each of its
/// types. Bytes that do not decode are [`PayloadError::Decode`]; a message
/// the domain type cannot hold is [`PayloadError::Invalid`].
pub fn decode_as<W, D>(any: &Any) -> Result<D, PayloadError>
where
    W: Message + Default,
    D: TryFrom<W, Error = WireError>,
{
    let wire = W::decode(&any.value[..]).map_err(|error| PayloadError::Decode {
        type_url: any.type_url.clone(),
        error,
    })?;
    D::try_from(wire).map_err(|error| PayloadError::Invalid {
        type_url: any.type_url.clone(),
        reason: Report::of(&error),
    })
}

/// The full name a type URL ends with: everything after its last `/`
/// (`type.googleapis.com/pkg.v1.Foo` and `/pkg.v1.Foo` both name
/// `pkg.v1.Foo`). Type URLs are compared by this.
pub fn type_name(type_url: &str) -> &str {
    type_url.rsplit('/').next().unwrap_or(type_url)
}

/// One message type's decoder.
type Decode<E> = Box<dyn Fn(&[u8]) -> Result<E, prost::DecodeError> + Send + Sync>;

/// The type URLs a domain's payload decodes from, each to its variant.
/// Built once:
///
/// ```ignore
/// Decoder::new()
///     .on::<v3::Opened>(|_| AccountEvent::Opened)
///     .on::<v3::Deposited>(|m| AccountEvent::Deposited(m.cents))
/// ```
pub struct Decoder<E> {
    by_name: BTreeMap<String, Decode<E>>,
}

impl<E> Default for Decoder<E> {
    fn default() -> Self {
        Decoder {
            by_name: BTreeMap::new(),
        }
    }
}

impl<E: 'static> Decoder<E> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Decode `M`'s type URL as `M`, then `wrap` it.
    pub fn on<M: Message + Name + Default + 'static>(mut self, wrap: fn(M) -> E) -> Self {
        self.by_name.insert(
            M::full_name(),
            Box::new(move |bytes| M::decode(bytes).map(wrap)),
        );
        self
    }

    /// `any` as the variant its type URL names.
    pub fn decode(&self, any: &Any) -> Result<E, PayloadError> {
        let decode = self.by_name.get(type_name(&any.type_url)).ok_or_else(|| {
            PayloadError::UnknownType {
                type_url: any.type_url.clone(),
            }
        })?;
        decode(&any.value).map_err(|error| PayloadError::Decode {
            type_url: any.type_url.clone(),
            error,
        })
    }

    /// Every type URL this decoder reads, in name order.
    pub fn type_urls(&self) -> Vec<String> {
        self.by_name.keys().map(|name| format!("/{name}")).collect()
    }
}

#[cfg(test)]
#[path = "payload.test.rs"]
mod tests;
