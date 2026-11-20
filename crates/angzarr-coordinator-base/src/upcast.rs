//! The upcast port, on core's messages, and its two drivers.
//!
//! Every payload a coordinator hands on is upcast first, through its
//! domain's [`Upcast`]: its upcaster sidecar, or an in-process table called
//! directly under the same contract. The port speaks core's messages, not a
//! transport's: pages (`EventPage`) or one lone payload (`Any`) in, payloads
//! keyed by the page's sequence (or by nothing, for the lone one) out.
//! [`upcast_pages`] and [`upcast_input`] are the two uses, and the one place
//! a port that cannot be reached becomes
//! `ANGZARR_REASON_UPCASTER_UNAVAILABLE`.

use std::fmt;

use angzarr_diagnostic::{code, Arg, Diagnostic, Report};
use angzarr_ports::{BoxFuture, MaybeSend, MaybeSync};
use angzarr_proto::v1::event_page::Body;
use angzarr_proto::v1::{reason_args, AngzarrReason, EventPage};
use prost_types::Any;

use crate::{flat_args, Code, Coded};

/// What an [`Upcast`] is asked to upcast.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Input<'a> {
    /// Stored pages of one stream, read back.
    Pages(&'a [EventPage]),
    /// One command's or fact's payload, before it is decoded.
    Lone(&'a Any),
}

/// What an upcast payload is keyed by: the sequence of the page it came
/// from, or nothing (the lone payload's).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Key {
    Sequence(u64),
    NoSequence,
}

/// One upcast payload and its key.
#[derive(Clone, Debug, PartialEq)]
pub struct Upcasted {
    pub key: Key,
    pub payload: Any,
}

/// What a domain's payloads are upcast through. Object-safe: the future is
/// boxed, `Send` on every target but wasm32.
pub trait Upcast: MaybeSend + MaybeSync {
    /// `input`, for `domain`, upcast.
    fn upcast<'a>(
        &'a self,
        domain: &'a str,
        input: Input<'a>,
    ) -> BoxFuture<'a, Result<Vec<Upcasted>, UpcastError>>;
}

/// Why a port could not upcast.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UpcastError {
    /// The upcaster could not be reached, or answered a retryable status:
    /// its failure as a report (code and args). Retry.
    Unavailable { cause: Report },
    /// The upcaster refused, under its own key and status: passed on as it
    /// is.
    Refused(Coded),
}

impl UpcastError {
    /// This failure for `domain`, as the coordinator answers it: an
    /// unreachable upcaster `ANGZARR_REASON_UPCASTER_UNAVAILABLE` (`domain`,
    /// and its failure as `cause` and `cause.<arg>`), a refusal as it is.
    pub fn coded(&self, domain: &str) -> Coded {
        match self {
            UpcastError::Unavailable { .. } => Coded::spec(
                AngzarrReason::UpcasterUnavailable,
                std::iter::once((reason_args::DOMAIN.to_string(), domain.to_string()))
                    .chain(flat_args(self)),
            ),
            UpcastError::Refused(coded) => coded.clone(),
        }
    }
}

impl fmt::Display for UpcastError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for UpcastError {
    fn code(&self) -> &str {
        match self {
            UpcastError::Unavailable { .. } => code!("upcast-unavailable"),
            UpcastError::Refused(_) => code!("upcast-refused"),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            UpcastError::Unavailable { cause } => vec![("cause", Arg::nested(cause))],
            UpcastError::Refused(coded) => vec![("reason", Arg::Str(coded.key.clone()))],
        }
    }
}

impl std::error::Error for UpcastError {}

/// One of every variant, each once: [`crate::diagnostic_samples`].
pub(crate) fn samples() -> Vec<UpcastError> {
    vec![
        UpcastError::Unavailable {
            cause: Report::new("annals-upcaster-io").arg("errno", 111u64),
        },
        UpcastError::Refused(Coded::new(
            Code::InvalidArgument,
            "player-upcast-bad-version",
            [],
        )),
    ]
}

/// `pages` of a `domain` stream, read from the store, upcast through
/// `port`: sent as one request, each answer put in the page of its
/// sequence (its sequence and meta kept; a page nothing answered, or a
/// framework meta-event, as it was). No pages, no request. A port that
/// fails is [`UpcastError::coded`] for `domain`.
pub async fn upcast_pages(
    port: &dyn Upcast,
    domain: &str,
    pages: &[EventPage],
) -> Result<Vec<EventPage>, Coded> {
    if pages.is_empty() {
        return Ok(Vec::new());
    }
    let answers = port
        .upcast(domain, Input::Pages(pages))
        .await
        .map_err(|error| error.coded(domain))?;
    Ok(pages
        .iter()
        .map(|page| upcast_page(page, &answers))
        .collect())
}

/// `payload`, an incoming command's or fact's, upcast through `port` for
/// `domain`: sent as a lone payload, the answer with no sequence taken. A
/// port that answers none leaves it as sent. A port that fails is
/// [`UpcastError::coded`] for `domain`.
pub async fn upcast_input(port: &dyn Upcast, domain: &str, payload: &Any) -> Result<Any, Coded> {
    let answers = port
        .upcast(domain, Input::Lone(payload))
        .await
        .map_err(|error| error.coded(domain))?;
    Ok(answers
        .into_iter()
        .find(|answer| answer.key == Key::NoSequence)
        .map_or_else(|| payload.clone(), |answer| answer.payload))
}

/// `page` with the payload `answers` give for its sequence (the first, if
/// there are several): an event only, a meta-event or an empty page is as
/// it was.
fn upcast_page(page: &EventPage, answers: &[Upcasted]) -> EventPage {
    let Some(Body::Event(_)) = page.body else {
        return page.clone();
    };
    match answers
        .iter()
        .find(|answer| answer.key == Key::Sequence(page.sequence))
    {
        Some(answer) => EventPage {
            body: Some(Body::Event(answer.payload.clone())),
            ..page.clone()
        },
        None => page.clone(),
    }
}

#[cfg(test)]
#[path = "upcast.test.rs"]
mod tests;
