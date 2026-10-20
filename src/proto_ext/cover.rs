//! Stream identity read from anything that carries a [`Cover`].

use crate::proto::{CommandBook, Cover, EventBook, Projection};

use super::constants::UNKNOWN_DOMAIN;

/// Read access to the stream a message addresses: (domain, edition, root).
pub trait CoverExt {
    /// The cover, when set.
    fn cover(&self) -> Option<&Cover>;

    /// The domain, or [`UNKNOWN_DOMAIN`] when there is no cover.
    fn domain(&self) -> &str {
        self.cover()
            .map(|c| c.domain.as_str())
            .unwrap_or(UNKNOWN_DOMAIN)
    }

    /// The edition name: `None` is the main timeline (types.proto,
    /// `Cover.edition`: main is the unset edition).
    fn edition_name(&self) -> Option<&str> {
        self.cover().and_then(|c| c.edition.as_deref())
    }

    /// The root as a UUID, when it is set and exactly 16 bytes.
    fn root_uuid(&self) -> Option<uuid::Uuid> {
        self.cover()
            .and_then(|c| c.root.as_ref())
            .and_then(|r| uuid::Uuid::try_from(r).ok())
    }

    /// The root as canonical text, or an empty string when unusable.
    fn root_text(&self) -> String {
        self.root_uuid().map(|u| u.to_string()).unwrap_or_default()
    }
}

impl CoverExt for Cover {
    fn cover(&self) -> Option<&Cover> {
        Some(self)
    }
}

impl CoverExt for EventBook {
    fn cover(&self) -> Option<&Cover> {
        self.cover.as_ref()
    }
}

impl CoverExt for CommandBook {
    fn cover(&self) -> Option<&Cover> {
        self.cover.as_ref()
    }
}

impl CoverExt for Projection {
    fn cover(&self) -> Option<&Cover> {
        self.cover.as_ref()
    }
}

#[cfg(test)]
#[path = "cover.test.rs"]
mod tests;
