//! The top an absolute path of the compile target starts from, as
//! `std::path` parses it: `/` on unix; a drive (`C:\`) or a share
//! (`\\server\share\`), verbatim or not, on Windows. The file-system
//! seam's re-rooting (`Rooted`) names a caller's path from it.

use std::path::{Component, Components, Path, PathBuf};

#[cfg(windows)]
use std::ffi::OsString;

/// The top a path starts from: `/` on unix.
#[cfg(unix)]
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Top;

#[cfg(unix)]
impl Top {
    /// The top `components` start from, taken from them; `None` when they
    /// are not absolute.
    pub fn take(components: &mut Components) -> Option<Top> {
        matches!(components.next(), Some(Component::RootDir)).then_some(Top)
    }

    /// The top named by the first of a re-rooted path's `components` (what
    /// follows the root), taken from them: on unix, nothing to take.
    pub fn from_real(_components: &mut Components) -> Option<Top> {
        Some(Top)
    }

    /// The top as a path names it.
    pub fn caller(&self) -> PathBuf {
        PathBuf::from("/")
    }

    /// Where the top is under `root`.
    pub fn under(&self, root: &Path) -> PathBuf {
        root.to_path_buf()
    }
}

/// The top a path starts from on Windows: a drive (held upper-case) or a
/// share.
#[cfg(windows)]
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Top {
    Disk(u8),
    Unc(OsString, OsString),
}

// Compiled only on Windows, where CI's Windows runner tests it. Mutation
// testing runs on unix, where its mutants cannot be killed.
#[cfg(windows)]
#[cfg_attr(test, mutants::skip)]
impl Top {
    pub fn take(components: &mut Components) -> Option<Top> {
        use std::path::Prefix;
        let Some(Component::Prefix(prefix)) = components.next() else {
            return None;
        };
        let top = match prefix.kind() {
            Prefix::Disk(d) | Prefix::VerbatimDisk(d) => Top::Disk(d.to_ascii_uppercase()),
            Prefix::UNC(server, share) | Prefix::VerbatimUNC(server, share) => {
                Top::Unc(server.to_owned(), share.to_owned())
            }
            _ => return None,
        };
        matches!(components.next(), Some(Component::RootDir)).then_some(top)
    }

    pub fn from_real(components: &mut Components) -> Option<Top> {
        let mut next = || match components.next() {
            Some(Component::Normal(part)) => Some(part.to_owned()),
            _ => None,
        };
        let first = next()?;
        if first == "UNC" {
            return Some(Top::Unc(next()?, next()?));
        }
        match first.as_encoded_bytes() {
            [d] if d.is_ascii_uppercase() => Some(Top::Disk(*d)),
            _ => None,
        }
    }

    pub fn caller(&self) -> PathBuf {
        match self {
            Top::Disk(d) => PathBuf::from(format!("{}:\\", char::from(*d))),
            Top::Unc(server, share) => {
                let mut top = OsString::from(r"\\");
                top.push(server);
                top.push(r"\");
                top.push(share);
                top.push(r"\");
                PathBuf::from(top)
            }
        }
    }

    pub fn under(&self, root: &Path) -> PathBuf {
        match self {
            Top::Disk(d) => root.join(char::from(*d).to_string()),
            Top::Unc(server, share) => root.join("UNC").join(server).join(share),
        }
    }
}

#[cfg(test)]
#[path = "top.test.rs"]
mod tests;
