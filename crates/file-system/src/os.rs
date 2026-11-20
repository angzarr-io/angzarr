//! The real filesystem, paths as given.

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ::os::Mode;

use crate::{real, CwdError, Entry, Fs, Meta, Open, Perms};

/// The real filesystem: `std::fs` on each path as given, a relative one
/// joined to the working directory it was made with, on the host's
/// syscalls ([`::os::host`]).
#[derive(Clone, Debug)]
pub struct Os {
    /// The host's syscalls.
    os: Arc<dyn ::os::Os>,
    /// The working directory, or why there is none.
    cwd: Result<PathBuf, CwdError>,
}

impl Os {
    /// The real filesystem on `os`, resolving relative paths against
    /// `cwd`, an absolute path (`InvalidInput` otherwise).
    pub fn new(os: Arc<dyn ::os::Os>, cwd: &Path) -> io::Result<Os> {
        Ok(Os {
            os,
            cwd: Ok(absolute(cwd.to_path_buf())?),
        })
    }

    /// The real filesystem on `os` with no working directory, the
    /// process's could not be read (`why`, as `std::env::current_dir`
    /// failed): a relative path fails with the [`CwdError`] it makes of
    /// `why`; an absolute one is as given.
    pub fn without_cwd(os: Arc<dyn ::os::Os>, why: io::Error) -> Os {
        Os {
            os,
            cwd: Err(CwdError::of(&why)),
        }
    }

    /// `path` as given, or joined to the working directory when relative
    /// (failing with the [`CwdError`] when there is none).
    fn at(&self, path: &Path) -> io::Result<PathBuf> {
        if path.is_absolute() {
            return Ok(path.to_path_buf());
        }
        let cwd = self.cwd.as_ref().map_err(CwdError::to_io)?;
        absolute(cwd.join(path))
    }
}

/// `path`, refused as `InvalidInput` unless absolute (on Windows, `C:x`
/// is not).
fn absolute(path: PathBuf) -> io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path)
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("not an absolute path: {}", path.display()),
        ))
    }
}

impl Fs for Os {
    fn cwd(&self) -> Result<&Path, CwdError> {
        self.cwd.as_deref().map_err(Clone::clone)
    }

    fn os(&self) -> &dyn ::os::Os {
        &*self.os
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        std::fs::read(self.at(path)?)
    }

    fn read_to_string(&self, path: &Path) -> io::Result<String> {
        std::fs::read_to_string(self.at(path)?)
    }

    fn write_atomic(&self, path: &Path, bytes: &[u8], perms: Perms) -> io::Result<()> {
        real::write_atomic(&*self.os, &self.at(path)?, bytes, perms)
    }

    fn open(&self, path: &Path, how: Open) -> io::Result<File> {
        real::open(&*self.os, &self.at(path)?, how)
    }

    fn create_dir_all(&self, path: &Path, mode: Mode) -> io::Result<()> {
        real::create_dir_all(&*self.os, &self.at(path)?, mode)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.os.remove_link(&self.at(path)?)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_dir_all(self.at(path)?)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(self.at(from)?, self.at(to)?)
    }

    fn set_permissions(&self, path: &Path, mode: Mode) -> io::Result<()> {
        self.os.set_mode(&self.at(path)?, mode)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<Entry>> {
        real::read_dir(&self.at(path)?)
    }

    fn metadata(&self, path: &Path) -> io::Result<Meta> {
        real::metadata(&*self.os, &self.at(path)?)
    }

    fn symlink_metadata(&self, path: &Path) -> io::Result<Meta> {
        real::symlink_metadata(&*self.os, &self.at(path)?)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        std::fs::exists(self.at(path)?)
    }

    fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
        real::canonicalize(&*self.os, &self.at(path)?)
    }

    fn symlink_dir(&self, target: &Path, link: &Path) -> io::Result<()> {
        self.os.link_dir(target, &self.at(link)?)
    }

    fn read_link(&self, link: &Path) -> io::Result<PathBuf> {
        self.os.read_link(&self.at(link)?)
    }

    fn make_temp_dir(&self, parent: &Path, prefix: &str) -> io::Result<PathBuf> {
        real::make_temp_dir(&self.at(parent)?, prefix)
    }

    fn real_path(&self, path: &Path) -> io::Result<PathBuf> {
        self.at(path)
    }

    fn same_file(&self, file: &File, path: &Path) -> io::Result<bool> {
        self.os.same_file(file, &self.at(path)?)
    }
}

#[cfg(test)]
#[path = "os.test.rs"]
mod tests;
