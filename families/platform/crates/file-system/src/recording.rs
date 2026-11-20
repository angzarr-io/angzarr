//! [`Recording`]: a filesystem that hands every call to another and keeps,
//! in order, each one that changes what is on disk, for a test that must
//! see the order of what its code did (a rename, then its directory's
//! sync). Behind `test-support`.

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use ::os::Mode;

use crate::{CwdError, Entry, Fs, Meta, Open, Perms};

/// A call that changes what is on disk, as its caller named its paths.
/// Kept whether it succeeded or failed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Call {
    WriteAtomic(PathBuf),
    /// An [`Fs::open`] that may create the file (any [`Open`] but `Read`).
    Create(PathBuf),
    CreateDirAll(PathBuf),
    RemoveFile(PathBuf),
    RemoveDirAll(PathBuf),
    Rename {
        from: PathBuf,
        to: PathBuf,
    },
    RenameNoreplace {
        from: PathBuf,
        to: PathBuf,
    },
    SyncDir(PathBuf),
    SetPermissions(PathBuf),
    SymlinkDir {
        target: PathBuf,
        link: PathBuf,
    },
    MakeTempDir(PathBuf),
}

/// Every call handed to `inner`; the ones that change the disk kept.
#[derive(Debug)]
pub struct Recording {
    inner: Arc<dyn Fs>,
    calls: Mutex<Vec<Call>>,
}

impl Recording {
    /// Over `inner`, nothing recorded yet.
    pub fn over(inner: Arc<dyn Fs>) -> Recording {
        Recording {
            inner,
            calls: Mutex::new(Vec::new()),
        }
    }

    /// What was called so far, in order.
    pub fn calls(&self) -> Vec<Call> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn keep(&self, call: Call) {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(call);
    }
}

impl Fs for Recording {
    fn cwd(&self) -> Result<&Path, CwdError> {
        self.inner.cwd()
    }

    fn os(&self) -> &dyn ::os::Os {
        self.inner.os()
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.inner.read(path)
    }

    fn read_to_string(&self, path: &Path) -> io::Result<String> {
        self.inner.read_to_string(path)
    }

    fn write_atomic(&self, path: &Path, bytes: &[u8], perms: Perms) -> io::Result<()> {
        self.keep(Call::WriteAtomic(path.to_path_buf()));
        self.inner.write_atomic(path, bytes, perms)
    }

    fn open(&self, path: &Path, how: Open) -> io::Result<File> {
        if how != Open::Read {
            self.keep(Call::Create(path.to_path_buf()));
        }
        self.inner.open(path, how)
    }

    fn create_dir_all(&self, path: &Path, mode: Mode) -> io::Result<()> {
        self.keep(Call::CreateDirAll(path.to_path_buf()));
        self.inner.create_dir_all(path, mode)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.keep(Call::RemoveFile(path.to_path_buf()));
        self.inner.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.keep(Call::RemoveDirAll(path.to_path_buf()));
        self.inner.remove_dir_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.keep(Call::Rename {
            from: from.to_path_buf(),
            to: to.to_path_buf(),
        });
        self.inner.rename(from, to)
    }

    fn rename_noreplace(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.keep(Call::RenameNoreplace {
            from: from.to_path_buf(),
            to: to.to_path_buf(),
        });
        self.inner.rename_noreplace(from, to)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.keep(Call::SyncDir(dir.to_path_buf()));
        self.inner.sync_dir(dir)
    }

    fn set_permissions(&self, path: &Path, mode: Mode) -> io::Result<()> {
        self.keep(Call::SetPermissions(path.to_path_buf()));
        self.inner.set_permissions(path, mode)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<Entry>> {
        self.inner.read_dir(path)
    }

    fn metadata(&self, path: &Path) -> io::Result<Meta> {
        self.inner.metadata(path)
    }

    fn symlink_metadata(&self, path: &Path) -> io::Result<Meta> {
        self.inner.symlink_metadata(path)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        self.inner.exists(path)
    }

    fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
        self.inner.canonicalize(path)
    }

    fn symlink_dir(&self, target: &Path, link: &Path) -> io::Result<()> {
        self.keep(Call::SymlinkDir {
            target: target.to_path_buf(),
            link: link.to_path_buf(),
        });
        self.inner.symlink_dir(target, link)
    }

    fn read_link(&self, link: &Path) -> io::Result<PathBuf> {
        self.inner.read_link(link)
    }

    fn make_temp_dir(&self, parent: &Path, prefix: &str) -> io::Result<PathBuf> {
        self.keep(Call::MakeTempDir(parent.to_path_buf()));
        self.inner.make_temp_dir(parent, prefix)
    }

    fn real_path(&self, path: &Path) -> io::Result<PathBuf> {
        self.inner.real_path(path)
    }

    fn same_file(&self, file: &File, path: &Path) -> io::Result<bool> {
        self.inner.same_file(file, path)
    }
}

#[cfg(test)]
#[path = "recording.test.rs"]
mod tests;
