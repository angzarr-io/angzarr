//! Each operation on an OS path, `std::fs` underneath and the host's
//! [`Os`] for what differs between operating systems: what
//! [`crate::Os`] and [`crate::Rooted`] both do once they know where a
//! caller's path is.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use ::os::{Flavor, Mode, Os};

use crate::{Entry, Kind, Meta, Open, Perms};

/// `path` as a person writes it ([`::os::Flavor::display_path`]): on
/// Windows without the verbatim prefix (`\\?\`) `std::fs::canonicalize`
/// adds, which git, Go and a junction's target never spell. A path that is
/// not UTF-8 is left as it is.
pub(crate) fn plain(flavor: Flavor, path: PathBuf) -> PathBuf {
    match path.to_str() {
        Some(text) => PathBuf::from(flavor.display_path(text)),
        None => path,
    }
}

/// `path` with every link and `.`/`..` resolved, spelled [`plain`].
pub(crate) fn canonicalize(os: &dyn Os, path: &Path) -> io::Result<PathBuf> {
    Ok(plain(os.flavor(), fs::canonicalize(path)?))
}

#[cfg(test)]
#[path = "real.test.rs"]
mod tests;

pub(crate) fn write_atomic(os: &dyn Os, path: &Path, bytes: &[u8], perms: Perms) -> io::Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "a path with no directory"))?;
    let mode = match perms {
        Perms::Exact(mode) => mode,
        Perms::KeepOr(mode) => match fs::metadata(path) {
            Ok(meta) => os.mode_of(&meta).unwrap_or(mode),
            Err(e) if e.kind() == io::ErrorKind::NotFound => mode,
            Err(e) => return Err(e),
        },
    };
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    // Dropped unpersisted (an error below), the temporary file is removed.
    let mut staged = tempfile::Builder::new()
        .prefix(&format!(".{name}."))
        .suffix(".tmp")
        .tempfile_in(dir)?;
    staged.write_all(bytes)?;
    os.set_file_mode(staged.as_file(), mode)?;
    staged.as_file().sync_all()?;
    staged.persist(path).map_err(|e| e.error)?;
    os.sync_dir(dir)
}

pub(crate) fn open(os: &dyn Os, path: &Path, how: Open) -> io::Result<File> {
    let mut options = OpenOptions::new();
    let mode = match how {
        Open::Read => {
            options.read(true);
            None
        }
        Open::Create(mode) => {
            options.read(true).write(true).create(true).truncate(false);
            Some(mode)
        }
        Open::CreateNew(mode) => {
            options.read(true).write(true).create_new(true);
            Some(mode)
        }
        Open::Append(mode) => {
            options.append(true).create(true);
            Some(mode)
        }
    };
    if let Some(mode) = mode {
        os.open_mode(&mut options, mode);
    }
    options.open(path)
}

pub(crate) fn create_dir_all(os: &dyn Os, path: &Path, mode: Mode) -> io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    os.dir_mode(&mut builder, mode);
    builder.create(path)
}

pub(crate) fn read_dir(path: &Path) -> io::Result<Vec<Entry>> {
    let mut entries = fs::read_dir(path)?
        .map(|entry| {
            let entry = entry?;
            Ok(Entry {
                name: entry.file_name(),
                kind: kind_of(entry.file_type()?),
            })
        })
        .collect::<io::Result<Vec<_>>>()?;
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(entries)
}

pub(crate) fn metadata(os: &dyn Os, path: &Path) -> io::Result<Meta> {
    meta(os, &fs::metadata(path)?)
}

pub(crate) fn symlink_metadata(os: &dyn Os, path: &Path) -> io::Result<Meta> {
    meta(os, &fs::symlink_metadata(path)?)
}

pub(crate) fn make_temp_dir(parent: &Path, prefix: &str) -> io::Result<PathBuf> {
    Ok(tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(parent)?
        .keep())
}

fn meta(os: &dyn Os, meta: &fs::Metadata) -> io::Result<Meta> {
    let kind = kind_of(meta.file_type());
    let mode = os.mode_of(meta);
    Ok(Meta {
        kind,
        len: meta.len(),
        executable: kind == Kind::File && os.flavor().is_executable(mode),
        mode,
        modified: meta.modified()?,
    })
}

fn kind_of(file_type: fs::FileType) -> Kind {
    if file_type.is_symlink() {
        Kind::Symlink
    } else if file_type.is_dir() {
        Kind::Dir
    } else if file_type.is_file() {
        Kind::File
    } else {
        Kind::Other
    }
}
