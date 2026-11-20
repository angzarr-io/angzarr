//! The journal file: taking it, replaying it into the in-memory store,
//! repairing a torn tail, and appending to it.

use std::ffi::OsString;
use std::fs::File;
use std::io::{self, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use angzarr_diagnostic::Diagnostic;
use angzarr_ports::memory::MemoryStore;
use angzarr_proto_jsonl::{encode_line, Problem, Reader};
use file_lock::{lock_within, LockError, Locked};
use file_system::{Fs, Open, Perms};
use os::Mode;

use crate::entry::{self, Replay};
use crate::error::{OpenError, TornTail};
use crate::schema::{self, entry::Op, Schema};

/// The journal file's mode, before the umask: its owner's alone, since it
/// holds what the store holds.
const JOURNAL_MODE: Mode = Mode::PRIVATE_FILE;

/// Whether an append is synced before the call returns.
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub enum Durability {
    /// `sync_all` after every append: an operation that returned is on
    /// disk. The default.
    #[default]
    Durable,
    /// No sync: an operation that returned is in the operating system's
    /// hands, and survives the process but not a power cut. For tests and
    /// ephemeral stores.
    Unsynced,
}

/// Where an accepted operation's line is appended: the journal file, or in
/// a test a stand-in that fails or tears.
pub(crate) trait Sink: Send {
    /// Append `line` (it ends with its line feed) to the journal, whole or
    /// not at all, and make it durable if the store was opened durable.
    fn append(&mut self, line: &[u8]) -> io::Result<()>;
}

/// The journal file, opened for append.
struct FileSink {
    file: File,
    durability: Durability,
}

impl Sink for FileSink {
    fn append(&mut self, line: &[u8]) -> io::Result<()> {
        (&self.file).write_all(line)?;
        sync(&self.file, self.durability)
    }
}

/// Make what was appended durable, unless the store was opened without.
// An fsync changes nothing a test can observe short of a power cut.
#[cfg_attr(test, mutants::skip)]
fn sync(file: &File, durability: Durability) -> io::Result<()> {
    match durability {
        Durability::Durable => file.sync_all(),
        Durability::Unsynced => Ok(()),
    }
}

/// An opened journal.
pub(crate) struct Journal {
    /// Held for as long as the journal is in use.
    pub lock: Locked,
    pub sink: Box<dyn Sink>,
    pub recovered: Option<TornTail>,
}

/// The journal's header as a line, line feed included.
fn header_line(schema: &Schema) -> Result<String, String> {
    let header = schema::Entry {
        op: Some(Op::Header(schema::Header {
            format: schema::FORMAT,
        })),
    };
    encode(schema, &header)
}

/// An entry as a line, line feed included; or why it cannot be.
pub(crate) fn encode(schema: &Schema, entry: &schema::Entry) -> Result<String, String> {
    let message = schema.to_dynamic(entry).map_err(|e| e.to_string())?;
    let mut line = encode_line(&message).map_err(|e| e.to_string())?;
    line.push('\n');
    Ok(line)
}

/// `<journal>.lock`: the file the journal's lock is taken on, so that
/// repairing the journal (replacing it) cannot unlock it.
fn lock_path(journal: &Path) -> PathBuf {
    let mut name: OsString = journal.as_os_str().to_owned();
    name.push(".lock");
    PathBuf::from(name)
}

fn take_lock(fs: &dyn Fs, journal: &Path) -> Result<Locked, OpenError> {
    // Not waited for: a journal held is refused at once.
    lock_within(fs, &lock_path(journal), Duration::ZERO).map_err(|e| match e {
        LockError::Timeout { .. } => OpenError::InUse {
            path: journal.display().to_string(),
        },
        other => OpenError::Lock {
            path: journal.display().to_string(),
            ext_err_txt: other.to_string(),
        },
    })
}

/// Take the journal at `journal`, replay it into `inner` and repair its
/// tail; a journal that is not there, or is empty, is made with its header.
pub(crate) fn open(
    fs: &Arc<dyn Fs>,
    journal: &Path,
    durability: Durability,
    schema: &Schema,
    inner: &MemoryStore,
) -> Result<Journal, OpenError> {
    if !journal.is_absolute() {
        return Err(OpenError::RelativePath {
            path: journal.display().to_string(),
        });
    }
    let lock = take_lock(&**fs, journal)?;
    let recovered = replay(fs, journal, schema, inner)?;
    let file = fs
        .open(journal, Open::Append(JOURNAL_MODE))
        .map_err(|e| OpenError::io(journal, e))?;
    Ok(Journal {
        lock,
        sink: Box::new(FileSink { file, durability }),
        recovered,
    })
}

/// A new journal: the header, atomically (the file, and its directory entry,
/// are on disk before anything is appended to it).
fn make(fs: &Arc<dyn Fs>, journal: &Path, schema: &Schema) -> Result<(), OpenError> {
    let header = header_line(schema).map_err(|e| OpenError::io(journal, e))?;
    fs.write_atomic(journal, header.as_bytes(), Perms::KeepOr(JOURNAL_MODE))
        .map_err(|e| OpenError::io(journal, e))
}

/// Replay the journal into `inner`. Returns the torn tail it removed, if it
/// removed one.
fn replay(
    fs: &Arc<dyn Fs>,
    journal: &Path,
    schema: &Schema,
    inner: &MemoryStore,
) -> Result<Option<TornTail>, OpenError> {
    let exists = fs.exists(journal).map_err(|e| OpenError::io(journal, e))?;
    let len = if exists {
        fs.metadata(journal)
            .map_err(|e| OpenError::io(journal, e))?
            .len
    } else {
        0
    };
    if len == 0 {
        make(fs, journal, schema)?;
        return Ok(None);
    }
    let mut format = schema::FORMAT;
    let torn = read_entries(fs, journal, schema, inner, &mut format)?;
    let Some((line, bytes)) = torn else {
        upgrade(fs, journal, schema, format)?;
        return Ok(None);
    };
    truncate(fs, journal, schema, len, bytes)?;
    upgrade(fs, journal, schema, format)?;
    Ok(Some(TornTail {
        path: journal.to_path_buf(),
        line,
        bytes,
    }))
}

/// Make a journal that was written in an older format the current one, so
/// that what is appended to it is read as the format it is in: only the
/// header line changes, and the journal is replaced atomically. A journal
/// already in the current format is left alone.
fn upgrade(
    fs: &Arc<dyn Fs>,
    journal: &Path,
    schema: &Schema,
    format: u32,
) -> Result<(), OpenError> {
    if format == schema::FORMAT {
        return Ok(());
    }
    let header = header_line(schema).map_err(|e| OpenError::io(journal, e))?;
    let whole = fs.read(journal).map_err(|e| OpenError::io(journal, e))?;
    // The header is the first line, which the replay has read whole.
    let end = whole
        .iter()
        .position(|byte| *byte == b'\n')
        .map_or(whole.len(), |at| at + 1);
    let mut upgraded = header.into_bytes();
    upgraded.extend_from_slice(&whole[end..]);
    fs.write_atomic(journal, &upgraded, Perms::KeepOr(JOURNAL_MODE))
        .map_err(|e| OpenError::io(journal, e))
}

/// Read every line into `inner`, and the format of the journal's header into
/// `format`. Returns the torn last line's number and length, if the journal
/// ends in one.
fn read_entries(
    fs: &Arc<dyn Fs>,
    journal: &Path,
    schema: &Schema,
    inner: &MemoryStore,
    format: &mut u32,
) -> Result<Option<(usize, usize)>, OpenError> {
    let file = fs
        .open(journal, Open::Read)
        .map_err(|e| OpenError::io(journal, e))?;
    let path = journal.display().to_string();
    let mut reader = Reader::new(BufReader::new(file), schema.entry().clone());
    let mut line = 0;
    for item in &mut reader {
        let message = match item {
            Ok(message) => message,
            Err(e) => match e.problem {
                Problem::TornTail { bytes } => return Ok(Some((e.line, bytes))),
                Problem::ReadFailed { ext_err_txt } => {
                    return Err(OpenError::Io { path, ext_err_txt })
                }
                _ => {
                    return Err(OpenError::Corrupt {
                        path,
                        line: e.line,
                        reason: e.code().to_string(),
                    })
                }
            },
        };
        line += 1;
        let entry = schema
            .from_dynamic(&message)
            .map_err(|e| OpenError::Corrupt {
                path: path.clone(),
                line,
                reason: e.to_string(),
            })?;
        if line == 1 {
            *format = check_header(&path, &entry)?;
            continue;
        }
        entry::replay(inner, entry).map_err(|e| OpenError::ReplayRefused {
            path: path.clone(),
            line,
            reason: replay_reason(&e),
        })?;
    }
    Ok(None)
}

/// The first line must be a header of a format this build reads; returns it.
fn check_header(path: &str, first: &schema::Entry) -> Result<u32, OpenError> {
    match &first.op {
        Some(Op::Header(header))
            if (schema::OLDEST_FORMAT..=schema::FORMAT).contains(&header.format) =>
        {
            Ok(header.format)
        }
        Some(Op::Header(header)) => Err(OpenError::UnsupportedFormat {
            path: path.to_string(),
            found: header.format,
            supported: schema::FORMAT,
        }),
        _ => Err(OpenError::NotAJournal {
            path: path.to_string(),
        }),
    }
}

fn replay_reason(replay: &Replay) -> String {
    match replay {
        Replay::Malformed(field) => format!("malformed {field}"),
        Replay::Refused(refused) => refused.code().to_string(),
        Replay::Suspended => "suspended".to_string(),
    }
}

/// Remove the torn last `bytes` of the journal, which is `len` long: the
/// journal is replaced, atomically, by its good prefix. A journal torn in its
/// header is made again.
fn truncate(
    fs: &Arc<dyn Fs>,
    journal: &Path,
    schema: &Schema,
    len: u64,
    bytes: usize,
) -> Result<(), OpenError> {
    let keep = usize::try_from(len)
        .unwrap_or(usize::MAX)
        .saturating_sub(bytes);
    if keep == 0 {
        return make(fs, journal, schema);
    }
    let mut whole = fs.read(journal).map_err(|e| OpenError::io(journal, e))?;
    whole.truncate(keep);
    fs.write_atomic(journal, &whole, Perms::KeepOr(JOURNAL_MODE))
        .map_err(|e| OpenError::io(journal, e))
}
