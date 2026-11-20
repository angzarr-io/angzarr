//! The logic that differs between operating systems, for every flavor on
//! every host.

use std::ffi::OsStr;
use std::path::PathBuf;

use typed_path::{Utf8WindowsComponent, Utf8WindowsPath, Utf8WindowsPrefix};

use crate::Mode;

/// The extensions Windows tries for a command with none of its own when
/// PATHEXT is unset or empty (cmd.exe's default, shortened to what can be
/// launched without a shell).
pub const DEFAULT_PATHEXT: &str = ".COM;.EXE;.BAT;.CMD";

/// An operating system's rules, as data: which one a path, a variable or
/// a search follows.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Flavor {
    Unix,
    Windows,
}

impl Flavor {
    /// The user's home directory from the environment `var` reads: HOME on
    /// unix, USERPROFILE on Windows (as Go's `os.UserHomeDir`, so Rust and
    /// Go taskloom share one `~/.ctxloom`). A variable set empty is unset.
    /// For this crate's tests, which hold [`Flavor::home_var`] to Go's.
    #[cfg(test)]
    pub fn home_dir(self, var: &dyn Fn(&str) -> Option<String>) -> Option<String> {
        var(self.home_var()).filter(|home| !home.is_empty())
    }

    /// The variables a process needs to start and run at all, kept by
    /// whatever runs one with its environment otherwise cleared (a test
    /// isolating the binary): none on unix; on Windows the system's
    /// directories and the temporary one, without which a process cannot
    /// find its system libraries or start its sockets. Test support.
    #[cfg(any(test, feature = "test-support"))]
    pub fn startup_vars(self) -> &'static [&'static str] {
        match self {
            Flavor::Unix => &[],
            Flavor::Windows => &["SYSTEMROOT", "SYSTEMDRIVE", "WINDIR", "TEMP", "TMP"],
        }
    }

    /// The variable the user's home directory is read from: HOME on unix,
    /// USERPROFILE on Windows (Go's `os.UserHomeDir`). For a caller holding
    /// the environment as the process gives it (not UTF-8, perhaps), which
    /// reads it without loss.
    pub fn home_var(self) -> &'static str {
        match self {
            Flavor::Unix => "HOME",
            Flavor::Windows => "USERPROFILE",
        }
    }

    /// How a message refers to the variable `name`, as the flavor's shell
    /// does: `$PATH` on unix, `%PATH%` on Windows (cmd's).
    pub fn var_ref(self, name: &str) -> String {
        match self {
            Flavor::Unix => format!("${name}"),
            Flavor::Windows => format!("%{name}%"),
        }
    }

    /// The key an environment variable's name is compared by: itself on
    /// unix; ASCII-uppercased on Windows, whose names are case-insensitive.
    pub fn env_key(self, name: &str) -> String {
        match self {
            Flavor::Unix => name.to_string(),
            Flavor::Windows => name.to_ascii_uppercase(),
        }
    }

    /// Whether the variable `name` starts with `prefix`, compared as
    /// [`Flavor::env_key`] compares names (`TASKLOOM_CONFIG_*`).
    pub fn env_has_prefix(self, name: &str, prefix: &str) -> bool {
        self.env_key(name).starts_with(&self.env_key(prefix))
    }

    /// PATH's directories, in order: split at `:` on unix, at `;` on
    /// Windows outside double quotes (which are dropped). Empty entries are
    /// dropped: an empty entry never means the working directory (POSIX
    /// reads one as `.`, a command hijack waiting to happen).
    pub fn split_search_path(self, path: &str) -> Vec<String> {
        let entries: Vec<String> = match self {
            Flavor::Unix => path.split(':').map(str::to_string).collect(),
            Flavor::Windows => {
                let (mut entries, mut entry, mut quoted) = (Vec::new(), String::new(), false);
                for c in path.chars() {
                    match c {
                        '"' => quoted = !quoted,
                        ';' if !quoted => entries.push(std::mem::take(&mut entry)),
                        c => entry.push(c),
                    }
                }
                entries.push(entry);
                entries
            }
        };
        entries.into_iter().filter(|e| !e.is_empty()).collect()
    }

    /// PATH's directories as the process holds PATH (which need not be
    /// UTF-8): [`Flavor::split_search_path`] when it is UTF-8. When it is
    /// not, it was encoded by the host, whose own split (`split_paths`)
    /// keeps every entry whole, empty ones dropped all the same.
    pub fn split_search_path_os(self, path: &OsStr) -> Vec<PathBuf> {
        match path.to_str() {
            Some(text) => self
                .split_search_path(text)
                .into_iter()
                .map(PathBuf::from)
                .collect(),
            None => std::env::split_paths(path)
                .filter(|dir| !dir.as_os_str().is_empty())
                .collect(),
        }
    }

    /// The file names a command is looked up by in each PATH directory:
    /// the command itself on unix; on Windows the command alone when it
    /// already ends in one of PATHEXT's extensions (any case), else the
    /// command with each extension of `pathext` (unset, or naming none:
    /// [`DEFAULT_PATHEXT`]) in order.
    pub fn candidates(self, command: &str, pathext: Option<&str>) -> Vec<String> {
        if self == Flavor::Unix {
            return vec![command.to_string()];
        }
        let named =
            |p: &'static str| -> Vec<&str> { p.split(';').filter(|e| !e.is_empty()).collect() };
        let mut extensions: Vec<&str> = pathext
            .map(|p| p.split(';').filter(|e| !e.is_empty()).collect())
            .unwrap_or_default();
        if extensions.is_empty() {
            extensions = named(DEFAULT_PATHEXT);
        }
        let lower = command.to_ascii_lowercase();
        if extensions
            .iter()
            .any(|ext| lower.ends_with(&ext.to_ascii_lowercase()))
        {
            return vec![command.to_string()];
        }
        extensions
            .iter()
            .map(|ext| format!("{command}{ext}"))
            .collect()
    }

    /// The file name a program `command` is installed as, so a search of
    /// PATH ([`Flavor::candidates`]) finds it: the command itself on unix;
    /// with `.exe` on Windows. Test support: a test installs a program
    /// under it.
    #[cfg(any(test, feature = "test-support"))]
    pub fn program_file(self, command: &str) -> String {
        match self {
            Flavor::Unix => command.to_string(),
            Flavor::Windows => format!("{command}.exe"),
        }
    }

    /// Whether a regular file with this mode can be run: any execute bit on
    /// unix (`None`, bits unknown, is not); any file on Windows, where
    /// [`Flavor::candidates`] has already chosen by extension.
    pub fn is_executable(self, mode: Option<Mode>) -> bool {
        match self {
            Flavor::Unix => mode.is_some_and(|m| m.bits() & 0o111 != 0),
            Flavor::Windows => true,
        }
    }

    /// `path` as a person (and Go's index.yaml) writes it: unchanged on
    /// unix; on Windows without the verbatim prefix `canonicalize` adds
    /// (`\\?\C:\x` is `C:\x`, `\\?\UNC\srv\share\x` is `\\srv\share\x`).
    pub fn display_path(self, path: &str) -> String {
        if self == Flavor::Unix {
            return path.to_string();
        }
        let Some(Utf8WindowsComponent::Prefix(prefix)) =
            Utf8WindowsPath::new(path).components().next()
        else {
            return path.to_string();
        };
        let rest = &path[prefix.as_str().len()..];
        match prefix.kind() {
            Utf8WindowsPrefix::VerbatimDisk(drive) => format!("{drive}:{rest}"),
            Utf8WindowsPrefix::VerbatimUNC(server, share) => {
                format!(r"\\{server}\{share}{rest}")
            }
            _ => path.to_string(),
        }
    }

    /// The key two paths are the same path by: the path itself on unix; on
    /// Windows its [`Flavor::display_path`] with `/` as `\`, case folded
    /// (ASCII) and no trailing separator but the top's.
    pub fn path_key(self, path: &str) -> String {
        if self == Flavor::Unix {
            return path.to_string();
        }
        let key = self
            .display_path(path)
            .replace('/', "\\")
            .to_ascii_lowercase();
        match self.split_top(&key) {
            Some((top, rest)) => format!("{top}{}", rest.trim_end_matches('\\')),
            None => key.trim_end_matches('\\').to_string(),
        }
    }

    /// An absolute `path`'s top and the rest: `/` on unix; on Windows a
    /// drive's root (`C:\`) or a share's (`\\srv\share\`), verbatim or not,
    /// either separator accepted. `None` for a path that is not absolute,
    /// including Windows' drive-relative `C:x` and rooted `\x`.
    pub fn split_top(self, path: &str) -> Option<(String, String)> {
        if self == Flavor::Unix {
            let rest = path.strip_prefix('/')?;
            return Some(("/".to_string(), rest.to_string()));
        }
        let Some(Utf8WindowsComponent::Prefix(prefix)) =
            Utf8WindowsPath::new(path).components().next()
        else {
            return None;
        };
        match prefix.kind() {
            Utf8WindowsPrefix::Disk(_)
            | Utf8WindowsPrefix::VerbatimDisk(_)
            | Utf8WindowsPrefix::UNC(..)
            | Utf8WindowsPrefix::VerbatimUNC(..) => {}
            _ => return None,
        }
        let end = prefix.as_str().len();
        let separator = path[end..]
            .chars()
            .next()
            .filter(|c| matches!(c, '\\' | '/'))?;
        let top = end + separator.len_utf8();
        Some((path[..top].to_string(), path[top..].to_string()))
    }

    /// The top a path from it is spelled from when none is given (a test's
    /// working directory, say): `/` on unix, `C:\` on Windows.
    pub fn top(self) -> &'static str {
        match self {
            Flavor::Unix => "/",
            Flavor::Windows => r"C:\",
        }
    }

    /// `path`, an absolute path spelled the unix way (`/a/b`), as this
    /// flavor spells it from [`Flavor::top`]: unchanged on unix; on
    /// Windows from `C:\` with `\` separators (`C:\a\b`). For a test's
    /// fixture paths, written once and absolute on every host.
    ///
    /// # Panics
    ///
    /// When `path` does not start with `/`: a fixture is absolute.
    pub fn from_top(self, path: &str) -> String {
        let rest = path
            .strip_prefix('/')
            .unwrap_or_else(|| panic!("not a path from the top: {path:?}"));
        match self {
            Flavor::Unix => path.to_string(),
            Flavor::Windows => format!("{}{}", self.top(), rest.replace('/', "\\")),
        }
    }
}

#[cfg(test)]
#[path = "flavor.test.rs"]
mod tests;
