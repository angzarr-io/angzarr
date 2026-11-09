//! [`ListenError`]: why a listener could not be bound or kept serving.

use std::fmt;
use std::net::SocketAddr;
use std::path::Path;

use angzarr_diagnostic::{code, Arg, Diagnostic};

/// Why a listener could not be bound, or stopped serving.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ListenError {
    /// A listener declared loopback is bound to an address that is not a
    /// loopback one: refused at bind, so no request arrives as loopback
    /// from the network.
    LoopbackOnWideBind { addr: SocketAddr },
    /// A live listener holds the socket path.
    SocketInUse { path: String },
    /// The socket path exists and is not a socket: never removed.
    NotASocket { path: String },
    /// The socket's directory is not a directory we own with mode 0700.
    DirectoryNotPrivate { path: String },
    /// The socket path has no directory to put the socket in.
    NoDirectory { path: String },
    /// TLS was asked of a Unix socket listener: TLS is for TCP, and a Unix
    /// socket's protection is its mode and its directory.
    TlsOnUds { path: String },
    /// The TLS configuration was refused (no identity, or a certificate or
    /// key that does not parse), with the TLS layer's `error`.
    TlsConfig { error: String },
    /// A system call failed: `op` on `target`, with the system's `error`.
    Io {
        op: &'static str,
        target: String,
        error: String,
    },
}

impl ListenError {
    pub(crate) fn socket_in_use(path: &Path) -> ListenError {
        ListenError::SocketInUse {
            path: path.display().to_string(),
        }
    }

    pub(crate) fn not_a_socket(path: &Path) -> ListenError {
        ListenError::NotASocket {
            path: path.display().to_string(),
        }
    }

    pub(crate) fn directory_not_private(path: &Path) -> ListenError {
        ListenError::DirectoryNotPrivate {
            path: path.display().to_string(),
        }
    }

    pub(crate) fn no_directory(path: &Path) -> ListenError {
        ListenError::NoDirectory {
            path: path.display().to_string(),
        }
    }

    /// The error of `op` on `target`, as a closure for `map_err`.
    pub(crate) fn io<'a>(
        op: &'static str,
        target: impl fmt::Display + 'a,
    ) -> impl FnOnce(std::io::Error) -> ListenError + 'a {
        move |error| ListenError::Io {
            op,
            target: target.to_string(),
            error: error.to_string(),
        }
    }
}

impl fmt::Display for ListenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for ListenError {
    fn code(&self) -> &str {
        match self {
            ListenError::LoopbackOnWideBind { .. } => code!("listen-loopback-on-wide-bind"),
            ListenError::SocketInUse { .. } => code!("listen-socket-in-use"),
            ListenError::NotASocket { .. } => code!("listen-not-a-socket"),
            ListenError::DirectoryNotPrivate { .. } => code!("listen-directory-not-private"),
            ListenError::NoDirectory { .. } => code!("listen-no-directory"),
            ListenError::TlsOnUds { .. } => code!("listen-tls-on-uds"),
            ListenError::TlsConfig { .. } => code!("listen-tls-config"),
            ListenError::Io { .. } => code!("listen-io"),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            ListenError::LoopbackOnWideBind { addr } => vec![("addr", addr.to_string().into())],
            ListenError::SocketInUse { path }
            | ListenError::NotASocket { path }
            | ListenError::DirectoryNotPrivate { path }
            | ListenError::NoDirectory { path }
            | ListenError::TlsOnUds { path } => vec![("path", path.as_str().into())],
            ListenError::TlsConfig { error } => vec![("error", error.as_str().into())],
            ListenError::Io { op, target, error } => vec![
                ("op", (*op).into()),
                ("target", target.as_str().into()),
                ("error", error.as_str().into()),
            ],
        }
    }
}

impl std::error::Error for ListenError {}

/// One of every variant, each once: [`crate::diagnostic_samples`].
pub(crate) fn samples() -> Vec<ListenError> {
    vec![
        ListenError::LoopbackOnWideBind {
            addr: SocketAddr::from(([0, 0, 0, 0], 7000)),
        },
        ListenError::SocketInUse {
            path: "/run/user/1000/angzarr/a.sock".to_string(),
        },
        ListenError::NotASocket {
            path: "/run/user/1000/angzarr/a.sock".to_string(),
        },
        ListenError::DirectoryNotPrivate {
            path: "/run/user/1000/angzarr".to_string(),
        },
        ListenError::NoDirectory {
            path: "a.sock".to_string(),
        },
        ListenError::TlsOnUds {
            path: "/run/user/1000/angzarr/a.sock".to_string(),
        },
        ListenError::TlsConfig {
            error: "no identity".to_string(),
        },
        ListenError::Io {
            op: "bind",
            target: "127.0.0.1:7000".to_string(),
            error: "address in use".to_string(),
        },
    ]
}
