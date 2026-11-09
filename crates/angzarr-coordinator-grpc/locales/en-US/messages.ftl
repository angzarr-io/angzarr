# Messages for angzarr-coordinator-grpc's errors. Each id is an error's code
# without the crate's name, which the build prefixes
# (angzarr_diagnostic::build); the variables are its args.
# The crate exports this as angzarr_coordinator_grpc::EN_US.

authorization-repeated = the request carries more than one authorization header
authorization-not-text = the authorization header is not text
authorization-not-bearer = the authorization header is not a bearer credential
authorization-empty = the bearer credential is empty
listen-loopback-on-wide-bind = a listener declared loopback is bound to { $addr }, which is not a loopback address
listen-socket-in-use = another process is listening on { $path }
listen-not-a-socket = { $path } exists and is not a socket
listen-directory-not-private = the directory of the socket, { $path }, is not a directory we own with mode 0700
listen-no-directory = the socket path { $path } has no directory
listen-tls-on-uds = TLS was asked of the Unix socket { $path }; TLS is for a TCP listener
listen-tls-config = the TLS configuration was refused: { $error }
listen-io = { $op } of { $target } failed: { $error }
