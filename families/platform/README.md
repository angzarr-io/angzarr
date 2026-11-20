# angzarr-platform

What differs between operating systems, behind one seam, for Rust
applications:

- **`os`**: path, environment and search-path logic for every flavor
  (Unix and Windows paths both testable on any host), and the host's
  syscalls (modes, links, file identity, directory sync, advisory locks,
  the stop signal) behind one trait made once at startup.
- **`file-system`**: the one seam every filesystem operation goes through,
  an `Fs`: the real filesystem, paths as given (`Os`), or the same
  `std::fs` re-rooted under a directory (`Rooted`, a temporary one in
  tests).
- **`file-lock`**: advisory file locks, taken one way: a lock on a lock
  file, re-checked against the file at its path, waited for a bounded time.

Every error is an `angzarr-diagnostic` `Diagnostic` (a stable code and
named args), its en-US message in the crate's Fluent catalog
(`locales/en-US/messages.ftl`, its ids prefixed with the package name at
build time, exported as `EN_US`).

## Use

Git dependencies, pinned by commit:

```toml
[dependencies]
os = { git = "https://github.com/angzarr-io/angzarr-platform", rev = "<commit>" }
file-system = { git = "https://github.com/angzarr-io/angzarr-platform", rev = "<commit>" }
file-lock = { git = "https://github.com/angzarr-io/angzarr-platform", rev = "<commit>" }
```

They depend on angzarr core's `angzarr-diagnostic` at the rev this
repository's Cargo.toml pins: pin angzarr core at that rev too, so the
graph holds one `angzarr-diagnostic`. Each crate's `test-support` feature,
for a dev-dependency, turns on the test-only API other crates' tests use
(`os::fixture` and the rest; `diagnostic_samples`).

## Build

`just build`, `just lint`, `just test`, `just fmt-check`, `just lint-deps`;
`just check-windows` compiles every crate for Windows on a Linux host.
`just hooks-install` installs the git hooks. See CLAUDE.md.

## Licence

Licensed under either of Apache License 2.0 or BSD 3-Clause, at your
option (`LICENSE-APACHE` and `LICENSE-BSD`).
