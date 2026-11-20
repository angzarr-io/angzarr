# Working rules

## Docs and code state the current design only

Version control holds the history; the docs and code do not. When a decision
changes, rewrite the text in place to say what is true now.

- No strikethroughs, "revised", "superseded", "originally", "previously",
  "moved to", "was X, now Y", or dated decision notes.
- No sections kept "for reference" after the design moved on — delete them.
- No code comments about what the code used to do.
- A rejected alternative may stay only as rationale for the current design
  ("not X, because …"), never as a story of how we got here.

## Few dependencies, no domain

These crates are what differs between operating systems, behind one seam,
and know nothing of any application that uses them: anything specific to
one belongs in that application's repository.

- **os** is the one crate that names an operating system (`just lint-os`,
  scripts/os-cfg-check.sh, holds the others to that). It depends on `fs4`
  (advisory locks: flock on unix, LockFileEx on Windows), `tokio`'s
  `signal` (the stop signal and the signals heard by name, made on the
  caller's runtime) and
  `typed-path` (every flavor's path logic on every host) and
  `process-wrap` (std frontend: a child as a process tree that one call
  kills, `os::process`; a process group via `nix` on unix, a job object
  via `windows` on Windows); on unix, `libc` (getpwuid_r for the
  account's home; setsid, getrlimit, close_range and fcntl for a
  detached spawn; renameat2 with RENAME_NOREPLACE on Linux and renamex_np
  with RENAME_EXCL on macOS for a rename that replaces nothing: calls std
  does not wrap; already in the graph through tokio and nix); on Windows,
  `junction`, `same-file` and `windows-sys` (its console, threading, COM
  and shell calls, MoveFileExW without MOVEFILE_REPLACE_EXISTING for a
  rename that replaces nothing, and CreateProcessW with a handle list for
  a detached spawn, which std cannot make), each pinned exactly in
  Cargo.toml. A post to a local stream socket at a path
  (`os::socket::post_local`: connect, write under a write timeout, close)
  is std's unix domain socket on unix, no dependency, and `Unsupported`
  on Windows, where no caller has measured an endpoint.
- **file-system** is the seam every filesystem operation goes through. It
  depends on `os`, `angzarr-diagnostic` (its errors' codes and args) and
  `tempfile` (an atomic write's staged file, a re-rooted filesystem's own
  temporary root). It and `os` alone call `std::fs` (clippy.toml's
  `disallowed-methods`, each one's `#![allow]` saying why). A directory's
  entries are made durable by `Fs::sync_dir` (after a rename into or out
  of it; `os::Os::sync_dir`: fsync on unix, nothing on Windows). Behind
  its `test-support` feature, `Recording` hands every call to another
  `Fs` and keeps, in order, each that changes the disk (`Call`), for a
  test that must see the order of its code's calls.
- **file-lock** depends on `os`, `file-system` and `angzarr-diagnostic`.

- **file-lock** and **file-system** are also what **store-jsonl** is built
  on. It is an angzarr store (`JsonlStore`) kept in one append-only JSON
  Lines journal, opened through an `Fs`, locked with `file-lock`, and
  single-process by construction (it claims none of the conformance suite's
  `shared_handles*` sections). It depends on `angzarr-ports` (the ports it
  implements, with the `memory` feature: the in-memory store the journal is
  replayed into; the `contract` feature for the tests) and
  `angzarr-proto-jsonl` (the JSON Lines codec), both angzarr core's, pinned
  with `angzarr-diagnostic`; on `prost` and `prost-types` (its journal
  messages, and the wire types it keeps as bytes), and `prost-reflect` (the
  dynamic messages and descriptors the codec reads and writes, the version
  `angzarr-proto-jsonl` uses). Its messages and descriptor are written by
  hand, so building it needs no `protoc`. For its tests, `tokio` (the
  conformance checks are async) with `macros` and `rt-multi-thread`.

`angzarr-diagnostic` and, for the tests only, `angzarr-presentation`
(its `test-support` feature: `english_for!`, a diagnostic's English
rendered from the catalogs) are angzarr core's shared crates, pinned by
one rev. A user pins angzarr core at that rev too, so its graph holds one
`angzarr-diagnostic`. A new dependency is named here with why, and passes
`just lint-deps` (deny.toml).

## Licence: Apache-2.0 OR BSD-3-Clause; not published

Licensed under either of Apache License 2.0 or BSD 3-Clause, at your option
(`LICENSE-APACHE`, `LICENSE-BSD`).
Every crate has `license = "Apache-2.0 OR BSD-3-Clause"` and `publish = false`, both inherited
from `[workspace.package]`. A new crate sets `license.workspace = true` and
`publish.workspace = true`.

## Errors are a key and args

Every error implements `angzarr_diagnostic::Diagnostic`: a stable code,
kebab-case under its crate's name (`file-lock-timeout`), and named args.
A code is written `code!("timeout")`, which prefixes the package name, so
no code spells its crate's name. Its `Display` is the code and args, never
English. The English is the crate's `locales/en-US/messages.ftl`, its ids
the codes' suffixes, which `build.rs` (`angzarr_diagnostic::build::catalogs`)
writes under the package name; `EN_US` is that output, named by
`CARGO_PKG_NAME`, one message per code using exactly its args; each crate's `diagnostic_samples()` (behind
`test-support`) holds one value of every error, and its tests hold the
catalog to them and each message's English to its expected text.

## Unicode in source is visible

The workspace denies three clippy lints, and every crate opts in with
`[lints] workspace = true`: `invisible_characters`, `unicode_not_nfc` and
`non_ascii_literal`. A non-ASCII character in a literal is written as an
escape (`"\u{e9}"`), so what the code holds is plain. There is no allow; a
new one needs a comment saying why.

## Tests live beside the code, in `*.test.rs`

A source file `foo.rs` keeps its tests in `foo.test.rs` in the same
directory, included at the end of `foo.rs`:

```rust
#[cfg(test)]
#[path = "foo.test.rs"]
mod tests;
```

A test's files are under a filesystem of its own, `Rooted::temp`, never
the real home or working directory.

## Use `just`

Every gate prints one line on a pass, `ok   <gate> (<scope>) <duration>`;
on a failure, a `FAIL` line, the extract worth reading, and the full log in
`target/logs/<gate>.log`. `VERBOSE=1 just <gate>` shows the full output.
`just test` runs cargo-nextest. `just check-windows` compiles every crate
for Windows on a Linux host (`rustup target add x86_64-pc-windows-gnu`);
CI builds and tests on Windows and macOS natively.

The complexity gate is clippy's `cognitive_complexity` (25),
`too_many_lines` (100) and `excessive_nesting` (6, clippy.toml), denied by
`just lint`. There is no `#[allow]` of them: a function over the gate is
refactored for cohesion, never split mechanically to pass.

## The git-hook gates are in gates/; the justfile gives their parameters

The gates and their recipes are `gates/gates.just` and
`gates/lefthook.yml`: the justfile imports the one and lefthook.yml extends
the other. The justfile keeps angzarr-platform's parameters for them
(`mutation_thresholds`, `workspace_wide`, `staged_tree`, `feature_rules`,
`features_prepare`) and its own `lint` (after `lint-os`); their tool configs are
`deny.toml`, `.gitleaks.toml` and `.cargo/mutants.toml`. `just
hooks-install` installs the hooks, once per clone (every worktree shares
them), and again after a change to the gates names a new hook
(`gates/MIGRATING.md`). They are a copy of angzarr-embedded's; a change
here does not reach it, nor taskloom's or ctxloom-rs's copies.

lefthook runs the gates one at a time, stopping at the first failure.
Gates 1 to 5 run at pre-commit, and pre-push runs the push's tests (`just
push-check`). Mutation (6) runs in no git hook: it runs while tests are
written and judged, at the end of an unattended run, and in the scheduled CI
full pass. A
failing gate is a finding: fix the cause and commit again; never
`--no-verify`, and never weaken a gate to get through.

1. **fmt** formats the staged `*.rs` files in the index (`just
   fmt-staged`); it does not refuse. A fully staged file's working copy gets
   the same content; a partially staged file's working copy is left exactly
   as it was, and named. `just fmt-check` is CI's check.
2. **secrets**: gitleaks over the staged changes (`.gitleaks.toml`). A fake
   secret in a test ends its line with `gitleaks:allow`.
3. **clippy**: `just lint`: clippy over the workspace.
4. **deps**: `just lint-deps`. A new licence, git source or advisory ignore
   is a change to `deny.toml`, with its reason.
5. **tests**: `just test-commit` runs `just test-staged`, which tests the
   crates with staged changes and their workspace dependents (normal, dev
   and build), on a throwaway worktree of HEAD plus the staged diff
   (`target/staged-tree`), so unstaged edits cannot pass or fail it. Red
   tests (failing tests, not a failed build) are committed on a branch other
   than the default one, with a warning that it cannot merge until they
   pass, and refused on the default branch.
6. **mutants** (no hook; by hand and in scheduled CI): `just mutants-push`
   runs `cargo mutants --in-diff` over what each ref given on stdin, as
   git's pre-push refs, adds (the diff from the remote's
   tip of that ref, or for a new branch from its merge base with the
   default branch, to the commit pushed), in that commit's tree, and holds
   every crate's new code to a 90% kill rate (`mutation_thresholds`; a new
   crate gets a threshold there before it gets code). Red tests fail it on
   every branch (no mutant can be judged against failing tests), as does a
   run that found mutants and tested fewer: a mutation run that judged
   nothing has shown nothing. The last run's mutants.out is kept in
   `target/logs/mutants-push/`. `just mutants-staged` makes the same
   judgement over the staged diff, by hand, and `just mutants-all` over the
   whole workspace at HEAD (the full pass; weekly in CI,
   `.github/workflows/mutation.yml`). A mutant judged equivalent is excluded by
   an `exclude_re` in `.cargo/mutants.toml` matching that one mutant, with a
   comment saying why, or by `#[cfg_attr(test, mutants::skip)]` beside it
   with the why (the `mutants` crate is a dev-dependency, so it links
   nothing), and never a lowered threshold.
