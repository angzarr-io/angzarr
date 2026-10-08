# Review 2026-09 remediation — working rules

Source of findings: `INDEX.md` (cross-repo index), `findings.jsonl` (canonical
`X-###` findings), per-repo reports in this directory, and per-agent work lists
in `work/*.jsonl`.

## Decisions already made (do not re-litigate)

- **Branching:** one integration branch per repo. Core: `rem/review-2026-09`
  (contains every July `rem/c*` branch and the D-7 basis_seq work). Agents work
  on a short-lived local branch cut from it and hand back for merge; nothing is
  pushed without the user's say-so.
- **MERGE_COMMUTATIVE = field-overlap merge.** A stale-sequence command is
  accepted when the fields it mutates don't overlap the fields changed in the
  window basis..actual; only overlap rejects (retryable FAILED_PRECONDITION).
  `types.proto` is right; `merge_strategy.feature` is being rewritten.
- **Enum numbering is unchanged** (ASYNC / FAIL_FAST / COMMUTATIVE are the zero
  values). No `*_UNSPECIFIED` variants.
- **Poker example findings are not fixed** — the example is being replaced by
  blackjack. Skip any finding whose only repo is `examples-rust`.
- **Saga sequence model:** sagas/PMs emit `angzarr_deferred` commands; the
  framework stamps sequences and `basis_seq` carries the destination head the
  emitter observed.

## Per-finding workflow

1. Re-verify the finding against the CURRENT branch (several were fixed by the
   July `rem/c*` branches now merged). If already fixed, record evidence and
   move on.
2. Write the failing test FIRST and run it red. Framework-contract behaviour
   (anything another language must also do) gets a cucumber scenario in
   angzarr-project first; specifics (status codes, exact messages) go in unit
   tests.
3. Fix. Run the test green.
4. Tests must assert real behaviour — no `let _ = ...`, no tautologies.
5. Mutation-test touched files after green (core:
   `just mutants <file>`);
   target ≥ 90% kill on behaviour-relevant mutants.
6. Commit per finding (or tight group) with the `X-###` IDs in the message.
   The pre-commit gate (fmt + lint + full unit tests) must pass. Never
   `--no-verify`, never `LEFTHOOK=0`.
7. Append a line to `work/STATUS.md`: `X-### | fixed|already-fixed|wontfix | commit | note`.

## Code rules

- Comments describe what the code IS, never what it replaced or why it
  changed — history goes in commit messages.
- Fix pre-existing breakage you touch; don't leave it red.
- In core, `.gitignore` has `**/bin/`: use `rg -uu` (or `--no-ignore`) for any
  "never called" search, or `src/bin` is silently skipped.
- Use `just` recipes (they run in containers). Don't run buf/protoc directly.

## Submodules

`angzarr-project` is pinned to a commit that exists only in the local repo at
`/home/babbitt/workspace/angzarr/angzarr-project` (branch `fix/review-2026-09`).
In a fresh worktree:

```
git submodule update --init sererr
git submodule update --init angzarr-project 2>/dev/null || true   # fails: commit not on GitHub
git -C angzarr-project fetch /home/babbitt/workspace/angzarr/angzarr-project fix/review-2026-09
git -C angzarr-project checkout -q "$(git ls-tree HEAD angzarr-project | awk '{print $3}')"
just check-submodules-clean
```

Never use `--recursive` (nested vendored submodules break the
submodule-clean gate). Never edit files inside a submodule — spec changes go
to the angzarr-project worktree at
`/home/babbitt/workspace/angzarr/angzarr-project.review`.
