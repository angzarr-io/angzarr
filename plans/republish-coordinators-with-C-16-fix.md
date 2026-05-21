# Republish coordinator images so consumers pick up C-16 fix

**Filed:** 2026-05-20
**Filed by:** examples-python acceptance tier (see context below)
**Severity:** Blocks 3 cluster-tier scenarios in `examples-python/main/angzarr-project/features/example/poker/player.feature` and almost certainly any cross-aggregate PM-mediated cascade in every other consumer language.

## TL;DR

The C-16 fix landed on `main` in `651318cc` (2026-05-17). The
coordinator images currently pinned in every consumer's `values.yaml`
are `b98e8de@sha256:...` (2026-05-01) — pre-fix. The angzarr-process-
manager binary in those images panics with
`ColumnDecode { index: "\"edition\"", source: UnexpectedNullError }`
at `src/storage/postgres/event_store.rs:519` (line as of `b98e8de`,
the SHA the consumer is pinned to; on current `main` the same
decode site is `:546`) on the first event it reads from Postgres
for a correlation. No event is processed; the PM-driven cascade
halts silently from the consumer's perspective.

The fix is already in the source tree (`edition_from_db` helper
called from `get_by_correlation`); what's needed is a release tag +
image publish + downstream bumper pass.

## Repro from a consumer

```sh
# Stand up a kind cluster with the current pinned coordinator images
cd /home/babbitt/workspace/angzarr/examples-python/fix-eu-1375-1376
just up

# Run a player.feature scenario that exercises the PM-driven cascade
PLAYER_URL=localhost:31320 TABLE_URL=localhost:31321 \
HAND_URL=localhost:31322 TOURNAMENT_URL=localhost:31323 \
RESERVATION_URL=localhost:31324 ANGZARR_NAMESPACE=angzarr \
uv run behave --stage acceptance --tags=~@wip --no-capture \
  angzarr-project/features/example/poker/player.feature \
  --name "Rebuild state after full buy-in lifecycle"

# Observe the panic
kubectl logs -n angzarr deployment/pmg-reservation-pm -c angzarr | grep -A2 panic
```

Expected: scenario passes (bankroll deducted by the PM cascade).
Actual: assertion fails (`Expected bankroll 500, got 1000`) because
the PM never ran; sidecar log shows the panic above immediately
after the first `pm.handle` span.

## What triggers the panic

`PostgresEventStore::get_by_correlation` decodes the `edition` column
as a non-Option `String`. The events written by the gateway / aggregate
binaries omit `edition` (NULL = main timeline) for almost every event
they emit. The first such row sqlx returns to the PM crashes the
tokio worker.

Pre-fix code (still in `b98e8de`):
```rust
let edition: String = row.get("edition");  // panics on NULL
```

Post-fix (`main` as of 651318cc):
```rust
let edition: String = edition_from_db(row.get("edition"));
```

`edition_from_db` is the C-16 helper that maps `Option<String>` to
the canonical empty-string main-timeline sentinel.

## Path to consumer green

Two interlocking pieces; both already exist in the consumer
infrastructure per
[`memory/project_supply_chain_digest_pinning.md`](../../../examples-python/fix-eu-1375-1376/.claude/projects/-home-babbitt-workspace-angzarr/memory/project_supply_chain_digest_pinning.md).
(or wherever the supply-chain doc lives in your tree.)

1. **Cut a semver release on core via versionator.** Do NOT manually
   `git tag` or hand-edit `VERSION` / `Cargo.toml` / `Chart.yaml` /
   `proto/buf_version.yaml` — `release.yml`'s `validate` job rejects
   any tag that doesn't match all four. Use:

   ```sh
   ~/.local/bin/versionator bump patch   # 0.5.1 → 0.5.2
   ~/.local/bin/versionator release push  # commits, tags, pushes
   ```

   Tag selection — payload between `v0.5.1` (latest) and the chosen
   release point:
   - `651318cc` only → Tier-1 remediations (13 Critical), incl. C-16.
   - current `main` (`a324df58` at filing) → also picks up the two
     Tier-2 batches (`6766ad2f`, `b3004f12`), the final Tier-2 batch
     (`60be4eb1`), and the `just mutants` / Docker-rate-limit unblock
     (`fd05b600`). Recommended unless we have a reason to ship just
     the Critical batch — the Tier-2 work is already merged and
     gating it behind a separate release is gratuitous churn.

   The `release.yml` workflow triggers on the `v*.*.*` tag push and
   runs `validate` → `build-containers` (skaffold, retags semver) →
   `Notify consumer repos to bump pinned digests` → `publish-helm`.

2. **Let the per-repo bumper PR roll the consumer pins.**
   `bump-coordinator-digests.yml` on each `examples-*` repo
   (cf. consumer task #14 in examples-python's outstanding list)
   takes a `coordinator-images-published` repository-dispatch from
   core and opens a PR updating `values.yaml`. The PR's CI run
   exercises the new images against acceptance tests for that
   consumer; merges only if green.

There is no consumer-side code change required — only a pin bump,
which the bumper handles. If the bumper itself is broken on a
particular consumer (the examples-python task #14 flags one), fix
that bumper before re-triggering — otherwise the dispatch is a
no-op there.

## Why this isn't already remediated automatically

Memory entry [`project_cleanup_bumper_race`](../../../examples-python/fix-eu-1375-1376/.claude/projects/-home-babbitt-workspace-angzarr/memory/project_cleanup_bumper_race.md)
captures the race: core prunes old digests before consumer bumpers
can re-pin.

`.github/workflows/cleanup-images.yml:80` keeps `min-versions-to-keep: 10`
with `ignore-versions: '^v?\d+\.\d+\.\d+$|^latest$|^\d+\.\d+$|^\d+$'` —
**semver tags are preserved, SHA tags are not**. So a `main`-push
re-dispatch (which would re-pin consumers at the next short-SHA tag)
keeps the race alive: the new SHA is one of 10 rotating slots.

A semver release closes the race: `v0.5.2` is excluded from cleanup,
so a `values.yaml` digest resolved through the `0.5.2` tag stays
pullable indefinitely. This is the load-bearing reason to tag rather
than just push to `main` and let CI re-dispatch.

Worth confirming the most-recent published `angzarr-process-manager`
digest at GHCR is still pullable before relying on the dispatch —
otherwise the bumper PRs land on a tag the consumer can't pull.

## Out of scope here

* Re-running deep-review tier-1 remediation. Already shipped.
* Touching consumer code. The fix is purely "rebuild + repin."

## Pre-flight

* `CROSS_REPO_DISPATCH_TOKEN` repo secret is set on `angzarr-io/core`.
  Without it, `release.yml`'s dispatch step is a silent no-op (logs a
  warning, exits 0) and consumers fall back to their daily bumper
  cron — slower and less observable.
* No in-flight bumper PRs open on any `examples-*` repo from a prior
  cycle; close/rebase them first so the new ones are unambiguous.

## Acceptance criteria

Core release:
* Versionator bump committed; `VERSION`, `Cargo.toml`, `proto/buf_version.yaml`,
  and `deploy/k8s/helm/angzarr/Chart.yaml` all match the new tag.
* `release.yml` `validate` job green (it cross-checks all four files
  against the tag).
* `build-containers` pushes semver-retagged coordinator images
  (`*:0.5.2`, `*:0.5`, `*:0`) to GHCR.
* `publish-helm` publishes the chart at
  `oci://ghcr.io/angzarr-io/charts/angzarr:0.5.2`.
* `Notify consumer repos to bump pinned digests` logs success (no
  `::warning::Dispatch to <repo> failed`) for **all six** consumer
  repos: `angzarr-examples-{rust,cpp,go,java,csharp,python}`.

Consumer rollout:
* A `coordinator-images-published` dispatch reaches each
  `examples-*-lang` repo (visible in each repo's Actions tab).
* Each repo's bumper PR opens with `values.yaml` digests resolving
  through the `0.5.2` semver tag.
* The repro scenario above passes against the bumped consumer pin.
* No `ColumnDecode { index: "edition" }` panic in the deployed
  pmg-reservation sidecar's logs across a full
  `behave --stage acceptance` run.
