# Plan: CLI-scaffolded implementors, blackjack in every language, acceptance on deployed aggregates

Status: DRAFT for approval (2026-10-01). Supersedes the per-language parts of
`blackjack-plan.md` §5 where they differ (that plan predates the decision that
client repos own codegen).

## 0. Goal and definition of done

1. `angzarr-cli` scaffolds and generates implementors for every language from
   templates that live in that language's client repo (`angzarr-client-<lang>`).
   The CLI holds only language-neutral parts (component model, lint,
   orchestration, scaffold-once rule). No language emitter remains in the CLI;
   no binding remains in `angzarr-router`.
2. Every examples repo's blackjack implementation is scaffolded with the CLI
   (stubs from `angzarr scaffold <lang>`, wiring from `angzarr codegen <lang>`)
   and filled in by hand with business logic only.
3. Poker is gone everywhere (it lives in git history).
4. The blackjack cucumber acceptance suite
   (`angzarr-project features/example/blackjack-acceptance/cluster.feature`)
   passes against deployed aggregates — every language, no `@needs-core-*` tags
   left — locally first, then in each examples repo's CI against published core
   images.

A language is **done** when all of these hold:

| # | Criterion |
|---|---|
| L1 | `client-<lang>` hosts the router FFI binding, thin consumer clients, the generic `ComponentHost`, the `testing` namespace, and `codegen/` templates (codegen + scaffold). Old engine code deleted. |
| L2 | `client-<lang>` CI: builds the router cdylib at a pinned rev, runs `angzarr-router/conformance`, generates code from the conformance protos with the CLI using its own templates, compiles + runs it, runs the client spec (parity incl. hosting, identity, consumer clients), mutation gate. |
| L3 | Template output is byte-equivalent to the CLI's Go emitter for the conformance + blackjack protos (before the emitter is deleted); then the emitter and `angzarr-router/bindings/<lang>` are deleted. |
| L4 | `examples-<lang>` blackjack: scaffolded by the CLI, business logic mirrors the Python template (structure per `feedback_python_is_structural_template`), unit tier runs `blackjack/` + `blackjack-framework/` features strict, mutation ≥ 90%. |
| L5 | `examples-<lang>` deploys the six components via its chart/skaffold on kind and passes the full acceptance suite locally against core images built from source; CI runs acceptance against published images (non-required until core publishes). |
| L6 | Business-vocabulary guard test in `client-<lang>` passes (no example/business identifiers). |

## 1. Current state (2026-10-01)

| Area | State |
|---|---|
| Spec | angzarr-project #17 `fix/review-2026-09` (framework) and #18 `feat/blackjack-spec` (blackjack, package `io.angzarr.examples.v1`, poker deleted). Latest: RejectionNotification.code (90704db / 72cf68f). |
| CLI | #1 `rem/review-2026-09` (lint, six Go emitters, facts, ABI 3); #2 `feat/client-repo-templates` (model JSON v1, template contract `docs/templates.md`, template fetch, Python emitter deleted). **No CI workflow.** |
| Router | #1 `rem/review-2026-09` (ABI 3, conformance absorbs client dispatch specs); #2 `feat/move-python-binding` (bindings/python removed). Bindings left: go 4.5k, java 4.7k, csharp 6.9k, cpp 3.8k, typescript 4.6k lines. **No CI workflow.** |
| client-python | #26: binding + templates + ComponentHost + Linux wheel; CI green; 97% mutation. In flight: speculative path, rejection code. |
| client-rust | #28: on the router crate (git dep), testing feature, hosting; in flight: rejection code. Rust stays on proc macros; needs scaffold-only templates. |
| client-go | `main` on `feat/cross-language-unification` (4 months old), own Go engine (to drop per router D2); only tooling files dirty. |
| client-java | `main` on `feat/migrate-to-new-proto-layout`, **28 uncommitted files** (step definitions, angzarr-project pointer). |
| client-csharp / client-cpp | open PR #7 each (`feat/migrate-to-new-proto-layout`), small dirty trees. |
| client-typescript | **does not exist** on GitHub (local dir has only `TIER5_PLAN.md`). No `examples-typescript` repo either. |
| examples-python | #16 blackjack on client-python ComponentHost, 206/206 scenarios; acceptance never run against deployed aggregates (published coordinator images predate the review). |
| examples-{rust,go,java,csharp,cpp} | poker deleted (one PR each, CI green); skeleton infra kept. |
| core | #27 `rem/review-2026-09` (2PC removed, outbox, all fixes); CI fixes pushed; never published — published images/chart are May vintage. |

## 2. Decisions this plan relies on

- Client repo is each language's single home (binding + consumer clients + generic host + templates); CLI renders templates fetched by pinned git ref (`templates=github.com/angzarr-io/angzarr-client-<lang>@<sha|tag>`), local path override for dev.
- Hosting is fully generic; no business concepts in client repos (guard test).
- Dispatch behaviour is specified only by `angzarr-router/conformance`; every client CI runs it.
- Native library packaging Linux-only for now.
- Consumers test against published artifacts in CI; local runs may build core from source.
- Python is the structural template; Go is the next language.
- Poker is never maintained; gates retarget at blackjack.

## 3. Phases

### Phase 0 — Land what is in flight (parallel, now)
1. RejectionNotification.code end to end: core #27 fills it; router #1 passes it through all bindings; client-python #26 and client-rust #28 expose it; examples-python reads it (drops "CODE: message" parsing).
2. client-python #26 speculative signal in ComponentHost; examples-python ledger honours it.
3. core #27 CI green (stale-sequence test, workflow recipe names, gateway buf in container); client-rust #28 CI green (router conformance in CI, hosting).
4. Add thin CI workflows to angzarr-cli (test, lint, generate-check, compile-go, smoke) and angzarr-router (Rust + each remaining binding's test/lint + conformance) — both repos currently have none.
Exit: every open PR green in CI.

### Phase 1 — Python acceptance on deployed aggregates (local kind)
1. kind cluster (rootless docker; containerd-snapshotter off), core coordinator images built from `rem/review-2026-09` via skaffold and loaded; core chart from the branch; postgres + rabbitmq per chart.
2. Deploy examples-python's six components (ComponentHost images) via its deploy recipes with a local image override.
3. Run all 36 acceptance scenarios including `@needs-core-*`; assertions via EventStreamSubscriber + EventQuery + LedgerQuery (no bookkeeping mirrors).
4. Root-cause every failure and fix where it belongs (core #27, examples-python #16, client-python #26, chart); spec bugs → angzarr-project.
5. Remove passing `@needs-core-*` tags upstream (#18).
6. Capture the run as a `just` recipe (`acceptance-local`) other languages reuse.
Exit: 36/36 locally; documented reproduction.

### Phase 2 — Harden the template contract (before fan-out)
1. Freeze model JSON schema v1 from what Python needed; document every field; add schema conformance tests in the CLI.
2. CLI golden tests: for each language, render the client repo's templates (pinned) over the conformance + blackjack protos and compare to committed goldens; CI runs it.
3. Template helper set reviewed against the five remaining emitters (type mapping, imports, casing, file layout) so ports need no new CLI helpers mid-flight.
4. Versioning: examples and client CI pin template sources by full commit SHA; CLI pins nothing language-specific.
Exit: contract stable; adding a language is template work only.

### Phase 3 — Language fan-out (Go first and fully; then Java, C#, C++, Rust in parallel)
Per language, three PRs (client, router, CLI) plus one examples PR:
1. **client-<lang>**: resolve existing WIP first (see §6 decisions); new branch from `main`; move `angzarr-router/bindings/<lang>`; drop old engine; generic ComponentHost + health/readiness; `testing` namespace; spec parity (compute_root `domain:key`, D-4 enums, `/` type URLs, facts, rejection code, hosting scenarios); `codegen/` templates reproducing the CLI emitter; guard test; CI per L2; Linux-only packaging (Go module + cgo, Maven classifier, NuGet runtimes/linux-x64, CMake package).
2. **Equivalence**: generate conformance + blackjack with the Go emitter and the templates; diff empty (or explained).
3. **router**: delete `bindings/<lang>` (+ recipes).
4. **CLI**: delete `<lang>.go` emitter; `codegen/scaffold <lang>` require `templates=`.
5. **examples-<lang>**: `angzarr scaffold <lang>` + `codegen <lang>` against the blackjack protos with the client templates; implement business logic mirroring Python (cards/shoe golden vectors, rules, ledger invariants L1–L4); unit tier (cucumber blackjack + framework, strict); mutation ≥ 90%; Containerfile/skaffold/helm for six components; acceptance harness; thin CI.
6. **Acceptance**: Phase-1 recipe on kind for this language; 36/36.
Rust variant: client-rust gains scaffold-only templates (macro-annotated stubs); codegen stays the macros; examples-rust scaffolded via `angzarr scaffold rust`; wiring via client-rust macros.
TypeScript is out of scope (§6.1).

### Phase 4 — Publish and wire CI
1. Merge order: angzarr-project #17 → #18; router #1 → #2 (+ per-language deletions); CLI #1 → #2 (+ per-language); core #27; client repos; examples repos. Re-pin anything pinned to branch commits that squash-merges orphan.
2. core release publishes coordinator images + chart (io.angzarr.v1); digest bumpers pin them in examples repos.
3. Remove `continue-on-error` from examples Acceptance jobs; make Acceptance required.
Exit: every examples repo's CI runs acceptance green against published images.

### Phase 5 — Cleanup
Router has no `bindings/`; CLI has no emitters; site code regions pull from examples-python blackjack (vendor bump); delete stale worktrees/branches; memory updated.

## 4. Orchestration
- One agent per repo-stream, at most ~5 concurrently; one branch + PR per repo per phase (no proliferation).
- Worktrees named `<repo>/<purpose>`; never edit others' worktrees or in-flight WIP.
- Every agent: tests first, hooks on, mutation after green, leave live runs alone, push fast-forward only.
- Disk hygiene: Go mutation/compile recipes use a throwaway GOCACHE; agents clean build dirs when done; prune dangling images; check `df` between phases.

## 5. Risks
| Risk | Mitigation |
|---|---|
| Disk exhaustion (go-build hit 196G) | throwaway GOCACHE in Go-heavy recipes; per-phase cleanup |
| Usage limits stopping agents mid-task | small resumable steps; commit often |
| BSR rate limits in binding builds | vendor protos from the angzarr-project submodule (no BSR) per `feedback_no_proto_copying` |
| Toolchain images lag (ROUTER-18) | republish angzarr-project images in Phase 2 |
| Core behaviour bugs surfacing only on cluster | Phase 1 before fan-out |
| Parallel agents editing the same branch | one owner per branch; coordination through me |

## 6. Decisions (user, 2026-10-01)
1. **TypeScript dropped for now**: no client/examples repos are created; Phase 5 deletes the TS router binding and the TS CLI emitter. Revisit later.
2. **Existing client WIP is finished and landed first**: client-java's uncommitted work, client-csharp #7, client-cpp #7 and client-go's branch are brought green and merged before that language's fan-out branch starts (a Phase 3 prerequisite per language).
3. **Cadence**: Go completes L1–L6 (including acceptance) first; then Java, C#, C++ and Rust overlap.
4. **Pinning**: commit SHAs only (no template tags); bumps are deliberate commits.
