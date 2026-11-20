# Changing the gates

These gates were the shared gates repository (`~/workspace/gates`), brought
into angzarr-embedded at its `8c131cf` with their history, as taskloom and ctxloom-rs
brought in their own copies. They are angzarr-embedded's own now: change them here,
and nothing here reaches the other repositories, nor anything there this
one. Carry a fix across by hand when more than one needs it.

## Moving a branch off the `.gates` link

A branch made before the gates came in has its justfile import
`.gates/gates.just` and its lefthook.yml extend `.gates/lefthook.yml`.
Rebasing it on main takes main's justfile, lefthook.yml and .gitignore;
then delete the checkout's `.gates` link (`rm .gates`), which nothing reads
any more and which .gitignore no longer hides. A branch's own change to the
`gates-link` or `gates-check` recipes or the `gates_dir` variable is to be
dropped in the rebase: they are gone, and `hooks-install` no longer depends
on `gates-link`.

## A new hook needs `lefthook install`

lefthook installs a hook only for the hooks its config names when it is run,
and does not notice new ones in an `extends` file. After a change to
`gates/lefthook.yml` that names a hook not installed yet, each clone runs,
once:

```sh
lefthook install          # or: just hooks-install
```

## Red tests on branches

The gates allow a commit with failing tests on any branch but the default
one (warned `RED  tests: ... cannot merge into <default> until they pass`),
refuse it on the default branch, and keep red tests out of it with three
hooks: pre-merge-commit (a merge commit is tested as a commit is), pre-push
(a push to the remote's default branch runs the whole workspace's tests on
the pushed commit) and post-merge (a fast-forward of the default branch to
local work, which no hook can refuse, is tested just after and the way back
named). With a remote, record its default branch (`git remote set-head
origin --auto`): without origin/HEAD the default branch is the first of
init.defaultBranch, main and master that is a branch.

## The mutation verdict is a recipe, and takes no feature rule

Carried from ctxloom (its rulings 99/105): the verdict is the private recipe
`_mutants-verdict <gate> <out> <rc> [thresholds]` in `gates.just`, inline jq
over cargo-mutants' `outcomes.json` (`outcomes[]` and `total_mutants`; the
count found no longer comes from `mutants.json`) and its exit code, with no
log read. Besides what it failed before, it fails a run that found mutants
and judged none of them at all (every one unviable; owner rulings 120, 124:
the whole run's rule, so a tier all unviable beside one that judged passes,
its row saying "none judged"). `mutants-push`, `mutants-staged`,
`mutants-all` and `_mutants-judge` refuse a non-empty `feature_rules`
(`_mutants-no-features`) and pass no feature to cargo mutants;
`features_prepare` runs before the tests alone. A crate whose tests need a
feature of its own turns it on through a dev-dependency on itself
(`<crate> = { path = ".", features = ["<feature>"] }`). Its self-test is
`scripts/mutants-verdict.selftest.sh`, in `just selftest`.
