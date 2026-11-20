#!/usr/bin/env bash
# Self-test of gates/drop-worktree.sh, the one way the gates and scripts let go
# of a throwaway worktree: it removes its own worktree and record and nothing
# else, even when another worktree's directory cannot be seen (as in a
# container that mounts only the checkout, where `git worktree prune` deleted
# every other worktree's record). Also holds that no gate or script runs a
# repository-wide `git worktree prune`.
set -euo pipefail
while IFS= read -r v; do unset "$v"; done < <(git rev-parse --local-env-vars)
here=$(cd "$(dirname "$0")" && pwd)
root=$(dirname "$here")
drop=$root/gates/drop-worktree.sh
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
fail() { echo "FAIL drop-worktree.selftest: $*" >&2; exit 1; }

# No gate, script or justfile line runs `worktree prune` (comments aside).
pruners=$(grep -nE '^[^#]*worktree[[:space:]]+prune' "$root"/gates/*.just "$root"/justfile "$root"/scripts/*.sh 2>/dev/null \
    | grep -v '/scripts/drop-worktree.selftest.sh:' || true)
[ -z "$pruners" ] || fail "a repository-wide prune: $pruners"

repo=$tmp/repo
git init -q -b main "$repo"
git -C "$repo" config user.email t@example.com
git -C "$repo" config user.name t
echo base > "$repo/README"
git -C "$repo" add . && git -C "$repo" commit -qm base
records() { ls -A "$repo/.git/worktrees" 2>/dev/null | sort | tr '\n' ' '; }

# A bystander whose directory is out of sight (unmounted), one whose path
# extends the throwaway's, and the throwaway itself.
git -C "$repo" worktree add -q --detach "$tmp/bystander" HEAD
git -C "$repo" worktree add -q --detach "$tmp/own.staged.more" HEAD
git -C "$repo" worktree add -q --detach "$tmp/own.staged" HEAD
mv "$tmp/bystander" "$tmp/bystander.unmounted"
"$drop" "$repo" "$tmp/own.staged"
[ ! -e "$tmp/own.staged" ] || fail "the worktree's directory is left"
[ "$(records)" = "bystander own.staged.more " ] || fail "records after a drop: $(records)"

# A record whose directory is already gone (an interrupted run) is dropped,
# so the next add at the path is not refused; the bystander still survives.
git -C "$repo" worktree add -q --detach "$tmp/own.staged" HEAD
rm -rf "$tmp/own.staged"
"$drop" "$repo" "$tmp/own.staged"
[ "$(records)" = "bystander own.staged.more " ] || fail "records after dropping a missing tree: $(records)"
git -C "$repo" worktree add -q --detach "$tmp/own.staged" HEAD || fail "the path cannot be added again"

# A dirty throwaway goes too (it is throwaway), its neighbours untouched.
echo dirt > "$tmp/own.staged/dirt"
"$drop" "$repo" "$tmp/own.staged"
[ ! -e "$tmp/own.staged" ] || fail "a dirty worktree's directory is left"
[ "$(records)" = "bystander own.staged.more " ] || fail "records after a dirty drop: $(records)"

# Nothing at all at the path: nothing to do, nothing touched.
"$drop" "$repo" "$tmp/never" || fail "dropping nothing failed"
[ "$(records)" = "bystander own.staged.more " ] || fail "records after dropping nothing: $(records)"

# The checkout itself is refused.
if "$drop" "$repo" "$repo" 2> /dev/null; then fail "the checkout itself was dropped"; fi
[ -d "$repo/.git" ] || fail "the checkout is gone"

echo "ok drop-worktree.selftest"
