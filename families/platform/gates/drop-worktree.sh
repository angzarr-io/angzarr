#!/usr/bin/env bash
# drop-worktree.sh <top> <path>: remove ONE linked worktree of the repository
# checked out at <top>, the one at <path>: its directory and its own record
# under the common git directory's worktrees/, and nothing else. A missing
# directory with a record left behind (an interrupted run) loses the record
# too, so the next `git worktree add` at <path> is not refused.
#
# Never `git worktree prune`: prune judges EVERY worktree of the repository and
# deletes the record of each whose directory it cannot see. In a container
# that mounts only this checkout (the dev container), that is every other
# worktree on the host, their sessions' uncommitted work cut off from the
# repository (2026-11-12: three worktrees' records lost that way).
set -euo pipefail
[ $# -eq 2 ] || { echo "usage: drop-worktree.sh <top> <path>" >&2; exit 2; }
# Under a git hook, git exports variables naming the real repository.
while IFS= read -r v; do unset "$v"; done < <(git rev-parse --local-env-vars)
top=$(realpath -m "$1")
path=$(realpath -m "$2")
common=$(git -C "$top" rev-parse --path-format=absolute --git-common-dir)
if [ "$path" = "$top" ] || [ "$path" = "$(realpath -m "$common/..")" ]; then
    echo "drop-worktree.sh: $path is the checkout itself, not a throwaway worktree" >&2
    exit 2
fi
# The records naming <path>: a record's gitdir file holds <worktree>/.git,
# absolute, or relative to the record (worktree.useRelativePaths).
own=()
for record in "$common"/worktrees/*; do
    [ -f "$record/gitdir" ] || continue
    gitdir=$(cat "$record/gitdir")
    case "$gitdir" in /*) ;; *) gitdir=$record/$gitdir ;; esac
    if [ "$(realpath -m "$(dirname "$gitdir")")" = "$path" ]; then own+=("$record"); fi
done
if [ -e "$path" ]; then
    git -C "$top" worktree remove --force "$path" > /dev/null 2>&1 || rm -rf "$path"
fi
for record in ${own[@]+"${own[@]}"}; do rm -rf "$record"; done
