#!/usr/bin/env bash
# The pre-push gate beyond the pushed commit's tests (gates/lefthook.yml's
# `tests`): `just check` over the working tree, then `just mutants-push` over
# what the push adds, the mutation run under the machine's shared mutation lock
# (gates/mutation-lock.sh). lefthook gives git's pushed refs on stdin and the
# remote's name as $1.
#
#     gates/pre-push.sh <remote>
#
# Stdin is read once into a file; with no refs on stdin (a hook run another
# way) the current branch's HEAD against its upstream name is judged.
set -euo pipefail
remote=${1:?usage: pre-push.sh <remote>}
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cat > "$work/refs" || true
if [ ! -s "$work/refs" ]; then
    branch=$(git rev-parse --abbrev-ref HEAD)
    echo "refs/heads/$branch $(git rev-parse HEAD) refs/heads/$branch 0000000000000000000000000000000000000000" > "$work/refs"
fi
${PRE_PUSH_JUST:-just} check
gates/mutation-lock.sh "pre-push" ${PRE_PUSH_JUST:-just} mutants-push "$remote" < "$work/refs"
