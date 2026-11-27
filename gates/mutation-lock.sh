#!/usr/bin/env bash
# Run a command under the machine's shared mutation lock: at most ONE mutation
# run (cargo mutants, any repository) on the machine at a time. The holder line
# "<owner> <worktree> <started>" is written to the holder file while the lock
# is held and removed when the command ends (the agreement is in
# ~/.cache/disk-slots/README).
#
#     gates/mutation-lock.sh <owner> <command>...
#
# Environment (the self-test's seam): MUTATION_LOCK, MUTATION_HOLDER. The wait
# has no timeout: the kernel lets a flock go when its holder dies.
set -euo pipefail
[ $# -ge 2 ] || { echo "usage: mutation-lock.sh <owner> <command>..." >&2; exit 2; }
owner=$1
shift
slots=${MUTATION_SLOTS:-$HOME/.cache/disk-slots}
lock=${MUTATION_LOCK:-$slots/mutation.lock}
holder=${MUTATION_HOLDER:-$slots/mutation.holder}
if ! command -v flock >/dev/null 2>&1; then
    echo "FAIL mutation-lock: flock is not on PATH; a mutation run must not go unlocked" >&2
    exit 1
fi
mkdir -p "$(dirname "$lock")" "$(dirname "$holder")"
exec 9>>"$lock"
if ! flock -n 9; then
    echo "wait mutation-lock: $lock is held: $(cat "$holder" 2>/dev/null || echo 'holder unknown')" >&2
    flock 9
fi
tag="pid=$$"
here=$(pwd)
echo "$owner $here $(date -u +%Y-%m-%dT%H:%M:%SZ) $tag" >> "$holder"
release() {
    # Only this run's line; the lock is still held, so no other writer races.
    if [ -f "$holder" ]; then
        { grep -vF " $tag" "$holder" || true; } > "$holder.$$" && mv "$holder.$$" "$holder"
    fi
}
trap release EXIT
# The command runs without the lock's descriptor, so nothing it leaves running
# holds the lock after it.
rc=0
"$@" 9>&- || rc=$?
exit "$rc"
