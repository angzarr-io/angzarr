#!/usr/bin/env bash
# Self-test of the pre-push gate: the shared mutation lock is taken around the
# mutation run (the holder line is written while held and removed after, a
# second run waits and says who holds it, and the command's status passes
# through), and pre-push.sh runs `just check` first and `mutants-push` under
# the lock with the pushed refs on its stdin.
set -euo pipefail
while IFS= read -r v; do unset "$v"; done < <(git rev-parse --local-env-vars)
here=$(cd "$(dirname "$0")" && pwd)
root=$(dirname "$(dirname "$here")")
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
fail() { echo "FAIL pre-push.selftest: $*" >&2; exit 1; }
export MUTATION_LOCK=$tmp/mutation.lock MUTATION_HOLDER=$tmp/mutation.holder
lockrun=$root/gates/mutation-lock.sh

# The holder line is there while the command runs, and gone after.
"$lockrun" me sh -c 'grep -q "^me " "$MUTATION_HOLDER"' || fail "no holder line while the lock was held"
[ ! -s "$MUTATION_HOLDER" ] || fail "holder line left behind: $(cat "$MUTATION_HOLDER")"

# The command's own status passes through, and the line is removed on failure.
rc=0; "$lockrun" me sh -c 'exit 7' || rc=$?
[ "$rc" = 7 ] || fail "status $rc, not 7"
[ ! -s "$MUTATION_HOLDER" ] || fail "holder line left after a failure"

# A second run waits for the first (never runs at once) and names the holder.
( "$lockrun" first sh -c 'sleep 2; echo first-done >> "$0"' "$tmp/order" ) &
sleep 0.5
"$lockrun" second sh -c 'echo second-ran >> "$0"' "$tmp/order" 2> "$tmp/err"
wait
[ "$(sed -n 1p "$tmp/order")" = first-done ] || fail "the second ran before the first ended: $(cat "$tmp/order")"
grep -q 'wait mutation-lock:.*first ' "$tmp/err" || fail "the wait did not name the holder: $(cat "$tmp/err")"

# No command, no run.
if "$lockrun" me 2>/dev/null; then fail "ran with no command"; fi

# pre-push.sh: check first, then mutants-push with the refs on stdin, under the lock.
cat > "$tmp/fakejust" <<'F'
#!/usr/bin/env bash
echo "$@" >> "$FAKE_LOG"
if [ "$1" = mutants-push ]; then
    cat >> "$FAKE_LOG"
    grep -q "^pre-push " "$MUTATION_HOLDER" || { echo "mutants-push ran without the lock" >> "$FAKE_LOG"; exit 9; }
fi
F
chmod +x "$tmp/fakejust"
export FAKE_LOG=$tmp/fake.log PRE_PUSH_JUST=$tmp/fakejust
echo "refs/heads/b aaaa refs/heads/b bbbb" | "$root/gates/pre-push.sh" origin
[ "$(sed -n 1p "$FAKE_LOG")" = check ] || fail "check did not run first: $(cat "$FAKE_LOG")"
[ "$(sed -n 2p "$FAKE_LOG")" = "mutants-push origin" ] || fail "mutants-push origin did not follow: $(cat "$FAKE_LOG")"
[ "$(sed -n 3p "$FAKE_LOG")" = "refs/heads/b aaaa refs/heads/b bbbb" ] || fail "the pushed refs did not reach mutants-push"
# A failing check stops before mutation.
: > "$FAKE_LOG"
cat > "$tmp/failcheck" <<'F'
#!/usr/bin/env bash
echo "$@" >> "$FAKE_LOG"
[ "$1" != check ] || exit 1
F
chmod +x "$tmp/failcheck"
if PRE_PUSH_JUST=$tmp/failcheck "$root/gates/pre-push.sh" origin </dev/null 2>/dev/null; then fail "a failing check passed"; fi
! grep -q mutants-push "$FAKE_LOG" || fail "mutation ran after a failing check"
echo "ok   pre-push.selftest"
